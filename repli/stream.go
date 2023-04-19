package repli

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
	"github.com/octu0/bitcaskdb/runtime"
)

const (
	SubjectCurrentFileID  string = "current_fileID"
	SubjectCurrentFileIds string = "current_fileids"
	SubjectCurrentIndex   string = "current_index"
	SubjectFetchSize      string = "fetch_size"
	SubjectFetchData      string = "fetch_data"
	SubjectRepli          string = "repli"
)

const (
	defaultReleaseTTL    time.Duration = 30 * time.Minute
	reservedPartKeySpace int64         = 4 * 1024 // 4KB
)

var (
	_ Emitter = (*streamEmitter)(nil)
	_ Reciver = (*streamReciver)(nil)
)

type (
	RequestCurrentFileID struct {
	}
	ResponseCurrentFileID struct {
		FileID datafile.FileID
		Err    string
	}
)

type (
	RequestCurrentFileIds struct {
	}
	ResponseCurrentFileIds struct {
		FileIds []datafile.FileID
		Err     string
	}
)

type (
	RequestCurrentIndex struct {
		FileID datafile.FileID
	}
	ResponseCurrentIndex struct {
		Index int64
		Err   string
	}
)

type (
	RequestFetchSize struct {
		FileID datafile.FileID
		Index  int64
	}
	ResponseFetchSize struct {
		Size int64
		EOF  bool
		Err  string
	}
)

type (
	RequestFetchData struct {
		FileID datafile.FileID
		Index  int64
		Size   int64
	}
	ResponseFetchData struct {
		Checksum    uint32
		PartKeys    []string
		EntryKey    []byte
		EntryValue  []byte
		EntryExpiry time.Time
		Err         string
	}
)

type RepliType uint8

const (
	RepliInsert RepliType = iota
	RepliDelete
	RepliCurrentFileID
	RepliMerge
)

type (
	PartialData struct {
		Data []byte
		Err  string
	}
	RepliData struct {
		Type        RepliType
		FileID      datafile.FileID
		Index       int64
		Size        int64
		Checksum    uint32
		EntryKey    []byte
		EntryValue  []byte
		EntryExpiry time.Time
		PartKeys    []string
	}
)

type partDataEncodeFunc func(
	out *bytes.Buffer,
	keys []string,
	data []byte,
) error

type releaseFunc func()

type emitType uint8

const (
	emitInsert emitType = iota
	emitDelete
	emitCurrentFileID
	emitMerge
)

type emitApply struct {
	emit        emitType
	deleteKey   []byte
	insertFiler indexer.Filer
	fileID      datafile.FileID
	mergedFiler []indexer.MergeFiler
}

type streamEmitter struct {
	mutex       *sync.RWMutex
	ctx         runtime.Context
	logger      *log.Logger
	tempDir     string
	maxPayload  int32
	releaseTTL  time.Duration
	emitApplyCh chan emitApply
	done        chan struct{}
	closed      bool
	src         Source
	server      *server.Server
	emitConn    *nats.Conn
	replyConn   *nats.Conn
	subs        []*nats.Subscription
	releases    []*releaseAndDeadline
}

type releaseAndDeadline struct {
	deadline time.Time
	release  releaseFunc
}

func (e *streamEmitter) addRelease(fn releaseFunc) {
	if fn == nil {
		return
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.releases = append(e.releases, &releaseAndDeadline{
		deadline: time.Now().Add(e.releaseTTL),
		release:  fn,
	})
}

func (e *streamEmitter) runRelease(target []*releaseAndDeadline) {
	for _, rd := range target {
		rd.release()
	}
}

func (e *streamEmitter) runReleaseLoop() {
	split := func(now time.Time) []*releaseAndDeadline {
		e.mutex.Lock()
		defer e.mutex.Unlock()

		nowUnixnano := now.UnixNano()
		nextLoop := make([]*releaseAndDeadline, 0, len(e.releases))
		target := make([]*releaseAndDeadline, 0, len(e.releases))
		for _, rd := range e.releases {
			if rd.deadline.UnixNano() < nowUnixnano {
				target = append(target, rd)
			} else {
				nextLoop = append(nextLoop, rd)
			}
		}
		e.releases = nextLoop
		return target
	}

	ticker := time.NewTicker(e.releaseTTL)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			e.runRelease(e.releases)
			return

		case <-ticker.C:
			target := split(time.Now())
			go e.runRelease(target)
		}
	}
}

func (e *streamEmitter) emitLoop() {
	for {
		select {
		case <-e.done:
			return
		case apply := <-e.emitApplyCh:
			switch apply.emit {
			case emitInsert:
				e.applyInsert(apply.insertFiler)
			case emitDelete:
				e.applyDelete(apply.deleteKey)
			case emitCurrentFileID:
				e.applyCurrentFileID(apply.fileID)
			case emitMerge:
				e.applyMerge(apply.mergedFiler)
			}
		}
	}
}

func (e *streamEmitter) applyInsert(filer indexer.Filer) {
	entry, err := e.src.Read(filer.FileID, filer.Index, filer.Size)
	if err != nil {
		e.logger.Printf("error: apply insert err: %+v", err)
		return
	}
	defer entry.Close()

	encodeFn := func(out *bytes.Buffer, keys []string, data []byte) error {
		return gob.NewEncoder(out).Encode(RepliData{
			Type:        RepliInsert,
			FileID:      filer.FileID,
			Index:       filer.Index,
			Size:        filer.Size,
			Checksum:    entry.Checksum,
			EntryKey:    entry.Key,
			EntryValue:  data,
			EntryExpiry: entry.Expiry,
			PartKeys:    keys,
		})
	}
	releaseFn, err := e.publishData(e.emitConn, SubjectRepli, entry, encodeFn)
	if err != nil {
		e.logger.Printf("error: apply insert err: %+v", err)
		return
	}
	e.addRelease(releaseFn)
}

func (e *streamEmitter) applyDelete(key []byte) {
	bufPool := e.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RepliData{
		Type:        RepliDelete,
		FileID:      datafile.FileID{},
		Index:       0,
		Size:        0,
		Checksum:    0,
		EntryKey:    key,
		EntryValue:  nil,
		EntryExpiry: time.Time{},
		PartKeys:    nil,
	}); err != nil {
		e.logger.Printf("error: apply delete err: %+v", err)
		return
	}

	if err := e.emitConn.Publish(SubjectRepli, out.Bytes()); err != nil {
		e.logger.Printf("error: apply delete err: %+v", err)
		return
	}
	e.emitConn.Flush()
}

func (e *streamEmitter) applyCurrentFileID(fileID datafile.FileID) {
	bufPool := e.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RepliData{
		Type:        RepliCurrentFileID,
		FileID:      fileID,
		Index:       0,
		Size:        0,
		Checksum:    0,
		EntryKey:    []byte{},
		EntryValue:  nil,
		EntryExpiry: time.Time{},
		PartKeys:    nil,
	}); err != nil {
		e.logger.Printf("error: apply current fileID err: %+v", err)
		return
	}

	if err := e.emitConn.Publish(SubjectRepli, out.Bytes()); err != nil {
		e.logger.Printf("error: apply current fileID err: %+v", err)
		return
	}
	e.emitConn.Flush()
}

func (e *streamEmitter) applyMerge(mergedFiler []indexer.MergeFiler) {
	bufPool := e.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(mergedFiler); err != nil {
		e.logger.Printf("error: apply merge err: %+v", err)
		return
	}

	key := []byte(fmt.Sprintf("merge%d", time.Now().UnixNano()))
	entry := &datafile.Entry{
		Key:       key,
		Value:     bytes.NewReader(out.Bytes()),
		TotalSize: int64(out.Len()),
		ValueSize: int64(out.Len()),
		Checksum:  0,
		Expiry:    time.Time{},
	}
	encodeFn := func(out *bytes.Buffer, keys []string, data []byte) error {
		return gob.NewEncoder(out).Encode(RepliData{
			Type:        RepliMerge,
			FileID:      datafile.FileID{},
			Index:       0,
			Size:        0,
			Checksum:    0,
			EntryKey:    key,
			EntryValue:  data,
			EntryExpiry: time.Time{},
			PartKeys:    keys,
		})
	}
	releaseFn, err := e.publishData(e.emitConn, SubjectRepli, entry, encodeFn)
	if err != nil {
		e.logger.Printf("error: apply merge err: %+v", err)
		return
	}
	e.addRelease(releaseFn)
}

func (e *streamEmitter) reconnectEmitter(conn *nats.Conn) {
	e.logger.Printf("warn: reconnected emitter: %s", conn.ConnectedUrl())
}

func (e *streamEmitter) reconnectReply(conn *nats.Conn) {
	e.logger.Printf("warn: reconnected replier: %s", conn.ConnectedUrl())
}

func (e *streamEmitter) Start(src Source, bindIP string, bindPort int) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		// maybe restart
		e.done = make(chan struct{})
	}

	go e.runReleaseLoop()
	go e.emitLoop()

	opt := &server.Options{
		Host:            bindIP,
		Port:            bindPort,
		ClientAdvertise: bindIP,
		HTTPPort:        -1,
		Cluster:         server.ClusterOpts{Port: -1},
		NoLog:           true,
		NoSigs:          true,
		Debug:           false,
		Trace:           false,
		MaxPayload:      e.maxPayload,
		PingInterval:    60 * time.Second,
		MaxPingsOut:     120,
		WriteDeadline:   10 * time.Second,
	}
	svr := server.New(opt)
	go svr.Start()

	if svr.ReadyForConnections(5*time.Second) != true {
		return errors.Errorf("error: unable to start server(%s:%d)", opt.Host, opt.Port)
	}

	natsUrl := fmt.Sprintf("nats://%s", svr.Addr().String())
	emitConn, err := conn(natsUrl, "emitter", e.reconnectEmitter)
	if err != nil {
		return errors.Wrapf(err, "nats emitter connect: %s", natsUrl)
	}

	replyConn, err := conn(natsUrl, "reply", e.reconnectReply)
	if err != nil {
		return errors.Wrapf(err, "nats reply connect: %s", natsUrl)
	}

	subCurrentFileID, err := replyConn.Subscribe(SubjectCurrentFileID, e.replyCurrentFileID(replyConn, src))
	if err != nil {
		return errors.Wrapf(err, "failed reply subj %s", SubjectCurrentFileIds)
	}

	subCurrentFileIds, err := replyConn.Subscribe(SubjectCurrentFileIds, e.replyCurrentFileIds(replyConn, src))
	if err != nil {
		return errors.Wrapf(err, "failed reply subj %s", SubjectCurrentFileIds)
	}
	subCurrentIndex, err := replyConn.Subscribe(SubjectCurrentIndex, e.replyCurrentIndex(replyConn, src))
	if err != nil {
		return errors.Wrapf(err, "failed reply subj %s", SubjectCurrentIndex)
	}
	subFetchSize, err := replyConn.Subscribe(SubjectFetchSize, e.replyFetchSize(replyConn, src))
	if err != nil {
		return errors.Wrapf(err, "failed reply subj %s", SubjectFetchSize)
	}
	subFetchData, err := replyConn.Subscribe(SubjectFetchData, e.replyFetchData(replyConn, src))
	if err != nil {
		return errors.Wrapf(err, "failed reply subj %s", SubjectFetchData)
	}

	emitConn.Flush()
	replyConn.Flush()

	e.src = src
	e.server = svr
	e.emitConn = emitConn
	e.replyConn = replyConn
	e.subs = []*nats.Subscription{
		subCurrentFileID,
		subCurrentFileIds,
		subCurrentIndex,
		subFetchSize,
		subFetchData,
	}
	e.closed = false
	return nil
}

func (e *streamEmitter) Stop() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return nil
	}

	if e.subs != nil {
		for _, sub := range e.subs {
			sub.Unsubscribe()
		}
	}
	if e.replyConn != nil {
		e.replyConn.Flush()
		e.replyConn.Close()
	}
	if e.emitConn != nil {
		e.emitConn.Flush()
		e.emitConn.Close()
	}
	close(e.done)

	if e.server != nil {
		e.server.Shutdown()
	}
	e.closed = true
	return nil
}

func (e *streamEmitter) publishReplyCurrentFileIds(conn *nats.Conn, subj string, res ResponseCurrentFileIds) {
	pool := e.ctx.Buffer().BufferPool()
	buf := pool.Get()
	defer pool.Put(buf)

	if err := gob.NewEncoder(buf).Encode(res); err != nil {
		e.logger.Printf("error: encode err: %+v", err)
		return
	}
	if err := conn.Publish(subj, buf.Bytes()); err != nil {
		e.logger.Printf("error: publish %s err: %+v", subj, err)
		return
	}
	conn.Flush()
}

func (e *streamEmitter) publishReplyCurrentFileID(conn *nats.Conn, subj string, res ResponseCurrentFileID) {
	pool := e.ctx.Buffer().BufferPool()
	buf := pool.Get()
	defer pool.Put(buf)

	if err := gob.NewEncoder(buf).Encode(res); err != nil {
		e.logger.Printf("error: encode err: %+v", err)
		return
	}
	if err := conn.Publish(subj, buf.Bytes()); err != nil {
		e.logger.Printf("error: publish %s err: %+v", subj, err)
		return
	}
	conn.Flush()
}

func (e *streamEmitter) replyCurrentFileID(conn *nats.Conn, src Source) nats.MsgHandler {
	return func(msg *nats.Msg) {
		req := RequestCurrentFileID{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&req); err != nil {
			e.publishReplyCurrentFileID(conn, msg.Reply, ResponseCurrentFileID{
				FileID: datafile.FileID{},
				Err:    err.Error(),
			})
			return
		}

		e.publishReplyCurrentFileID(conn, msg.Reply, ResponseCurrentFileID{
			FileID: src.CurrentFileID(),
			Err:    "",
		})
	}
}

func (e *streamEmitter) replyCurrentFileIds(conn *nats.Conn, src Source) nats.MsgHandler {
	return func(msg *nats.Msg) {
		req := RequestCurrentFileIds{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&req); err != nil {
			e.publishReplyCurrentFileIds(conn, msg.Reply, ResponseCurrentFileIds{
				FileIds: nil,
				Err:     err.Error(),
			})
			return
		}

		e.publishReplyCurrentFileIds(conn, msg.Reply, ResponseCurrentFileIds{
			FileIds: src.FileIds(),
			Err:     "",
		})
	}
}

func (e *streamEmitter) publishReplyCurrentIndex(conn *nats.Conn, subj string, res ResponseCurrentIndex) {
	pool := e.ctx.Buffer().BufferPool()
	buf := pool.Get()
	defer pool.Put(buf)

	if err := gob.NewEncoder(buf).Encode(res); err != nil {
		e.logger.Printf("error: encode err: %+v", err)
		return
	}
	if err := conn.Publish(subj, buf.Bytes()); err != nil {
		e.logger.Printf("error: publish %s err: %+v", subj, err)
		return
	}
	conn.Flush()
}

func (e *streamEmitter) replyCurrentIndex(conn *nats.Conn, src Source) nats.MsgHandler {
	return func(msg *nats.Msg) {
		req := RequestCurrentIndex{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&req); err != nil {
			e.publishReplyCurrentIndex(conn, msg.Reply, ResponseCurrentIndex{
				Index: -1,
				Err:   err.Error(),
			})
			return
		}

		e.publishReplyCurrentIndex(conn, msg.Reply, ResponseCurrentIndex{
			Index: src.LastIndex(req.FileID),
			Err:   "",
		})
	}
}

func (e *streamEmitter) publishReplyFetchSize(conn *nats.Conn, subj string, res ResponseFetchSize) {
	pool := e.ctx.Buffer().BufferPool()
	buf := pool.Get()
	defer pool.Put(buf)

	if err := gob.NewEncoder(buf).Encode(res); err != nil {
		e.logger.Printf("error: encode err: %+v", err)
		return
	}
	if err := conn.Publish(subj, buf.Bytes()); err != nil {
		e.logger.Printf("error: publish %s err: %+v", subj, err)
		return
	}
	conn.Flush()
}

func (e *streamEmitter) replyFetchSize(conn *nats.Conn, src Source) nats.MsgHandler {
	return func(msg *nats.Msg) {
		req := RequestFetchSize{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&req); err != nil {
			e.publishReplyFetchSize(conn, msg.Reply, ResponseFetchSize{
				Size: -1,
				EOF:  true,
				Err:  err.Error(),
			})
			return
		}
		header, eofType, err := src.Header(req.FileID, req.Index)
		if err != nil {
			e.publishReplyFetchSize(conn, msg.Reply, ResponseFetchSize{
				Size: -1,
				EOF:  true,
				Err:  err.Error(),
			})
			return
		}

		// file found but empty
		if header == nil && eofType == datafile.IsEOF {
			e.publishReplyFetchSize(conn, msg.Reply, ResponseFetchSize{
				Size: 0,
				EOF:  bool(eofType),
				Err:  "",
			})
			return
		}

		e.publishReplyFetchSize(conn, msg.Reply, ResponseFetchSize{
			Size: header.TotalSize,
			EOF:  bool(eofType),
			Err:  "",
		})
	}
}

func (e *streamEmitter) publishReplyFetchDataError(conn *nats.Conn, subj string, err error) {
	pool := e.ctx.Buffer().BufferPool()
	buf := pool.Get()
	defer pool.Put(buf)

	if err := gob.NewEncoder(buf).Encode(ResponseFetchData{
		Checksum:    0,
		PartKeys:    nil,
		EntryKey:    nil,
		EntryValue:  nil,
		EntryExpiry: time.Time{},
		Err:         err.Error(),
	}); err != nil {
		e.logger.Printf("error: encode err: %+v", err)
		return
	}
	if err := conn.Publish(subj, buf.Bytes()); err != nil {
		e.logger.Printf("error: publish %s err: %+v", subj, err)
		return
	}
	conn.Flush()
}

func (e *streamEmitter) replyFetchData(conn *nats.Conn, src Source) nats.MsgHandler {
	return func(msg *nats.Msg) {
		req := RequestFetchData{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&req); err != nil {
			e.publishReplyFetchDataError(conn, msg.Reply, err)
			return
		}

		entry, err := src.Read(req.FileID, req.Index, req.Size)
		if err != nil {
			e.publishReplyFetchDataError(conn, msg.Reply, err)
			return
		}
		defer entry.Close()

		encodeFn := func(out *bytes.Buffer, keys []string, value []byte) error {
			return gob.NewEncoder(out).Encode(ResponseFetchData{
				Checksum:    entry.Checksum,
				PartKeys:    keys,
				EntryKey:    entry.Key,
				EntryExpiry: entry.Expiry,
				EntryValue:  value,
				Err:         "",
			})
		}
		releaseFn, err := e.publishData(conn, msg.Reply, entry, encodeFn)
		if err != nil {
			e.logger.Printf("error: publish data %s err:%+v", msg.Reply, err)
			return
		}
		e.addRelease(releaseFn)
	}
}

func (e *streamEmitter) EmitInsert(filer indexer.Filer) error {
	e.mutex.RLock()
	src := e.src
	closed := e.closed
	e.mutex.RUnlock()

	if src == nil {
		return errors.Errorf("maybe not Start")
	}
	if closed {
		// drop when closed, no emit to transmit differences when resuming server
		return nil
	}

	e.emitApplyCh <- emitApply{emit: emitInsert, insertFiler: filer}
	return nil
}

func (e *streamEmitter) EmitDelete(key []byte) error {
	e.mutex.RLock()
	src := e.src
	closed := e.closed
	e.mutex.RUnlock()

	if src == nil {
		return errors.Errorf("maybe not Start")
	}
	if closed {
		// drop when closed, no emit to transmit differences when resuming server
		return nil
	}

	e.emitApplyCh <- emitApply{emit: emitDelete, deleteKey: key}
	return nil
}

func (e *streamEmitter) EmitCurrentFileID(fileID datafile.FileID) error {
	e.mutex.RLock()
	src := e.src
	closed := e.closed
	e.mutex.RUnlock()

	if src == nil {
		return errors.Errorf("maybe not Start")
	}
	if closed {
		// drop when closed
		return nil
	}

	e.emitApplyCh <- emitApply{emit: emitCurrentFileID, fileID: fileID}
	return nil
}

func (e *streamEmitter) EmitMerge(merged []indexer.MergeFiler) error {
	e.mutex.RLock()
	src := e.src
	closed := e.closed
	e.mutex.RUnlock()

	if src == nil {
		return errors.Errorf("maybe not Start")
	}
	if closed {
		// drop when closed, no emit to transmit differences when resuming server
		return nil
	}

	e.emitApplyCh <- emitApply{emit: emitMerge, mergedFiler: merged}
	return nil
}

func (e *streamEmitter) replyPartial(conn *nats.Conn, f *os.File, start, end int64) nats.MsgHandler {
	return func(msg *nats.Msg) {
		bufPool := e.ctx.Buffer().BufferPool()
		buf := bufPool.Get()
		defer bufPool.Put(buf)

		size := end - start
		buf.Grow(int(size))
		rawBytes := buf.Bytes()

		out := bufPool.Get()
		defer bufPool.Put(out)

		n, err := f.ReadAt(rawBytes[:size], start)
		if err != nil {
			e.logger.Printf("error: replyPartial read error offset=%d size=%d err:%+v", start, size, err)
			return
		}

		if int64(n) < size {
			e.logger.Printf("error: replyPartial read size error requestsize=%d readsize=%d", size, n)
			return
		}

		if err := gob.NewEncoder(out).Encode(PartialData{
			Data: rawBytes[:n],
			Err:  "",
		}); err != nil {
			e.logger.Printf("error: encode err: %+v", err)
			return
		}

		if err := conn.Publish(msg.Reply, out.Bytes()); err != nil {
			e.logger.Printf("error: publish %s err: %+v", msg.Reply, err)
			return
		}
		conn.Flush()
	}
}

func (e *streamEmitter) publishDataPartial(conn *nats.Conn, subj string, entry *datafile.Entry, encodeFn partDataEncodeFunc) (releaseFunc, error) {
	bufPool := e.ctx.Buffer().BufferPool()

	f, err := os.CreateTemp(e.tempDir, "repli")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	releaseTemp := func() {
		f.Close()
		os.Remove(f.Name())
	}

	if _, err := f.ReadFrom(entry); err != nil {
		releaseTemp()
		return nil, errors.WithStack(err)
	}
	f.Seek(0, io.SeekStart)

	maxPayload := int64(e.maxPayload) - reservedPartKeySpace
	partSize := (entry.ValueSize / maxPayload) + 1
	partRequestKeys := make([]string, 0, partSize)
	subs := make([]*nats.Subscription, 0, partSize)
	for i := int64(0); i < partSize; i += 1 {
		start := i * maxPayload
		end := start + maxPayload
		if entry.ValueSize < end {
			end = entry.ValueSize
		}
		// part.(int32).p.00000-00000 => around 30 bytes.
		key := fmt.Sprintf("part.%s.p.%09d-%09d", entry.Key, start, end)

		sub, err := conn.Subscribe(key, e.replyPartial(conn, f, start, end))
		if err != nil {
			releaseTemp()
			return nil, errors.WithStack(err)
		}
		subs = append(subs, sub)
		partRequestKeys = append(partRequestKeys, key)
	}

	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := encodeFn(out, partRequestKeys, nil); err != nil {
		releaseTemp()
		return nil, errors.WithStack(err)
	}

	if err := conn.Publish(subj, out.Bytes()); err != nil {
		releaseTemp()
		return nil, errors.WithStack(err)
	}
	conn.Flush()

	releaseFn := func() {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
		releaseTemp()
	}
	return releaseFn, nil
}

func (e *streamEmitter) publishDataSingle(conn *nats.Conn, subj string, entry io.Reader, encodeFn partDataEncodeFunc) (releaseFunc, error) {
	bufPool := e.ctx.Buffer().BufferPool()
	buf := bufPool.Get()
	defer bufPool.Put(buf)

	buf.Grow(int(e.maxPayload))
	rawBytes := buf.Bytes()
	n, err := entry.Read(rawBytes[:e.maxPayload])
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := bufPool.Get()
	defer bufPool.Put(out)
	if err := encodeFn(out, nil, rawBytes[:n]); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := conn.Publish(subj, out.Bytes()); err != nil {
		return nil, errors.Wrapf(err, "publish %s size=%d", subj, out.Len())
	}
	conn.Flush()
	return nil, nil
}

func (e *streamEmitter) publishData(conn *nats.Conn, subj string, entry *datafile.Entry, encodeFn partDataEncodeFunc) (releaseFunc, error) {
	if int64(e.maxPayload) <= entry.ValueSize {
		return e.publishDataPartial(conn, subj, entry, encodeFn)
	}
	return e.publishDataSingle(conn, subj, entry, encodeFn)
}

func NewStreamEmitter(ctx runtime.Context, logger *log.Logger, tempDir string, maxPayload int32) *streamEmitter {
	if ctx == nil {
		ctx = runtime.DefaultContext()
	}
	if logger == nil {
		logger = log.Default()
	}
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	if maxPayload == 0 || int64(maxPayload) < reservedPartKeySpace {
		maxPayload = int32(10 * 1024 * 1024)
	}
	return &streamEmitter{
		mutex:       new(sync.RWMutex),
		ctx:         ctx,
		logger:      logger,
		tempDir:     tempDir,
		maxPayload:  maxPayload,
		releaseTTL:  defaultReleaseTTL,
		emitApplyCh: make(chan emitApply, 1024),
		done:        make(chan struct{}),
		closed:      false,
		src:         nil,
		server:      nil,
		emitConn:    nil,
		replyConn:   nil,
		subs:        nil,
	}
}

type streamReciver struct {
	mutex          *sync.RWMutex
	ctx            runtime.Context
	logger         *log.Logger
	tempDir        string
	requestTimeout time.Duration
	doneBehind     bool
	mergeWait      *sync.WaitGroup
	dst            Destination
	client         *nats.Conn
	subs           []*nats.Subscription
}

type streamFetchDataEntry struct {
	checksum uint32
	key      []byte
	value    io.Reader
	expiry   time.Time
	release  releaseFunc
}

func (r *streamReciver) reconnect(conn *nats.Conn) {
	r.logger.Printf("info: reconnected: %s", conn.ConnectedUrl())

	r.mutex.Lock()
	for _, sub := range r.subs {
		sub.Unsubscribe()
	}
	r.subs = nil
	r.doneBehind = false
	r.mutex.Unlock()

	if err := r.recvStart(r.dst, conn); err != nil {
		r.logger.Printf("error: reconnect recvStart() failure: %+v", err)
	}
}

func (r *streamReciver) Start(dst Destination, serverIP string, serverPort int) error {
	natsUrl := fmt.Sprintf("nats://%s:%d", serverIP, serverPort)
	client, err := conn(natsUrl, "client", r.reconnect)
	if err != nil {
		return errors.Wrapf(err, "nats client connect: %s", natsUrl)
	}

	if err := r.recvStart(dst, client); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *streamReciver) recvStart(dst Destination, conn *nats.Conn) error {
	repliTemp, err := openTemporaryRepliData(r.ctx, r.tempDir)
	if err != nil {
		return errors.Wrap(err, "temporary repli data open")
	}
	defer repliTemp.Remove()

	subRepli, err := conn.Subscribe(SubjectRepli, r.recvRepliData(conn, dst, repliTemp))
	if err != nil {
		return errors.Wrapf(err, "failed to subscribe %s", SubjectRepli)
	}
	conn.Flush()

	r.mergeWait.Add(1)
	if err := r.requestBehindData(conn, dst, repliTemp); err != nil {
		return errors.Wrap(err, "failed to get behind reqests")
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.dst = dst
	r.client = conn
	r.subs = []*nats.Subscription{
		subRepli,
	}
	r.doneBehind = true
	return nil
}

func (r *streamReciver) Stop() error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	r.mergeWait.Wait()

	if r.subs != nil {
		for _, sub := range r.subs {
			sub.Unsubscribe()
		}
	}
	if r.client != nil {
		r.client.Drain()
		r.client.Close()
	}
	r.doneBehind = false

	return nil
}

func (r *streamReciver) reqCurrentFileIds(conn *nats.Conn) ([]datafile.FileID, error) {
	bufPool := r.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RequestCurrentFileIds{}); err != nil {
		return []datafile.FileID{}, errors.WithStack(err)
	}

	msg, err := conn.Request(SubjectCurrentFileIds, out.Bytes(), r.requestTimeout)
	if err != nil {
		return []datafile.FileID{}, errors.WithStack(err)
	}

	res := ResponseCurrentFileIds{}
	if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
		return []datafile.FileID{}, errors.WithStack(err)
	}
	if res.Err != "" {
		return []datafile.FileID{}, errors.Errorf(res.Err)
	}
	return res.FileIds, nil
}

func (r *streamReciver) reqCurrentFileID(conn *nats.Conn) (datafile.FileID, error) {
	bufPool := r.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RequestCurrentFileID{}); err != nil {
		return datafile.FileID{}, errors.WithStack(err)
	}

	msg, err := conn.Request(SubjectCurrentFileID, out.Bytes(), r.requestTimeout)
	if err != nil {
		return datafile.FileID{}, errors.WithStack(err)
	}

	res := ResponseCurrentFileID{}
	if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
		return datafile.FileID{}, errors.WithStack(err)
	}
	if res.Err != "" {
		return datafile.FileID{}, errors.Errorf(res.Err)
	}
	return res.FileID, nil
}

func (r *streamReciver) reqCurrentIndex(conn *nats.Conn, fileID datafile.FileID) (int64, error) {
	bufPool := r.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RequestCurrentIndex{
		FileID: fileID,
	}); err != nil {
		return 0, errors.WithStack(err)
	}

	msg, err := conn.Request(SubjectCurrentIndex, out.Bytes(), r.requestTimeout)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	res := ResponseCurrentIndex{}
	if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
		return 0, errors.WithStack(err)
	}
	if res.Err != "" {
		return 0, errors.Errorf(res.Err)
	}
	return res.Index, nil
}

func (r *streamReciver) reqFetchSize(conn *nats.Conn, fileID datafile.FileID, index int64) (int64, bool, error) {
	bufPool := r.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RequestFetchSize{
		FileID: fileID,
		Index:  index,
	}); err != nil {
		return 0, true, errors.WithStack(err)
	}

	msg, err := conn.Request(SubjectFetchSize, out.Bytes(), r.requestTimeout)
	if err != nil {
		return 0, true, errors.WithStack(err)
	}

	res := ResponseFetchSize{}
	if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
		return 0, true, errors.WithStack(err)
	}
	if res.Err != "" {
		return 0, true, errors.Errorf(res.Err)
	}
	return res.Size, res.EOF, nil
}

func (r *streamReciver) reqFetchDataPartial(conn *nats.Conn, checksum uint32, key []byte, expiry time.Time, partKeys []string) (*streamFetchDataEntry, error) {
	f, err := os.CreateTemp(r.tempDir, "fetchrecv")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	releaseTemp := func() {
		f.Close()
		os.Remove(f.Name())
	}

	for _, key := range partKeys {
		msg, err := conn.Request(key, []byte{}, r.requestTimeout)
		if err != nil {
			releaseTemp()
			return nil, errors.WithStack(err)
		}

		res := PartialData{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			releaseTemp()
			return nil, errors.WithStack(err)
		}
		if res.Err != "" {
			releaseTemp()
			return nil, errors.Errorf("partial data error:%s", res.Err)
		}
		f.Write(res.Data)
	}

	f.Seek(0, io.SeekStart)
	releaseFn := func() {
		releaseTemp()
	}
	return &streamFetchDataEntry{
		checksum: checksum,
		key:      key,
		value:    f,
		expiry:   expiry,
		release:  releaseFn,
	}, nil
}

func (r *streamReciver) reqFetchData(conn *nats.Conn, fileID datafile.FileID, index int64, size int64) (*streamFetchDataEntry, error) {
	bufPool := r.ctx.Buffer().BufferPool()
	out := bufPool.Get()
	defer bufPool.Put(out)

	if err := gob.NewEncoder(out).Encode(RequestFetchData{
		FileID: fileID,
		Index:  index,
		Size:   size,
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	msg, err := conn.Request(SubjectFetchData, out.Bytes(), r.requestTimeout)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res := ResponseFetchData{}
	if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
		return nil, errors.WithStack(err)
	}
	if res.Err != "" {
		return nil, errors.Errorf(res.Err)
	}

	if 0 < len(res.PartKeys) && len(res.EntryValue) == 0 {
		return r.reqFetchDataPartial(conn, res.Checksum, res.EntryKey, res.EntryExpiry, res.PartKeys)
	}
	// single data
	return &streamFetchDataEntry{
		checksum: res.Checksum,
		key:      res.EntryKey,
		value:    bytes.NewReader(res.EntryValue),
		expiry:   res.EntryExpiry,
		release:  nil,
	}, nil
}

func (r *streamReciver) reqDiffData(conn *nats.Conn, dst Destination, fileID datafile.FileID, lastIndex int64) error {
	size, isEOF, err := r.reqFetchSize(conn, fileID, lastIndex)
	if err != nil {
		return errors.WithStack(err)
	}

	// file found but empty
	if size == 0 && isEOF {
		return nil
	}

	entry, err := r.reqFetchData(conn, fileID, lastIndex, size)
	if err != nil {
		return errors.WithStack(err)
	}
	if entry.release != nil {
		defer entry.release()
	}

	if err := dst.Insert(fileID, lastIndex, entry.checksum, entry.key, entry.value, entry.expiry); err != nil {
		return errors.WithStack(err)
	}

	if isEOF != true {
		// fetch next
		return r.reqDiffData(conn, dst, fileID, lastIndex+size)
	}
	return nil
}

func (r *streamReciver) requestBehindData(client *nats.Conn, dst Destination, repliTemp *temporaryRepliData) error {
	requestedFileIds := make(map[datafile.FileID]struct{})

	// Retrieve FileIDs that destination has that are behind
	for _, f := range dst.LastFiles() {
		lastIndex, err := r.reqCurrentIndex(client, f.FileID)
		if err != nil {
			return errors.Wrapf(err, "failed request current file index: fileID=%d", f.FileID)
		}

		requestedFileIds[f.FileID] = struct{}{} // mark

		if lastIndex < 0 {
			continue // not found
		}

		if f.Index < lastIndex {
			// server has new index
			if err := r.reqDiffData(client, dst, f.FileID, f.Index); err != nil {
				return errors.Wrapf(err, "failed request fetch data: fileID=%d index=%d", f.FileID, f.Index)
			}
		}
	}

	sourceFileIds, err := r.reqCurrentFileIds(client)
	if err != nil {
		return errors.Wrap(err, "failed request current file ids")
	}

	// Get FileIDs that destination not request
	for _, fileID := range sourceFileIds {
		if _, ok := requestedFileIds[fileID]; ok {
			continue
		}
		if err := r.reqDiffData(client, dst, fileID, 0); err != nil {
			return errors.Wrapf(err, "failed request fetch data: fileID=%d index=0", fileID)
		}
	}

	return r.mergeRepliData(client, dst, repliTemp)
}

func (r *streamReciver) mergeRepliData(client *nats.Conn, dst Destination, repliTemp *temporaryRepliData) error {
	defer r.mergeWait.Done()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	repliTemp.CloseWrite()

	if err := repliTemp.ReadAll(func(data RepliData) error {
		return r.takeRepliData(client, dst, data)
	}); err != nil {
		return errors.WithStack(err)
	}

	fileID, err := r.reqCurrentFileID(client)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := dst.SetCurrentFileID(fileID); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *streamReciver) takeRepliData(client *nats.Conn, dst Destination, data RepliData) error {
	switch data.Type {
	case RepliDelete:
		return dst.Delete(data.EntryKey)
	case RepliInsert:
		if 0 < len(data.PartKeys) && len(data.EntryValue) == 0 {
			return r.recvInsertRepliDataPartial(client, dst, data.FileID, data.Index, data.Checksum, data.EntryKey, data.EntryExpiry, data.PartKeys)
		}
		return dst.Insert(data.FileID, data.Index, data.Checksum, data.EntryKey, bytes.NewReader(data.EntryValue), data.EntryExpiry)
	case RepliCurrentFileID:
		return dst.SetCurrentFileID(data.FileID)
	case RepliMerge:
		if 0 < len(data.PartKeys) && len(data.EntryValue) == 0 {
			return r.recvMergeRepliDataPartial(client, dst, data.PartKeys)
		}
		return r.recvMergeRepliDataSingle(client, dst, data.EntryValue)
	}
	r.logger.Printf("warn: unknown repli.type = %v %+v", data.Type, data)
	return nil
}

func (r *streamReciver) recvRepliData(client *nats.Conn, dst Destination, repliTemp *temporaryRepliData) nats.MsgHandler {
	return func(msg *nats.Msg) {
		bufPool := r.ctx.Buffer().BufferPool()
		out := bufPool.Get()
		defer bufPool.Put(out)

		res := RepliData{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			r.logger.Printf("error: decode err: %+v", err)
			return
		}

		r.mutex.RLock()
		behind := r.doneBehind
		r.mutex.RUnlock()

		if behind != true {
			if err := repliTemp.Write(res); err != nil {
				r.logger.Printf("error: repli temp write err: %+v", err)
			}
			return
		}

		if err := r.takeRepliData(client, dst, res); err != nil {
			r.logger.Printf("error: take repli err: %+v", err)
		}
	}
}

func (r *streamReciver) recvInsertRepliDataPartial(client *nats.Conn, dst Destination, fileID datafile.FileID, index int64, checksum uint32, key []byte, expiry time.Time, partKeys []string) error {
	entry, err := r.reqFetchDataPartial(client, checksum, key, expiry, partKeys)
	if err != nil {
		return errors.Wrapf(err, "failed fetch data partial keys")
	}
	if entry.release != nil {
		defer entry.release()
	}

	return dst.Insert(fileID, index, checksum, entry.key, entry.value, entry.expiry)
}

func (r *streamReciver) recvMergeRepliDataPartial(client *nats.Conn, dst Destination, partKeys []string) error {
	entry, err := r.reqFetchDataPartial(client, 0, []byte{}, time.Time{}, partKeys)
	if err != nil {
		return errors.Wrapf(err, "failed fetch data partial keys")
	}
	if entry.release != nil {
		defer entry.release()
	}

	mergedFiler := make([]indexer.MergeFiler, 0)
	if err := gob.NewDecoder(entry.value).Decode(&mergedFiler); err != nil {
		return errors.Wrapf(err, "failed partial decode merged filer")
	}

	return dst.Merge(mergedFiler)
}

func (r *streamReciver) recvMergeRepliDataSingle(client *nats.Conn, dst Destination, data []byte) error {
	mergedFiler := make([]indexer.MergeFiler, 0)
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&mergedFiler); err != nil {
		return errors.Wrapf(err, "failed single decode merged filer")
	}

	return dst.Merge(mergedFiler)
}

func NewStreamReciver(ctx runtime.Context, logger *log.Logger, tempDir string, rto time.Duration) *streamReciver {
	if ctx == nil {
		ctx = runtime.DefaultContext()
	}
	if logger == nil {
		logger = log.Default()
	}
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	if rto < 1 {
		rto = 10 * time.Second
	}
	return &streamReciver{
		mutex:          new(sync.RWMutex),
		ctx:            ctx,
		logger:         logger,
		tempDir:        tempDir,
		requestTimeout: rto,
		doneBehind:     false,
		mergeWait:      new(sync.WaitGroup),
		dst:            nil,
		client:         nil,
		subs:           nil,
	}
}

var (
	errWriteClosed = errors.New("write closed")
)

type temporaryRepliData struct {
	m          *sync.RWMutex
	ctx        runtime.Context
	f          *os.File
	enc        *gob.Encoder
	closeWrite bool
}

func (t *temporaryRepliData) Remove() error {
	if err := t.f.Close(); err != nil {
		return err
	}
	return os.Remove(t.f.Name())
}

func (t *temporaryRepliData) ReadAll(fn func(RepliData) error) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.f.Seek(0, io.SeekStart)

	dec := gob.NewDecoder(t.f)
	for {
		data := RepliData{}
		if err := dec.Decode(&data); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		if err := fn(data); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *temporaryRepliData) CloseWrite() {
	t.m.Lock()
	defer t.m.Unlock()

	t.closeWrite = true
}

func (t *temporaryRepliData) Write(data RepliData) error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.closeWrite {
		return errors.WithStack(errWriteClosed)
	}

	return t.enc.Encode(data)
}

func openTemporaryRepliData(ctx runtime.Context, tempDir string) (*temporaryRepliData, error) {
	f, err := os.CreateTemp(tempDir, "repli_temp")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &temporaryRepliData{
		m:          new(sync.RWMutex),
		ctx:        ctx,
		f:          f,
		enc:        gob.NewEncoder(f),
		closeWrite: false,
	}, nil
}

func conn(url string, name string, reconnect nats.ConnHandler) (*nats.Conn, error) {
	return nats.Connect(
		url,
		nats.NoEcho(),
		nats.DontRandomize(),
		nats.Name(name),
		nats.ReconnectJitter(100*time.Millisecond, 300*time.Millisecond),
		nats.ReconnectWait(100*time.Millisecond),
		nats.MaxReconnects(-1),
		nats.PingInterval(10*time.Second),
		nats.ReconnectHandler(reconnect),
	)
}
