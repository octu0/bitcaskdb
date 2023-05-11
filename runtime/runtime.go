package runtime

import (
	goruntime "runtime"
)

func SetFinalizer(obj interface{}, finalizer interface{}) {
	goruntime.SetFinalizer(obj, finalizer)
}
