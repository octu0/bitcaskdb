
<a name="v0.3.6"></a>
## [v0.3.6](https://github.com/prologic/bitcask/compare/v0.3.5...v0.3.6) (2020-11-17)

### Bug Fixes

* Fix typo in labeler ([#172](https://github.com/prologic/bitcask/issues/172))
* Fix builds configuration for goreleaser
* Fix (again) goreleaser config
* Fix goreleaser config and improve release notes / changelog
* Fix recoverDatafile error covering ([#162](https://github.com/prologic/bitcask/issues/162))
* Fix loadIndex to be deterministic ([#115](https://github.com/prologic/bitcask/issues/115))

### Features

* Add configuration options for FileMode ([#183](https://github.com/prologic/bitcask/issues/183))
* Add imports and log in example code ([#182](https://github.com/prologic/bitcask/issues/182))
* Add empty changelog
* Add DependaBot config
* Add DeleteAll function ([#116](https://github.com/prologic/bitcask/issues/116))

### Updates

* Update README.md
* Update CHANGELOG for v0.3.6
* Update CHANGELOG for v0.3.6
* Update deps ([#140](https://github.com/prologic/bitcask/issues/140))
* Update README.md


<a name="v0.3.5"></a>
## [v0.3.5](https://github.com/prologic/bitcask/compare/v0.3.4...v0.3.5) (2019-10-20)

### Bug Fixes

* Fix setup target in Makefile to install mockery correctly
* Fix glfmt/golint issues
* Fix spelling mistake in README s/Sponser/Sponsor

### Features

* Add *.db to ignore future accidental commits of a bitcask db to the repo
* Add unit test for opening bad database with corrupted/invalid datafiles ([#105](https://github.com/prologic/bitcask/issues/105))

### Updates

* Update Drone CI test pipeline
* Update README.md
* Update to Go 1.13 and update README with new benchmarks ([#89](https://github.com/prologic/bitcask/issues/89))
* Update README.md


<a name="v0.3.4"></a>
## [v0.3.4](https://github.com/prologic/bitcask/compare/v0.3.3...v0.3.4) (2019-09-02)


<a name="v0.3.3"></a>
## [v0.3.3](https://github.com/prologic/bitcask/compare/v0.3.2...v0.3.3) (2019-09-02)

### Bug Fixes

* Fix a bug wit the decoder passing the wrong value for the value's offset into the buffer ([#77](https://github.com/prologic/bitcask/issues/77))
* Fix typo ([#65](https://github.com/prologic/bitcask/issues/65))
* Fix and cleanup some unnecessary internal sub-packages and duplication

### Updates

* Update README.md
* Update README.md
* Update README.md
* Update README.md


<a name="v0.3.2"></a>
## [v0.3.2](https://github.com/prologic/bitcask/compare/v0.3.1...v0.3.2) (2019-08-08)

### Updates

* Update README.md
* Update README.md
* Update CONTRIBUTING.md


<a name="v0.3.1"></a>
## [v0.3.1](https://github.com/prologic/bitcask/compare/v0.3.0...v0.3.1) (2019-08-05)

### Updates

* Update README.md
* Update README.md
* Update README.md


<a name="v0.3.0"></a>
## [v0.3.0](https://github.com/prologic/bitcask/compare/v0.2.2...v0.3.0) (2019-07-29)

### Updates

* Update README.md
* Update README.md


<a name="v0.2.2"></a>
## [v0.2.2](https://github.com/prologic/bitcask/compare/v0.2.1...v0.2.2) (2019-07-27)


<a name="v0.2.1"></a>
## [v0.2.1](https://github.com/prologic/bitcask/compare/v0.2.0...v0.2.1) (2019-07-25)


<a name="v0.2.0"></a>
## [v0.2.0](https://github.com/prologic/bitcask/compare/v0.1.7...v0.2.0) (2019-07-25)

### Bug Fixes

* Fix issue(db file Merge issue in windows env): ([#15](https://github.com/prologic/bitcask/issues/15))


<a name="v0.1.7"></a>
## [v0.1.7](https://github.com/prologic/bitcask/compare/v0.1.6...v0.1.7) (2019-07-19)

### Bug Fixes

* Fix mismatched key casing. ([#12](https://github.com/prologic/bitcask/issues/12))
* Fix outdated README ([#11](https://github.com/prologic/bitcask/issues/11))
* Fix typos in bitcask.go docs ([#10](https://github.com/prologic/bitcask/issues/10))

### Updates

* Update generated protobuf code
* Update README.md


<a name="v0.1.6"></a>
## [v0.1.6](https://github.com/prologic/bitcask/compare/v0.1.5...v0.1.6) (2019-04-01)

### Features

* Add Development section to README documenting use of Protobuf and tooling required. [#6](https://github.com/prologic/bitcask/issues/6)
* Add other badges from img.shields.io


<a name="v0.1.5"></a>
## [v0.1.5](https://github.com/prologic/bitcask/compare/v0.1.4...v0.1.5) (2019-03-30)

### Documentation

* Document using the Docker Image

### Features

* Add Dockerfile to publish images to Docker Hub

### Updates

* Update README.md


<a name="v0.1.4"></a>
## [v0.1.4](https://github.com/prologic/bitcask/compare/v0.1.3...v0.1.4) (2019-03-23)


<a name="v0.1.3"></a>
## [v0.1.3](https://github.com/prologic/bitcask/compare/v0.1.2...v0.1.3) (2019-03-23)


<a name="v0.1.2"></a>
## [v0.1.2](https://github.com/prologic/bitcask/compare/v0.1.1...v0.1.2) (2019-03-22)


<a name="v0.1.1"></a>
## [v0.1.1](https://github.com/prologic/bitcask/compare/v0.1.0...v0.1.1) (2019-03-22)


<a name="v0.1.0"></a>
## [v0.1.0](https://github.com/prologic/bitcask/compare/0.0.26...v0.1.0) (2019-03-22)


<a name="0.0.26"></a>
## [0.0.26](https://github.com/prologic/bitcask/compare/0.0.25...0.0.26) (2019-03-21)

### Features

* Add docs for bitcask
* Add docs for options
* Add KeYS command to server (bitraftd)
* Add Len() to exported API (extended API)
* Add Keys() to exported API (extended API)
* Add EXISTS command to server (bitraftd)


<a name="0.0.25"></a>
## [0.0.25](https://github.com/prologic/bitcask/compare/0.0.24...0.0.25) (2019-03-21)

### Features

* Add Has() to exported API (extended API)
* Add MergeOpen test case

### Updates

* Update README.md
* Update README.md


<a name="0.0.24"></a>
## [0.0.24](https://github.com/prologic/bitcask/compare/0.0.23...0.0.24) (2019-03-20)


<a name="0.0.23"></a>
## [0.0.23](https://github.com/prologic/bitcask/compare/0.0.22...0.0.23) (2019-03-20)

### Features

* Add bitcaskd to install target


<a name="0.0.22"></a>
## [0.0.22](https://github.com/prologic/bitcask/compare/0.0.21...0.0.22) (2019-03-18)


<a name="0.0.21"></a>
## [0.0.21](https://github.com/prologic/bitcask/compare/0.0.20...0.0.21) (2019-03-18)


<a name="0.0.20"></a>
## [0.0.20](https://github.com/prologic/bitcask/compare/0.0.19...0.0.20) (2019-03-17)


<a name="0.0.19"></a>
## [0.0.19](https://github.com/prologic/bitcask/compare/0.0.18...0.0.19) (2019-03-17)


<a name="0.0.18"></a>
## [0.0.18](https://github.com/prologic/bitcask/compare/0.0.17...0.0.18) (2019-03-16)


<a name="0.0.17"></a>
## [0.0.17](https://github.com/prologic/bitcask/compare/0.0.16...0.0.17) (2019-03-16)

### Features

* Add CRC Checksum checks on reading values back


<a name="0.0.16"></a>
## [0.0.16](https://github.com/prologic/bitcask/compare/0.0.15...0.0.16) (2019-03-16)


<a name="0.0.15"></a>
## [0.0.15](https://github.com/prologic/bitcask/compare/0.0.14...0.0.15) (2019-03-16)

### Bug Fixes

* Fix a race condition + Use my fork of trie


<a name="0.0.14"></a>
## [0.0.14](https://github.com/prologic/bitcask/compare/0.0.13...0.0.14) (2019-03-16)


<a name="0.0.13"></a>
## [0.0.13](https://github.com/prologic/bitcask/compare/0.0.12...0.0.13) (2019-03-16)

### Features

* Add prefix scan for keys using a Trie


<a name="0.0.12"></a>
## [0.0.12](https://github.com/prologic/bitcask/compare/0.0.11...0.0.12) (2019-03-14)


<a name="0.0.11"></a>
## [0.0.11](https://github.com/prologic/bitcask/compare/0.0.10...0.0.11) (2019-03-14)

### Updates

* Update README.md


<a name="0.0.10"></a>
## [0.0.10](https://github.com/prologic/bitcask/compare/0.0.9...0.0.10) (2019-03-14)

### Bug Fixes

* Fix concurrent read bug
* Fix concurrent write bug with multiple goroutines writing to the to the active datafile

### Updates

* Update README.md


<a name="0.0.9"></a>
## [0.0.9](https://github.com/prologic/bitcask/compare/0.0.8...0.0.9) (2019-03-14)


<a name="0.0.8"></a>
## [0.0.8](https://github.com/prologic/bitcask/compare/0.0.7...0.0.8) (2019-03-13)


<a name="0.0.7"></a>
## [0.0.7](https://github.com/prologic/bitcask/compare/0.0.6...0.0.7) (2019-03-13)


<a name="0.0.6"></a>
## [0.0.6](https://github.com/prologic/bitcask/compare/0.0.5...0.0.6) (2019-03-13)

### Bug Fixes

* Fix usage output of bitcaskd


<a name="0.0.5"></a>
## [0.0.5](https://github.com/prologic/bitcask/compare/0.0.4...0.0.5) (2019-03-13)

### Features

* Add a simple Redis compatible server daemon (bitcaskd)

### Updates

* Update README.md


<a name="0.0.4"></a>
## [0.0.4](https://github.com/prologic/bitcask/compare/0.0.3...0.0.4) (2019-03-13)

### Features

* Add flock on database Open()/Close() to prevent multiple concurrent processes write access. Fixes [#2](https://github.com/prologic/bitcask/issues/2)


<a name="0.0.3"></a>
## [0.0.3](https://github.com/prologic/bitcask/compare/0.0.2...0.0.3) (2019-03-13)


<a name="0.0.2"></a>
## [0.0.2](https://github.com/prologic/bitcask/compare/0.0.1...0.0.2) (2019-03-13)


<a name="0.0.1"></a>
## 0.0.1 (2019-03-13)

