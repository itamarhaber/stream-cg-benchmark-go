
## Overview

TBD


## Getting Started

### Installing
This benchmark go program is **know to be supported for go >= 1.11**. 
The easiest way to get and install the Subscriber Go program is to use `go get` and then `go install`:

```
go get github.com/itamarhaber/stream-cg-benchmark
cd $GOPATH/src/github.com/itamarhaber/stream-cg-benchmark
make
```

#### Updating 
To update the Subscriber Go program use `go get -u` to retrieve the latest version:.
```
go get -u github.com/itamarhaber/stream-cg-benchmark
cd $GOPATH/src/github.com/itamarhaber/stream-cg-benchmark
make
```

#### Limitations 

There are know limitations on old go version due to the radix/v3 dependency, given that on old versions, the go command in GOPATH mode does not distinguish between major versions, meaning that it will look for the package `package github.com/mediocregopher/radix/v3` instead of v3 of `package github.com/mediocregopher/radix`.
Therefore you should only use this tool on go >= 1.11. 

## Usage of stream-cg-benchmark

TBD