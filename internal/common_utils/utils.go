package commonutils

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
)

func CopyToSyncMap[K comparable, V any](src map[K]V, dst *sync.Map) {
	for k, v := range src {
		dst.Store(k, v)
	}
}

func GoID() int64 {
	// A small buffer is enough for the first line of runtime.Stack
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	// The first line looks like: "goroutine 123 [running]:\n"
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		return -1
	}
	n, err := strconv.ParseInt(string(b[:i]), 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func PrintCaller(msg string, pageID uint64, skip int) {
	// skip=0 -> this function
	// skip=1 -> caller of this function
	// skip=2 -> caller's caller, and so on
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		fmt.Println("No caller info")
		return
	}

	fn := runtime.FuncForPC(pc)
	name := "unknown"
	if fn != nil {
		name = fn.Name()
	}

	fmt.Printf("%s called from %s:%d (%s) for pageID: %d, GID: %d \n", msg, filepath.Base(file), line, name, pageID, GoID())
}
