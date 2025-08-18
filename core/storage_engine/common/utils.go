package common

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"golang.org/x/time/rate"
)

// chunkSize: size of each read/write chunk
const chunkSize = 4 * 1024 * 1024 // 4 MiB

var bufPool = sync.Pool{
	New: func() interface{} { return make([]byte, chunkSize) },
}

func lowerPriority() error {
	// Increase niceness to 19 (lowest priority). PRIO_PROCESS, who = 0 (this process)
	const niceness = 19
	if err := syscall.Setpriority(syscall.PRIO_PROCESS, 0, niceness); err != nil {
		// On some systems Setpriority may require privileges to change in certain ways,
		// but increasing niceness (making nicer) is usually allowed.
		return fmt.Errorf("setpriority failed: %w", err)
	}
	return nil
}

func CopyThrottled(srcPath, dstPath string, rateBytesPerSec int64, verify bool) error {
	// set lower priority
	lowerPriority()
	// open files
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open src: %w", err)
	}
	defer src.Close()

	// create dest file with same permissions where possible
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open dst: %w", err)
	}
	defer func() {
		_ = dst.Sync()
		_ = dst.Close()
	}()

	// Set up throughput limiter using golang.org/x/time/rate
	var limiter *rate.Limiter
	if rateBytesPerSec > 0 {
		limiter = rate.NewLimiter(rate.Limit(rateBytesPerSec), int(chunkSize)) // burst = chunkSize
	}

	var (
		readOff  int64
		tmpSum   = sha256.New()
		tmpSumOK bool
	)

	for {
		buf := bufPool.Get().([]byte)
		n, rerr := src.ReadAt(buf[:chunkSize], readOff)
		if n > 0 {
			// throttle: wait until enough tokens available for n bytes
			if limiter != nil {
				if err := limiter.WaitN(context.TODO(), n); err != nil {
					bufPool.Put(buf)
					return fmt.Errorf("rate limiter error: %w", err)
				}
			}

			// write chunk
			w := 0
			for w < n {
				m, werr := dst.Write(buf[w:n])
				if werr != nil {
					bufPool.Put(buf)
					return fmt.Errorf("write error: %w", werr)
				}
				w += m
			}

			// optional verify: update rolling checksum for chunk (you can do per-chunk checksum instead)
			if verify {
				tmpSum.Write(buf[:n])
				tmpSumOK = true
			}

			readOff += int64(n)
			// release buffer
			bufPool.Put(buf)
		}

		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			// any other read error
			return fmt.Errorf("read error: %w", rerr)
		}
	}

	// flush to disk
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sync error: %w", err)
	}

	if verify && tmpSumOK {
		sum := tmpSum.Sum(nil)
		fmt.Printf("local copy checksum (sha256): %x\n", sum)
		// optionally return or compare with remote checksum if available
	}

	return nil
}
