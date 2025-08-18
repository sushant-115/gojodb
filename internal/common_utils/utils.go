package commonutils

import "sync"

func CopyToSyncMap[K comparable, V any](src map[K]V, dst *sync.Map) {
	for k, v := range src {
		dst.Store(k, v)
	}
}
