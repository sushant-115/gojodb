package pagemanager

import (
	"container/list" // For LRU
	"sync"           // For sync.RWMutex
	"time"

	commonutils "github.com/sushant-115/gojodb/internal/common_utils"
)

// --- Page Management ---

const (
	InvalidPageID PageID = 0 // Also used for header, but generally indicates invalid/unallocated
)

type LSN uint64 // Log Sequence Number
const InvalidLSN LSN = 0

// PageID represents a unique identifier for a page on disk.
type PageID uint64

// Page represents an in-memory copy of a disk page.
type Page struct {
	id       PageID
	data     []byte
	pinCount uint32
	isDirty  bool
	lsn      LSN // LSN of the last log record that modified this page
	// For LRU
	lruElement *list.Element // Pointer to the element in LRU list

	// --- NEW: Page Latch ---
	// This mutex protects the in-memory contents of this specific page.
	// It's a lightweight lock for physical concurrency control.
	latch sync.RWMutex
	// --- END NEW ---
	updatedAt time.Time
}

// NewPage creates a new Page instance.
func NewPage(id PageID, size int) *Page {
	return &Page{
		id:       id,
		data:     make([]byte, size),
		pinCount: 0,
		isDirty:  false,
		lsn:      InvalidLSN,
	}
}

func (p *Page) Reset() {
	p.id = InvalidPageID
	p.pinCount = 0
	p.isDirty = false
	p.lsn = InvalidLSN
	p.lruElement = nil
	// Zero out data to prevent old data from leaking, especially for security or integrity checks
	for i := range p.data {
		p.data[i] = 0
	}
}
func (p *Page) GetLruElement() *list.Element     { return p.lruElement }
func (p *Page) SetLruElement(elem *list.Element) { p.lruElement = elem }
func (p *Page) GetData() []byte                  { return p.data }
func (p *Page) SetData(newData []byte) bool      { copy(p.data, newData); return true }
func (p *Page) GetPageID() PageID                { return p.id }
func (p *Page) SetPageID(id PageID)              { p.id = id }
func (p *Page) IsDirty() bool                    { return p.isDirty }
func (p *Page) Pin()                             { p.pinCount++ }
func (p *Page) Unpin() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}
func (p *Page) GetPinCount() uint32         { return p.pinCount }
func (p *Page) SetPinCount(pinCount uint32) { p.pinCount = pinCount }
func (p *Page) SetDirty(dirty bool)         { p.isDirty = dirty }
func (p *Page) GetLSN() LSN                 { return p.lsn }
func (p *Page) SetLSN(lsn LSN)              { p.lsn = lsn }
func (p *PageID) GetID() uint64 {
	return uint64(*p)
}
func (p *Page) UpdatedAt(t time.Time)   { p.updatedAt = t }
func (p *Page) GetUpdatedAt() time.Time { return p.updatedAt }

// --- NEW: Latch Methods ---

// RLock acquires a read (shared) latch on the page.
func (p *Page) RLock() {
	p.latch.RLock()
}

// RUnlock releases a read (shared) latch on the page.
func (p *Page) RUnlock() {
	p.latch.RUnlock()
}

// Lock acquires a write (exclusive) latch on the page.
func (p *Page) Lock() {
	commonutils.PrintCaller("Page lock from", uint64(p.id), 2)
	p.latch.Lock()
}

func (p *Page) TryLock() bool {
	return p.latch.TryLock()
}

// Unlock releases a write (exclusive) latch on the page.
func (p *Page) Unlock() {
	commonutils.PrintCaller("Page unlock from", uint64(p.id), 2)
	p.latch.Unlock()
}

// --- END NEW ---
type PageManager struct {
}
