package btree

// import "container/list"

// // --- Page Management ---

// // PageID represents a unique identifier for a page on disk.
// type PageID uint64

// // Page represents an in-memory copy of a disk page.
// type Page struct {
// 	id       PageID
// 	data     []byte
// 	pinCount uint32
// 	isDirty  bool
// 	lsn      LSN // LSN of the last log record that modified this page
// 	// For LRU
// 	lruElement *list.Element // Pointer to the element in LRU list
// }

// // NewPage creates a new Page instance.
// func NewPage(id PageID, size int) *Page {
// 	return &Page{
// 		id:       id,
// 		data:     make([]byte, size),
// 		pinCount: 0,
// 		isDirty:  false,
// 		lsn:      InvalidLSN,
// 	}
// }

// func (p *Page) Reset() {
// 	p.id = InvalidPageID
// 	p.pinCount = 0
// 	p.isDirty = false
// 	p.lsn = InvalidLSN
// 	p.lruElement = nil
// 	// Zero out data to prevent old data from leaking, especially for security or integrity checks
// 	for i := range p.data {
// 		p.data[i] = 0
// 	}
// }

// func (p *Page) GetData() []byte   { return p.data }
// func (p *Page) GetPageID() PageID { return p.id }
// func (p *Page) IsDirty() bool     { return p.isDirty }
// func (p *Page) Pin()              { p.pinCount++ }
// func (p *Page) Unpin() {
// 	if p.pinCount > 0 {
// 		p.pinCount--
// 	}
// }
// func (p *Page) GetPinCount() uint32 { return p.pinCount }
// func (p *Page) SetDirty(dirty bool) { p.isDirty = dirty }
// func (p *Page) GetLSN() LSN         { return p.lsn }
// func (p *Page) SetLSN(lsn LSN)      { p.lsn = lsn }
