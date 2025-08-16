package logreplication

import (
	"log"
	"sync"

	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/replication/events"
	"github.com/sushant-115/gojodb/core/security/encryption/internaltls"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

type ReplicationOrchestrator struct {
	ReplMgrs      map[indexing.IndexType]ReplicationManagerInterface
	EventReceiver *events.EventReceiver

	mu       sync.Mutex
	stopChan chan struct{}
}

func NewReplicationOrchestrator(addr string, replMgrs map[indexing.IndexType]ReplicationManagerInterface) *ReplicationOrchestrator {
	eventsReceiver := events.NewEventReceiver(addr, 10*1024, internaltls.GetTestServerCert())
	return &ReplicationOrchestrator{
		ReplMgrs:      replMgrs,
		EventReceiver: eventsReceiver,
		mu:            sync.Mutex{},
		stopChan:      make(chan struct{}),
	}
}

func (r *ReplicationOrchestrator) Start() error {
	err := r.EventReceiver.Start()
	if err != nil {
		return err
	}
	go r.handleInboundStream()
	return nil
}

func (r *ReplicationOrchestrator) Stop() {
	r.EventReceiver.Close()
	close(r.stopChan)
}

func (r *ReplicationOrchestrator) handleInboundStream() {
	log.Println("Orch Handle inbound stream")
	eventsCh := r.EventReceiver.Events()
	for event := range eventsCh {
		log.Println("Received event: ", len(event))
		lr, err := wal.DecodeLogRecord(event)
		if err != nil || lr == nil {
			log.Println("Error: Couldn't decode the log record: ", string(event))
			continue
		}
		r.mu.Lock()
		replMgr, ok := r.ReplMgrs[lr.IndexType]
		if !ok {
			log.Println("Error: found unknown/invalid index type in the replication event: ", lr.IndexType)
			continue
		}
		r.mu.Unlock()
		if err := replMgr.ApplyLogRecord(*lr); err != nil {
			log.Println("Error: Couldn't apply the log record: ", lr)
		}
	}
}
