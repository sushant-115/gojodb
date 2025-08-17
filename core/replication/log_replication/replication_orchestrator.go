package logreplication

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"sync"

	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/replication/events"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

type ReplicationOrchestrator struct {
	ReplMgrs      map[indexing.IndexType]ReplicationManagerInterface
	EventReceiver *events.EventReceiver

	mu       sync.Mutex
	stopChan chan struct{}
}

func NewReplicationOrchestrator(addr string, replMgrs map[indexing.IndexType]ReplicationManagerInterface, serverCert *tls.Config) *ReplicationOrchestrator {
	cfg := events.ReceiverConfig{
		Addr:    addr,
		URLPath: "/events",
		TLS:     serverCert,
	}
	eventsReceiver, err := events.NewEventReceiver(cfg, nil, events.ReceiverHooks{
		OnAccepted: func() {
			fmt.Println("Received a new connection")
		},
		OnDropped: func(reason string) {
			fmt.Println("Dropped the packets due to ", reason)
		},
		OnStreamStart: func(remote string) {
			fmt.Println("Started the stream from: ", remote)
		},
		OnStreamClose: func(remote string, err error) {
			fmt.Println("Stream closed from: ", remote, " Due to: ", err)
		},
		OnError: func(stage string, err error) {
			fmt.Println("Error occured: ", stage, err)
		},
	})
	if err != nil {

	}
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
	r.EventReceiver.Close(context.Background())
	close(r.stopChan)
}

func (r *ReplicationOrchestrator) handleInboundStream() {
	log.Println("Orch Handle inbound stream")
	eventsCh := r.EventReceiver.Events()
	for {
		select {
		case event, ok := <-eventsCh:
			if !ok {
				// Channel is closed, so we can exit.
				log.Println("Event channel closed, stopping inbound stream handler.")
				return
			}

			log.Println("Received event: ", len(event))
			lr, err := wal.DecodeLogRecord(event)
			if err != nil || lr == nil {
				log.Println("Error: Couldn't decode the log record: ", string(event))
				continue
			}

			r.mu.Lock()
			replMgr, ok := r.ReplMgrs[lr.IndexType]
			r.mu.Unlock() // Unlock immediately after accessing the map

			if !ok {
				log.Println("Error: found unknown/invalid index type in the replication event: ", lr.IndexType)
				continue
			}

			if err := replMgr.ApplyLogRecord(*lr); err != nil {
				log.Println("Error: Couldn't apply the log record: ", lr)
			}
		case <-r.stopChan:
			log.Println("Received stop signal, stopping inbound stream handler.")
			return
		}
	}
}
