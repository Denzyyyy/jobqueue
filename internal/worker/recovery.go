package worker

import (
	"context"
	"log"
	"time"

	"github.com/Denzyyyy/jobqueue/internal/storage"
)

// LeaseRecovery periodically reclaims expired job leases
type LeaseRecovery struct {
	store    storage.Store
	interval time.Duration
	stopCh   chan struct{}
}

// NewLeaseRecovery creates a new lease recovery service
func NewLeaseRecovery(store storage.Store, interval time.Duration) *LeaseRecovery {
	if interval == 0 {
		interval = 10 * time.Second
	}
	
	return &LeaseRecovery{
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the recovery loop
func (r *LeaseRecovery) Start() {
	log.Printf("[recovery] Starting lease recovery service (check every %v)", r.interval)
	
	go r.recoveryLoop()
}

// Stop gracefully stops the recovery service
func (r *LeaseRecovery) Stop() {
	log.Println("[recovery] Stopping lease recovery service...")
	close(r.stopCh)
}

// recoveryLoop periodically checks for and reclaims expired leases
func (r *LeaseRecovery) recoveryLoop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			r.reclaimExpiredLeases()
			
		case <-r.stopCh:
			log.Println("[recovery] Recovery loop stopped")
			return
		}
	}
}

// reclaimExpiredLeases finds and reclaims jobs with expired leases
func (r *LeaseRecovery) reclaimExpiredLeases() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	reclaimed, err := r.store.ReclaimExpiredLeases(ctx)
	if err != nil {
		log.Printf("[recovery] Error reclaiming expired leases: %v", err)
		return
	}
	
	if reclaimed > 0 {
		log.Printf("[recovery] 🔄 Reclaimed %d expired job(s)", reclaimed)
	}
}