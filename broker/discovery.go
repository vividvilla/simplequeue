package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vivek-ng/simplequeue/queue"
	"github.com/vivek-ng/simplequeue/store"
)

const brokerKey = "broker.json"

// registerBroker writes this broker's identity and heartbeat into broker.json.
// Uses CAS to avoid stomping another broker's registration.
func (b *Broker) registerBroker(ctx context.Context) error {
	state, etag, err := loadBrokerState(ctx, b.store)
	if err != nil {
		return fmt.Errorf("register broker: load: %w", err)
	}

	now := b.clock.Now()

	// If another broker is registered and its heartbeat is fresh, back off.
	if state.Broker != "" && state.Broker != b.cfg.BrokerID {
		if now.Sub(state.BrokerHeartbeat) < b.cfg.StaleJobTimeout {
			return fmt.Errorf("another broker %q is active (heartbeat %s ago)", state.Broker, now.Sub(state.BrokerHeartbeat))
		}
		b.log.Info("taking over from stale broker", "old_broker", state.Broker, "stale_since", state.BrokerHeartbeat)
	}

	state.Broker = b.cfg.BrokerID
	state.BrokerHeartbeat = now

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("register broker: marshal: %w", err)
	}
	_, err = b.store.PutIf(ctx, brokerKey, data, etag)
	if errors.Is(err, store.ErrPreconditionFail) {
		return fmt.Errorf("register broker: CAS conflict, will retry")
	}
	return err
}

// loadBrokerState reads broker.json and returns the BrokerState + ETag.
func loadBrokerState(ctx context.Context, s store.ObjectStore) (*queue.BrokerState, string, error) {
	obj, err := s.Get(ctx, brokerKey)
	if errors.Is(err, store.ErrNotFound) {
		return &queue.BrokerState{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	var state queue.BrokerState
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		return nil, "", fmt.Errorf("unmarshal broker state: %w", err)
	}
	return &state, obj.ETag, nil
}

// DiscoverBroker reads broker.json to find the active broker address.
// Returns the broker address or an error if no broker is registered or it's stale.
func DiscoverBroker(ctx context.Context, s store.ObjectStore, staleTimeout time.Duration) (string, error) {
	obj, err := s.Get(ctx, brokerKey)
	if err != nil {
		return "", fmt.Errorf("discover broker: %w", err)
	}

	var state queue.BrokerState
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		return "", fmt.Errorf("discover broker: unmarshal: %w", err)
	}

	if state.Broker == "" {
		return "", fmt.Errorf("no broker registered")
	}

	if time.Since(state.BrokerHeartbeat) > staleTimeout {
		return "", fmt.Errorf("broker %q heartbeat is stale (%s ago)", state.Broker, time.Since(state.BrokerHeartbeat))
	}

	return state.Broker, nil
}
