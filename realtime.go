package pocketbase

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/SierraSoftworks/multicast/v2"
	"github.com/donovanhide/eventsource"
)

type (
	Event struct {
		Action string          `json:"action"`
		Record *map[string]any `json:"record"`
		Error  error           `json:"error"`
	}

	RealtimeConnectionManager struct {
		client            *Client
		once              sync.Once
		stream            *multicast.Channel[Event]
		counter           atomic.Int64
		targets           sync.Map
		mergedTargets     []string
		mergedTargetsLock sync.RWMutex
		error             error
		clientID          string
		debug             bool
		typeFactories     sync.Map
	}

	InitEvent[T any] struct {
		ClientID string `json:"clientId"`
	}

	SubscriptionsSet struct {
		ClientID      string   `json:"clientId"`
		Subscriptions []string `json:"subscriptions"`
	}

	EventStream struct {
		C           chan *Event
		Unsubscribe func()
	}
)

func NewRealtimeConnectionManager(client *Client) *RealtimeConnectionManager {
	return &RealtimeConnectionManager{
		client:        client,
		stream:        multicast.New[Event](),
		counter:       atomic.Int64{},
		targets:       sync.Map{},
		typeFactories: sync.Map{},
	}
}

func (r *RealtimeConnectionManager) init() {
	r.targets.Clear()
	r.connectToRealtime()
}

func (r *RealtimeConnectionManager) Subscribe(collectionName string, targets ...string) (*EventStream, error) {
	r.once.Do(r.init)

	if len(targets) == 0 {
		targets = []string{collectionName}
	}
	for i, t := range targets {
		if t == "*" {
			targets[i] = collectionName
		}
	}

	subscriptionID, err := r.addTarget(collectionName, targets...)
	if err != nil {
		return nil, err
	}

	closed := make(chan bool)
	c := make(chan *Event)
	stream := &EventStream{
		C: c,
		Unsubscribe: func() {
			r.removeTarget(subscriptionID)
			r.dbg("sending closed signal")
			closed <- true
			for {
				_, ok := <-c
				if !ok {
					r.dbg("real channel is closed from unsubscribe")
					return
				}
			}
		},
	}

	go func() {
		defer func() {
			r.dbg("closing channel")
			close(stream.C)
		}()
		l := r.stream.Listen()
		for {
			select {
			case <-closed:
				r.dbg("channel is closed from unsubscribe")
				return
			case e, ok := <-l.C:
				if !ok {
					panic("evergreen channel is closed")
				}
				r.dbg(fmt.Sprintf("received event: %+v", e))
				if isRecordInTargetList(e.Record, targets) {
					r.dbg("sending event to stream")
					stream.C <- &e
				}
			}
		}
	}()

	return stream, nil
}

func (r *RealtimeConnectionManager) recalcMergedTargets() {
	r.mergedTargetsLock.Lock()
	defer r.mergedTargetsLock.Unlock()

	mergedTargets := []string{}
	r.targets.Range(func(key, value any) bool {
		mergedTargets = append(mergedTargets, value.([]string)...)
		return true
	})
	r.dbg(fmt.Sprintf("recalculated mergedTargets: %+v\n", mergedTargets))
	r.mergedTargets = mergedTargets
}

func (r *RealtimeConnectionManager) addTarget(collectionName string, targets ...string) (string, error) {
	r.dbg(fmt.Sprintf("adding targets to %s: %+v", collectionName, targets))

	subscriptionID := fmt.Sprintf("%s_%d", collectionName, r.counter.Add(1))
	r.targets.Store(subscriptionID, targets)

	r.recalcMergedTargets()

	if err := r.updateRealtimeSubscription(); err != nil {
		return "", err
	}

	return subscriptionID, nil
}

func (r *RealtimeConnectionManager) Close() error {
	return nil
}

func (r *RealtimeConnectionManager) updateRealtimeSubscription() error {
	r.mergedTargetsLock.RLock()
	r.dbg(fmt.Sprintf("updating realtime subscription: %+v", r.mergedTargets))

	s := SubscriptionsSet{
		ClientID:      r.clientID,
		Subscriptions: r.mergedTargets,
	}
	r.mergedTargetsLock.RUnlock()
	resp, subscribeErr := r.client.Request().SetBody(s).Post(r.client.BaseUrl() + "/api/realtime")
	if subscribeErr != nil {
		return subscribeErr
	}
	if code := resp.StatusCode(); code != http.StatusNoContent {
		return fmt.Errorf("subscribe stream failed. resp status code is %v", code)
	}

	return nil
}

func (r *RealtimeConnectionManager) removeTarget(subscriptionID string) error {
	r.dbg(fmt.Sprintf("removing target: %s", subscriptionID))
	r.targets.Delete(subscriptionID)
	r.recalcMergedTargets()
	return r.updateRealtimeSubscription()
}

func (r *RealtimeConnectionManager) dbg(msg string) error {
	if r.debug {
		log.Printf("dbg: %s", msg)
	}
	return nil
}

func (r *RealtimeConnectionManager) connectToRealtime() error {
	r.dbg("connectToRealtime")
	ctx, cancel := context.WithCancel(context.Background())

	r.dbg("sending request")
	req := r.client.Request(WithContext(ctx), WithDoNotParseResponse(true))
	resp, respErr := req.Get(r.client.BaseUrl() + "/api/realtime")
	if respErr != nil {
		r.error = respErr
		cancel()
		return r.error
	}

	r.dbg("decoding response")
	d := eventsource.NewDecoder(resp.RawBody())
	ev, decodeErr := d.Decode()
	if decodeErr != nil {
		r.dbg(fmt.Sprintf("decodeErr: %v", decodeErr))
		r.error = decodeErr
		cancel()
		return r.error
	}
	r.dbg("checking event")
	if event := ev.Event(); event != "PB_CONNECT" {
		r.error = fmt.Errorf("first event must be PB_CONNECT, but got %s", event)
		r.dbg(fmt.Sprintf("r.error: %v", r.error))
		cancel()
		return r.error
	}

	r.dbg("unmarshalling init event")
	var initEvent InitEvent[map[string]any]
	if err := json.Unmarshal([]byte(ev.Data()), &initEvent); err != nil {
		r.error = fmt.Errorf("failed to unmarshal init event: %w", err)
		r.dbg(fmt.Sprintf("r.error: %v", r.error))
		cancel()
		return r.error
	}

	r.dbg("setting clientID to " + initEvent.ClientID)
	r.clientID = initEvent.ClientID

	go func() {
		for {
			ev, err := d.Decode()
			if err != nil {
				r.dbg(fmt.Sprintf("Error decoding event: %v", err))
				cancel()
				r.mergedTargetsLock.RLock()
				hasTargets := len(r.mergedTargets) > 0
				r.mergedTargetsLock.RUnlock()
				if hasTargets {
					r.dbg("there are still subscriptions listening,reconnecting")
					r.connectToRealtime()
					r.updateRealtimeSubscription()
				} else {
					r.Close()
				}
				return
			}
			r.handleSSEEvent(ev)
		}
	}()

	return nil
}

func (r *RealtimeConnectionManager) handleSSEEvent(ev eventsource.Event) {
	var e Event
	r.dbg(fmt.Sprintf("SSE event: %+v", ev))
	e.Error = json.Unmarshal([]byte(ev.Data()), &e)
	r.stream.C <- e
}

func isRecordInTargetList(record *map[string]any, targets []string) bool {
	log.Printf("checking record: %+v for targets: %+v\n", record, targets)
	collectionName, ok := (*record)["collectionName"].(string)
	if !ok {
		return false
	}
	collectionNameMatch := slices.Contains(targets, collectionName)

	collectionID, ok := (*record)["collectionId"].(string)
	if !ok {
		return false
	}
	collectionIDMatch := slices.Contains(targets, collectionID)

	recordID, ok := (*record)["id"].(string)
	if !ok {
		return false
	}
	recordIDMatch := slices.Contains(targets, collectionName+"/"+recordID) || slices.Contains(targets, collectionID+"/"+recordID)

	return collectionNameMatch || collectionIDMatch || recordIDMatch
}
