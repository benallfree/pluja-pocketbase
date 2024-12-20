package pocketbase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
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
		client                        *Client
		once                          sync.Once
		stream                        *multicast.Channel[*Event]
		counter                       atomic.Int64
		targets                       sync.Map
		connectionRestartNeededSignal chan error
		targetsDirtySignal            chan bool
		realtimeConnectionReadySignal chan bool
		mergedTargets                 []string
		mergedTargetsLock             sync.RWMutex
		clientID                      string
		debug                         bool
		typeFactories                 sync.Map
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
		client:                        client,
		stream:                        multicast.New[*Event](),
		counter:                       atomic.Int64{},
		targets:                       sync.Map{},
		typeFactories:                 sync.Map{},
		targetsDirtySignal:            make(chan bool, 1),
		connectionRestartNeededSignal: make(chan error, 1),
		realtimeConnectionReadySignal: make(chan bool, 1),
	}
}

func (r *RealtimeConnectionManager) init() {
	go r.startContinuousRealtimeConnection()
}

type QueryOptions struct {
	Fields  string `json:"fields,omitempty"`
	Filter  string `json:"filter,omitempty"`
	Sort    string `json:"sort,omitempty"`
	Page    int    `json:"page,omitempty"`
	PerPage int    `json:"perPage,omitempty"`
}

type TargetOptions struct {
	Query QueryOptions `json:"query,omitempty"`
}

func WithFields(fields ...string) TargetOptionMaker {
	return func(o *TargetOptions) {
		// Required fields for realtime functionality
		allFields := append([]string{"id", "collectionName", "collectionId"}, fields...)

		// Sort and remove duplicates
		slices.Sort(allFields)
		allFields = slices.Compact(allFields)

		o.Query.Fields = strings.Join(allFields, ",")
	}
}

type TargetOptionMaker func(o *TargetOptions)

func WithTarget(name string, options ...TargetOptionMaker) string {
	opts := TargetOptions{}
	for _, o := range options {
		o(&opts)
	}

	// Marshal query options to JSON
	queryJSON, err := json.Marshal(opts)
	if err != nil {
		// In case of error, return unmodified name
		return name
	}

	// Return name with query options as URL parameter
	return fmt.Sprintf("%s?options=%s", name, url.QueryEscape(string(queryJSON)))
}

func (r *RealtimeConnectionManager) Subscribe(collectionName string, targets ...string) (*EventStream, error) {
	r.Dbg("Top of Subscribe")
	r.once.Do(r.init)
	if len(targets) == 0 {
		targets = []string{collectionName}
	}
	for i, t := range targets {
		if t == "*" {
			targets[i] = collectionName
		}
	}
	r.Dbg(fmt.Sprintf("subscribing to %s: %+v", collectionName, targets))

	subscriptionID, err := r.addTarget(collectionName, targets...)
	if err != nil {
		return nil, err
	}
	r.Dbg(fmt.Sprintf("subscriptionID: %s", subscriptionID))

	closed := make(chan bool)
	c := make(chan *Event)
	stream := &EventStream{
		C: c,
		Unsubscribe: func() {
			r.removeTarget(subscriptionID)
			r.Dbg("sending closed signal")
			closed <- true
			for {
				_, ok := <-c
				if !ok {
					r.Dbg("real channel is closed from unsubscribe")
					return
				}
			}
		},
	}

	go func() {
		defer func() {
			r.Dbg("closing channel")
			close(stream.C)
		}()
		l := r.stream.Listen()
		r.Dbg("listening for events")
		for {
			select {
			case <-closed:
				r.Dbg("channel is closed from unsubscribe")
				return
			case e, ok := <-l.C:
				if !ok {
					panic("evergreen channel is closed")
				}
				r.Dbg(fmt.Sprintf("received event: %+v", e))
				if r.isRecordInTargetList(e.Record, targets) {
					r.Dbg("sending event to stream")
					stream.C <- e
				}
			}
		}
	}()

	return stream, nil
}

func (r *RealtimeConnectionManager) recalcMergedTargets() {
	r.mergedTargetsLock.Lock()
	defer r.mergedTargetsLock.Unlock()

	// Store old targets for comparison
	oldTargets := make([]string, len(r.mergedTargets))
	copy(oldTargets, r.mergedTargets)

	// Calculate new targets
	mergedTargets := []string{}
	r.targets.Range(func(key, value any) bool {
		mergedTargets = append(mergedTargets, value.([]string)...)
		return true
	})
	slices.Sort(mergedTargets)
	r.Dbg(fmt.Sprintf("recalculated mergedTargets: %+v\n", mergedTargets))
	r.mergedTargets = mergedTargets

	// Compare and signal if different
	if !slices.Equal(oldTargets, mergedTargets) {
		r.Dbg("targets changed, sending targetsDirtySignal")
		select {
		case r.targetsDirtySignal <- true:
			r.Dbg("sent targets dirty signal")
		default:
			r.Dbg("targets dirty signal channel full, skipping")
		}
	}
}

func (r *RealtimeConnectionManager) addTarget(collectionName string, targets ...string) (string, error) {
	r.Dbg(fmt.Sprintf("adding targets to %s: %+v", collectionName, targets))

	subscriptionID := fmt.Sprintf("%s_%d", collectionName, r.counter.Add(1))
	r.targets.Store(subscriptionID, targets)

	r.recalcMergedTargets()

	return subscriptionID, nil
}

func (r *RealtimeConnectionManager) removeTarget(subscriptionID string) {
	r.Dbg(fmt.Sprintf("removing target: %s", subscriptionID))
	r.targets.Delete(subscriptionID)
	r.recalcMergedTargets()
}

func (r *RealtimeConnectionManager) updateRealtimeSubscription() error {
	r.once.Do(r.startContinuousRealtimeConnection)

	r.Dbg("updating subscriptions")
	r.mergedTargetsLock.RLock()
	defer r.mergedTargetsLock.RUnlock()
	r.Dbg(fmt.Sprintf("updating realtime subscription: %+v", r.mergedTargets))

	s := SubscriptionsSet{
		ClientID:      r.clientID,
		Subscriptions: r.mergedTargets,
	}
	resp, subscribeErr := r.client.Request().SetBody(s).Post(r.client.BaseUrl() + "/api/realtime")
	if subscribeErr != nil {
		return subscribeErr
	}
	if code := resp.StatusCode(); code != http.StatusNoContent {
		return fmt.Errorf("subscribe stream failed. resp status code is %v", code)
	}

	return nil
}

func (r *RealtimeConnectionManager) Dbg(msg string) error {
	if r.debug {
		slog.Debug("dbg", "message", msg)
	}
	return nil
}

func (r *RealtimeConnectionManager) startContinuousRealtimeConnection() {

	// Continue with reconnection loop
	for {
		r.Dbg("starting realtime connection")
		go r.connectToRealtime()
		select {
		case <-r.realtimeConnectionReadySignal:
			r.Dbg("realtime connection ready")
			r.updateRealtimeSubscription()
		eventLoop:
			for {
				r.Dbg("main event loop")
				select {
				case <-r.targetsDirtySignal:
					r.Dbg("targetsDirtySignal received, updating subscriptions")
					r.updateRealtimeSubscription()
				case err := <-r.connectionRestartNeededSignal:
					r.Dbg(fmt.Sprintf("connectionRestartNeededSignal received, restarting connection: %v", err))
					break eventLoop
				}
			}
		case err := <-r.connectionRestartNeededSignal:
			r.Dbg(fmt.Sprintf("connectionRestartNeededSignal received, restarting connection: %v", err))
		}

	}
	r.Dbg("exiting realtime connection loop")
}

func (r *RealtimeConnectionManager) connectToRealtime() {
	r.Dbg("connectToRealtime")
	ctx, cancel := context.WithCancel(context.Background())

	r.Dbg("sending request")
	req := r.client.Request(WithContext(ctx), WithDoNotParseResponse(true))
	resp, respErr := req.Get(r.client.BaseUrl() + "/api/realtime")
	if respErr != nil {
		r.connectionRestartNeededSignal <- fmt.Errorf("respErr: %v", respErr)
		cancel()
		return
	}

	r.Dbg("decoding response")
	d := eventsource.NewDecoder(resp.RawBody())
	ev, decodeErr := d.Decode()
	if decodeErr != nil {
		r.connectionRestartNeededSignal <- fmt.Errorf("decodeErr: %v", decodeErr)
		cancel()
		return
	}
	r.Dbg("checking event")
	if event := ev.Event(); event != "PB_CONNECT" {
		r.connectionRestartNeededSignal <- fmt.Errorf("first event must be PB_CONNECT, but got %s", event)
		cancel()
		return
	}

	r.Dbg("unmarshalling init event")
	var initEvent InitEvent[map[string]any]
	if err := json.Unmarshal([]byte(ev.Data()), &initEvent); err != nil {
		r.connectionRestartNeededSignal <- fmt.Errorf("failed to unmarshal init event: %w", err)
		cancel()
		return
	}

	r.Dbg("setting clientID to " + initEvent.ClientID)
	r.clientID = initEvent.ClientID
	r.realtimeConnectionReadySignal <- true
	for {
		r.Dbg("awaiting raw events in loop")
		ev, err := d.Decode()
		if err != nil {
			r.connectionRestartNeededSignal <- fmt.Errorf("error decoding event: %v", err)
			cancel()
			return
		}
		var e Event
		r.Dbg(fmt.Sprintf("SSE event: %+v", ev))
		e.Error = json.Unmarshal([]byte(ev.Data()), &e)
		r.stream.C <- &e
	}
}

func (r *RealtimeConnectionManager) isRecordInTargetList(record *map[string]any, targets []string) bool {
	// Strip query parameters from targets
	strippedTargets := make([]string, len(targets))
	for i, target := range targets {
		strippedTargets[i] = strings.Split(target, "?")[0]
	}
	targets = strippedTargets
	r.Dbg(fmt.Sprintf("checking record: %+v for targets: %+v\n", record, targets))
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
