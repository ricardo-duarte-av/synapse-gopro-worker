// Package replication consumes Synapse's cache-invalidation stream.
//
// Synapse's stream replication travels over Redis pub/sub on a channel named
// after the homeserver's server_name, and every worker both publishes and
// subscribes to it. (This is separate from the HTTP replication listener and
// instance_map, which only writer workers need — a federation reader is nobody's
// RPC target and has neither.) Consuming the stream is read-only and needs no
// credentials beyond reaching Redis.
//
// We use it for exactly one thing: knowing when it is no longer safe to serve
// something we cached. Synapse's data is immutable in the sense that a state
// group's contents never change, but rooms get purged and events get deleted,
// and a cache entry admitted beforehand then describes something gone from the
// database. That is not theoretical — it served a 403 where Synapse served 404.
//
// The invariant this package exists to enforce:
//
//	the caches are armed only while we are connected and listening.
//
// A subscriber that quietly misses a purge is worse than having no cache at
// all, because the staleness is invisible. So every connect, disconnect and
// parse failure disarms and empties the caches, and only a healthy subscription
// arms them again.
package replication

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Synapse's fake cache names for bulk invalidations. They are not real cached
// functions; they are how Synapse says "I deleted things here and I am not
// going to enumerate them".
const (
	// purgeHistoryCache is sent when events in a room have been purged.
	purgeHistoryCache = "ph_cache_fake"
	// deleteRoomCache is sent when a whole room has been deleted.
	deleteRoomCache = "dr_cache_fake"
	// currentStateCache is sent when a room's current state changed. We do not
	// cache current state, so it is ignored — named here so it is visibly a
	// decision rather than an oversight.
	currentStateCache = "cs_cache_fake"
)

// perEventCaches are Synapse cache functions whose key begins with an event ID
// we may be holding. Handling them keeps an ordinary invalidation from costing
// the whole cache.
var perEventCaches = map[string]int{
	"_get_state_group_for_event": 0, // keys: [event_id]
	"have_seen_event":            1, // keys: [room_id, event_id]
}

// Config selects the Redis carrying Synapse's replication stream.
type Config struct {
	// Enabled turns consumption on. When false the caches stay armed and
	// inherit Synapse's staleness, which is only acceptable knowingly.
	Enabled bool `yaml:"enabled"`
	// Address is a unix socket path or host:port.
	Address string `yaml:"address"`
	// Channel must equal Synapse's server_name. It is required rather than
	// derived because one Redis can carry several homeservers' streams, and
	// subscribing to the wrong one would silently never invalidate anything.
	Channel  string `yaml:"channel"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

func (c Config) validate() error {
	if c.Address == "" {
		return errors.New("replication.address is required when replication is enabled")
	}
	if c.Channel == "" {
		return errors.New("replication.channel is required when replication is enabled; it must match Synapse's server_name")
	}
	return nil
}

// Caches is the part of the store this package drives.
type Caches interface {
	// SetCachesArmed turns every cache on or off, emptying them when off.
	SetCachesArmed(bool)
	// PurgeCaches empties every cache, leaving them armed.
	PurgeCaches()
	// DropEvent removes one event from the event-keyed caches.
	DropEvent(eventID string)
}

// Subscriber keeps the caches consistent with Synapse's invalidations.
type Subscriber struct {
	cfg    Config
	caches Caches
	log    zerolog.Logger
	m      *metrics
}

// New builds a Subscriber. It does not connect; call Run.
func New(cfg Config, caches Caches, log zerolog.Logger) (*Subscriber, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Subscriber{
		cfg:    cfg,
		caches: caches,
		log:    log.With().Str("component", "replication").Logger(),
		m:      newMetrics(),
	}, nil
}

// Run consumes the stream until ctx is cancelled, reconnecting as needed.
//
// The caches start disarmed and are armed only once a subscription is
// confirmed, so a worker that cannot reach Redis serves correctly from the
// database rather than quickly from a cache it cannot invalidate.
func (s *Subscriber) Run(ctx context.Context) {
	s.disarm("startup")

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for ctx.Err() == nil {
		err := s.session(ctx)
		if ctx.Err() != nil {
			break
		}
		s.disarm("disconnected")
		s.log.Warn().Err(err).Dur("retry_in", backoff).
			Msg("replication stream lost; caches disarmed until it returns")

		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	s.disarm("shutdown")
}

// session holds one subscription for as long as it stays healthy.
func (s *Subscriber) session(ctx context.Context) error {
	opts := &redis.Options{
		Addr:     s.cfg.Address,
		Password: s.cfg.Password,
		DB:       s.cfg.DB,
	}
	if strings.HasPrefix(s.cfg.Address, "/") {
		opts.Network = "unix"
	}
	client := redis.NewClient(opts)
	defer client.Close()

	pubsub := client.Subscribe(ctx, s.cfg.Channel)
	defer pubsub.Close()

	// ChannelWithSubscriptions rather than Channel: go-redis reconnects and
	// resubscribes on its own, without the read loop ever returning. That is
	// convenient for ordinary clients and dangerous here — a purge published
	// while we were disconnected would be missed with nothing to show for it.
	// Surfacing the resubscribe lets every gap in the stream empty the caches.
	ch := pubsub.ChannelWithSubscriptions()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return errors.New("replication channel closed")
			}
			switch v := ev.(type) {
			case *redis.Subscription:
				if v.Kind != "subscribe" {
					continue
				}
				// Either the first subscribe or a silent reconnect. Both mean
				// we cannot account for what happened in between.
				s.arm()
				s.log.Info().
					Str("channel", s.cfg.Channel).
					Str("address", s.cfg.Address).
					Msg("subscribed to Synapse's replication stream; caches emptied and armed")
			case *redis.Message:
				s.handle(v.Payload)
			case *redis.Pong:
				// Health check; carries no data.
			}
		}
	}
}

func (s *Subscriber) arm() {
	s.caches.SetCachesArmed(false) // empties whatever survived
	s.caches.SetCachesArmed(true)
	s.m.connected.Set(1)
	s.m.flushes.WithLabelValues("connected").Inc()
}

func (s *Subscriber) disarm(reason string) {
	s.caches.SetCachesArmed(false)
	s.m.connected.Set(0)
	s.m.flushes.WithLabelValues(reason).Inc()
}

// handle acts on one replication command.
//
// Anything unrecognised is counted and ignored: the stream carries presence,
// receipts, typing and more, none of which can affect an immutable cache.
func (s *Subscriber) handle(payload string) {
	cmd, rest, _ := strings.Cut(payload, " ")
	if cmd != "RDATA" {
		s.m.commands.WithLabelValues(cmd).Inc()
		return
	}

	// RDATA <stream_name> <instance_name> <token> <row_json>
	parts := strings.SplitN(rest, " ", 4)
	if len(parts) != 4 {
		s.malformed("rdata_fields", payload)
		return
	}
	stream, row := parts[0], parts[3]
	s.m.commands.WithLabelValues("RDATA/" + stream).Inc()
	s.m.lastMessage.SetToCurrentTime()

	if stream != "caches" {
		return
	}
	s.handleCacheRow(row, payload)
}

// handleCacheRow applies one CachesStream row: [cache_func, keys, ts].
func (s *Subscriber) handleCacheRow(row, payload string) {
	var parsed []json.RawMessage
	if err := json.Unmarshal([]byte(row), &parsed); err != nil || len(parsed) < 2 {
		s.malformed("caches_row", payload)
		return
	}

	var cacheFunc string
	if err := json.Unmarshal(parsed[0], &cacheFunc); err != nil {
		s.malformed("caches_func", payload)
		return
	}

	// A null keys field means "invalidate everything for this function".
	var keys []json.RawMessage
	nullKeys := string(parsed[1]) == "null"
	if !nullKeys {
		if err := json.Unmarshal(parsed[1], &keys); err != nil {
			s.malformed("caches_keys", payload)
			return
		}
	}

	switch {
	case cacheFunc == purgeHistoryCache || cacheFunc == deleteRoomCache:
		// Events were deleted, and Synapse deliberately does not say which.
		// Our caches are keyed by state group, event ID and auth-chain hash,
		// none of which maps back to a room, so the whole cache goes.
		s.caches.PurgeCaches()
		s.m.invalidations.WithLabelValues(cacheFunc).Inc()
		s.m.flushes.WithLabelValues(cacheFunc).Inc()
		s.log.Info().Str("cache_func", cacheFunc).Str("keys", string(parsed[1])).
			Msg("Synapse deleted events; caches emptied")

	case cacheFunc == currentStateCache:
		// Current state is deliberately never cached here.
		s.m.invalidations.WithLabelValues("ignored").Inc()

	default:
		idx, ok := perEventCaches[cacheFunc]
		if !ok {
			s.m.invalidations.WithLabelValues("ignored").Inc()
			return
		}
		if nullKeys {
			// "Invalidate all" for a cache we mirror per event.
			s.caches.PurgeCaches()
			s.m.flushes.WithLabelValues(cacheFunc + "_all").Inc()
			return
		}
		if idx >= len(keys) {
			s.malformed("caches_key_index", payload)
			return
		}
		var eventID string
		if err := json.Unmarshal(keys[idx], &eventID); err != nil {
			s.malformed("caches_event_id", payload)
			return
		}
		s.caches.DropEvent(eventID)
		s.m.invalidations.WithLabelValues(cacheFunc).Inc()
	}
}

// malformed handles a row we cannot understand.
//
// This is the case that must not be shrugged off. A row we failed to parse may
// have been the purge that makes our cache wrong, so the safe reading of "I do
// not understand this" is "I have missed an invalidation".
func (s *Subscriber) malformed(reason, payload string) {
	s.caches.PurgeCaches()
	s.m.malformed.WithLabelValues(reason).Inc()
	s.m.flushes.WithLabelValues("malformed").Inc()
	s.log.Warn().Str("reason", reason).Str("payload", truncate(payload, 256)).
		Msg("could not parse a replication row; caches emptied rather than trusted")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
