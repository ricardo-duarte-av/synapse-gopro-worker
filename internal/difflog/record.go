// Package difflog persists disagreements between the Synapse worker and our
// native implementation.
//
// Shadow mode runs for weeks before an endpoint is promoted, so this has to be
// durable across restarts and bounded on disk. Records are written as JSON
// Lines to a rotating file set, and cumulative counters are checkpointed to a
// separate stats file so a restart does not reset the evidence — unlike the
// Prometheus counters, which do.
package difflog

import (
	"encoding/json"
	"time"
)

// Kind classifies why a request was logged.
type Kind string

const (
	// KindStatus means the two implementations returned different HTTP statuses.
	KindStatus Kind = "status_mismatch"
	// KindBody means the statuses agreed but the response bodies did not.
	KindBody Kind = "body_mismatch"
	// KindNativeError means the native implementation failed outright.
	KindNativeError Kind = "native_error"
	// KindNativeTimeout means the native implementation exceeded its budget.
	KindNativeTimeout Kind = "native_timeout"
)

// Record is one logged disagreement, serialised as a single JSON line.
type Record struct {
	Time     time.Time `json:"time"`
	Kind     Kind      `json:"kind"`
	Endpoint string    `json:"endpoint"`

	// Origin is the remote server that made the request, and URI is the exact
	// request line. Together they are enough to replay the request.
	Origin string `json:"origin,omitempty"`
	URI    string `json:"uri"`

	// RoomID and EventID are decoded for grepping; the URI remains canonical.
	RoomID  string `json:"room_id,omitempty"`
	EventID string `json:"event_id,omitempty"`

	SynapseStatus int `json:"synapse_status"`
	NativeStatus  int `json:"native_status"`

	SynapseDurationMS float64 `json:"synapse_duration_ms"`
	NativeDurationMS  float64 `json:"native_duration_ms"`

	// Diff summarises the disagreement in terms of the response shape. It is
	// what makes the log usable without reading the full bodies.
	Diff *Diff `json:"diff,omitempty"`

	// NativeError carries the failure text for KindNativeError.
	NativeError string `json:"native_error,omitempty"`

	// Bodies are retained for replay and may be truncated; see BodyLimit.
	SynapseBody json.RawMessage `json:"synapse_body,omitempty"`
	NativeBody  json.RawMessage `json:"native_body,omitempty"`
	// BodiesTruncated reports that one or both bodies were cut to fit.
	BodiesTruncated bool `json:"bodies_truncated,omitempty"`
}

// Diff summarises how two responses disagree.
//
// All four federation response fields (pdu_ids, auth_chain_ids, pdus,
// auth_chain) are unordered collections of events, so the useful summary is
// which event IDs each side has that the other does not.
type Diff struct {
	// Fields holds one entry per response field that disagreed.
	Fields []FieldDiff `json:"fields"`
}

// FieldDiff describes the disagreement within a single response field.
type FieldDiff struct {
	// Field is the JSON key, e.g. "pdu_ids" or "auth_chain".
	Field string `json:"field"`

	// MissingFromNative lists event IDs Synapse returned that we did not.
	// These are the dangerous ones: we are withholding data a remote server
	// is entitled to.
	MissingFromNative []string `json:"missing_from_native,omitempty"`
	// ExtraInNative lists event IDs we returned that Synapse did not. These
	// are more dangerous still: we may be leaking data.
	ExtraInNative []string `json:"extra_in_native,omitempty"`

	// ContentMismatch lists event IDs present on both sides whose event
	// bodies differ.
	ContentMismatch []string `json:"content_mismatch,omitempty"`

	// Counts on each side, so a truncated list still conveys the scale.
	SynapseCount int `json:"synapse_count"`
	NativeCount  int `json:"native_count"`
	// ListsTruncated reports that the ID lists above were capped.
	ListsTruncated bool `json:"lists_truncated,omitempty"`
}

// Empty reports whether the diff found nothing, which should never be logged.
func (d *Diff) Empty() bool {
	if d == nil {
		return true
	}
	for _, f := range d.Fields {
		if len(f.MissingFromNative) > 0 || len(f.ExtraInNative) > 0 ||
			len(f.ContentMismatch) > 0 || f.SynapseCount != f.NativeCount {
			return false
		}
	}
	// Fields may be present but all clean, which is still no disagreement.
	return true
}
