package fedapi

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"

	"github.com/daedric/synapse-gopro-worker/internal/ratelimit"
)

// limitExceededBody is Synapse's 429 response.
//
// Synapse answers with M_LIMIT_EXCEEDED, the message "Too Many Requests", a
// retry_after_ms field, and a Retry-After header rounded up to whole seconds.
// Remote servers back off on these, so the shape matters as much as the status.
type limitExceededBody struct {
	ErrCode      string `json:"errcode"`
	Error        string `json:"error"`
	RetryAfterMS int    `json:"retry_after_ms"`
}

// writeLimitExceeded sends the 429 Synapse would send.
func writeLimitExceeded(w http.ResponseWriter, settings ratelimit.FederationSettings) {
	retryAfterMS := settings.RetryAfterMS()

	// Synapse rounds the header up to whole seconds.
	seconds := int(math.Ceil(float64(retryAfterMS) / 1000))
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)

	_ = json.NewEncoder(w).Encode(limitExceededBody{
		ErrCode:      "M_LIMIT_EXCEEDED",
		Error:        "Too Many Requests",
		RetryAfterMS: retryAfterMS,
	})
}
