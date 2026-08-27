// Package native computes an endpoint's answer from our own implementation.
//
// It exists so the shadow comparator and the request path cannot disagree
// about what "our answer" is. They previously would have had to each carry
// their own dispatch and error mapping, and that pattern has already caused a
// bug in this project: the diff-log replay kept a private copy of the
// comparison rules and drifted from the live comparator twice.
//
// A canary makes the distinction matter much more than it did in shadow mode.
// Shadow answers are only ever compared; canary answers are served to remote
// servers.
package native

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

// Resolver computes native answers.
//
// State is the odd one out: it writes its answer rather than returning it,
// because a /state response can reach 97MB here and returning one would defeat
// the point of implementing the endpoint at all. Callers that only want to
// compare pass io.Discard and use the digest.
type Resolver interface {
	StateIDs(ctx context.Context, origin, roomID, eventID string) (*matrixstate.StateIDsResponse, error)
	Event(ctx context.Context, origin, serverName, eventID string) (*matrixstate.TransactionResponse, error)
	GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*matrixstate.MissingEventsResponse, error)
	State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error)
}

// Request identifies what to answer.
type Request struct {
	Endpoint string
	Origin   string
	RoomID   string
	EventID  string
	// Body is the raw request body, for endpoints whose answer depends on it.
	// Carried as bytes rather than parsed so the shadow comparator and the
	// request path parse it identically.
	Body []byte
}

// MissingEventsRequest is the POST body of /get_missing_events.
//
// Synapse defaults limit to 10 and the lists to empty, and caps the limit at
// 20 inside the handler. A body that will not parse is a client error, not an
// internal one.
type MissingEventsRequest struct {
	EarliestEvents []string `json:"earliest_events"`
	LatestEvents   []string `json:"latest_events"`
	Limit          *int     `json:"limit"`
}

// Answer computes one response.
//
// The returned status is what Synapse would return, including its error
// statuses; err is reserved for failures that mean we could not produce an
// answer at all, which on the request path is what triggers falling back to
// the proxy.
func Answer(ctx context.Context, r Resolver, serverName string, request Request) (body []byte, status int, err error) {
	switch request.Endpoint {
	case "state_ids":
		resp, err := r.StateIDs(ctx, request.Origin, request.RoomID, request.EventID)
		if err != nil {
			return matrixError(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, err
		}
		return body, http.StatusOK, nil

	case "get_missing_events":
		var body MissingEventsRequest
		if len(request.Body) > 0 {
			if err := json.Unmarshal(request.Body, &body); err != nil {
				return matrixError(matrixstate.ErrBadJSON())
			}
		}
		limit := 10
		if body.Limit != nil {
			limit = *body.Limit
		}
		resp, err := r.GetMissingEvents(ctx, request.Origin, serverName, request.RoomID,
			body.EarliestEvents, body.LatestEvents, limit)
		if err != nil {
			return matrixError(err)
		}
		out, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, err
		}
		return out, http.StatusOK, nil

	case "event":
		resp, err := r.Event(ctx, request.Origin, serverName, request.EventID)
		if errors.Is(err, matrixstate.ErrEventNotFound) {
			// Synapse answers an unknown event with 404 and an empty body,
			// not a JSON error.
			return nil, http.StatusNotFound, nil
		}
		if err != nil {
			return matrixError(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, err
		}
		return body, http.StatusOK, nil
	}
	return nil, 0, fmt.Errorf("native: no implementation for %q", request.Endpoint)
}

// matrixError turns a Matrix error into the body and status it is served with,
// leaving anything else as an internal failure.
func matrixError(err error) ([]byte, int, error) {
	var me *matrixstate.MatrixError
	if errors.As(err, &me) {
		body, mErr := json.Marshal(me)
		if mErr != nil {
			return nil, 0, mErr
		}
		return body, me.Status, nil
	}
	return nil, 0, err
}
