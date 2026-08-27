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
	State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error)
}

// Request identifies what to answer.
type Request struct {
	Endpoint string
	Origin   string
	RoomID   string
	EventID  string
}

// Answer computes one response.
//
// The returned status is what Synapse would return, including its error
// statuses; err is reserved for failures that mean we could not produce an
// answer at all, which on the request path is what triggers falling back to
// the proxy.
func Answer(ctx context.Context, r Resolver, serverName string, req Request) (body []byte, status int, err error) {
	switch req.Endpoint {
	case "state_ids":
		resp, err := r.StateIDs(ctx, req.Origin, req.RoomID, req.EventID)
		if err != nil {
			return matrixError(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, err
		}
		return body, http.StatusOK, nil

	case "event":
		resp, err := r.Event(ctx, req.Origin, serverName, req.EventID)
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
	return nil, 0, fmt.Errorf("native: no implementation for %q", req.Endpoint)
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
