package matrixstate

import (
	"fmt"
	"net/http"
)

// MatrixError is a Matrix API error with the status and body Synapse would
// return. The wording is copied from Synapse deliberately: while the native
// implementation is being compared against it, any difference in the response
// is a difference worth seeing.
type MatrixError struct {
	Status  int    `json:"-"`
	ErrCode string `json:"errcode"`
	Message string `json:"error"`
}

func (e *MatrixError) Error() string {
	return fmt.Sprintf("%s (%d): %s", e.ErrCode, e.Status, e.Message)
}

// Errors mirroring Synapse's federation responses.

func errHostNotInRoom() *MatrixError {
	return &MatrixError{Status: 403, ErrCode: "M_FORBIDDEN", Message: "Host not in room."}
}

func errPartialState() *MatrixError {
	return &MatrixError{
		Status:  403,
		ErrCode: "M_UNABLE_DUE_TO_PARTIAL_STATE",
		Message: "Unable to authorise you right now; room is partial-stated here.",
	}
}

func errServerBanned() *MatrixError {
	return &MatrixError{Status: 403, ErrCode: "M_FORBIDDEN", Message: "Server is banned from room"}
}

func errEventNotFound(eventID string) *MatrixError {
	return &MatrixError{
		Status:  404,
		ErrCode: "M_NOT_FOUND",
		Message: fmt.Sprintf("Could not find event %s", eventID),
	}
}

func errEventNotInRoom(eventID, roomID string) *MatrixError {
	return &MatrixError{
		Status:  404,
		ErrCode: "M_NOT_FOUND",
		Message: fmt.Sprintf("Could not find event %s in room %s", eventID, roomID),
	}
}

func errStateNotKnown(eventID string) *MatrixError {
	return &MatrixError{
		Status:  404,
		ErrCode: "M_NOT_FOUND",
		Message: fmt.Sprintf("State not known at event %s", eventID),
	}
}

// ErrBadJSON is Synapse's answer to a request body it cannot parse.
func ErrBadJSON() *MatrixError {
	return &MatrixError{
		Status:  http.StatusBadRequest,
		ErrCode: "M_NOT_JSON",
		Message: "Content not JSON.",
	}
}
