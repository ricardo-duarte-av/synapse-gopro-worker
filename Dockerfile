FROM golang:1.25-alpine AS build
WORKDIR /src

# Download dependencies first so they cache independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# .dockerignore strips .git, so build info is passed in rather than derived.
ARG TAG=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

# GOEXPERIMENT=jsonv2 is required by mautrix's federation/pdu package, which
# implements per-room-version redaction. Redaction is security-relevant -- it is
# what strips content from an event a server may not fully see -- so it is worth
# depending on a maintained implementation rather than hand-rolling the
# per-version rules. Note this also switches encoding/json to the v2 backend
# process-wide; shadow comparison against Synapse is what would surface any
# resulting difference in output.
RUN CGO_ENABLED=0 GOEXPERIMENT=jsonv2 go build -trimpath \
      -ldflags="-s -w \
        -X main.tag=${TAG} \
        -X main.commit=${COMMIT} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/gopro-worker ./cmd/gopro-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gopro-worker /gopro-worker
ENTRYPOINT ["/gopro-worker"]
CMD ["-config", "/data/gopro-worker.yaml"]
