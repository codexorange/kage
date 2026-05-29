# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.26.2-alpine AS builder

# git is needed if any dependency uses a VCS source; ca-certificates for TLS.
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Copy dependency manifests first to leverage Docker layer cache.
COPY go.mod go.sum* ./
RUN go mod download

# Copy the rest of the source and build a fully static binary.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/kaged \
    ./cmd/kaged

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# scratch gives the smallest possible image and the smallest attack surface.
# The binary is statically linked so no libc is needed.
FROM scratch

# Bring in TLS root certificates and timezone data from the builder stage.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy the compiled binary.
COPY --from=builder /out/kaged /kaged

# Kafka wire protocol port.
EXPOSE 9092

# Data directory — mount a volume here in production.
VOLUME ["/data"]

# Run as an unprivileged user (UID 65534 = nobody on most Linux distros).
USER 65534:65534

ENTRYPOINT ["/kaged"]
