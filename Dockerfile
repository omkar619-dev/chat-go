# syntax=docker/dockerfile:1

# ---------- build ----------
FROM golang:1.25-alpine AS build
WORKDIR /src

# Dependencies get their own layer, ahead of the source. go.mod and go.sum
# change rarely; the code changes every commit. Copied in this order, editing a
# handler reuses the cached module download instead of re-fetching the lot.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 is not housekeeping here — it is the reason this image runs on
# the homelab at all. A cgo binary links against the build image's glibc, and
# glibc built for the x86-64-v2 baseline dies at process start on the Old PC's
# Core 2 Duo, which predates SSE4.2. With cgo off, Go links no libc and targets
# GOAMD64=v1, so the binary runs on any amd64 chip. Every dependency here is
# pure Go (pgx, go-redis, kafka-go, coder/websocket, bcrypt), so nothing is lost.
ENV CGO_ENABLED=0 GOOS=linux

# -s -w strip the symbol table and DWARF debug info. Smaller image, faster pull
# over the homelab's 100Mbit USB-Ethernet; the cost is that a panic trace loses
# nothing useful (line numbers survive) but a debugger cannot attach.
RUN mkdir -p /out && \
    go build -ldflags="-s -w" -o /out/ \
      ./cmd/gateway ./cmd/persister ./cmd/indexer ./cmd/bot

# ---------- runtime ----------
FROM alpine:3.20

# ca-certificates costs a couple of hundred KB and buys the ability to make an
# outbound HTTPS call. Nothing does one today. When something eventually does,
# its absence shows up as "x509: certificate signed by unknown authority",
# which reads like a broken server rather than a missing package.
RUN apk add --no-cache ca-certificates && \
    adduser -D -u 10001 -h /app chat

# All four binaries in one image. They share a module, nearly all their
# dependencies and every internal package, so building them together costs
# almost nothing over building one — and it leaves ONE image to build, push,
# scan and pull, rather than four that must be kept at the same version.
COPY --from=build /out/ /app/

USER chat
WORKDIR /app

EXPOSE 8090

# The gateway is the default because it is the only process that serves
# traffic. The other three are background consumers and their Deployments
# override this with their own command — same image, different entrypoint.
CMD ["/app/gateway"]
