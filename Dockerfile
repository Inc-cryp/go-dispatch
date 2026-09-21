# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1: build. Static binary, no cgo, so the runtime image needs no libc.
# ---------------------------------------------------------------------------
ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# The module has no dependencies, so there is no go.sum to copy and no module
# cache to warm. Copying the sources first and building second is still the
# right order: it keeps the layer cache honest if files are added later.
COPY go.mod ./
COPY queue/       ./queue/
COPY eventbus/    ./eventbus/
COPY ratelimit/   ./ratelimit/
COPY worker/      ./worker/
COPY cmd/         ./cmd/

# -trimpath strips absolute build paths from the binary.
# -s -w drop the symbol table and DWARF data (smaller image, harder to debug).
# CGO_ENABLED=0 is what makes the scratch stage below possible.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags='-s -w -buildid=' \
        -o /out/dispatchd ./cmd/dispatchd

# ---------------------------------------------------------------------------
# Stage 2: runtime. Scratch is the smallest and least attackable base there is:
# no shell, no package manager, no libc. The binary is static, so it runs.
# ---------------------------------------------------------------------------
FROM scratch

# TLS roots are not needed by this binary (it serves HTTP and opens no
# outbound TLS connections), so they are deliberately omitted. If a future
# version calls an external HTTPS API, copy /etc/ssl/certs/ca-certificates.crt
# from the build stage.

# Run as a non-root user. scratch has no /etc/passwd, so the numeric UID is
# what matters; 65534 is the conventional "nobody".
USER 65534:65534

COPY --from=build /out/dispatchd /dispatchd

EXPOSE 8080

# There is no shell in this image, so the exec form is mandatory rather than a
# style preference: CMD ["sh", "-c", ...] would fail at runtime.
#
# dispatchd is configured by flags only — it reads no environment variables, so
# there is deliberately no ENV block here pretending otherwise. Override the
# workload with `docker run dispatch:dev -subjects 5000 -rate 100`.
ENTRYPOINT ["/dispatchd"]
CMD ["-workers", "8", "-rate", "200", "-burst", "50", "-subjects", "2000", "-addr", ":8080"]
