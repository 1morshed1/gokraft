# Multi-stage build shared by all gokraft binaries. Which one is built is chosen
# by the CMD_NAME build arg (collector, logwriter, metricwriter, tracewriter,
# queryapi, loadgen). This keeps one small, cache-friendly Dockerfile for the
# whole monorepo instead of six near-identical ones.
#
#   docker build --build-arg CMD_NAME=collector -t gokraft/collector .

# ---- build stage ----
FROM golang:1.25 AS build
WORKDIR /src

# Copy go.mod/go.sum first so the module download layer is cached across code
# changes — a standard Go+Docker optimization.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD_NAME=collector
# CGO disabled -> a fully static binary that runs in a scratch/distroless image.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/app ./cmd/${CMD_NAME}

# ---- runtime stage ----
# distroless: no shell, no package manager, tiny attack surface. The queryapi
# also needs the web/ dir, copied below (harmless for the other binaries).
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/app /app/app
COPY web /app/web
USER nonroot:nonroot
ENTRYPOINT ["/app/app"]
