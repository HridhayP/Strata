# Multi-stage build: a static strata-server (and strata-bench) on distroless.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/strata-server ./cmd/strata-bench \
 && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /data /data
EXPOSE 7000 9100
ENTRYPOINT ["/usr/local/bin/strata-server"]
