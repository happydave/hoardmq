# HoardMQ broker image: a static binary on a distroless base.
#
#   docker build -t hoardmq:dev .
#
# The container runs `hoardmq serve` with the configuration at
# /etc/hoardmq/config.json (mount or bake one) and its data under
# /var/lib/hoardmq. The Helm chart in deploy/helm/hoardmq supplies both.
FROM docker.io/library/golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOFLAGS=-trimpath go build -ldflags='-s -w' -o /out/hoardmq ./cmd/hoardmq

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hoardmq /usr/local/bin/hoardmq
COPY LICENSE-APACHE LICENSE-MIT NOTICE /usr/share/doc/hoardmq/
ENV HOARDMQ_CONFIG=/etc/hoardmq/config.json
VOLUME /var/lib/hoardmq
EXPOSE 1883 7000 9090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/hoardmq"]
CMD ["serve"]
