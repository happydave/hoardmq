# HoardMQ reference Dockerfile. The project publishes no image: copy this
# file, adapt it, and build your own image from a source checkout.
#
#   podman build --build-arg VERSION="$(git describe --tags --always --dirty)" \
#     -t <registry>/hoardmq:<tag> .
#   podman build --build-arg VERSION=... --build-arg CONTRIB=_authsql \
#     -t <registry>/hoardmq-authsql:<tag> .
#
# VERSION is the image's only version source: .dockerignore leaves .git out
# of the build context, so `hoardmq version` prints "devel" (with CONTRIB,
# "devel (/src)") in an image built without it, even from a tagged checkout. CONTRIB names directories under
# contrib/ to compile in with hoardmq-build (contrib/README.md); their
# dependencies are fetched through the Go module proxy during the build.
#
# Both bases are pinned by version and digest. To refresh a pin, pull the
# tag, read its digest (podman image inspect --format '{{.Digest}}' <tag>)
# and replace both parts of the reference.
#
# The container runs `hoardmq serve` with the configuration at
# /etc/hoardmq/config.json (mount or bake one) and its data under
# /var/lib/hoardmq. The reference Helm chart in deploy/helm/hoardmq supplies
# both.
FROM docker.io/library/golang:1.26.10-alpine3.24@sha256:87c65a688975c0b1d4585d204874c45ca78354670e3a5f11368c5a492303d696 AS build
ARG VERSION=""
ARG CONTRIB=""
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN set -eu; \
    case "$VERSION" in *" "*) echo "VERSION holds a space: '$VERSION'" >&2; exit 1;; esac; \
    ldflags="-s -w"; \
    if [ -n "$VERSION" ]; then \
      ldflags="$ldflags -X=github.com/happydave/hoardmq/internal/cli.stampedVersion=$VERSION"; \
    fi; \
    export CGO_ENABLED=0 GOFLAGS="-trimpath '-ldflags=$ldflags'"; \
    if [ -z "$CONTRIB" ]; then \
      go build -o /out/hoardmq ./cmd/hoardmq; \
      exit 0; \
    fi; \
    with=""; \
    for d in $CONTRIB; do \
      if [ ! -d "contrib/$d" ]; then echo "CONTRIB: no directory contrib/$d" >&2; exit 1; fi; \
      mkdir -p /modules; \
      cp -r "contrib/$d" "/modules/$d"; \
      mv "/modules/$d/go.mod.txt" "/modules/$d/go.mod"; \
      mv "/modules/$d/go.sum.txt" "/modules/$d/go.sum"; \
      with="$with --with $(cd "/modules/$d" && go list -m)=/modules/$d"; \
    done; \
    go run ./cmd/hoardmq-build --hoardmq-dir /src $with --output /out/hoardmq

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/hoardmq /usr/local/bin/hoardmq
COPY LICENSE-APACHE LICENSE-MIT NOTICE /usr/share/doc/hoardmq/
ENV HOARDMQ_CONFIG=/etc/hoardmq/config.json
VOLUME /var/lib/hoardmq
EXPOSE 1883 7000 9090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/hoardmq"]
CMD ["serve"]
