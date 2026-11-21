# syntax=docker/dockerfile:1
#
# Targets:
#   worker (default)  whisper-network + whisper-cli + ffmpeg. Can also run `server`.
#   server            whisper-network only, on distroless (no media tools at all).
#   downloader        whisper-network + yt-dlp + deno, for `worker --downloader`
#                     (no whisper-cli, no ffmpeg).

# Base images are pinned by digest for reproducible builds; dependabot's docker
# ecosystem updates the tag and digest together.
# GO_VERSION is informational; the golang base image tag below is what is used.
ARG GO_VERSION=1.27.4
# The whisper.cpp release, without the leading v. Its tagged source reports
# itself as "<release>-dev", so the worker image states the release
# explicitly (WHISPER_ENGINE_VERSION below) for transcript provenance.
ARG WHISPER_RELEASE=1.9.5
ARG WHISPER_VERSION=v${WHISPER_RELEASE}
# Commit that WHISPER_VERSION must resolve to (guards against a moved tag).
ARG WHISPER_COMMIT=d1be6fde11ac6e0407606b4e42fe72d34add8037

# yt-dlp (standalone release binary) and deno (the JS runtime yt-dlp uses for
# some extractors), both verified against pinned checksums. The weekly
# ytdlp-bump workflow updates the YTDLP_* values.
ARG YTDLP_VERSION=2026.10.26
ARG YTDLP_SHA256_AMD64=767ba9dce7052aa196f8b12652f433d76de128ba168966fa84b6394d5e6c6795
ARG YTDLP_SHA256_ARM64=e87e9533e89fe5889abaa21f5114dfe7756d6aa6219b83551add04d48b18e483
ARG DENO_VERSION=2.9.8
ARG DENO_SHA256_AMD64=e890975bc2549807bbcc0b584ffafda72cc7c93349dd99c80483dd784f6c4609
ARG DENO_SHA256_ARM64=d49596c20b9788cf8f5d8ddf644103d4bde56ed239753f0bdb92ba9361d51905

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS debian
# The Go toolchain runs on the build host's platform and cross-compiles, so
# multi-arch builds never emulate the Go build.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:5e8afd493a7124a8586e134d9b532731161d4b9becf9dee5a5956b107426f93d AS golang
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS distroless

# ---------------------------------------------------------------------------
# whisper.cpp: pinned release, portable CPU build.
FROM debian AS whisper
ARG WHISPER_VERSION
ARG WHISPER_COMMIT
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential cmake git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${WHISPER_VERSION}" https://github.com/ggml-org/whisper.cpp /src
WORKDIR /src
RUN test "$(git rev-parse HEAD)" = "${WHISPER_COMMIT}"
# On amd64, ggml builds every CPU variant (SSE4.2 … AVX-512) as loadable
# backends and selects the best one at runtime. Other architectures get a
# portable baseline build.
RUN if [ "${TARGETARCH}" = "amd64" ]; then \
      VARIANTS="-DGGML_BACKEND_DL=ON -DGGML_CPU_ALL_VARIANTS=ON"; \
    else \
      VARIANTS=""; \
    fi \
 && cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON \
      -DGGML_NATIVE=OFF ${VARIANTS} \
      -DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_SERVER=OFF \
 && cmake --build build -j"$(nproc)" --target whisper-cli \
 && mkdir -p /opt/whisper \
 && cp -a build/bin/whisper-cli build/bin/*.so* /opt/whisper/

# ---------------------------------------------------------------------------
# whisper-network: pure Go, static.
FROM golang AS builder
ARG VERSION=development
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags="-s -w -X 'github.com/deweysasser/whisper-network/program.Version=${VERSION}'" \
      -o /out/whisper-network . \
 && install -d -m 0700 /out/data

# ---------------------------------------------------------------------------
FROM distroless AS server
COPY --from=builder /out/whisper-network /usr/local/bin/whisper-network
# Mode 0700 (set explicitly: COPY would otherwise create the destination 0755),
# owned by the non-root user. A new named volume copies this
# directory's ownership and mode, so the non-root server
# can write to it without a chown step.
COPY --from=builder --chown=65532:65532 --chmod=0700 /out/data /data
VOLUME ["/data"]
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/whisper-network"]
CMD ["server"]

# ---------------------------------------------------------------------------
FROM debian AS worker
ARG WHISPER_RELEASE
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/whisper-network --create-home whisper
# whisper-cli and its libraries live together in /opt/whisper: ggml loads the
# CPU backend variants from the executable's directory, and the worker's
# sandbox grants read access to that directory.
COPY --from=whisper /opt/whisper/ /opt/whisper/
ENV PATH=/opt/whisper:$PATH
# Reported in worker facts and transcript provenance instead of whisper-cli's
# own "-dev" version string. The whisper stage verified that the source is
# exactly this release's tagged commit (WHISPER_COMMIT).
ENV WHISPER_ENGINE_VERSION="whisper.cpp ${WHISPER_RELEASE}"
RUN echo /opt/whisper > /etc/ld.so.conf.d/whisper.conf && ldconfig \
 && whisper-cli --version && ffmpeg -version | head -1
COPY --from=builder /out/whisper-network /usr/local/bin/whisper-network
USER 10001
VOLUME ["/var/lib/whisper-network"]
ENTRYPOINT ["/usr/local/bin/whisper-network"]
CMD ["worker", "--help"]

# ---------------------------------------------------------------------------
# Downloader tools: fetched and checksum-verified here so the final image
# needs neither curl nor unzip.
FROM debian AS downloader-tools
ARG TARGETARCH
ARG YTDLP_VERSION
ARG YTDLP_SHA256_AMD64
ARG YTDLP_SHA256_ARM64
ARG DENO_VERSION
ARG DENO_SHA256_AMD64
ARG DENO_SHA256_ARM64
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl unzip ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /dl
RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) ytdlp_file=yt-dlp_linux;         ytdlp_sha="${YTDLP_SHA256_AMD64}"; deno_arch=x86_64;  deno_sha="${DENO_SHA256_AMD64}" ;; \
      arm64) ytdlp_file=yt-dlp_linux_aarch64; ytdlp_sha="${YTDLP_SHA256_ARM64}"; deno_arch=aarch64; deno_sha="${DENO_SHA256_ARM64}" ;; \
      *) echo "unsupported architecture ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o yt-dlp "https://github.com/yt-dlp/yt-dlp/releases/download/${YTDLP_VERSION}/${ytdlp_file}"; \
    echo "${ytdlp_sha}  yt-dlp" | sha256sum -c -; \
    curl -fsSL -o deno.zip "https://github.com/denoland/deno/releases/download/v${DENO_VERSION}/deno-${deno_arch}-unknown-linux-gnu.zip"; \
    echo "${deno_sha}  deno.zip" | sha256sum -c -; \
    unzip -q deno.zip deno; \
    chmod 0755 yt-dlp deno

# ---------------------------------------------------------------------------
# Downloader worker: fetches URL jobs through a filtering proxy under a
# Landlock sandbox. No whisper or ffmpeg: it never processes media.
FROM debian AS downloader
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/whisper-network --create-home whisper
COPY --from=downloader-tools /dl/yt-dlp /dl/deno /usr/local/bin/
RUN yt-dlp --version && deno --version
COPY --from=builder /out/whisper-network /usr/local/bin/whisper-network
USER 10001
# Smoke test: the real deno and yt-dlp under the exact sandbox and limits of
# a fetch child (Landlock, seccomp, rlimits, connect only to the proxy port).
# Fails the build if either cannot run there, or the sandbox is below full.
RUN whisper-network downloader-selftest
VOLUME ["/var/lib/whisper-network"]
ENTRYPOINT ["/usr/local/bin/whisper-network"]
CMD ["worker", "--downloader", "--help"]
