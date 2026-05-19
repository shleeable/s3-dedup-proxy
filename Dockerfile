FROM docker.io/library/debian:trixie-slim AS build

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        gzip \
        openjdk-25-jre \
    && curl -o /usr/share/keyrings/sbt.asc "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
    && echo "deb [signed-by=/usr/share/keyrings/sbt.asc] https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends sbt

COPY . /s3-dedup-proxy
WORKDIR /s3-dedup-proxy

RUN sbt stage

FROM docker.io/library/debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        openjdk-25-jre

COPY --from=build /s3-dedup-proxy/target/universal/stage /s3-dedup-proxy

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -sf http://localhost:23279/ || exit 1

EXPOSE 23278 23279

RUN mkdir -p /tmp/s3dedupproxy-buffer; chown 10001:10001 /tmp/s3dedupproxy-buffer

USER 10001
WORKDIR /s3-dedup-proxy

ENTRYPOINT ["/s3-dedup-proxy/bin/s3-dedup-proxy"]
CMD ["-Dconfig.file=application.conf"]
