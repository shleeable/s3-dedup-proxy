FROM docker.io/library/debian:bookworm-slim AS root

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        openjdk-17-jre \
        nginx

FROM root AS build

COPY . /poolmgr
WORKDIR /poolmgr

RUN git submodule init && git submodule update
RUN ./gradlew shadowJar

FROM root

WORKDIR /

COPY --from=build /poolmgr/build/libs/jortage-poolmgr-1.5.5.jar /jortage-poolmgr-1.5.5.jar
COPY compose/config.jkson /

COPY compose/s3.service /etc/nginx/sites-enabled/

ENTRYPOINT ["java", "-jar", "/jortage-poolmgr-1.5.5.jar"]
