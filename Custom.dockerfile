FROM golang:1.12 as builder

# NAME and VERSION are the name of the software in releases.hashicorp.com
# and the version to download. Example: NAME=consul VERSION=1.2.3.
ARG NAME
ARG VERSION

# Set ARGs as ENV so that they can be used in ENTRYPOINT/CMD
ENV NAME=$NAME
ENV VERSION=$VERSION

ADD . /src
RUN cd /src && GOOS=linux GOARCH=amd64 CGO_ENABLED=0  go build -o ${NAME}

FROM alpine:3.9

ARG NAME

ENV NAME=$NAME

COPY --from=builder /src/${NAME} /bin/

RUN set -eux && \
    apk add --no-cache ca-certificates curl dumb-init libcap su-exec iputils jq

# Create a non-root user to run the software.
RUN addgroup ${NAME} && \
    adduser -S -G ${NAME} ${NAME}

RUN consul version
# The /consul/data dir is used by Consul to store state. The agent will be started
# with /consul/config as the configuration directory so you can add additional
# config files in that location.
RUN mkdir -p /consul/data && \
    mkdir -p /consul/config && \
    chown -R ${NAME}:${NAME} /consul

# set up nsswitch.conf for Go's "netgo" implementation which is used by Consul,
# otherwise DNS supercedes the container's hosts file, which we don't want.
RUN test -e /etc/nsswitch.conf || echo 'hosts: files dns' > /etc/nsswitch.conf

# Server RPC is used for communication between Consul clients and servers for internal
# request forwarding.
EXPOSE 8300

# Serf LAN and WAN (WAN is used only by Consul servers) are used for gossip between
# Consul agents. LAN is within the datacenter and WAN is between just the Consul
# servers in all datacenters.
EXPOSE 8301 8301/udp 8302 8302/udp

# HTTP and DNS (both TCP and UDP) are the primary interfaces that applications
# use to interact with Consul.
EXPOSE 8500 8600 8600/udp


# Consul doesn't need root privileges so we run it as the consul user from the
# entry point script. The entry point script also uses dumb-init as the top-level
# process to reap any zombie processes created by Consul sub-processes.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]

# By default you'll get an insecure single-node development server that stores
# everything in RAM, exposes a web UI and HTTP endpoints, and bootstraps itself.
# Don't use this configuration for production.
CMD ["agent"]
