FROM alpine:3

ARG version=v1.0.0-rc2
COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh && \
    wget https://github.com/threefoldtech/rmb-rs/releases/download/$version/rmb-relay -O /usr/sbin/rmb-relay &&\
    chmod +x /usr/sbin/rmb-relay

ENTRYPOINT ["/entrypoint.sh"]
