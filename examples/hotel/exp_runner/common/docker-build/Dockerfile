FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3t64 ca-certificates && rm -rf /var/lib/apt/lists/*
ARG BINARY_NAME
ARG GEN_CONFIG_PATH
ARG LOG_LEVEL=info
ENV LOG_LEVEL=${LOG_LEVEL}
COPY .build-bin/${BINARY_NAME} /usr/local/bin/${BINARY_NAME}
COPY ${GEN_CONFIG_PATH} /usr/gen_config.json
COPY --chmod=755 exp_runner/common/docker-build/entrypoint.sh /usr/entrypoint.sh
ENTRYPOINT ["sh", "-c"]
CMD ["/usr/entrypoint.sh"]
