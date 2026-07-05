ARG BUILD_FROM
FROM $BUILD_FROM

SHELL ["/bin/ash", "-o", "pipefail", "-c"]

RUN apk add --no-cache bash python3 ca-certificates

COPY run.sh /run.sh
COPY app /app

RUN chmod 0755 /run.sh

CMD ["/run.sh"]
