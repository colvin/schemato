FROM postgrest/postgrest:latest

WORKDIR /app
USER root
COPY docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
COPY postgrest.conf .
COPY schemata/*.sql ./
COPY bin/schemato /usr/bin/schemato

CMD [ "docker-entrypoint.sh" ]
