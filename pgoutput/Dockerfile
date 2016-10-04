FROM postgres:9.5.4

RUN mkdir /tmp/postgresplugin
COPY . /tmp/postgresplugin/pgoutput
# Contents of "scripts" fix pg_hba.conf and postgresql.conf
COPY scripts /docker-entrypoint-initdb.d

# Deliberately run in multiple steps.  This way we can harness intermediate
# images, and not have to re-install the build tools after the first build
RUN \
     apt-get update \
  && apt-get install -y gcc make postgresql-server-dev-9.5

RUN (cd /tmp/postgresplugin/pgoutput; make; make install)

# Remove the build systems to reduce security surface area
RUN apt-get purge -y --auto-remove  gcc make postgresql-server-dev-9.5

# Add VOLUMEs to allow backup of config, logs and databases
VOLUME  ["/var/log/postgresql", "/var/lib/postgresql"]
