FROM postgres:9.5.4

RUN mkdir /tmp/postgresplugin
COPY . /tmp/postgresplugin/pgoutput
COPY scripts /docker-entrypoint-initdb.d

# Install tools to build our plugin, build it, and install it
RUN \
  apt-get update && \
  apt-get install -y gcc make postgresql-server-dev-9.5 && \
  cd /tmp/postgresplugin/pgoutput && \
  make install && \
  rm -rf /tmp/postgresplugin

# Add VOLUMEs to allow backup of config, logs and databases
VOLUME  ["/var/log/postgresql", "/var/lib/postgresql"]
