from postgres:9.5.4

#Deliberately run in multiple steps.  This way we can harness intermediate images, and not have to re-install the build tools after the first build
RUN apt-get update && apt-get install gcc make postgresql-server-dev-9.5

RUN mkdir /tmp/postgressplugin
COPY . /tmp/postgressplugin/pgoutput

RUN (cd /tmp/postgressplugin/pgoutput; make; make install)

#Remove the build systems to reduce security surface area
RUN apt-get purge -y --auto-remove  gcc make postgresql-server-dev-9.5
