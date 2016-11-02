MODULE_big = transicator_output
OBJS = transicator_output.o output_string.o output_proto.o transicator.pb-c.o

PG_CONFIG = pg_config
PG_CPPFLAGS=-I/usr/local/include 
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
SHLIB_LINK = -lprotobuf-c

output_proto.c: transicator.pb-c.h

transicator.pb-c.o: transicator.pb-c.c
transicator.pb-c.c: ../common/transicator.proto
	protoc-c -I../common --c_out=. ../common/transicator.proto
transicator.pb-c.h: ../common/transicator.proto
	protoc-c -I../common --c_out=. ../common/transicator.proto
