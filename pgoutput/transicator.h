#ifndef TRANSICATOR_PLUGIN_H
#define TRANSICATOR_PLUGIN_H

typedef struct {
  MemoryContext    memCtx;
  uint32           index;
  int              isBinary;
} DecodingState;

extern void transicatorOutputChangeString(
  struct LogicalDecodingContext *ctx,
  ReorderBufferTXN *txn,
  Relation relation,
  ReorderBufferChange *change,
  DecodingState* state);

extern void transicatorOutputChangeProto(
  struct LogicalDecodingContext *ctx,
  ReorderBufferTXN *txn,
  Relation relation,
  ReorderBufferChange *change,
  DecodingState* state);

extern uint64 convert_xid(TransactionId xid);

#endif
