/*
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
