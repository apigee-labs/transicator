#include <postgres.h>
#include <catalog/pg_type.h>
#include <access/xlogdefs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <replication/output_plugin.h>
#include <replication/logical.h>
#include <transicator.h>

/*
 * This plugin encodes logical encoding records into JSON. JSON is produced
 * using string concatenation -- after all, isn't that the point of JSON?
 *
 * Based largely on the "test_encoding.c" sample from the Postgres source.
 */

/* This is required by postgres. */
PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void outputStart(
    struct LogicalDecodingContext *ctx,
    OutputPluginOptions *options,
    bool is_init
) {
  ListCell   *option;
  int binaryFormat = 0;
  DecodingState* state;

  foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);
    if (!strcmp(elem->defname, "protobuf")) {
      binaryFormat = 1;
    }
  }

  if (binaryFormat) {
    elog(LOG, "Logical decoding output in protobuf format");
  } else {
    elog(LOG, "Logical decoding output in JSON format");
  }

  /* Create state for decoding. */
  state = (DecodingState*)palloc(sizeof(DecodingState));
  /* Include a memory context that we will reset for each output run. */
  state->memCtx =
    AllocSetContextCreate(ctx->context, "transicator_output",
      ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE,
      ALLOCSET_DEFAULT_MAXSIZE);
  state->index = 0;
  state->isBinary = binaryFormat;

  if (binaryFormat) {
    options->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
  } else {
    options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
  }

  ctx->output_plugin_private = state;
}


static void outputStop(
    struct LogicalDecodingContext *ctx
) {
  /* This will free all memory that we allocated. */
  DecodingState* state = (DecodingState*)(ctx->output_plugin_private);
  MemoryContextDelete(state->memCtx);
}

static bool outputFilter(
    struct LogicalDecodingContext *ctx,
    RepOriginId origin_id
) {
  /* False means yes */
  return FALSE;
}

static void outputBegin(
    struct LogicalDecodingContext* ctx,
    ReorderBufferTXN *txn
) {
  /* Reset "index" for start of new transaction */
  DecodingState* state = (DecodingState*)(ctx->output_plugin_private);
  state->index = 0;
}

static void outputCommit(
    struct LogicalDecodingContext* ctx,
    ReorderBufferTXN *txn,
    XLogRecPtr commitLSN
) {
}

static void outputChange(
    struct LogicalDecodingContext *ctx,
    ReorderBufferTXN *txn,
    Relation relation,
    ReorderBufferChange *change
) {
  MemoryContext oldMemCtx;
  DecodingState* state = (DecodingState*)(ctx->output_plugin_private);

  /* Switch to our private memory context so that we will not leak. */
  oldMemCtx = MemoryContextSwitchTo(state->memCtx);
  if (state->isBinary) {
    transicatorOutputChangeProto(ctx, txn, relation, change, state);
  } else {
    transicatorOutputChangeString(ctx, txn, relation, change, state);
  }

  /* Switch back to original context and release everything we "palloc"ed */
  MemoryContextSwitchTo(oldMemCtx);
  MemoryContextReset(state->memCtx);
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
  cb->startup_cb = outputStart;
  cb->shutdown_cb = outputStop;
  cb->begin_cb = outputBegin;
  cb->commit_cb = outputCommit;
  cb->change_cb = outputChange;
  cb->filter_by_origin_cb = outputFilter;
}
