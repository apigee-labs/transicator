#include <postgres.h>
#include <replication/output_plugin.h>
#include <replication/logical.h>

/* This is required by postgres. */
PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void outputTx(StringInfo out, ReorderBufferTXN *txn, const char* op) {
  appendStringInfo(out, "%s XID %li first %llu final %llu\n",
    op, txn->xid, txn->first_lsn, txn->final_lsn);
}

static void outputStart(
    struct LogicalDecodingContext *ctx,
    OutputPluginOptions *options,
    bool is_init
) {
  options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}


static void outputStop(
    struct LogicalDecodingContext *ctx
) {
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
  OutputPluginPrepareWrite(ctx, true);
  outputTx(ctx->out, txn, "BEGIN");
  OutputPluginWrite(ctx, true);
}

static void outputCommit(
    struct LogicalDecodingContext* ctx,
    ReorderBufferTXN *txn,
    XLogRecPtr commitLSN
) {
  OutputPluginPrepareWrite(ctx, true);
  outputTx(ctx->out, txn, "COMMIT");
  OutputPluginWrite(ctx, true);
}

static void outputChange(
    struct LogicalDecodingContext *ctx,
    ReorderBufferTXN *txn,
    Relation relation,
    ReorderBufferChange *change
) {
  OutputPluginPrepareWrite(ctx, true);
  outputTx(ctx->out, txn, "CHANGE");
  OutputPluginWrite(ctx, true);
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
  cb->startup_cb = outputStart;
  cb->shutdown_cb = outputStop;
  cb->begin_cb = outputBegin;
  cb->commit_cb = outputCommit;
  cb->change_cb = outputChange;
  cb->filter_by_origin_cb = outputFilter;
}
