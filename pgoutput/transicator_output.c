#include <postgres.h>
#include <catalog/pg_type.h>
#include <access/xlogdefs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <replication/output_plugin.h>
#include <replication/logical.h>

/*
 * This plugin encodes logical encoding records into JSON. JSON is produced
 * using string concatenation -- after all, isn't that the point of JSON?
 *
 * Based largely on the "test_encoding.c" sample from the Postgres source.
 */

/* This is required by postgres. */
PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct {
  uint32 index;
} DecodingState;

static void print_literal(StringInfo s, Oid typid, char *outputstr)
{
	const char *valptr;

	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
    case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case BITOID:
		case VARBITOID:
      appendStringInfoChar(s, '"');
      appendStringInfoString(s, outputstr);
      appendStringInfoChar(s, '"');
			break;

		case BOOLOID:
			if (strcmp(outputstr, "t") == 0) {
				appendStringInfoString(s, "\"true\"");
			} else {
				appendStringInfoString(s, "\"false\"");
      }
			break;

		default:
			appendStringInfoChar(s, '"');
			for (valptr = outputstr; *valptr; valptr++)
			{
				char		ch = *valptr;

				if (SQL_STR_DOUBLE(ch, false)) {
					appendStringInfoChar(s, ch);
        }
				appendStringInfoChar(s, ch);
			}
			appendStringInfoChar(s, '"');
			break;
	}
}

static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int			natt;
  int     first = 1;

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		bool		isnull;		/* column is null? */

		attr = tupdesc->attrs[natt];

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped) {
			continue;
    }

		/*
		 * Don't print system columns, oid will already have been printed if
		 * present.
		 */
		if (attr->attnum < 0) {
			continue;
    }

		typid = attr->atttypid;

		/* get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
    if (isnull) {
      continue;
    }

    if (first) {
      first = 0;
    } else {
      appendStringInfoChar(s, ',');
    }
    appendStringInfo(s, "\"%s\":{\"type\":%i,\"value\":",
      NameStr(attr->attname), typid);

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		/* print data */
		if (isnull) {
			appendStringInfoString(s, "null");
    }	else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval)) {
      /* TODO What does this mean? */
			appendStringInfoString(s, "\"unchanged-toast-datum\"");
    }	else if (!typisvarlena) {
			print_literal(s, typid, OidOutputFunctionCall(typoutput, origval));
		} else {
			Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
			print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
		}
    appendStringInfoChar(s, '}');
	}
}

static void outputStart(
    struct LogicalDecodingContext *ctx,
    OutputPluginOptions *options,
    bool is_init
) {
  DecodingState* state = (DecodingState*)malloc(sizeof(DecodingState));
  state->index = 0;
  ctx->output_plugin_private = state;
  options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}


static void outputStop(
    struct LogicalDecodingContext *ctx
) {
  free(ctx->output_plugin_private);
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
	Form_pg_class class_form;
	TupleDesc	tupdesc;
  DecodingState* state = (DecodingState*)(ctx->output_plugin_private);

	class_form = RelationGetForm(relation);
	tupdesc = RelationGetDescr(relation);

	OutputPluginPrepareWrite(ctx, true);
  appendStringInfoChar(ctx->out, '{');

  /* TODO will this produce double-quoted table names? */
  char* tableName =
    quote_qualified_identifier(
      get_namespace_name(
        get_rel_namespace(RelationGetRelid(relation))),
        NameStr(class_form->relname));

  appendStringInfo(ctx->out, "\"table\":\"%s\"", tableName);
  appendStringInfo(ctx->out, ",\"changeSequence\":%lu", change->lsn);
  appendStringInfo(ctx->out, ",\"commitSequence\":%lu", txn->final_lsn);
  appendStringInfo(ctx->out, ",\"commitIndex\":%u", state->index);
  appendStringInfo(ctx->out, ",\"txid\":%u", txn->xid);

  state->index++;

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			appendStringInfoString(ctx->out, ",\"operation\":1");
			if (change->data.tp.newtuple != NULL) {
        appendStringInfoString(ctx->out, ",\"newRow\":{");
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.newtuple->tuple);
        appendStringInfoChar(ctx->out, '}');
      }
			break;

		case REORDER_BUFFER_CHANGE_UPDATE:
			appendStringInfoString(ctx->out, ",\"operation\":2");
			if (change->data.tp.oldtuple != NULL) {
        appendStringInfoString(ctx->out, ",\"oldRow\":{");
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.oldtuple->tuple);
				appendStringInfoChar(ctx->out, '}');
			}
			if (change->data.tp.newtuple != NULL) {
        appendStringInfoString(ctx->out, ",\"newRow\":{");
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.newtuple->tuple);
        appendStringInfoChar(ctx->out, '}');
      }
			break;

		case REORDER_BUFFER_CHANGE_DELETE:
			appendStringInfoString(ctx->out, ",\"operation\":3");
			if (change->data.tp.oldtuple != NULL) {
        appendStringInfoString(ctx->out, ",\"oldRow\":{");
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.oldtuple->tuple);
        appendStringInfoChar(ctx->out, '}');
      }
			break;

		default:
			Assert(false);
	}

  appendStringInfoChar(ctx->out, '}');
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
