#include <postgres.h>
#include <catalog/pg_type.h>
#include <access/xlogdefs.h>
#include <access/xlog.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <replication/output_plugin.h>
#include <replication/logical.h>
#include <transicator.h>
#include <transicator.pb-c.h>

static size_t countColumns(TupleDesc tupdesc) {
  int natt;
  size_t cols = 0;

  for (natt = 0; natt < tupdesc->natts; natt++)
  {
      Form_pg_attribute attr = tupdesc->attrs[natt];
      if (!(attr->attisdropped) && !(attr->attnum < 0)) {
        cols++;
      }
  }
  return cols;
}

static void setValue(Oid typ, Oid outputtyp, Datum val, Common__ValuePb* cv) {
  switch (typ) {
    case BOOLOID:
      cv->value_case = COMMON__VALUE_PB__VALUE_BOOL;
      cv->bool_ = DatumGetBool(val);
      break;
    case INT2OID:
      cv->value_case = COMMON__VALUE_PB__VALUE_INT;
      cv->int_ = DatumGetInt16(val);
      break;
    case INT4OID:
    case OIDOID:
      cv->value_case = COMMON__VALUE_PB__VALUE_INT;
      cv->int_ = DatumGetInt32(val);
      break;
    case INT8OID:
      cv->value_case = COMMON__VALUE_PB__VALUE_INT;
      cv->int_ = DatumGetInt64(val);
      break;
    case FLOAT4OID:
      cv->value_case = COMMON__VALUE_PB__VALUE_DOUBLE;
      cv->double_ = DatumGetFloat4(val);
      break;
    case FLOAT8OID:
      cv->value_case = COMMON__VALUE_PB__VALUE_DOUBLE;
      cv->double_ = DatumGetFloat8(val);
      break;
    case BYTEAOID:
      /* Length of the byte array is the length plus header */
      cv->value_case = COMMON__VALUE_PB__VALUE_BYTES;
      cv->bytes.len = VARSIZE(val) - 4;
      cv->bytes.data = (uint8_t*)VARDATA(val);
      break;
    default:
      /* Convert everything else into a string */
      cv->value_case = COMMON__VALUE_PB__VALUE_STRING;
      cv->string = OidOutputFunctionCall(outputtyp, val);
      break;
  }
}

static void tuple_to_proto(
  StringInfo s, TupleDesc tupdesc,
  HeapTuple tuple, Common__ColumnPb** cols)
{
  int	natt;
  int cp = 0;
  int colnum = 0;

  for (natt = 0; natt < tupdesc->natts; natt++)
	{
    Common__ColumnPb* col;
    Form_pg_attribute attr;
    bool isnull;
    Datum origval;
    Datum finalval;
    Datum typ;

    attr = tupdesc->attrs[natt];
    if ((attr->attisdropped) || (attr->attnum < 0)) {
			continue;
    }

    col = (Common__ColumnPb*)palloc(sizeof(Common__ColumnPb));
    common__column_pb__init(col);
    cols[colnum++] = col;

    typ =  attr->atttypid;
    col->name = NameStr(attr->attname);
    col->type = typ;
    col->has_type = 1;

		/* get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

    if (!isnull) {
  		Oid			typoutput;	/* output function */
  		bool		typisvarlena;

      Common__ValuePb* cv = (Common__ValuePb*)palloc(sizeof(Common__ValuePb));
      common__value_pb__init(cv);
      col->value = cv;

  		/* query output function */
  		getTypeOutputInfo(typ,
  						          &typoutput, &typisvarlena);

      if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval)) {
        /* TODO What does this mean? */
        cv->value_case = COMMON__VALUE_PB__VALUE_STRING;
        cv->string = "\"unchanged-toast-datum\"";
      }	else if (!typisvarlena) {
        setValue(typ, typoutput, origval, cv);
  		} else {
  			finalval = PointerGetDatum(PG_DETOAST_DATUM(origval));
        setValue(typ, typoutput, finalval, cv);
  		}
    }
    cp++;
  }
}

void transicatorOutputChangeProto(
  struct LogicalDecodingContext *ctx,
  ReorderBufferTXN *txn,
  Relation relation,
  ReorderBufferChange *change,
  DecodingState* state) {

  /*
   * TODO use utils/palloc.h and utils/memutils.h to create and reset
   * a memory context on every plugin run rather than alloc/free.
   */

  Form_pg_class class_form;
  TupleDesc	tupdesc;
  size_t numCols;
  size_t packSize;
  uint8_t* pack;
  Common__ChangePb pb = COMMON__CHANGE_PB__INIT;

  class_form = RelationGetForm(relation);
  tupdesc = RelationGetDescr(relation);

  pb.table =
    quote_qualified_identifier(
      get_namespace_name(
        get_rel_namespace(RelationGetRelid(relation))),
        NameStr(class_form->relname));

  pb.sequence = NULL;
  pb.changesequence = change->lsn;
  pb.has_changesequence = 1;
  pb.commitsequence = txn->final_lsn;
  pb.has_commitsequence = 1;
  pb.commitindex = state->index;
  pb.has_commitindex = 1;
  pb.transactionidepoch = convert_xid(txn->xid);
  pb.has_transactionidepoch = 1;

  state->index++;

  switch (change->action)
  {
    case REORDER_BUFFER_CHANGE_INSERT:
      pb.operation = 1;
      if (change->data.tp.newtuple != NULL) {
        numCols = countColumns(tupdesc);
        pb.n_newcolumns = numCols;
        pb.newcolumns = (Common__ColumnPb**)palloc(sizeof(Common__ColumnPb*) * numCols);
        tuple_to_proto(ctx->out, tupdesc, &change->data.tp.newtuple->tuple, pb.newcolumns);
      }
      break;

    case REORDER_BUFFER_CHANGE_UPDATE:
      pb.operation = 2;
      numCols = countColumns(tupdesc);
      if (change->data.tp.oldtuple != NULL) {
        pb.n_oldcolumns = numCols;
        pb.oldcolumns = (Common__ColumnPb**)palloc(sizeof(Common__ColumnPb*) * numCols);
        tuple_to_proto(ctx->out, tupdesc, &change->data.tp.oldtuple->tuple, pb.oldcolumns);
      }
      if (change->data.tp.newtuple != NULL) {
        pb.n_newcolumns = numCols;
        pb.newcolumns = (Common__ColumnPb**)palloc(sizeof(Common__ColumnPb*) * numCols);
        tuple_to_proto(ctx->out, tupdesc, &change->data.tp.newtuple->tuple, pb.newcolumns);
      }
      break;

    case REORDER_BUFFER_CHANGE_DELETE:
      pb.operation = 3;
      if (change->data.tp.oldtuple != NULL) {
        numCols = countColumns(tupdesc);
        pb.n_oldcolumns = numCols;
        pb.oldcolumns = (Common__ColumnPb**)palloc(sizeof(Common__ColumnPb*) * numCols);
        tuple_to_proto(ctx->out, tupdesc, &change->data.tp.oldtuple->tuple, pb.oldcolumns);
      }
      break;

    default:
      Assert(false);
  }

  packSize = common__change_pb__get_packed_size(&pb);
  pack = (uint8_t*)palloc(sizeof(uint8_t) * packSize);
  common__change_pb__pack(&pb, pack);

  OutputPluginPrepareWrite(ctx, true);
  appendBinaryStringInfo(ctx->out, (char*)pack, packSize);
}
