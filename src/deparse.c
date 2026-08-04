/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Remote SQL template generation for pg_replica_fanout_fdw: a column
 *		  list, an optional pushed-down WHERE predicate, and the ctid-range
 *		  bound for one replica's slice.
 *
 *		  Every replica is a byte-identical physical copy of the primary's
 *		  catalog, so unlike postgres_fdw we don't need a shippable-objects
 *		  catalog or type/operator/collation checks: any IMMUTABLE
 *		  expression built only from this baserel's own Vars is safe to ship
 *		  as-is.  The qual printer below is trimmed from postgres_fdw's
 *		  deparseExpr family (contrib/postgres_fdw/deparse.c) to just the
 *		  node types repfdw_foreign_expr_walker() accepts.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_replica_fanout_fdw.h"

static char *repfdw_remote_colname(Oid foreigntableid, int attnum);
static bool repfdw_foreign_expr_walker(Node *node, PlannerInfo *root,
										RelOptInfo *baserel);
static void repfdw_deparse_expr(StringInfo buf, Expr *node,
								 Oid foreigntableid);
static void repfdw_deparse_var(StringInfo buf, Var *node, Oid foreigntableid);
static void repfdw_deparse_const(StringInfo buf, Const *node);
static void repfdw_deparse_string_literal(StringInfo buf, const char *val);
static void repfdw_deparse_op_expr(StringInfo buf, OpExpr *node,
									Oid foreigntableid);
static void repfdw_deparse_scalar_array_op_expr(StringInfo buf,
												 ScalarArrayOpExpr *node,
												 Oid foreigntableid);
static void repfdw_deparse_bool_expr(StringInfo buf, BoolExpr *node,
									  Oid foreigntableid);
static void repfdw_deparse_null_test(StringInfo buf, NullTest *node,
									  Oid foreigntableid);
static void repfdw_deparse_operator_name(StringInfo buf,
										  Form_pg_operator opform);

/*
 * repfdw_remote_colname
 *		Resolve the remote column name for attnum on foreigntableid: its
 *		"column_name" option if set, else the local attribute name.
 */
static char *
repfdw_remote_colname(Oid foreigntableid, int attnum)
{
	List	   *coloptions = GetForeignColumnOptions(foreigntableid, attnum);
	char	   *remotename = NULL;
	ListCell   *lc;

	foreach(lc, coloptions)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			remotename = defGetString(def);
			break;
		}
	}
	if (remotename == NULL)
		remotename = get_attname(foreigntableid, attnum, false);

	return remotename;
}

/*
 * RepFdwDeparseTemplate
 *		Build "SELECT <cols|NULL> FROM <schema>.<table>", with no WHERE
 *		clause yet -- RepFdwBuildBoundedSql appends the pushed predicate and
 *		the per-slice ctid bound.  retrieved_attrs is a list of ascending
 *		attnums (as produced by GetForeignPlan from the plan-time
 *		attrs_used bitmap).
 */
char *
RepFdwDeparseTemplate(Oid foreigntableid, List *retrieved_attrs,
					  const char *schema, const char *table)
{
	StringInfoData buf;
	ListCell   *lc;
	bool		first = true;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "SELECT ");

	if (retrieved_attrs == NIL)
		appendStringInfoString(&buf, "NULL");
	else
	{
		foreach(lc, retrieved_attrs)
		{
			int			attnum = lfirst_int(lc);
			char	   *remotename = repfdw_remote_colname(foreigntableid, attnum);

			if (!first)
				appendStringInfoString(&buf, ", ");
			appendStringInfoString(&buf, quote_identifier(remotename));
			first = false;
		}
	}

	appendStringInfo(&buf, " FROM %s",
					 quote_qualified_identifier(schema, table));

	return buf.data;
}

/*
 * RepFdwDeparseCountTemplate
 *		Build "SELECT count(*) FROM <schema>.<table>" for the count(*)
 *		pushdown plan shape -- no columns, no WHERE yet (RepFdwBuildBoundedSql
 *		appends the per-slice ctid bound, same as for a plain scan).
 */
char *
RepFdwDeparseCountTemplate(const char *schema, const char *table)
{
	return psprintf("SELECT count(*) FROM %s",
					quote_qualified_identifier(schema, table));
}

/*
 * RepFdwBuildBoundedSql
 *		Append the ctid-range WHERE clause for one replica's slice to a
 *		deparsed template, using $1/$2 bind-parameter placeholders for
 *		whichever bound(s) are present (NULL/NULL means the whole
 *		relation, i.e. no WHERE clause at all).  The parameter order here
 *		must match the values RepFdwStartOneQuery binds.
 */
char *
RepFdwBuildBoundedSql(const char *base_sql, const char *remote_pred,
					 const RepFdwCtidBound *bound)
{
	StringInfoData buf;
	bool		need_and = false;

	initStringInfo(&buf);
	appendStringInfoString(&buf, base_sql);

	if ((remote_pred == NULL || remote_pred[0] == '\0') &&
		bound->lo == NULL && bound->hi == NULL)
		return buf.data;

	appendStringInfoString(&buf, " WHERE ");

	if (remote_pred != NULL && remote_pred[0] != '\0')
	{
		appendStringInfo(&buf, "(%s)", remote_pred);
		need_and = true;
	}

	if (bound->lo != NULL)
	{
		if (need_and)
			appendStringInfoString(&buf, " AND ");
		appendStringInfoString(&buf, "ctid >= $1");
		need_and = true;
	}

	if (bound->hi != NULL)
	{
		if (need_and)
			appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "ctid < $%d", bound->lo != NULL ? 2 : 1);
	}

	return buf.data;
}

/*
 * repfdw_foreign_expr_walker
 *		Recursive shippability classifier.  Returns true only for trees
 *		built from node types this deparser knows how to print, referencing
 *		only baserel's own user columns.  The caller (RepFdwIsForeignQual)
 *		additionally requires the whole expression to be immutable: N
 *		replicas each evaluate the pushed predicate independently against
 *		their own ctid slice, so shipping anything that could evaluate
 *		differently per replica (a STABLE function reading a replica's own
 *		clock/snapshot, or anything VOLATILE) risks a wrong answer -- this
 *		is why we're stricter here than postgres_fdw, which ships STABLE
 *		functions freely.
 */
static bool
repfdw_foreign_expr_walker(Node *node, PlannerInfo *root, RelOptInfo *baserel)
{
	if (node == NULL)
		return true;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				return var->varno == baserel->relid &&
					var->varlevelsup == 0 &&
					var->varattno > 0;
			}

		case T_Const:
			return true;

		case T_OpExpr:
			return repfdw_foreign_expr_walker((Node *) ((OpExpr *) node)->args,
											  root, baserel);

		case T_ScalarArrayOpExpr:
			return repfdw_foreign_expr_walker((Node *) ((ScalarArrayOpExpr *) node)->args,
											  root, baserel);

		case T_BoolExpr:
			return repfdw_foreign_expr_walker((Node *) ((BoolExpr *) node)->args,
											  root, baserel);

		case T_NullTest:
			return repfdw_foreign_expr_walker((Node *) ((NullTest *) node)->arg,
											  root, baserel);

		case T_List:
			{
				ListCell   *lc;

				foreach(lc, (List *) node)
				{
					if (!repfdw_foreign_expr_walker((Node *) lfirst(lc), root,
													baserel))
						return false;
				}
				return true;
			}

		default:
			return false;
	}
}

/*
 * RepFdwIsForeignQual
 *		Public shippability gate for one top-level restriction clause.
 *		Immutability is required in addition to the structural checks in
 *		repfdw_foreign_expr_walker: N replicas each evaluate the predicate
 *		independently against their own ctid slice, so the fan-out answer
 *		equals the local answer only if the expression is guaranteed to
 *		evaluate identically everywhere.  A STABLE function such as now()
 *		or current_user can differ between replicas' clocks/snapshots and
 *		silently produce a wrong answer -- this is why we reject STABLE too,
 *		unlike postgres_fdw which ships stable functions freely.
 */
bool
RepFdwIsForeignQual(PlannerInfo *root, RelOptInfo *baserel, Expr *expr)
{
	if (contain_mutable_functions((Node *) expr))
		return false;

	return repfdw_foreign_expr_walker((Node *) expr, root, baserel);
}

/*
 * repfdw_deparse_string_literal
 *		Append a SQL string literal for val to buf.  Copied from
 *		postgres_fdw's deparseStringLiteral (contrib/postgres_fdw/deparse.c).
 */
static void
repfdw_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	if (strchr(val, '\\') != NULL)
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * repfdw_deparse_var
 *		Print a Var as its remote column name (it must be one of baserel's
 *		own user columns -- guaranteed by repfdw_foreign_expr_walker).
 */
static void
repfdw_deparse_var(StringInfo buf, Var *node, Oid foreigntableid)
{
	char	   *remotename = repfdw_remote_colname(foreigntableid,
													node->varattno);

	appendStringInfoString(buf, quote_identifier(remotename));
}

/*
 * repfdw_deparse_const
 *		Print a Const as a literal, with an explicit ::type cast so the
 *		remote parses it with exactly the same type as the coordinator.
 *		Trimmed from postgres_fdw's deparseConst (showtype == 0 behavior),
 *		but since our replicas are byte-identical catalogs there is no
 *		"maybe the remote type differs" concern to hedge against, so we
 *		always show the type rather than reproducing its default-typing
 *		heuristic.
 */
static void
repfdw_deparse_const(StringInfo buf, Const *node)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfo(buf, "NULL::%s",
						 format_type_be(node->consttype));
		return;
	}

	getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, node->constvalue);

	switch (node->consttype)
	{
		case BOOLOID:
			appendStringInfoString(buf, strcmp(extval, "t") == 0 ? "true" : "false");
			break;
		default:
			repfdw_deparse_string_literal(buf, extval);
			break;
	}

	pfree(extval);

	appendStringInfo(buf, "::%s", format_type_be(node->consttype));
}

/*
 * repfdw_deparse_operator_name
 *		Print an operator name, schema-qualified unless it's in pg_catalog.
 *		Copied from postgres_fdw's deparseOperatorName.
 */
static void
repfdw_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
{
	char	   *opname = NameStr(opform->oprname);

	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname = get_namespace_name(opform->oprnamespace);

		appendStringInfo(buf, "OPERATOR(%s.%s)",
						 quote_identifier(opnspname), opname);
	}
	else
		appendStringInfoString(buf, opname);
}

/*
 * repfdw_deparse_op_expr
 *		Print an OpExpr, always parenthesized.  Modeled on postgres_fdw's
 *		deparseOpExpr, minus the Const-cast-suppression heuristic (not
 *		needed here: byte-identical catalogs mean there's no "local vs.
 *		remote column type differs" case to hedge against).
 */
static void
repfdw_deparse_op_expr(StringInfo buf, OpExpr *node, Oid foreigntableid)
{
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;

	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	appendStringInfoChar(buf, '(');

	if (oprkind == 'b')
	{
		repfdw_deparse_expr(buf, linitial(node->args), foreigntableid);
		appendStringInfoChar(buf, ' ');
	}

	repfdw_deparse_operator_name(buf, form);
	appendStringInfoChar(buf, ' ');

	repfdw_deparse_expr(buf, llast(node->args), foreigntableid);

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * repfdw_deparse_scalar_array_op_expr
 *		Print "(lhs OP ANY/ALL (rhs))".  Modeled on postgres_fdw's
 *		deparseScalarArrayOpExpr.
 */
static void
repfdw_deparse_scalar_array_op_expr(StringInfo buf, ScalarArrayOpExpr *node,
									Oid foreigntableid)
{
	HeapTuple	tuple;
	Form_pg_operator form;

	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	appendStringInfoChar(buf, '(');

	repfdw_deparse_expr(buf, linitial(node->args), foreigntableid);
	appendStringInfoChar(buf, ' ');

	repfdw_deparse_operator_name(buf, form);
	appendStringInfo(buf, " %s (", node->useOr ? "ANY" : "ALL");

	repfdw_deparse_expr(buf, lsecond(node->args), foreigntableid);

	appendStringInfoChar(buf, ')');
	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * repfdw_deparse_bool_expr
 *		Print "(a AND b ...)", "(a OR b ...)", or "(NOT a)".  Modeled on
 *		postgres_fdw's deparseBoolExpr.
 */
static void
repfdw_deparse_bool_expr(StringInfo buf, BoolExpr *node, Oid foreigntableid)
{
	const char *op = NULL;
	bool		first;
	ListCell   *lc;

	if (node->boolop == NOT_EXPR)
	{
		appendStringInfoString(buf, "(NOT ");
		repfdw_deparse_expr(buf, linitial(node->args), foreigntableid);
		appendStringInfoChar(buf, ')');
		return;
	}

	op = (node->boolop == AND_EXPR) ? "AND" : "OR";

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		repfdw_deparse_expr(buf, (Expr *) lfirst(lc), foreigntableid);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * repfdw_deparse_null_test
 *		Print "(x IS [NOT] NULL)".  Modeled on postgres_fdw's
 *		deparseNullTest, minus its rowtype IS DISTINCT FROM NULL case: our
 *		Var gate only ever hands this a plain column reference with
 *		argisrow == false, for which plain IS [NOT] NULL is exact.
 */
static void
repfdw_deparse_null_test(StringInfo buf, NullTest *node, Oid foreigntableid)
{
	appendStringInfoChar(buf, '(');
	repfdw_deparse_expr(buf, node->arg, foreigntableid);

	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
}

/*
 * repfdw_deparse_expr
 *		Dispatch on node type.  Must accept exactly the node types
 *		repfdw_foreign_expr_walker accepts.
 */
static void
repfdw_deparse_expr(StringInfo buf, Expr *node, Oid foreigntableid)
{
	switch (nodeTag(node))
	{
		case T_Var:
			repfdw_deparse_var(buf, (Var *) node, foreigntableid);
			break;
		case T_Const:
			repfdw_deparse_const(buf, (Const *) node);
			break;
		case T_OpExpr:
			repfdw_deparse_op_expr(buf, (OpExpr *) node, foreigntableid);
			break;
		case T_ScalarArrayOpExpr:
			repfdw_deparse_scalar_array_op_expr(buf, (ScalarArrayOpExpr *) node,
												foreigntableid);
			break;
		case T_BoolExpr:
			repfdw_deparse_bool_expr(buf, (BoolExpr *) node, foreigntableid);
			break;
		case T_NullTest:
			repfdw_deparse_null_test(buf, (NullTest *) node, foreigntableid);
			break;
		default:
			elog(ERROR,
				 "pg_replica_fanout_fdw: unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * RepFdwDeparseQuals
 *		Deparse the AND of remote_exprs (a list of shippable clause Exprs,
 *		not RestrictInfos) into a SQL predicate string, with no leading
 *		WHERE.  Returns NULL if remote_exprs is empty.
 */
char *
RepFdwDeparseQuals(Oid foreigntableid, List *remote_exprs)
{
	StringInfoData buf;
	ListCell   *lc;
	bool		first = true;

	if (remote_exprs == NIL)
		return NULL;

	initStringInfo(&buf);

	foreach(lc, remote_exprs)
	{
		if (!first)
			appendStringInfoString(&buf, " AND ");
		repfdw_deparse_expr(&buf, (Expr *) lfirst(lc), foreigntableid);
		first = false;
	}

	return buf.data;
}

/*
 * RepFdwGetSplittableIn
 *		If remote_conds is exactly one shippable "col = ANY (ARRAY[literals])"
 *		clause -- i.e. a plain IN-list on a column against a literal array --
 *		return that ScalarArrayOpExpr; otherwise NULL.  This is the clause the
 *		value-split mode partitions across replicas.
 */
ScalarArrayOpExpr *
RepFdwGetSplittableIn(List *remote_conds)
{
	RestrictInfo *ri;
	ScalarArrayOpExpr *saoe;
	Node	   *arr;

	if (list_length(remote_conds) != 1)
		return NULL;
	ri = (RestrictInfo *) linitial(remote_conds);
	if (!IsA(ri->clause, ScalarArrayOpExpr))
		return NULL;
	saoe = (ScalarArrayOpExpr *) ri->clause;
	if (!saoe->useOr || list_length(saoe->args) != 2)
		return NULL;			/* ANY (IN), not ALL, with a scalar/array pair */
	if (!IsA(linitial(saoe->args), Var))
		return NULL;			/* a plain column on the left */
	arr = (Node *) lsecond(saoe->args);
	if (!IsA(arr, Const) || ((Const *) arr)->constisnull)
		return NULL;			/* a literal, non-null array on the right */
	return saoe;
}

/* qsort_arg comparator over Datums using a type's btree compare proc */
typedef struct RepFdwDatumSortCtx
{
	FmgrInfo   *cmp;
	Oid			collation;
} RepFdwDatumSortCtx;

static int
repfdw_datum_cmp(const void *a, const void *b, void *arg)
{
	RepFdwDatumSortCtx *ctx = (RepFdwDatumSortCtx *) arg;
	Datum		x = *(const Datum *) a;
	Datum		y = *(const Datum *) b;

	return DatumGetInt32(FunctionCall2Coll(ctx->cmp, ctx->collation, x, y));
}

/*
 * repfdw_in_sorted_unique
 *		Extract the IN clause's array elements as a palloc'd Datum array, with
 *		NULLs dropped, sorted into the element type's btree key order, and
 *		de-duplicated.  Sorting gives each replica a contiguous key range (so its
 *		index scan reads a tight run of leaf pages); de-duplication guarantees a
 *		value lands in exactly one chunk, so no row is emitted by two replicas.
 *		If the type has no btree ordering, the values are left in list order
 *		(still correct, just without the locality benefit).
 */
static void
repfdw_in_sorted_unique(ScalarArrayOpExpr *saoe,
						Datum **elems_out, int *nelems_out,
						Oid *elemtype_out, int16 *elmlen_out,
						bool *elmbyval_out, char *elmalign_out)
{
	Const	   *arrconst = (Const *) lsecond(saoe->args);
	ArrayType  *arr = DatumGetArrayTypeP(arrconst->constvalue);
	Oid			elemtype = ARR_ELEMTYPE(arr);
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	Datum	   *out;
	int			nout = 0;
	TypeCacheEntry *tce;
	int			i;

	get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
	deconstruct_array(arr, elemtype, elmlen, elmbyval, elmalign,
					  &elems, &nulls, &nelems);

	out = (Datum *) palloc(sizeof(Datum) * Max(nelems, 1));
	for (i = 0; i < nelems; i++)
		if (!nulls[i])
			out[nout++] = elems[i];

	tce = lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO);
	if (nout > 1 && OidIsValid(tce->cmp_proc_finfo.fn_oid))
	{
		RepFdwDatumSortCtx ctx;
		int			w = 0;

		ctx.cmp = &tce->cmp_proc_finfo;
		ctx.collation = saoe->inputcollid;
		qsort_arg(out, nout, sizeof(Datum), repfdw_datum_cmp, &ctx);

		/* drop duplicates (adjacent after sorting); compare to last kept */
		for (i = 0; i < nout; i++)
		{
			if (w == 0 ||
				DatumGetInt32(FunctionCall2Coll(ctx.cmp, ctx.collation,
												out[w - 1], out[i])) != 0)
				out[w++] = out[i];
		}
		nout = w;
	}

	*elems_out = out;
	*nelems_out = nout;
	*elemtype_out = elemtype;
	*elmlen_out = elmlen;
	*elmbyval_out = elmbyval;
	*elmalign_out = elmalign;
}

/*
 * RepFdwInValueCount
 *		Number of distinct non-null values in the IN clause's array.
 */
int
RepFdwInValueCount(ScalarArrayOpExpr *saoe)
{
	Datum	   *elems;
	int			nelems;
	Oid			elemtype;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;

	repfdw_in_sorted_unique(saoe, &elems, &nelems, &elemtype,
							&elmlen, &elmbyval, &elmalign);
	return nelems;
}

/*
 * RepFdwDeparseInChunk
 *		Deparse "col = ANY (ARRAY[chunk])" for one contiguous chunk (part of
 *		nparts) of the sorted, de-duplicated value list.  The chunks partition
 *		the whole list, so their union reproduces the original IN exactly.
 */
char *
RepFdwDeparseInChunk(Oid foreigntableid, ScalarArrayOpExpr *saoe,
					 int part, int nparts)
{
	Const	   *arrconst = (Const *) lsecond(saoe->args);
	Datum	   *elems;
	int			nelems;
	Oid			elemtype;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	int			lo,
				hi;
	ArrayType  *subarr;
	Const	   *subconst;
	ScalarArrayOpExpr *sub;

	repfdw_in_sorted_unique(saoe, &elems, &nelems, &elemtype,
							&elmlen, &elmbyval, &elmalign);

	lo = (int) ((int64) nelems * part / nparts);
	hi = (int) ((int64) nelems * (part + 1) / nparts);

	subarr = construct_array(&elems[lo], hi - lo, elemtype,
							 elmlen, elmbyval, elmalign);
	subconst = makeConst(arrconst->consttype, -1, arrconst->constcollid,
						 -1, PointerGetDatum(subarr), false, false);

	/* copy the SAOE and swap in the chunk array; keeps op/collation/version
	 * fields intact */
	sub = copyObject(saoe);
	sub->args = list_make2(copyObject(linitial(saoe->args)), subconst);

	return RepFdwDeparseQuals(foreigntableid, list_make1(sub));
}
