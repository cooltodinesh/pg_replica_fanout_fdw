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
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

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
 *		must match the values RepFdwStartQueries binds.
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
			{
				OpExpr	   *oe = (OpExpr *) node;

				if (list_length(oe->args) != 1 && list_length(oe->args) != 2)
					return false;
				return repfdw_foreign_expr_walker((Node *) oe->args, root,
												  baserel);
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *sae = (ScalarArrayOpExpr *) node;

				if (list_length(sae->args) != 2)
					return false;
				return repfdw_foreign_expr_walker((Node *) sae->args, root,
												  baserel);
			}

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
