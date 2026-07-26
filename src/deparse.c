/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Remote SQL template generation for pg_replica_fanout_fdw: a column list
 *		  and the ctid-range WHERE clause for one replica's slice.  No
 *		  user-qual pushdown.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "pg_replica_fanout_fdw.h"

/*
 * RepFdwDeparseTemplate
 *		Build "SELECT <cols|NULL> FROM <schema>.<table>", with no WHERE
 *		clause yet -- RepFdwBuildBoundedSql appends the per-slice ctid
 *		bound.  retrieved_attrs is a list of ascending attnums (as produced
 *		by GetForeignPlan from the plan-time attrs_used bitmap).  A column's
 *		"column_name" option, if set, is used as the remote name instead of
 *		the local attribute name.
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
			List	   *coloptions = GetForeignColumnOptions(foreigntableid, attnum);
			char	   *remotename = NULL;
			ListCell   *lc2;

			foreach(lc2, coloptions)
			{
				DefElem    *def = (DefElem *) lfirst(lc2);

				if (strcmp(def->defname, "column_name") == 0)
				{
					remotename = defGetString(def);
					break;
				}
			}
			if (remotename == NULL)
				remotename = get_attname(foreigntableid, attnum, false);

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
 * RepFdwBuildBoundedSql
 *		Append the ctid-range WHERE clause for one replica's slice to a
 *		deparsed template, using $1/$2 bind-parameter placeholders for
 *		whichever bound(s) are present (NULL/NULL means the whole
 *		relation, i.e. no WHERE clause at all).  The parameter order here
 *		must match the values RepFdwStartQueries binds.
 */
char *
RepFdwBuildBoundedSql(const char *base_sql, const RepFdwCtidBound *bound)
{
	if (bound->lo == NULL && bound->hi == NULL)
		return pstrdup(base_sql);
	else if (bound->lo == NULL)
		return psprintf("%s WHERE ctid < $1", base_sql);
	else if (bound->hi == NULL)
		return psprintf("%s WHERE ctid >= $1", base_sql);
	else
		return psprintf("%s WHERE ctid >= $1 AND ctid < $2", base_sql);
}
