/*-------------------------------------------------------------------------
 *
 * option.c
 *		  Validator and option parsing for pg_replica_fdw
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "pg_replica_fdw.h"

PG_FUNCTION_INFO_V1(pg_replica_fdw_validator);

typedef struct RepFdwValidOption
{
	const char *keyword;
	Oid			optcontext;
} RepFdwValidOption;

static const RepFdwValidOption valid_options[] = {
	{"replicas", ForeignServerRelationId},
	{"dbname", ForeignServerRelationId},
	{"fetch_size", ForeignServerRelationId},
	{"connect_timeout", ForeignServerRelationId},
	{"application_name", ForeignServerRelationId},
	{"consistency", ForeignServerRelationId},
	{"user", UserMappingRelationId},
	{"password", UserMappingRelationId},
	{"table_name", ForeignTableRelationId},
	{"schema_name", ForeignTableRelationId},
	{"min_blocks_per_slice", ForeignTableRelationId},
	{"fetch_size", ForeignTableRelationId},
	{"column_name", AttributeRelationId},
	{NULL, InvalidOid}
};

static bool
is_valid_option(const char *keyword, Oid context)
{
	const RepFdwValidOption *opt;

	for (opt = valid_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
			return true;
	}
	return false;
}

/*
 * parse_positive_int_option
 *		FDW options are always passed as strings (DefElem->arg is a
 *		T_String), never bare numeric literals, so defGetInt32() doesn't
 *		apply here -- parse the string by hand instead.
 */
static int
parse_positive_int_option(DefElem *def)
{
	char	   *value = defGetString(def);
	int			ival;

	if (!parse_int(value, &ival, 0, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for integer option \"%s\": %s",
						def->defname, value)));
	if (ival <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" must be an integer value greater than zero",
						def->defname)));
	return ival;
}

Datum
pg_replica_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;
	bool		have_replicas = false;

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			const RepFdwValidOption *opt;
			StringInfoData buf;

			initStringInfo(&buf);
			for (opt = valid_options; opt->keyword; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s",
									  (buf.len > 0) ? ", " : "", opt->keyword);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 buf.len > 0 ?
					 errhint("Valid options in this context: %s", buf.data) :
					 errhint("There are no valid options in this context.")));
		}

		if (strcmp(def->defname, "replicas") == 0)
		{
			/* validate syntax (result is discarded, this is just a check) */
			(void) RepFdwParseReplicas(defGetString(def));
			have_replicas = true;
		}
		else if (strcmp(def->defname, "consistency") == 0)
		{
			char	   *value = defGetString(def);

			if (strcmp(value, "none") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid value for option \"consistency\": \"%s\"",
								value),
						 errhint("consistency='lsn' not supported; only 'none' is valid.")));
		}
		else if (strcmp(def->defname, "fetch_size") == 0 ||
				 strcmp(def->defname, "connect_timeout") == 0 ||
				 strcmp(def->defname, "min_blocks_per_slice") == 0)
		{
			(void) parse_positive_int_option(def);
		}
	}

	/*
	 * The validator receives the full set of options that will end up on
	 * the object, so this is a reliable place to enforce "replicas" being
	 * present on a foreign server.
	 */
	if (catalog == ForeignServerRelationId && !have_replicas)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("option \"replicas\" is required for a pg_replica_fdw server")));

	PG_RETURN_VOID();
}

/*
 * RepFdwParseReplicas
 *		Parse a comma-separated 'host[:port],host[:port],...' string into a
 *		List of RepHostPort *.  Order is preserved (order = replica index).
 */
List *
RepFdwParseReplicas(const char *replicas_str)
{
	List	   *result = NIL;
	char	   *str;
	char	   *cur;

	if (replicas_str == NULL || replicas_str[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid value for option \"replicas\""),
				 errdetail("Must be a non-empty comma-separated list of host[:port].")));

	str = pstrdup(replicas_str);
	cur = str;
	for (;;)
	{
		char	   *comma = strchr(cur, ',');
		char	   *tok = cur;
		char	   *end;
		char	   *colon;
		RepHostPort *hp;

		if (comma != NULL)
			*comma = '\0';

		while (*tok == ' ' || *tok == '\t')
			tok++;
		end = tok + strlen(tok);
		while (end > tok && (end[-1] == ' ' || end[-1] == '\t'))
			*--end = '\0';

		if (*tok == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid value for option \"replicas\""),
					 errdetail("Contains an empty host entry.")));

		hp = palloc_object(RepHostPort);
		colon = strrchr(tok, ':');
		if (colon != NULL)
		{
			int			port;

			*colon = '\0';
			if (!parse_int(colon + 1, &port, 0, NULL) || port <= 0 || port > 65535)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid port in \"replicas\" entry \"%s:%s\"",
								tok, colon + 1)));
			hp->host = pstrdup(tok);
			hp->port = port;
		}
		else
		{
			hp->host = pstrdup(tok);
			hp->port = 5432;
		}
		result = lappend(result, hp);

		if (comma == NULL)
			break;
		cur = comma + 1;
	}

	pfree(str);
	return result;
}

/*
 * RepFdwGetOptions
 *		Collect all options for a foreign table into a freshly-palloc'd
 *		RepFdwOptions.
 */
void
RepFdwGetOptions(Oid foreigntableid, RepFdwOptions **opts_out)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);
	RepFdwOptions *opts = palloc0_object(RepFdwOptions);
	ListCell   *lc;
	bool		have_replicas = false;

	opts->fetch_size = 1000;
	opts->connect_timeout = 5;
	opts->application_name = "pg_replica_fdw";
	opts->min_blocks_per_slice = 128;

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "replicas") == 0)
		{
			opts->replicas = RepFdwParseReplicas(defGetString(def));
			have_replicas = true;
		}
		else if (strcmp(def->defname, "dbname") == 0)
			opts->dbname = defGetString(def);
		else if (strcmp(def->defname, "fetch_size") == 0)
			opts->fetch_size = parse_positive_int_option(def);
		else if (strcmp(def->defname, "connect_timeout") == 0)
			opts->connect_timeout = parse_positive_int_option(def);
		else if (strcmp(def->defname, "application_name") == 0)
			opts->application_name = defGetString(def);
		/* "consistency" is accepted but has no runtime effect (only 'none' is supported) */
	}

	if (!have_replicas)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("foreign server \"%s\" is missing required option \"replicas\"",
						server->servername)));

	if (opts->dbname == NULL)
		opts->dbname = get_database_name(MyDatabaseId);

	opts->schema_name = get_namespace_name(get_rel_namespace(foreigntableid));
	opts->table_name = get_rel_name(foreigntableid);

	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table_name") == 0)
			opts->table_name = defGetString(def);
		else if (strcmp(def->defname, "schema_name") == 0)
			opts->schema_name = defGetString(def);
		else if (strcmp(def->defname, "min_blocks_per_slice") == 0)
			opts->min_blocks_per_slice = parse_positive_int_option(def);
		else if (strcmp(def->defname, "fetch_size") == 0)
			opts->fetch_size = parse_positive_int_option(def);
	}

	*opts_out = opts;
}
