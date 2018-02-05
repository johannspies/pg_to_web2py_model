#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Requires at least python3.6
This script expect the following to be in a model where this output will be used:

from gluon.dal import SQLCustomType
tsv = SQLCustomType(
    type ='text',
    native='tsvector' )
citext = SQLCustomType(
    type ='text',
    native='citext' )
uuidtype = SQLCustomType(
    type = 'text',
    native = 'uuid'
)
boolean = SQLCustomType(
    type = 'boolean',
    native = 'boolean'
)

"""
import argparse
import psycopg2 as pg2
import psycopg2.pool as pgpool

import re


""" The folowing constant was taken from 
web2py/gluon/packages/dal/pydal/contrib/reserved_sql_keywords.py"""
POSTGRESQL_NONRESERVED = set((
    'A',
    'ABORT',
    'ABS',
    'ABSENT',
    'ABSOLUTE',
    'ACCESS',
    'ACCORDING',
    'ACTION',
    'ADA',
    'ADD',
    'ADMIN',
    'AFTER',
    'AGGREGATE',
    'ALIAS',
    'ALLOCATE',
    'ALSO',
    'ALTER',
    'ALWAYS',
    'ARE',
    'ARRAY_AGG',
    'ASENSITIVE',
    'ASSERTION',
    'ASSIGNMENT',
    'AT',
    'ATOMIC',
    'ATTRIBUTE',
    'ATTRIBUTES',
    'AVG',
    'BACKWARD',
    'BASE64',
    'BEFORE',
    'BEGIN',
    'BERNOULLI',
    'BIT_LENGTH',
    'BITVAR',
    'BLOB',
    'BOM',
    'BREADTH',
    'BY',
    'C',
    'CACHE',
    'CALL',
    'CALLED',
    'CARDINALITY',
    'CASCADE',
    'CASCADED',
    'CATALOG',
    'CATALOG_NAME',
    'CEIL',
    'CEILING',
    'CHAIN',
    'CHAR_LENGTH',
    'CHARACTER_LENGTH',
    'CHARACTER_SET_CATALOG',
    'CHARACTER_SET_NAME',
    'CHARACTER_SET_SCHEMA',
    'CHARACTERISTICS',
    'CHARACTERS',
    'CHECKED',
    'CHECKPOINT',
    'CLASS',
    'CLASS_ORIGIN',
    'CLOB',
    'CLOSE',
    'CLUSTER',
    'COBOL',
    'COLLATION',
    'COLLATION_CATALOG',
    'COLLATION_NAME',
    'COLLATION_SCHEMA',
    'COLLECT',
    'COLUMN_NAME',
    'COLUMNS',
    'COMMAND_FUNCTION',
    'COMMAND_FUNCTION_CODE',
    'COMMENT',
    'COMMIT',
    'COMMITTED',
    'COMPLETION',
    'CONCURRENTLY',
    'CONDITION',
    'CONDITION_NUMBER',
    'CONFIGURATION',
    'CONNECT',
    'CONNECTION',
    'CONNECTION_NAME',
    'CONSTRAINT_CATALOG',
    'CONSTRAINT_NAME',
    'CONSTRAINT_SCHEMA',
    'CONSTRAINTS',
    'CONSTRUCTOR',
    'CONTAINS',
    'CONTENT',
    'CONTINUE',
    'CONVERSION',
    'CONVERT',
    'COPY',
    'CORR',
    'CORRESPONDING',
    'COST',
    'COUNT',
    'COVAR_POP',
    'COVAR_SAMP',
    'CREATEDB',
    'CREATEROLE',
    'CREATEUSER',
    'CSV',
    'CUBE',
    'CUME_DIST',
    'CURRENT',
    'CURRENT_DEFAULT_TRANSFORM_GROUP',
    'CURRENT_PATH',
    'CURRENT_TRANSFORM_GROUP_FOR_TYPE',
    'CURSOR',
    'CURSOR_NAME',
    'CYCLE',
    'DATA',
    'DATABASE',
    'DATE',
    'DATETIME_INTERVAL_CODE',
    'DATETIME_INTERVAL_PRECISION',
    'DAY',
    'DEALLOCATE',
    'DECLARE',
    'DEFAULTS',
    'DEFERRED',
    'DEFINED',
    'DEFINER',
    'DEGREE',
    'DELETE',
    'DELIMITER',
    'DELIMITERS',
    'DENSE_RANK',
    'DEPTH',
    'DEREF',
    'DERIVED',
    'DESCRIBE',
    'DESCRIPTOR',
    'DESTROY',
    'DESTRUCTOR',
    'DETERMINISTIC',
    'DIAGNOSTICS',
    'DICTIONARY',
    'DISABLE',
    'DISCARD',
    'DISCONNECT',
    'DISPATCH',
    'DOCUMENT',
    'DOMAIN',
    'DOUBLE',
    'DROP',
    'DYNAMIC',
    'DYNAMIC_FUNCTION',
    'DYNAMIC_FUNCTION_CODE',
    'EACH',
    'ELEMENT',
    'EMPTY',
    'ENABLE',
    'ENCODING',
    'ENCRYPTED',
    'END-EXEC',
    'ENUM',
    'EQUALS',
    'ESCAPE',
    'EVERY',
    'EXCEPTION',
    'EXCLUDE',
    'EXCLUDING',
    'EXCLUSIVE',
    'EXEC',
    'EXECUTE',
    'EXISTING',
    'EXP',
    'EXPLAIN',
    'EXTERNAL',
    'FAMILY',
    'FILTER',
    'FINAL',
    'FIRST',
    'FIRST_VALUE',
    'FLAG',
    'FLOOR',
    'FOLLOWING',
    'FORCE',
    'FORTRAN',
    'FORWARD',
    'FOUND',
    'FREE',
    'FUNCTION',
    'FUSION',
    'G',
    'GENERAL',
    'GENERATED',
    'GET',
    'GLOBAL',
    'GO',
    'GOTO',
    'GRANTED',
    'GROUPING',
    'HANDLER',
    'HEADER',
    'HEX',
    'HIERARCHY',
    'HOLD',
    'HOST',
    'HOUR',
    #    'ID',
    'IDENTITY',
    'IF',
    'IGNORE',
    'IMMEDIATE',
    'IMMUTABLE',
    'IMPLEMENTATION',
    'IMPLICIT',
    'INCLUDING',
    'INCREMENT',
    'INDENT',
    'INDEX',
    'INDEXES',
    'INDICATOR',
    'INFIX',
    'INHERIT',
    'INHERITS',
    'INITIALIZE',
    'INPUT',
    'INSENSITIVE',
    'INSERT',
    'INSTANCE',
    'INSTANTIABLE',
    'INSTEAD',
    'INTERSECTION',
    'INVOKER',
    'ISOLATION',
    'ITERATE',
    'K',
    'KEY',
    'KEY_MEMBER',
    'KEY_TYPE',
    'LAG',
    'LANCOMPILER',
    'LANGUAGE',
    'LARGE',
    'LAST',
    'LAST_VALUE',
    'LATERAL',
    'LC_COLLATE',
    'LC_CTYPE',
    'LEAD',
    'LENGTH',
    'LESS',
    'LEVEL',
    'LIKE_REGEX',
    'LISTEN',
    'LN',
    'LOAD',
    'LOCAL',
    'LOCATION',
    'LOCATOR',
    'LOCK',
    'LOGIN',
    'LOWER',
    'M',
    'MAP',
    'MAPPING',
    'MATCH',
    'MATCHED',
    'MAX',
    'MAX_CARDINALITY',
    'MAXVALUE',
    'MEMBER',
    'MERGE',
    'MESSAGE_LENGTH',
    'MESSAGE_OCTET_LENGTH',
    'MESSAGE_TEXT',
    'METHOD',
    'MIN',
    'MINUTE',
    'MINVALUE',
    'MOD',
    'MODE',
    'MODIFIES',
    'MODIFY',
    'MODULE',
    'MONTH',
    'MORE',
    'MOVE',
    'MULTISET',
    'MUMPS',
    #    'NAME',
    'NAMES',
    'NAMESPACE',
    'NCLOB',
    'NESTING',
    'NEXT',
    'NFC',
    'NFD',
    'NFKC',
    'NFKD',
    'NIL',
    'NO',
    'NOCREATEDB',
    'NOCREATEROLE',
    'NOCREATEUSER',
    'NOINHERIT',
    'NOLOGIN',
    'NORMALIZE',
    'NORMALIZED',
    'NOSUPERUSER',
    'NOTHING',
    'NOTIFY',
    'NOWAIT',
    'NTH_VALUE',
    'NTILE',
    'NULLABLE',
    'NULLS',
    'NUMBER',
    'OBJECT',
    'OCCURRENCES_REGEX',
    'OCTET_LENGTH',
    'OCTETS',
    'OF',
    'OIDS',
    'OPEN',
    'OPERATION',
    'OPERATOR',
    'OPTION',
    'OPTIONS',
    'ORDERING',
    'ORDINALITY',
    'OTHERS',
    'OUTPUT',
    'OVER',
    'OVERRIDING',
    'OWNED',
    'OWNER',
    'P',
    'PAD',
    'PARAMETER',
    'PARAMETER_MODE',
    'PARAMETER_NAME',
    'PARAMETER_ORDINAL_POSITION',
    'PARAMETER_SPECIFIC_CATALOG',
    'PARAMETER_SPECIFIC_NAME',
    'PARAMETER_SPECIFIC_SCHEMA',
    'PARAMETERS',
    'PARSER',
    'PARTIAL',
    'PARTITION',
    'PASCAL',
    'PASSING',
    #    'PASSWORD',
    'PATH',
    'PERCENT_RANK',
    'PERCENTILE_CONT',
    'PERCENTILE_DISC',
    'PLANS',
    'PLI',
    'POSITION_REGEX',
    'POSTFIX',
    'POWER',
    'PRECEDING',
    'PREFIX',
    'PREORDER',
    'PREPARE',
    'PREPARED',
    'PRESERVE',
    'PRIOR',
    'PRIVILEGES',
    'PROCEDURAL',
    'PROCEDURE',
    'PUBLIC',
    'QUOTE',
    'RANGE',
    'RANK',
    'READ',
    'READS',
    'REASSIGN',
    'RECHECK',
    'RECURSIVE',
    'REF',
    'REFERENCING',
    'REGR_AVGX',
    'REGR_AVGY',
    'REGR_COUNT',
    'REGR_INTERCEPT',
    'REGR_R2',
    'REGR_SLOPE',
    'REGR_SXX',
    'REGR_SXY',
    'REGR_SYY',
    'REINDEX',
    'RELATIVE',
    'RELEASE',
    'RENAME',
    'REPEATABLE',
    'REPLACE',
    'REPLICA',
    'RESET',
    'RESPECT',
    'RESTART',
    'RESTRICT',
    'RESULT',
    'RETURN',
    'RETURNED_CARDINALITY',
    'RETURNED_LENGTH',
    'RETURNED_OCTET_LENGTH',
    'RETURNED_SQLSTATE',
    'RETURNS',
    'REVOKE',
    #    'ROLE',
    'ROLLBACK',
    'ROLLUP',
    'ROUTINE',
    'ROUTINE_CATALOG',
    'ROUTINE_NAME',
    'ROUTINE_SCHEMA',
    'ROW_COUNT',
    'ROW_NUMBER',
    'ROWS',
    'RULE',
    'SAVEPOINT',
    'SCALE',
    'SCHEMA',
    'SCHEMA_NAME',
    'SCOPE',
    'SCOPE_CATALOG',
    'SCOPE_NAME',
    'SCOPE_SCHEMA',
    'SCROLL',
    'SEARCH',
    'SECOND',
    'SECTION',
    'SECURITY',
    'SELF',
    'SENSITIVE',
    'SEQUENCE',
    'SERIALIZABLE',
    'SERVER',
    'SERVER_NAME',
    'SESSION',
    'SET',
    'SETS',
    'SHARE',
    'SHOW',
    'SIMPLE',
    'SIZE',
    'SOURCE',
    'SPACE',
    'SPECIFIC',
    'SPECIFIC_NAME',
    'SPECIFICTYPE',
    'SQL',
    'SQLCODE',
    'SQLERROR',
    'SQLEXCEPTION',
    'SQLSTATE',
    'SQLWARNING',
    'SQRT',
    'STABLE',
    'STANDALONE',
    'START',
    'STATE',
    'STATEMENT',
    'STATIC',
    'STATISTICS',
    'STDDEV_POP',
    'STDDEV_SAMP',
    'STDIN',
    'STDOUT',
    'STORAGE',
    'STRICT',
    'STRIP',
    'STRUCTURE',
    'STYLE',
    'SUBCLASS_ORIGIN',
    'SUBLIST',
    'SUBMULTISET',
    'SUBSTRING_REGEX',
    'SUM',
    'SUPERUSER',
    'SYSID',
    'SYSTEM',
    'SYSTEM_USER',
    'T',
    #    'TABLE_NAME',
    'TABLESAMPLE',
    'TABLESPACE',
    'TEMP',
    'TEMPLATE',
    'TEMPORARY',
    'TERMINATE',
    'TEXT',
    'THAN',
    'TIES',
    'TIMEZONE_HOUR',
    'TIMEZONE_MINUTE',
    'TOP_LEVEL_COUNT',
    'TRANSACTION',
    'TRANSACTION_ACTIVE',
    'TRANSACTIONS_COMMITTED',
    'TRANSACTIONS_ROLLED_BACK',
    'TRANSFORM',
    'TRANSFORMS',
    'TRANSLATE',
    'TRANSLATE_REGEX',
    'TRANSLATION',
    'TRIGGER',
    'TRIGGER_CATALOG',
    'TRIGGER_NAME',
    'TRIGGER_SCHEMA',
    'TRIM_ARRAY',
    'TRUNCATE',
    'TRUSTED',
    'TYPE',
    'UESCAPE',
    'UNBOUNDED',
    'UNCOMMITTED',
    'UNDER',
    'UNENCRYPTED',
    'UNKNOWN',
    'UNLISTEN',
    'UNNAMED',
    'UNNEST',
    'UNTIL',
    'UNTYPED',
    'UPDATE',
    'UPPER',
    'URI',
    'USAGE',
    'USER_DEFINED_TYPE_CATALOG',
    'USER_DEFINED_TYPE_CODE',
    'USER_DEFINED_TYPE_NAME',
    'USER_DEFINED_TYPE_SCHEMA',
    'VACUUM',
    'VALID',
    'VALIDATOR',
    'VALUE',
    'VAR_POP',
    'VAR_SAMP',
    'VARBINARY',
    'VARIABLE',
    'VARYING',
    'VERSION',
    'VIEW',
    'VOLATILE',
    'WHENEVER',
    'WHITESPACE',
    'WIDTH_BUCKET',
    'WINDOW',
    'WITHIN',
    'WITHOUT',
    'WORK',
    'WRAPPER',
    'WRITE',
    'XML',
    'XMLAGG',
    'XMLBINARY',
    'XMLCAST',
    'XMLCOMMENT',
    'XMLDECLARATION',
    'XMLDOCUMENT',
    'XMLEXISTS',
    'XMLITERATE',
    'XMLNAMESPACES',
    'XMLQUERY',
    'XMLSCHEMA',
    'XMLTABLE',
    'XMLTEXT',
    'XMLVALIDATE',
    'YEAR',
    'YES',
    'ZONE',
))


def make_pool(dbname, port):
    """Create psycopg2  connection pool to database"""
    pool = pgpool.SimpleConnectionPool(1, 50,
                                       database=dbname,
                                       host='localhost',
                                       user='xxxx',
                                       password='xxxxx',
                                       port=port)
                

    return pool


def get_con(pool):
    con = pool.getconn()
    cur = con.cursor()
    return con, cur


def dictanswer(result, cur):
    """combine the column names and tuple values in a list of dicts"""
    if not result:
        return None
    x = [i.name for i in cur.description]
    if type(result) == tuple:
        result = [result]
    assert len(x) == len(result[0])
    ll = []
    for i in result:
        ll.append({d[0]: d[1] for d in list(zip((x), list(i)))})
    return ll


def runquery(query):
    """Execute query and release con and return the result and success"""
    r = None
    # try:
    #     pool.putconn(con)
    # except:
    #     pass
    con, cur = get_con(pool)
    #   cur.execute("set enable_seqscan = false;")
    #   cur.execute("set random_page_cost = 1;")
    try:
        if query.lower().startswith('insert'):
            pass
        cur.execute(query)
    except pg2.Error as e:
        print("""Could not complete query "{}"
                 Error message:{}""".format(query, e.pgerror))
        print(cur.statusmessage)
        pool.putconn(con, close=True)
        exit(1)
    except:
        print(traceback.format_exc())
        pool.putconn(con, close=True)
        exit(1)

    feedback = cur.statusmessage
    if feedback == 'SELECT 1':
        answer = cur.fetchone()
    elif feedback.startswith('SELECT'):
        answer = cur.fetchall()
    elif (feedback == ('INSERT 0')
          or
          feedback == ('DELETE 0')
          or
          feedback == ('UPDATE 0')):
        pool.putconn(con)
        return None, feedback
    elif (feedback.startswith('INSERT')
          or
          feedback.startswith('DELETE')
          or
          feedback.startswith('UPDATE')):
        con.commit()
        pool.putconn(con)
        return None, feedback
    else:
        pool.putconn(con)
        return None, feedback
    result = dictanswer(answer, cur)
    pool.putconn(con, close=True)
    return result, feedback


def tablenames(schema, pool):
    """Get the table names for this schema and return an iterator
       This function must be called like this:

       ts =  loop.run_until_complete(tables('wos_2017_1'))
       t = [x['table_name'] for x in ts]
       where t will be a list of plain table names (strings) in this case
    """
    query = """
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            lower(table_schema) = lower('{}')
            AND 
            lower(table_name) not in ('auth_cas',
                                      'auth_event', 
                                      'auth_group',
                                      'auth_membership',
                                      'auth_permission',
                                      'auth_user'
            )
            AND table_type = 'BASE TABLE' order by 1
                """.format(schema)
    result, feedback = runquery(query)
    if result:
        return result
    else:
        print("""No tablenames found. Statusmessage: {}""".format(feedback))
        exit(1)


def fields(schema, table):
    query = """
            SELECT
                column_name,
                data_type
            FROM
                information_schema.columns
            WHERE
                table_name = '{0}'
                AND table_schema = '{1}'
            ORDER BY
                ordinal_position
            """.format(schema, table)
    result, feedback = runquery(query)
    return result


def fieldtup(f):
    """
    :param f:
    :return: {table_name: list of tuples with (column_name,data_type)
    """
    l = []
    for k in f:
        for x in f[k]:
            l.append((x['column_name'], x['data_type']))
    return k, l


def tablefields(schema):
    """
    :param schema:
    :return: dictionary with tablenames as keys and a list of tuples with (fieldname,fieldtype) for the table
    as value
    """
    result = tablenames(schema, pool)
    t = [x['table_name'] for x in result]
    fs = [{x: fields(x, schema)} for x in t]
    tfs = {}
    for f in fs:
        table, fls = fieldtup(f)
        tfs[table] = fls
    return tfs


def translate_fieldtype(ft):
    tr = {"text": "text",
          "regproc": "text",
          "ARRAY": "text",
          "smallint": "integer",
          "abstime": "time",
          "interval": "text",
          "date": "date",
          "name": "text",
          "anyarray": "text",
          "numeric": "double",
          "integer": "integer",
          "boolean": "boolean",
          "USER-DEFINED": "citext",
          "bigint": "bigint",
          "character varying": "string",
          "timestamp without time zone": "datetime",
          "real": "double",
          "xml": "text",
          "double precision": "double",
          "uuid": "uuidtype",
          "character": "string",
          "tsvector": "tsv",
          "bytea": "text",
          "timestamp with time zone": "datetime",
          "char": "string",
          "inet": "string"}
    if ft in tr:
        return tr[ft]
    else:
        return "text"


def model(t, fld):
    """Where t is the table name and fld a list with the table definitions containing
       the fieldname and fieldtype
       ms: model string
       fn: fieldname
       ft: fieldtype
       """
    tn = t
    if tn.upper() in POSTGRESQL_NONRESERVED:
        msb = f"db.define_table('c_{t}',\n"
    else:
        msb = f"db.define_table('{tn}',\n"
    for i in fld:
        fn = i[0]
        ft = translate_fieldtype(i[1])

        if fn == "id":
            pass
        elif fn.upper() in POSTGRESQL_NONRESERVED:
            fn = f'c_{fn}'
            msb = msb + f"\tField('{fn}', '{ft}' , rname = '{i[0]}'),\n"
        else:
            msb = msb + f"\tField('{fn}', '{ft}'),\n"
    msb = msb + f"""\tmigrate = False,\n\trname = '{schema}.{t}')"""
    # print(msb)
    return msb


def main():
    tfs = tablefields(schema)
    l = open(outputfile, "a")
    for i in tfs.keys():
        t = tfs[i]
        l.write(model(i, t))
        l.write("\n")
        # print(model(i, t))
    l.close()
    return


if __name__ == '__main__':
    """ For testing purposes define the commandline parameters like this:
    dbname = 'wos'
    schema = 'wos_2017_2'
    port = 63334
    main()
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dbname", required=True,
                        help="Database to connect to")
    parser.add_argument("-S", "--schema", required=False,
                        help="Schema to use. Default: 'public'")
    parser.add_argument("-p", "--port", required=False,
                        help="Schema to use. Default: 5432")
    parser.add_argument("-o", "--outputfile", required=False,
                        help="Outputfile -  default: '/tmp/models_created.py'")
    args = parser.parse_args()
    if args.schema:
        schema = args.schema
    else:
        schema = 'Public'
    if args.port:
        port = args.port
    else:
        port = 5432
            
    dbname = args.dbname

    if args.outputfile:
        outputfile = args.outputfile
    else:
        outputfile = '/tmp/models_created.py'
####for debugging ##############
    # dbname = 'js'
    # schema = 'wos_2017_2'
    # port = 63334
    # ###################################
    pool = make_pool(dbname, port)
    main()
