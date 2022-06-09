from sqlalchemy import types, util
from sqlalchemy.engine import default
from sqlalchemy.sql.sqltypes import String

import pyiotdb
from .Compiler import (
    IoTDBCompiler,
    IoTDBTypeCompiler
)

TYPES_MAP = {
    "BOOLEAN": types.Boolean,
    "INT32": types.Integer,
    "INT64": types.BigInteger,
    "FLOAT": types.Float,
    "DOUBLE": types.DECIMAL,
    "TEXT": types.Text,
    "LONG": types.BigInteger
}


class IoTDBDialect(default.DefaultDialect):
    name = 'iotdb'
    driver = 'pyiotdb'
    statement_compiler = IoTDBCompiler
    type_compiler = IoTDBTypeCompiler
    convert_unicode = True

    supports_unicode_statements = True
    supports_unicode_binds = True
    description_encoding = None

    if hasattr(String, "RETURNS_UNICODE"):
        returns_unicode_strings = String.RETURNS_UNICODE

    else:
        def _check_unicode_returns(self, connection, additional_tests=None):
            return True

        _check_unicode_returns = _check_unicode_returns

    def _get_default_schema_name(self, connection):
        return "root"

    @util.memoized_property
    def _dialect_specific_select_one(self):
        return "SHOW VERSION"

    def create_connect_args(self, url):
        # inherits the docstring from interfaces.Dialect.create_connect_args
        opts = url.translate_connect_args()
        opts.update(url.query)
        opts.update({"sqlalchemy_mode": True})
        return [[], opts]

    @classmethod
    def dbapi(cls):
        return pyiotdb

    def has_schema(self, connection, schema):
        return schema in self.get_schema_names(connection)

    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name in self.get_table_names(connection, schema=schema)

    def get_schema_names(self, connection, **kw):
        cursor = connection.execute(
            "SHOW STORAGE GROUP"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_table_names(self, connection, schema=None, **kw):
        cursor = connection.execute(
            "SHOW DEVICES %s.**" % (schema or self.default_schema_name)
        )
        return [row[0].replace(schema + ".", "", 1) for row in cursor.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kw):
        cursor = connection.execute(
            "SHOW TIMESERIES %s.%s.*" % (schema, table_name)
        )
        columns = [self._general_time_column_info()]
        for row in cursor.fetchall():
            columns.append(self._create_column_info(row, schema, table_name))
        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def _general_time_column_info(self):
        return {
            "name": "Time",
            "type": self._resolve_type("LONG"),
            "nullable": True,
            "default": None
        }

    def _create_column_info(self, row, schema, table_name):
        return {
            "name": row[0].replace(schema + "." + table_name + ".", "", 1),
            "type": self._resolve_type(row[3]),
            # In Crate every column is nullable except PK
            # Primary Key Constraints are not nullable anyway, no matter what
            # we return here, so it's fine to return always `True`
            "nullable": True,
            "default": None
        }

    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, types.UserDefinedType)
