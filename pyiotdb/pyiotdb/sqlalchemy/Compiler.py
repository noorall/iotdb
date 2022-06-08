from sqlalchemy.sql import compiler


class IoTDBCompiler(compiler.SQLCompiler):
    pass


class IoTDBTypeCompiler(compiler.GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_NUMERIC(self, type_, **kw):
        return "LONG"

    def visit_DECIMAL(self, type_, **kw):
        return "DOUBLE"

    def visit_INTEGER(self, type_, **kw):
        return "INT32"

    def visit_SMALLINT(self, type_, **kw):
        return "INT32"

    def visit_BIGINT(self, type_, **kw):
        return "INT64"

    def visit_TIMESTAMP(self, type_, **kw):
        return "LONG"

    def visit_text(self, type_, **kw):
        return "TEXT"
