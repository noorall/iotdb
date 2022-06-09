from sqlalchemy.sql import compiler


class IoTDBCompiler(compiler.SQLCompiler):

    def _compose_select_body(
            self, text, select, inner_columns, froms, byfrom, kwargs
    ):
        colunms = []
        for i in range(len(inner_columns)):
            if "Time" in inner_columns[i].replace('"', "").split():
                colunms.append(str(i))
        inner_columns = filter(lambda x: "Time" not in x.replace('"', "").split(), inner_columns)
        text += ", ".join(inner_columns)

        if colunms:
            text += " \nFROM Time Index "
            text += " ".join(colunms)

        if froms:
            text += " \nFROM "

            if select._hints:
                text += ", ".join(
                    [
                        f._compiler_dispatch(
                            self, asfrom=True, fromhints=byfrom, **kwargs
                        )
                        for f in froms
                    ]
                )
            else:
                text += ", ".join(
                    [
                        f._compiler_dispatch(self, asfrom=True, **kwargs).replace('"', '')
                        for f in froms
                    ]
                )
        else:
            text += self.default_from()

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nWHERE " + t

        if select._group_by_clause.clauses:
            text += self.group_by_clause(select, **kwargs)

        if select._having is not None:
            t = select._having._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nHAVING " + t

        if select._order_by_clause.clauses:
            text += self.order_by_clause(select, **kwargs)

        if (
                select._limit_clause is not None
                or select._offset_clause is not None
        ):
            text += self.limit_clause(select, **kwargs)

        if select._for_update_arg is not None:
            text += self.for_update_clause(select, **kwargs)

        return text

    def order_by_clause(self, select, **kw):
        """allow dialects to customize how ORDER BY is rendered."""

        order_by = select._order_by_clause._compiler_dispatch(self, **kw)
        if "Time" in order_by:
            return " ORDER BY " + order_by
        else:
            return ""

    def group_by_clause(self, select, **kw):
        """allow dialects to customize how GROUP BY is rendered."""
        return ""


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
        return "LONG"

    def visit_TIMESTAMP(self, type_, **kw):
        return "LONG"

    def visit_text(self, type_, **kw):
        return "TEXT"
