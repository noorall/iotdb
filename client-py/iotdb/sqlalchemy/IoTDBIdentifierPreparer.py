# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from sqlalchemy.sql.compiler import IdentifierPreparer


class IoTDBIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect, **kw):
        quote = "`"
        super(IoTDBIdentifierPreparer, self).__init__(
            dialect, initial_quote=quote, escape_quote=quote, **kw
        )

    def format_table(self, table, use_schema=True, name=None):
        """Prepare a quoted table and schema name."""

        if name is None:
            name = table.name
        result = name

        effective_schema = self.schema_for_object(table)

        if not self.omit_schema and use_schema and effective_schema:
            result = effective_schema + "." + result
        return result

    def format_column(
            self,
            column,
            use_table=False,
            name=None,
            table_name=None,
            use_schema=False,
    ):
        """Prepare a quoted column name."""

        if name is None:
            name = column.name
        if not getattr(column, "is_literal", False):
            if use_table:
                return (
                        table_name
                        + "."
                        + name
                )
            else:
                return name
        else:
            # literal textual elements get stuck into ColumnClause a lot,
            # which shouldn't get quoted

            if use_table:
                return (
                        table_name
                        + "."
                        + name
                )
            else:
                return name
