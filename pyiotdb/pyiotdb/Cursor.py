import logging

from iotdb.thrift.rpc.ttypes import TSExecuteStatementReq, TSExecuteBatchStatementReq
from iotdb.thrift.rpc.ttypes import TSExecuteStatementResp
from iotdb.utils.SessionDataSet import SessionDataSet
from thrift.transport import TTransport

from .Exceptions import ProgrammingError

logger = logging.getLogger("IoTDB")
import warnings


class Cursor(object):
    SUCCESS_CODE = 200

    def __init__(self, connection, client, session_id, statement_id, sqlalchemy_mode):
        self.__connection = connection
        self.__client = client
        self.__session_id = session_id
        self.__statement_id = statement_id
        self.__sqlalchemy_mode = sqlalchemy_mode
        self.__arraysize = 1
        self.__is_close = False
        self.__result = None
        self.__rows = None
        self.__rowcount = -1

    @property
    def description(self):
        if self.__is_close:
            return

        description = []

        col_names = self.__result["col_names"]
        col_types = self.__result["col_types"]

        for i in range(len(col_names)):
            description.append((col_names[i],
                                None if self.__sqlalchemy_mode is True else col_types[i].value,
                                None,
                                None,
                                None,
                                None,
                                col_names[i] == "Time"))
        return tuple(description)

    @property
    def arraysize(self):
        return self.__arraysize

    @arraysize.setter
    def arraysize(self, value):
        try:
            self.__arraysize = int(value)
        except TypeError:
            self.__arraysize = 1

    @property
    def rowcount(self):
        if self.__is_close or self.__result is None:
            return -1
        return len(self.__result.get("rows")) or -1

    def execute(self, operation, parameters=None):
        if self.__connection.is_close:
            raise ProgrammingError("Connection closed!")

        if self.__is_close:
            raise ProgrammingError("Cursor closed!")

        if parameters is None:
            sql = operation
        else:
            sql = operation % parameters

        if self.__sqlalchemy_mode is True:
            sql_seqs = []
            time_indexs = []
            seqs = sql.split("\n")
            for seq in seqs:
                if seq.find("FROM Time Index") >= 0:
                    time_indexs = [int(index) for index in seq.replace("FROM Time Index", "").split()]
                else:
                    sql_seqs.append(seq)
            sql = "\n".join(sql_seqs)

        request = TSExecuteStatementReq(self.__session_id, sql, self.__statement_id)
        try:
            resp: TSExecuteStatementResp = self.__client.executeStatement(request)
            if resp.status.code == Cursor.SUCCESS_CODE:
                if resp.columns is not None:
                    with SessionDataSet(
                            sql,
                            resp.columns,
                            resp.dataTypeList,
                            resp.columnNameIndexMap,
                            resp.queryId,
                            self.__client,
                            self.__statement_id,
                            self.__session_id,
                            resp.queryDataSet,
                            resp.ignoreTimeStamp,
                    ) as data_set:
                        data = data_set.todf()

                        if self.__sqlalchemy_mode is True and "Time" in data.columns:
                            time_column = data.columns[0]
                            time_column_value = data.Time
                            del data[time_column]
                            for index in time_indexs:
                                data.insert(index, time_column + str(index), time_column_value)

                        self.__result = {
                            "col_names": data.columns.tolist(),
                            "col_types": data_set.get_column_types(),
                            "rows": data.values.tolist()
                        }

                else:
                    self.__result = {
                        "col_names": None,
                        "col_types": None,
                        "rows": []
                    }
                self.__rows = iter(self.__result["rows"])
            else:
                raise ProgrammingError(resp.status.message)
            logger.debug(
                "execute statement {} message: {}".format(sql, resp.status.message)
            )
        except TTransport.TException as e:
            raise RuntimeError("execution of non-query statement fails because: ", e)

    def executemany(self, operation, seq_of_parameters=None):
        if self.__connection.is_close:
            raise ProgrammingError("Connection closed!")

        if self.__is_close:
            raise ProgrammingError("Cursor closed!")

        sqls = []
        if seq_of_parameters is None:
            sqls.append(operation)
        else:
            for parameters in seq_of_parameters:
                sqls.append(operation % parameters)

        rows = []

        for sql in sqls:
            self.execute(sql)
            rows.extend(self.__result["rows"])

        self.__result["rows"] = rows
        self.__rows = iter(self.__result["rows"])

    def fetchone(self):
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchmany(self, count=None):
        if count is None:
            count = self.__arraysize
        if count == 0:
            return self.fetchall()
        result = []
        for i in range(count):
            try:
                result.append(self.next())
            except StopIteration:
                pass
        return result

    def fetchall(self):
        result = []
        iterate = True
        while iterate:
            try:
                result.append(self.next())
            except StopIteration:
                iterate = False
        return result

    def next(self):
        self.__result: SessionDataSet
        if self.__result is None:
            raise ProgrammingError("No result available!")
        elif not self.__is_close:
            return next(self.__rows)
        else:
            raise ProgrammingError("Cursor closed!")

    __next__ = next

    def close(self):
        self.__is_close = True
        self.__result = None

    def __iter__(self):
        warnings.warn("DB-API extension cursor.__iter__() used")
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
