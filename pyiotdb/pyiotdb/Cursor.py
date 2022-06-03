import logging

from iotdb.thrift.rpc.ttypes import TSExecuteStatementReq
from iotdb.thrift.rpc.ttypes import TSExecuteStatementResp
from iotdb.utils.SessionDataSet import SessionDataSet
from thrift.transport import TTransport

from .Exceptions import ProgrammingError

logger = logging.getLogger("IoTDB")


class Cursor(object):
    SUCCESS_CODE = 200

    def __init__(self, connection, client, session_id, statement_id):
        self.__connection = connection
        self.__client = client
        self.__session_id = session_id
        self.__statement_id = statement_id
        self.__arraysize = 1
        self.__is_close = False
        self.__result = None
        self.__rows = None
        self.__rowcount = -1

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
        return self.__rowcount

    def execute(self, operation, parameters=None):
        if self.__connection.is_close:
            raise ProgrammingError("Connection closed!")

        if self.__is_close:
            raise ProgrammingError("Cursor closed!")

        if parameters is None:
            sql = operation
        else:
            sql = operation % parameters

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
                        self.__result = data_set.todf()
                        self.__rows = iter(self.__result.values.tolist())
            else:
                raise ProgrammingError(resp.status.message)
            logger.debug(
                "execute statement {} message: {}".format(sql, resp.status.message)
            )
        except TTransport.TException as e:
            raise RuntimeError("execution of non-query statement fails because: ", e)

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
