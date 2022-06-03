import logging
import time

from .Cursor import Cursor
from .Exceptions import ProgrammingError, ConnectionError, NotSupportedError

from iotdb.utils.SessionDataSet import SessionDataSet

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport

from .thrift.rpc.TSIService import (
    Client,
    TSOpenSessionReq,
    TSCloseSessionReq
)
from .thrift.rpc.ttypes import (
    TSProtocolVersion,
    TSSetTimeZoneReq
)

logger = logging.getLogger("IoTDB")


class Connection(object):
    DEFAULT_FETCH_SIZE = 10000
    DEFAULT_USER = "root"
    DEFAULT_PASSWORD = "root"
    DEFAULT_ZONE_ID = time.strftime("%z")

    def __init__(
        self,
        host,
        port,
        user=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        fetch_size=DEFAULT_FETCH_SIZE,
        zone_id=DEFAULT_ZONE_ID,
    ):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__fetch_size = fetch_size
        self.__is_close = True
        self.__transport = None
        self.__client = None
        self.protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
        self.__session_id = None
        self.__statement_id = None
        self.__zone_id = zone_id
        self.open(False)
        if self.__is_close:
            raise ConnectionError("Failed to open session!")

    def open(self, enable_rpc_compression):
        if not self.__is_close:
            return
        self.__transport = TTransport.TFramedTransport(
            TSocket.TSocket(self.__host, self.__port)
        )

        if not self.__transport.isOpen():
            try:
                self.__transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        if enable_rpc_compression:
            self.__client = Client(TCompactProtocol.TCompactProtocol(self.__transport))
        else:
            self.__client = Client(TBinaryProtocol.TBinaryProtocol(self.__transport))

        open_req = TSOpenSessionReq(
            client_protocol=self.protocol_version,
            username=self.__user,
            password=self.__password,
            zoneId=self.__zone_id,
            configuration={"version": "V_0_13"},
        )

        try:
            open_resp = self.__client.openSession(open_req)

            if self.protocol_version != open_resp.serverProtocolVersion:
                logger.exception(
                    "Protocol differ, Client version is {}, but Server version is {}".format(
                        self.protocol_version, open_resp.serverProtocolVersion
                    )
                )
                # version is less than 0.10
                if open_resp.serverProtocolVersion == 0:
                    raise TTransport.TException(message="Protocol not supported.")

            self.__session_id = open_resp.sessionId
            self.__statement_id = self.__client.requestStatementId(self.__session_id)

        except Exception as e:
            self.__transport.close()
            logger.exception("session closed because: ", exc_info=e)

        if self.__zone_id is not None:
            self.set_time_zone(self.__zone_id)
        else:
            self.__zone_id = self.get_time_zone()

        self.__is_close = False

    def set_time_zone(self, zone_id):
        request = TSSetTimeZoneReq(self.__session_id, zone_id)
        try:
            status = self.__client.setTimeZone(request)
            logger.debug(
                "setting time zone_id as {}, message: {}".format(
                    zone_id, status.message
                )
            )
        except TTransport.TException as e:
            raise RuntimeError("Could not set time zone because: ", e)
        self.__zone_id = zone_id

    def get_time_zone(self):
        if self.__zone_id is not None:
            return self.__zone_id
        try:
            resp = self.__client.getTimeZone(self.__session_id)
        except TTransport.TException as e:
            raise RuntimeError("Could not get time zone because: ", e)
        return resp.timeZone

    def close(self):
        if self.__is_close:
            return
        req = TSCloseSessionReq(self.__session_id)
        try:
            self.__client.closeSession(req)
        except TTransport.TException as e:
            logger.exception(
                "Error occurs when closing session at server. Maybe server is down. Error message: ",
                exc_info=e,
            )
        finally:
            self.__is_close = True
            if self.__transport is not None:
                self.__transport.close()
        self.__is_close = True

    def commit(self):
        raise NotSupportedError("Commit operation is not supported!")

    def rollback(self):
        raise NotSupportedError("Rollback operation is not supported!")

    def cursor(self):
        return Cursor(self, self.__client, self.__session_id, self.__statement_id)

    @property
    def client(self):
        return self.__client

    @property
    def is_close(self):
        return self.__is_close

    @property
    def session_id(self):
        return self.__session_id

    @property
    def statement_id(self):
        return self.__statement_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
