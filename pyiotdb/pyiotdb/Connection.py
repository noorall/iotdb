import time

from iotdb.Session import Session

from .Exceptions import ProgrammingError, ConnectionError, NotSupportedError


class Connection:
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
        self._session = Session(host, port, user, password, fetch_size, zone_id)
        self._session.open(False)
        if not self._session.is_open():
            self._closed = True
            raise ConnectionError("Failed to open session!")
        self._closed = False

    def close(self):
        self._closed = True
        self._session.close()

    def commit(self):
        raise NotSupportedError("Commit operation is not supported!")

    def rollback(self):
        raise NotSupportedError("Rollback operation is not supported!")

    def cursor(self):
        pass

    @property
    def session(self):
        return self._session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
