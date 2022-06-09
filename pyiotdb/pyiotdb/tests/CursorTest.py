import unittest

from pyiotdb import connect


class CursorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = connect("127.0.0.1", "6667", "root", "root", True)

    def test_executemany(self):
        cur = self.conn.cursor()
        cur.executemany("select * from root.factory.room2.device1 where time > %s", [(1), (2), (3)])
        for row in cur.fetchall():
            print(row)
        print(cur.rowcount)
        print(cur.description)

    def test_ddl(self):
        cur = self.conn.cursor()
        cur.execute("show storage group")
        for row in cur.fetchall():
            print(row)
        print(cur.rowcount)
        print(cur.description)

    def test_storage_group(self):
        cur = self.conn.cursor()
        cur.execute("show storage group")
        for row in cur.fetchall():
            print(row)

    def test_show_devices(self):
        cur = self.conn.cursor()
        cur.execute("show devices root.factory.**")
        for row in cur.fetchall():
            print(row)

    def test_sqlalchemy_mode(self):
        query = "select status \n" \
                "Time Position 1 3 5 \n" \
                "from root.factory.room1.device2"
        cur = self.conn.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            print(row)

    def test_show_timeseries(self):
        cur = self.conn.cursor()
        schema = "root.factory1"
        table = "room1.device2"
        cur.execute(
            'SELECT temperature AS temperature, status AS status FROM root.factory.room2.device1 WHERE temperature < 40 ORDER BY Time DESC LIMIT 10000')
        for row in cur.fetchall():
            print(row)
