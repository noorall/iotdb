import unittest

from pyiotdb.pyiotdb import connect


class CursorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = connect("127.0.0.1", "6667")
        self.cur = self.conn.cursor()

    def test_executemany(self):
        self.cur.executemany("select * from root.factory.room2.device1 where time > %s",[(1),(2),(3)])
        for row in self.cur.fetchall():
            print(row)
        print(self.cur.rowcount)
        print(self.cur.description)
