__author__ = 'Marvin'

from simpledb.formatted_storage.tx import Transaction
from simpledb.formatted_storage.record import Schema, TableInfo, RID
from simpledb.formatted_storage.index.index import Index
from simpledb.query_prosessor.query import Constant, TableScan


class HashIndex(Index):
    """
    A static hash implementation of the Index interface.
    A fixed number of buckets is allocated (currently, 100),
    and each bucket is implemented as a file of index records.
    """
    NUM_BUCKETS = 100

    def __init__(self, idxname, sch, tx):
        """
        Opens a hash index for the specified index.
        :param dixname: the name of the index
        :param sch: the schema of the index records
        :param tx: the calling transaction
        """
        assert isinstance(sch, Schema)
        assert isinstance(tx, Transaction)
        self._idxname = idxname
        self._sch = sch
        self._tx = tx
        self._searchkey = None
        self._ts = None

    def close(self):
        """
        Closes the index by closing the current table scan.
        """
        if self._ts is not None:
            assert isinstance(self._ts, TableScan)
            self._ts.close()

    def before_first(self, search_key):
        """
        Positions the index before the first index record
        having the specified search key.
        The method hashes the search key to determine the bucket,
        and then opens a table scan on the file
        corresponding to the bucket.
        The table scan for the previous bucket (if any) is closed.
        """
        assert isinstance(search_key, Constant)
        self.close()
        self._searchkey = search_key
        bucket = hash(search_key) % self.NUM_BUCKETS
        tblname = self._idxname + bucket
        ti = TableInfo(tblname, self._sch)
        self._ts = TableScan(ti, self._tx)

    def next(self):
        """
        Moves to the next record having the search key.
        The method loops through the table scan for the bucket,
        looking for a matching record, and returning false
        if there are no more such records.
        """
        while self._ts.next():
            if self._ts.get_val("dataval") == self._searchkey:
                return True
        return False

    def get_data_rid(self):
        """
        Retrieves the dataRID from the current record
        in the table scan for the bucket.
        """
        blknum = self._ts.get_int("block")
        id = self._ts.get_int("id")
        return RID(blknum, id)

    def insert(self, data_val, data_rid):
        """
        Inserts a new record into the table scan for the bucket.
        """
        assert isinstance(data_rid, RID)
        self.before_first(data_val)
        self._ts.insert()
        self._ts.set_int("block", data_rid.block_number())
        self._ts.set_int("id", data_rid.id())
        self._ts.set_val("dataval", data_val)

    @staticmethod
    def search_cost(numblocks, rpb):
        """
        Returns the cost of searching an index file having the
        specified number of blocks.
        The method assumes that all buckets are about the
        same size, and so the cost is simply the size of
        the bucket.
        :param numblocks: the number of blocks of index records
        :param rpb: the number of records per block (not used here)
        :return: the cost of traversing the index
        """
        return numblocks // HashIndex.NUM_BUCKETS