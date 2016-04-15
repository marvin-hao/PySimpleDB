__author__ = 'Marvin'

from simpledb.query_prosessor.query import *
from simpledb.plain_storage.file import *
from simpledb.formatted_storage.record import *
from simpledb.query_prosessor.materialize import TempTable, MaterializePlan
from simpledb.shared_service.macro import *


class BufferNeeds:
    """
    A class containing static methods,
    which estimate the optimal number of buffers
    to allocate for a scan.
    """
    @staticmethod
    def best_root(size: int) -> int:
        """
        This method considers the various roots
        of the specified output size (in blocks),
        and returns the highest root that is less than
        the number of available buffers.
        :param size: the size of the output file
        :return: the highest number less than the number of available buffers, that is a root of the plan's output size
        """
        avail = SimpleDB.buffer_mgr().available()
        if avail <= 1:
            return 1
        k = sys.maxsize
        i = 1.0
        while k > avail:
            i += 1
            k = math.ceil(size ** (1 / i))

        return k

    @staticmethod
    def best_factor(size: int) -> int:
        """
        This method considers the various factors
        of the specified output size (in blocks),
        and returns the highest factor that is less than
        the number of available buffers.
        :param size: the size of the output file
        :return: the highest number less than the number of available buffers,
                 that is a factor of the plan's output size
        """
        avail = SimpleDB.buffer_mgr().available()
        if avail <= 1:
            return 1
        k = size
        i = 1.0
        while k > avail:
            i += 1
            k = math.ceil(size / i)
        return k


class ChunkScan(Scan):
    """
    The class for the chunk operator.
    """
    def __init__(self, ti: TableInfo, startbnum: int, endbnum: int, tx: Transaction):
        """
        Creates a chunk consisting of the specified pages.
        :param ti: the metadata for the chunked table
        :param startbnum: the starting block number
        :param endbnum: the ending block number
        :param tx: the current transaction
        """
        self._current = -1
        self._rp = None
        self._pages = []
        self._startbnum = startbnum
        self._endbnum = endbnum
        self._sch = ti.schema()
        filename = ti.file_name()
        i = startbnum
        while i <= endbnum:
            blk = Block(filename, i)
            self._pages.append(RecordPage(blk, ti, tx))

        self.before_first()

    def __move_to_block(self, blknum: int):
        self._current = blknum
        self._rp = self._pages[self._current - self._startbnum]
        self._rp.move_to_id(-1)

    def before_first(self):
        self.__move_to_block(self._startbnum)

    def close(self):
        for r in self._pages:
            r.close()

    def has_field(self, fldname):
        return self._sch.has_field(fldname)

    def get_string(self, fldname):
        return self._rp.get_string(fldname)

    def get_int(self, fldname):
        return self._rp.get_int(fldname)

    def get_val(self, fldname):
        if self._sch.type(fldname) == INTEGER:
            return IntConstant(self._rp.get_int(fldname))
        else:
            return StringConstant(self._rp.get_string(fldname))

    def next(self):
        """
        Moves to the next record in the current block of the chunk.
        If there are no more records, then make
        the next block be current.
        If there are no more blocks in the chunk, return false.
        """
        while True:
            if self._rp.next():
                return True
            if self._current == self._endbnum:
                return False
            self.__move_to_block(self._current + 1)


class MultiBufferProductScan(Scan):
    """
    The Scan class for the muti-buffer version of the
    product operator.
    """
    def __init__(self, lhsscan: Scan, ti: TableInfo, tx:Transaction):
        """
        Creates the scan class for the product of the LHS scan and a table.
        :param lhsscan: the LHS scan
        :param ti: the metadata for the RHS table
        :param tx: the current transaction
        """
        self._lhsscan = lhsscan
        self._rhsscan = None
        self._prodscan = None
        self._ti = ti
        self._tx = tx
        self._filesize = tx.size(ti.file_name())
        self._chunksize = BufferNeeds.best_factor(self._filesize)
        self._nextblknum = 0
        self.before_first()

    def __use_next_chunk(self):
        if self._rhsscan is not None:
            self._rhsscan.close()

        if self._nextblknum >= self._filesize:
            return False

        end = self._nextblknum + self._chunksize - 1

        if end >= self._filesize:
            end = self._filesize - 1

        self._rhsscan = ChunkScan(self._ti, self._nextblknum, end, self._tx)
        self._lhsscan.before_first()
        self._prodscan = ProductScan(self._lhsscan, self._rhsscan)
        self._nextblknum = end + 1
        return True

    def before_first(self):
        """
        Positions the scan before the first record.
        That is, the LHS scan is positioned at its first record,
        and the RHS scan is positioned before the first record of the first chunk.
        """
        self._nextblknum = 0
        self.__use_next_chunk()

    def has_field(self, fldname):
        return self._prodscan.has_field(fldname)

    def get_string(self, fldname):
        """
        Returns the string value of the specified field.
        The value is obtained from whichever scan
        contains the field.
        """
        return self._prodscan.get_string(fldname)

    def get_int(self, fldname):
        return self._prodscan.get_int(fldname)

    def get_val(self, fldname):
        return self._prodscan.get_val(fldname)

    def close(self):
        self._prodscan.close()

    def next(self):
        """
        Moves to the next record in the current scan.
        If there are no more records in the current chunk,
        then move to the next LHS record and the beginning of that chunk.
        If there are no more LHS records, then move to the next chunk
        and begin again.
        """
        while not self._prodscan.next():
            if not self.__use_next_chunk():
                return False
        return True


class MultiBufferProductPlan(Plan):
    """
    The Plan class for the muti-buffer version of the
    product operator.
    """
    def __init__(self, lhs: Plan, rhs: Plan, tx: Transaction):
        """
        Creates a product plan for the specified queries.
        :param lhs: the plan for the LHS query
        :param rhs: the plan for the RHS query
        :param tx: the calling transaction
        """
        self._lhs = lhs
        self._rhs = rhs
        self._tx = tx
        self._schema = Schema()
        self._schema.add_all(lhs.schema())
        self._schema.add_all(rhs.schema())

    def __copy_records_from(self, p: Plan) -> TempTable:
        src = p.open()
        sch = p.schema()
        tt = TempTable(sch, self._tx)
        dest = tt.open()
        while src.next():
            dest.insert()
            for fldname in sch.fields():
                dest.set_val(fldname, src.get_val(fldname))
        src.close()
        dest.close()
        return tt

    def schema(self):
        """
        Returns the schema of the product,
        which is the union of the schemas of the underlying queries.
        """
        return self._schema

    def distinct_values(self, fldname):
        """
        Estimates the distinct number of field values in the product.
        Since the product does not increase or decrease field values,
        the estimate is the same as in the appropriate underlying query.
        """
        if self._lhs.schema().has_field(fldname):
            return self._lhs.distinct_values(fldname)
        else:
            return self._rhs.distinct_values(fldname)

    def records_output(self):
        """
        Estimates the number of output records in the product.
        The formula is:
        R(product(p1,p2)) = R(p1)*R(p2)
        """
        return self._lhs.records_output() * self._rhs.records_output()

    def blocks_accessed(self):
        """
        Returns an estimate of the number of block accesses
        required to execute the query. The formula is:
        B(product(p1,p2)) = B(p2) + B(p1)*C(p2)
        where C(p2) is the number of chunks of p2.
        The method uses the current number of available buffers
        to calculate C(p2), and so this value may differ
        when the query scan is opened.
        """
        # this guesses at the # of chunks
        avail = SimpleDB.buffer_mgr().available()
        size = MaterializePlan(self._rhs, self._tx).blocks_accessed()
        numchunks = size // avail
        return self._rhs.blocks_accessed() + self._lhs.blocks_accessed() * numchunks

    def open(self):
        """
        A scan for this query is created and returned, as follows.
        First, the method materializes its RHS query.
        It then determines the optimal chunk size,
        based on the size of the materialized file and the
        number of available buffers.
        It creates a chunk plan for each chunk, saving them in a list.
        Finally, it creates a multiscan for this list of plans,
        and returns that scan.
        """
        tt = self.__copy_records_from(self._rhs)
        ti = tt.get_table_info()
        leftscan = self._lhs.open()
        return MultiBufferProductScan(leftscan, ti, self._tx)
