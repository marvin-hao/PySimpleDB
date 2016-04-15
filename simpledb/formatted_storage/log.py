__author__ = 'Marvin'

from simpledb.plain_storage.file import MaxPage, Block
from simpledb.shared_service.server import SimpleDB
from simpledb.shared_service.util import synchronized


class BasicLogRecord:
    """
    A class that provides the ability to read the values of a log record.
    The class has no idea what values are there.
    Instead, the methods nextInt() and nextString() read the values sequentially.
    Thus the client is responsible for knowing how many values
    are in the log record, and what their types are.
    """

    def __init__(self, pg, pos):
        """
        A log record located at the specified position of the specified page.
        This constructor is called exclusively by LogIterator#next()
        :param pg: the page containing the log record
        :param pos: the position of the log record
        """
        assert isinstance(pg, MaxPage)
        self._pg = pg
        self._pos = pos

    def next_int(self):
        """
        Returns the next value of the current log record, assuming it is an integer.
        :return: the next value of the current log record
        """
        result = self._pg.get_int(self._pos)
        self._pos += MaxPage.INT_SIZE
        return result

    def next_string(self):
        """
        Returns the next value of the current log record,
        assuming it is a string.
        :return: the next value of the current log record
        """
        result = self._pg.get_string(self._pos)
        self._pos += MaxPage.str_size(len(result))
        return result

    def __iter__(self):
        """
        To make this class a iterable
        """
        return self


class LogIterator:
    """
    A class that provides the ability to move through the records of the log file in reverse order.
    """

    def __init__(self, blk):
        """
        Creates an iterator for the records in the log file,
        positioned after the last log record.
        This constructor is called exclusively by LogMgr.iterator()
        """
        assert isinstance(blk, Block)
        self._blk = blk
        self._pg = MaxPage()
        self._pg.read(self._blk)
        self._currentrec = self._pg.get_int(LogMgr.LAST_POS)

    def has_next(self):
        """
        Determines if the current log record
        is the earliest record in the log file.
        :return: true if there is an earlier record
        """
        return self._currentrec > 0 or self._blk.number() > 0

    def next(self):
        """
        Moves to the next log record in reverse order.
        If the current log record is the earliest in its block,
        then the method moves to the next oldest block,
        and returns the log record from there.
        :return: the next earliest log record
        """
        if self._currentrec == 0:
            self.__move_to_next_block()
        self._currentrec = self._pg.get_int(self._currentrec)
        return BasicLogRecord(self._pg, self._currentrec + MaxPage.INT_SIZE)

    def generator(self):
        """
        To be compatible with Python's compelling features, we further implement a generator.
        This will allow us to use list comprehension
        :return: a generator of BasicLogRecord
        """
        while self.has_next():
            yield self.next()

    def __move_to_next_block(self):
        """
        Moves to the next log block in reverse order,
        and positions it after the last record in that block.
        """
        self._blk = Block(self._blk.file_name(), self._blk.number()-1)
        self._pg.read(self._blk)
        self._currentrec = self._pg.get_int(LogMgr.LAST_POS)


class LogMgr:
    """
    The low-level log manager.
    This log manager is responsible for writing log records into a log file.
    A log record can be any sequence of integer and string values.
    The log manager does not understand the meaning of these values,
    which are written and read by the recovery manager
    The format of a block in a log file is like:
    pointer to the last record--------
    """
    LAST_POS = 0  # This variable is always zero.

    def __init__(self, logfile):
        """
        Creates the manager for the specified log file.
        If the log file does not yet exist, it is created
        with an empty first block.
        This constructor depends on a FileMgr object
        that it gets from the method simpledb.server.SimpleDB.fileMgr().
        That object is created during system initialization.
        Thus this constructor cannot be called until
        simpledb.server.SimpleDB.initFileMgr(String)
        is called first.
        :param logfile: the name of the log file
        """
        print(SimpleDB)
        self._mypage = MaxPage()
        assert isinstance(logfile, str)
        self._logfile = logfile
        logsize = SimpleDB.file_mgr().size(logfile)
        if logsize == 0:
            self.__append_new_block()
        else:
            self._currentblk = Block(logfile, logsize - 1)
            self._mypage.read(self._currentblk)
            self._currentpos = self.__get_last_record_position() + MaxPage.INT_SIZE

    def flush(self, lsn):
        """
        Ensures that the log records corresponding to the
        specified LSN has been written to disk.
        All earlier log records will also be written to disk.
        :param lsn: the LSN of a log record
        """
        if lsn >= self.__current_lsn():
            self.__flush()

    @synchronized
    def iterator(self):
        """
        Returns an iterator for the log records,
        which will be returned in reverse order starting with the most recent.
        """
        self.__flush()
        return LogIterator(self._currentblk)

    @synchronized
    def append(self, rec):
        """
        Appends a log record to the file.
        The record contains an arbitrary array of strings and integers.
        The method also writes an integer to the end of each log record whose value
        is the offset of the corresponding integer for the previous log record.
        These integers allow log records to be read in reverse order.
        :param rec: the list of values
        :return: the LSN of the final value
        """
        recsize = MaxPage.INT_SIZE  # 4 bytes for the integer that points to the previous log record
        for obj in rec:
            recsize += self.__size(obj)
        assert recsize <= MaxPage.BLOCK_SIZE  # Here I added a preventor
        if self._currentpos + recsize >= MaxPage.BLOCK_SIZE:  # the log record doesn't fit,
            self.__flush()  # so move to the next block.
            self.__append_new_block()  # If recsize >= BLOCK_SIZE, then BOOOOOOOOMB. XD
        for obj in rec:
            self.__append_val(obj)
        self.__finalize_record()
        return self.__current_lsn()

    def __append_val(self, val):
        """
        Adds the specified value to the page at the position denoted by currentpos.
        Then increments currentpos by the size of the value.
        :param val: the integer or string to be added to the page
        """
        if isinstance(val, str):
            self._mypage.set_string(self._currentpos, val)
        else:
            self._mypage.set_int(self._currentpos, val)
        self._currentpos += self.__size(val)

    def __size(self, val):
        """
        Calculates the size of the specified integer or string.
        :param val: the value
        :return: the size of the value, in bytes
        """
        if isinstance(val, str):
            return MaxPage.str_size(len(val))
        else:
            return MaxPage.INT_SIZE

    def __current_lsn(self):
        """
        Returns the LSN of the most recent log record.
        As implemented, the LSN is the block number where the record is stored.
        Thus every log record in a block has the same LSN.
        :return: the LSN of the most recent log record
        """
        return self._currentblk.number()

    def __flush(self):
        """
        Writes the current page to the log file.
        """
        self._mypage.write(self._currentblk)

    def __append_new_block(self):
        """
        Clear the current page, and append it to the log file.
        """
        self._mypage.clear_contents()  # The original code doesn't have this step
        self.__set_last_record_position(0)
        self._currentblk = self._mypage.append(self._logfile)
        self._currentpos = MaxPage.INT_SIZE

    def __finalize_record(self):
        """
        Sets up a circular chain of pointers to the records in the page.
        There is an integer added to the end of each log record
        whose value is the offset of the previous log record.
        """
        self._mypage.set_int(self._currentpos, self.__get_last_record_position())
        self.__set_last_record_position(self._currentpos)
        self._currentpos += MaxPage.INT_SIZE

    def __get_last_record_position(self):
        """
        The first four bytes of the page contain an integer whose value
        is the offset of the integer for the last log record in the page.
        """
        return self._mypage.get_int(LogMgr.LAST_POS)

    def __set_last_record_position(self, pos):
        self._mypage.set_int(LogMgr.LAST_POS, pos)

