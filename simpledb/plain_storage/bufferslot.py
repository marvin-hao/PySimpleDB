__author__ = 'Marvin'
import time
import threading
current_milli_time = lambda: int(round(time.time() * 1000))
from simpledb.plain_storage.file import MaxPage, Block
from simpledb.shared_service.server import SimpleDB
from simpledb.shared_service.util import synchronized


class PageFormatter:
    """
    An interface used to initialize a new block on disk.
    There will be an implementing class for each "type" of
    disk block.
    """

    def format(self, page):
        """
        Initializes a page, whose contents will be
        written to a new disk block.
        This method is called only during the method
        Buffer.assignToNew.
        :param page: an instance of Page
        """
        raise NotImplementedError()


class BufferAbortException(Exception):
    """
    A runtime exception indicating that the transaction
    needs to abort because a buffer request could not be satisfied.
    """
    pass


class BufferSlot:
    """
    An individual buffer.
    A buffer wraps a page and stores information about its status,
    such as the disk block associated with the page,
    the number of times the block has been pinned,
    whether the contents of the page have been modified,
    and if so, the id of the modifying transaction and
    the LSN of the corresponding log record.
    """

    def __init__(self):
        """
        Creates a new buffer, wrapping a new
        simpledb.simpledb.file.Page page.
        This constructor is called exclusively by the class BasicBufferMgr.
        It depends on the simpledb.log.LogMgr LogMgr object that it gets from the class simpledb.server.SimpleDB.
        That object is created during system initialization. Thus this constructor cannot be called until
        simpledb.server.SimpleDB.initFileAndLogMgr(String) or is called first.
        """
        self._contents = MaxPage()
        self._blk = None
        self._pins = 0
        self._modified_by = -1
        self._log_sequence_number = -1

    def get_int(self, offset):
        """
        Returns the integer value at the specified offset of the buffer's page.
        If an integer was not stored at that location, the behavior of the method is unpredictable.
        :param offset: the byte offset of the page
        :return: the integer value at that offset
        """
        return self._contents.get_int(offset)

    def get_string(self, offset):
        """
        Returns the string value at the specified offset of the buffer's page.
        If a string was not stored at that location, the behavior of the method is unpredictable.
        :param offset: the byte offset of the page
        :return: the string value at that offset
        """
        return self._contents.get_string(offset)

    def set_int(self, offset, val, txnum, lsn):
        """
        Writes an integer to the specified offset of the buffer's page.
        This method assumes that the transaction has already written an appropriate log record.
        The buffer saves the id of the transaction and the LSN of the log record.
        A negative lsn value indicates that a log record was not necessary.
        :param offset: the byte offset within the page
        :param val: the new integer value to be written
        :param txnum: the id of the transaction performing the modification
        :param lsn: the LSN of the corresponding log record
        """
        self._modified_by = txnum
        if lsn >= 0:
            self._log_sequence_number = lsn
        self._contents.set_int(offset, val)

    def set_string(self, offset, val, txnum, lsn):
        """
        Writes a string to the specified offset of the buffer's page.
        This method assumes that the transaction has already written an appropriate log record.
        A negative lsn value indicates that a log record was not necessary.
        The buffer saves the id of the transaction and the LSN of the log record.
        :param offset: the byte offset within the page
        :param val: the new string value to be written
        :param txnum: the id of the transaction performing the modification
        :param lsn: the LSN of the corresponding log record
        """
        assert isinstance(val, str)
        self._modified_by = txnum
        if lsn >= 0:
            self._log_sequence_number = lsn
        self._contents.set_string(offset, val)

    def block(self):
        """
        Returns a reference to the disk block that the buffer is pinned to.
        :return: a reference to a disk block
        """
        return self._blk

    def flush(self):
        """
        Writes the page to its disk block if the page is dirty.
        The method ensures that the corresponding log
        record has been written to disk prior to writing the page to disk.
        """
        if self._modified_by > 0:
            SimpleDB.log_mgr().flush(self._log_sequence_number)
            self._contents.write(self._blk)
            self._modified_by = -1

    def pin(self):
        """
        Increases the buffer's pin count.
        """
        self._pins += 1

    def unpin(self):
        """
        Decreases the buffer's pin count.
        """
        self._pins -= 1

    def is_pinned(self):
        """
        Returns true if the buffer is currently pinned (that is, if it has a nonzero pin count).
        :return: true if the buffer is pinned
        """
        return self._pins > 0

    def is_modified_by(self, txnum):
        """
        Returns true if the buffer is dirty due to a modification by the specified transaction.
        :param txnum: the id of the transaction
        :return: true if the transaction modified the buffer
        """
        return self._modified_by == txnum

    def assign_to_block(self, b):
        """
        Reads the contents of the specified block into the buffer's page.
        If the buffer was dirty, then the contents of the previous page are first written to disk.
        :param b: a reference to the data block
        """
        assert isinstance(b, Block)
        self.flush()
        self._blk = b
        self._contents.read(self._blk)
        self._pins = 0

    def assign_to_new(self, filename, fmtr):
        """
        Initializes the buffer's page according to the specified formatter,
        and appends the page to the specified file.
        If the buffer was dirty, then the contents
        of the previous page are first written to disk.
        :param filename: the name of the file
        :param fmtr: a page formatter, used to initialize the page
        """
        assert isinstance(filename, str)
        assert isinstance(fmtr, PageFormatter)
        self.flush()
        fmtr.format(self._contents)
        self._blk = self._contents.append(filename)
        self._pins = 0


class BasicBufferMgr:
    """
    Manages the pinning and unpinning of buffers to blocks.
    """
    def __init__(self, numbuffs):
        """
        Creates a buffer manager having the specified number of buffer slots.
        This constructor depends on both the FileMgr and LogMgr objects
        that it gets from the class SimpleDB.
        Those objects are created during system initialization.
        Thus this constructor cannot be called until initFileAndLogMgr(String) or is called first.
        :param numbuffs: the number of buffer slots to allocate
        """
        self._bufferpool = [BufferSlot() for count in range(numbuffs)]
        # First time feeling that Python is more concise syntactically
        self._num_available = numbuffs

    @synchronized
    def flush_all(self, txnum):
        """
        Flushes the dirty buffers modified by the specified transaction.
        :param txnum: the transaction's id number
        """
        [buff.flush() for buff in self._bufferpool if buff.is_modified_by(txnum)]

    @synchronized
    def pin(self, blk):
        """
        Pins a buffer to the specified block.
        If there is already a buffer assigned to that block then that buffer is used;
        otherwise, an unpinned buffer from the pool is chosen.
        Returns a null value if there are no available buffers.
        :param blk: a reference to a disk block
        :return: the pinned buffer
        """
        buff = self.__find_existing_buffer(blk)
        if buff is None:
            buff = self.__choose_unpinned_buffer()
            if buff is None:
                return None
            buff.assign_to_block(blk)
        if not buff.is_pinned():
            self._num_available -= 1
        buff.pin()
        return buff

    @synchronized
    def pin_new(self, filename, fmtr):
        """
        Allocates a new block in the specified file, and pins a buffer to it.
        Returns null (without allocating the block) if there are no available buffers.
        :param filename: the name of the file
        :param fmtr: a pageformatter object, used to format the new block
        :return: the pinned buffer
        """
        assert isinstance(filename, str)
        assert isinstance(fmtr, PageFormatter)
        buff = self.__choose_unpinned_buffer()
        if buff is None:
            return None
        buff.assign_to_new(filename, fmtr)
        self._num_available -= 1
        buff.pin()
        return buff

    @synchronized
    def unpin(self, buff):
        """
        Unpins the specified buffer.
        :param buff: the buffer to be unpinned
        """
        assert isinstance(buff, BufferSlot)
        buff.unpin()
        if not buff.is_pinned():
            self._num_available += 1

    def available(self):
        """
        Returns the number of available (i.e. unpinned) buffers.
        :return: the number of available buffers
        """
        return self._num_available

    def __find_existing_buffer(self, blk):
        for buff in self._bufferpool:
            b = buff.block()
            if b is not None and b == blk:
                return buff
        return None

    def __choose_unpinned_buffer(self):
        for buff in self._bufferpool:
            if not buff.is_pinned():
                return buff
        return None


class BufferMgr:
    """
    The publicly-accessible buffer manager.
    A buffer manager wraps a basic buffer manager, and
    provides the same methods. The difference is that
    the methods pin(Block) and pinNew(String, PageFormatter)
    will never return null.
    If no buffers are currently available, then the
    calling thread will be placed on a waiting list.
    The waiting threads are removed from the list when
    a buffer becomes available.
    If a thread has been waiting for a buffer for an
    excessive amount of time (currently, 10 seconds)
    then a BufferAbortException is thrown.
    """
    MAX_TIME = 10000  # 10 seconds

    def __init__(self, numbuffers):
        """
        Creates a new buffer manager having the specified number of buffers.
        This constructor depends on both the FileMgr and LogMgr objects
        that it gets from the class simpledb.server.SimpleDB.
        Those objects are created during system initialization.
        Thus this constructor cannot be called until initFileAndLogMgr(String) is called first.
        :param numbuffers: the number of buffer slots to allocate
        """
        self._buffer_mgr = BasicBufferMgr(numbuffers)
        self._cv = threading.Condition()  # for implementing the wait-notify mechanism

    def pin(self, blk):
        """
        Pins a buffer to the specified block, potentially
        waiting until a buffer becomes available.
        If no buffer becomes available within a fixed
        time period, then a {@link BufferAbortException} is thrown.
        :param blk: a reference to a disk block
        :return: the buffer pinned to that block
        """
        assert isinstance(blk, Block)
        try:
            self._cv.acquire()
            timestamp = current_milli_time()
            buff = self._buffer_mgr.pin(blk)
            while buff is None and not self.__waiting_too_long(timestamp):
                self._cv.wait()
                buff = self._buffer_mgr.pin(blk)
            self._cv.release()
            if buff is None:
                raise BufferAbortException()
            return buff
        except Exception:
            raise BufferAbortException()

    def pin_new(self, filename, fmtr):
        """
        Pins a buffer to a new block in the specified file,
        potentially waiting until a buffer becomes available.
        If no buffer becomes available within a fixed
        time period, then a BufferAbortException is thrown.
        :param filename: the name of the file
        :param fmtr: the formatter used to initialize the page
        :return: the buffer pinned to that block
        """
        try:
            self._cv.acquire()
            timestamp = current_milli_time()
            buff = self._buffer_mgr.pin_new(filename, fmtr)
            while buff is None and not self.__waiting_too_long(timestamp):
                self._cv.wait(BufferMgr.MAX_TIME//1000)
                buff = self._buffer_mgr.pin_new(filename, fmtr)
            self._cv.release()
            if buff is None:  # still not get a buffer
                raise BufferAbortException()
            return buff
        except Exception:
            raise BufferAbortException()

    def unpin(self, buff):
        """
        Unpins the specified buffer. If the buffer's pin count becomes 0,
        then the threads on the wait list are notified.
        :param buff: the buffer to be unpinned
        """
        self._buffer_mgr.unpin(buff)
        if not buff.is_pinned():
            self._cv.acquire()
            self._cv.notify_all()
            self._cv.release()

    def flush_all(self, txnum):
        """
        Flushes the dirty buffers modified by the specified transaction.
        :param txnum: the transaction's id number
        """
        self._buffer_mgr.flush_all(txnum)

    def available(self):
        """
        Returns the number of available (ie unpinned) buffers.
        :return: the number of available buffers
        """
        return self._buffer_mgr.available()

    def __waiting_too_long(self, starttime):
        return current_milli_time() - starttime > BufferMgr.MAX_TIME




