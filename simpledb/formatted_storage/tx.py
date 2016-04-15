__author__ = 'Marvin'
from simpledb.plain_storage.bufferslot import *
from simpledb.plain_storage.lock import PageLockMgr
from simpledb.formatted_storage.recovery import RecoveryMgr


class BufferList:
    """
    Manages the transaction's currently-pinned buffers.
    """
    def __init__(self):
        self._buffers = {}
        self._pins = []
        self._buffer_mgr = SimpleDB.buffer_mgr()

    def get_buffer(self, blk):
        """
        Returns the buffer pinned to the specified block.
        The method returns null if the transaction has not
        pinned the block.
        :param blk: a reference to the disk block
        :return: the buffer pinned to that block
        """
        return self._buffers.get(blk)

    def pin(self, blk):
        """
        Pins the block and keeps track of the buffer internally.
        :param blk: a reference to the disk block
        """
        buff = self._buffer_mgr.pin(blk)
        self._buffers[blk] = buff
        self._pins.append(blk)

    def pin_new(self, filename, fmtr):
        """
        Appends a new block to the specified file and pins it.
        :param filename: the name of the file
        :param fmtr: the formatter used to initialize the new page
        :return: a reference to the newly-created block
        """
        assert isinstance(filename, str)
        assert isinstance(fmtr, PageFormatter)
        buff = self._buffer_mgr.pin_new(filename, fmtr)
        assert isinstance(buff, BufferSlot)
        blk = buff.block()
        self._buffers[blk] = buff
        self._pins.append(blk)
        return blk

    def unpin(self, blk):
        """
        Unpins the specified block.
        :param blk: a reference to the disk block
        """
        buff = self._buffers.get(blk)
        self._buffer_mgr.unpin(buff)
        self._pins.remove(blk)
        if not blk in self._pins:
            del self._buffers[blk]

    def unpin_all(self):
        """
        Unpins any buffers still pinned by this transaction.
        """
        for blk in self._pins:
            buff = self._buffers.get(blk)
            self._buffer_mgr.unpin(buff)
        self._buffers.clear()
        self._pins.clear()


class Transaction:
    """
    Provides transaction management for clients,
    ensuring that all transactions are serializable,recoverable,
    and in general satisfy the ACID properties.
    """
    __next_tx_num = 0
    __END_OF_FILE = -1

    def __init__(self):
        """
        Creates a new transaction and its associated
        recovery and concurrency managers.
        This constructor depends on the file, log, and buffer
        managers that it gets from the class SimpleDB.
        Those objects are created during system initialization.
        Thus this constructor cannot be called until either init(String) or
        initFileLogAndBufferMgr(String) is called first.
        """
        self._txnum = self.__next_tx_number()
        self._recovery_mrg = RecoveryMgr(self._txnum)
        self._concur_mgr = PageLockMgr()
        self._my_buffers = BufferList()

    def commit(self):
        """
        Commits the current transaction.
        Flushes all modified buffers (and their log records),
        writes and flushes a commit record to the log,
        releases all locks, and unpins any pinned buffers.
        """
        self._recovery_mrg.commit()
        self._concur_mgr.release()
        self._my_buffers.unpin_all()
        print("transaction "+str(self._txnum)+" committed")

    def rollback(self):
        """
        Rolls back the current transaction.
        Undoes any modified values,
        flushes those buffers,
        writes and flushes a rollback record to the log,
        releases all locks, and unpins any pinned buffers.
        """
        self._recovery_mrg.rollback()
        self._concur_mgr.release()
        self._my_buffers.unpin_all()
        print("transaction " + str(self._txnum) + " rolled back")

    def recover(self):
        """
        Flushes all modified buffers.
        Then goes through the log, rolling back all
        uncommitted transactions.  Finally,
        writes a quiescent checkpoint record to the log.
        This method is called only during system startup,
        before user transactions begin.
        """
        SimpleDB.buffer_mgr().flush_all(self._txnum)
        self._recovery_mrg.recover()

    def pin(self, blk):
        """
        Pins the specified block.
        The transaction manages the buffer for the client.
        :param blk: a reference to the disk block
        """
        self._my_buffers.pin(blk)

    def unpin(self, blk):
        """
        Unpins the specified block.
        The transaction looks up the buffer pinned to this block, and unpins it.
        :param blk: a reference to the disk block
        """
        self._my_buffers.unpin(blk)

    def get_int(self, blk, offset):
        """
        Returns the integer value stored at the
        specified offset of the specified block.
        The method first obtains an SLock on the block,
        then it calls the buffer to retrieve the value.
        :param blk: a reference to a disk block
        :param offset: the byte offset within the block
        :return: the integer stored at that offset
        """
        self._concur_mgr.slock(blk)
        buff = self._my_buffers.get_buffer(blk)
        assert isinstance(buff, BufferSlot)
        return buff.get_int(offset)

    def get_string(self, blk, offset):
        """
        Returns the string value stored at the
        specified offset of the specified block.
        The method first obtains an SLock on the block,
        then it calls the buffer to retrieve the value.
        :param blk: a reference to a disk block
        :param offset: the byte offset within the block
        :return: the string stored at that offset
        """
        self._concur_mgr.slock(blk)
        buff = self._my_buffers.get_buffer(blk)
        assert isinstance(buff, BufferSlot)
        return buff.get_string(offset)

    def set_int(self, blk, offset, val):
        """
        Stores an integer at the specified offset
        of the specified block.
        The method first obtains an XLock on the block.
        It then reads the current value at that offset,
        puts it into an update log record, and
        writes that record to the log.
        Finally, it calls the buffer to store the value,
        passing in the LSN of the log record and the transaction's id.
        :param blk: a reference to the disk block
        :param offset: a byte offset within that block
        :param val: the value to be stored
        """
        self._concur_mgr.xlock(blk)
        buff = self._my_buffers.get_buffer(blk)
        assert isinstance(buff, BufferSlot)
        lsn = self._recovery_mrg.set_int(buff, offset, val)
        buff.set_int(offset, val, self._txnum, lsn)

    def set_string(self, blk, offset, val):
        """
        Stores a string at the specified offset
        of the specified block.
        The method first obtains an XLock on the block.
        It then reads the current value at that offset,
        puts it into an update log record, and
        writes that record to the log.
        Finally, it calls the buffer to store the value,
        passing in the LSN of the log record and the transaction's id.
        :param blk: a reference to the disk block
        :param offset: a byte offset within that block
        :param val: the value to be stored
        """
        self._concur_mgr.xlock(blk)
        buff = self._my_buffers.get_buffer(blk)
        assert isinstance(buff, BufferSlot)
        lsn = self._recovery_mrg.set_string(buff, offset, val)
        buff.set_string(offset, val, self._txnum, lsn)

    def size(self, filename):
        """
        Returns the number of blocks in the specified file.
        This method first obtains an SLock on the
        "end of the file", before asking the file manager
        to return the file size.
        :param filename: the name of the file
        :return: the number of blocks in the file
        """
        dummyblk = Block(filename, self.__END_OF_FILE)
        self._concur_mgr.slock(dummyblk)
        return SimpleDB.file_mgr().size(filename)

    def append(self, filename, fmtr):
        """
        Appends a new block to the end of the specified file
        and returns a reference to it.
        This method first obtains an XLock on the
        "end of the file", before performing the append.
        :param filename: the name of the file
        :param fmtr: the formatter used to initialize the new page
        :return: a reference to the newly-created disk block
        """
        dummyblk = Block(filename, self.__END_OF_FILE)
        self._concur_mgr.xlock(dummyblk)
        blk = self._my_buffers.pin_new(filename, fmtr)
        self.unpin(blk)
        return blk

    @synchronized
    def __next_tx_number(self):
        Transaction.__next_tx_num += 1
        print("new transaction: " + str(Transaction.__next_tx_num))
        return Transaction.__next_tx_num
