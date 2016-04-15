__author__ = 'Marvin'
import time
import threading
current_milli_time = lambda: int(round(time.time() * 1000))
from simpledb.plain_storage.file import Block


class LockAbortException(Exception):
    """
    A runtime exception indicating that the transaction
    needs to abort because a lock could not be obtained.
    """
    pass


class PageLockTable:
    """
    The lock table, which provides methods to lock and unlock blocks.
    If a transaction requests a lock that causes a conflict with an
    existing lock, then that transaction is placed on a wait list.
    There is only one wait list for all blocks.
    When the last lock on a block is unlocked, then all transactions
    are removed from the wait list and rescheduled.
    If one of those transactions discovers that the lock it is waiting for
    is still locked, it will place itself back on the wait list.
    """
    MAX_TIME = 10000

    def __init__(self):
        self._locks = {}
        self._cv = threading.Condition()

    def slock(self, blk):
        """
        Grants an SLock on the specified block.
        If an XLock exists when the method is called,
        then the calling thread will be placed on a wait list
        until the lock is released.
        If the thread remains on the wait list for a certain
        amount of time (currently 10 seconds),
        then an exception is thrown.
        :param blk: a reference to the disk block
        """
        assert isinstance(blk, Block)
        try:
            self._cv.acquire()
            timestamp = current_milli_time()
            while self.__has_xlock(blk) and not self.__waiting_too_long(timestamp):
                self._cv.wait(PageLockTable.MAX_TIME//1000)
            if self.__has_xlock(blk):
                raise LockAbortException()
            val = self.__get_lock_val(blk)  # will not be negative
            self._locks[blk] = val + 1
            self._cv.release()
        except Exception:
            raise LockAbortException()

    def xlock(self, blk):
        """
        Grants an XLock on the specified block.
        If a lock of any type exists when the method is called,
        then the calling thread will be placed on a wait list
        until the locks are released.
        If the thread remains on the wait list for a certain
        amount of time (currently 10 seconds),
        then an exception is thrown.
        :param blk: a reference to the disk block
        """
        assert isinstance(blk, Block)
        try:
            self._cv.acquire()
            timestamp = current_milli_time()
            while self.__has_other_slocks(blk) and not self.__waiting_too_long(timestamp):
                self._cv.wait(PageLockTable.MAX_TIME//1000)
            if self.__has_other_slocks(blk):
                raise LockAbortException()
            self._locks[blk] = -1
            self._cv.release()
        except Exception:
            raise LockAbortException()

    def unlock(self, blk):
        """
        Releases a lock on the specified block.
        If this lock is the last lock on that block,
        then the waiting transactions are notified.
        :param blk: a reference to the disk block
        """
        self._cv.acquire()
        val = self.__get_lock_val(blk)
        if val > 1:
            self._locks[blk] = val-1
        else:
            del self._locks[blk]
            self._cv.notify_all()
            self._cv.release()

    def __has_xlock(self, blk):
        assert isinstance(blk, Block)
        return self.__get_lock_val(blk) < 0

    def __has_other_slocks(self, blk):
        assert isinstance(blk, Block)
        return self.__get_lock_val(blk) > 1

    def __waiting_too_long(self, starttime):
        return current_milli_time()-starttime > PageLockTable.MAX_TIME

    def __get_lock_val(self, blk):
        assert isinstance(blk, Block)
        return self._locks.get(blk, 0)


class PageLockMgr:
    """
    The concurrency manager for the transaction.
    Each transaction has its own concurrency manager.
    The concurrency manager keeps track of which locks the
    transaction currently has, and interacts with the
    global lock table as needed.
    """

    def __init__(self):
        """
        The global lock table.  This variable is static because all transactions
        share the same table.
        """
        self._locktbl = PageLockTable()
        self._locks = {}

    def slock(self, blk):
        """
        Obtains an SLock on the block, if necessary.
        The method will ask the lock table for an SLock
        if the transaction currently has no locks on that block.
        :param blk: a reference to the disk block
        """
        if self._locks.get(blk) is None:
            self._locktbl.slock(blk)
            self._locks[blk] = "S"

    def xlock(self, blk):
        """
        Obtains an XLock on the block, if necessary.
        If the transaction does not have an XLock on that block,
        then the method first gets an SLock on that block
        (if necessary), and then upgrades it to an XLock.
        :param blk: a reference to the disk block
        """
        if not self.__has_xlock(blk):
            self.slock(blk)
            self._locktbl.xlock(blk)
            self._locks[blk] = "X"

    def release(self):
        """
        Releases all locks by asking the lock table to unlock each one.
        """
        for blk in self._locks.keys():
            self._locktbl.unlock(blk)
        self._locks.clear()

    def __has_xlock(self, blk):
        locktype = self._locks.get(blk)
        return not locktype is None and locktype == "X"