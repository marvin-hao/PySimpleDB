__author__ = 'Marvin'

from simpledb.formatted_storage.log import BasicLogRecord
from simpledb.plain_storage.bufferslot import *


class LogRecord:
    """
    The interface implemented by each type of log record.
    The six different types of log record
    """
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5
    log_mgr = SimpleDB.log_mgr()

    def write_to_log(self):
        """
        Writes the record to the log and returns its LSN.
        :return: the LSN of the record in the log
        """
        raise NotImplementedError()

    def op(self):
        """
        Returns the transaction id stored with the log record.
        :return: the log record's transaction id
        """
        raise NotImplementedError()

    def tx_number(self):
        """
        Returns the transaction id stored with the log record.
        :return: the log record's transaction id
        """
        raise NotImplementedError()

    def undo(self, txnum):
        """
        Undoes the operation encoded by this log record.
        The only log record types for which this method
        does anything interesting are SETINT and SETSTRING.
        :param txnum: the id of the transaction that is performing the undo.
        """
        raise NotImplementedError()


class SetIntRecord(LogRecord):
    def __init__(self, txnum=None, blk=None, offset=None, val=None, rec=None):
        """
        Creates a new setint log record.
        Or creates a log record by reading five other values from the log.
        :param txnum: the ID of the specified transaction
        :param blk: the block containing the value
        :param offset: the offset of the value in the block
        :param val: the new value
        :param rec: the basic log record
        """
        if rec is None:
            assert isinstance(blk, Block)
            self._txnum = txnum
            self._blk = blk
            self._offset = offset
            self._val = val
        else:
            assert isinstance(rec, BasicLogRecord)
            self._txnum = rec.next_int()
            filename = rec.next_string()
            blknum = rec.next_int()
            self._blk = Block(filename, blknum)
            self._offset = rec.next_int()
            self._val = rec.next_int()

    def write_to_log(self):
        """
        Writes a setInt record to the log.
        This log record contains the SETINT operator,
        followed by the transaction id, the filename, number,
        and offset of the modified block, and the previous
        integer value at that offset.
        :return: the LSN of the last log value
        """
        rec = [self.SETINT, self._txnum, self._blk.file_name(),
               self._blk.number(), self._offset, self._val]
        return self.log_mgr.append(rec)

    def op(self):
        return self.SETINT

    def tx_number(self):
        return self._txnum

    def __str__(self):
        return "<SETINT " + str(self._txnum) + " " + str(self._blk) + " " + str(self._offset) + " " + self._val + ">"

    def undo(self, txnum):
        """
        Replaces the specified data value with the value saved in the log record.
        The method pins a buffer to the specified block,
        calls setInt to restore the saved value
        (using a dummy LSN), and unpins the buffer.
        """
        buff_mgr = SimpleDB.buffer_mgr()
        assert isinstance(buff_mgr, BufferMgr)
        buff = buff_mgr.pin(self._blk)
        buff.set_int(self._offset, self._val, self._txnum, -1)
        buff_mgr.unpin(buff)


class SetStringRecord(LogRecord):
    def __init__(self, txnum=None, blk=None, offset=None, val=None, rec=None):
        """
        Creates a new setstring log record.
        Or creates a log record by reading five other values from the log.
        :param txnum: the ID of the specified transaction
        :param blk: the block containing the value
        :param offset: the offset of the value in the block
        :param val: the new value
        :param rec: the basic log record
        """
        if rec is None:
            assert isinstance(blk, Block)
            assert isinstance(val, str)
            self._txnum = txnum
            self._blk = blk
            self._offset = offset
            self._val = val
        else:
            assert isinstance(rec, BasicLogRecord)
            self._txnum = rec.next_int()
            filename = rec.next_string()
            blknum = rec.next_int()
            self._blk = Block(filename, blknum)
            self._offset = rec.next_int()
            self._val = rec.next_string()

    def write_to_log(self):
        """
        Writes a setString record to the log.
        This log record contains the SETSTRING operator,
        followed by the transaction id, the filename, number,
        and offset of the modified block, and the previous
        string value at that offset.
        :return: the LSN of the last log value
        """
        rec = [self.SETSTRING, self._txnum, self._blk.file_name(), self._blk.number(), self._offset, self._val]
        return self.log_mgr.append(rec)

    def op(self):
        return self.SETSTRING

    def tx_number(self):
        return self._txnum

    def __str__(self):
        return "<SETSTRING " + str(self._txnum) + " " + str(self._blk) + " " + str(self._offset) + \
               " " + str(self._val) + ">"

    def undo(self, txnum):
        """
        Replaces the specified data value with the value saved in the log record.
        The method pins a buffer to the specified block,
        calls setString to restore the saved value
        (using a dummy LSN), and unpins the buffer.
        """
        buff_mgr = SimpleDB.buffer_mgr()
        assert isinstance(buff_mgr, BufferMgr)
        buff = buff_mgr.pin(self._blk)
        buff.set_string(self._offset, self._val, txnum, -1)
        buff_mgr.unpin(buff)


class CheckpointRecord(LogRecord):
    """
    The CHECKPOINT log record.
    """
    def __init__(self, rec=None):
        """
        Creates a quiescent checkpoint record.
        Or creates a log record by reading no other values
        from the basic log record.
        :param rec: the basic log record
        """
        if rec is None:
            pass
        else:
            pass

    def write_to_log(self):
        """
        Writes a checkpoint record to the log.
        This log record contains the CHECKPOINT operator,
        and nothing else.
        :return: the LSN of the last log value
        """
        rec = [self.CHECKPOINT]
        return self.log_mgr.append(rec)

    def op(self):
        return self.CHECKPOINT

    def tx_number(self):
        """
        Checkpoint records have no associated transaction,
        and so the method returns a "dummy", negative txid.
        """
        return -1  # dummy value

    def __str__(self):
        return "<CHECKPOINT>"

    def undo(self, txnum):
        """
        Does nothing, because a checkpoint record
        contains no undo information.
        """
        pass


class CommitRecord(LogRecord):
    """
    The COMMIT log record
    """
    def __init__(self, txnum=None, rec=None):
        """
        Creates a new commit log record for the specified transaction.
        Or creates a log record by reading one other value from the log.
        :param txnum: the ID of the specified transaction
        :param rec: the basic log record
        """
        if rec is None:
            self._txnum = txnum
        else:
            assert isinstance(rec, BasicLogRecord)
            self._txnum = rec.next_int()

    def write_to_log(self):
        """
        Writes a commit record to the log.
        This log record contains the COMMIT operator,
        followed by the transaction id.
        :return: the LSN of the last log value
        """
        rec = [self.COMMIT, self._txnum]
        return self.log_mgr.append(rec)

    def op(self):
        return self.COMMIT

    def tx_number(self):
        return self._txnum

    def __str__(self):
        return "<COMMIT " + str(self._txnum) + ">"

    def undo(self, txnum):
        """
        Does nothing, because a commit record contains no undo information.
        """
        pass


class RollbackRecord(LogRecord):
    """
    The ROLLBACK log record.
    """
    def __init__(self, txnum=None, rec=None):
        """
        Creates a new rollback log record for the specified transaction.
        Or creates a log record by reading one other value from the log.
        :param txnum: the ID of the specified transaction
        :param rec: the basic log record
        """
        if rec is None:
            self._txnum = txnum
        else:
            assert isinstance(rec, BasicLogRecord)
            self._txnum = rec.next_int()

    def write_to_log(self):
        """
        Writes a rollback record to the log.
        This log record contains the ROLLBACK operator,
        followed by the transaction id.
        :return: the LSN of the last log value
        """
        rec = [self.ROLLBACK, self._txnum]
        return self.log_mgr.append(rec)

    def op(self):
        return self.ROLLBACK

    def tx_number(self):
        return self._txnum

    def __str__(self):
        return "<ROLLBACK " + str(self._txnum) + ">"

    def undo(self, txnum):
        """
        Does nothing, because a rollback record contains no undo information.
        """
        pass


class StartRecord(LogRecord):
    def __init__(self, txnum=None, rec=None):
        """
        Creates a start rollback log record for the specified transaction.
        Or creates a log record by reading one other value from the log.
        :param txnum: the ID of the specified transaction
        :param rec: the basic log record
        """
        if rec is None:
            self._txnum = txnum
        else:
            assert isinstance(rec, BasicLogRecord)
            self._txnum = rec.next_int()

    def write_to_log(self):
        """
        Writes a start record to the log.
        This log record contains the START operator,
        followed by the transaction id.
        :return: the LSN of the last log value
        """
        rec = [self.START, self._txnum]
        return self.log_mgr.append(rec)

    def op(self):
        return self.START

    def tx_number(self):
        return self._txnum

    def __str__(self):
        return "<START " + str(self._txnum) + ">"

    def undo(self, txnum):
        """
        Does nothing, because a start record contains no undo information.
        """
        pass


class LogRecordIterator:
    """
    A class that provides the ability to read records from the log in reverse order.
    Unlike the similar class LogIterator, this class understands the meaning of the log records.
    """
    def __init__(self):
        self._iter = SimpleDB.log_mgr().iterator()

    def has_next(self):
        return self._iter.has_next()

    def next(self):
        rec = self._iter.next()
        assert isinstance(rec, BasicLogRecord)
        op = rec.next_int()
        if op == LogRecord.CHECKPOINT:
            return CheckpointRecord(rec=rec)
        elif op == LogRecord.START:
            return StartRecord(rec=rec)
        elif op == LogRecord.COMMIT:
            return CommitRecord(rec=rec)
        elif op == LogRecord.ROLLBACK:
            return RollbackRecord(rec=rec)
        elif op == LogRecord.SETINT:
            return SetIntRecord(rec=rec)
        elif op == LogRecord.SETSTRING:
            return SetStringRecord(rec=rec)
        else:
            return None


class RecoveryMgr:
    """
    The recovery manager.  Each transaction has its own recovery manager.
    """
    def __init__(self, txnum):
        """
        Creates a recovery manager for the specified transaction.
        :param txnum: the ID of the specified transaction
        """
        self._txnum = txnum
        StartRecord(txnum).write_to_log()

    def commit(self):
        """
        Writes a commit record to the log, and flushes it to disk.
        """
        SimpleDB.buffer_mgr().flush_all(self._txnum)
        lsn = CommitRecord(self._txnum).write_to_log()
        SimpleDB.log_mgr().flush(lsn)

    def rollback(self):
        """
        Writes a rollback record to the log, and flushes it to disk.
        """
        self.__do_rollback()
        SimpleDB.buffer_mgr().flush_all(self._txnum)
        lsn = CommitRecord(self._txnum).write_to_log()
        SimpleDB.log_mgr().flush(lsn)

    def recover(self):
        """
        Recovers uncompleted transactions from the log,
        then writes a quiescent checkpoint record to the log and flushes it.
        """
        self.__do_recover()
        SimpleDB.buffer_mgr().flush_all(self._txnum)
        lsn = CheckpointRecord().write_to_log()
        SimpleDB.log_mgr().flush(lsn)

    def set_int(self, buff, offset, newval):
        """
        Writes a setint record to the log, and returns its lsn.
        Updates to temporary files are not logged; instead, a
        "dummy" negative lsn is returned.
        :param buff: the buffer containing the page
        :param offset: the offset of the value in the page
        :param newval: the value to be written
        """
        assert isinstance(buff, BufferSlot)
        oldval = buff.get_int(offset)
        blk = buff.block()
        if self.__is_temp_block(blk):
            return -1
        else:
            return SetIntRecord(self._txnum, blk, offset, oldval).write_to_log()

    def set_string(self, buff, offset, newval):
        """
        Writes a setstring record to the log, and returns its lsn.
        Updates to temporary files are not logged; instead, a
        "dummy" negative lsn is returned.
        :param buff: the buffer containing the page
        :param offset: the offset of the value in the page
        :param newval: the value to be written
        """
        assert isinstance(buff, BufferSlot)
        oldval = buff.get_string(offset)
        blk = buff.block()
        if self.__is_temp_block(blk):
            return -1
        else:
            return SetStringRecord(self._txnum, blk, offset, oldval).write_to_log()

    def __do_rollback(self):
        """
        Rolls back the transaction.
        The method iterates through the log records,
        calling undo() for each log record it finds
        for the transaction,
        until it finds the transaction's START record.
        """
        iterator = LogRecordIterator()
        while iterator.has_next():
            rec = iterator.next()
            if rec.tx_number() == self._txnum:
                if rec.op() == LogRecord.START:
                    return
                rec.undo(self._txnum)

    def __do_recover(self):
        """
        Does a complete database recovery.
        The method iterates through the log records.
        Whenever it finds a log record for an unfinished
        transaction, it calls undo() on that record.
        The method stops when it encounters a CHECKPOINT record
        or the end of the log.
        """
        finished_txs = []
        iterator = LogRecordIterator()
        while iterator.has_next():
            rec = iterator.next()
            assert isinstance(rec, LogRecord)
            if rec.op() == LogRecord.CHECKPOINT:
                return
            if rec.op() == LogRecord.COMMIT or rec.op() == LogRecord.ROLLBACK:
                finished_txs.append(rec.tx_number())
            elif not rec.tx_number() in finished_txs:
                rec.undo(self._txnum)

    def __is_temp_block(self, blk):
        """
        Determines whether a block comes from a temporary file or not.
        """
        assert isinstance(blk, Block)
        return blk.file_name().startswith("temp")







