__author__ = 'Marvin'
from simpledb.plain_storage.file import MaxPage, Block
from simpledb.plain_storage.bufferslot import PageFormatter
from simpledb.formatted_storage.tx import Transaction
from simpledb.shared_service.macro import *


class FieldInfo:
    def __init__(self, Type, length):
        self.type = Type
        self.length = length


class Schema:
    """
    The record schema of a table.
    A schema contains the name and type of
    each field of the table, as well as the length
    of each varchar field.
    """
    def __init__(self):
        """
        Creates an empty schema.
        Field information can be added to a schema
        via the five addXXX methods.
        """
        self._info = {}

    def add_field(self, fldname, Type, length):
        """
        Adds a field to the schema having a specified
        name, type, and length.
        If the field type is "integer", then the length
        value is irrelevant.
        :param fldname: the name of the fields in SqlTypes
        :param length: the conceptual length of a string field.
        """
        self._info[fldname] = FieldInfo(Type, length)

    def add_int_field(self, fldname):
        """
        Adds an integer field to the schema.
        :param fldname: the name of the field
        """
        self.add_field(fldname, INTEGER, 0)

    def add_string_field(self, fldname, length):
        """
        Adds a string field to the schema.
        The length is the conceptual length of the field.
        For example, if the field is defined as varchar(8),
        then its length is 8.
        :param fldname: the name of the field
        :param length: the number of chars in the varchar definition
        """
        self.add_field(fldname, VARCHAR, length)

    def add(self, fldname, sch):
        """
        Adds a field to the schema having the same
        type and length as the corresponding field
        in another schema.
        :param fldname: the name of the field
        :param sch: the other schema
        """
        assert isinstance(sch, Schema)
        Type = sch.type(fldname)
        length = sch.length(fldname)
        self.add_field(fldname, Type, length)

    def add_all(self, sch):
        """
        Adds all of the fields in the specified schema to the current schema.
        :param sch: the other schema
        """
        assert isinstance(sch, Schema)
        self._info.update(sch._info)

    def type(self, fldname):
        """
        Returns the type of the specified field, using the constants in SqlTypes.
        :param fldname: the name of the field
        :return: the integer type of the field
        """
        return self._info.get(fldname).type

    def length(self, fldname):
        """
        Returns the conceptual length of the specified field.
        If the field is not a string field, then
        the return value is undefined.
        :param fldname: the name of the field
        :return: the conceptual length of the field
        """
        return self._info.get(fldname).length

    def has_field(self, fldname):
        """
        Returns true if the specified field is in the schema
        :param fldname: the name of the field
        :return: true if the field is in the schema
        """
        return fldname in self._info.keys()

    def fields(self):
        """
        Returns a collection containing the name of
        each field in the schema.
        :return: the collection of the schema's field names
        """
        return self._info.keys()


class TableInfo:
    """
    The metadata about a table and its records.
    """
    def __init__(self, tblname, schema, offset=None, recordlen=None):
        """
        Creates a TableInfo object, given a table name
        and schema. The constructor calculates the
        physical offset of each field.
        This constructor is used when a table is created.
        Or,
        creates a TableInfo object from the
        specified metadata.
        This constructor is used when the metadata
        is retrieved from the catalog.
        :param tblname: the name of the table
        :param schema: the schema of the table's records
        :param offset: the already-calculated offsets of the fields within a record
        :param recordlen: the already-calculated length of each record
        """
        assert isinstance(schema, Schema)
        self._schema = schema
        self._tblname = tblname
        if offset is None:
            self._offset = {}
            pos = 0
            for fldname in schema.fields():
                self._offset[fldname] = pos
                pos += self.__length_in_bytes(fldname)
            self._recordlen = pos
        else:
            assert isinstance(offset, dict)
            self._offset = offset
            self._recordlen = recordlen

    def file_name(self):
        """
        Returns the filename assigned to this table.
        Currently, the filename is the table name
        followed by ".tbl".
        :return: the name of the file assigned to the table
        """
        return self._tblname + ".tbl"

    def schema(self):
        """
        Returns the schema of the table's records
        :return: the table's record schema
        """
        return self._schema

    def offset(self, fldname):
        """
        Returns the offset of a specified field within a record
        :param fldname: the name of the field
        :return: the offset of that field within a record
        """
        return self._offset.get(fldname)

    def record_length(self):
        """
        Returns the length of a record, in bytes.
        :return: the length in bytes of a record
        """
        return self._recordlen

    def __length_in_bytes(self, fldname):
        """
        Returns the length of a record, in bytes.
        :return: the length in bytes of a record
        """
        fldtype = self._schema.type(fldname)
        if fldtype == INTEGER:
            return MaxPage.INT_SIZE
        else:
            return MaxPage.str_size(self._schema.length(fldname))


class RecordFormatter(PageFormatter):
    """
    An object that can format a page to look like a block of empty records.
    """
    def __init__(self, ti):
        """
        Creates a formatter for a new page of a table.
        :param ti: the table's metadata
        """
        assert isinstance(ti, TableInfo)
        self._ti = ti

    def format(self, page):
        """
        Formats the page by allocating as many record slots
        as possible, given the record length.
        Each record slot is assigned a flag of EMPTY.
        Each integer field is given a value of 0, and
        each string field is given a value of "".
        """
        assert isinstance(page, MaxPage)
        recsize = self._ti.record_length() + MaxPage.INT_SIZE
        pos = 0
        while pos + recsize <= MaxPage.BLOCK_SIZE:
            page.set_int(pos, RecordPage.EMPTY)
            self.__make_default_record(page, pos)
            pos += recsize

    def __make_default_record(self, page, pos):
        assert isinstance(page, MaxPage)
        for fldname in self._ti.schema().fields():
            offset = self._ti.offset(fldname)
            if self._ti.schema().type(fldname) == INTEGER:
                page.set_int(pos + MaxPage.INT_SIZE + offset, 0)  # INT_SIZE if left for EMPTY
            else:
                page.set_string(pos + MaxPage.INT_SIZE + offset, "")
        # after formatting, the page is actually blank


class RecordPage:
    """
    Manages the placement and plain_storage of records in a block.
    This class is used in Block level
    """
    EMPTY = 0
    INUSE = 1

    def __init__(self, blk, ti, tx):
        """
        Creates the record manager for the specified block.
        The current record is set to be prior to the first one.
        :param blk: a reference to the disk block
        :param ti: the table's metadata
        :param tx: the transaction performing the operations
        """
        assert isinstance(blk, Block)
        assert isinstance(ti, TableInfo)
        assert isinstance(tx, Transaction)
        self._blk = blk
        self._ti = ti
        self._tx = tx
        self._slotsize = ti.record_length() + MaxPage.INT_SIZE
        self._currentslot = -1
        tx.pin(blk)

    def close(self):
        """
        Closes the manager, by unpinning the block.
        """
        if not self._blk is None:
            self._tx.unpin(self._blk)
            self._blk = None

    def next(self):
        """
        Moves to the next record in the block.
        :return: false if there is no next record
        """
        return self.__search_for(self.INUSE)

    def get_int(self, fldname):
        """
        Returns the integer value stored for the
        specified field of the current record.
        :param fldname: the name of the field.
        :return: the integer stored in that field
        """
        position = self.__fieldpos(fldname)
        return self._tx.get_int(self._blk, position)

    def get_string(self, fldname):
        """
        Returns the string value stored for the
        specified field of the current record.
        :param fldname: the name of the field.
        :return: the string stored in that field
        """
        position = self.__fieldpos(fldname)
        return self._tx.get_string(self._blk, position)

    def set_int(self, fldname, val):
        """
        Stores an integer at the specified field of the current record.
        :param fldname: the name of the field
        :param val: the integer value stored in that field
        """
        position = self.__fieldpos(fldname)
        self._tx.set_int(self._blk, position, val)

    def set_string(self, fldname, val):
        """
        Stores a string at the specified field of the current record.
        :param fldname: the name of the field
        :param val: the string value stored in that field
        """
        position = self.__fieldpos(fldname)
        self._tx.set_string(self._blk, position, val)

    def delete(self):
        """
        Deletes the current record.
        Deletion is performed by just marking the record
        as "deleted"; the current record does not change.
        To get to the next record, call next().
        """
        position = self.__currentpos()
        self._tx.set_int(self._blk, position, self.EMPTY)

    def insert(self):
        """
        Inserts a new, blank record somewhere in the page.
        Return false if there were no available slots.
        :return: false if the insertion was not possible
        """
        self._currentslot = -1
        found = self.__search_for(self.EMPTY)
        if found:
            position = self.__currentpos()
            self._tx.set_int(self._blk, position, self.INUSE)  # cannot be used alone
        return found

    def move_to_id(self, ID):
        """
        Sets the current record to be the record having the specified ID.
        :param ID: the ID of the record within the page.
        """
        self._currentslot = ID

    def current_id(self):
        """
        Returns the ID of the current record.
        :return: the ID of the current record
        """
        return self._currentslot

    def __currentpos(self):
        return self._currentslot * self._slotsize

    def __fieldpos(self, fldname):
        offset = MaxPage.INT_SIZE + self._ti.offset(fldname)  # INT_SIZE is forEMPTY of INUSE flag
                                                           # this offset is the relative offset
                                                           # from the starting position of a specific record
        return self.__currentpos() + offset

    def __is_valid_slot(self):
        return self.__currentpos() + self._slotsize <= MaxPage.BLOCK_SIZE

    def __search_for(self, flag):
        self._currentslot += 1
        while self.__is_valid_slot():
            position = self.__currentpos()
            if self._tx.get_int(self._blk, position) == flag:
                return True
            self._currentslot += 1
        return False


class RID:
    """
    An identifier for a record within a file.
    An RID consists of the block number in the file,
    and the ID of the record in that block.
    """
    def __init__(self, blknum, ID):
        """
        Creates a RID for the record having the
        specified ID in the specified block.
        :param blknum: the block number where the record lives
        :param ID: the record's ID
        """
        self._blknum = blknum
        self._id = ID

    def block_number(self):
        """
        Returns the block number associated with this RID.
        :return: the block number
        """
        return self._blknum

    def id(self):
        """
        Returns the ID associated with this RID.
        :return: the ID
        """
        return self._id

    def __eq__(self, other):
        assert isinstance(other, RID)
        return self._blknum == other._blknum and self._id == other._id

    def __ne__(self, other):
        if not isinstance(other, RID):
            return True
        else:
            return self._blknum != other._blknum or self._id != other._id

    def __str__(self):
        return "[" + str(self._blknum) + ", " + str(self._id) + "]"


class RecordFile:
    """
    Manages a file of records.
    There are methods for iterating through the records
    and accessing their contents.
    This class is used in file level
    """
    def __init__(self, ti, tx):
        """
        Constructs an object to manage a file of records.
        If the file does not exist, it is created.
        :param ti: the table metadata
        :param tx: the transaction
        :return:
        """
        assert isinstance(ti, TableInfo)
        assert isinstance(tx, Transaction)
        self._ti = ti
        self._tx = tx
        self._filename = ti.file_name()
        self._rp = None
        self._currentblknum = 0
        if tx.size(self._filename) == 0:
            self.__append_block()
        self.__move_to(0)

    def close(self):
        """
        Closes the record file.
        """
        self._rp.close()

    def before_first(self):
        """
        Positions the current record so that a call to method next
        will wind up at the first record.
        Since every time calling move_to method, a new RecordPage
        object will be created, and the old one is discarded
        """
        self.__move_to(0)

    def next(self):
        """
        Moves to the next record. Returns false if there is no next record.
        :return: false if there is no next record.
        """
        while True:
            if self._rp.next():
                return True
            if self.__at_last_block():  # if there is no more record in current block
                                        # and such block is the last block
                return False
            self.__move_to(self._currentblknum + 1)

    def get_int(self, fldname):
        """
        Returns the value of the specified field in the current record.
        :param fldname: the name of the field
        :return: the integer value at that field
        """
        return self._rp.get_int(fldname)

    def get_string(self, fldname):
        """
        Returns the value of the specified field in the current record.
        :param fldname: the name of the field
        :return: the string value at that field
        """
        return self._rp.get_string(fldname)

    def set_int(self, fldname, val):
        """
        Sets the value of the specified field in the current record.
        :param fldname: the name of the field
        :param val: the new value for the field
        """
        self._rp.set_int(fldname, val)

    def set_string(self, fldname, val):
        """
        Sets the value of the specified field in the current record.
        :param fldname: the name of the field
        :param val: the new value for the field
        """
        self._rp.set_string(fldname, val)

    def delete(self):
        """
        Deletes the current record.
        The client must call next() to move to
        the next record.
        Calls to methods on a deleted record  <--------
        have unspecified behavior.
        :return:
        """
        self._rp.delete()

    def insert(self):
        """
        Inserts a new, blank record somewhere in the file
        beginning at the current record.
        If the new record does not fit into an existing block,
        then a new block is appended to the file.
        Must be used combined with set_int or set_string
        """
        while not self._rp.insert():
            if self.__at_last_block():
                self.__append_block()
            self.__move_to(self._currentblknum + 1)

    def move_to_rid(self, rid):
        """
        Positions the current record as indicated by the specified RID.
        :param rid: a record identifier
        """
        assert isinstance(rid, RID)
        self.__move_to(rid.block_number())
        self._rp.move_to_id(rid.id())

    def current_rid(self):
        """
        Returns the RID of the current record.
        :return: a record identifier
        """
        ID = self._rp.current_id()
        return RID(self._currentblknum, ID)

    def __move_to(self, b):
        if not self._rp is None:
            assert isinstance(self._rp, RecordPage)
            self._rp.close()
        self._currentblknum = b
        blk = Block(self._filename, self._currentblknum)
        self._rp = RecordPage(blk, self._ti, self._tx)

    def __at_last_block(self):
        return self._currentblknum == (self._tx.size(self._filename) - 1)

    def __append_block(self):
        fmtr = RecordFormatter(self._ti)
        self._tx.append(self._filename, fmtr)