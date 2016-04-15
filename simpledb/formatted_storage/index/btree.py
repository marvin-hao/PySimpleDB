__author__ = 'Marvin'

import sys
import math

from simpledb.plain_storage.file import MaxPage, Block
from simpledb.plain_storage.bufferslot import PageFormatter
from simpledb.formatted_storage.record import TableInfo, RID, Schema
from simpledb.formatted_storage.tx import Transaction
from simpledb.query_prosessor.query import IntConstant, StringConstant, Constant
from simpledb.formatted_storage.index.index import Index
from simpledb.shared_service.macro import *


class BTPageFormatter(PageFormatter):
    """
    An object that can format a page to look like an
    empty B-tree block.
    """
    def __init__(self, ti, flag):
        """
        Creates a formatter for a new page of the
        specified B-tree index.
        :param ti: the index's metadata
        :param flag: the page's initial flag value
        """
        assert isinstance(ti, TableInfo)
        self._ti = ti
        self.flag = flag

    def format(self, page):
        """
        Formats the page by initializing as many index-record slots
        as possible to have default values.
        Each integer field is given a value of 0, and
        each string field is given a value of "".
        The location that indicates the number of records
        in the page is also set to 0.
        """
        assert isinstance(page, MaxPage)
        page.set_int(0, self.flag)
        page.set_int(MaxPage.INT_SIZE, 0)  # #records = 0
        recsize = self._ti.record_length()
        pos = MaxPage.INT_SIZE * 2
        while pos + recsize <= BLOCK_SIZE:
            self.__make_default_record(page, pos)
            pos += recsize

    def __make_default_record(self, page, pos):
        assert isinstance(page, MaxPage)
        for fldname in self._ti.schema().fields():
            offset = self._ti.offset(fldname)
            if self._ti.schema().type(fldname) == INTEGER:
                page.set_int(pos+offset, 0)
            else:
                page.set_string(pos+offset, "")


class BTreePage:
    """
    B-tree directory and leaf pages have many commonalities:
    in particular, their records are stored in sorted order,
    and pages split when full.
    A BTreePage object contains this common functionality.
    """
    def __init__(self, currentblk, ti, tx):
        """
        Opens a page for the specified B-tree block.
        :param currentblk: a reference to the B-tree block
        :param ti: the metadata for the particular B-tree file
        :param tx: the calling transaction
        """
        assert isinstance(currentblk, Block)
        assert isinstance(ti, TableInfo)
        assert isinstance(tx, Transaction)
        self._currentblk = currentblk
        self._ti = ti
        self._tx = tx
        self._slotsize = ti.record_length()
        tx.pin(currentblk)

    def __slotpos(self, slot):
        return MaxPage.INT_SIZE + MaxPage.INT_SIZE + slot * self._slotsize

    def __fldpos(self, slot, fldname):
        offset = self._ti.offset(fldname)
        return self.__slotpos(slot) + offset

    def __get_int(self, slot, fldname):
        pos = self.__fldpos(slot, fldname)
        return self._tx.get_int(self._currentblk, pos)

    def __get_string(self, slot, fldname):
        pos = self.__fldpos(slot, fldname)
        return self._tx.get_string(self._currentblk, pos)

    def __get_val(self, slot, fldname):
        Type = self._ti.schema().type(fldname)
        if Type == INTEGER:
            return IntConstant(self.__get_int(slot, fldname))
        else:
            return StringConstant(self.__get_string(slot, fldname))

    def __set_int(self, slot, fldname, val):
        pos = self.__fldpos(slot, fldname)
        self._tx.set_int(self._currentblk, pos, val)

    def __set_string(self, slot, fldname, val):
        pos = self.__fldpos(slot, fldname)
        self._tx.set_int(self._currentblk, pos, val)

    def __set_val(self, slot, fldname, val):
        assert isinstance(val, Constant)
        Type = self._ti.schema().type(fldname)
        if Type == INTEGER:
            self.__set_int(slot, fldname, val.as_python_val())
        else:
            self.__set_string(slot, fldname, val.as_python_val())

    def set_num_recs(self, n):
        self._tx.set_int(self._currentblk, MaxPage.INT_SIZE, n)

    def get_num_recs(self):
        """
        Returns the number of index records in this page.
        :return: the number of index records in this page
        """
        return self._tx.get_int(self._currentblk, MaxPage.INT_SIZE)

    def __copy_record(self, From, To):
        sch = self._ti.schema()
        for fldname in sch.fields():
            self.__set_val(To, fldname, self.__get_val(From, fldname))

    def __insert(self, slot):
        i = self.get_num_recs()
        while i > slot:
            self.__copy_record(i-1, i)
            i -= 1
        self.set_num_recs(self.get_num_recs() + 1)

    def delete(self, slot):
        """
        Deletes the index record at the specified slot.
        :param slot: the slot of the deleted index record
        """
        i = slot + 1
        while i < self.get_num_recs():
            self.__copy_record(i, i-1)
            i += 1
        self.set_num_recs(self.get_num_recs()-1)

    def __transfer_records(self, slot, dest):
        """
        Transfer all the record after slot-1
        """
        assert isinstance(dest, BTreePage)
        destslot = 0
        while slot < self.get_num_recs():
            dest.__insert(destslot)
            sch = self._ti.schema()
            for fldname in sch.fields():
                dest.__set_val(destslot, fldname, self.__get_val(slot, fldname))
            self.delete(slot)
            destslot += 1

    def get_data_val(self, slot):
        """
        Returns the dataval of the record at the specified slot.
        :param slot: the integer slot of an index record
        :return: the dataval of the record at that slot
        """
        return self.__get_val(slot, "dataval")

    def get_flag(self):
        """
        Returns the value of the page's flag field
        :return: the value of the page's flag field
        """
        return self._tx.get_int(self._currentblk, 0)

    def set_flag(self, val):
        """
        Sets the page's flag field to the specified value
        :param val: the new value of the page flag
        """
        self._tx.set_int(self._currentblk, 0, val)

    def append_new(self, flag):
        """
        Appends a new block to the end of the specified B-tree file,
        having the specified flag value.
        :param flag: the initial value of the flag
        :return: a reference to the newly-created block
        """
        return self._tx.append(self._ti.file_name(), BTPageFormatter(self._ti, flag))

    def close(self):
        """
        Closes the page by unpinning its buffer.
        """
        if not self._currentblk is None:
            self._tx.unpin(self._currentblk)
            self._currentblk = None

    def is_full(self):
        """
        Returns true if the block is full.
        :return: true if the block is full.
        """
        return self.__slotpos(self.get_num_recs() + 1) < MaxPage.BLOCK_SIZE

    def find_slot_befor(self, searchkey):
        """
        Calculates the position where the first record having
        the specified search key should be, then returns
        the position before it.
        :param searchkey: the search key
        :return: the position before where the search key goes
        """
        assert isinstance(searchkey, Constant)
        slot = 0
        while slot < self.get_num_recs() and self.get_data_val(slot) < searchkey:
            slot += 1
        return slot - 1

    def split(self, splitpos, flag):
        """
        Splits the page at the specified position.
        A new page is created, and the records of the page
        starting at the split position are transferred to the new page.
        :param splitpos: the split position
        :param flag: the initial value of the flag field
        :return: the reference to the new block
        """
        newblk = self.append_new(flag)
        newpage = BTreePage(newblk, self._ti, self._tx)
        self.__transfer_records(splitpos, newpage)
        newpage.set_flag(flag)
        newpage.close()
        assert isinstance(newblk, Block)
        return newblk

    # Methods called only by BTreeDir

    def get_child_num(self, slot):
        """
        Returns the block number stored in the index record
        at the specified slot.
        :param slot: the slot of an index record
        :return: the block number stored in that record
        """
        return self.__get_int(slot, "block")

    def insert_dir(self, slot, val, blknum):
        """
        Inserts a directory entry at the specified slot.
        :param slot: the slot of an index record
        :param val: the dataval to be stored
        :param blknum:
        """
        assert isinstance(val, Constant)
        self.__insert(slot)
        self.__set_val(slot, "dataval", val)
        self.__set_int(slot, "block", blknum)

    #  Methods called only by BTreeLeaf

    def get_data_rid(self, slot):
        """
        Returns the dataRID value stored in the specified leaf index record.
        :param slot: the slot of the desired index record
        :return: the dataRID value store at that slot
        """
        return RID(self.__get_int(slot, "block"), self.__get_int(slot, "id"))

    def insert_leaf(self, slot, val, rid):
        assert isinstance(val, Constant)
        assert isinstance(rid, RID)
        self.__insert(slot)
        self.__set_val(slot, "dataval", val)
        self.__set_int(slot, "block", rid.block_number())
        self.__set_int(slot, "id", rid.id())


class DirEntry:
    """
    A directory entry has two components: the number of the child block,
    and the dataval of the first record in that block.
    """
    def __init__(self, dataval, blocknum):
        """
        Creates a new entry for the specified dataval and block number.
        :param dataval: the dataval
        :param blocknum: the block number
        """
        assert isinstance(dataval, Constant)
        self._dataval = dataval
        self._blocknum = blocknum

    def data_val(self):
        """
        Returns the dataval component of the entry
        :return: the dataval component of the entry
        """
        return self._dataval

    def block_number(self):
        """
        Returns the block number component of the entry
        :return: the block number component of the entry
        """
        return self._blocknum


class BTreeLeaf:
    """
     An object that holds the contents of a B-tree leaf block.
    """
    def __init__(self, blk, ti, searchkey, tx):
        """
        Opens a page to hold the specified leaf block.
        The page is positioned immediately before the first record
        having the specified search key (if any).
        :param blk: a reference to the disk block
        :param ti: the metadata of the B-tree leaf file
        :param searchkey: the search key value
        :param tx: the calling transaction
        """
        assert isinstance(blk, Block)
        assert isinstance(ti, TableInfo)
        assert isinstance(searchkey, Constant)
        assert isinstance(tx, Transaction)

        self._ti = ti
        self._tx = tx
        self._searchkey = searchkey
        self._contents = BTreePage(blk, ti, tx)
        self._currentslot = self._contents.find_slot_befor(searchkey)

    def close(self):
        """
        Closes the leaf page
        """
        self._contents.close()

    def next(self):
        """
        Moves to the next leaf record having the
        previously-specified search key.
        Returns false if there is no more such records.
        :return: false if there are no more leaf records for the search key
        """
        self._currentslot += 1
        if self._currentslot >= self._contents.get_num_recs():
            return self.__try_over_flow()
        elif self._contents.get_data_val(self._currentslot) == self._searchkey:
            return True
        else:
            return self.__try_over_flow()

    def get_data_rid(self):
        """
        Returns the dataRID value of the current leaf record.
        :return: the dataRID of the current record
        """
        return self._contents.get_data_rid(self._currentslot)

    def delete(self, data_rid):
        """
        Deletes the leaf record having the specified dataRID
        :param data_rid: the dataRId whose record is to be deleted
        """
        while self.next():
            if self.get_data_rid() == data_rid:
                self._contents.delete(self._currentslot)

    def insert(self, data_rid):
        """
        Inserts a new leaf record having the specified dataRID
        and the previously-specified search key.
        If the record does not fit in the page, then
        the page splits and the method returns the
        directory entry for the new page;
        otherwise, the method returns null.
        If all of the records in the page have the same dataval,
        then the block does not split; instead, all but one of the
        records are placed into an overflow block.
        :param data_rid: the dataRID value of the new record
        :return: the directory entry of the newly-split page, if one exists.
        """
        assert isinstance(data_rid, RID)
        # bug fix:  If the page has an overflow page
        # and the searchkey of the new record would be lowest in its page,
        # we need to first move the entire contents of that page to a new block
        # and then insert the new record in the now-empty current page.
        if self._contents.get_flag() >= 0 and self._contents.get_data_val(0) > self._searchkey:
            firstval = self._contents.get_data_val(0)
            newblk = self._contents.split(0, self._contents.get_flag())
            self._currentslot = 0
            self._contents.set_flag(-1)
            self._contents.insert_leaf(self._currentslot, self._searchkey, data_rid)
            return DirEntry(firstval, newblk.number())

        self._currentslot += 1
        self._contents.insert_leaf(self._currentslot, self._searchkey, data_rid)
        if not self._contents.is_full():  # <------------
            return None

        # else page is full, so split it
        firstkey = self._contents.get_data_val(0)
        lastkey = self._contents.get_data_val(self._contents.get_num_recs() - 1)
        if lastkey == firstkey:
            # create an overflow block to hold all but the first record
            newblk = self._contents.split(1, self._contents .get_flag())
            self._contents.set_flag(newblk.number())  # <----------
            return None
        else:
            splitpos = self._contents.get_num_recs() // 2
            splitkey = self._contents.get_data_val(splitpos)
            if splitkey == firstkey:
                # move right, looking for the next key
                while self._contents.get_data_val(splitpos) == splitkey:
                    splitpos += 1
                splitkey = self._contents.get_data_val(splitpos)
            else:
                # move left, looking for first entry having that key
                while self._contents.get_data_val(splitpos - 1) == splitkey:
                    splitpos -= 1
            newblk = self._contents.split(splitpos, -1)
            return DirEntry(splitkey, newblk.number())

    def __try_over_flow(self):
        firstkey = self._contents.get_data_val(0)
        flag = self._contents.get_flag()
        if self._searchkey != firstkey or flag < 0:
            return False
        self._contents.close()
        nextblk = Block(self._ti.file_name(), flag)
        self._contents = BTreePage(nextblk, self._ti, self._tx)
        self._currentslot = 0
        return True


class BTreeDir:
    """
    A B-tree directory block.
    """
    def __init__(self, blk, ti, tx):
        """
        Creates an object to hold the contents of the specified
        B-tree block.
        :param blk: a reference to the specified B-tree block
        :param ti: the metadata of the B-tree directory file
        :param tx: the calling transaction
        """
        assert isinstance(blk, Block)
        assert isinstance(ti, TableInfo)
        assert isinstance(tx, Transaction)

        self._ti = ti
        self._tx = tx
        self._filename = blk.file_name()
        self._contents = BTreePage(blk, ti, tx)

    def __find_child_block(self, searchkey):
        slot = self._contents.find_slot_befor(searchkey)
        if self._contents.get_data_val(slot + 1) == searchkey:
            slot += 1
        blknum = self._contents.get_child_num(slot)
        return Block(self._filename, blknum)

    def __insert_entry(self, e):
        assert isinstance(e, DirEntry)
        newslot = 1 + self._contents.find_slot_befor(e.data_val())
        self._contents.insert_dir(newslot, e.data_val(), e.block_number())
        if not self._contents.is_full():
            return None
        level = self._contents.get_flag()
        splitpos = self._contents.get_num_recs() // 2
        splitval = self._contents.get_data_val(splitpos)
        newblk = self._contents.split(splitpos, level)
        return DirEntry(splitval, newblk.number())

    def close(self):
        """
        Closes the directory page.
        """
        self._contents.close()

    def search(self, searchkey):
        """
        Returns the block number of the B-tree leaf block
        that contains the specified search key.
        :param searchkey: the search key value
        :return: the block number of the leaf block containing that search key
        """
        childblk = self.__find_child_block(searchkey)
        while self._contents.get_flag() > 0:
            self._contents.close()
            self._contents = BTreePage(childblk, self._ti, self._tx)
            childblk = self.__find_child_block(searchkey)
        return childblk.number()

    def make_new_root(self, e):
        """
        Creates a new root block for the B-tree.
        The new root will have two children:
        the old root, and the specified block.
        Since the root must always be in block 0 of the file,
        the contents of the old root will get transferred to a new block.
        :param e: the directory entry to be added as a child of the new root
        """
        assert isinstance(e, DirEntry)
        firstval = self._contents.get_data_val(0)
        level = self._contents.get_flag()
        newblk = self._contents.split(0, level)
        oldroot = DirEntry(firstval, newblk.number())
        self.__insert_entry(oldroot)
        self.__insert_entry(e)
        self._contents.set_flag(level + 1)

    def insert(self, e):
        """
        Inserts a new directory entry into the B-tree block.
        If the block is at level 0, then the entry is inserted there.
        Otherwise, the entry is inserted into the appropriate
        child node, and the return value is examined.
        A non-null return value indicates that the child node
        split, and so the returned entry is inserted into
        this block.
        If this block splits, then the method similarly returns
        the entry information of the new block to its caller;
        otherwise, the method returns null.
        :param e: the directory entry to be inserted
        :return: the directory entry of the newly-split block, if one exists; otherwise, null
        """
        assert isinstance(e, DirEntry)
        if self._contents.get_flag() == 0:
            return self.__insert_entry(e)
        else:
            childblk = self.__find_child_block(e.data_val())
            child = BTreeDir(childblk, self._ti, self._tx)
            myentry = child.insert(e)
            child.close()
            return self.__insert_entry(myentry) if not myentry is None else None


class BTreeIndex(Index):
    """
    A B-tree implementation of the Index interface.
    """
    def __init__(self, idxname, leafsch, tx):
        """
        Opens a B-tree index for the specified index.
        The method determines the appropriate files
        for the leaf and directory records,
        creating them if they did not exist.
        :param idxname: the name of the index
        :param leafsch: the schema of the leaf index records
        :param tx: the calling transaction
        """
        assert isinstance(leafsch, Schema)
        assert isinstance(tx, Transaction)
        self._tx = tx
        self._leaf = None

        # deal with the leaves

        leaftbl = idxname + "leaf"
        self._leaf_ti = TableInfo(leaftbl, leafsch)
        if tx.size(self._leaf_ti.file_name() == 0):
            tx.append(self._leaf_ti.file_name(), BTPageFormatter(self._leaf_ti, -1))

        # deal with the directory

        dirsch = Schema()
        dirsch.add("block", leafsch)
        dirsch.add("dataval", leafsch)
        dirtbl = idxname + "dir"
        self._dir_ti = TableInfo(dirtbl, dirsch)
        self._rootblk = Block(self._dir_ti.file_name(), 0)
        if tx.size(self._dir_ti.file_name() == 0):

            # create new root block

            tx.append(self._dir_ti.file_name(), BTPageFormatter(self._dir_ti, 0))
        page = BTreePage(self._rootblk, self._dir_ti, tx)
        if page.get_num_recs() == 0:

            # insert initial directory entry

            fldtype = dirsch.type("dataval")
            minval = IntConstant(-sys.maxsize) if fldtype == INTEGER else StringConstant("")
            page.insert_dir(0, minval, 0)
        page.close()

    def close(self):
        """
        Closes the index by closing its open leaf page, if necessary.
        """
        if not self._leaf is None:
            assert isinstance(self._leaf, BTreeLeaf)
            self._leaf.close()

    def before_first(self, search_key):
        """
        Traverses the directory to find the leaf block corresponding
        to the specified search key.
        The method then opens a page for that leaf block, and
        positions the page before the first record (if any)
        having that search key.
        The leaf page is kept open, for use by the methods next
        and getDataRid.
        """
        self.close()
        root = BTreeDir(self._rootblk, self._dir_ti, self._tx)
        blknum = root.search(search_key)
        root.close()
        leafblk = Block(self._leaf_ti.file_name(), blknum)
        self._leaf = BTreeLeaf(leafblk, self._leaf_ti, search_key, self._tx)

    def next(self):
        """
        Moves to the next leaf record having the
        previously-specified search key.
        Returns false if there are no more such leaf records.
        """
        if not self._leaf is None:
            return self._leaf.next()

    def get_data_rid(self):
        """
        Returns the dataRID value from the current leaf record.
        """
        if not self._leaf is None:
            return self.get_data_rid()

    def delete(self, data_val, data_rid):
        """
        Deletes the specified index record.
        The method first traverses the directory to find
        the leaf page containing that record; then it
        deletes the record from the page.
        """
        assert isinstance(data_val, Constant)
        assert isinstance(data_rid, RID)
        self.before_first(data_val)
        self._leaf.delete(data_rid)
        self._leaf.close()

    def insert(self, data_val, data_rid):
        """
        Inserts the specified record into the index.
        The method first traverses the directory to find
        the appropriate leaf page; then it inserts
        the record into the leaf.
        If the insertion causes the leaf to split, then
        the method calls insert on the root,
        passing it the directory entry of the new leaf page.
        If the root node splits, then makeNewRoot is called.
        """
        self.before_first(data_val)
        e = self._leaf.insert(data_rid)
        self._leaf.close()
        if e is None:
            return
        root = BTreeDir(self._rootblk, self._dir_ti, self._tx)
        e2 = root.insert(e)
        if not e2 is None:
            root.make_new_root(e2)
        root.close()

    @staticmethod
    def search_cost(numblocks, rpb):
        """
        Estimates the number of block accesses
        required to find all index records having
        a particular search key.
        :param numblocks: the number of blocks in the B-tree directory
        :param rpb: the number of index entries per block
        :return: the estimated traversal cost
        """
        return 1 + int(math.log(numblocks) / math.log(rpb))





