__author__ = 'Marvin'
from simpledb.formatted_storage.tx import Transaction
from simpledb.formatted_storage.record import Schema, TableInfo, RecordFile
from simpledb.shared_service.server import SimpleDB
from simpledb.shared_service.util import synchronized
from simpledb.formatted_storage.index.index import Index
from simpledb.formatted_storage.index.hash import HashIndex
from simpledb.plain_storage.file import MaxPage
from simpledb.shared_service.macro import *


class StatInfo:
    """
    Holds three pieces of statistical information about a table:
    the number of blocks, the number of records,
    and the number of distinct values for each field.
    """
    def __init__(self, num_blocks, num_recs):
        """
        Creates a StatInfo object.
        Note that the number of distinct values is not
        passed into the constructor.
        The object fakes this value.
        :param num_blocks: the number of blocks in the table
        :param num_recs: the number of records in the table
        """
        self._num_blocks = num_blocks
        self._num_recs = num_recs

    def blocks_accessed(self):
        """
        Returns the estimated number of blocks in the table.
        :return: the estimated number of blocks in the table
        """
        return self._num_blocks

    def records_output(self):
        """
        Returns the estimated number of records in the table.
        :return: the estimated number of records in the table.
        """
        return self._num_recs

    def distinct_values(self, fldname):
        """
        Returns the estimated number of distinct values
        for the specified field.
        :param fldname: the name of the field
        :return: a guess as to the number of distinct field values
        """
        return 1 + self._num_recs // 3


class IndexInfo:
    """
    The information about an index.
    This information is used by the query planner in order to
    estimate the costs of using the index,
    and to obtain the schema of the index records.
    Its methods are essentially the same as those of Plan.
    """
    def __init__(self, idxname, tblname, fldname, tx):
        """
        Creates an IndexInfo object for the specified index.
        :param idxname: the name of the index
        :param tblname: the name of the table
        :param fldname: the name of the indexed field
        :param tx: the calling transaction
        """
        assert isinstance(tx, Transaction)
        self._idxname = idxname
        self._fldname = fldname
        self._tx = tx
        self._ti = SimpleDB.md_mgr().get_table_info(tblname, tx)
        self._si = SimpleDB.md_mgr().get_stat_info(tblname, self._ti, tx)

    def __schema(self) -> Schema:
        """
        Returns the schema of the index records.
        The schema consists of the dataRID (which is
        represented as two integers, the block number and the
        record ID) and the dataval (which is the indexed field).
        Schema information about the indexed field is obtained
        via the table's metadata.
        :return the schema of the index records
        """
        sch = Schema()
        sch.add_int_field("block")
        sch.add_int_field("id")
        if self._ti.schema().type(self._fldname) == INTEGER:
            sch.add_int_field("dataval")
        else:
            fldlen = self._ti.schema().length(self._fldname)
            sch.add_string_field("dataval", fldlen)

    def open(self) -> Index:
        """
        Opens the index described by this object.
        :return the Index object associated with this information
        """
        sch = self.__schema()

        # Create new HashIndex for hash indexing

        return HashIndex(self._idxname, sch, self._tx)

    def blocks_accessed(self) -> int:
        """
        Estimates the number of block accesses required to
        find all index records having a particular search key.
        The method uses the table's metadata to estimate the
        size of the index file and the number of index records
        per block.
        It then passes this information to the traversalCost
        method of the appropriate index type,
        which provides the estimate.
        :return the number of block accesses required to traverse the index
        """
        idxti = TableInfo("", self.__schema())
        rpb = MaxPage.BLOCK_SIZE // idxti.record_length()
        numblocks = self._si.recrods_output() // rpb

        # Call HashIndex.search_cost for hash indexing

        return HashIndex.search_cost(numblocks, rpb)

    def records_output(self) -> int:
        """
        Returns the estimated number of records having a
        search key.  This value is the same as doing a select
        query; that is, it is the number of records in the table
        divided by the number of distinct values of the indexed field.
        :return the estimated number of records having a search key
        """
        return self._si.recrods_output() // self._si.distinct_values(self._fldname)

    def distinct_values(self, fname) -> int:
        """
        Returns the distinct values for a specified field
        in the underlying table, or 1 for the indexed field.
        :param fname the specified field
        """
        if self._fldname == fname:
            return 1
        else:
            return min(self._si.distinct_values(self._fldname), self.records_output())


class TableMgr:
    """
    The table manager.
    There are methods to create a table, save the metadata
    in the catalog, and obtain the metadata of a
    previously-created table.
    """

    MAX_NAME = 16  # The maximum number of characters in any
                   # tablename or fieldname.
                   # Currently, this value is 16.

    def __init__(self, is_new, tx):
        """
        Creates a new catalog manager for the database system.
        If the database is new, then the two catalog tables
        are created.
        :param is_new: has the value true if the database is new
        :param tx: the startup transaction
        """
        assert isinstance(tx, Transaction)
        tcat_schema = Schema()
        tcat_schema.add_string_field("tblname", self.MAX_NAME)
        tcat_schema.add_int_field("reclength")
        self._tcat_info = TableInfo("tblcat", tcat_schema)

        fcat_schema = Schema()
        fcat_schema.add_string_field("tblname", self.MAX_NAME)
        fcat_schema.add_string_field("fldname", self.MAX_NAME)
        fcat_schema.add_int_field("type")
        fcat_schema.add_int_field("length")
        fcat_schema.add_int_field("offset")
        self._fcat_info = TableInfo("fldcat", fcat_schema)

        if is_new:
            self.create_table("tblcat", tcat_schema, tx)
            self.create_table("fldcat", fcat_schema, tx)

    def create_table(self, tblname, sch, tx):
        """
        Creates a new table having the specified name and schema.
        :param tblname: the name of the new table
        :param sch: the table's schema
        :param tx: the transaction creating the table
        """
        assert isinstance(sch, Schema)
        assert isinstance(tx, Transaction)
        ti = TableInfo(tblname, sch)

        # insert one record into tblcat

        tcatfile = RecordFile(self._tcat_info, tx)
        tcatfile.insert()
        tcatfile.set_string("tblname", tblname)
        tcatfile.set_int("reclength", ti.record_length())
        tcatfile.close()

        # insert a record into fldcat for each field

        fcatfile = RecordFile(self._fcat_info, tx)
        for fldname in sch.fields():
            fcatfile.insert()
            fcatfile.set_string("tblname", tblname)
            fcatfile.set_string("fldname", fldname)
            fcatfile.set_int("type", sch.type(fldname))
            fcatfile.set_int("length", sch.length(fldname))
            fcatfile.set_int("offset", ti.offset(fldname))
        fcatfile.close()

    def get_table_info(self, tblname, tx):
        """
        Retrieves the metadata for the specified table out of the catalog.
        :param tblname: the name of the table
        :param tx: the transaction
        :return: the table's stored metadata
        """
        assert isinstance(tx, Transaction)
        tcatfile = RecordFile(self._tcat_info, tx)
        reclen = -1
        while tcatfile.next():
            if tcatfile.get_string("tblname") == tblname:
                reclen = tcatfile.get_int("reclength")
                break
        tcatfile.close()

        fcatfile = RecordFile(self._fcat_info, tx)
        sch = Schema()
        offsets = {}

        while fcatfile.next():
            if fcatfile.get_string("tblname") == tblname:
                fldname = fcatfile.get_string("fldname")
                fldtype = fcatfile.get_int("type")
                fldlen = fcatfile.get_int("length")
                offset = fcatfile.get_int("offset")
                offsets[fldname] = offset
                sch.add_field(fldname, fldtype, fldlen)
        fcatfile.close()
        return TableInfo(tblname, sch, offsets, reclen)


class IndexMgr:
    """
    The index manager.
    The index manager has similar functionalty to the table manager.
    """
    def __init__(self, is_new, tblmgr, tx):
        """
        Creates the index manager.
        This constructor is called during system startup.
        If the database is new, then the idxcat table is created.
        :param is_new: indicates whether this is a new database
        :param tx: the system startup transaction
        """
        assert isinstance(tblmgr, TableMgr)
        assert isinstance(tx, Transaction)
        if is_new:
            sch = Schema()
            sch.add_string_field("indexname", TableMgr.MAX_NAME)
            sch.add_string_field("tablename", TableMgr.MAX_NAME)
            sch.add_string_field("fieldname", TableMgr.MAX_NAME)
            tblmgr.create_table("idxcat", sch, tx)
        self._ti = tblmgr.get_table_info("idxcat", tx)

    def create_index(self, idxname, tblname, fldname, tx):
        """
        Creates an index of the specified type for the specified field.
        A unique ID is assigned to this index, and its information
        is stored in the idxcat table.
        :param idxname: the name of the index
        :param tblname: the name of the indexed table
        :param fldname: the name of the indexed field
        :param tx: the calling transaction
        """
        assert isinstance(tx, Transaction)
        rf = RecordFile(self._ti, tx)
        rf.insert()
        rf.set_string("indexname", idxname)
        rf.set_string("tablename", tblname)
        rf.set_string("fieldname", fldname)
        rf.close()

    def get_index_info(self, tblname, tx):
        """
        Returns a map containing the index info for all indexes
        on the specified table.
        :param tblname: the name of the table
        :param tx: the calling transaction
        :return: a map of IndexInfo objects, keyed by their field names
        """
        assert isinstance(tx, Transaction)
        result = {}
        rf = RecordFile(self._ti, tx)
        while rf.next():
            if rf.get_string("tablename") == tblname:
                idxname = rf.get_string("indexname")
                fldname = rf.get_string("fieldname")
                ii = IndexInfo(idxname, tblname, fldname, tx)
                result[fldname] = ii
        rf.close()
        return result


class StatMgr:
    """
    The statistics manager, which is responsible for
    keeping statistical information about each table.
    The manager does not store this information in the database.
    Instead, it calculates this information on system startup,
    and periodically refreshes it.
    """
    def __init__(self, tbl_mgr: TableMgr, tx: Transaction):
        """
        Creates the statistics manager.
        The initial statistics are calculated by
        traversing the entire database.
        :param tbl_mgr:
        :param tx: the startup transaction
        """
        self._tbl_mgr = tbl_mgr
        self._tablestats = {}
        self._numcalls = 0
        self.__refresh_statistics(tx)

    @synchronized
    def __refresh_statistics(self, tx: Transaction):
        self._tablestats = {}
        self._numcalls = 0
        tcatmd = self._tbl_mgr.get_table_info("tblcat", tx)
        tcatfile = RecordFile(tcatmd, tx)
        while tcatfile.next():
            tblname = tcatfile.get_string("tblname")
            md = self._tbl_mgr.get_table_info(tblname, tx)
            si = self.__calc_table_stats(md, tx)
            self._tablestats[tblname] = si
        tcatfile.close()

    @synchronized
    def __calc_table_stats(self, ti: TableInfo, tx: Transaction) -> StatInfo:
        num_recs = 0
        rf = RecordFile(ti, tx)
        numblocks = 0
        while rf.next():
            num_recs += 1
            numblocks = rf.current_rid().block_number() + 1
        rf.close()
        return StatInfo(numblocks, num_recs)

    @synchronized
    def get_stat_info(self, tblname: str, ti: TableInfo, tx: Transaction) -> StatInfo:
        """
        Returns the statistical information about the specified table.
        :param tblname: the name of the table
        :param ti: the table's metadata
        :param tx: the calling transaction
        :return: the statistical information about the table
        """
        self._numcalls += 1
        if self._numcalls > 100:
            self.__refresh_statistics(tx)
        si = self._tablestats.get(tblname)
        if si is None:
            si = self.__calc_table_stats(ti, tx)
            self._tablestats[tblname] = si
        return si


class ViewMgr:
    MAX_VIEWDEF = 80

    def __init__(self, is_new, tbl_mgr: TableMgr, tx: Transaction):
        self._tbl_mgr = tbl_mgr
        if is_new:
            sch = Schema()
            sch.add_string_field("viewname", TableMgr.MAX_NAME)
            sch.add_string_field("viewdef", self.MAX_VIEWDEF)
            tbl_mgr.create_table("viewcat", sch, tx)

    def create_view(self, vname, vdef, tx: Transaction):
        ti = self._tbl_mgr.get_table_info("viewcat", tx)
        rf = RecordFile(ti, tx)
        rf.insert()
        rf.set_string("viewname", vname)
        rf.set_string("viewdef", vdef)
        rf.close()

    def get_view_def(self, vname, tx: Transaction) -> str:
        result = None
        ti = self._tbl_mgr.get_table_info("viewcat", tx)
        rf = RecordFile(ti, tx)
        while rf.next():
            if rf.get_string("viewname") == vname:
                result = rf.get_string("viewdef")
                break
        rf.close()
        return result


class MetaDataMgr:
    _tblmgr = None
    _viewmgr = None
    _statmgr = None
    _idxmgr = None

    def __init__(self, isnew, tx: Transaction):
        MetaDataMgr._tblmgr = TableMgr(isnew, tx)
        MetaDataMgr._viewmgr = ViewMgr(isnew, MetaDataMgr._tblmgr, tx)
        MetaDataMgr._statmgr = StatMgr(MetaDataMgr._tblmgr, tx)
        MetaDataMgr._idxmgr = IndexMgr(isnew, self._tblmgr, tx)

    def create_table(self, tblname, sch: Schema, tx: Transaction):
        MetaDataMgr._tblmgr.create_table(tblname, sch, tx)

    def get_table_info(self, tblname, tx: Transaction) -> TableInfo:
        return MetaDataMgr._tblmgr.get_table_info(tblname, tx)

    def create_view(self, viewname, viewdef, tx: Transaction):
        MetaDataMgr._viewmgr.create_view(viewname, viewdef, tx)

    def get_view_def(self, viewname, tx: Transaction) -> str:
        return MetaDataMgr._viewmgr.get_view_def(viewname, tx)

    def create_index(self, idxname, tblname, fldname, tx: Transaction):
        MetaDataMgr._idxmgr.create_index(idxname, tblname, fldname, tx)

    def get_index_info(self, tblname, tx: Transaction) -> dict:
        return MetaDataMgr._idxmgr.get_index_info(tblname, tx)

    def get_stat_info(self, tblname, ti: TableInfo, tx: Transaction):
        return MetaDataMgr._statmgr.get_stat_info(tblname, ti, tx)
