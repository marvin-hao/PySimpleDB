__author__ = 'Marvin'

import math

from simpledb.formatted_storage.record import *
from simpledb.query_prosessor.query import *
from simpledb.formatted_storage.tx import Transaction
from simpledb.shared_service.util import synchronized


class TempTable:
    """
    A class that creates temporary tables.
    A temporary table is not registered in the catalog.
    The class therefore has a method getTableInfo to return the
    table's metadata.
    """
    next_table_num = 0

    def __init__(self, sch: Schema, tx: Transaction):
        """
        Allocates a name for for a new temporary table
        having the specified schema.
        :param sch: the new table's schema
        :param tx:  the calling transaction
        """
        tblname = self.__next_table_name()
        self._ti = TableInfo(tblname, sch)
        self._tx = tx

    @synchronized
    def __next_table_name(self) -> str:
        TempTable.next_table_num += 1
        return "temp" + str(TempTable.next_table_num)

    def open(self) -> UpdateScan:
        """
        Opens a table scan for the temporary table.
        """
        return TableScan(self._ti, self._tx)

    def get_table_info(self):
        """
        Return the table's metadata.
        :return: the table's metadata
        """
        return self._ti


class MaterializePlan(Plan):
    """
    The Plan class for the materialize operator.
    """
    def __init__(self, srcplan: Plan, tx: Transaction):
        """
        Creates a materialize plan for the specified query.
        :param srcplan: the plan of the underlying query
        :param tx: the calling transaction
        """
        self._srcplan = srcplan
        self._tx = tx

    def schema(self):
        """
        Returns the schema of the materialized table,
        which is the same as in the underlying plan.
        """
        return self._srcplan.schema()

    def distinct_values(self, fldname):
        """
        Returns the number of distinct field values,
        which is the same as in the underlying plan.
        """
        return self._srcplan.distinct_values(fldname)

    def records_output(self):
        """
        Returns the number of records in the materialized table,
        which is the same as in the underlying plan.
        """
        return self._srcplan.records_output()

    def blocks_accessed(self):
        """
        Returns the estimated number of blocks in the
        materialized table.
        It does not include the one-time cost
        of materializing the records.
        """
        # create a dummy TableInfo object to calculate record length
        ti = TableInfo("", self._srcplan.schema())
        rpb = MaxPage.BLOCK_SIZE / ti.record_length()
        return math.ceil(self._srcplan.records_output()/rpb)

    def open(self):
        """
        This method loops through the underlying query,
        copying its output records into a temporary table.
        It then returns a table scan for that table.
        """
        sch = self._srcplan.schema()
        assert isinstance(sch, Schema)
        temp = TempTable(sch, self._tx)
        src = self._srcplan.open()
        assert isinstance(src, Scan)
        dest = temp.open()
        while src.next():
            dest.insert()
            for fldname in sch.fields():
                dest.set_val(fldname, src.get_val(fldname))
        src.close()
        dest.before_first()
        return dest


class RecordComparator:
    """
    A comparator for scans.
    """
    def __init__(self, fields: list):
        """
        Creates a comparator using the specified fields,
        using the ordering implied by its iterator.
        :param fields: a list of field names
        """
        self._fields = fields

    def compare(self, s1: Scan, s2: Scan) -> int:
        """
        Compares the current records of the two specified scans.
        The sort fields are considered in turn.
        When a field is encountered for which the records have
        different values, those values are used as the result
        of the comparison.
        If the two records have the same values for all
        sort fields, then the method returns 0.
        :param s1: the first scan
        :param s2: the second scan
        :return: the result of comparing each scan's current record according to the field list
        """
        for fldname in self._fields:
            val1 = s1.get_val(fldname)
            val2 = s2.get_val(fldname)
            if val1 > val2:
                return 1
            elif val1 < val2:
                return -1
        return 0


class SortScan(Scan):
    """
    The Scan class for the sort operator.
    """
    def __init__(self, runs: list, comp: RecordComparator):
        """
        Creates a sort scan, given a list of 1 or 2 runs.
        If there is only 1 run, then s2 will be null and
        hasmore2 will be false.
        :param runs: the list of runs
        :param comp: the record comparator
        """
        self._s1 = None
        self._s2 = None
        self._currentscan = None
        self._hasmore2 = False
        self._savedposition = []

        self._comp = comp
        assert isinstance(runs[0], UpdateScan)
        self._s1 = runs[0].open()
        self._hasmore1 = self._s1.next()
        if len(runs) > 1:
            self._s2 = runs[1].open()
            self._hasmore2 = self._s2.next()

    def restore_position(self):
        """
        Moves the scan to its previously-saved position.
        """
        rid1 = self._savedposition[0]
        rid2 = self._savedposition[1]
        self._s1.move_rod_rid(rid1)
        if rid2 is not None:
            self._s2.move_to_rid(rid2)

    def save_position(self):
        """
        Saves the position of the current record,
        so that it can be restored at a later time.
        """
        rid1 = self._s1.get_rid()
        rid2 = self._s2.get_rid() if self._s2 is not None else None
        self._savedposition = rid1 + rid2

    def has_field(self, fldname):
        """
        Returns true if the specified field is in the current scan.
        """
        return self._currentscan.has_field(fldname)

    def get_string(self, fldname):
        """
        Gets the string value of the specified field
        of the current scan.
        """
        return self._currentscan.get_string(fldname)

    def get_int(self, fldname):
        """
        Gets the integer value of the specified field
        of the current scan.
        """
        return self._currentscan.get_int(fldname)

    def get_val(self, fldname):
        """
        Gets the Constant value of the specified field
        of the current scan.
        """
        return self._currentscan.get_val(fldname)

    def close(self):
        """
        Closes the two underlying scans.
        """
        self._s1.close()
        if self._s2 is not None:
            self._s2.close()

    def next(self):
        """
        Moves to the next record in sorted order.
        First, the current scan is moved to the next record.
        Then the lowest record of the two scans is found, and that
        scan is chosen to be the new current scan.
        """
        if self._currentscan is not None:
            if self._currentscan == self._s1:
                self._hasmore1 = self._s1.next()
            elif self._currentscan == self._s2:
                self._hasmore2 = self._s2.next()

        if not self._hasmore1 and not self._hasmore2:
            return False
        elif self._hasmore1 and self._hasmore2:
            if self._comp.compare(self._s1, self._s2) < 0:
                self._currentscan = self._s1
            else:
                self._currentscan = self._s2
        elif self._hasmore1:
            self._currentscan = self._s1
        elif self._hasmore2:
            self._currentscan = self._s2

        return True

    def before_first(self):
        """
        Positions the scan before the first record in sorted order.
        Internally, it moves to the first record of each underlying scan.
        The variable currentscan is set to null, indicating that there is
        no current scan.
        """
        self._currentscan = None
        self._s1.before_first()
        self._hasmore1 = self._s1.next()
        if self._s2 is not None:
            self._s2.befor_first()
            self._hasmore2 = self._s2.next()


class SortPlan(Plan):
    """
    The Plan class for the sort operator.
    """
    def __init__(self, p: Plan, sortfields: list, tx: Transaction):
        """
        Creates a sort plan for the specified query.
        :param p: the plan for the underlying query
        :param sortfields: the fields to sort by
        :param tx: the calling transaction
        """
        self._p = p
        self._tx = tx
        self._sch = p.schema()
        self._comp = RecordComparator(sortfields)

    def __copy(self, src: Scan, dest: UpdateScan):
        dest.insert()
        for fldname in self._sch.fields():
            dest.set_val(fldname, src.get_val(fldname))
        return src.next()

    def __merge_two_runs(self, p1: TempTable, p2: TempTable):
        src1 = p1.open()
        src2 = p2.open()
        result = TempTable(self._sch, self._tx)
        dest = result.open()

        hasmore1 = src1.next()
        hasmore2 = src2.next()
        while hasmore1 and hasmore2:
            if self._comp.compare(src1, src2) < 0:
                hasmore1 = self.__copy(src1, dest)
            else:
                hasmore2 = self.__copy(src2, dest)

        if hasmore1:
            while hasmore1:
                hasmore1 = self.__copy(src1, dest)
        else:
            while hasmore2:
                hasmore2 = self.__copy(src2, dest)

        src1.close()
        src2.close()
        dest.close()
        return result

    def __do_a_merge_iteration(self, runs: list):
        result = []
        while len(runs) > 1:
            p1 = runs[0]
            runs.remove(p1)
            p2 = runs[0]
            runs.remove(p2)
            result.append(self.__merge_two_runs(p1, p2))

        if len(runs) == 1:
            result.append(runs[0])
        return result

    def __split_into_runs(self, src: Scan):
        """
        split a scan into a bunch of non-decreasing scans
        """
        temps = []
        src.before_first()
        if not src.next():
            return temps
        currenttemp = TempTable(self._sch, self._tx)
        temps.append(currenttemp)
        currentscan = currenttemp.open()
        while self.__copy(src, currentscan):
            if self._comp.compare(src, currentscan) < 0:
                # start a new run
                currentscan.close()
                currenttemp = TempTable(self._sch, self._tx)
                temps.append(currenttemp)
                currentscan = currenttemp.open()
        currentscan.close()
        return temps

    def schema(self):
        """
        Returns the schema of the sorted table, which
        is the same as in the underlying query.
        """
        return self._sch

    def distinct_values(self, fldname):
        """
        Returns the number of distinct field values in
        the sorted table, which is the same as in
        the underlying query.
        """
        return self._p.distinct_values(fldname)

    def records_output(self):
        """
        Returns the number of records in the sorted table,
        which is the same as in the underlying query.
        """
        return self._p.records_output()

    def blocks_accessed(self):
        """
        Returns the number of blocks in the sorted table,
        which is the same as it would be in a
        materialized table.
        It does not include the one-time cost
        of materializing and sorting the records.
        """
        mp = MaterializePlan(self._p, self._tx)  # not opened; just for analysis
        return mp.blocks_accessed()  # an estimation

    def open(self):
        """
        This method is where most of the action is.
        Up to 2 sorted temporary tables are created,
        and are passed into SortScan for final merging.
        """
        src = self._p.open()
        runs = self.__split_into_runs(src)
        src.close()
        while len(runs) > 2:
            runs = self.__do_a_merge_iteration(runs)
        return SortScan(runs, self._comp)


class MergeJoinScan(Scan):
    """
    The Scan class for the mergejoin operator.
    """
    def __init__(self, s1: Scan, s2: SortScan, fldname1: str, fldname2: str):
        """
        Creates a mergejoin scan for the two underlying sorted scans.
        :param s1: the LHS sorted scan
        :param s2: the RHS sorted scan
        :param fldname1: the LHS join field
        :param fldname2: the RHS join field
        """
        self._s1 = s1
        self._s2 = s2
        self._fldname1 = fldname1
        self._fldname2 = fldname2
        self._joinval = None
        self.before_first()

    def before_first(self):
        """
        Positions the scan before the first record,
        by positioning each underlying scan before
        their first records.
        """
        self._s1.before_first()
        self._s2.before_first()

    def close(self):
        """
        Closes the scan by closing the two underlying scans.
        """
        self._s1.close()
        self._s2.close()

    def get_val(self, fldname):
        """
        Returns the value of the specified field.
        The value is obtained from whichever scan
        contains the field.
        """
        if self._s1.has_field(fldname):
            return self._s1.get_val(fldname)
        else:
            return self._s2.get_val(fldname)

    def get_int(self, fldname):
        """
        Returns the integer value of the specified field.
        The value is obtained from whichever scan
        contains the field.
        """
        if self._s1.has_field(fldname):
            return self._s1.get_int(fldname)
        else:
            return self._s2.get_int(fldname)

    def get_string(self, fldname):
        """
        Returns the string value of the specified field.
        The value is obtained from whichever scan
        contains the field.
        """
        if self._s1.has_field(fldname):
            return self._s1.get_string(fldname)
        else:
            return self._s2.get_string(fldname)

    def has_field(self, fldname):
        """
        Returns true if the specified field is in
        either of the underlying scans.
        """
        return self._s1.has_field(fldname) or self._s2.has_field(fldname)

    def next(self):
        """
        Moves to the next record.  This is where the action is.
        If the next RHS record has the same join value,
        then move to it.
        Otherwise, if the next LHS record has the same join value,
        then reposition the RHS scan back to the first record
        having that join value.
        Otherwise, repeatedly move the scan having the smallest
        value until a common join value is found.
        When one of the scans runs out of records, return false.
        """
        hasmore2 = self._s2.next()
        if hasmore2 and self._s2.get_val(self._fldname2) == self._joinval:
            return True

        hasmore1 = self._s1.next()
        if hasmore1 and self._s1.get_val(self._fldname1) == self._joinval:
            self._s2.restore_position()
            return True

        while hasmore1 and hasmore2:
            v1 = self._s1.get_val(self._fldname1)
            v2 = self._s2.get_val(self._fldname2)
            if v1 < v2:
                hasmore1 = self._s1.next()
            elif v1 > v2:
                hasmore2 = self._s2.next()
            else:
                self._s2.save_position()
                self._joinval = self._s2.get_val(self._fldname2)
                return True

        return False


class MergeJoinPlan(Plan):
    """
    The Plan class for the mergejoin operator.
    """
    def __init__(self, p1: Plan, p2: Plan, fldname1: str, fldname2: str, tx: Transaction):
        """
        Creates a mergejoin plan for the two specified queries.
        The RHS must be materialized after it is sorted,
        in order to deal with possible duplicates.
        :param p1: the LHS query plan
        :param p2: the RHS query plan
        :param fldname1: the LHS join field
        :param fldname2: the RHS join field
        """
        self._fldname1 = fldname1
        sortlist1 = [fldname1]
        self._p1 = SortPlan(p1, sortlist1, tx)

        self._fldname2 = fldname2
        sortlist2 = [fldname2]
        self._p2 = SortPlan(p2, sortlist2, tx)

        self._sch = Schema()
        self._sch.add_all(p1.schema())
        self._sch.add_all(p2.schema())

    def open(self):
        """
        The method first sorts its two underlying scans
        on their join field. It then returns a mergejoin scan
        of the two sorted table scans.
        """
        s1 = self._p1.open()
        s2 = self._p2.open()
        assert isinstance(s2, SortPlan)
        return MergeJoinScan(s1, s2, self._fldname1, self._fldname1)

    def blocks_accessed(self):
        """
        Returns the number of block acceses required to
        mergejoin the sorted tables.
        Since a mergejoin can be preformed with a single
        pass through each table, the method returns
        the sum of the block accesses of the
        materialized sorted tables.
        It does not include the one-time cost
        of materializing and sorting the records.
        """
        return self._p1.blocks_accessed() + self._p2.blocks_accessed()

    def records_output(self):
        """
        Returns the number of records in the join.
        Assuming uniform distribution, the formula is:
        R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
        """
        maxvals = max(self._p1.distinct_values(self._fldname1), self._p2.distinct_values(self._fldname2))
        return self._p1.records_output() * self._p2.records_output() // maxvals

    def distinct_values(self, fldname):
        """
        Estimates the distinct number of field values in the join.
        Since the join does not increase or decrease field values,
        the estimate is the same as in the appropriate underlying query.
        """
        if self._p1.schema().has_field(fldname):
            return self._p1.distinct_values(fldname)
        else:
            return self._p2.distinct_values(fldname)

    def schema(self):
        """
        Returns the schema of the join,
        which is the union of the schemas of the underlying queries.
        """
        return self._sch


class AggregationFn:
    """
    The interface implemented by aggregation functions.
    Aggregation functions are used by the groupby operator.
    """
    def process_first(self, s: Scan):
        """
        Uses the current record of the specified scan
        to be the first record in the group.
        :param s: the scan to aggregate over.
        """
        raise NotImplementedError()

    def process_next(self, s: Scan):
        """
        Uses the current record of the specified scan
        to be the next record in the group.
        :param s: the scan to aggregate over.
        """
        raise NotImplementedError()

    def field_name(self) -> str:
        """
        Returns the name of the new aggregation field.
        :return: the name of the new aggregation field
        """
        raise NotImplementedError()

    def value(self) -> Constant:
        """
        Returns the computed aggregation value.
        :return: the computed aggregation value
        """
        raise NotImplementedError()


class MaxFn(AggregationFn):
    """
    The max aggregation function.
    """
    def __init__(self, fldname: str):
        """
        Creates a max aggregation function for the specified field.
        :param fldname: the name of the aggregated field
        """
        self._fldname = fldname
        self._val = None

    def process_first(self, s: Scan):
        """
        Starts a new maximum to be the
        field value in the current record.
        """
        self._val = s.get_val(self._fldname)

    def process_next(self, s: Scan):
        """
        Replaces the current maximum by the field value
        in the current record, if it is higher.
        """
        newval = s.get_val(self._fldname)
        if newval > self._val:
            self._val = newval

    def field_name(self):
        """
        Returns the field's name, prepended by "maxof".
        """
        return "maxof" + self._fldname

    def value(self):
        """
        Returns the current maximum.
        """
        return self._val


class CountFn(AggregationFn):
    """
    The count aggregation function.
    """
    def __init__(self, fldname: str):
        """
        Creates a count aggregation function for the specified field.
        :param fldname: the name of the aggregated field
        """
        self._fldname = fldname
        self._count = 0

    def process_first(self, s: Scan):
        """
        Starts a new count.
        Since SimpleDB does not support null values,
        every record will be counted,
        regardless of the field.
        The current count is thus set to 1.
        """
        self._count = 1

    def process_next(self, s: Scan):
        """
        Since SimpleDB does not support null values,
        this method always increments the count,
        regardless of the field.
        """
        self._count += 1

    def field_name(self):
        """
        Returns the field's name, prepended by "countof".
        """
        return "countof" + self._fldname

    def value(self):
        """
        Returns the current count.
        """
        return IntConstant(self._count)


class GroupValue:
    """
    An object that holds the values of the grouping fields
    for the current record of a scan.
    """
    def __init__(self, s: Scan, fields: list):
        """
        Creates a new group value, given the specified scan
        and list of fields.
        The values in the current record of each field are
        stored.
        :param s: a scan
        :param fields: the list of fields
        """
        self._vals = {}
        for fldname in fields:
            self._vals[fldname] = s.get_val(fldname)

    def get_val(self, fldname:str) -> Constant:
        """
        Returns the Constant value of the specified field in the group.
        :param fldname: the name of a field
        :return: the value of the field in the group
        """
        return self._vals[fldname]

    def __eq__(self, other):
        """
        Two GroupValue objects are equal if they have the same values
        for their grouping fields.
        """
        assert isinstance(other, GroupValue)
        for fldname in self._vals.keys():
            v1 = self._vals.get(fldname)
            v2 = other.get_val(fldname)
            if v1 != v2:
                return False
        return True

    def __hash__(self):
        """
        The hashcode of a GroupValue object is the sum of the
        hashcodes of its field values.
        """
        hashval = 0
        for c in self._vals.values():
            hashval += hash(c)
        return hashval


class GroupByScan(Scan):
    """
    The Scan class for the groupby operator.
    """
    def __init__(self, s: Scan, groupfields: list, aggfns: list):
        """
        Creates a groupby scan, given a grouped table scan.
        :param s: the grouped scan
        :param groupfields: the group fields
        :param aggfns: the aggregation functions
        """
        self._s = s
        self._groupfields = groupfields
        self._aggfns = aggfns
        self._groupval = None
        self._moregroups = None
        self.before_first()

    def before_first(self):
        """
        Positions the scan before the first group.
        Internally, the underlying scan is always
        positioned at the first record of a group, which
        means that this method moves to the
        first underlying record.
        """
        self._s.before_first()
        self._moregroups = self._s.next()

    def has_field(self, fldname):
        """
        Returns true if the specified field is either a
        grouping field or created by an aggregation function.
        """
        if fldname in self._groupfields:
            return True
        for fn in self._aggfns:
            if fn.field_name() == fldname:
                return True
        return False

    def get_val(self, fldname):
        """
        Gets the Constant value of the specified field.
        If the field is a group field, then its value can
        be obtained from the saved group value.
        Otherwise, the value is obtained from the
        appropriate aggregation function.
        """
        if fldname in self._groupfields:
            return self._groupval.get_val(fldname)
        for fn in self._aggfns:
            if fn.field_name() == fldname:
                return fn.value()
        raise RuntimeError("field " + fldname + " not found.")

    def get_string(self, fldname):
        """
        Gets the string value of the specified field.
        If the field is a group field, then its value can
        be obtained from the saved group value.
        Otherwise, the value is obtained from the
        appropriate aggregation function.
        """
        return self.get_val(fldname).as_python_val()

    def get_int(self, fldname):
        """
        Gets the integer value of the specified field.
        If the field is a group field, then its value can
        be obtained from the saved group value.
        Otherwise, the value is obtained from the
        appropriate aggregation function.
        """
        return self.get_val(fldname).as_python_val()

    def close(self):
        """
        Closes the scan by closing the underlying scan.
        """
        self._s.close()

    def next(self):
        """
        Moves to the next group.
        The key of the group is determined by the
        group values at the current record.
        The method repeatedly reads underlying records until
        it encounters a record having a different key.
        The aggregation functions are called for each record
        in the group.
        The values of the grouping fields for the group are saved.
        """
        if not self._moregroups:
            return False

        for fn in self._aggfns:
            fn.process_first(self._s)

        self._groupval = GroupValue(self._s, self._groupfields)

        self._moregroups = self._s.next()
        while self._moregroups:
            gv = GroupValue(self._s, self._groupfields)
            if not self._groupval == gv:
                break
            for fn in self._aggfns:
                fn.process_next(self._s)
            self._moregroups = self._s.next()

        return True


class GroupByPlan(Plan):
    """
    The Plan class for the groupby operator.
    """
    def __init__(self, p: Plan, groupfields: list, aggfns: list, tx: Transaction):
        """
        Creates a groupby plan for the underlying query.
        The grouping is determined by the specified
        collection of group fields,
        and the aggregation is computed by the
        specified collection of aggregation functions.
        :param p: a plan for the underlying query
        :param groupfields: the group fields
        :param aggfns: the aggregation functions
        :param tx: the calling transaction
        """
        grouplist = []
        grouplist.extend(groupfields)
        self._p = SortPlan(p, grouplist, tx)
        self._groupfields = groupfields
        self._aggfns = aggfns
        self._sch = Schema()
        for fldname in groupfields:
            self._sch.add(fldname, p.schema())
        for fn in aggfns:
            assert isinstance(fn, AggregationFn)
            self._sch.add_int_field(fn.field_name())

    def open(self):
        """
        This method opens a sort plan for the specified plan.
        The sort plan ensures that the underlying records
        will be appropriately grouped.
        """
        s = self._p.open()
        return GroupByScan(s, self._groupfields, self._aggfns)

    def blocks_accessed(self):
        """
        Returns the number of blocks required to
        compute the aggregation,
        which is one pass through the sorted table.
        It does not include the one-time cost
        of materializing and sorting the records.
        """
        return self._p.blocks_accessed()

    def records_output(self):
        """
        Returns the number of groups.  Assuming equal distribution,
        this is the product of the distinct values
        for each grouping field.
        """
        numgroups = 1
        for fldname in self._groupfields:
            numgroups *= self._p.distinct_values(fldname)
        return numgroups

    def distinct_values(self, fldname):
        """
        Returns the number of distinct values for the
        specified field.  If the field is a grouping field,
        then the number of distinct values is the same
        as in the underlying query.
        If the field is an aggregate field, then we
        assume that all values are distinct.
        """
        if self._p.schema().has_field(fldname):
            return self._p.distinct_values(fldname)
        else:
            return self.records_output()

    def schema(self):
        """
        Returns the schema of the output table.
        The schema consists of the group fields,
        plus one field for each aggregation function.
        """
        return self._sch