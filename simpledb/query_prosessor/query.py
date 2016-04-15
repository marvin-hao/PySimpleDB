__author__ = 'Marvin'

import sys

from simpledb.formatted_storage.record import Schema, TableInfo, RecordFile, RID
from simpledb.formatted_storage.tx import Transaction
from simpledb.shared_service.server import SimpleDB
from simpledb.formatted_storage.index.index import Index
from simpledb.formatted_storage.metadata import IndexInfo
from simpledb.shared_service.macro import *


class Constant:
    """
    The interface that denotes values stored in the database.
    """

    def as_python_val(self):
        """
        Returns the Python object corresponding to this constant.
        :return: the Python value of the constant
        """
        raise NotImplementedError()


class Scan:
    """
    The interface will be implemented by each query scan.
    There is a Scan class for each relational algebra operator.
    """

    def before_first(self):
        """
        Positions the scan before its first record.
        """
        raise NotImplementedError()

    def next(self):
        """
        Moves the scan to the next record.
        :return false if there is no next record
        """
        raise NotImplementedError()

    def close(self):
        """
        Closes the scan and its subscans, if any.
        """
        raise NotImplementedError()

    def get_val(self, fldname) -> Constant:
        """
        Returns the value of the specified field in the current record.
        The value is expressed as a Constant.
        :param fldname: the name of the field
        :return the value of that field, expressed as a Constant.
        """
        raise NotImplementedError()

    def get_int(self, fldname) -> int:
        """
        Returns the value of the specified integer field
        in the current record.
        :param fldname: the name of the field
        :return the field's integer value in the current record
        """
        raise NotImplementedError()

    def get_string(self, fldname) -> str:
        """
        Returns the value of the specified string field
        in the current record.
        :param fldname: the name of the field
        :return the field's string value in the current record
        """
        raise NotImplementedError()

    def has_field(self, fldname):
        """
        Returns true if the scan has the specified field.
        :param fldname: the name of the field
        :return true if the scan has that field
        """
        raise NotImplementedError()


class UpdateScan(Scan):
    """
    The interface implemented by all updateable scans.
    """

    def set_val(self, fldname, val):
        """
        Modifies the field value of the current record.
        :param fldname: the name of the field
        :param val: the new value, expressed as a Constant
        """
        raise NotImplementedError()

    def set_int(self, fldname, val):
        """
        Modifies the field value of the current record.
        :param fldname: the name of the field
        :param val: the new integer value
        """
        raise NotImplementedError()

    def set_string(self, fldname, val):
        """
        Modifies the field value of the current record.
        :param fldname: the name of the field
        :param val: the new string value
        """
        raise NotImplementedError()

    def insert(self):
        """
        Inserts the current record from the scan.
        """
        raise NotImplementedError()

    def delete(self):
        """
        Deletes the current record from the scan.
        """
        raise NotImplementedError()

    def get_rid(self) -> RID:
        """
        Returns the RID of the current record.
        :return the RID of the current record
        """
        raise NotImplementedError()

    def move_to_rid(self, rid):
        """
        Positions the scan so that the current record has
        the specified RID.
        :param rid: the RID of the desired record
        """
        raise NotImplementedError()


class Expression:
    """
    The interface corresponding to SQL expressions.
    """

    def is_constant(self):
        """
        Returns true if the expression is a constant.
        :return: true if the expression is a constant
        """
        raise NotImplementedError()

    def is_field_name(self):
        """
        Returns true if the expression is a field reference.
        :return: true if the expression denotes a field
        """
        raise NotImplementedError()

    def as_constant(self):
        """
        Returns the constant corresponding to a constant expression.
        Throws an exception if the expression does not denote a constant.
        :return: the expression as a constant
        """
        raise NotImplementedError()

    def as_field_name(self):
        """
        Returns the field name corresponding to a constant expression.
        Throws an exception if the expression does not denote a field.
        :return: the expression as a field name
        """
        raise NotImplementedError()

    def evaluate(self, s):
        """
        Evaluates the expression with respect to the
        current record of the specified scan.
        :param s: the scan
        :return the value of the expression, as a Constant
        """
        raise NotImplementedError()

    def applies_to(self, sch):
        """
        Determines if all of the fields mentioned in this expression
        are contained in the specified schema.
        :param sch: the schema
        :return true if all fields in the expression are in the schema
        """
        raise NotImplementedError()


class Plan:
    """
    The interface implemented by each query plan.
    There is a Plan class for each relational algebra operator.
    """

    def open(self) -> Scan:
        """
        Opens a scan corresponding to this plan.
        The scan will be positioned before its first record.
        :return a scan
        """
        raise NotImplementedError()

    def blocks_accessed(self) -> int:
        """
        Returns an estimate of the number of block accesses
        that will occur when the scan is read to completion.
        :return the estimated number of block accesses
        """
        raise NotImplementedError()

    def records_output(self) -> int:
        """
        Returns an estimate of the number of records
        in the query's output table.
        :return the estimated number of output records
        """
        raise NotImplementedError()

    def distinct_values(self, fldname) -> int:
        """
        Returns an estimate of the number of distinct values
        for the specified field in the query's output table.
        :param fldname the name of a field
        :return the estimated number of distinct field values in the output
        """
        raise NotImplementedError()

    def schema(self) -> Schema:
        """
        Returns the schema of the query.
        :return the query's schema
        """
        raise NotImplementedError()


class IntConstant(Constant):
    """
    The class that wraps Python ints as database constants.
    """

    def __init__(self, n):
        """
        Create a constant by wrapping the specified int.
        :param n: the int value
        """
        self._val = n

    def as_python_val(self):
        """
        Unwraps the Integer and returns it.
        """
        return self._val

    def __eq__(self, other):
        return isinstance(other, IntConstant) and self._val == other._val

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ge__(self, other):
        if isinstance(other, IntConstant):
            return self._val >= other._val
        else:
            raise TypeError()

    def __gt__(self, other):
        if isinstance(other, IntConstant):
            return self._val > other._val
        else:
            raise TypeError()

    def __le__(self, other):
        if isinstance(other, IntConstant):
            return self._val <= other._val
        else:
            raise TypeError()

    def __lt__(self, other):
        if isinstance(other, IntConstant):
            return self._val < other._val
        else:
            raise TypeError()

    def __hash__(self):
        return hash(self._val)

    def __str__(self):
        return str(self._val)


class StringConstant(Constant):
    """
    The class that wraps Python strings as database constants.
    """

    def __init__(self, s):
        """
        Create a constant by wrapping the specified string.
        :param s: the string value
        """
        self._val = s

    def as_python_val(self):
        """
        Unwraps the string and returns it.
        """
        return self._val

    def __eq__(self, other):
        if isinstance(other, StringConstant):
            return self._val == other._val

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ge__(self, other):
        if isinstance(other, StringConstant):
            return self._val >= other._val
        else:
            raise TypeError()

    def __gt__(self, other):
        if isinstance(other, StringConstant):
            return self._val > other._val
        else:
            raise TypeError()

    def __le__(self, other):
        if isinstance(other, StringConstant):
            return self._val <= other._val
        else:
            raise TypeError()

    def __lt__(self, other):
        if isinstance(other, StringConstant):
            return self._val < other._val
        else:
            raise TypeError()

    def __hash__(self):
        return hash(self._val)

    def __str__(self):
        return self._val


class ClassCastException(Exception):
    pass


class FieldNameExpression(Expression):
    """
    An expression consisting entirely of a single field.
    """

    def __init__(self, fldname):
        """
        Creates a new expression by wrapping a field.
        :param fldname: the name of the wrapped field
        """
        self._fldname = fldname

    def is_constant(self):
        """
        Returns false.
        """
        return False

    def is_field_name(self):
        """
        Returns true.
        """
        return True

    def as_constant(self):
        """
        This method should never be called.
        Throws a ClassCastException.
        Here we use Type
        """
        raise ClassCastException()

    def as_field_name(self):
        """
        Unwraps the field name and returns it.
        """
        return self._fldname

    def evaluate(self, s):
        """
        Evaluates the field by getting its value in the scan.
        """
        assert isinstance(s, Scan)
        return s.get_val(self._fldname)

    def applies_to(self, sch):
        """
        Returns true if the field is in the specified schema.
        """
        assert isinstance(sch, Schema)
        return sch.has_field(self._fldname)

    def __str__(self):
        return self._fldname


class ConstantExpression(Expression):
    """
    An expression consisting entirely of a single constant.
    """

    def __init__(self, c: Constant):
        """
        reates a new expression by wrapping a constant.
        :param c: the constant
        """
        self._val = c

    def is_constant(self):
        """
        Returns true.
        """
        return True

    def is_field_name(self):
        """
        Returns false.
        """
        return False

    def as_constant(self):
        """
        Unwraps the constant and returns it.
        """
        return self._val

    def as_field_name(self):
        """
        This method should never be called.
        Throws a ClassCastException.
        """
        raise ClassCastException()

    def evaluate(self, s):
        """
        Returns the constant, regardless of the scan.
        """
        assert isinstance(s, Scan)
        return self._val

    def applies_to(self, sch):
        """
        Returns true, because a constant applies to any schema.
        """
        assert isinstance(sch, Schema)
        return True

    def __str__(self):
        return str(self._val)


class Term:
    """
    A term is a comparison between two expressions.
    """

    def __init__(self, lhs, rhs):
        """
        Creates a new term that compares two expressions
        for equality.
        :param lhs: the LHS expression
        :param rhs: the RHS expression
        """
        assert isinstance(lhs, Expression)
        assert isinstance(rhs, Expression)
        self._lhs = lhs
        self._rhs = rhs

    def reduction_factor(self, p):
        """
        Calculates the extent to which selecting on the term reduces
        the number of records output by a query.
        For example if the reduction factor is 2, then the
        term cuts the size of the output in half.
        :param p: the query's plan
        :return: the integer reduction factor.
        """
        assert isinstance(p, Plan)

        if self._lhs.is_field_name() and self._rhs.is_field_name():
            lhs_name = self._lhs.as_field_name()
            rhs_name = self._rhs.as_field_name()
            return max(p.distinct_values(lhs_name), p.distinct_values(rhs_name))
        elif self._lhs.is_field_name():
            lhs_name = self._lhs.as_field_name()
            return p.distinct_values(lhs_name)
        elif self._rhs.is_field_name():
            rhs_name = self._rhs.as_field_name()
            return p.distinct_values(rhs_name)
        elif self._lhs.as_constant() == self._rhs.as_constant():
            return 1
        else:
            return sys.maxsize

    def equates_with_constant(self, fldname):
        """
        Determines if this term is of the form "F=c"
        where F is the specified field and c is some constant.
        If so, the method returns that constant.
        If not, the method returns None.
        :param fldname: the name of the field
        :return: either the constant or None
        """
        if self._lhs.is_field_name() and self._rhs.is_constant() and self._lhs.as_field_name() == fldname:
            return self._rhs.as_constant()
        elif self._rhs.is_field_name() and self._lhs.is_constant() and self._rhs.as_field_name() == fldname:
            return self._lhs.as_constant()
        else:
            return None

    def equates_with_field(self, fldname):
        """
        Determines if this term is of the form "F1=F2"
        where F1 is the specified field and F2 is another field.
        If so, the method returns the name of that field.
        If not, the method returns None.
        :param fldname: the name of the field
        :return: either the name of the other field, or None
        """
        if self._lhs.is_field_name() and self._rhs.is_field_name() and self._lhs.as_field_name() == fldname:
            return self._rhs.as_field_name()
        elif self._rhs.is_field_name() and self._lhs.is_field_name() and self._rhs.as_field_name() == fldname:
            return self._lhs.as_field_name()
        else:
            return None

    def applies_to(self, sch):
        """
        Returns true if both of the term's expressions
        apply to the specified schema.
        :param sch: the schema
        :return: true if both expressions apply to the schema
        """
        assert isinstance(sch, Schema)
        return self._lhs.applies_to(sch) and self._rhs.applies_to(sch)

    def is_satisfied(self, s):
        """
        Returns true if both of the term's expressions
        evaluate to the same constant,
        with respect to the specified scan.
        :param s: the scan
        :return: true if both expressions have the same value in the scan
        """
        assert isinstance(s, Scan)
        lhs_val = self._lhs.evaluate(s)
        rhs_val = self._rhs.evaluate(s)
        return rhs_val == lhs_val

    def __str__(self):
        return str(self._lhs) + "=" + str(self._rhs)


class Predicate:
    """
    A predicate is a Boolean combination of terms.
    """

    def __init__(self, t=None):
        """
        Creates an empty predicate, corresponding to "true".
        Or creates a predicate containing a single term.
        :param t: the term
        """
        if t is None:
            self._terms = []
        else:
            assert isinstance(t, Term)
            self._terms = [t]

    def conjoin_with(self, pred):
        """
        Modifies the predicate to be the conjunction of
        itself and the specified predicate.
        :param pred: the other predicate
        """
        assert isinstance(pred, Predicate)
        self._terms.extend(pred._terms)

    def is_satisfied(self, s):
        """
        Returns true if the predicate evaluates to true
        with respect to the specified scan.
        :param s: the scan
        :return: true if the predicate is true in the scan
        """
        assert isinstance(s, Scan)
        for t in self._terms:
            if not t.is_satisfied(s):
                return False
        return True

    def reduction_factor(self, p):
        """
        Calculates the extent to which selecting on the predicate
        reduces the number of records output by a query.
        For example if the reduction factor is 2, then the
        predicate cuts the size of the output in half.
        :param p: the query's plan
        :return: the integer reduction factor.
        """
        factor = 1
        for t in self._terms:
            factor *= t.reduction_factor(p)
        return factor

    def select_pred(self, sch):
        """
        Returns the subpredicate that applies to the specified schema.
        :param sch: the schema
        :return: the subpredicate applying to the schema
        """
        assert isinstance(sch, Schema)
        result = Predicate()
        for t in self._terms:
            if t.applies_to(sch):
                result._terms.append(t)
        if len(result._terms) == 0:
            return None
        else:
            return result

    def join_pred(self, sch1, sch2):
        """
        Returns the subpredicate consisting of terms that apply
        to the union of the two specified schemas,
         but not to either schema separately.
        :param sch1: the first schema
        :param sch2: the second schema
        :return: the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
        """
        assert isinstance(sch1, Schema)
        assert isinstance(sch2, Schema)
        result = Predicate()
        newsch = Schema()
        newsch.add_all(sch1)
        newsch.add_all(sch2)
        for t in self._terms:
            if not t.applies_to(sch1) and not t.applies_to(sch2) and t.applies_to(newsch):
                result._terms.append(t)
        if len(result._terms) == 0:
            return None
        else:
            return result

    def equates_with_constant(self, fldname):
        """
        Determines if there is a term of the form "F=c"
        where F is the specified field and c is some constant.
        If so, the method returns that constant.
        If not, the method returns None.
        :param fldname: the name of the field
        :return: either the constant or None
        """
        for t in self._terms:
            c = t.equates_with_constant(fldname)
            if not c is None:
                return c
        return None

    def equates_with_field(self, fldname):
        """
        Determines if there is a term of the form "F1=F2"
        where F1 is the specified field and F2 is another field.
        If so, the method returns the name of that field.
        If not, the method returns None.
        :param fldname: the name of the field
        :return: the name of the other field, or null
        """
        for t in self._terms:
            s = t.equates_with_field(fldname)
            if not s is None:
                return s
        return None

    def __str__(self):
        if len(self._terms) == 0:
            return ""
        else:
            " and ".join([str(i) for i in self._terms])


class TableScan(UpdateScan):
    """
    The Scan class corresponding to a table.
    A table scan is just a wrapper for a RecordFile object;
    most methods just delegate to the corresponding
    RecordFile methods.
    """

    def __init__(self, ti, tx):
        """
        Creates a new table scan,
        and opens its corresponding record file.
        :param ti: the table's metadata
        :param tx: the calling transaction
        """
        assert isinstance(ti, TableInfo)
        assert isinstance(tx, Transaction)
        self._rf = RecordFile(ti, tx)
        self._sch = ti.schema()

    # Scan methods

    def before_first(self):
        self._rf.before_first()

    def next(self):
        return self._rf.next()

    def close(self):
        self._rf.close()

    def get_val(self, fldname):
        """
        Returns the value of the specified field, as a Constant.
        The schema is examined to determine the field's type.
        If INTEGER, then the record file's getInt method is called;
        otherwise, the getString method is called.
        """
        if self._sch.type(fldname) == INTEGER:
            return IntConstant(self._rf.get_int(fldname))
        else:
            StringConstant(self._rf.get_string(fldname))

    def get_int(self, fldname):
        return self._rf.get_int(fldname)

    def get_string(self, fldname):
        return self._rf.get_string(fldname)

    def has_field(self, fldname):
        return self._sch.has_field(fldname)

    # UpdateScan methods

    def set_val(self, fldname, val):
        """
        Sets the value of the specified field, as a Constant.
        The schema is examined to determine the field's type.
        If INTEGER, then the record file's setInt method is called;
        otherwise, the setString method is called.
        """
        assert isinstance(val, Constant)
        if self._sch.type(fldname) == INTEGER:
            self._rf.set_int(fldname, val.as_python_val())
        else:
            self._rf.set_string(fldname, val.as_python_val())

    def set_int(self, fldname, val):
        assert isinstance(val, int)
        self._rf.set_int(fldname, val)

    def set_string(self, fldname, val):
        assert isinstance(val, str)
        self._rf.set_string(fldname, val)

    def delete(self):
        self._rf.delete()

    def insert(self):
        self._rf.insert()

    def get_rid(self):
        return self._rf.current_rid()

    def move_to_rid(self, rid):
        assert isinstance(rid, RID)
        self._rf.move_to_rid(rid)


class SelectScan(UpdateScan):
    """
    The scan class corresponding to the select relational algebra operator.
    All methods except next delegate their work to the underlying scan.
    """

    def __init__(self, s, pred):
        """
        Creates a select scan having the specified underlying scan and predicate.
        :param s: the scan of the underlying query
        :param pred: the selection predicate
        """
        assert isinstance(s, Scan)
        assert isinstance(pred, Predicate)
        self._s = s
        self._pred = pred

    # Scan methods

    def before_first(self):
        self._s.before_first()

    def next(self):
        """
        Move to the next record satisfying the predicate.
        The method repeatedly calls next on the underlying scan
        until a suitable record is found, or the underlying scan
        contains no more records.
        """
        while self._s.next():
            if self._pred.is_satisfied(self._s):
                return True
        return False

    def close(self):
        self._s.close()

    def get_val(self, fldname):
        return self._s.get_val(fldname)

    def get_int(self, fldname):
        return self._s.get_int(fldname)

    def get_string(self, fldname):
        return self._s.get_string(fldname)

    def has_field(self, fldname):
        return self._s.has_field(fldname)

    # UpdateScan methods
    def set_val(self, fldname, val):
        assert isinstance(val, Constant)
        if isinstance(self._s, UpdateScan):
            self._s.set_val(fldname, val)

    def set_int(self, fldname, val):
        assert isinstance(val, int)
        if isinstance(self._s, UpdateScan):
            self._s.set_int(fldname, val)

    def set_string(self, fldname, val):
        assert isinstance(val, str)
        if isinstance(self._s, UpdateScan):
            self._s.set_string(fldname, val)

    def delete(self):
        if isinstance(self._s, UpdateScan):
            self._s.delete()

    def insert(self):
        if isinstance(self._s, UpdateScan):
            self._s.insert()

    def get_rid(self):
        if isinstance(self._s, UpdateScan):
            self._s.get_rid()

    def move_to_rid(self, rid):
        assert isinstance(rid, RID)
        if isinstance(self._s, UpdateScan):
            self._s.move_to_rid(rid)


class ProductScan(Scan):
    """
    The scan class corresponding to the product relational algebra operator.
    """

    def __init__(self, s1, s2):
        """
        Creates a product scan having the two underlying scans.
        :param s1: the LHS scan
        :param s2: the RHS scan
        """
        assert isinstance(s1, Scan)
        assert isinstance(s2, Scan)
        self._s1 = s1
        self._s2 = s2
        s1.next()

    def before_first(self):
        """
        Positions the scan before its first record.
        In other words, the LHS scan is positioned at
        its first record, and the RHS scan
        is positioned before its first record.
        """
        self._s1.before_first()
        self._s1.next()
        self._s2.before_first()

    def next(self):
        """
        Moves the scan to the next record.
        The method moves to the next RHS record, if possible.
        Otherwise, it moves to the next LHS record and the
        first RHS record.
        If there are no more LHS records, the method returns false.
        """
        if self._s2.next():
            return True
        else:
            self._s2.before_first()
            return self._s2.next() and self._s1.next()

    def close(self):
        """
        Closes both underlying scans.
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
            return self._s2.get_val(fldname)  # This logic must be based on using has_field method first

    def get_int(self, fldname):
        """
        Returns the integer value of the specified field.
        The value is obtained from whichever scan
        contains the field.
        """
        if self._s1.has_field(fldname):
            return self._s1.get_int(fldname)
        else:
            return self._s2.get_int(fldname)  # This logic must be based on using has_field method first

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


class ProjectScan(Scan):
    """
    The scan class corresponding to the project relational algebra operator.
    All methods except hasField delegate their work to the underlying scan.
    """

    def __init__(self, s, fieldlist):
        """
        Creates a project scan having the specified
        underlying scan and field list.
        :param s: the underlying scan
        :param fieldlist: the list of field names
        """
        assert isinstance(s, Scan)
        self._s = s
        self._fieldlist = list(fieldlist)
        assert isinstance(self._fieldlist, list)

    def before_first(self):
        self._s.before_first()

    def next(self):
        return self._s.next()

    def close(self):
        self._s.close()

    def has_field(self, fldname):
        """
        Returns true if the specified field is in the projection list.
        """
        return fldname in self._fieldlist

    def get_val(self, fldname):
        if self.has_field(fldname):
            return self._s.get_string(fldname)
        else:
            raise RuntimeError("field " + fldname + "not found")

    def get_int(self, fldname):
        if self.has_field(fldname):
            return self._s.get_int(fldname)
        else:
            raise RuntimeError("field " + fldname + "not found")

    def get_string(self, fldname):
        if self.has_field(fldname):
            return self._s.get_string(fldname)
        else:
            return RuntimeError("field " + fldname + "not found")


class TablePlan(Plan):
    """
    The Plan class corresponding to a table.
    """

    def __init__(self, tblname, tx):
        """
        Creates a leaf node in the query tree corresponding to the specified table.
        :param tblname: the name of the table
        :param tx: the calling transaction
        """
        assert isinstance(tx, Transaction)
        self._tx = tx
        self._ti = SimpleDB.md_mgr().get_table_info(tblname, tx)
        self._si = SimpleDB.md_mgr().get_stat_info(tblname, self._ti, tx)

    def open(self):
        """
        Creates a table scan for this query.
        """
        return TableScan(self._ti, self._tx)

    def blocks_accessed(self):
        """
         Estimates the number of block accesses for the table,
        which is obtainable from the statistics manager.
        """
        return self._si.blocks_accessed()

    def records_output(self):
        """
         Estimates the number of records in the table,
        which is obtainable from the statistics manager.
        """
        return self._si.records_output()

    def distinct_values(self, fldname):
        """
        Estimates the number of distinct field values in the table,
        which is obtainable from the statistics manager.
        """
        return self._si.distinct_values(fldname)

    def schema(self):
        """
        Determines the schema of the table,
        which is obtainable from the catalog manager.
        """
        return self._ti.schema()


class ProjectPlan(Plan):
    """
    The Plan class corresponding to the project
    relational algebra operator.
    """

    def __init__(self, p: Plan, fieldlist: list):
        self._schema = Schema()
        self._p = p
        for fldname in fieldlist:
            self._schema.add(fldname, p.schema())

    def open(self):
        """
        Creates a project scan for this query.
        """
        s = self._p.open()
        return ProjectScan(s, self._schema.fields())

    def blocks_accessed(self):
        """
        Estimates the number of block accesses in the projection,
        which is the same as in the underlying query.
        """
        return self._p.blocks_accessed()

    def records_output(self):
        """
        Estimates the number of output records in the projection,
        which is the same as in the underlying query.
        """
        return self._p.records_output()

    def distinct_values(self, fldname):
        """
        Estimates the number of distinct field values
        in the projection,
        which is the same as in the underlying query.
        """
        return self._p.distinct_values(fldname)

    def schema(self):
        """
        Returns the schema of the projection,
        which is taken from the field list.
        """
        return self._schema


class ProductPlan(Plan):
    """
    The Plan class corresponding to the <i>product</i>
    relational algebra operator.
    """

    def __init__(self, p1: Plan, p2: Plan):
        """
        Creates a new product node in the query tree,
        having the two specified subqueries.
        :param p1: the left-hand subquery
        :param p2: the right-hand subquery
        """
        self._p1 = p1
        self._p2 = p2
        self._schema = Schema()
        self._schema.add_all(p1.schema())
        self._schema.add_all(p2.schema())

    def open(self):
        """
        Creates a product scan for this query.
        """
        s1 = self._p1.open()
        s2 = self._p2.open()
        return ProductScan(s1, s2)

    def blocks_accessed(self):
        """
        Estimates the number of block accesses in the product.
        The formula is:
        B(product(p1,p2)) = B(p1) + R(p1)*B(p2)
        """
        return self._p1.blocks_accessed() + self._p1.records_output() * self._p2.blocks_accessed()

    def records_output(self):
        """
        Estimates the number of output records in the product.
        The formula is:
        R(product(p1,p2)) = R(p1)*R(p2)
        """
        return self._p1.records_output() * self._p2.records_output()

    def distinct_values(self, fldname):
        """
        Estimates the distinct number of field values in the product.
        Since the product does not increase or decrease field values,
        the estimate is the same as in the appropriate underlying query.
        """
        if self._p1.schema().has_field(fldname):
            return self._p1.distinct_values(fldname)
        else:
            return self._p2.distinct_values(fldname)

    def schema(self):
        """
        Returns the schema of the product,
        which is the union of the schemas of the underlying queries.
        """
        return self._schema


class SelectPlan(Plan):
    """
    The Plan class corresponding to the <i>select</i>
    relational algebra operator.
    """

    def __init__(self, p: Plan, pred: Predicate):
        """
        Creates a new select node in the query tree,
        having the specified subquery and predicate.
        :param p: the subquery
        :param pred: the predicate
        """
        self._p = p
        self._pred = pred

    def open(self):
        """
        Creates a select scan for this query.
        """
        s = self._p.open()
        return SelectScan(s, self._pred)

    def blocks_accessed(self):
        """
        Estimates the number of block accesses in the selection,
        which is the same as in the underlying query.
        """
        return self._p.blocks_accessed()

    def records_output(self):
        """
        Estimates the number of output records in the selection,
        which is determined by the
        reduction factor of the predicate.
        """
        return self._p.records_output() // self._pred.reduction_factor(self._p)

    def distinct_values(self, fldname):
        """
        Estimates the number of distinct field values
        in the projection.
        If the predicate contains a term equating the specified
        field to a constant, then this value will be 1.
        Otherwise, it will be the number of the distinct values
        in the underlying query
        (but not more than the size of the output table).
        """
        if self._pred.equates_with_constant(fldname) is not None:
            return 1
        else:
            fldname2 = self._pred.equates_with_field(fldname)
            if fldname2 is not None:
                return min(self._p.distinct_values(fldname2), self._p.distinct_values(fldname))
            else:
                return min(self._p.distinct_values(fldname), self.records_output())

    def schema(self):
        """
        Returns the schema of the selection,
        which is the same as in the underlying query.
        """
        return self._p.schema()


class IndexJoinScan(Scan):
    """
    The scan class corresponding to the indexjoin relational
    algebra operator.
    The code is very similar to that of ProductScan,
    which makes sense because an index join is essentially
    the product of each LHS record with the matching RHS index records.
    """

    def __init__(self, s: Scan, idx: Index, joinfield: str, ts: TableScan):
        """
        Creates an index join scan for the specified LHS scan and
        RHS index.
        :param s: the LHS scan
        :param idx: the RHS index
        :param joinfield: the LHS field used for joining
        """
        self._s = s
        self._idx = idx
        self._joinfield = joinfield
        self._ts = ts
        self.before_first()

    def __reset_index(self):
        searchkey = self._s.get_val(self._joinfield)
        self._idx.before_first(searchkey)

    def before_first(self):
        """
        Positions the scan before the first record.
        That is, the LHS scan will be positioned at its
        first record, and the index will be positioned
        before the first record for the join value.
        """
        self._s.before_first()
        self._s.next()
        self.__reset_index()

    def has_field(self, fldname):
        return self._ts.has_field(fldname) or self._s.has_field(fldname)

    def get_string(self, fldname):
        if self._ts.has_field(fldname):
            return self._ts.get_string(fldname)
        else:
            return self._s.get_string(fldname)

    def get_int(self, fldname):
        if self._ts.has_field(fldname):
            return self._ts.get_int(fldname)
        else:
            return self._s.get_int(fldname)

    def get_val(self, fldname):
        if self._ts.has_field(fldname):
            return self._ts.get_val(fldname)
        else:
            return self._s.get_val(fldname)

    def close(self):
        self._s.close()
        self._idx.close()
        self._ts.close()

    def next(self):
        """
        Moves the scan to the next record.
        The method moves to the next index record, if possible.
        Otherwise, it moves to the next LHS record and the
        first index record.
        If there are no more LHS records, the method returns false.
        """
        while True:
            if self._idx.next():
                self._ts.move_to_rid(self._idx.get_data_rid())
                return True

            if not self._s.next():
                return False
            self.__reset_index()


class IndexJoinPlan(Plan):
    """
    The Plan class corresponding to the <i>indexjoin</i>
    relational algebra operator.
    """

    def __init__(self, p1: Plan, p2: Plan, ii: IndexInfo, joinfield: str, tx: Transaction):
        """
        Implements the join operator,
        using the specified LHS and RHS plans.
        :param p1: the left-hand plan
        :param p2: the right-hand plan
        :param ii: information about the right-hand index
        :param joinfield: the left-hand field used for joining
        :param tx: the calling transaction
        """
        self._sch = Schema()
        self._p1 = p1
        self._p2 = p2
        self._ii = ii
        self._joinfield = joinfield
        self._sch.add_all(p1.schema())
        self._sch.add_all(p2.schema())

    def open(self):
        """
        Opens an indexjoin scan for this query
        """
        s = self._p1.open()
        # throws an exception if p2 is not a tableplan
        assert isinstance(self._p2, TablePlan)
        ts = self._p2.open()
        idx = self._ii.open()
        assert isinstance(idx, Index)
        return IndexJoinScan(s, idx, self._joinfield, ts)

    def blocks_accessed(self):
        """
        Estimates the number of block accesses to compute the join.
        The formula is:
        B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
              + R(indexjoin(p1,p2,idx)
        """
        return self._p1.blocks_accessed() + self._p1.records_output() * self._ii.blocks_accessed() + \
               self.records_output()

    def records_output(self):
        """
        Estimates the number of output records in the join.
        The formula is:
        R(indexjoin(p1,p2,idx)) = R(p1)*R(idx)
        """
        return self._p1.records_output() * self._ii.records_output()

    def distinct_values(self, fldname):
        """
        Estimates the number of distinct values for the
        specified field.
        """
        if self._p1.schema().has_field(fldname):
            return self._p1.distinct_values(fldname)
        else:
            return self._p2.distinct_values(fldname)

    def schema(self):
        """
        Returns the schema of the index join.
        """
        return self._sch


class IndexSelectScan(Scan):
    """
    The scan class corresponding to the select relational
    algebra operator.
    """

    def __init__(self, idx: Index, val: Constant, ts: TableScan):
        """
        Creates an index select scan for the specified
        index and selection constant.
        :param idx: the index
        :param val: the selection constant
        """
        self._idx = idx
        self._val = val
        self._ts = ts
        self.before_first()

    def before_first(self):
        """
        Positions the scan before the first record,
        which in this case means positioning the index
        before the first instance of the selection constant.
        """
        self._idx.before_first(self._val)

    def has_field(self, fldname):
        """
        Returns whether the data record has the specified field.
        """
        return self._ts.has_field(fldname)

    def get_string(self, fldname):
        """
        Returns the value of the field of the current data record.
        """
        return self._ts.get_int(fldname)

    def get_int(self, fldname):
        return self._ts.get_int(fldname)

    def get_val(self, fldname):
        return self._ts.get_val(fldname)

    def close(self):
        self._idx.close()
        self._ts.close()

    def next(self):
        """
        Moves to the next record, which in this case means
        moving the index to the next record satisfying the
        selection constant, and returning false if there are
        no more such index records.
        If there is a next record, the method moves the
        tablescan to the corresponding data record.
        """
        ok = self._idx.next()
        if ok:
            rid = self._idx.get_data_rid()
            self._ts.move_to_rid(rid)
        return ok


class IndexSelectPlan(Plan):
    """
    The Plan class corresponding to the indexselect
    relational algebra operator.
    """

    def __init__(self, p: Plan, ii: IndexInfo, val: Constant, tx: Transaction):
        self._p = p
        self._ii = ii
        self._val = val

    def open(self):
        # throws an exception if p is not a tableplan.
        assert isinstance(self._p, TablePlan)
        ts = self._p.open()
        idx = self._ii.open()
        assert isinstance(idx, Index)
        return IndexSelectScan(idx, self._val, ts)

    def schema(self):
        return self._p.schema()

    def distinct_values(self, fldname):
        """
        Returns the distinct values as defined by the index.
        """
        return self._ii.distinct_values(fldname)

    def records_output(self):
        """
        Estimates the number of output records in the index selection,
        which is the same as the number of search key values
        for the index.
        """
        return self._ii.records_output()

    def blocks_accessed(self):
        """
        Estimates the number of block accesses to compute the
        index selection, which is the same as the
        index traversal cost plus the number of matching data records.
        """
        return self._ii.blocks_accessed() + self.records_output()