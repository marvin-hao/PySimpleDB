__author__ = 'Marvin'
from simpledb.query_prosessor.query import *
from simpledb.query_prosessor.parse import *
from simpledb.shared_service.server import SimpleDB
from simpledb.formatted_storage.index.index import Index
from simpledb.formatted_storage.metadata import IndexInfo


class QueryPlanner:
    """
    The interface implemented by planners for
    the SQL select statement.
    """
    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        """
        Creates a plan for the parsed query.
        :param data: the parsed representation of the query
        :param tx: the calling transaction
        :return: a plan for that query
        """
        raise NotImplementedError()


class UpdatePlanner:
    """
    The interface implemented by the planners
    for SQL insert, delete, and modify statements.
    """
    def execute_insert(self, data: InsertData, tx: Transaction) -> int:
        """
        Executes the specified insert statement, and
        returns the number of affected records.
        :param data: the parsed representation of the insert statement
        :param tx: the calling transaction
        :return: the number of affected records
        """
        raise NotImplementedError()

    def execute_delete(self, data: DeleteData, tx: Transaction) -> int:
        """
        Executes the specified delete statement, and
        returns the number of affected records.
        :param data: the parsed representation of the delete statement
        :param tx: the calling transaction
        :return: the number of affected records
        """
        raise NotImplementedError()

    def execute_modify(self, data: ModifyData, tx: Transaction) -> int:
        """
        Executes the specified modify statement, and
        returns the number of affected records.
        :param data: the parsed representation of the modify statement
        :param tx: the calling transaction
        :return: the number of affected records
        """
        raise NotImplementedError()

    def execute_create_table(self, data: CreateTableData, tx: Transaction) -> int:
        """
        Executes the specified create table statement, and
        returns the number of affected records.
        :param data: the parsed representation of the create table statement
        :param tx: the calling transaction
        :return: the number of affected records
        """
        raise NotImplementedError()

    def execute_create_view(self, data: CreateViewData, tx: Transaction) -> int:
        """
        Executes the specified create view statement, and
        returns the number of affected records.
        :param data: the parsed representation of the create view statement
        :param tx: the calling transaction
        :return: the number of affected records
        """
        raise NotImplementedError()

    def execute_create_index(self, data: CreateIndexData, tx: Transaction) -> int:
        """
        Executes the specified create index statement, and
        returns the number of affected records.
        :param data: the parsed representation of the create index statement
        :param tx: the calling transaction
        :return: the number of affected records
        """
        raise NotImplementedError()


class BasicQueryPlanner(QueryPlanner):
    """
    The simplest, most naive query planner possible.
    """
    def create_plan(self, data: QueryData, tx: Transaction):
        # Step 1: Create a plan for each mentioned table or view
        plans = []
        for tblname in data.tables():
            viewdef = SimpleDB.md_mgr().get_view_def(tblname, tx)
            if viewdef is not None:
                plans.append(SimpleDB.planner().create_query_plan(viewdef, tx))
            else:
                plans.append(TablePlan(tblname, tx))

        # Step 2: Create the product of all table plans
        p = plans[0]
        del(plans[0])
        for next_plan in plans:
            p = ProductPlan(p, next_plan)

        # Step 3: Add a selection plan for the predicate
        p = SelectPlan(p, data.pred())

        # Step 4: Project on the field names
        p = ProjectPlan(p, data.fields())
        return p


class BasicUpdatePlanner(UpdatePlanner):
    """
    The basic planner for SQL update statements.
    """
    def execute_delete(self, data: DeleteData, tx: Transaction):
        p = TablePlan(data.table_name(), tx)
        p = SelectPlan(p, data.pred())
        us = p.open()
        assert isinstance(us, UpdateScan)
        count = 0
        while us.next():
            us.delete()
            count += 1
        us.close()
        return count

    def execute_modify(self, data: ModifyData, tx: Transaction):
        p = TablePlan(data.table_name(), tx)
        p = SelectPlan(p, data.pred())
        us = p.open()
        assert isinstance(us, UpdateScan)
        count = 0
        while us.next():
            val = data.new_value().evaluate(us)
            us.set_val(data.target_field(), val)
            count += 1
        us.close()
        return count

    def execute_insert(self, data: InsertData, tx: Transaction):
        p = TablePlan(data.table_name(), tx)
        us = p.open()
        assert isinstance(us, UpdateScan)
        us.insert()
        for fldname, val in zip(data.fields(), data.vals()):
            us.set_val(fldname, val)
        us.close()
        return 1

    def execute_create_table(self, data: CreateTableData, tx: Transaction):
        SimpleDB.md_mgr().create_table(data.table_name(), data.new_schema(), tx)
        return 0

    def execute_create_index(self, data: CreateIndexData, tx: Transaction):
        SimpleDB.md_mgr().create_index(data.index_name(), data.table_name(), data.field_name(), tx)
        return 0

    def execute_create_view(self, data: CreateViewData, tx: Transaction):
        SimpleDB.md_mgr().create_view(data.view_name(), data.view_def(), tx)
        return 0


class Planner:
    """
    The object that executes SQL statements.
    """
    def __init__(self, qplanner: QueryPlanner, uplanner: UpdatePlanner):
        self._qplanner = qplanner
        self._uplanner = uplanner

    def create_query_plan(self, qry: str, tx: Transaction) -> Plan:
        """
        Creates a plan for an SQL select statement, using the supplied planner.
        :param qry: the SQL query string
        :param tx: the transaction
        :return: the scan corresponding to the query plan
        """
        parser = Parser(qry)
        data = parser.query()
        assert isinstance(data, QueryData)
        return self._qplanner.create_plan(data, tx)

    def execute_update(self, cmd: str, tx: Transaction) -> int:
        """
        Executes an SQL insert, delete, modify, or create statement.
        The method dispatches to the appropriate method of the
        supplied update planner, depending on what the parser returns.
        :param cmd: the SQL update string
        :param tx: the transaction
        :return: an integer denoting the number of affected records
        """
        parser = Parser(cmd)
        obj = parser.update_cmd()
        if isinstance(obj, InsertData):
            return self._uplanner.execute_insert(obj, tx)
        elif isinstance(obj, DeleteData):
            return self._uplanner.execute_delete(obj, tx)
        elif isinstance(obj, ModifyData):
            return self._uplanner.execute_modify(obj, tx)
        elif isinstance(obj, CreateTableData):
            return self._uplanner.execute_create_table(obj, tx)
        elif isinstance(obj, CreateIndexData):
            return self._uplanner.execute_create_index(obj, tx)
        elif isinstance(obj, CreateViewData):
            return self._uplanner.execute_create_view(obj, tx)
        else:
            return 0


class IndexUpdatePlanner(UpdatePlanner):
    """
    A modification of the basic update planner.
    It dispatches each update statement to the corresponding
    index planner.
    """
    def execute_create_index(self, data: CreateIndexData, tx: Transaction):
        SimpleDB.md_mgr().create_index(data.index_name(), data.table_name(), data.field_name(), tx)
        return 0

    def execute_create_view(self, data: CreateViewData, tx: Transaction):
        SimpleDB.md_mgr().create_view(data.view_name(), data.view_def(), tx)
        return 0

    def execute_create_table(self, data: CreateTableData, tx: Transaction):
        SimpleDB.md_mgr().create_table(data.table_name(), data.new_schema(), tx)

    def execute_modify(self, data: ModifyData, tx: Transaction):
        tblname = data.table_name()
        fldname = data.target_field()
        p = TablePlan(tblname, tx)
        p = SelectPlan(p, data.pred())

        ii = SimpleDB.md_mgr().get_index_info(tblname, tx)[fldname]
        assert isinstance(ii, IndexInfo)
        idx = None if ii is None else ii.open()
        assert isinstance(idx, Index)

        s = p.open()
        assert isinstance(s, UpdateScan)

        count = 0

        while s.next():
            # first, update the record
            newval = data.new_value().evaluate(s)
            oldval = s.get_val(fldname)
            s.set_val(data.target_field(), newval)

            # then update the appropriate index, if it exists
            if idx is not None:
                rid = s.get_rid()
                idx.delete(oldval, rid)
                idx.insert(newval, rid)
            count += 1

        if idx is not None:
            idx.close()
        s.close()
        return count

    def execute_delete(self, data: DeleteData, tx: Transaction):
        tblname = data.table_name()
        p = TablePlan(tblname, tx)
        p = SelectPlan(p, data.pred())
        indexes = SimpleDB.md_mgr().get_index_info(tblname, tx)
        assert isinstance(indexes, dict)
        s = p.open()
        assert isinstance(s, UpdateScan)

        count = 0
        while s.next():
            # first, delete the record's RID from every index
            rid = s.get_rid()
            for fldname in indexes.keys():
                val = s.get_val(fldname)
                idx = indexes[fldname].open()
                assert isinstance(idx, Index)
                idx.delete(val, rid)
                idx.close()

            # then delete the record
            s.delete()
            count += 1
        s.close()
        return count

    def execute_insert(self, data: InsertData, tx: Transaction):
        tblname = data.table_name()
        p = TablePlan(tblname, tx)

        # first, insert the record
        s = p.open()
        assert isinstance(s, UpdateScan)
        s.insert()
        rid = s.get_rid()

        # then modify each field, inserting an index record if appropriate
        indexes = SimpleDB.md_mgr().get_index_info(tblname, tx)
        assert isinstance(indexes, dict)
        val_iter = iter(data.vals())
        for fldname in data.fields():
            val = next(val_iter)
            print("Modify field " + fldname + " to val " + str(val))
            s.set_val(fldname, val)

            ii = indexes[fldname]
            if ii is not None:
                idx = ii.open()
                idx.insert(val, rid)
                idx.close()
        s.close()
        return 1