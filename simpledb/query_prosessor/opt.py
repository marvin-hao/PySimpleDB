__author__ = 'Marvin'

from simpledb.query_prosessor.multibuffer import MultiBufferProductPlan
from simpledb.query_prosessor.planner import QueryPlanner, QueryData
from simpledb.query_prosessor.query import *


class TablePlanner:
    """
    This class contains methods for planning a single table.
    """
    def __init__(self, tblname: str, mypred: Predicate, tx: Transaction):
        """
        Creates a new table planner.
        The specified predicate applies to the entire query.
        The table planner is responsible for determining
        which portion of the predicate is useful to the table,
        and when indexes are useful.
        :param tblname: the name of the table
        :param mypred: the query predicate
        :param tx: the calling transaction
        """
        self._mypred = mypred
        self._tx = tx
        self._myplan = TablePlan(tblname, tx)
        self._myschema = self._myplan.schema()
        self._indexes = SimpleDB.md_mgr().get_index_info(tblname, tx)
        assert isinstance(self._indexes, dict)

    def make_product_plan(self, current: Plan) -> Plan:
        """
        Constructs a product plan of the specified plan and
        this table.
        :param current: the specified plan
        :return: a product plan of the specified plan and this table
        """
        p = self.__add_select_pred(self._myplan)
        return MultiBufferProductPlan(current, p, self._tx)

    def __add_join_pred(self, p: Plan, currsch: Schema) -> Plan:
        joinpred = self._mypred.join_pred(currsch, self._myschema)
        if joinpred is not None:
            return SelectPlan(p, joinpred)
        else:
            return p

    def __add_select_pred(self, p: Plan) -> Plan:
        selectpred = self._mypred.select_pred(self._myschema)
        if selectpred is not None:
            return SelectPlan(p, selectpred)
        else:
            return p

    def __make_product_join(self, current: Plan, currsch: Schema) -> Plan:
        p = self.make_product_plan(current)
        return self.__add_join_pred(p, currsch)

    def __make_index_join(self, current: Plan, currsch: Schema) -> Plan:
        for fldname in self._indexes.keys():
            outerfield = self._mypred.equates_with_field(fldname)
            if outerfield is not None and currsch.has_field(outerfield):
                ii = self._indexes[fldname]
                p = IndexJoinPlan(current, self._myplan, ii, outerfield, self._tx)
                p = self.__add_select_pred(p)
                return self.__add_join_pred(p, currsch)
        return None

    def __make_index_select(self):
        for fldname in self._indexes.keys():
            val = self._mypred.equates_with_constant(fldname)
            if val is not None:
                ii = self._indexes[fldname]
                return IndexSelectPlan(self._myplan, ii, val, self._tx)
        return None

    def make_join_plan(self, current: Plan):
        """
        Constructs a join plan of the specified plan
        and the table.  The plan will use an indexjoin, if possible.
        (Which means that if an indexselect is also possible,
        the indexjoin operator takes precedence.)
        The method returns null if no join is possible.
        :param current: the specified plan
        :return: a join plan of the plan and this table
        """
        currsch = current.schema()
        joinpred = self._mypred.join_pred(self._myschema, currsch)
        if joinpred is None:
            return None

        p = self.__make_index_join(current, currsch)
        if p is None:
            p = self.__make_product_join(current, currsch)
        return p

    def make_select_plan(self) -> Plan:
        """
        Constructs a select plan for the table.
        The plan will use an indexselect, if possible.
        :return: a select plan for the table.
        """
        p = self.__make_index_select()
        if p is None:
            p = self._myplan
            return self.__add_select_pred(p)


class HeuristicQueryPlanner(QueryPlanner):
    """
    A query planner that optimizes using a heuristic-based algorithm.
    """
    def __init__(self):
        self._tableplanners = []

    def __get_lowest_product_plan(self, current: Plan) -> Plan:
        besttp = None
        bestplan = None
        for tp in self._tableplanners:
            assert isinstance(tp, TablePlanner)
            plan = tp.make_product_plan(current)
            if bestplan is None or plan.records_output() < bestplan.records_output():
                besttp = tp
                bestplan = plan

        self._tableplanners.remove(besttp)
        return bestplan

    def __get_lowest_join_plan(self, current: Plan) -> Plan:
        besttp = None
        bestplan = None
        for tp in self._tableplanners:
            assert isinstance(tp, TablePlanner)
            plan = tp.make_join_plan(current)
            if plan is not None and (bestplan is None or plan.records_output() < bestplan.records_output()):
                besttp = tp
                bestplan = plan
        if bestplan is not None:
            self._tableplanners.remove(besttp)
        return bestplan

    def __get_lowest_select_plan(self):
        besttp = None
        bestplan = None
        for tp in self._tableplanners:
            assert isinstance(tp, TablePlanner)
            plan = tp.make_select_plan()
            if bestplan is None or plan.records_output() < bestplan.records_output():
                besttp = tp
                bestplan = plan
        self._tableplanners.remove(besttp)
        return bestplan

    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        """
        Creates an optimized left-deep query plan using the following
        heuristics.
        H1. Choose the smallest table (considering selection predicates)
        to be first in the join order.
        H2. Add the table to the join order which
        results in the smallest output.
        """

        # Step 1:  Create a TablePlanner object for each mentioned table
        for tblname in data.tables():
            tp = TablePlanner(tblname, data.pred(), tx)
            self._tableplanners.append(tp)

        # Step 2:  Choose the lowest-size plan to begin the join order
        currentplan = self.__get_lowest_select_plan()

        # Step 3:  Repeatedly add a plan to the join order
        while len(self._tableplanners) > 0:
            p = self.__get_lowest_join_plan(currentplan)
            if p is not None:
                currentplan = p
            else:
                currentplan = self.__get_lowest_product_plan(currentplan)

        # Step 4.  Project on the field names and return
        return ProjectPlan(currentplan, data.fields())