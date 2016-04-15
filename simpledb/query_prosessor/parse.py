__author__ = 'Marvin'
from tokenize import tokenize, NUMBER, STRING, OP, NAME
from io import BytesIO

from simpledb.formatted_storage.record import Schema
from simpledb.query_prosessor.query import Predicate, Expression, Constant, StringConstant, IntConstant, FieldNameExpression, \
    ConstantExpression, Term
from simpledb.formatted_storage.metadata import ViewMgr


class BadSyntaxException(RuntimeError):
    """
    A runtime exception indicating that the submitted query
    has incorrect syntax.
    """
    pass


class CreateTableData:
    """
    Data for the SQL create table statement.
    """

    def __init__(self, tblname: str, sch: Schema):
        """
        Saves the table name and schema.
        """
        self._tblname = tblname
        self._sch = sch

    def table_name(self) -> str:
        """
        Returns the name of the new table.
        :return: the name of the new table
        """
        return self._tblname

    def new_schema(self) -> Schema:
        """
        Returns the schema of the new table.
        :return: eturn the schema of the new table
        """
        return self._sch


class CreateIndexData:
    """
    The parser for the create index statement.
    """

    def __init__(self, idxname, tblname, fldname):
        """
        Saves the table and field names of the specified index.
        """
        self._idxname = idxname
        self._tblname = tblname
        self._fldname = fldname

    def index_name(self):
        return self._idxname

    def table_name(self):
        return self._tblname

    def field_name(self):
        return self._fldname


class QueryData:
    """
    Data for the SQL select statement.
    """

    def __init__(self, fields: list, tables: list, pred: Predicate):
        """
        Saves the field and table list and predicate.
        """
        self._fields = fields
        self._tables = tables
        self._pred = pred

    def fields(self):
        return self._fields

    def tables(self):
        return self._tables

    def pred(self):
        return self._pred

    def __str__(self):
        result = "select "
        result += ", ".join(self._fields)
        result += " from "
        result += ", ".join(self._tables)
        predstring = str(self._pred)
        if predstring != "":
            result += " where " + predstring
        return result


class CreateViewData:
    """
    Data for the SQL create view statement.
    """

    def __init__(self, viewname: str, qrydata: QueryData):
        """
        Saves the view name and its definition.
        """
        if len(str(qrydata)) > ViewMgr.MAX_VIEWDEF:
            raise Exception("View definition is too long.")
        else:
            self._viewname = viewname
            self._qrydata = qrydata

    def view_name(self):
        return self._viewname

    def view_def(self):
        return str(self._qrydata)


class InsertData:
    """
    Data for the SQL insert statement.
    """

    def __init__(self, tblname: str, flds: list, vals: list):
        """
        Saves the table name and the field and value lists.
        """
        self._tblname = tblname
        self._flds = flds
        self._vals = vals

    def table_name(self):
        return self._tblname

    def fields(self):
        return self._flds

    def vals(self):
        return self._vals


class ModifyData:
    """
    Data for the SQL update statement.
    """

    def __init__(self, tblname: str, fldname: str, newval: Expression, pred: Predicate):
        """
        Saves the table name, the modified field and its new value, and the predicate.
        """
        self._tblname = tblname
        self._fldname = fldname
        self._newval = newval
        self._pred = pred

    def table_name(self):
        return self._tblname

    def target_field(self):
        return self._fldname

    def new_value(self):
        return self._newval

    def pred(self):
        return self._pred


class DeleteData:
    """
    Data for the SQL delete statement.
    """

    def __init__(self, tblname: str, pred: Predicate):
        """
        Saves the table name and predicate.
        """
        self._tblname = tblname
        self._pred = pred

    def table_name(self):
        return self._tblname

    def pred(self):
        return self._pred


class Lexer:
    """
    The lexical analyzer.
    """

    def __init__(self, s: str):
        """
        Creates a new lexical analyzer for SQL statement s.
        :param s: the SQL statement
        """
        modified_s = s.replace(".", " ").lower()
        self.__init_keywords()
        self._tok_generator = tokenize(BytesIO(modified_s.encode('utf-8')).readline)
        next(self._tok_generator)  # skip header
        self._current = next(self._tok_generator)

    def match_delim(self, d):
        return self._current.type == OP and self._current.string == d

    def match_int_constant(self):
        return self._current.type == NUMBER

    def match_string_constant(self):
        return self._current.type == STRING

    def match_keyword(self, w):
        return self._current.type == NAME and self._current.string == w

    def match_id(self):
        return self._current.type == NAME and self._current.string not in self._keywords

    def eat_delim(self, d):
        """
        Throws an exception if the current token is not the
        specified delimiter.
        Otherwise, moves to the next token.
        """
        if not self.match_delim(d):
            raise BadSyntaxException()
        else:
            self.__next_token()

    def eat_int_constant(self):
        if not self.match_int_constant():
            raise BadSyntaxException()
        else:
            i = int(self._current.string)
            self.__next_token()
            return i

    def eat_string_constant(self):
        if not self.match_string_constant():
            raise BadSyntaxException()
        else:
            s = self._current.string.replace("'", "")
            self.__next_token()
            return s

    def eat_keyword(self, w):
        if not self.match_keyword(w):
            raise BadSyntaxException()
        else:
            self.__next_token()

    def eat_id(self):
        if not self.match_id():
            raise BadSyntaxException()
        else:
            s = self._current.string
            self.__next_token()
            return s

    def __init_keywords(self):
        self._keywords = ["select", "from", "where", "and",
                          "insert", "into", "values",
                          "delete", "update", "set",
                          "create", "table",
                          "int", "varchar",
                          "view", "as",
                          "index", "on"]

    def __next_token(self):
        try:
            self._current = next(self._tok_generator)
        except StopIteration:
            raise BadSyntaxException()


class Parser:
    """
    The SimpleDB parser.
    """

    def __init__(self, s: str):
        self._lex = Lexer(s)

    # Methods for parsing predicates, terms, expressions, constants, and fields
    def field(self) -> str:
        return self._lex.eat_id()

    def constant(self) -> Constant:
        if self._lex.match_string_constant():
            return StringConstant(self._lex.eat_string_constant())
        else:
            return IntConstant(self._lex.eat_int_constant())

    def expression(self) -> Expression:
        if self._lex.match_id():
            return FieldNameExpression(self.field())
        else:
            return ConstantExpression(self.constant())

    def term(self) -> Term:
        lhs = self.expression()
        self._lex.eat_delim("=")
        rhs = self.expression()
        return Term(lhs, rhs)

    def predicate(self):
        pred = Predicate(self.term())
        if self._lex.match_keyword("and"):
            self._lex.eat_keyword("and")

            # recursive conjunction

            pred.conjoin_with(self.predicate())
        return pred

    # Methods for parsing queries

    def __select_list(self) -> list:
        l = [self.field()]
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            l.extend(self.__select_list())
        return l

    def __table_list(self) -> list:
        l = [self._lex.eat_id()]
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            l.extend(self.__table_list())
        return l

    def query(self) -> QueryData:
        self._lex.eat_keyword("select")
        fields = self.__select_list()
        self._lex.eat_keyword("from")
        tables = self.__table_list()
        pred = Predicate()
        if self._lex.match_keyword("where"):
            self._lex.eat_keyword("where")
            pred = self.predicate()
        return QueryData(fields, tables, pred)

    # Method for parsing create table commands
    def __field_type(self, fldname: str) -> Schema:
        schema = Schema()
        if self._lex.match_keyword("int"):
            self._lex.eat_keyword("int")
            schema.add_int_field(fldname)
        else:
            self._lex.eat_keyword("varchar")
            self._lex.eat_delim("(")
            str_len = self._lex.eat_int_constant()
            self._lex.eat_delim(")")
            schema.add_string_field(fldname, str_len)
        return schema

    def __field_def(self):
        fldname = self.field()
        return self.__field_type(fldname)

    def __field_defs(self):
        schema = self.__field_def()
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            schema2 = self.__field_defs()
            schema.add_all(schema2)
        return schema

    def create_table(self) -> CreateTableData:
        self._lex.eat_keyword("table")
        tblname = self._lex.eat_id()
        self._lex.eat_delim("(")
        sch = self.__field_defs()
        self._lex.eat_delim(")")
        return CreateTableData(tblname, sch)

    # Method for parsing create view commands
    def create_view(self) -> CreateViewData:
        self._lex.eat_keyword("view")
        viewname = self._lex.eat_id()
        self._lex.eat_keyword("as")
        qd = self.query()
        return CreateViewData(viewname, qd)

    # Method for parsing create index commands
    def creat_index(self) -> CreateIndexData:
        self._lex.eat_keyword("index")
        idxname = self._lex.eat_id()
        self._lex.eat_keyword("on")
        tblname = self._lex.eat_id()
        self._lex.eat_delim("(")
        fldname = self.field()
        self._lex.eat_delim(")")
        return CreateIndexData(idxname, tblname, fldname)

    # Method for parsing modify commands
    def modify(self) -> ModifyData:
        self._lex.eat_keyword("update")
        tblname = self._lex.eat_id()
        self._lex.eat_keyword("set")
        fldname  = self._lex.eat_id()
        self._lex.eat_delim("=")
        newval = self.expression()
        pred = Predicate()
        if self._lex.match_keyword("where"):
            self._lex.eat_keyword("where")
            pred = self.predicate()
        return ModifyData(tblname, fldname, newval, pred)

    # Method for parsing delete commands
    def delete(self) -> DeleteData:
        self._lex.eat_keyword("delete")
        self._lex.eat_keyword("from")
        tblname = self._lex.eat_id()
        pred = Predicate()
        if self._lex.match_keyword("where"):
            self._lex.eat_keyword("where")
            pred = self.predicate()
        return DeleteData(tblname, pred)

    # Methods for parsing insert commands
    def __field_list(self):
        l = [self.field()]
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            l.extend(self.__field_list())
        return l

    def __const_list(self):
        l = [self.constant()]
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            l.extend(self.__const_list())
        return l

    def insert(self):
        self._lex.eat_keyword("insert")
        self._lex.eat_keyword("into")
        tblname = self._lex.eat_id()
        self._lex.eat_delim("(")
        flds = self.__field_list()
        self._lex.eat_delim(")")
        self._lex.eat_keyword("values")
        self._lex.eat_delim("(")
        vals = self.__const_list()
        self._lex.eat_delim(")")
        return InsertData(tblname, flds, vals)

    # Methods for parsing the various update commands
    def __create(self):
        self._lex.eat_keyword("create")
        if self._lex.match_keyword("table"):
            return self.create_table()
        elif self._lex.match_keyword("view"):
            return self.create_view()
        else:
            return self.creat_index()

    def update_cmd(self):
        if self._lex.match_keyword("insert"):
            return self.insert()
        elif self._lex.match_keyword("delete"):
            return self.delete()
        elif self._lex.match_keyword("update"):
            return self.modify()
        else:
            return self.__create()