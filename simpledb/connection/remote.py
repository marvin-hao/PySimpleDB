__author__ = 'Marvin'
import Pyro4.core

from simpledb.formatted_storage.record import Schema
from simpledb.formatted_storage.tx import Transaction
from simpledb.query_prosessor.query import Plan
from simpledb.shared_service.server import SimpleDB
from simpledb.shared_service.macro import *


class RemoteMetaData:
    def get_column_count(self):
        raise NotImplementedError()

    def get_column_name(self, column: int) -> str:
        raise NotImplementedError()

    def get_column_type(self, column: int) -> int:
        raise NotImplementedError()

    def get_column_display_size(self, column: int) -> int:
        raise NotImplementedError()


class RemoteResultSet:
    def next(self) -> bool:
        raise NotImplementedError()

    def get_int(self, fldname: str) -> int:
        raise NotImplementedError()

    def get_string(self, fldname: str) -> str:
        raise NotImplementedError()

    def get_meta_data(self) -> RemoteMetaData:
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


class RemoteStatement:
    def execute_query(self, qry: str) -> RemoteResultSet:
        raise NotImplementedError()

    def execute_update(self, cmd: str) -> int:
        raise NotImplementedError()


class RemoteConnection:
    def create_statement(self) -> RemoteStatement:
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


class RemoteDriver:
    def connect(self) -> RemoteConnection:
        raise NotImplementedError()


class RemoteMetaDataImpl(RemoteMetaData):
    """
    The server-side implementation of RemoteMetaData.
    """
    def __init__(self, sch: Schema):
        """
        Creates a metadata object that wraps the specified schema.
        The method also creates a list to hold the schema's
        collection of field names,
        so that the fields can be accessed by position.
        :param sch: the schema
        """
        self._sch = sch
        self._fields = []
        self._fields.extend(self._sch.fields())

    def get_column_count(self):
        """
        Returns the size of the field list.
        """
        return len(self._fields)

    def get_column_name(self, column: int):
        """
        Returns the field name for the specified column number.
        In JDBC, column numbers start with 1, so the field
        is taken from position (column-1) in the list.
        """
        return self._fields[column - 1]

    def get_column_type(self, column: int):
        """
        Returns the type of the specified column.
        The method first finds the name of the field in that column,
        and then looks up its type in the schema.
        """
        fldname = self.get_column_name(column)
        return self._sch.type(fldname)

    def get_column_display_size(self, column: int):
        """
        Returns the number of characters required to display the
        specified column.
        For a string-type field, the method simply looks up the
        field's length in the schema and returns that.
        For an int-type field, the method needs to decide how
        large integers can be.
        Here, the method arbitrarily chooses 6 characters,
        which means that integers over 999,999 will
        probably get displayed improperly.
        """
        fldname = self.get_column_name(column)
        fldtype = self._sch.type(fldname)
        fldlength = self._sch.length(fldname)
        if fldtype == INTEGER:
            return 6  # accommodate 6-digit integers
        else:
            return fldlength


class RemoteConnectionImpl(RemoteConnection):
    """
    The server-side implementation of RemoteConnection.
    """
    def __init__(self):
        """
        Creates a remote connection
        and begins a new transaction for it.
        """
        self._tx = Transaction()

    def create_statement(self):
        """
        Creates a new RemoteStatement for this connection.
        """
        daemon = SimpleDB.server_daemon
        assert isinstance(daemon, Pyro4.core.Daemon)
        uri = daemon.register(RemoteStatementImpl(self))
        return Pyro4.core.Proxy(uri)

    def close(self):
        """
        Closes the connection.
        The current transaction is committed.
        """
        self._tx.commit()

    # The following methods are used by the server-side classes.

    def get_transaction(self):
        """
        Returns the transaction currently associated with
        this connection.
        :return: the transaction associated with this connection
        """
        return self._tx

    def commit(self):
        """
        Commits the current transaction,
        and begins a new one.
        """
        self._tx.commit()
        self._tx = Transaction()

    def rollback(self):
        """
        Rolls back the current transaction,
        and begins a new one.
        """
        self._tx.rollback()
        self._tx = Transaction()

    def __getstate__(self):
        return {}


class RemoteResultSetImpl(RemoteResultSet):
    """
    The server-side implementation of RemoteResultSet.
    """
    def __init__(self, plan: Plan, rconn: RemoteConnectionImpl):
        """
        Creates a RemoteResultSet object.
        The specified plan is opened, and the scan is saved.
        :param plan: the query plan
        """
        self._s = plan.open()
        self._sch = plan.schema()
        self._rconn = rconn

    def next(self):
        """
        Moves to the next record in the result set,
        by moving to the next record in the saved scan.
        """
        try:
            return self._s.next()
        except RuntimeError as e:
            self._rconn.rollback()
            raise e

    def get_int(self, fldname: str):
        """
        Returns the integer value of the specified field,
         by returning the corresponding value on the saved scan.
        """
        try:
            fldname = fldname.lower()
            return self._s.get_int(fldname)
        except RuntimeError as e:
            self._rconn.rollback()
            raise e

    def get_string(self, fldname: str):
        """
        Returns the integer value of the specified field,
        by returning the corresponding value on the saved scan.
        """
        try:
            fldname = fldname.lower()
            return self._s.get_string(fldname)
        except RuntimeError as e:
            self._rconn.rollback()
            raise e

    def get_meta_data(self):
        """
        Returns the result set's metadata,
        by passing its schema into the RemoteMetaData constructor.
        """
        return Pyro4.core.Proxy(SimpleDB.server_daemon.register(RemoteMetaDataImpl(self._sch)))

    def close(self):
        """
        Closes the result set by closing its scan.
        """
        self._s.close()
        self._rconn.commit()

    def __getstate__(self):
        return {}


class RemoteStatementImpl(RemoteStatement):
    """
    The server-side implementation of RemoteStatement.
    """
    def __init__(self, rconn: RemoteConnectionImpl):
        self._rconn = rconn

    def execute_query(self, qry: str):
        """
        Executes the specified SQL query string.
        The method calls the query planner to create a plan
        for the query. It then sends the plan to the
        RemoteResultSetImpl constructor for processing.
        """
        try:
            tx = self._rconn.get_transaction()
            pln = SimpleDB.planner().create_query_plan(qry, tx)
            return Pyro4.core.Proxy(SimpleDB.server_daemon.register(RemoteResultSetImpl(pln, self._rconn)))
        except RuntimeError as e:
            self._rconn.rollback()
            raise e

    def execute_update(self, cmd: str):
        """
        Executes the specified SQL update command.
        The method sends the command to the update planner,
        which executes it.
        """
        try:
            tx = self._rconn.get_transaction()
            result = SimpleDB.planner().execute_update(cmd, tx)
            self._rconn.commit()
            return result
        except RuntimeError as e:
            self._rconn.rollback()
            raise e

    def __getstate__(self):
        return {}


class RemoteDriverImpl(RemoteDriver):
    """
    The server-side implementation of RemoteDriver.
    """
    def connect(self):
        """
        Creates a new RemoteConnectionImpl object and returns it.
        """
        daemon = SimpleDB.server_daemon
        assert isinstance(daemon, Pyro4.core.Daemon)
        rmt_conn_impl = RemoteConnectionImpl()
        uri = daemon.register(rmt_conn_impl)
        return Pyro4.core.Proxy(uri)


class SimpleMetaData:
    """
    An adapter class that wraps RemoteMetaData.
    """
    def __init__(self, md: RemoteMetaData):
        self._rmd = md

    def get_column_count(self):
        return self._rmd.get_column_count()

    def get_column_name(self, column: int) -> str:
        return self._rmd.get_column_name(column)

    def get_column_type(self, column: int) -> int:
        return self._rmd.get_column_type(column)

    def get_column_display_size(self, column: int) -> int:
        return self._rmd.get_column_display_size(column)


class SimpleResultSet:
    """
    An adapter class that wraps RemoteResultSet.
    """
    def __init__(self, s: RemoteResultSet):
        self._rrs = s

    def next(self):
        return self._rrs.next()

    def get_int(self, fldname):
        return self._rrs.get_int(fldname)

    def get_string(self, fldname):
        return self._rrs.get_string(fldname)

    def get_meta_data(self):
        rmd = self._rrs.get_meta_data()
        return SimpleMetaData(rmd)

    def close(self):
        return self._rrs.close()


class SimpleStatement:
    """
    An adapter class that wraps RemoteStatement.
    """
    def __init__(self, s: RemoteStatement):
        self._rstmt = s

    def execute_query(self, qry):
        rrs = self._rstmt.execute_query(qry)
        return SimpleResultSet(rrs)

    def execute_update(self, cmd):
        return self._rstmt.execute_update(cmd)


class SimpleConnection:
    """
    An adapter class that wraps RemoteConnection.
    """
    def __init__(self, c: RemoteConnection):
        self._rconn = c

    def create_statement(self):
        rstmt = self._rconn.create_statement()
        return SimpleStatement(rstmt)

    def close(self):
        self._rconn.close()


class SimpleDriver:
    """
    The SimpleDB database driver.
    """
    def connect(self, url: str):
        #Pyro4.config.SERIALIZERS_ACCEPTED.add("pickle")
        #Pyro4.config.SERIALIZER = "pickle"
        rmt_driver = Pyro4.core.Proxy("PYRONAME:simpledb")
        rmt_conn = rmt_driver.connect()
        return SimpleConnection(rmt_conn)







