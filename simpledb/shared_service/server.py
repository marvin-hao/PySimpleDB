__author__ = 'Marvin'
import socket
import select
from os import path

import Pyro4.naming
import Pyro4.core


class SimpleDB:
    """
    The class that provides system-wide static global values.
    """

    BUFFER_SIZE = 8
    LOG_FILE = "simpledb.log"

    # In Python, we simply make "private static" attribute "public static"

    fm = None
    bm = None
    logm = None
    mdm = None
    server_daemon = None

    @staticmethod
    def init_file_mgr(dirname):
        """
        Initializes only the file manager.
        :param dirname: the name of the database directory
        """
        from simpledb.plain_storage.file import FileMgr  # The original code has circular dependencies, we resolve this problem
                                           # by importing module here instead
        assert isinstance(dirname, str)
        SimpleDB.fm = FileMgr(dirname)

    @staticmethod
    def init_file_and_log_mgr(dirname):
        """
        Initializes the file and log managers.
        :param dirname: the name of the database directory
        """
        from simpledb.formatted_storage.log import LogMgr
        assert isinstance(dirname, str)
        SimpleDB.init_file_mgr(dirname)
        SimpleDB.logm = LogMgr(SimpleDB.LOG_FILE)
        from simpledb.formatted_storage.recovery import LogRecord
        LogRecord.log_mgr = SimpleDB.logm

    @staticmethod
    def init_file_log_and_buffer_mgr(dirname):
        """
        Initializes the file, log, and buffer managers.
        :param dirname: the name of the database directory
        """
        from simpledb.plain_storage.bufferslot import BufferMgr
        assert isinstance(dirname, str)
        SimpleDB.init_file_and_log_mgr(dirname)
        SimpleDB.bm = BufferMgr(SimpleDB.BUFFER_SIZE)

    @staticmethod
    def init(dirname):
        """
        Initializes the system.
        This method is called during system startup.
        :param dirname: the name of the database directory
        """
        assert isinstance(dirname, str)
        SimpleDB.init_file_log_and_buffer_mgr(dirname)
        from simpledb.formatted_storage.tx import Transaction
        tx = Transaction()
        isnew = SimpleDB.fm.is_new()
        if isnew:
            print("creating new database")
        else:
            print("recovering existing database")
            tx.recover()
        SimpleDB.init_meta_data_mgr(isnew, tx)
        tx.commit()

    @staticmethod
    def init_meta_data_mgr(isnew, tx):
        from simpledb.formatted_storage.metadata import MetaDataMgr
        SimpleDB.mdm = MetaDataMgr(isnew, tx)

    @staticmethod
    def file_mgr():
        return SimpleDB.fm

    @staticmethod
    def buffer_mgr():
        return SimpleDB.bm

    @staticmethod
    def log_mgr():
        return SimpleDB.logm

    @staticmethod
    def md_mgr():
        return SimpleDB.mdm

    @staticmethod
    def planner():
        """
        Creates a planner for SQL commands.
        To change how the planner works, modify this method.
        :return: the system's planner for SQL commands
        """
        from simpledb.query_prosessor.planner import BasicQueryPlanner, BasicUpdatePlanner, Planner
        qplanner = BasicQueryPlanner()
        uplanner = BasicUpdatePlanner()
        return Planner(qplanner, uplanner)


class Startup:
    @staticmethod
    def main(dbname):
        # configure and initialize the database
        SimpleDB.init(dbname)
        print("dbpath: " + path.join(path.expanduser("~"), dbname))

        from simpledb.connection.remote import RemoteDriverImpl
        Pyro4.config.SERVERTYPE = "thread"
        hostname = socket.gethostname()

        nameserverUri, nameserverDaemon, broadcastServer = Pyro4.naming.startNS(host=hostname)
        assert broadcastServer is not None, "expect a broadcast server to be created"

        # create a Pyro daemon
        SimpleDB.server_daemon = Pyro4.core.Daemon(host=hostname)

        # register a server object with the daemon
        serveruri = SimpleDB.server_daemon.register(RemoteDriverImpl())

        # register it with the embedded nameserver directly
        nameserverDaemon.nameserver.register("simpledb", serveruri)

        print("database server ready")

        try:
            # below is our custom event loop.
            while True:
                # create sets of the socket objects we will be waiting on
                # (a set provides fast lookup compared to a list)
                nameserverSockets = set(nameserverDaemon.sockets)
                pyroSockets = set(SimpleDB.server_daemon.sockets)
                rs = [broadcastServer]  # only the broadcast server is directly usable as a select() object
                rs.extend(nameserverSockets)
                rs.extend(pyroSockets)
                rs, _, _ = select.select(rs, [], [], 3)
                eventsForNameserver = []
                eventsForDaemon = []
                for s in rs:
                    if s is broadcastServer:
                        broadcastServer.processRequest()
                    elif s in nameserverSockets:
                        eventsForNameserver.append(s)
                    elif s in pyroSockets:
                        eventsForDaemon.append(s)
                if eventsForNameserver:
                    nameserverDaemon.events(eventsForNameserver)
                if eventsForDaemon:
                    print("Daemon received a request")
                    SimpleDB.server_daemon.events(eventsForDaemon)
        except KeyboardInterrupt:
            nameserverDaemon.close()
            broadcastServer.close()
            SimpleDB.server_daemon.close()
            print("database server shut down")