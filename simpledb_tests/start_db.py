__author__ = 'Marvin'
import sys
import shutil
from os import path


if __name__ == "__main__":
    sys.path.append(path.dirname(__file__))

    from simpledb.shared_service.server import Startup
    dbname = sys.argv[1]
    dbpath = path.join(path.expanduser("~"), dbname)

    if path.exists(dbpath):
        shutil.rmtree(dbpath)

    Startup.main(dbname)
