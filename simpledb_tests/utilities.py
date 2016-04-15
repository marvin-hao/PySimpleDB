__author__ = 'Marvin'

from os import path, listdir, remove
import shutil
import sys


def remove_db():
    home_dir = path.expanduser("~")
    db_directory = path.join(home_dir, "test")
    shutil.rmtree(db_directory)


def remove_all():
    home_dir = path.expanduser("~")
    db_directory = path.join(home_dir, "test")
    dir_listing = listdir(db_directory)
    [remove(path.join(db_directory, f)) for f in dir_listing if path.isfile(path.join(db_directory, f))]


def remove_some_start_with(prefix):
    home_dir = path.expanduser("~")
    db_directory = path.join(home_dir, "test")
    dir_listing = listdir(db_directory)
    [remove(path.join(db_directory, f)) for f in dir_listing if
     f.startswith(prefix) and path.isfile(path.join(db_directory, f))]


def keep(prefix):
    sys.stdout.flush()
    home_dir = path.expanduser("~")
    db_directory = path.join(home_dir, "test")
    dir_listing = listdir(db_directory)
    [shutil.copyfile(path.join(db_directory, f), path.join(db_directory, "copy_"+f)) for f in dir_listing if
     f.startswith(prefix) and path.isfile(path.join(db_directory, f))]