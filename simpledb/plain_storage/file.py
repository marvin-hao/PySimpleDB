__author__ = 'Marvin'

import os
from os import path
import sys
import struct
import math

from simpledb.shared_service.macro import *
from simpledb.shared_service.server import SimpleDB
from simpledb.shared_service.util import synchronized, java_string_hashcode


class BlockHeader:
    def __init__(self, blk=None, bb=None):
        if blk is None:
            self.__init_header()
        else:
            if bb is None:
                raise Exception("empty header")
            self.read_header(bb)
            self.blk = blk

    def __init_header(self):
        raise NotImplementedError()

    def format_header(self) -> bytearray:
        raise NotImplementedError()

    def read_header(self, bb: bytearray):
        raise NotImplementedError()

    def new_blk_header(self, blk: Block):
        raise NotImplementedError()


class Block:
    """
    A reference to a disk block.A Block object consists of a filename and a block number.
    It does not hold the contents of the block; instead, that is the job of an object.
    """

    def __init__(self, filename, blknum):
        """
        Constructs a block reference for the specified filename and block number.
        :param filename: the name of the file
        :param blknum: the block number
        """
        self._filename = filename
        self._blknum = blknum

    def file_name(self):
        """
        Returns the name of the file where the block lives.
        :return: the filename
        """
        return self._filename

    def number(self):
        """
        Returns the location of the block within the file.
        :return: return the block number
        """
        return self._blknum

    def __eq__(self, other):
        """
        used to compare whether two objects have identical filename and blknum
        after this overriding, we can simply use "a == b" syntax
        :param other: the object compared with current object
        """
        if isinstance(other,
                      self.__class__) and self._blknum == other._blknum and self._filename == other._filename:
            return True
        else:
            return False

    def __ne__(self, other):
        if isinstance(other,
                      self.__class__) and self._blknum == other._blknum and self._filename == other._filename:
            return False
        else:
            return True

    def __str__(self):
        """
        used to return a string object giving the formatted information of an instance
        """
        return "[fileTest " + str(self._filename) + ", block " + str(self._blknum) + "]"

    def __hash__(self):
        """
        make Block hashable so that it can be the key of a map
        """
        return hash((self._filename, self._blknum))

    def hash_code(self):
        return java_string_hashcode(self.__str__())


class FileMgr:
    """
    The SimpleDB file manager. The database system stores its data as files within a specified directory.
    The file manager provides methods for reading the contents of a file block to a Java byte buffer,
    writing the contents of a byte buffer to a file block,
    and appending the contents of a byte buffer to the end of a file.
    These methods are called exclusively by the class {@link simpledb.simpledb.file.Page Page},
    and are thus package-private.
    The class also contains two public methods:
    isNew() is called during system initialization by simpledb.server.SimpleDB
    size(String) is called by the log manager and transaction manager to determine the end of the file.
    """

    def __init__(self, dbname):
        """
        Creates a file manager for the specified database.
        The database will be stored in a folder of that name
        in the user's home directory.
        If the folder does not exist, then a folder containing
        an empty database is created automatically.
        Files for all temporary tables (i.e. tables beginning with "temp") are deleted.
        :param dbname: the name of the directory that holds the database
        """
        self._openFiles = {}
        home_dir = path.expanduser("~")
        self._dbDirectory = path.join(home_dir, dbname)
        if not path.isdir(self._dbDirectory):
            self._isNew = True
            try:
                os.mkdir(self._dbDirectory)
            except FileExistsError:
                raise FileExistsError("cannot create " + dbname)
        else:
            self._isNew = False

        # eliminate temporary files
        dir_listing = os.listdir(self._dbDirectory)
        files = [f for f in dir_listing if path.isfile(path.join(self._dbDirectory, f))]
        for file_to_delete in files:
            if file_to_delete.startswith("temp"):
                os.remove(path.join(self._dbDirectory, file_to_delete))

    @synchronized
    def read(self, blk, bb):
        """
        Reads the contents of a disk block into a bytearray
        :param blk: disk block, a Block object
        :param bb: the bytearray
        :return: number of bytes read into from file
        """
        try:
            assert isinstance(bb, bytearray) and isinstance(blk, Block)
            # not yet clear the content of bb (it's useless)
            fc = self.get_file(blk.file_name())
            fc.seek(BLOCK_SIZE * blk.number())
            return fc.readinto(bb)  # Read up to len(b) bytes into bytearray b and return the number of bytes read.
        except IOError:
            raise RuntimeError("cannot read block" + blk)

    @synchronized
    def write(self, blk, bb):
        """
        Writes the contents of a bytearray into a disk block.
        :param blk: disk block, a Block object
        :param bb: the bytearray
        :return: number of bytes written into file
        """
        try:
            assert isinstance(bb, bytearray) and isinstance(blk, Block)
            fc = self.get_file(blk.file_name())
            fc.seek(BLOCK_SIZE * blk.number())
            fc.write(bb)
        except IOError:
            raise RuntimeError("cannot write block" + blk)

    @synchronized
    def size(self, filename):
        """
        Returns the number of blocks in the specified file.
        Since every opened file might have buffered data
        which are not flushed back to file.
        Thus, using seek() and tell() methods is the alternative
        in Python as we want.
        :param filename: the name of the file
        :return: the number of blocks in the file
        """
        try:
            file = self.get_file(filename)
            file.seek(0, 2)
            return math.ceil(file.tell() / BLOCK_SIZE)
        except IOError:
            raise RuntimeError("cannot plain_storage" + filename)

    @synchronized
    def append(self, filename, bb):
        """
        Appends the contents in a bytebuffer to the end
        :param filename: the name of the file
        :param bb: the bytearray
        :return: a reference to the newly-created block.
        """
        assert isinstance(filename, str) and isinstance(bb, bytearray)
        newblknum = self.size(filename)
        blk = Block(filename, newblknum)
        self.write(blk, bb)
        return blk

    def is_new(self):
        """
        Returns a boolean indicating whether the file manager
        had to create a new database directory.
        :return: true if the database is new
        """
        return self._isNew

    def get_file(self, filename):
        """
        Returns the file for the specified filename.
        The file is stored in a map keyed on the filename.
        If the file is not open, then it is opened and the file
        is added to the dictionary.
        :param filename: the specified filename
        :return: the file channel associated with the open file.
        """
        try:
            assert isinstance(filename, str)
            fc = self._openFiles.get(filename)
            if fc is None:
                full_path = path.join(self._dbDirectory, filename)
                if os.path.exists(full_path):
                    fc = open(full_path, 'r+b', buffering=0)  # open a binary file for reading and writing
                else:
                    fc = open(full_path, 'w+b', buffering=0)
                self._openFiles[filename] = fc
            return fc
        except:
            raise IOError("Cannot open" + filename)

    def __getstate__(self):
        result = self.__dict__.copy()
        del result["_openFiles"]
        return result


class Page:
    """
    The contents of a disk block in memory.
    A page is treated as an array of BLOCK_SIZE bytes.
    There are methods to get/set values into this array,
    and to read/write the contents of this array to a disk block.
    """

    INT_SIZE = len(struct.pack("i", 0))  # Return the number of bytes in an integer

    MAX_BYTES_PER_CHAR = len(struct.pack("I", sys.maxunicode))  # Keep the possible max size of a character

    def __init__(self):
        """
        Creates a new page.  Although the constructor takes no arguments,
        it depends on a FileMgr object which is created during system initialization.
        Thus this constructor cannot be called until at least one of the
        initialization static functions is called first.
        """
        self._contents = bytearray(BLOCK_SIZE)
        self._file_mgr = SimpleDB.file_mgr()

    def read(self, blk: Block):
        raise NotImplementedError()

    def write(self, blk: Block):
        raise NotImplementedError()

    def append(self, filename):
        raise NotImplementedError()

    def set_int(self, offset, val):
        struct.pack_into("i", self._contents, offset, val)

    def get_int(self, offset):
        """
        Returns the integer value at a specified offset of the page.
        If an integer was not stored at that location,
        the behavior of the method is unpredictable.
        :param offset: the byte offset within the page
        :return: the integer value at that offset
        """
        return struct.unpack_from("i", self._contents, offset)[0]

    def set_tinyint(self, offset, val):
        """
        TINYINT is viewed as a char of 1 byte
        BOOLEAN value is stored as TINYINT(0) or TINYINT(1)
        """
        if 0 <= val <= 255:
            struct.pack_into("c", self._contents, offset, val)
        else:
            raise UnicodeTranslateError("tiny int value out of range")

    def get_tinyint(self, offset):
        return int(struct.unpack_from("c", self._contents, offset)[0])

    def get_nbytes(self, offset, n):
        return self._contents[offset: offset + n]

    def set_nbytes(self, offset, n, values: bytes):
        self._contents[offset: offset + n] = values

    def set_uint(self, offset, val):
        struct.pack_into("I", self._contents, offset, val)

    def get_uint(self, offset):
        return struct.unpack_from("I", self._contents, offset)[0]

    def get_short(self, offset):
        return struct.unpack_from("h", self._contents, offset)[0]

    def set_short(self, offset, val):
        struct.pack_into("h", self._contents, offset, val)

    def get_ushort(self, offset):
        return struct.unpack_from("H", self._contents, offset)[0]

    def set_ushort(self, offset, val):
        struct.pack_into("H", self._contents, offset, val)

    def get_int64(self, offset):
        return struct.unpack_from("q", self._contents, offset)[0]

    def set_int64(self, offset, val):
        struct.pack_into("q", self._contents, offset, val)

    def get_float(self, offset):
        return struct.unpack_from("f", self._contents, offset)[0]

    def set_float(self, offset, val):
        struct.pack_into("f", self._contents, offset, val)

    def get_double(self, offset):
        return struct.unpack_from("d", self._contents, offset)[0]

    def set_double(self, offset, val):
        struct.pack_into("d", self._contents, offset, val)

    def get_string(self, offset, length):
        """
        the correctness of the length is guaranteed from outside
        """
        return self._contents[offset: offset + length].decode("utf8")

    def set_string(self, offset, val):
        """
        the correctness of the length is guaranteed from outside
        """
        string_in_bytes = bytearray(val, "utf8")
        self._contents[offset: offset + len(string_in_bytes)] = string_in_bytes

    def clear(self):
        """
        Clear all the contents in self._contest
        """
        self._contents = bytearray(BLOCK_SIZE)


class MaxPage(Page):
    @staticmethod
    def str_size(n):
        return MaxPage.INT_SIZE + n * MaxPage.MAX_BYTES_PER_CHAR  # The first position keeps the size of the string

    def __init__(self):
        super().__init__()

    @synchronized
    def read(self, blk):
        """
        Populates the page with the contents of the specified disk block.
        :param blk: a reference to a disk block
        """
        assert isinstance(blk, Block)
        self._file_mgr.read(blk, self._contents)

    @synchronized
    def write(self, blk):
        """
        Writes the contents of the page to the specified disk block.
        :param blk: a reference to a disk block
        """
        assert isinstance(blk, Block)
        self._file_mgr.write(blk, self._contents)

    @synchronized
    def append(self, filename):
        """
        Appends the contents of the page to the specified file.
        :param filename: the name of the file
        :return: a newly-created disk block
        """
        return self._file_mgr.append(filename, self._contents)

    @synchronized
    def get_string(self, offset):
        """
        Returns the string value at the specified offset of the page.
        If a string was not stored at that location,
        the behavior of the method is unpredictable.
        Note that all the strings are stored following big endian.
        :param offset: the byte offset within the page
        :return: the string value at that offset
        """
        size = self.get_int(offset)
        if size <= 0 or size > 400:
            return ""  # This is where Python is different with Java
        string_byte_array = self._contents[offset + MaxPage.INT_SIZE:offset + MaxPage.INT_SIZE + size]
        fmt = ">" + str(len(string_byte_array)) + "s"
        return struct.unpack(fmt, string_byte_array)[0].decode("utf-32-be")

    @synchronized
    def set_string(self, offset, val):
        assert isinstance(val, str)
        string_byte_array = bytearray(val, "utf-32-be")
        size = len(string_byte_array)
        int_byte_array = struct.pack("I", size)
        self._contents[offset:offset + MaxPage.INT_SIZE + size] = int_byte_array + string_byte_array
        # A bytearray object added by a bytes object yields a concatenated bytearray object. That's cool!