__author__ = 'Marvin'
import struct

from simpledb.plain_storage.file import BlockHeader, Page, Block
from simpledb.shared_service.util import synchronized


class OracleBlockHeader(BlockHeader):
    def __init__(self, blk=None, bb=None):
        super().__init__(blk, bb)

    def __init_header(self):
        self.body_offset = 6  # specifies the offset of the body, which is also right after the end of the header
        self.table_directory_offset = 6  # a 2-byte unsigned short integer
        self.row_directory_offset = 6  # a 2-byte unsigned short integer
        self.table_dir = []  # each entry in table_dir is a 8-byte hashed value specifies a table.
        self.row_dir = []  # each entry in row_dir is a 2-byte offset of a row piece.

    def format_header(self) -> bytearray:
        # the length of the header is the value of body_offset
        header = bytearray(self.body_offset)

        # write the first 6 indispensable bytes
        fmt = "HHH"
        struct.pack_into(fmt, header, 0, self.body_offset, self.table_directory_offset, self.row_directory_offset)

        # write the table_dir
        if self.table_dir:
            struct.pack_into("l" * len(self.table_dir), header, 6, *self.table_dir)

        # write the row_dir
        if self.row_dir:
            struct.pack_into("h" * len(self.row_dir), header, 6 + 8 * len(self.table_dir), *self.row_dir)

        return header

    def read_header(self, bb: bytearray):
        # read the first 6 bytes
        fmt = "HHH"
        (self.body_offset, self.table_directory_offset, self.row_directory_offset) = struct.unpack_from(fmt, bb, 0)

        # read the table_dir
        self.table_dir = list(
            struct.unpack_from("l" * ((self.row_directory_offset - self.table_directory_offset) // 8), bb,
                               self.table_directory_offset))

        # read the row_dir
        self.row_dir = list(
            struct.unpack_from("h" * ((self.body_offset - self.row_directory_offset) // 2), bb,
                               self.row_directory_offset))

    def new_blk_header(self, blk: Block):
        self.__init_header()
        self.blk = blk

    def add_row(self, offset):
        if offset < self.body_offset:
            raise Exception("row offset error")
        self.row_dir.append(offset)
        self.body_offset += 2

    def delete_row(self, offset):
        try:
            ind = self.row_dir.index(offset)
            self.row_dir[ind] = -offset
        except ValueError:
            print("deletion happens at wrong position.")

    def add_table(self):
        raise NotImplementedError()  # todo

    def delete_table(self):
        raise NotImplementedError()  # todo


class OraclePage(Page):
    """
    This Page class deals with strings in a UTF8-manner,
    which means the number of bytes of a string may not be a fixed number of times of its length.
    This Page class also uses OracleBlockHeader as its header class.

    Note that the consistency of header and contents is guaranteed from outside
    """
    def __init__(self):
        super().__init__()
        self._header = None

    @synchronized
    def read(self, blk: Block):
        self._file_mgr.read(blk, self._contents)
        self._header = OracleBlockHeader(blk, self._contents)

    @synchronized
    def write(self, blk: Block):
        header_bb = self._header.format_header()
        self._contents[:len(header_bb)] = header_bb  # refresh the header in content in case of any changes
        self._file_mgr.write(blk, self._contents)

    @synchronized
    def append(self, filename):
        header_bb = OracleBlockHeader().format_header()
        self._contents[:len(header_bb)] = header_bb
        self._file_mgr.append(filename, self._contents)