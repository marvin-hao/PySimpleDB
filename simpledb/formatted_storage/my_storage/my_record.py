__author__ = 'Marvin'

from simpledb.shared_service.macro import *
from simpledb.formatted_storage.record import RID
from simpledb.plain_storage.file import Page, Block

from collections import OrderedDict
from math import ceil


class FieldInfo:
    def __init__(self, fldname, fldtype, lentype, nulltype=NULLABLE, fldlength=0):
        self.fldname = fldname
        self.fldtype = fldtype
        self.lentype = lentype
        self.nulltype = nulltype
        self.fldlength = fldlength

        if fldtype == TINYINT or fldtype == BOOLEAN or fldtype == CHAR:
            self.fldlength = 1  # physically stored as a single character
        elif fldtype == SMALLINT:
            self.fldlength = 2
        elif fldtype == INTEGER or fldtype == FLOAT or fldtype == DATE:
            self.fldlength = 4  # DATE value can be stored as an INTEGER, see datetime.date.toordinal()
        elif fldtype == BIGINT or fldtype == DOUBLE or fldtype == TIMESTAMP:
            self.fldlength = 8  # TIMESTAMP value can be stored as a DOUBLE, and can be converted to DATE


class Schema:
    def __init__(self):
        self._info = OrderedDict()

    def add_field(self, fldname, fldtype, lentype, nulltype=NULLABLE, fldlength=0):
        self._info[fldname] = FieldInfo(fldname, fldtype, lentype, nulltype, fldlength)

    def add(self, fldname, sch: Schema):
        self._info[fldname] = FieldInfo(fldname, sch.fldtype(fldname), sch.lentype(fldname), sch.nulltype(fldname),
                                        sch.fldlength(fldname))

    def add_all(self, sch: Schema):
        self._info.update(sch)

    def fldtype(self, fldname):
        return self._info.get(fldname).fldtype

    def lentype(self, fldname):
        return self._info.get(fldname).lentype

    def nulltype(self, fldname):
        return self._info.get(fldname).nulltype

    def fldlength(self, fldname):
        return self._info.get(fldname).fldlength

    def has_field(self, fldname):
        return fldname in self._info.keys()

    def fields(self):
        return self._info.keys()


class TableInfo:
    def __init__(self, tblname, schema: Schema):
        self._tblname = tblname
        self._schema = schema

        self._fix_notnull_fields = []
        self._var_notnull_fields = []
        self._fix_nullable_fields = []
        self._var_nullable_fields = []
        self._fix_notnull_fields_offset = []
        self._fix_nullable_length = []

        for fldname in schema.fields():
            fldtype = self._schema.fldtype(fldname)
            nulltype = self._schema.nulltype(fldname)

            if fldtype == FIXED_LENGTH and nulltype == NOTNULL:
                if len(self._fix_notnull_fields_offset) == 0:
                    self._fix_notnull_fields_offset.append(0)
                else:
                    # the offset of current fixed-length field is the sum of the last fixed-length field's offset
                    # and the length of the last fixed_field
                    self._fix_notnull_fields_offset.append(
                        self._fix_notnull_fields_offset[-1] + self.schema().fldlength(self._fix_notnull_fields[-1]))
                # finally we add the new fixed-length filed
                self._fix_notnull_fields.append(fldname)
            elif fldtype == FIXED_LENGTH and nulltype == NULLABLE:
                self._fix_nullable_fields.append(fldname)
                self._fix_nullable_length.append(schema.fldlength(fldname))
            elif fldtype == VARIABLE_LENGTH and nulltype == NOTNULL:
                self._var_notnull_fields.append(fldname)
            else:
                self._var_nullable_fields.append(fldname)

    def tblname(self):
        return self._tblname

    def schema(self):
        return self._schema

    def fix_notnull_fields(self):
        return self._fix_notnull_fields

    def fix_nullable_fields(self):
        return self._fix_nullable_fields

    def var_notnull_fields(self):
        return self._var_notnull_fields

    def var_nullable_fields(self):
        return self._var_nullable_fields

    def nullable_fields(self):
        return self._fix_nullable_fields + self._var_nullable_fields

    def has_var_nullable_fields(self):
        return not not self._var_nullable_fields

    def has_fix_nullable_fields(self):
        return not not self._fix_nullable_fields

    def has_fix_notnull_fields(self):
        return not not self._fix_notnull_fields

    def has_var_notnull_fields(self):
        return not not self._var_notnull_fields

    def has_nullable_fields(self):
        return self.has_var_nullable_fields() or self.has_fix_nullable_fields()

    def null_num(self):
        return len(self.nullable_fields())

    def fix_notnull_field_offset(self, fldname):
        return self._fix_notnull_fields_offset[self._fix_notnull_fields.index(fldname)]

    def fix_nullable_length_info(self):
        return self._fix_nullable_length


class LinkedRecordAccessor:
    def __init__(self, ti: TableInfo, page: Page, rec_offset):
        self._ti = ti
        self._page = page
        self._schema = self._ti.schema()
        self._rec_offset = rec_offset

    def set_page(self, page: Page):
        self._page = page

    def set_next_pos(self, offset, val: int):
        """
        set two out of four bytes in the header
        """
        self._page.set_ushort(offset, val)

    def get_next_pos(self, offset) -> int:
        return self._page.get_ushort(offset)

    def get_field_value(self, fldname):
        # for not-null fixed-length fields...
        if fldname in self._ti.fix_notnull_fields():
            # the actual offset is:
            # the record offset in the page
            # + the header size (4)
            # + the offset of a certain fixed-length field following the header
            offset = self._rec_offset + 4 + self._ti.fix_notnull_field_offset(fldname)
            length = self._schema.fldlength(fldname)
            fldtype = self._schema.fldtype(fldname)

            # only physical int or float type are fixed-length type
            if fldtype in [TINYINT, INTEGER, BIGINT, CHAR, BOOLEAN, SMALLINT]:
                return self.__unpack_value(length, offset, "int")
            elif fldtype in [FLOAT, DOUBLE, DATE, TIMESTAMP]:
                return self.__unpack_value(length, offset, "float")

        # for not-null variable-length fields...
        elif fldname in self._ti.var_notnull_fields():
            fldind = self._ti.var_notnull_fields().index(fldname)
            dir_entry_offset = self.__var_notnull_dir_offset() + fldind * 2
            data_offset = self._page.get_ushort(self._rec_offset + dir_entry_offset)

            # now decide the length of the data
            # this can be achieved by finding the offset of the next data

            if fldind == len(self._ti.var_notnull_fields()):
                # if the current field is the last not-null variable-length filed
                # then we have to see if there are more fields following this field

                if self._ti.has_var_nullable_fields():
                    next_data_offset = self.__var_nullable_data_offset()
                else:
                    # regardless whether there's any fixed-length nullable fields
                    next_data_offset = self.__fix_nullable_data_offset()
            else:
                next_data_offset = self._page.get_ushort(self._rec_offset + dir_entry_offset + 2)

            # string is the only possible type of variable-length field
            return self.__unpack_value(next_data_offset - data_offset, data_offset, "string")

        # for nullable variable-length fields...
        elif fldname in self._ti.var_nullable_fields():
            fldind = self._ti.var_nullable_fields().index(fldname)
            bitind = fldind % 8
            bitmap = self._page.get_nbytes(self.__var_null_bitmap_offset(), ceil((fldind + 1) / 8))
            assert not not bitmap
            if bitmap[-1] & (2 ** (7 - bitind)) == 0:  # meaning the corresponding bitmap is set as 1 (null value)
                # the bits are aligned to the left
                return None
            else:
                # set irrelevant bits to 1
                if bitind != 7:
                    bitmap[-1] |= 2 ** (7 - bitind) - 1
                field_ind = sum([(8 - bin(byte).count("1")) for byte in bitmap]) - 1
                dir_entry_offset = self.__var_nullbale_dir_offset() + field_ind * 2
                data_offset = self._page.get_ushort(self._rec_offset + dir_entry_offset)

                # now decide the next_data_offset
                is_the_last = False
                var_notnull_data_offset = self.__var_notnull_data_offset()
                if var_notnull_data_offset != -1:
                    if dir_entry_offset + 2 == var_notnull_data_offset:
                        is_the_last = True
                else:
                    if dir_entry_offset + 2 == self.__var_nullable_data_offset():
                        is_the_last = True

                if is_the_last:
                    next_data_offset = self.__fix_nullable_data_offset()
                else:
                    next_data_offset = self._page.get_ushort(self._rec_offset + dir_entry_offset + 2)
                return self.__unpack_value(next_data_offset - data_offset, data_offset, "string")

        # for nullable fixed-length fields...
        else:
            fldind = self._ti.fix_nullable_fields().index(fldname)
            bitind = fldind % 8
            bitmap = self._page.get_nbytes(self.__fix_null_bitmap_offset(), ceil((fldind + 1) / 8))
            assert not not bitmap
            if bitmap[-1] & (2 ** (7 - bitind)) == 0:
                return None
            else:
                fldlength = self._schema.fldlength(fldname)
                fldtype = self._schema.fldtype(fldname)
                int_bitmap = []
                bitmap = self._page.get_nbytes(self.__fix_null_bitmap_offset(), ceil((fldind + 1) / 8))
                [int_bitmap.extend(list(bin(byte))[2:]) for byte in bitmap]
                del int_bitmap[fldind:]
                length_info = self._ti.fix_nullable_length_info()[0: fldind]
                assert len(length_info) == len(int_bitmap)
                if len(length_info) == 0:
                    data_offset = self.__fix_nullable_data_offset()
                else:
                    data_offset = sum(
                        [a * b for a, b in zip(length_info, int_bitmap)]) + self.__fix_nullable_data_offset()
                return self.__unpack_value(fldlength, data_offset, fldtype)

    def __unpack_value(self, length, offset, valtype):
        if valtype == "int":
            if length == 1:
                return self._page.get_tinyint(offset)
            if length == 2:
                return self._page.get_short(offset)
            if length == 4:
                return self._page.get_int(offset)
            if length == 8:
                return self._page.get_int64(offset)
        elif valtype == "float":
            if length == 4:
                return self._page.get_float(offset)
            if length == 8:
                return self._page.get_double(offset)
        elif valtype == "string":
            return self._page.get_string(offset, length)

    def __fix_null_bitmap_offset(self):
        """
        :return: the offset of the null bitmap for fixed-length fields
        """
        return 4 + self._ti.fix_notnull_field_offset(self._ti.fix_notnull_fields()[-1]) + self._ti.schema().fldlength(
            self._ti.fix_notnull_fields()[-1])

    def __var_null_bitmap_offset(self):
        """
        :return: the offset of the null bitmap for variable-length fields
        """
        return self.__fix_null_bitmap_offset() + ceil(len(self._ti.fix_nullable_fields()) / 8)

    def __var_notnull_dir_offset(self):
        """
        :return: the offset of the offset dir for not-null variable-length fields
        """
        return self.__var_notnull_dir_offset() + ceil(len(self._ti.var_nullable_fields()) / 8)

    def __var_nullbale_dir_offset(self):
        """
        :return: the offset of the offset dir for nullable variable-length fields
        """
        return self.__var_notnull_dir_offset() + len(self._ti.var_notnull_fields()) * 2

    def __var_notnull_data_offset(self):
        if not self._ti.has_var_notnull_fields():
            return -1
        else:
            return self._page.get_ushort(self._rec_offset + self.__var_notnull_dir_offset())

    def __var_nullable_data_offset(self):
        if not self._ti.has_var_nullable_fields():
            return -1
        else:
            return self._page.get_ushort(self._rec_offset + self.__var_nullbale_dir_offset())

    def __fix_nullable_data_offset(self):
        """
        the offset of the first nullable fixed-length field is stored in the header
        if there's no such fields, the offset is actually the ending of the record
        not that the end of a record is not necessarily the beginning of the next record (due to deletion)
        """
        if not self._ti.has_fix_nullable_fields():
            return -1
        else:
            return self._page.get_ushort(self._rec_offset + 2)

    def __is_null(self, fldname):
        if fldname in self._ti.fix_nullable_fields():
            fldind = self._ti.fix_nullable_fields().index(fldname)
            byte_ind = fldind // 8
            byte_in_bitmap = self._page.get_tinyint(self._rec_offset + self.__fix_null_bitmap_offset() + byte_ind)
            bit_ind = fldind % 8
            if byte_in_bitmap & (2 ** bit_ind) == 0:
                return False
            else:
                return True
        elif fldname in self._ti.var_nullable_fields():
            fldind = self._ti.var_nullable_fields().index(fldname)
            byte_ind = fldind // 8
            byte_in_bitmap = self._page.get_tinyint(self._rec_offset + self.__var_null_bitmap_offset() + byte_ind)
            bit_ind = fldind % 8
            if byte_in_bitmap & (2 ** bit_ind) == 0:
                return False
            else:
                return True
        # if the field is not nullable, always return false
        return False

















