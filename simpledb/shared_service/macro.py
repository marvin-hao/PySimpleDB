__author__ = 'Marvin'

BLOCK_SIZE = 400  # The number of bytes in a block. This value is set unreasonably low,
                  # so that it is easier to create and test databases having a lot of blocks.
                  # A more realistic value would be 4K.

# type of the length of fields
FIXED_LENGTH = 0
VARIABLE_LENGTH = 1

# if the field is nullable
NOTNULL = 0
NULLABLE = 1

# Provides the names of sql types
ARRAY = 2003
BIGINT = -5  # in use
BINARY = -2
BIT = -7
BLOB = 2004
BOOLEAN = 16  # in use
CHAR = 1  # in use
CLOB = 2005
DATALINK = 70
DATE = 91  # in use
DECIMAL = 3
DISTINCT = 2001
DOUBLE = 8  # in use
FLOAT = 6  # in use
INTEGER = 4  # in use
JAVA_OBJECT = 2000
LONGNVARCHAR = -16
LONGVARBINARY = -4
LONGVARCHAR = -1
NCHAR = -15
NCLOB = 2011
NULL = 0
NUMERIC = 2
NVARCHAR = -9
OTHER = 1111
REAL = 7
REF = 2006
ROWID = -8
SMALLINT = 5  # in use
SQLXML = 2009
STRUCT = 2002
TIME = 92
TIMESTAMP = 93  # in use
TINYINT = -6  # in use
VARBINARY = -3
VARCHAR = 12  # in use