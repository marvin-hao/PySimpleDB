__author__ = 'Marvin'
from simpledb.connection.remote import SimpleDriver

d = SimpleDriver()
conn = d.connect("foo")
stmt = conn.create_statement()
print("create a new table...")
s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)"
stmt.execute_update(s)
print("insert data...")
s = "insert into STUDENT(SId, SName, MajorId, GradYear) values "
studvals = ["(1, 'joe', 10, 2004)",
            "(2, 'amy', 20, 2004)",
            "(3, 'max', 10, 2005)",
            "(4, 'sue', 20, 2005)",
            "(5, 'bob', 30, 2003)",
            "(6, 'kim', 20, 2001)",
            "(7, 'art', 30, 2004)",
            "(8, 'pat', 20, 2001)",
            "(9, 'lee', 10, 2004)"]
for val in studvals:
    stmt.execute_update(s + val)

qry = "select sname, gradyear from student where gradyear = 2004 "
print("find the students who graduated in 2004")
rs = stmt.execute_query(qry)
while rs.next():
    print(rs.get_string("SName"))

print("delete them...")
update = "delete from STUDENT where gradyear = 2004"
stmt.execute_update(update)

print("now find them again...")
rs = stmt.execute_query(qry)
while rs.next():
    print(rs.get_string("SName"))
print("results have been shown already.")