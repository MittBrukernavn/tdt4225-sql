# I HAVE NOT RUN THIS CODE
# It should in principle work, but it's just placeholder for until we've set up the database

from DbConnector import DbConnector

connection = DbConnector()

print('creating test table')
connection.cursor.execute('CREATE TABLE `TestTable`(`id` int(11) NOT NULL AUTO_INCREMENT)')

connection.cursor.execute('SHOW TABLES')
# alternatively fetchone
tables = connection.cursor.fetchall()
print(tables)

print('Deleting test tables')
connection.cursor.execute('DROP TABLE `TestTable`')

connection.cursor.execute('SHOW TABLES')
# alternatively fetchone
tables = connection.cursor.fetchall()
print(tables)


connection.close_connection()