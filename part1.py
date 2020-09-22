# I have run this code locally, but against mysql 8.0, so errors might exist

from DbConnector import DbConnector

connection = DbConnector()

print('creating test table')
connection.cursor.execute('CREATE TABLE `TestTable`(`id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT)')

connection.cursor.execute('SHOW TABLES')
# alternatively fetchone
tables = connection.cursor.fetchall()
print(tables)

connection.commit()

print('Deleting test tables')
connection.cursor.execute('DROP TABLE `TestTable`')

connection.commit()

connection.cursor.execute('SHOW TABLES')
# alternatively fetchone
tables = connection.cursor.fetchall()
print(tables)


connection.close_connection()