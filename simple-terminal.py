from DbConnector import DbConnector

# (very) simple mysql cli, for debugging purposes

connection = DbConnector()

cmd = input('mysql> ')

while cmd != 'exit':
    connection.cursor.execute(cmd)
    output = connection.cursor.fetchall()
    if(output):
        print(output)
    cmd = input('mysql> ')