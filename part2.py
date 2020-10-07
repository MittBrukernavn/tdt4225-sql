from DbConnector import DbConnector
from tabulate import tabulate


class Task2Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def task2_1(self):
        query = 'SELECT (SELECT COUNT(*) FROM User AS `User count`), (SELECT COUNT(*) FROM Activity AS `Activity count`), (SELECT COUNT(*) FROM Trackpoint AS `Trackpoint count`);'
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        print(tabulate(results, headers=['User count', 'Activity count', 'Trackpoint count']))

def main():
    try:
        task2program = Task2Program()
        task2program.task2_1()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if task2program:
            task2program.connection.close_connection()


if __name__ == '__main__':
    main()