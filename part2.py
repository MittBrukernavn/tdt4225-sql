from DbConnector import DbConnector
from tabulate import tabulate


class Task2Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def task2_1(self):
        query = "SELECT (SELECT COUNT(*) FROM User AS `User count`), (SELECT COUNT(*) FROM Activity AS `Activity count`), (SELECT COUNT(*) FROM Trackpoint AS `Trackpoint count`);"
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        print(tabulate(results, headers=self.cursor.column_names))

    def task2_2(self):
        query = "SELECT AVG(T.activity_count) AS 'Average activity count' FROM (SELECT user_id, COUNT(Activity.id) AS activity_count FROM Activity GROUP BY user_id) T;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))

    def task2_3(self):
        query = "SELECT User.id AS `User id`, COUNT(Activity.id) AS `Number of activities` FROM User INNER JOIN Activity ON User.id=Activity.user_id GROUP BY User.id ORDER BY `Number of activities` DESC LIMIT 20;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))

    def task2_4(self):
        query = "SELECT DISTINCT(user_id) AS `Users who have taken a taxi` FROM Activity WHERE transportation_mode='taxi';"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))
    
    def task2_5(self):
        query = "SELECT transportation_mode, COUNT(id) as `Number of activities` FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))
    
    def task2_6a(self):
        query = "SELECT Year(start_date_time) AS `Year with most activities`, COUNT(*) AS `Number of activities` FROM Activity GROUP BY Year(start_date_time) ORDER BY `Number of activities` DESC LIMIT 1;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        #print(tabulate(results, headers=self.cursor.column_names))
        return results[0][0]
    
    def task2_6b(self):
        query = "SELECT YEAR(start_date_time) AS `Year with most recorded hours`, SUM(HOUR(TIMEDIFF(end_date_time, start_date_time)) + MINUTE(TIMEDIFF(end_date_time, start_date_time))/60 + SECOND(TIMEDIFF(end_date_time, start_date_time))/(60*60)) AS total_hours FROM Activity GROUP BY YEAR(start_date_time) ORDER BY total_hours DESC LIMIT 1;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))

        if self.task2_6a() == results[0][0]:
            return "Yes, this the the year with the highest recorded activities."
        else:
            return "Nope. This year is not the one with the highest recorded activites"

def main():
    try:
        task2program = Task2Program()
        print("Task 2.1:")
        task2program.task2_1()
        print("Task 2.2:")
        task2program.task2_2()
        print("Task 2.3:")
        task2program.task2_3()
        print("Task 2.4:")
        task2program.task2_4()
        print("Task 2.5:")
        task2program.task2_5()
        
       
        print("Task 2.6a:")
        print("Year with most activities: ", task2program.task2_6a())

        print("Task 2.6b:")
        print(task2program.task2_6b())
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if task2program:
            task2program.connection.close_connection()


if __name__ == '__main__':
    main()