from DbConnector import DbConnector
from tabulate import tabulate


class Task2Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def task2_1(self):
        query = "SELECT (SELECT COUNT(*) FROM User) AS `User count`, (SELECT COUNT(*) FROM Activity) AS `Activity count`, (SELECT COUNT(*) FROM Trackpoint) AS `Trackpoint count`;"
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

    ## TODO: Task 2.7
    def task2_7(self):
        pass

    def task2_8(self):
        query = "SELECT Sub.UserID, Sub.Altitude_km FROM ( SELECT Activity.user_id AS userID, SUM(CASE WHEN TP1.altitude IS NOT NULL AND TP2.altitude IS NOT NULL THEN (TP2.altitude - TP1.altitude) * 0.0003048 ELSE 0 END) AS Altitude_km FROM Trackpoint AS TP1 INNER JOIN Trackpoint AS TP2 ON TP1.activity_id=TP2.activity_id AND TP1.id+1 = TP2.id INNER JOIN Activity ON Activity.id = TP1.activity_id AND Activity.id = TP2.activity_id WHERE TP2.altitude > TP1.altitude GROUP BY Activity.user_id ) AS Sub ORDER BY Altitude_km DESC LIMIT 20;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))
    
    def task2_9(self):
        query = "SELECT Activity.user_id, COUNT(DISTINCT(activity_id)) as `Number of illegal activities` FROM (SELECT TP1.activity_id AS activity_id, (TP2.date_days - TP1.date_days) AS minute_diff FROM Trackpoint AS TP1 INNER JOIN Trackpoint AS TP2 ON TP1.activity_id=TP2.activity_id AND TP1.id+1=TP2.id HAVING minute_diff >= 0.00347222222) AS Subtable JOIN Activity ON Activity.id = Subtable.activity_id GROUP BY Activity.user_id;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))

    def task2_10(self):
        # Results vary slightly depending on how much leeway you give the coordinates. 
        # Trying to match the exact coordinates given provides you with 0 results.
        query = "SELECT DISTINCT(User.id) AS `Users in forbidden City of Beijing` FROM User INNER JOIN Activity ON User.id=Activity.user_id INNER JOIN Trackpoint ON Activity.id=Trackpoint.activity_id WHERE (lat>39.910 AND lat<39.922) AND (lon>116.390 AND lon<116.404);"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))

    ## TODO: Task 2.11
    def task2_11(self):
        pass

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

        ## TODO: Task 2.7. Uncomment below when functions are made.
        # print("Task 2.7:")
        # task2program.task2_7()
        
        print("Task 2.8:")
        task2program.task2_8()

        print("Task 2.9:")
        task2program.task2_9()
        
        print("Task 2.10:")
        task2program.task2_10()

        ## TODO: Task 2.11. Uncomment below when functions are made.
        # print("Task 2.7:")
        # task2program.task2_7()
       
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if task2program:
            task2program.connection.close_connection()

if __name__ == '__main__':
    main()