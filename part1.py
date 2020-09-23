from DbConnector import DbConnector

def main():
    connection = DbConnector()

    def create_table(table_name, definition):
        query = f'CREATE TABLE IF NOT EXISTS {table_name} ({definition})'
        connection.cursor.execute(query)
        connection.commit()
    
    tables = {
        'User': '''
            id CHAR(3) PRIMARY KEY NOT NULL,
            has_labels BOOLEAN NOT NULL
        ''',
        'Activity': '''
            id INT PRIMARY KEY NOT NULL,
            user_id CHAR(3) NOT NULL,
            transportation_mode ENUM('walk', 'bike', 'bus', 'car', 'subway', 'train', 'airplane', 'boat', 'run', 'motorcycle', 'taxi') DEFAULT NULL,
            start_date_time TEXT NOT NULL,
            end_date_time TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
        ''',
        'Trackpoint': '''
            id INT PRIMARY KEY NOT NULL,
            activity_id INT NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            altitude DOUBLE,
            date_days DOUBLE NOT NULL,
            date_time DATETIME NOT NULL,
            FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
        ''',
    }

    for table_name, table_definition in tables.items():
        create_table(table_name, table_definition)
    
    connection.close_connection()

if __name__ == '__main__':
    main()