from DbConnector import DbConnector
from os import getcwd, walk

def main():
    connection = DbConnector()

    def create_table(table_name, definition):
        query = f'CREATE TABLE IF NOT EXISTS {table_name} ({definition})'
        connection.cursor.execute(query)
        connection.commit()
    
    # table definitions
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

    # create the tables
    for table_name, table_definition in tables.items():
        create_table(table_name, table_definition)
    
    working_directory = getcwd()

    data_directory, user_ids, _ = next(walk(f'{working_directory}/dataset/Data'))

    # sorting the user IDs makes it (much) easier to deal with has_labels
    user_ids.sort()
    
    user_data = [[user_id, False] for user_id in user_ids]

    with open(f'{working_directory}/dataset/labeled_ids.txt') as f:
        for line in f:
            assert line.strip() == user_data[int(line)][0]
            user_data[int(line)][1] = True
    
    print(*user_data, sep='\n')
    
    connection.cursor.executemany('INSERT INTO User (id, has_labels) VALUES (%s, %s)', user_data)
    connection.commit()

    # Insert activities:
    activity_data = []
    trackpoint_data = []
    for [user_id, has_labels] in user_data:
        pass


    # Insert trackpoints:

    connection.close_connection()

if __name__ == '__main__':
    main()