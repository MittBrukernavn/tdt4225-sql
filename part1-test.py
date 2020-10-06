from DbConnector import DbConnector
from os import getcwd, walk
from time import time as t


def main():
    connection = DbConnector()
    t0 = t()
    
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
            id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
            user_id CHAR(3) NOT NULL,
            transportation_mode ENUM('walk', 'bike', 'bus', 'car', 'subway', 'train', 'airplane', 'boat', 'run', 'motorcycle', 'taxi') DEFAULT NULL,
            start_date_time DATETIME NOT NULL,
            end_date_time DATETIME NOT NULL,
            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
        ''',
        'Trackpoint': '''
            id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
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
            # make sure our logic works (would fail e.g. if some person IDs are missing)
            assert line.strip() == user_data[int(line)][0]
            user_data[int(line)][1] = True
    
    # print(*user_data, sep='\n')
    t1 = t()
    print(f'Inserting users. Time elapsed: {t1 - t0} seconds')
    connection.cursor.executemany('INSERT INTO User (id, has_labels) VALUES (%s, %s)', user_data)
    connection.commit()
    t2 = t()
    print(f'{len(user_data)} users inserted in {t2 - t1} seconds ({t2 - t1} total)')
    print('Processing activity and trackpoint data.')
    # read data about activities and trackpoints
    activity_data = []
    trackpoint_data = []
    activity_id = 1 # manually setting the activity IDs
    # this allows us to know the foreign keys without needing to insert activities one by one and checking the inserted IDs
    for [user_id, has_labels] in user_data:
        labels = {}
        if has_labels:
            with open(f'{data_directory}/{user_id}/labels.txt', 'r') as f:
                f.readline() # skip header
                for line in f:
                    start_date, start_time, end_date, end_time, transport_mode = line.strip().split()
                    # dates are slightly differently formatted in the labels-files than the .plt-files:
                    start_date = start_date.replace('/', '-')
                    end_date = end_date.replace('/', '-')
                    labels[f'{start_date} {start_time}'] = (f'{end_date} {end_time}', transport_mode)
        # each .plt file is a single activity
        _, _, activity_filenames = next(walk(f'{data_directory}/{user_id}/Trajectory'))
        for activity_filename in activity_filenames:
            with open(f'{data_directory}/{user_id}/Trajectory/{activity_filename}') as activity_file:
                # skip 6 first lines
                for _ in range(6):
                    activity_file.readline()
                lines = activity_file.readlines() # Doing a full read of the file might be risky
                # but I really don't hope or believe there is any one file too big to open in memory 
                if len(lines) >= 2500: # skip activities with more than 2500 trackpoints
                    continue
                _, _, _, _, _, start_date, start_time = lines[0].strip().split(',')
                _, _, _, _, _, end_date, end_time = lines[-1].strip().split(',')
                transportation_mode = None
                if f'{start_date} {start_time}' in labels: # if data is labeled
                    _end_date_and_time, mode = labels[f'{start_date} {start_time}']
                    if (f'{end_date}{end_time}' == lines[-1][-1]):
                    # assert end_date_and_time == f'{end_date} {end_time}', f'{end_date_and_time} is not {end_date} {end_time}' # Just making sure
                        transportation_mode = mode
                activity_data.append([activity_id, user_id, transportation_mode, f'{start_date} {start_time}', f'{end_date} {end_time}'])
                # activity data is dealt with - time to get trackpoint data
                for line in lines:
                    lat, lon, _, alt, date_days, date, time = line.strip().split(',')
                    trackpoint_data.append([activity_id, lat, lon, alt, date_days, f'{date} {time}'])
                activity_id += 1
    t3 = t()
    print(f'Processing activity and trackpoints took {t3 - t2} seconds. Total time elapsed: {t3 - t0} seconds')
    # Insert activities:
    print('Inserting activities...')
    connection.cursor.executemany('INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)', activity_data)
    connection.commit()
    t4 = t()
    print(f'{len(activity_data)} activities inserted in {t4 - t3} seconds. Total time elapsed: {t4 - t0}')

    # Insert trackpoints:
    print('Inserting trackpoints...')

    batch_size = 100000
    for i in range(0, len(trackpoint_data), batch_size):
        print(f'{(100*i/len(trackpoint_data)):.2f}%, {i} out of {len(trackpoint_data)} trackpoints inserted')
        connection.cursor.executemany('INSERT INTO Trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)', trackpoint_data[i:i+batch_size])
        connection.commit()
    t5 = t()
    print(f'{len(trackpoint_data)} trackpoints inserted in {t5 - t4} seconds. Total time elapsed: {t5 - t0} seconds')
    connection.cursor.execute('UPDATE Trackpoint SET altitude=NULL WHERE altitude=-777')
    connection.commit()
    connection.close_connection()

if __name__ == '__main__':
    main()