#!/usr/bin/env python3
import sqlite3
import psycopg2
from datetime import datetime, timezone, timedelta
from sys import argv
from csv import writer
from re import findall


def read_log_file(log_file_pathname):
    """
    reads and return all the bytes from the log file
    @param log_file_pathname : the pathname of a FarCry server log file
    @return all the bytes from the log file
    """
    try:
        with open(log_file_pathname, 'r') as log_data:
            return log_data.read()
    except (PermissionError, FileNotFoundError, OSError):
        raise Exception('Invalid file')


def parse_log_start_time(log_data):
    """
    get a object datetime representing time the FarCry engine began to log event
    @param log_data : the data read from a far Cry server's log file
    @return a object datetime.datetime
    """
    # format of first line in log file
    format_log_date = 'Log Started at %A, %B %d, %Y %X'
    # get time zone in log file
    tz = log_data.partition('g_timezone,')[-1].partition(')')[0]
    obj_datetime = datetime.strptime(log_data.partition('\n')[0], format_log_date)
    # change timezone of object datetime
    obj_datetime = obj_datetime.replace(tzinfo = timezone(timedelta(hours=int(tz))))
    return obj_datetime


def parse_log_time(obj_log_time, time_string):
    """
    convert time ingame become object datetime
    @param obj_log_time : object datetime representing time the FarCry engine began to log event
    @param time_string : time ingame when have a event (a player kill or be kill by another player) (format : 'MM:SS')
    @return object datetime representing time
    """
    delta_second = int(time_string[3:]) - obj_log_time.second
    delta_minute = int(time_string[:2]) - obj_log_time.minute
    # change current hours if minute over 59
    if delta_minute < 0:
        delta_hour = 1
    else:
        delta_hour = 0
    obj_datetime = obj_log_time + timedelta(hours=delta_hour,
                                            minutes=delta_minute,
                                            seconds=delta_second)
    return obj_datetime


def parse_match_game_mode_and_map_name(log_data):
    """
    take game mode and map name in log file
    @param log_data : the data read from a far Cry server's log file
    @return a tuple (game_mode, map_name)
    """
    temporary = log_data.partition('Loading level Levels/')[-1].partition(' -')[0]
    temporary = temporary.split()
    return (temporary[-1], temporary[0][:-1])


def parse_frags(log_data, obj_datetime):
    """
    take a list contain all event when a player kill or be kill by another player
    the list include (datetime.datetime(), killer name, victim name, weapon name) or
                     (datetime.datetime(), killer name) if the player suicide
    @param log_data : the data read from a far Cry server's log file
    @param obj_datetime : object datetime representing time the FarCry engine began to log event
    @return a list of frags
    """
    output = []
    for line in log_data.split('\n'):
        if 'killed' in line:
            temp_list = list(findall('<([0-5]\\d):([0-5]\\d)> <\w+> ([\w ]+) killed (?:itself|([\\w ]+) with (\\w+))', line)[0])
            new_datetime = parse_log_time(obj_datetime, ' '.join(temp_list[0:2]))
            if temp_list[-1]: # when the player kill another player
                output.append(tuple([new_datetime] + temp_list[2:]))
            else: # when the player kill itself
                output.append(tuple([new_datetime] + temp_list[2:-2]))
    return output


def prettify_frags(frags):
    """
    take a list with emoji
    @param frags : a list of frags
    @return a list strings, each with follwing format
    """
    def take_icon(weapon):
        for key, value in icon_dict.items():
            if weapon in value:
                return key
        raise ValueError('Do not {} in dataset'.format(weapon))

    icon_dict = {
        '🔫' : ['Falcon', 'Shotgun', 'P90', 'MP5', 'M4', 'AG36', 'OICW', 'SniperRifle',
                'M249', 'MG', 'VehicleMountedAutoMG', 'VehicleMountedMG'],
        '💣' : ['HandGrenade', 'AG36Grenade', 'OICWGrenade', 'StickyExplosive'],
        '🚙' : ['Vehicle'],
        '🚀' : ['Rocket', 'VehicleMountedRocketMG', 'VehicleRocket'],
        '🔪' : ['Machete'],
        '🚤' : ['Boat']
    }

    prettified_frags = []
    for ele in frags:
        if len(ele) == 4:
            icon = take_icon(ele[3])
            prettified_frags.append('[{}] 😛 {} {} 😦 {}'.format(str(ele[0]), ele[1], icon, ele[2]))
        else:
            prettified_frags.append('[{}] 😦 {} ☠'.format(str(ele[0]), ele[1]))
    return prettified_frags


def parse_game_session_start_and_end_times(log_data, map_name, obj_datetime):
    """
    get time start and time end of session
    @param log_data : the data read from a far Cry server's log file
    @param map_name : map name
    @param obj_datetime : object datetime representing time the FarCry engine began to log event
    @return the approximate start and end time of the game session
    """
    start_time = log_data.partition('  Level ' + map_name + ' loaded in ')[0][-6:-1]
    end_time = log_data.partition('Statistics')
    if not end_time[1]:
        raise ValueError('error: stack overflow')
    end_time = end_time[0][-10:-5]

    start_time = parse_log_time(obj_datetime, start_time)
    end_time = parse_log_time(obj_datetime, end_time)
    return start_time, end_time


def write_frag_csv_file(file_csv, frags):
    """
    write list frags become file csv
    @param file_csv : file csv name
    @param frags : a list of frags
    @return None
    """
    with open(file_csv, 'w') as myfile:
        wr = writer(myfile, lineterminator='\n')
        wr.writerows(frags)


def insert_match_to_sqlite(file_pathname, start_time, end_time, game_mode, map_name, frags):
    """
    insert data into sqlite database in match table
    @param file_pathname : the path and anme of th Far Cry's SQLite database
    @param start_time : a datettime.datetime object with time zone
                        information corresponding to the start of the game session
    @param end_time : a datettime.datetime object with time zone
                      information corresponding to the end of the game session
    @param game_mode : multiplayer mode of the game session
    @param map_name : name of the map that was played
    @param frags : a list of tuple of following
    @return last id of database
    """
    def insert_frags_to_sqlite(connection, match_id):
        """
        insert data into sqlite database in match_frag table
        @param connection : a sqlite Connection object
        @param match_id : current id
        """
        conn_frags = connection.cursor()
        for frag in frags:
            if len(frag) == 4:
                conn_frags.execute('INSERT INTO match_frag\
                                    (match_id, frag_time, killer_name, victim_name, weapon_code)\
                                    VALUES\
                                    (?, ?, ?, ?, ?)',(match_id, *frag))
            else:
                conn_frags.execute('INSERT INTO match_frag\
                                    (match_id, frag_time, killer_name)\
                                    VALUES\
                                    (?, ?, ?)',(match_id, *frag))
        conn_frags.close()        

    try:
        conn_db = sqlite3.connect(file_pathname)
        conn_match = conn_db.cursor()
        conn_match.execute('INSERT INTO match\
                (start_time, end_time, game_mode, map_name)\
                VALUES\
                (?, ?, ?, ?)', (start_time, end_time, game_mode, map_name))
        last_id = conn_match.lastrowid
        insert_frags_to_sqlite(conn_db, last_id)
        conn_db.commit()
    except (Exception, psycopg2.Error) as error:
        raise Exception("Error while connecting to PostgresSQL", error)
    finally:
        if conn_db:
            conn_match.close()
            conn_db.close()
    return last_id


def insert_match_to_postgresql(properties, start_time, end_time, game_mode, map_name, frags):
    """
    insert data into postgressql database in match table
    @param properties : a tuple contain user, password, database, host
                        information corresponding to the start of the game session
    @param end_time : a datettime.datetime object with time zone
                      information corresponding to the end of the game session
    @param game_mode : multiplayer mode of the game session
    @param map_name : name of the map that was played
    @param frags : a list of tuple of following
    @return last id of database
    """
    def insert_frags_to_postgresql():
        """
        insert data into sqlite database in match_frag table
        """
        cursor_frags = connection.cursor()
        for frag in frags:
            if len(frag) == 4:
                cursor_frags.execute('INSERT INTO match_frag\
                                    (match_id, frag_time, killer_name, victim_name, weapon_code)\
                                    VALUES\
                                    (%s, %s, %s, %s, %s);',(last_uuid, *frag))
            else:
                cursor_frags.execute('INSERT INTO match_frag\
                                    (match_id, frag_time, killer_name)\
                                    VALUES\
                                    (%s, %s, %s);',(last_uuid, *frag))
        cursor_frags.close()

    try:
        connection = psycopg2.connect(host=properties[0],
                                      dbname=properties[1],
                                      user=properties[2],
                                      password=properties[3])
        cursor = connection.cursor()
        cursor.execute("INSERT INTO match (start_time, end_time, game_mode, map_name)\
                        VALUES (%s, %s, %s, %s)\
                        RETURNING match_id;", (start_time, end_time, game_mode, map_name))
        last_uuid = cursor.fetchone()[0]
        insert_frags_to_postgresql()
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        raise Exception("Error while connecting to PostgresSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
    return last_uuid


def take_all_player_in_frags(frags, index):
    output = set()
    for frag in frags:
        if len(frag) == 2:
            continue
        output.update([frag[index]])
    return output


def take_list_player(frags, column1, column2, player):
    list_longest = []
    list_current = []
    for frag in frags:
        if len(frag) == 2:
            continue
        if frag[column2] == player:
            if len(list_current) > len(list_longest):
                list_longest = list_current.copy()
            list_current.clear()
        elif frag[column1] == player:
            frag = list(frag)
            if column1 < column2:
                list_current.append(frag[0:1]+frag[2:])
            else:
                list_current.append(frag[0:2]+frag[-1:])
    return list_longest


def calculate_serial(frags, column1, column2):
    serier_kill = {}
    for player in take_all_player_in_frags(frags, column1):
        serier_kill[player] = take_list_player(frags, column1, column2, player)
    return serier_kill


def calculate_serial_killer(frags):
    return calculate_serial(frags, 1, 2)


def calculate_serial_loser(frags):
    return calculate_serial(frags, 2, 1)


def main():
    # read data from log file
    log_data = read_log_file(argv[1])
    # get time began write log file
    log_start_time = parse_log_start_time(log_data)
    # get game mode and map name
    game_mode, map_name = parse_match_game_mode_and_map_name(log_data)
    # get event shooting in game (a player kill or be kill by another player)
    frags = parse_frags(log_data, log_start_time)
    # get start time and end time of a match
    start_time, end_time = parse_game_session_start_and_end_times(log_data, map_name, log_start_time)
    
    serial_killers = calculate_serial_killer(frags)
    for player_name, kill_serier in serial_killers.items():
        print('[%s]' % player_name)
        print('\n'.join([', '.join(([str(e) for e in kill])) for kill in kill_serier]))
    
    serial_killers = calculate_serial_loser(frags)
    for player_name, kill_serier in serial_killers.items():
        print('[%s]' % player_name)
        print('\n'.join([', '.join(([str(e) for e in kill])) for kill in kill_serier]))

    # insert data into sqlite database
    insert_match_to_sqlite('farcry.db', start_time, end_time, game_mode, map_name, frags)
    # insert data into postgresql database
    properties = ('localhost', 'farcry', 'postgres', '1')
    insert_match_to_postgresql(properties, start_time, end_time, game_mode, map_name, frags)


if __name__ == "__main__":
    # try:
    #     main()
    # except Exception as error:
    #     print('ERROR : ', error)
    main()