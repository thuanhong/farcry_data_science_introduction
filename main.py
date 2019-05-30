#!/usr/bin/env python3
from datetime import datetime, timezone, timedelta
from sys import argv
from re import findall
from json import dumps


def read_log_file(log_file_pathname):
    try:
        with open(log_file_pathname, 'r') as log_data:
            return log_data.read()
    except (PermissionError, FileNotFoundError, OSError):
        print('Invalid file')


def parse_log_start_time(log_data):
    format_log_date = 'Log Started at %A, %B %d, %Y %X'
    tz = log_data.partition('g_timezone,')[-1].partition(')')[0]
    obj_datetime = datetime.strptime(log_data.partition('\n')[0], format_log_date)
    obj_datetime = obj_datetime.replace(tzinfo = timezone(timedelta(hours=int(tz))))
    return obj_datetime


def parse_log_time(obj_log_time, time_string):

    delta_second = int(time_string[3:]) - obj_log_time.second
    delta_minute = int(time_string[:2]) - obj_log_time.minute

    if delta_minute < 0:
        delta_hour = 1
    else:
        delta_hour = 0
    obj_datetime = obj_log_time + timedelta(hours=delta_hour,
                                            minutes=delta_minute,
                                            seconds=delta_second)
    return obj_datetime


def parse_match_mode_and_map(log_data):
    temporary = log_data.partition('Loading level Levels/')[-1].partition(' -')[0]
    temporary = temporary.split()
    return (temporary[-1], temporary[0][:-1])


def parse_frags(log_data):
    obj_datetime = parse_log_start_time(log_data)
    output = []
    for line in log_data.split('\n'):
        if 'killed' in line:
            line = line.split()
            new_datetime = parse_log_time(obj_datetime, line[0][1:-1])
            if len(line) == 7:
                output.append((new_datetime, line[2], line[4], line[6]))
            else:
                output.append((new_datetime, line[2], line[4]))
    return output


def prettify_frags(frags):
    def take_icon(weapon):
        for key, value in icon_dict.items():
            if weapon in value:
                return key
        raise ValueError('Do not weapon in dataset')

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


def parse_game_session_start_and_end_times(log_data):
    mode_map = parse_match_mode_and_map(log_data)

    start_time = log_data.partition('  Level ' + mode_map[1] + ' loaded in ')[0][-6:-1]
    end_time = log_data.partition('Statistics')
    if not end_time[1]:
        print('error: stack overflow')
        exit(1)
    end_time = end_time[0][-10:-5]

    obj_datetime = parse_log_start_time(log_data)

    start_time = parse_log_time(obj_datetime, start_time)
    end_time = parse_log_time(obj_datetime, end_time)


def main():
    log_data = read_log_file(argv[1])
    # frags = parse_frags(log_data)
    # obj_datetime = parse_log_start_time(log_data)
    parse_game_session_start_and_end_times(log_data)
    # print(type(parse_log_start_time(log_data).hour))


if __name__ == "__main__":
    main()