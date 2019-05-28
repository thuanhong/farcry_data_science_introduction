#!/usr/bin/env python3
from datetime import datetime
from sys import argv


def read_log_file(log_file_pathname):
    try:
        with open(log_file_pathname, 'r') as log_data:
            return log_data.read()
    except (PermissionError, FileNotFoundError, OSError):
        print('Invalid file')


def parse_log_start_time(log_data):
    format_log_date = 'Log Started at %A, %B %d, %Y %X'
    return datetime.strptime(log_data.partition('\n')[0], format_log_date)


def main():
    log_data = read_log_file(argv[1])
    print(parse_log_start_time(log_data).tzinfo)


if __name__ == "__main__":
    main()    