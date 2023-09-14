"""
session_count.py - A Python script to parse an EaaSI session log CSV file and show the session count over time
Optionally specify <session_csv_path> <user_json_path> <session_count_csv_path> as arguments, otherwise the
latest session and user files in the current directory will be used for input, and a corresponding
session_count_<datetime>.csv file will be created for output.
"""
import csv
import json
import sys
from datetime import datetime
from glob import glob
from typing import List, Dict, Tuple

CSV_HEADERS = ['start_timestamp', 'end_timestamp', 'user_id', 'environment_id', 'object_id']


def get_user_dict(user_json_path: str) -> Dict[str, str]:
    with open(user_json_path, newline='') as json_file:
        user_info_list = json.load(json_file)

    # print(user_info)

    print(f'There are {len(user_info_list)} users defined in {user_json_path}')
    return {user_info['id']: user_info['username']
            for user_info in user_info_list
            }


def get_event_list(session_csv_path: str, user_dict: Dict[str, str]) -> List[Tuple[datetime, int, str]]:
    event_list = []
    with open(session_csv_path, newline='') as csv_file:
        session_reader = csv.reader(csv_file, delimiter=',', quotechar='|')

        for row_list in session_reader:
            row_dict = dict(zip(CSV_HEADERS, row_list))
            row_dict['start_timestamp'] = datetime.fromisoformat(row_dict['start_timestamp'].split('+')[0])
            row_dict['end_timestamp'] = datetime.fromisoformat(row_dict['end_timestamp'].split('+')[0])

            # print(row_dict)

            # Ignore records with stupid timestamps
            if row_dict['start_timestamp'].year < 2022 or row_dict['end_timestamp'].year < 2022:
                continue

            # Just use user_id if we can't look up user_name
            user_name = user_dict.get(row_dict['user_id'], row_dict['user_id'])

            event_list.append((row_dict['start_timestamp'], 1, user_name))  # Session start event
            event_list.append((row_dict['end_timestamp'], -1, user_name))  # Session end event

    print(f'There are {len(event_list)} session start/end events read from {session_csv_path}')

    return sorted(event_list)  # Sort by ascending event time


def write_session_count_csv_file(event_list: List[Tuple[datetime, int, str]], session_count_csv_path: str) -> None:
    session_count = 0
    max_session_count = 0
    current_users = {}
    with open(session_count_csv_path, 'w') as session_count_file:
        session_count_file.write('datetime,sessions,users\n')
        for event in event_list:
            timestamp = event[0]
            session_count = session_count + event[1]

            if event[1] == 1:  # Session start event
                current_users[event[2]] = current_users.get(event[2], 0) + 1
            if event[1] == -1:  # Session end event
                current_users[event[2]] = current_users[event[2]] - 1
                if not current_users[event[2]]:
                    del current_users[event[2]]

            current_user_string = ' '.join(sorted([f'{key}={value}' for key, value in current_users.items()]))

            # print(timestamp, session_count, current_user_string)
            session_count_file.write(f'{timestamp},{session_count},{current_user_string}\n')

            max_session_count = max(max_session_count, session_count)

    print(f'There was a maximum of {max_session_count} concurrent sessions between {event_list[0][0]} '
          f'and {event_list[-1][0]}')

    print(f'Session information written to {session_count_csv_path}')


def main():
    if len(sys.argv) >= 2:
        session_csv_path = sys.argv[1]
    else:
        session_csv_path = max(glob('sessions_*.csv'))  # Latest session file in current directory

    if len(sys.argv) >= 3:
        user_json_path = sys.argv[2]
    else:
        user_json_path = max(glob('users_*.json'))  # Latest user file in current directory

    if len(sys.argv) >= 4:
        session_count_csv_path = sys.argv[3]
    else:
        session_count_csv_path = session_csv_path.replace('sessions_', 'session_count_')

    user_dict = get_user_dict(user_json_path)

    event_list = get_event_list(session_csv_path, user_dict)
    # print(event_list)

    write_session_count_csv_file(event_list, session_count_csv_path)


if __name__ == '__main__':
    main()
