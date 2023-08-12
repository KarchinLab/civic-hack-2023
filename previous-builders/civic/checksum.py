from datetime import datetime
import os


def get_current_version_number():
    now = datetime.now()
    first = now.replace(day=1)
    return first.strftime('%Y.%m.%d')


def get_last_run_version_number():
    try:
        with open('latest_version.txt', 'r') as f:
            return f.readline()
    except IOError:
        return ''


def write_updated_last_version_number():
    with open('latest_version.txt', 'w+') as f:
        f.write(get_current_version_number())


latest_version = get_last_run_version_number()
current_version = get_current_version_number()
if latest_version is None or latest_version == current_version:
    print(f'Civic up to date with current month version: {current_version}')
else:
    write_updated_last_version_number()
    os.system('~/miniconda3/bin/python3 build_civic.py')
