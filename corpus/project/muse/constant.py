import os

TOTAL = 'log/total.txt'
SUCCESS = 'log/success.txt'
FAIL = 'log/fail.txt'


def read_file(path):
    if os.path.exists(path):
        with open(path) as f:
            return set([x.rstrip('\n') for x in f])
    return set()


def get_remaining():
    total = read_file(TOTAL)
    success = read_file(SUCCESS)
    remaining = sorted(list(total - success))
    return remaining

def get_all():
    total = read_file(TOTAL)
    return sorted(list(total))
