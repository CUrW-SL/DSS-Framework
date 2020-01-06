import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from gen_db import CurwFcstAdapter, CurwObsAdapter, CurwSimAdapter


def update_current_rainfall_values(variable_routine, locations):
    print('')
