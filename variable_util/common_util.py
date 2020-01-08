import croniter
from datetime import datetime, timedelta


def get_iteration_gap_of_cron_exp(cron_exp):
    cron = croniter.croniter(cron_exp, datetime.now())
    date1 = cron.get_next(datetime)
    date2 = cron.get_next(datetime)
    difference = (date2 - date1).total_seconds()
    d = divmod(difference, 86400)  # days
    h = divmod(d[1], 3600)  # hours
    m = divmod(h[1], 60)  # minutes
    s = m[1]  # seconds
    print([d[0], h[0], m[0], s])
    return {'days': d[0], 'hours': h[0], 'minutes': m[0], 'seconds': s}


# search in list of dictionaries
def search_in_dictionary_list(input_list, key, value):
    for row in input_list:
        if row[key] == value:
            return row
    return None


def lower_time_limit(current_time, cron_exp):
    time_gap = get_iteration_gap_of_cron_exp(cron_exp)
    lower_limit = current_time - timedelta(days=time_gap['days'],
                                           hours=time_gap['hours'],
                                           minutes=time_gap['minutes'],
                                           seconds=time_gap['seconds'])
    print('lower_time_limit|lower_limit : ', lower_limit)
    return lower_limit
