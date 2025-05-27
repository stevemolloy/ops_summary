from urllib.request import urlopen
import json
from datetime import datetime

URL = "https://downtime.maxiv.lu.se/"

def get_stats_from_webapp(url):
    with urlopen(url) as fp:
        rawdata = fp.read()

    rawstr = rawdata.decode("utf8")

    downtimestr = '[{' + rawstr.split('downtimeevents')[1].split('[{')[1].split('}]')[0] + '}]'
    downtime_data = [i for i in json.loads(downtimestr) if not i['archived']]
    for n, dat in enumerate(downtime_data):
        downtime_data[n]['date'] = datetime.strptime(dat['date']+'='+dat['time'], '%Y-%m-%d=%H:%M')

    deliverystr = '[{' + rawstr.split('deliveryplans')[1].split('[{')[1].split('}]')[0] + '}]'
    delivery_data = json.loads(deliverystr)
    for n, dat in enumerate(delivery_data):
        delivery_data[n]['date'] = datetime.strptime(dat['date'], '%Y-%m-%d')
        
    return delivery_data, downtime_data

delivery_data, downtime_data = get_stats_from_webapp(URL)

def get_availability(start_date, end_date, accelerator):
    del_hours = get_del_hours(start_date, end_date, accelerator)
    if del_hours==0:
        return None
    downtime_hours = get_downtime_hours(start_date, end_date, accelerator)
    return 1 - downtime_hours/del_hours

def get_mtbf(start_date, end_date, accelerator):
    del_hours = get_del_hours(start_date, end_date, accelerator)
    if del_hours==0:
        return None
    N = get_downtime_count(start_date, end_date, accelerator)
    return del_hours / (N+1)

def get_mttr(start_date, end_date, accelerator):
    N = get_downtime_count(start_date, end_date, accelerator)
    if N==0:
        return None
    downtime_hours = get_downtime_hours(start_date, end_date, accelerator)
    return downtime_hours / N
    
def get_del_hours(start_date, end_date, accelerator):
    plans = [
        dat[accelerator+'plan'] 
        for dat in delivery_data
        if dat['date'] > start_date and dat['date'] <= end_date
    ]
    return sum(plans)

def get_downtime_minutes(start_date, end_date, accelerator):
    total = 0
    for dat in downtime_data:
        if dat['date'] > start_date and dat['date'] <= end_date and dat['machine']==accelerator:
            total += dat['duration']
    return total

def get_downtime_hours(start_date, end_date, accelerator):
    return get_downtime_minutes(start_date, end_date, accelerator) / 60

def get_downtime_count(start_date, end_date, accelerator):
    total = 0
    for dat in downtime_data:
        if dat['date'] > start_date and dat['date'] <= end_date and dat['machine']==accelerator:
            total += 1
    return total

