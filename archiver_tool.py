from datetime import datetime
from math import log10, ceil
from pytz import timezone
from argparse import ArgumentParser, FileType, ArgumentTypeError
import requests
import json
import asyncio
from functools import partial
import re
import logging
import warnings
from dataclasses import dataclass
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import time

@dataclass
class ArchiverAttributeDetails:
    id: str
    name: str
    table: str

@dataclass
class ArchiverData:
    name: str
    time: [str]
    data: [str]

    def get_taurus_format(self):
        output = []
        output.append(f'"# DATASET= {self.name}"')
        output.append('"# SNAPSHOT_TIME= "')

        for d in zip(self.time, self.data):
            timestamp = d[0].strftime("%Y-%m-%d_%H:%M:%S.%f")
            output.append(f'{timestamp} {d[1]}')

        return '\n'.join(output) + '\n'

    def write_taurus_file(self, fname):
        with open(fname, 'w') as f:
            f.write(self.get_taurus_format())

DB_TYPE = 'postgresql://hdb_viewer'
DB_USER = '2tQXXVJtax+QLj61tg1Zxg+AByTLTt526AHcM+XmVCVW'
DB_URL = 'timescaledb.maxiv.lu.se'
DB_PORT = '15432'
DB_NAMES = {
        'accelerator': 'hdb_machine',
        'machine': 'hdb_machine',
        'balder':  'hdb_balder',
        'biomax':  'hdb_biomax',
        'bloch':   'hdb_bloch',
        'cosaxs':  'hdb_cosaxs',
        'cry':     'hdb_cry',
        'danmax':  'hdb_danmax',
        'dummymax':'hdb_dummymax',
        'femtomax':'hdb_femtomax',
        'finest':  'hdb_finest',
        'flexpes': 'hdb_flexpes',
        'formax':  'hdb_formax',
        'gunlaser':'hdb_gunlaser',
        'hippie':  'hdb_hippie',
        'maxpeem': 'hdb_maxpeem',
        'micromax':'hdb_micromax',
        'nanomax': 'hdb_nanomax',
        'softimax':'hdb_softimax',
        'species': 'hdb_species',
        'veritas': 'hdb_veritas',
    }

def shell2sql(input_string):
    return input_string.replace('*', '%').replace('_', '.')

def get_ids_and_tables(searchstr, DB_CONN_STR):
    engine = create_engine(DB_CONN_STR)
    query = text(f"""
                 SELECT att_conf_id, att_name, table_name
                 FROM att_conf
                 WHERE att_name ~ '.*{searchstr}'
                 ORDER BY att_conf_id
            """)
    
    with Session(engine) as session:
        result = session.execute(query)
    
    return [ArchiverAttributeDetails(id=row[0], name=row[1], table=row[2]) for row in result]

def search_for_attributes(searchstr, database):
    DB_CONN_STR = f'{DB_TYPE}:{DB_USER}@{DB_URL}:{DB_PORT}/{DB_NAMES[database]}'

    details = get_ids_and_tables(searchstr, DB_CONN_STR)
    return [d.name for d in details]

def get_single_attr_data(attr, start, end, session):
    print(f"get_single_attr_data called: {time.time()}")
    query = text(f"""
                 SELECT * FROM {attr.table}
                 WHERE att_conf_id = {attr.id} AND data_time BETWEEN '{start}' AND '{end}'
                 ORDER BY data_time
            """)

    result = session.execute(query)

    return result

def get_data(searchstr, start, end, DB_CONN_STR):
    logger = logging.getLogger(__name__)
    start_naive = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
    end_naive = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
    start_cet = CET.localize(start_naive)
    end_cet = CET.localize(end_naive)
    start_utc = start_cet.astimezone(UTC)
    end_utc = end_cet.astimezone(UTC)

    attrs = get_ids_and_tables(searchstr, DB_CONN_STR)

    data = []
    
    engine = create_engine(DB_CONN_STR)
    with Session(engine) as session:
        for attr in attrs:
            logger.info('Getting data for "{}"'.format(attr.name))
            res = get_single_attr_data(attr, start_utc.isoformat(), end_utc.isoformat(), session)
            data_obj = ArchiverData(name=attr.name, time=[], data=[])
            for row in res:
                timestamp = row[1]
                data_obj.time.append(timestamp.astimezone(CET))
                data_obj.data.append(row[2])
            data.append(data_obj)

    return data

def main2():
    data = get_data('r3*dcct*current', '2022/11/27 10:00:00', '2022/11/27 10:20:00', DB_CONN_STR)
    fname_root = 'test_file'
    for n, d in enumerate(data):
        print(d.get_taurus_format())
        fname = f'{fname_root}_{n:03}.dat'
        d.write_taurus_file(fname)

BASEURL = 'https://hdbppviewer.maxiv.lu.se/'
SEARCHURL = BASEURL + 'search'
QUERYURL = BASEURL + 'query'
CONTROLURL_LIST = {
        'accelerator': "g-v-csdb-0.maxiv.lu.se:10000",
        'balder':      "b-v-balder-csdb-0.maxiv.lu.se:10000",
        'biomax':      "b-v-biomax-csdb-0.maxiv.lu.se:10000",
        'bloch':       "b-v-bloch-csdb-0.maxiv.lu.se:10000",
        'cosaxs':      "b-v-cosaxs-csdb-0.maxiv.lu.se:10000",
        'cry':         "b-v-cry-csdb-0.maxiv.lu.se:10000",
        'danmax':      "b-v-danmax-csdb-0.maxiv.lu.se:10000",
        'dummymax':    "b-v-dummymax-csdb-0.maxiv.lu.se:10000",
        'femtomax':    "b-v-femtomax-csdb-0.maxiv.lu.se:10000",
        'finest':      "b-v-finest-csdb-0.maxiv.lu.se:10000",
        'flexpes':     "b-v-flexpes-csdb-0.maxiv.lu.se:10000",
        'formax':      "b-v-formax-csdb-0.maxiv.lu.se:10000",
        'hippie':      "b-v-hippie-csdb-0.maxiv.lu.se:10000",
        'maxpeem':     "b-v-maxpeem-csdb-0.maxiv.lu.se:10000",
        'micromax':    "b-v-micromax-csdb-0.maxiv.lu.se:10000",
        'nanomax':     "b-v-nanomax-csdb-0.maxiv.lu.se:10000",
        'softimax':    "b-v-softimax-csdb-0.maxiv.lu.se:10000",
        'species':     "b-v-species-csdb-0.maxiv.lu.se:10000",
        'veritas':     "b-v-veritas-csdb-0.maxiv.lu.se:10000",
        'gunlaser':    "b-v0-gunlaser-csdb-0.maxiv.lu.se:10000",
        }
UTC = timezone('UTC')
CET = timezone('CET')

def makesearchpayload(searchterm, database):
    controlurl = CONTROLURL_LIST[database]
    return {
            'target': searchterm,
            'cs': controlurl,
            }

def makequerypayload(signal, start, end, interval, database):
    start_naive = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
    end_naive = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
    start_cet = CET.localize(start_naive)
    end_cet = CET.localize(end_naive)
    start_utc = start_cet.astimezone(UTC)
    end_utc = end_cet.astimezone(UTC)
    controlurl = CONTROLURL_LIST[database]
    return {
            'targets': [{'target': signal, 'cs': controlurl,}],
            'range': {
                'from': start_utc.isoformat(),
                'to': end_utc.isoformat(),
                },
            'interval': interval
            }

def parse_response(resp, database):
    controlurl = CONTROLURL_LIST[database]
    if not resp.status_code == 200:
        data = {
                'target': resp.text,
                'datapoints': [],
                }
    else:
        data = json.loads(resp.text)[0]
    output = []
    target_str = data['target'].replace(controlurl+'/', controlurl+'//')
    datetime_str = datetime.isoformat(datetime.now(), sep=':')
    output.append('"# DATASET= tango://' + target_str + '"')
    output.append('"# SNAPSHOT_TIME= ' + datetime_str + '"')
    for vals in data['datapoints']:
        dt = datetime.fromtimestamp(vals[1] / 1000) #, tz=CET)
        timestamp = dt.strftime("%Y-%m-%d_%H:%M:%S.%f")
        output.append('{} {}'.format(timestamp, vals[0]))
    return '\n'.join(output) + '\n'

def get_attributes(search_strs, database):
    logger = logging.getLogger(__name__)
    if isinstance(search_strs, str):
        search_strs = [search_strs]
    attributes = []
    for sig in search_strs:
        logger.info('Getting matching attribute names for "{}"'.format(sig))
        search_payload = makesearchpayload(sig, database)
        logger.info('Posting {} to {}'.format(search_payload, SEARCHURL))
        try:
            search_resp = requests.post(SEARCHURL, json=search_payload)
        except requests.exceptions.ConnectionError:
            err_str = '''
            Cannot reach {}. Make sure you are inside the MAX-IV firewall.
            '''
            raise ValueError(err_str.format(SEARCHURL))
        attributes += json.loads(search_resp.text)
    logger.info('Found the following attributes: {}'.format(attributes))
    return attributes

async def do_request(start, end, signals, interval, database):
    logger = logging.getLogger(__name__)
    loop = asyncio.get_event_loop()
    futures, responses = [], []
    for sig in signals:
        payload = makequerypayload(sig, start, end, interval, database)
        logger.info('Submitting: {}'.format(payload))
        futures.append(loop.run_in_executor(
                None,
                partial(requests.post, url=QUERYURL, json=payload),
                ))
    task_count = len(futures)
    for i, fut in enumerate(futures):
        logger.info(
                'Waiting for query {} of {} to complete'.format(
                    i+1,
                    task_count
                    )
                )
        resp = await fut
        responses.append(resp)
        logger.info('Query {} of {} completed'.format(i+1, task_count))
    return responses

def sync_do_request(start, end, signals, interval):
    responses = []
    for sig in signals:
        payload = makequerypayload(sig, start, end, interval)
        resp = requests.post(url=QUERYURL, json=payload)
        responses.append(resp)
    return responses

def query(start, end, signals, interval='0.1s'):
    attrs = get_attributes(signals)
    if len(attrs) == 0:
        raise ValueError('No attribute matching', signals)
    if not len(attrs) == 1:
        raise ValueError('Multiple attributes matched', attrs)
    responses = sync_do_request(start, end, attrs, interval)
    a = [parse_response(resp) for resp in responses]
    datastr = a[0]
    data, timestamp = [], []
    for line in datastr.split('\n')[2:]:
        split = line.split()
        if len(split) < 2:
            break
        data.append(float(split[1]))
        timestamp.append(datetime.strptime(split[0], "%Y-%m-%d_%H:%M:%S.%f"))
    return (timestamp, data)


def main():
    def interval_value(val):
        return val

    parser = ArgumentParser(
            description='Get data from HDB++ archiver',
            epilog='''
            When specifying signals, note that the wildcard
            character, '*', will not work as in a POSIX
            shell, but will be interpreted as part of the regex.  Where
            you would use '*' at a POSIX shell, you probably want '.*'.
            On ZSH, the '.*' will give an error -- zsh: no matches found.
            This is due to old globbing rules in that shell, and you need
            to escape the wildcard character to make it work -- '.\*'

            Example: archiver-tool --start 2022-09-13T13:00:00 --end 2022-09-13T13:10:00 --database accelerator r3.*dcct.*inst.*
            ''')
    parser.add_argument(
            'signal', type=str, nargs='+',
            help='''
            Signal(s) to acquire. These are all interpreted as regex's
            beginning and ending with '.*'.
            ''',
           )
    parser.add_argument(
            '-d', '--database', default='accelerator', type=str,
            help='''
            The database you want to query. Typically this will be
            "accelerator" or the name of your beamline.
            '''
            )
    parser.add_argument(
            '-f', '--file', type=str,
            help='''
            Root name of file(s) in which to save the data. In the case of
            aquisition of a single attribute, a single file will be created
            with the name FILE.dat. In the case of multiple attribute
            aquisition, each attribute will have the name FILE001.dat,
            FILE002.dat, etc.
            If the file(s) already exist(s), it/they will be overwritten, so
            use with care. Use of this option suppresses standard output.
            '''
            )
    parser.add_argument(
            '-i', '--interval', type=interval_value, default='0.1s',
            help='''
            Force a sampling interval for the data. By default this will be
            0.1s; i.e., as dense as possible.
            This should be written in the form of a number and a time-unit;
            e.g., "1s" to sample every second, "2m" to sample every two
            minutes, "1h" to sample every hour, etc.
            '''
            )
    parser.add_argument(
            '-v', '--verbose', action='store_true',
            help='Verbose output'
            )
    parser.add_argument(
            '--timescale', action='store_true',
            help='''Use the timescale database. This is the default behaviour. This flag
            is only here to allow for backwards compatibility'''
            )
    parser.add_argument(
            '--old_db', action='store_true',
            help='Use the old database instead of timescale'
            )
    required = parser.add_argument_group('required arguments')
    required.add_argument(
            '-s', '--start', type=str, required=True,
            help='Start of time-range',
            )
    required.add_argument(
            '-e', '--end', type=str, required=True,
            help='End of time-range'
            )

    args = parser.parse_args()
    verbose = args.verbose

    database = args.database.lower()
    # CONTROLURL = CONTROLURL_LIST[args.database.lower()]

    DB_CONN_STR = f'{DB_TYPE}:{DB_USER}@{DB_URL}:{DB_PORT}/{DB_NAMES[database]}'

    if args.timescale:
        warnings.warn("This library uses the timescale DB by default, and so the '--timescale' flag is redundant")

    logger = logging.getLogger(__name__)
    if verbose:
        logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - - %(levelname)s - %(message)s'
                )
    else:
        logging.basicConfig(
                format='%(asctime)s - - %(levelname)s - %(message)s'
                )

    logger.info(args.signal)

    if args.old_db:
        loop = asyncio.get_event_loop()

        start, end, interval = args.start, args.end, args.interval

        attributes = get_attributes(args.signal, database)
        response = loop.run_until_complete(
                do_request(start, end, attributes, interval, database)
                )
        if args.file:
            numfiles = len(attributes)
            numdigits = ceil(log10(numfiles + 1))
            for i, resp in enumerate(response):
                filename = args.file + str(i+1).zfill(numdigits) + '.dat'
                logger.info('Writing to {}'.format(filename))
                with open(filename, 'w') as f:
                    f.write(parse_response(resp, database))
        else:
            for resp in response:
                print(parse_response(resp, database))
    else:
        if args.file:
            filecounter = 0
            numfiles = 1000
            numdigits = ceil(log10(numfiles + 1))
            for sigs in args.signal:
                logger.info('Querying DB for {}'.format(sigs))
                data = get_data(sigs, args.start, args.end, DB_CONN_STR)
                for d in data:
                    logger.info('Writing data for {}'.format(d.name))
                    filecounter += 1
                    filename = args.file + str(filecounter).zfill(numdigits) + '.dat'
                    d.write_taurus_file(filename)
        else:
            for sigs in args.signal:
                logger.info('Querying DB for {}'.format(sigs))
                data = get_data(sigs, args.start, args.end, DB_CONN_STR)
                for n, d in enumerate(data):
                    print(d.get_taurus_format())


if __name__ == "__main__":
    main()
