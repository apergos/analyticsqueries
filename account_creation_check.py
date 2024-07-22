#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
for a given wiki, and a given user id interval, get
user id, name, registration fields for that wiki; 
then for a second user id interval, get the same
fields for the loginwiki; find all user names in the
source wiki not on the loginwiki and record those

after that, each of those user names should I guess
be checked to see if they exist on loginwiki at all,
it might be one out of 10 that has this issue, and
that's a lot, hard to know

100k or so entries is about a month's worth of entries
on en wiki, 200k on loginwiki ought to get that, and
there we are.
"""

import getopt
import glob
import os.path
import sys
import time
from getpass import getpass
import configparser
# WMF production wants the first, fedora uses the second
try:
    import mariadb as MySQLdb
except ImportError:
    pass
try:
    import MySQLdb
except ImportError:
    pass

KNOWN_ACTIONS = ['source', 'login', 'compare', 'gone', 'global']
# about 30 days worth of new user accounts for enwiki
DEFAULT_UID_INTERVAL = 100

class QueryRunner():
    '''
    munge and run queries on db servers for specific wikis
    '''
    def __init__(self, dbconn, args):
        if not dbconn:
            raise ValueError("first arg dbconn cannot be None")
        self.dbconn = dbconn
        self.cursor = self.dbconn.conn.cursor()
        self.args = args

    def run_query(self, query, sleep=0):
        '''
        run a sql query, return all rows
        '''
        result = None
        if sleep:
            time.sleep(sleep)
        try:
            if self.args['dryrun'] or self.args['verbose']:
                print(f"query to be run on {self.dbconn.hostname} is", query)
            if self.args['dryrun']:
                return None
            self.cursor.execute(query.encode('utf-8'))
            result = self.cursor.fetchall()
        except MySQLdb.Error as ex:
            raise MySQLdb.Error(
                "exception running query on host "
                f"{self.dbconn.hostname}, wiki {self.dbconn.wikidb} ({ex.args[0]}:{ex.args[1]})")
        return result

    def run_simple_query(self, query):
        '''
        run query and return the output as a string
        this should be run only where the request is for one field's value from a single row
        '''
        rows = self.run_query(query)
        if self.args['dryrun']:
            return None

        if len(rows) > 1:
            raise RuntimeError(f"simple query expects one row back but got {len(rows)}")

        return rows[0]

    def init_conn(self):
        '''
        set the database for queries
        '''
        self.cursor.execute(f"use {self.dbconn.wikidb}")

    def get_max_uid(self):
        '''
        for a given wiki, return the max uid
        '''
        result = self.run_simple_query('select max(user_id) from user;')
        if self.args['dryrun']:
            # make something up
            return 20
        if not result:
            raise RuntimeError('Failed to get max user id from db')
        return int(result[0])

    def get_uid_range(self, interval, end_uid):
        '''
        for a given wiki, given an end uid and an interval,
        return start and end uids covering at most interval entries
        if -1 is specified, end_uid returned will be the largest uid in the database
        if the interval size is larger than end_uid (or the largest uid in the db),
        start_uid returned will be 1
        '''
        if end_uid == -1:
            end_uid = self.get_max_uid()
        start_uid = end_uid - interval
        start_uid = max(start_uid, 1)
        return { 'start': start_uid, 'end': end_uid }

    def get_user_info(self, uids, outputpath):
        '''
        get id, name and registration date for the users in the specified
        uid range, write them to an output file, one row per line
        '''
        query = "SELECT user_id, user_registration, user_name FROM user "
        where = f"WHERE user_id >= {uids['start']} AND user_id < {uids['end']} "
        order = "ORDER BY user_id DESC;"
        rows = self.run_query(query + where + order)
        if self.args['dryrun']:
            return
        with open(outputpath, "a", encoding="utf-8") as output:
            for row in rows:
                if self.args['dryrun'] or self.args['verbose']:
                    print("row:", row)
                uid = row[0]
                registration = row[1]
                if not registration:
                    registration = 'NULL'
                else:
                    registration = registration.decode('utf-8')
                name = row[2].decode('utf-8')
                output.write(f"{uid}, {registration}, {name}\n")
            output.close()

    def get_user_batches(self, uids, outputpath, count):
        '''
        get user info in batches of count size
        '''
        # create it once; the batch writes will append.
        with open(outputpath, "w", encoding="utf-8") as output:
            output.close()

        start = int(uids['start'])
        end = start + count
        if end > int(uids['end']):
            end = int(uids['end']) + 1
        while start < end:
            self.get_user_info({'start': start, 'end': end}, outputpath)
            if end > uids['end']:
                break
            start += count
            end += count
            if end > uids['end']:
                end = uids['end'] + 1

    def prep_name_for_query(self, field):
        '''
        do the minimum we can to make user name
        usable in an sql query; backslashes are still no go though
        '''
        if "'" in field:
            field = field.replace("'", "''")
        return "'" + field + "'"

    def check_missing_uids(self, uids_file, outputpath):
        '''
        check the uids in the missing uids file, in batches, to see if any
        of them are in the loginwiki user table, because they might have
        been autocreated earlier from some other wiki than the one we used
        as the source wiki
        '''
        # create it once; the batch writes will append.
        with open(outputpath, "w", encoding="utf-8") as output:
            output.close()

        with open(uids_file, "r", encoding="utf-8") as uid_input:
            content = uid_input.read()
            uid_input.close()
            entries = content.splitlines()
            # format:  uid registr_date name
            rows = [entry.split(' ',2) for entry in entries]
            # names = [row[2] for row in rows]

        select = "SELECT user_name FROM user WHERE user_name IN "
        last = len(rows)
        for i in range(0,last,50):
            end = min(i + 50, last)
            # skip names with '\' but we can manually check these later
            where = ",".join([self.prep_name_for_query(row[2]) for row in rows[i:end]
                              if "\\" not in row[2]])
            query = f"{select} ({where});"
            found_batch = self.run_query(query)
            decoded_batch = [row[0].decode('utf-8').rstrip(',') for row in found_batch]
            if self.args['dryrun']:
                continue

            # this will include all names with \ in them too, we can live with that
            really_missing = [row for row in rows[i:end] if row[2] not in decoded_batch]
            with open(outputpath, "a", encoding="utf-8") as gone_out:
                for gone in really_missing:
                    gone_out.write(" ".join(gone) + "\n")

    def check_global_users(self, uids_file, outputpath):
        '''
        check the uids in the gone uids file, in batches, to see if any
        of them are in the global user table, and write out those which
        are not.
        '''
        # create them once; the batch writes will append.
        with open(outputpath, "w", encoding="utf-8") as output:
            output.close()
        with open(outputpath + "_present", "w", encoding="utf-8") as output:
            output.close()

        with open(uids_file, "r", encoding="utf-8") as uid_input:
            content = uid_input.read()
            uid_input.close()
            entries = content.splitlines()
            # format:  uid registr_date name
            rows = [entry.split(' ',2) for entry in entries]
            # names = [row[2] for row in rows]

        select = "SELECT gu_id, gu_registration, gu_name FROM globaluser WHERE gu_name IN "
        last = len(rows)
        for i in range(0,last,50):
            end = min(i + 50, last)
            # skip names with '\' but we can manually check these later
            where = ",".join([self.prep_name_for_query(row[2]) for row in rows[i:end]
                              if "\\" not in row[2]])
            query = f"{select} ({where});"
            found_batch = self.run_query(query)
            if self.args['dryrun']:
                continue
            decoded_batch = [(str(row[0]), row[1].decode('utf-8'), row[2].decode('utf-8')) for row in found_batch]
            decoded_names = [row[2] for row in decoded_batch]

            # this will include all names with \ in them too, we can live with that
            gone = [row for row in rows[i:end] if row[2] not in decoded_names]
            with open(outputpath, "a", encoding="utf-8") as global_out:
                for global_missing in gone:
                    global_out.write(" ".join(global_missing) + "\n")

            # also record separately the entries present in the global user table,
            # the registration dates might be interesting
            with open(outputpath + "_present", "a", encoding="utf-8") as global_out:
                for global_present in decoded_batch:
                    global_out.write(" ".join(global_present) + "\n")


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: python3 account_creation_check.py [--actions <item,item,item>] [--config <path>]
    [--loginwiki_uids <startid,endid>] [--sourcewiki <wikidbname>]
    [--sourcewiki_uids <startid,endid>] [--outputdir <dir>] [--dryrun] [--verbose] [--help]

This script determines the appropriate mariadb hostname for the specified wiki and for loginwiki,
    runs queries on each of them to retrieve the specified fields from the user table for the specified
    user id intervals, records each set of entries to files in the specified directory, checks each
    entry on the source wiki to verify that a user of the same name is in the loginwiki list, and
    records any missing entries to a third output file.

Arguments:

     --actions          (-a):  actions to run, separated by a comma
                               possible choices: source, login, compare, gone, global
                               default: source,login,compare,gone
     --config           (-c):  file containing config settings for this script, see sample
                               ac_config.ini.sample for more information
                               default: ac_config.ini in the current working directory
     --loginwiki        (-l):  name of loginwiki database
                               default: loginwiki
     --login_uids       (-L):  start and end user ids, separated by a comma
                               default: largest uid - sourcewiki uid interval size,largest uid
     --sourcewiki       (-s):  name of wiki database
                               default: enwiki
     --source_uids      (-S):  default: largest uid - 100000, largest uid
     --outputdir        (-o):  directory in which to write output files; this directory
                               must already exist, it will not be created
                               default: output subdirectory in current working directory
     --dryrun           (-d):  print commands that would be run instead of running them
                               default: false
     --verbose          (-v):  display progress information and commands as they are run
                               default: false     
     --help             (-h):  display this message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


class DBConn():
    '''
    manage a connection to some database
    '''
    def __init__(self, wikidb, user, password, settings):
        self.wikidb = wikidb
        self.dbcreds =  {'user': user, 'password': password}
        self.settings = settings
        self.conn = None
        self.hostname = None
        self.section_name = None
        self.dbconfig = {}

    def get_db_section_info(self):
        '''
        read section-based db lists from flat files in a specific directory
        we expect the filenames to be s1.dblist, s2.dblist and so on
        each such file should have one wiki database name per line, with
        no other content except possibly comments starting with #
        '''
        section_names = glob.glob(os.path.join(self.settings['main']['dblists_dir'], "s*.dblist"))
        for section in section_names:
            section_name = os.path.basename(section).split(".")[0]
            if not section_name[1:].isdigit():
                # some other thing like "securepollglobal.dblist" etc
                continue
            with open(section, "r", encoding="utf-8") as dblist:
                content = dblist.read()
                entries = content.splitlines()
                entries = [entry for entry in entries if not entry.startswith('#')]
                self.dbconfig[section_name] = entries
        return self.dbconfig

    def get_db_hostname(self):
        '''
        get the analytics hostname serving the wiki db
        '''
        section_name = ""
        dbs_by_section = self.get_db_section_info()

        for section, dbs in dbs_by_section.items():
            if self.wikidb in dbs:
                section_name = section
                break
        if not section_name:
            raise RuntimeError(f"failed to find section name for {self.wikidb}")

        self.section_name = section_name
        return self.settings['main']['hostname_templ'].format(section=section_name)

    def get_db_host_port(self):
        '''
        given a wiki db name, get the hostname and port of the analytics
        server that hosts it
        '''
        hostname = self.get_db_hostname()
        if not self.section_name[0] == 's' and self.section_name[1:].isdigit():
            raise ValueError(f"bad section name {self.section_name}")
        section_number = int(self.section_name[1:])
        port = self.settings['main']['port']
        if port:
            port = int(port)
        else:
            port = 3310 + section_number
        return (hostname, port)

    def get_conn(self):
        '''
        get an open connection to the db if we don't already have one,
        and return it
        '''
        if self.conn:
            return self.conn
        hostname, port = self.get_db_host_port()
        try:
            dbconn = MySQLdb.connect(
                host=hostname, port=port,
                user=self.dbcreds['user'], passwd=self.dbcreds['password'])
            self.conn = dbconn
            self.hostname = hostname
            return self.conn
            # return dbconn.cursor(), dbconn.thread_id()
        except MySQLdb.Error as ex:
            raise MySQLdb.Error(
                "failed to connect to or get cursor from "
                f"{hostname}:{port}, {ex.args[0]}:{ex.args[1]}")


class OptHandler():
    '''
    manage settings and options and their values
    '''
    DBLISTS_PATH = "/srv/mediawiki-config/dblists"
    HOSTNAME_TEMPLATE = "{section}-analytics-replica.eqiad.wmnet"
    BATCHSIZE = "5"
    DEFAULT_DB_USER = 'root'

    @staticmethod
    def get_opt_defaults():
        '''
        set an array of arguments to their defaults and return it
        '''
        args = {}

        args['actions'] = KNOWN_ACTIONS

        cwd = os.getcwd()
        args['config'] = os.path.join(cwd, "ac_config.ini")

        args['loginwiki'] = 'loginwiki'
        args['sourcewiki'] = 'enwiki'

        args['outputdir'] = os.path.join(cwd, "output")

        args['dryrun'] = False
        args['verbose'] = False
        args['help'] = False

        return args

    @staticmethod
    def val_to_uids(val):
        '''
        convert num1,num2 to array {'start':num1, 'end': num2}
        '''
        fields = val.split(',')
        if len(fields) != 2:
            print("fields is", fields)
            usage("bad value specified for uids argument")
        return {'start': int(fields[0]), 'end': int(fields[1])}

    @staticmethod
    def get_opt_values(options, args):
        '''
        get and validate (somewhat) options, stuffing
        the values into the args array
        '''
        for (opt, val) in options:
            if opt in ["-a", "--actions"]:
                args['actions'] = val.split(",")
            elif opt in ["-c", "--config"]:
                args['config'] = val
            elif opt in ["-l", "--loginwiki"]:
                args['loginwiki'] = val
            elif opt in ["-L", "--login_uids"]:
                args['login_uids'] = OptHandler.val_to_uids(val)
            elif opt in ["-s", "--sourcewiki"]:
                args['sourcewiki'] = val
            elif opt in ["-S", "--source_uids"]:
                args['source_uids'] = OptHandler.val_to_uids(val)
            elif opt in ["-o", "--outputdir"]:
                args['outputdir'] = val
            elif opt in ["-d", "--dryrun"]:
                args['dryrun'] = True
            elif opt in ["-v", "--verbose"]:
                args['verbose'] = True
            elif opt in ["-h", "--help"]:
                usage("Help for this script")
            else:
                usage(f"Unknown option specified: {opt}")

        if not os.path.exists(args['outputdir']):
            usage(f"No such output path exists {args['outputdir']}")

    @staticmethod
    def get_settings(path):
        '''
        get settings from specified path
        '''
        settings = configparser.ConfigParser()
        settings['DEFAULT'] = {'dblists_dir': OptHandler.DBLISTS_PATH,
                               'hostname_templ': OptHandler.HOSTNAME_TEMPLATE,
                               'port': '',
                               'batchsize': OptHandler.BATCHSIZE,
                               'dbuser': OptHandler.DEFAULT_DB_USER}

        if not path:
            return settings

        if not os.path.exists(path):
            usage(f"No such config file {path}")

        settings.read(path)

        if 'main' not in settings.sections():
            raise LookupError("The mandatory settings section 'main' was not specified.")

        return settings


def compare_user_info(source_uid_file, login_uid_file, missing_output):
    '''
    find all entries in source uids file not in login uids.
    note that these aren't necessarily missing completely from
    loginwiki, accounts might have been created on some other
    wiki much earlier and registered on the loginwiki at that
    time, hence missing from our uid interval
    '''
    with open(login_uid_file, "r", encoding="utf-8") as login_input:
        content = login_input.read()
        login_input.close()
        login_entries = content.splitlines()
        # format:  uid registr_date name
        login_rows = [login_entry.split(' ',2) for login_entry in login_entries]
        login_names = [login_row[2] for login_row in login_rows]

    with open(source_uid_file, "r", encoding="utf-8") as source_input:
        content = source_input.read()
        source_input.close()
        source_entries = content.splitlines()
        # format:  uid registr_date name
        source_rows = [source_entry.split(' ',2) for source_entry in source_entries]
        missing = [source_row for source_row in source_rows if source_row[2] not in login_names]

    with open(missing_output, "w", encoding="utf-8") as missing_out:
        for row in missing:
            missing_out.write(" ".join(row) + "\n")
        missing_out.close()

def do_main():
    '''
    entry point
    '''
    args = OptHandler.get_opt_defaults()
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 'a:c:l:L:s:S:o:dvh', ['actions=', 'config=', 'loginwiki=', 'login_uids=',
                                            'sourcewiki=', 'source_uids=',
                                            'outputdir=','dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    if remainder:
        usage(f"Unknown option(s) specified: {remainder[0]}")

    OptHandler.get_opt_values(options, args)
    if (args['dryrun'] or args['verbose']):
        print("command line arguments retrieved")
    for action in args['actions']:
        if action not in KNOWN_ACTIONS:
            usage(f"Unknown action specified: {action}, known are {KNOWN_ACTIONS}")

    settings = OptHandler.get_settings(args['config'])
    if (args['dryrun'] or args['verbose']):
        print("settings retrieved")

    mysql_user = settings['main']['dbuser']

    if ('source' in args['actions'] or 'login' in args['actions'] or
        'gone' in args['actions'] or 'global' in args['actions']):
        mysql_password = getpass("DB password: ")

    if 'source' in args['actions']:
        source_dbconn = DBConn(args['sourcewiki'], mysql_user, mysql_password, settings)
        source_dbconn.get_conn()
        if (args['dryrun'] or args['verbose']):
            print("source wiki db connection established")

        source_queries = QueryRunner(source_dbconn, args)
        source_queries.init_conn()

        if 'source_uids' not in args:
            args['source_uids'] = source_queries.get_uid_range(DEFAULT_UID_INTERVAL, -1)
        elif args['source_uids']['end'] == -1:
            args['source_uids']['end'] = source_queries.get_max_uid()
        if (args['dryrun'] or args['verbose']):
            print("source uids is", args['source_uids'])

    if 'login' in args['actions'] or 'gone' in args['actions']:
        login_dbconn = DBConn(args['loginwiki'], mysql_user, mysql_password, settings)
        login_dbconn.get_conn()
        if (args['dryrun'] or args['verbose']):
            print("login wiki db connection established")

        login_queries = QueryRunner(login_dbconn, args)
        login_queries.init_conn()

    if 'login' in args['actions']:
        if 'login_uids' not in args:
            args['login_uids'] = login_queries.get_uid_range(2 * DEFAULT_UID_INTERVAL, -1)
        elif args['login_uids']['end'] == -1:
            args['login_uids']['end'] = login_queries.get_max_uid()

        if (args['dryrun'] or args['verbose']):
            print("login uids is", args['login_uids'])

    if 'global' in args['actions']:
        global_dbconn = DBConn('centralauth', mysql_user, mysql_password, settings)
        global_dbconn.get_conn()
        if (args['dryrun'] or args['verbose']):
            print("centralauth db connection established")

        global_queries = QueryRunner(global_dbconn, args)
        global_queries.init_conn()

    date = time.strftime("%Y%m%d", time.gmtime())
    count = int(settings['main']['batchsize'])

    source_output = os.path.join(args['outputdir'], f"source_uids_{date}")
    login_output = os.path.join(args['outputdir'], f"login_uids_{date}")
    compare_output = os.path.join(args['outputdir'], f"missing_uids_{date}")
    gone_output = os.path.join(args['outputdir'], f"gone_uids_{date}")
    global_output = os.path.join(args['outputdir'], f"global_uids_{date}")

    if 'source' in args['actions']:
        source_queries.get_user_batches(args['source_uids'], source_output, count)

    if 'login' in args['actions']:
        login_queries.get_user_batches(args['login_uids'], login_output, count)

    if 'compare' in args['actions']:
        compare_user_info(source_output, login_output, compare_output)

    if 'gone' in args['actions']:
        login_queries.check_missing_uids(compare_output, gone_output)

    if 'global' in args['actions']:
        global_queries.check_global_users(gone_output, global_output)


if __name__ == '__main__':
    do_main()
