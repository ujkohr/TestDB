#!/usr/bin/env python3
# -*- coding: utf-8 -*
'''
    Description :

    The script uses 'master-g2-mrat.json.txt' as a source file and writes it's content into
    'g2_capacity_wrat_verdicts' (destination) table.
    At first, it checks for existance of a hash of a previous processed files
    written in 'used_file_hash' table.
    Then,it fetches current test run (TestRunID) from 'g2_capacity_metadata' table.
    At last, it tries to fill the destination table with all available data in source file.
    If succeeded, it writes source file hash into the 'used_file_hash' table.
    Else, it rollbacks all changes in the database.
'''

# linter: do not inspect naming convention (103) and too long lines (301)
# pylint: disable=C0103
# pylint: disable=C0301

import sys
import os
import inspect
import argparse
import logging
import re
import json
import hashlib
import mysql.connector

__all__ = []    # no public modules
__version__ = 1.0
__date__ = '2018-09-11'
__updated__ = '2018-09-20'
__author__ = "Damir Padavic"
__copyright__ = "Ericsson, Platypus Project, 2018. All rights reserved"
__credits__ = ["Platypus team"]
__license__ = "Ericsson proprietary license for internal use only"
__maintainer__ = "Damir Padavic (epaddam)"
__email__ = "damir.padavic@ericsson.com"
__status__ = "Prototype"   # "Prototype", "Development", or "Production"

def setfilelist(dir_path):

    "File list generator."
    for root, folders, files in os.walk(dir_path):
        for file in files:
            filelist.append(root + "\\" + file)


NM = re.compile(r'^-?\d*\.?\d*$') # regex as IsNumeric() function
CONFIG_FILE = r"C:\Platypus\logdirs.conf" # configuration file path

filelist = []
conn = None
JSNFILE = None
BUF_SIZE = 65536
def_source = None

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
current_python_file = inspect.getfile(inspect.currentframe())

if __name__ == "__main__":

    logging.info("%s has started !", current_python_file)
    # read configuration file
    try:
        with open(CONFIG_FILE, 'r') as f:
            while True:
                confline = (f.readline()).strip()
                if confline is not None:    #check for EOF
                    if confline != '':  # check for empty line
                        if confline[0] != "#":    # check for a comment
                            pos = confline.find('=')
                            if pos != -1:
                                if confline[:pos].strip() == 'WRAT_VERDICTS_SOURCE':
                                    def_source = (confline[(pos+1):]).strip()
                                    break
                else:
                    break
    except FileNotFoundError:   # No such file: logdirs.conf
        def_source = None
        logging.info("No such file %s", CONFIG_FILE)


    if def_source is None:
        def_source = os.path.dirname(os.path.realpath(__file__)) + r'\master-g2-mrat.json.txt'    # if there is no record in config file, assume that source file is in script directory


    program_shortdesc = __import__('__main__').__doc__.split(r'\n')[0]

    program_desc = '''
        {}
        Created by {} on {}.
        © {}.
        Licenced under {}.

    Detailed usage description:

        Run the script with parameter from logdirs.conf file or use script directory if record or config file does not exist :
            platypus-wrat-verdict-dbwriter.py
        Run the script with source file as a parameter
            platypus-wrat-verdict-dbwriter.py --in_path <path to the source file>
        Run the script with source directory as a parameter
            platypus-wrat-verdict-dbwriter.py --in_path <path to the source directory> --is_dir True
    '''.format(program_shortdesc, __author__, __date__, __copyright__, __license__)

    program_version = '{} {} ({})'.format(os.path.basename(sys.argv[0]), "v{}".format(__version__), str(__updated__))

    argp = argparse.ArgumentParser(description=program_desc, formatter_class=argparse.RawDescriptionHelpFormatter) # keep formatting as in script
    argp.add_argument('-v', '--version', action='version', version=program_version)
    argp.add_argument("--in_path", help="a path to a source file or directory", default=def_source) # read it from logdirs.conf
    argp.add_argument("--is_dir", help="a path is a directory (default = False)", type=bool, default=False)
    args = argp.parse_args()

    if args.in_path != '' and (not args.is_dir):    # an argument is a path to a file
        filelist.append(args.in_path)
    elif args.in_path != '' and args.is_dir:    # an argument is a path to a directory
        setfilelist(args.in_path)
    else:
        logging.error("  %s:%s %s %r", current_python_file, inspect.currentframe().f_code.co_name, "No path to source file/directory !", "")
        sys.exit(22)

    try:
        for pfile in filelist:
            # parsed file must be checked for previous use - no data should be written twice !
            statinfo = os.stat(pfile)
            hasher = hashlib.md5()
            # hasher.update(str.encode(pfile + str(statinfo.st_size))) # (file name + file size) hash

            with open(pfile, 'rb') as f:
                while True:
                    data = f.read(BUF_SIZE) # read chunks of the source file
                    if not data:
                        break
                    hasher.update(data)

            try:
                # open connection as late as possible and close it as soon as possible
                conn = mysql.connector.connect(host='mysqlpc01.lmera.ericsson.se', user='mpswprofiling', password='CJkQ9EQcXTgnSt', db='test_mpswprofiling')
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(hash) FROM used_file_hash WHERE hash = "' + hasher.hexdigest() + '"')
                hashcount = cursor.fetchone() # get result, this is NOT row count - it is always one (1) !
                if hashcount[0] == 0: # no record exists, this is a brand new log
                    # get last row from g2_capacity_metadata (current test run)
                    cursor.execute('SELECT MAX(TestRunId) FROM g2_capacity_metadata')
                    new_trID = cursor.fetchone()[0]

                    # parsed file is not JSON, individual lines are !
                    JSNFILE = open(pfile, "r", newline='\n') # for Windows file format: newline='\r\n'
                    while True:
                        # read file line by line
                        jsonline = JSNFILE.readline()
                        if jsonline == '':
                            break
                        pyobj = json.loads(jsonline)

                        if 'wrat-verdict' in pyobj:
                        # create a row for the data
                            cursor.execute('INSERT INTO g2_capacity_wrat_verdicts (TestRunId) VALUES (' + str(new_trID) + ')')
                            lastrowID = cursor.lastrowid # there must be autoincrement column for this one to work

                            # in general, mapping fields from log to parser or parser to db would be required
                            # this could be done via db table as a mapping interface

                            mtdt_v = pyobj['metadata']['traffStatFilePath']
                            # string variable in MySQL statement MUST be in double quotes !!!
                            stmt = 'UPDATE g2_capacity_wrat_verdicts SET TraffStatFile = "' + mtdt_v + '" WHERE ID = ' + str(lastrowID)
                            cursor.execute(stmt)

                            # write all available fields in wrat-verdict JSON section - no hardcoding !
                            for wrat_v in pyobj['wrat-verdict'].items():
                                stmt = 'UPDATE g2_capacity_wrat_verdicts SET ' + wrat_v[0] + ' = ' + wrat_v[1] + ' WHERE ID = ' + str(lastrowID)
                                cursor.execute(stmt)

                    # file is succesfully written into the database, record it's hash
                    cursor.execute('INSERT INTO used_file_hash (hash) VALUES ("' + hasher.hexdigest() + '")')

                    # commit data as a transaction
                    conn.commit()
                    logging.info("%s is succesfully written to the database.", pfile)

                else:
                    # duplicate hash record found !
                    logging.info("%s has already been parsed !", pfile)
            except mysql.connector.Error as exc:
                if exc.errno == 2003 or exc.errno == 1045:
                    # cannot connect or access denied
                    logging.error("  %s:%s %s %r", current_python_file, inspect.currentframe().f_code.co_name, exc.msg, "")

                else:
                    # return database to previous consistent state
                    conn.rollback()

            finally:
                if conn is not None:
                    conn.close()
                if JSNFILE is not None:
                    JSNFILE.close()

    except FileNotFoundError as exc:
        logging.error("  %s:%s %s %r", current_python_file, inspect.currentframe().f_code.co_name, exc.strerror, pfile)
