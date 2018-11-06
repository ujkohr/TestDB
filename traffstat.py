"Traffic statistic log parser."

# linter: do not inspect naming convention (103) and too long lines (301)
# pylint: disable=C0103
# pylint: disable=C0301

import sys
import os
import re
import json
# import csv
import hashlib
# import MySQLdb
import mysql.connector
# import pyodbc

NM = re.compile(r'^-?\d*\.?\d*$') # regex as IsNumeric() function
LOGDIR = r"C:\Platypus\Logs"  # logs directory
# CSVFILE = r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\traffstat.csv" # LOAD DATA INFILE
CSVFILE = r"C:\local_data\traffstat.csv" # LOAD DATA LOCAL INFILE
# CSVSQLPATH = "'/ProgramData/MySQL/MySQL Server 8.0/Uploads/traffstat.csv'" # LOAD DATA INFILE
CSVSQLPATH = "'C:/local_data/traffstat.csv'" # LOAD DATA LOCAL INFILE
PARSEDDIR = r"C:\Platypus\parsed_logs_json" # parsed json data directory

# def write_to_db():
#     "Write data from csv file to database."

#     try:
#         # local_infile option must be set here at client and at the server side also !!!
#         conn = MySQLdb.connect(host='localhost', user='platypus_exec', passwd='platty_1', db='platypus', local_infile=1) # LOAD DATA LOCAL INFILE
#         # conn = MySQLdb.connect(host='localhost', user='platypus_exec', passwd='platty_1', db='platypus') # LOAD DATA INFILE
#         cursor = conn.cursor()
#         cursor.execute("LOAD DATA LOCAL INFILE " + CSVSQLPATH + " INTO TABLE platypus.traff_stat FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES")
#         conn.commit()

#     except Exception as exc:
#         print(exc)

#     finally:
#         conn.close()



# def parse_log(log_file):
#     "Log file parser."

#     try:
#         TSLOG = open(log_file, "r")
#         tslist = [] # row list
#         fieldlist = [] # list of field names
#         STARTSTR = "SUMMARY"
#         firstrow = True

#         while True:
#             tstxt = TSLOG.readline()
#             if tstxt == '':
#                 break   # EOF
#             pos = tstxt.find(STARTSTR)
#             if pos != -1:  # row start found
#                 tsrow = [] # list of field-value pairs
#                 while True:
#                     tsline = TSLOG.readline()
#                     if tsline[0:5] != '-----' and tsline[-6:-1] != '-----': # process all lines which contain fields (test just first 5 and last 5 characters)
#                         tstemp = tsline.split() # delimiter is space, field names are going to be found by type
#                         attname = ""
#                         # !!! this for loop requires further optimization !!!
#                         for z in tstemp:
#                             if NM.match(z) is None: # not numeric, it is field name or it's part
#                                 attname = attname + z + " "
#                             else: # numeric, field value is found
#                                 attname = str(attname[0:-1]) # remove last character form field name - it is space
#                                 # tsrow.append([attname, z]) # write field and it's value to row as list
#                                 if firstrow:
#                                     fieldlist.append(attname)
#                                 tsrow.append(z)
#                                 attname = ""
#                     else:
#                         tslist.append(tsrow) # end of lines with fields, write row to row list
#                         firstrow = False
#                         break
#                 del tsrow # tsrow.clear() deletes also record in tslist ?!

#         # print("The log contains " + str(len(tslist)) + " row(s)")
#         TSLOG.flush()
#         TSLOG.close()

#         # TSFILE = open(r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\traffstat.json", "w")
#         # TSFILE.write(json.dumps(tslist, indent=4, sort_keys=True))
#         # TSFILE.close()

#         # write data to temporary csv file suitable for fast SQL import method
#         TSFILE = open(CSVFILE, "w")
#         wr = csv.writer(TSFILE, quoting=csv.QUOTE_NONE)
#         wr.writerow(fieldlist)  # write header
#         wr.writerows(tslist)    # write data
#         TSFILE.flush()
#         TSFILE.close()

#         write_to_db()   # call 'write to database' function

#     except FileNotFoundError:
#         print("File not found !")

filelist = []
conn = None
JSNFILE = None

def setfilelist():
    "File list generator."
    for root, folders, files in os.walk(PARSEDDIR):
        for file in files:
            filelist.append(root + "\\" + file)


# void main()

# search for arguments
if len(sys.argv) == 1:  # there is no argument, search for files in a directory
    setfilelist()
elif len(sys.argv) == 2:    # an argument is a path
    filelist.append(sys.argv[1])
else:
    print("Invalid argument !")
    sys.exit(22)

try:
    for pfile in filelist:
        # log file must be checked for previous use - no log should be parsed twice !
        statinfo = os.stat(pfile)
        hasher = hashlib.md5()
        hasher.update(str.encode(pfile + str(statinfo.st_size))) # (file name + file size) hash
        try:
            # open connection as late as possible and close it as soon as possible
            # conn = MySQLdb.connect(host='localhost', user='platypus_exec', passwd='platty_1', db='platypus') # local db engine
            conn = mysql.connector.connect(host='mysqlpc01.lmera.ericsson.se', user='mpswprofiling', password='CJkQ9EQcXTgnSt', db='test_mpswprofiling')
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(hash) FROM used_file_hash WHERE hash = "' + hasher.hexdigest() + '"')
            hashcount = cursor.fetchone() # get result, this is NOT row count - it is always one (1) !
            if hashcount[0] == 0: # no record exists, this is brand new log
                # get last row from g2_capacity_metadata (current test run)
                # fetching it's (outdated) value from the some file is very, very bad practice !!!
                cursor.execute('SELECT MAX(TestRunId) FROM g2_capacity_metadata')
                new_trID = cursor.fetchone()[0]

                # get last row from g2_capacity_metadata and increment it by one
                # fetching it's (outdated) value from the some file is very, very bad practice !!!

                # new_trID = cursor.fetchone()[0] + 1
                # # create rows in related tables and commit changes to allow others to create valid records
                # try:
                #     cursor.execute('INSERT INTO g2_capacity_metadata (TestRunId) VALUES (' + str(new_trID) + ')')
                #     cursor.execute('INSERT INTO g2_capacity_wrat_verdicts (TestRunId) VALUES (' + str(new_trID) + ')')
                #     conn.commit()
                # except MySQLdb.Error as exc:    # does not work without python.linting.pylintArgs : --extension-pkg-whitelist=_mysql option
                #     print(exc)
                #     conn.rollback()
                #     break

                # parsed file is not JSON, individual lines are !
                JSNFILE = open(pfile, "r", newline='\n') # for Windows file format: newline='\r\n'
                while True:
                    # read file line byline
                    jsonline = JSNFILE.readline()
                    if jsonline == '':
                        break
                    pyobj = json.loads(jsonline)

                    # create a row for the data

                    cursor.execute('INSERT INTO g2_capacity_wrat_verdicts (TestRunId) VALUES (' + str(new_trID) + ')')
                    lastrowID = cursor.lastrowid # there must be autoincrement column for this one to work

                    # in general, mapping fields from log to parser or parser to db would be required
                    # this could be done via db table as mapping interface

                    mtdt_v = pyobj['metadata']['traffStatFilePath']
                    # string variable in MySQL statement MUST be in double quotes !!!
                    stmt = 'UPDATE g2_capacity_wrat_verdicts SET TraffStatFile = "' + mtdt_v + '" WHERE ID = ' + str(lastrowID)
                    cursor.execute(stmt)

                    # write all available fields in wrat-verdict JSON section - no hardcoding !
                    for wrat_v in pyobj['wrat-verdict'].items():
                        stmt = 'UPDATE g2_capacity_wrat_verdicts SET ' + wrat_v[0] + ' = ' + wrat_v[1] + ' WHERE ID = ' + str(lastrowID)
                        cursor.execute(stmt)


                # delete orphan TestRunID from g2_capacity_metadata and g2_capacity_wrat_verdicts
                # cursor.execute('DELETE FROM g2_capacity_metadata WHERE TestRunId = ' + str(new_trID))
                # conn.commit()

                # file is succesfully written into the database, record it's hash
                cursor.execute('INSERT INTO used_file_hash (hash) VALUES ("' + hasher.hexdigest() + '")')

                # commit data as a transaction, rollback on error
                conn.commit()

            else:
                # write to parser log file
                print(pfile + "' has already been parsed !")
        except mysql.connector.Error as exc:
            if exc.errno == 2003 or exc.errno == 1045:
                # cannot connect or access denied
                print(exc.msg)
            else:
                conn.rollback()

        finally:
            if conn is not None:
                conn.close()
            if JSNFILE is not None:
                JSNFILE.close()

        # print(hasher.hexdigest())
        # print(file + str(statinfo.st_size))

except FileNotFoundError:
    print("File not found !")
