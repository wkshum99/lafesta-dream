from __future__ import division

import csv
import pandas as pd
import datetime
import time
import string
import numpy as np
import helper as hp
import sq
import jp
import sys
import os

from sqlalchemy import create_engine

class log_reader:
    Gb = 1000000000
    
    def __init__(self, location, temp_csv, log_dir, zip_dir, sqlitedb, mode='full'):

        self.temp_csv = temp_csv
        self.log_dir = log_dir
        self.zip_dir = zip_dir
        self.sqlitedb = sqlitedb
        self.location = location
        self.mode = mode

    def connect_sqlite(self):
        return create_engine('sqlite:///'+self.sqlitedb)

    def check_file_format(self, file):
        if file[-4:] == '.zip':
            return 'zip'
        elif file[-4:] == 'r.gz':
            return 'gzip'
        else:
            if (file[0:9] == 'hk-ssg140'):
                return 'txt'
            else:
                return 'other'

    def main(self):

        if len(os.listdir(log_dir)) <= 0:
            sys.exit('There is no file in the log directory, program ended')

        print('Started at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

        if (self.mode == 'single'):
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            log_file = os.path.join(self.log_dir, 'hk-ssg140.log-'+yesterday.strftime('%Y%m%d'))
                                                                         
            jp.clean_juniper_file(log_file, self.temp_csv)

    
            juniper = jp.read_syslog_juniper(self.temp_csv)
    
            juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
            juniper['total_size'] = juniper['sent_size'] + juniper['received_size']        
    
            temp_result = jp.process_group(juniper)
    
            temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'dst_zone', 'count', 'total_size'])
            temp_result['location'] = self.location
    
            temp_result.to_sql('data', self.connect_sqlite(), index=False, if_exists='append')
            print (log_file + ' processed')

            hp.zip_file(zip_dir + log_file + '.zip', os.path.join(log_dir, log_file))
            
            os.remove(os.path.join(log_dir, log_file))                                                                         
                                                                         
        else:    
            for file in os.listdir(log_dir):
                file_format = self.check_file_format(file)
                if (file_format == 'zip'):
                    log_file = hp.open_zip(os.path.join(log_dir, file))

                elif (file_format == 'txt'):
                    log_file = os.path.join(log_dir, file)
                    with open(log_file, 'r') as in_file:
                        log_file = in_file.read()

                jp.clean_juniper_file(log_file, temp_csv)

                juniper = jp.read_syslog_juniper(temp_csv)

                juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
                juniper['total_size'] = juniper['sent_size'] + juniper['received_size']

                temp_result = jp.process_group(juniper)

                temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'dstzone', 'count', 'total_size'])
                temp_result['location'] = self.location

                temp_result.to_sql('data', self.connect_sqlite(), index=False, if_exists='append')
                print (file + ' processed')

                if (file_format == 'zip'):
                    os.rename(os.path.join(self.log_dir, file), os.path.join(self.zip_dir, file))
                elif (file_format == 'txt'):
                    hp.zip_file(self.zip_dir + file + '.zip', os.path.join(self.log_dir, file))
                    os.remove(os.path.join(self.log_dir, file))
              
        os.remove(self.temp_csv)

        print('Completed at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

temp_csv = 'v:'+os.sep+'temp'+os.sep+'temp.log'
log_dir = 'v:'+os.sep+'temp'+os.sep+'log'+os.sep
zip_dir = 'v:'+os.sep+'temp'+os.sep+'a'+os.sep
sqlitedb = 'v:'+os.sep+'temp'+os.sep+'access_log.db'
mode = 'full'

juniper = log_reader('HK', temp_csv, log_dir, zip_dir, sqlitedb, mode)
juniper.main()

