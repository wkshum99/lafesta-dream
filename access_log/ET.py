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
import urllib
import os

from sqlalchemy import create_engine

class log_reader:
    Gb = 1000000000
    
    def __init__(self, location, temp_csv, log_dir, zip_dir, sqlitedb, mode='full'):

        #temp_csv = 'v:'+os.sep+'temp'+os.sep+'temp.log'
        #log_dir = 'v:'+os.sep+'temp'+os.sep+'log'+os.sep
        #zip_dir = 'v:'+os.sep+'temp'+os.sep+'a'+os.sep
        #sqlitedb = 'v:\\temp\\access_log.db'
        self.temp_csv = temp_csv
        self.log_dir = log_dir
        self.zip_dir = zip_dir
        self.sqlitedb = sqlitedb
        self.location = location
        self.mode = mode

    def connect_sqlite(self):
        return create_engine('sqlite:///'+self.sqlitedb)

    def valid_file(self, file):
        if (file[-4:] != '.zip' and file[0:9] == 'hk-ssg140'):
            return True
        else:
            return False

    def main(self):

        print('Started at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

        if (self.mode == 'single'):
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            log_file = os.path.join(self.log_dir, 'hk-ssg140.log-'+yesterday.strftime('%Y%m%d'))
                                                                         
            jp.clean_juniper_file(log_file, self.temp_csv)
    
            juniper = [] 
            temp_result = []
    
            juniper = jp.read_syslog_juniper(self.temp_csv)
    
            juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
            juniper['total_size'] = juniper['sent_size'] + juniper['received_size']        
    
            temp_result = jp.process_group(juniper)
    
            temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'count', 'total_size'])
            temp_result['location'] = self.location
    
            try:
                temp_result.to_sql('data', self.connect_sqlite(), index=False, if_exists='append')
                print (log_file + ' processed')
            except:
                pass
    
            hp.zip_file(zip_dir + log_file + '.zip', os.path.join(log_dir, log_file))
            
            os.remove(os.path.join(log_dir, log_file))                                                                         
                                                                         
        else:    
            for file in os.listdir(log_dir):
                if (self.valid_file(file)):
                    log_file = os.path.join(log_dir, file)
                    jp.clean_juniper_file(log_file, temp_csv)
    
                    juniper = [] 
                    temp_result = []
    
                    juniper = jp.read_syslog_juniper(temp_csv)
    
                    juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
                    juniper['total_size'] = juniper['sent_size'] + juniper['received_size']        
    
                    temp_result = jp.process_group(juniper)
    
                    temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'count', 'total_size'])
                    temp_result['location'] = self.location
        
                    try:
                        temp_result.to_sql('data', self.connect_sqlite(), index=False, if_exists='append')
                        print (file + ' processed')
                    except:
                        pass
    
                    hp.zip_file(self.zip_dir + file + '.zip', os.path.join(self.log_dir, file))
                
                    os.remove(os.path.join(self.log_dir, file))
              
        os.remove(self.temp_csv)

        print('Completed at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

temp_csv = 'e:'+os.sep+'temp'+os.sep+'temp.log'
log_dir = 'e:'+os.sep+'temp'+os.sep+'log'+os.sep
zip_dir = 'e:'+os.sep+'temp'+os.sep+'a'+os.sep
sqlitedb = 'e:'+os.sep+'temp'+os.sep+'access_log.db'

juniper = log_reader('HK', temp_csv, log_dir, zip_dir, sqlitedb, mode='full')
juniper.main()

