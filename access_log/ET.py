
# coding: utf-8

# In[15]:

from __future__ import division


# In[16]:

import csv
import pandas as pd
import time
import string
import numpy as np
import helper as hp
import sq
import jp
import urllib
import os


# In[17]:

from sqlalchemy import create_engine


# In[18]:

pd.set_option('display.max_columns', None)
Gb = 1000000000


# In[19]:

temp_csv = 'v:'+os.sep+'temp'+os.sep+'temp.log'


# In[20]:

log_dir = 'v:'+os.sep+'temp'+os.sep+'log'+os.sep


# In[21]:

zip_dir = 'v:'+os.sep+'temp'+os.sep+'a'+os.sep


# In[22]:

sqlitedb = 'v:\\temp\\access_log.db'


# In[23]:

disk_engine = create_engine('sqlite:///'+sqlitedb)


# In[24]:

#store = pd.HDFStore('access_log.h5')


# In[25]:

#squid = sq.read_squid_log('access.log')
#squid.tail()
#safe_websites = 'google|microsoft|trendmicro|gstatic.com|bdpinsight.eu|.gov.hk'
#access = squid[squid['URL'].str.contains(safe_websites) == False]
#sum(squid.bytes)/Gb


# In[32]:

def valid_file(file):
    if (file[-4:] != '.zip' and file[0:9] == 'hk-ssg140'):
        return True
    else:
        return False


# In[33]:

print('Started at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

mode = 'full'
if (mode == 'single'):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    log_file = os.path.join(log_dir, 'hk-ssg140.log-'+yesterday.strftime('%Y%m%d'))
                                                                         
    jp.clean_juniper_file(log_file, temp_csv)
    
    juniper = [] 
    temp_result = []
    
    juniper = jp.read_syslog_juniper(temp_csv)
    
    juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    juniper['total_size'] = juniper['sent_size'] + juniper['received_size']        
    
    temp_result = jp.process_group(juniper)
    
    temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'count', 'total_size'])
    temp_result['location'] = 'HK'
    
    try:
        temp_result.to_sql('data', disk_engine, index=False, if_exists='append')
        print (log_file + ' processed')
    except:
        pass
    
    hp.zip_file(zip_dir + log_file + '.zip', os.path.join(log_dir, log_file))
            
    os.remove(os.path.join(log_dir, log_file))                                                                         
                                                                         
else:    
    for file in os.listdir(log_dir):
        if (valid_file(file)):
            log_file = os.path.join(log_dir, file)
            jp.clean_juniper_file(log_file, temp_csv)
    
            juniper = [] 
            temp_result = []
    
            juniper = jp.read_syslog_juniper(temp_csv)
    
            juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
            juniper['total_size'] = juniper['sent_size'] + juniper['received_size']        
    
            temp_result = jp.process_group(juniper)
    
            temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'count', 'total_size'])
            temp_result['location'] = 'HK'
    
            try:
                temp_result.to_sql('data', disk_engine, index=False, if_exists='append')
                print (file + ' processed')
            except:
                pass
    
            hp.zip_file(zip_dir + file + '.zip', os.path.join(log_dir, file))
            
            os.remove(os.path.join(log_dir, file))
              
os.remove(temp_csv)

print('Completed at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

