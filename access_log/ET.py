
# coding: utf-8

# In[1]:

from __future__ import division


# In[2]:

import csv
import pandas as pd
import datetime
import string
import numpy as np
import helper as hp
import sq
import jp
import urllib
import os


# In[3]:

from sqlalchemy import create_engine


# In[4]:

pd.set_option('display.max_columns', None)
Gb = 1000000000


# In[5]:

temp_csv = 'v:'+os.sep+'temp'+os.sep+'temp.log'


# In[6]:

log_dir = 'v:'+os.sep+'temp'+os.sep+'log'+os.sep


# In[7]:

zip_dir = 'v:'+os.sep+'temp'+os.sep+'a'+os.sep


# In[8]:

sqlitedb = 'v:\\temp\\access_log.db'


# In[9]:

#log_file = 'c:'+os.sep+'temp'+os.sep+'hk-ssg140.log-20150922'


# In[10]:

disk_engine = create_engine('sqlite:///'+sqlitedb)


# In[11]:

#store = pd.HDFStore('access_log.h5')


# In[12]:

#squid = sq.read_squid_log('access.log')
#squid.tail()
#safe_websites = 'google|microsoft|trendmicro|gstatic.com|bdpinsight.eu|.gov.hk'
#access = squid[squid['URL'].str.contains(safe_websites) == False]
#sum(squid.bytes)/Gb


# In[15]:

def valid_file(file):
    if (file[-4:] != '.zip' & file[0:9] == 'hk-ssg140'):
        return True
    else:
        return False


# In[13]:

print('Started at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

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
    
    #zip = zipfile.ZipFile(zip_dir + file + '.zip', 'w')
    #zip.write(os.path.join(log_dir, file), compress_type=zipfile.ZIP_DEFLATED)
    #zip.close()
        hp.zip_file(zip_dir + file + '.zip', os.path.join(log_dir, file))
            
        os.remove(os.path.join(log_dir, file))
              
os.remove(temp_csv)

print('Completed at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

