
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

temp_csv = 'c:'+os.sep+'temp'+os.sep+'temp.log'


# In[6]:

log_dir = 'c:'+os.sep+'temp'+os.sep+'log'+os.sep


# In[7]:

#log_file = 'c:'+os.sep+'temp'+os.sep+'hk-ssg140.log-20150922'


# In[8]:

disk_engine = create_engine('sqlite:///c:\\temp\\access_log.db')


# In[9]:

#store = pd.HDFStore('access_log.h5')


# In[10]:

'''
def get_server_from_URL(URLs):
    result = []
    
    for u in URLs:
        if u.startswith('http://'):
            temp = u.split('http://')[1]
            # further split on '/' character to return only server 
            result.append(temp.split('/')[0])
        
        elif u.startswith('http://'):
            temp = u.split('https://')[1]
            # further split on '/' character to return only server 
            result.append(temp.split('/')[0])
        
    return result
'''    


# In[11]:

'''
def remove_safe_site(URLs, safe_list):
    result = []
    for u in URLs:
        m = False
        for s in safe_list:
            if s in u:
                m = True
                break
                
        if (m == False):
            result.append(u)
            
    result = pd.Series(result).unique()
        
    return result 
'''    


# In[12]:

'''
def get_match_site(URLs, safe_list):
    result = []
    for u in URLs:
        m = False
        for s in safe_list:
            if s in u:
                m = True                
                break
                                
        if (m == True):
            result.append(u)
            
    result = pd.Series(result).unique()
        
    return result   
'''    


# In[13]:

#squid = sq.read_squid_log('access.log')
#squid.tail()
#safe_websites = 'google|microsoft|trendmicro|gstatic.com|bdpinsight.eu|.gov.hk'
#access = squid[squid['URL'].str.contains(safe_websites) == False]
#sum(squid.bytes)/Gb


# In[14]:

'''
def clean_juniper_file(infile, outfile):
    with open(infile, 'r') as in_file:
        with open(outfile, 'w') as out_file:
            for line in in_file:
                if ('system-notification-00257(traffic)' in line):                                                
                    idx_1 = line.find('service=')
                    idx_2 = line.find(' proto=')
                    if (idx_1 and idx_2):
                        out_file.write(line[:idx_1]+line[idx_1:idx_2].replace(' ', '')+line[idx_2:])
'''                        


# In[15]:

'''
def open_csv(filename, chunksize=20000):
    
    df = pd.DataFrame()
    
    for c in pd.read_table(filename, sep='\s{1,}|"', chunksize=chunksize, iterator=True, engine='python', index_col=False, header=None, parse_dates=[[0, 1, 2], [9,10]]):        
        df = pd.concat([df, c])        
        
    print(str(len(df)) + " rows processed.")
    
    return df

def juniper_log_cleansing(x):
    if str(x).find('=') > 0:
        return str(x)[str(x).find('=')+1:]
    elif str(x).find(':') > 0:
        return str(x)[:str(x).find(':')]    
    else:
        return x

def read_syslog_juniper(filename):
       
    df = open_csv(filename)
    
    df = df[df[7].str.contains('information') == False]
    
    # restructure the dataframe
    i = [0]
    i.extend(range(3, 4))
    i.extend([1])
    i.extend(range(9, 13))
    i.extend([14])
    i.extend([16])
    i.extend(range(17, 25))
    df = df.iloc[:, i]
    
    df.columns = ['time', 'device_name', 'traffic_start', 'duration', 'policy_no', 'service', 'protocol', 'src_zone', 'dst_zone', 'action', 'sent_size', 'received_size', 'src_address', 'dst_address', 'src_port', 'dst_port', 'remarks']
    df[['device_name', 'duration', 'policy_no', 'service', 'protocol', 'src_zone', 'dst_zone', 'action', 'sent_size', 'received_size', 'src_address', 'dst_address', 'src_port', 'dst_port']] = df[['device_name', 'duration', 'policy_no', 'service', 'protocol', 'src_zone', 'dst_zone', 'action', 'sent_size', 'received_size', 'src_address', 'dst_address', 'src_port', 'dst_port']].applymap(juniper_log_cleansing)
    df[['policy_no', 'protocol', 'sent_size', 'received_size', 'src_port', 'dst_port']] = df[['policy_no', 'protocol', 'sent_size', 'received_size', 'src_port', 'dst_port']].convert_objects(convert_numeric = True)
    
    df = df.reset_index(drop=True)
    
    return df      
'''


# In[16]:

'''
def process_group(grouped):
    temp_result = []
    for d, src, dst, srv, a in grouped.groups.keys():
        temp_result.append([d, src, dst, srv, len(juniper.iloc[grouped.groups[(d, src, dst, srv, a)]]), 
                            np.sum(juniper.iloc[grouped.groups[(d, src, dst, srv, a)]]).total_size, a])
    
    return temp_result
'''    


# In[17]:

for file in os.listdir(log_dir):
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
    
    os.remove(temp_csv)

