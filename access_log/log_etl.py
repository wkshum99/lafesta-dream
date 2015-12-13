from __future__ import division
import pandas as pd
import datetime
import time
import numpy as np
import helper as hp
import sys
import os
import sqlalchemy

log_dir = 'D:'+os.sep+'temp'+os.sep+'log'+os.sep
#zip_dir = 'E:'+os.sep+'temp'+os.sep+'a'+os.sep
sqlitedb = 'D:'+os.sep+'temp'+os.sep+'sg_access_log.db'
mode = 'full'
Gb = 1000000000

def check_file_format(file):
    if file[-4:] == '.zip':
        return 'zip'
    elif file[-4:] == 'r.gz':
        return 'gzip'
    else:
        if (file[0:11] == 'sgkd-ssg140'):
            return 'txt'
        else:
            return 'other'

# def get_values(s, labels):
#     # get values based on a list of labels
#     # input: a string to look for and a list of labels to match
#     # output: a list, value is None if label not present
#     result = []
#     for l in labels:
#         start = s.find(l)
#         if start != -1:
#             start += len(l)
#             end = s[start:].find('=')
#             if end != -1:
#                 end = start+s[start:start + end].rfind(' ')
#                 '''
#                     special handling for some label names that span more than one word
#                 '''
#                 if l == 'src zone=':
#                     result.append(s[start:end-4])
#                 elif l == 'start_time=':
#                     temp = s[start:end-4]
#                     result.append(temp[1:-6])
#                 else:
#                     result.append(s[start:end])
#             else:
#                 # if last labels, strip off the newline char
#                 result.append(s[start:-1])
#         else:
#             result.append(None)
#
#     return result

# def read_log_file(file):
#     result = pd.DataFrame()
#     with open(file, 'r') as in_file:
#         i = 1
#         for line in in_file:
#             #print i
#             if 'system-notification-00257(traffic)' in line:
#                 # split the line into list of values
#                 temp = get_values(line, ['start_time=', 'src zone=', 'dst zone=', 'src=', 'dst=', 'sent=',
#                                      'rcvd=', 'reason='])
#                 temp.append(1) # added for count of rows
#                 #print temp
#             # if date already existed
#             if result.empty != True:
#                 if len(temp) > 0:
#                     if result[(result[0]== temp[0]) & (result[1] == temp[1]) &
#                             (result[2] == temp[2]) & (result[3] == temp[3]) &(result[4] == temp[4])].empty != True:
#                         try:
#                             result[(result[0]== temp[0]) & (result[1] == temp[1]) & (result[2] == temp[2]) &
#                                (result[3] == temp[3]) & (result[4] == temp[4])][[5, 6, 7]] + [int(temp[5]), int(temp[6]), int(temp[8])]
#                         except:
#                             print "cannot append data row: " + i
#                             pass
#                     else:
#                         try:
#                             result = result.append([[temp[0], temp[1], temp[2], temp[3], temp[4], int(temp[5]), int(temp[6]), int(temp[8])]])
#                         except:
#                             print "cannot append data row: " + i
#                             pass
#             else:
#                 try:
#                     result = result.append([[temp[0], temp[1], temp[2], temp[3], temp[4], int(temp[5]), int(temp[6]), int(temp[8])]])
#                 except:
#                     print "cannot append data row: " + i
#                     pass
#             i+=1
#     # write data to sqldb
#     result.to_sql('data', connect_sqlite(sqlitedb))

def get_values(x):
    # remove the label-of-next-field
    # input: string
    # output: string
    if x[0] == '"':
        x = x[1:]
    x = str(x).split()
    return x[0]

def read_log_file(file):
    # read csv file into dataframe,
    # and grab only interesting columns
    # input: file
    # output: dataframe
    df = pd.read_table(file, sep='[a-z]*=', quotechar='"', header=None, engine='python')
    #df = df.iloc[:,[2, 3, 7, 8, 9, 10, 11, 12, 13]]
    df = df[pd.isnull(df[2])!=True][[2, 3, 7, 8, 9, 10, 11, 12, 13]]
    df.columns = ['time', 'duration', 'src_zone', 'dst_zone', 'status', 'sent', 'received', 'src_ip', 'dst_ip']
    df[['time', 'duration', 'src_zone', 'dst_zone', 'status', 'src_ip', 'dst_ip']] = df[['time', 'duration', 'src_zone', 'dst_zone',
            'status', 'src_ip', 'dst_ip']].applymap(get_values)
    df[['sent', 'received']] = df[['sent', 'received']].astype(float)
    df['total'] = df.sent + df.received
    return df

def df_groupby(df, group):
    return df.groupby(group)
    #result.to_sql('data', connect_sqlite(sqlitedb), index=False, if_exists='append')
    #return i

def process_group(grouped):
    temp_result = pd.DataFrame()
    for t, sz, dz, si, di in grouped.groups.keys():
        temp_result.append(t, sz, dz, si, di, len(grouped.get_group((t, sz, dz, si, di))),
                            np.sum(grouped.get_groups((t, sz, dz, si, di))), ignore_index=True)
    return temp_result

def connect_sqlite(db):
    return sqlalchemy.create_engine('sqlite:///'+db)

def main():

    if len(os.listdir(log_dir)) <= 0:
        sys.exit('There is no file in the log directory, program ended')

    print('Started at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))
    # loop for files in the directory
    for file in os.listdir(log_dir):
        file_format = check_file_format(file)
        if file_format == 'zip':
            log_file = hp.open_zip(os.path.join(log_dir, file))
        elif (file_format == 'txt'):
            df = read_log_file(file)
            grouped = df_groupby(df, ['time', 'src_zone', 'dst_zone', 'src_ip', 'dst_ip'])
            result = process_group(grouped)
            result.to_sql('data', connect_sqlite(sqlitedb), index=False, if_exists='append')
            nrow = read_log_file(os.path.join(log_dir, file))
        else:
            # skip this file if non txt and zip
            continue
        print('file ' + file + ' (' + str(nrow) + ' lines) is processed at ' + time.strftime('%Y-%m-%d %H:%M:%S'))


        # juniper['date'] = juniper['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # juniper['total_size'] = juniper['sent_size'] + juniper['received_size']
        #
        # temp_result = process_group(juniper)
        #
        # temp_result = pd.DataFrame(temp_result, columns=['date', 'src', 'dst', 'srczone', 'dstzone', 'count', 'total_size'])
        # temp_result['location'] = self.location
        #
        # temp_result.to_sql('data', self.connect_sqlite(), index=False, if_exists='append')
        # print (file + ' processed')
        #
        # if (file_format == 'zip'):
        #     os.rename(os.path.join(self.log_dir, file), os.path.join(self.zip_dir, file))
        # elif (file_format == 'txt'):
        #     hp.zip_file(self.zip_dir + file + '.zip', os.path.join(self.log_dir, file))
        #     os.remove(os.path.join(self.log_dir, file))

#os.remove(self.temp_csv)

    print('Completed at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

main()
