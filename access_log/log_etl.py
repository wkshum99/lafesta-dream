from __future__ import division
import pandas as pd
import numpy as np
import time
import sys
import os
import sqlalchemy

log_dir = 'E:'+os.sep+'temp'+os.sep+'log'+os.sep
sqlitedb = 'E:'+os.sep+'temp'+os.sep+'sg_access_log.db'
mode = 'full'
Gb = 1000000000

def check_file_format(file):
    if file[-4:] == '.zip':
        return 'zip'
    elif file[-4:] == 'r.gz':
        return 'gzip'
    else:
        return 'txt'

def get_values(x):
    # remove the label-of-next-field
    # input: string
    # output: string
    if x != None:
        if x[0] == '"':
            x = x[1:]
        x = str(x).split()
        return x[0]
    return x

def read_log_file(file):
    # read csv file into dataframe,
    # and grab only interesting columns
    # input: file
    # output: dataframe
    df = pd.read_table(file, sep='[a-z]*=', quotechar='"', header=None, engine='python')
    #df = df.iloc[:,[2, 3, 7, 8, 9, 10, 11, 12, 13]]
    df = df[pd.isnull(df[2])!=True][[2, 7, 8, 9, 10, 11, 12, 13]]
    df.columns = ['time', 'src_zone', 'dst_zone', 'status', 'sent', 'received', 'src_ip', 'dst_ip']
    df[['time', 'src_zone', 'dst_zone', 'status', 'src_ip', 'dst_ip']] = \
        df[['time', 'src_zone', 'dst_zone', 'status', 'src_ip', 'dst_ip']].applymap(get_values)
    df[['sent', 'received']] = df[['sent', 'received']].astype(float)
    df['total'] = df.sent + df.received
    return df

def df_groupby(df, group):
    return df.groupby(group, sort=False, as_index=False)

def process_group(grouped):
    #temp_result = pd.DataFrame()
    #for t, sz, dz, si, di in grouped.groups.keys():
    #    temp_result.append([t, sz, dz, si, di, grouped.get_group((t, sz, dz, si, di)).size()[0],
    #                        grouped.get_group((t, sz, dz, si, di))[['total']].sum()[0]], ignore_index=True)
    return grouped.agg([np.sum, np.size])

def connect_sqlite(db):
    return sqlalchemy.create_engine('sqlite:///'+db)

def main():

    if len(os.listdir(log_dir)) <= 0:
        sys.exit('There is no file in the log directory, program ended')

    print('Started at: ' + time.strftime('%Y-%m-%d %H:%M:%S'))
    # loop for files in the directory
    for file in os.listdir(log_dir):
        file_format = check_file_format(os.path.join(log_dir, file))
        if file_format == 'zip':
            log_file = hp.open_zip(os.path.join(log_dir, file))
        elif (file_format == 'txt'):
            df = read_log_file(os.path.join(log_dir, file))
            grouped = df_groupby(df, ['time', 'src_zone', 'dst_zone', 'src_ip', 'dst_ip'])
            grouped = process_group(grouped)
            grouped = grouped.reset_index()
            grouped[['time', 'src_zone', 'dst_zone', 'src_ip', 'dst_ip', 'total']].to_sql('data', connect_sqlite(sqlitedb), index=False, if_exists='replace')
            #nrow = read_log_file(os.path.join(log_dir, file))
        else:
            # skip this file if non txt and zip
            continue
        print('file ' + file + ' is processed at ' + time.strftime('%Y-%m-%d %H:%M:%S'))

main()