import pandas as pd
import datetime
import helper as hp

def read_squid_log(filename):
    #result = []
    if (filename)[-3:] == '.gz':
        filename = hp.open_gz(filename)
    #else:
    #    f = open_txt(filename)

    #f = f.split('\n')

    # file santization (remove duplicate space characters)
    #for row in f:
    #    if row != '':
    #        row = row.rstrip('\n')
    #        temp = remove_duplicate_spaces(row).split(' ')

            # convert first field back to datetime
    #        temp[0] = convert_ts(temp[0])
    #        result.append(temp)

    # convert list into pandas dataframe
    #df = pd.DataFrame(result)
    
    df = pd.read_table(filename, sep=' {1,}', engine='python', index_col=False, header=None, parse_dates=[0])
    
    # remove blank column
    df.drop(df[[7]], axis=1, inplace='TRUE')
    
    # rename columns
    df.columns = ['date', 'duration', 'client_address', 'result_code', 'bytes', 'method', 'URL', 'hierarchy', 'type']
    
    # convet unix time to date
    df['date'] = pd.to_datetime(df['date'],unit='s')    
    
    return df