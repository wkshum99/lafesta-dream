#import string
#import datetime
import pandas as pd
#import numpy as np
import gzip
import tarfile

def open_gz(filename):
    with gzip.open(filename, 'r') as f:
        file = f.read()
	return file

def open_tar_gz(filename):
    file = []
    with tarfile.open(filename, 'r:gz') as f:
        for i in f.getmembers():
            t= f.extractfile(i)
            file.append(t.read())
	return file

def open_txt(filename):
    with open(filename, 'r') as f:
        file= f.read()
	return file

def open_csv(filename, chunksize=20000):
    
    total_chunksize = 0
    
    df = pd.DataFrame()
    
    for c in pd.read_table(filename, sep=' {1,}|"', chunksize=chunksize, iterator=True, engine='python', index_col=False, header=None, parse_dates=[[0, 1, 2], [9,10]]):
        df = pd.concat([df, c])
        total_chunksize += chunksize
        
    print(str(total_chunksize) + " rows processed.")
    
    return df

#def remove_duplicate_spaces(word):
#    #return ''.join(ch for ch, _ in itertools.groupby(word))
#    return string.join(word.split())
    
#def convert_ts(timestamp):
#    time = datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
#    return time
