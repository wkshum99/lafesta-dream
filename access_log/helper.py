#import string
#import datetime
#import pandas as pd
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

#def remove_duplicate_spaces(word):
#    #return ''.join(ch for ch, _ in itertools.groupby(word))
#    return string.join(word.split())
    
#def convert_ts(timestamp):
#    time = datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
#    return time
