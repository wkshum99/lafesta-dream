import pandas as pd
import numpy as np
import helper as hp

def clean_juniper_file(infile, outfile):
    with open(infile, 'r') as in_file:
        with open(outfile, 'w') as out_file:
            for line in in_file:
                if ('system-notification-00257(traffic)' in line):                                                
                    idx_1 = line.find('service=')
                    idx_2 = line.find(' proto=')
                    if (idx_1 and idx_2):
                        out_file.write(line[:idx_1]+line[idx_1:idx_2].replace(' ', '')+line[idx_2:])
                        
def open_juniper_log(filename, chunksize=20000):
    
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
       
    df = open_juniper_log(filename)
    
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

def process_group(juniper):
    temp_result = []
    grouped = juniper.groupby(['date', 'src_address', 'dst_address', 'service', 'action'])
    for d, src, dst, srv, a in grouped.groups.keys():
        temp_result.append([d, src, dst, srv, len(juniper.iloc[grouped.groups[(d, src, dst, srv, a)]]), 
                            np.sum(juniper.iloc[grouped.groups[(d, src, dst, srv, a)]]).total_size, a])
    
    return temp_result