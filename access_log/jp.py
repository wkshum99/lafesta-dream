import pandas as pd

def juniper_log_cleansing(x):
    if x.find('=') > 0:
        return x[x.find('=')+1:]
    elif x.find(':') > 0:
        return x[:x.find(':')]    
    else:
        return x

def read_syslog_juniper(filename):
    
    #if (filename)[-3:] == '.gz':
    #    f = hp.open_gz(filename)
    #elif (filename)[-6:] == 'tar.gz':
    #    f = hp.open_tar_gz(filename)
    #else:
    #    f = hp.open_txt(filename)
    
    #f = f.split('\n')
    
    df = pd.read_table(filename, sep=' {1,}|"', engine='python', index_col=False, header=None, parse_dates=[[0, 1, 2], [9,10]])
    
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
    
    return df