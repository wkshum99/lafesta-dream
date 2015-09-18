import helper as hp

def process_juniper_line(line):
    result = []
    line = hp.remove_duplicate_spaces(line).split(' ')
    result.append(line[0] + ' ' + line[1] + ' ' + line[2])
    result.append(line[3])
    result.append(line[4] + ' ' + line[5])
    result.append(line[6])
    result.append(line[8] + ' ' + line[9])
    for i in range(10, 13):
        result.append(line[i])
    result.append(line[14] + ' ' + line[15])    
    result.append(line[16] + ' ' + line[17])
    for i in range(18, 25):
        result.append(line[i])
    result.append(line[26] + ' ' + line[27]) 
    result.append(line[28] + ' ' + line[29]) 
    for i in range(30, len(line) - 4):
        result.append(line[i])
    result.append(line[32] + ' ' + line[33] + ' ' + line[34] + ' ' + line[35])
    return result

def read_syslog_juniper(filename, filter=None):
    result = []
    
    if (filename)[-3:] == '.gz':
        f = hp.open_gz(filename)
    elif (filename)[-6:] == 'tar.gz':
        f = hp.open_tar_gz(filename)
    else:
        f = hp.open_txt(filename)

    f = f.split('\n')

    # file santization (remove duplicate space characters)
    for row in f:
        if row != '':
            row = row.rstrip('\n')
            temp = hp.remove_duplicate_spaces(row).split(' ')

            # convert first field back to datetime
            #temp[0] = convert_ts(temp[0])
            result.append(temp)
            
    f = ''
    
    # convert list into pandas dataframe
    df = pd.DataFrame(result)
    # remove blank column
    #df.drop(df[[7]], axis=1, inplace='TRUE')
    # rename columns
    #df.columns = ['time', 'duration', 'client_address', 'result_code', 'bytes', 'method', 'URL', 'hierarchy', 'type']
    
    return df