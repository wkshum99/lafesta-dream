import os

file = 'log' + os.sep + 'sgkd-ssg140.log.1.chunk001'


def get_values(s, labels):
    # get values based on a list of labels
    # input: a string to look for and a list of labels to match
    # output: a list, value is None if label not present
    result = []
    for l in labels:
        start = s.find(l)
        if start != -1:
            start += len(l)
            end = s[start:].find('=')
            if end != -1:
                end = start+s[start:start + end].rfind(' ')
                '''
                    special handling for some label names that span more than one word
                '''
                if l == 'src zone=':
                    result.append(s[start:end-4])
                elif l == 'start_time=':
                    temp = s[start:end-4]
                    result.append(temp[1:-6])
                else:
                    result.append(s[start:end])
            else:
                # if last labels, strip off the newline char
                result.append(s[start:-1])
        else:
            result.append(None)

    return result



with open(file, 'rb') as f:
    for i in range(100):
        line = f.readline()
        if 'system-notification-00257(traffic)' in line:
            #idx_1 = line.find('service=')
            #idx_2 = line.find(' proto=')
            #line = line[:idx_1]+line[idx_1:idx_2].replace(' ', '')+line[idx_2:]
            #print str(len(line.split())) + ' ' + line.strip()
            print get_values(line, ['start_time=', 'duration=', 'src zone=', 'dst zone=', 'src=', 'dst=', 'sent=', 'rcvd=', 'reason='])

#s = 'Dec  1 04:02:09 172.20.73.3 SG-SSG140: NetScreen device_id=SG-SSG140  [Root]system-notification-00257(traffic): start_time="2015-12-01 04:06:44" duration=62 policy_id=17 service=udp/port:3000 proto=17 src zone=AP_LAN dst zone=SG_LAN action=Permit sent=90 rcvd=0 src=172.20.72.19 dst=172.20.73.48 src_port=49188 dst_port=3000 src-xlated ip=172.20.72.19 port=49188 dst-xlated ip=172.20.73.48 port=3000 session_id=47320 reason=Close - AGE OUT'
#print get_values(s, ['start_time=', 'duration=', 'src zone=', 'dst zone=', 'src=', 'dst=', 'reason='])
#print get_values(s, ['reason='])

