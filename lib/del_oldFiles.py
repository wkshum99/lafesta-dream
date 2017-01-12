import os
import datetime
import sys

''' utilties for deleting old files after certain days of its last modified date
    correct syntax is del_oldFiles.py folderpath numberofdays'''

def delOldFiles(thisFile, daysDiff, daysKeep):
    if (daysDiff > daysKeep):
        try:
            os.unlink(thisFile)
            # print ('{0} is {1} old, deleted'.format(thisFile, daysDiff))
            print ('%s is %s old, deleted' % (thisFile, str(daysDiff)))
        except:
            #print ('{0} cannot be deleted'.format(thisFile))
            print ('%s cannot be deleted' % thisFile)
            print sys.exc_info()[0]
            pass

def enumFolder(inPath, daysKeep):

    now = datetime.datetime.now()

    #print ('Process started at: {0}'.format(now))
    print ('Process started at: %s' % now)

    for file in os.listdir(inPath):
        thisFile = os.path.join(inPath, file)
        if os.path.isfile(thisFile):
            lastModified = datetime.datetime.fromtimestamp(os.stat(thisFile).st_mtime)
            daysDiff = (now - lastModified).days
            delOldFiles(thisFile, daysDiff, daysKeep)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit('please make sure you specify the path, and number of days of files to keep')

    inPath = sys.argv[1]
    daysKeep = int(sys.argv[2])

    if not isinstance(daysKeep, int):
        sys.exit('please specify a number for the number of days of files to keep')

    enumFolder(inPath, daysKeep)