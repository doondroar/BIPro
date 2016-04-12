'''
Created on 2016Äê3ÔÂ25ÈÕ

@author: Administrator
'''

import time
import re
import sys

COL_DELIMITER = '\x01';

def convertTime(str):
    # convert: 12/Feb/2014:03:17:50 +0800
    #      to: 2014-02-12 03:17:50
    #  [note]: timezone is not considered
    if str:
        return time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(str[:-6],'%d/%b/%Y:%H:%M:%S'))
    
def parseLog(inFile,outFile,dirtyFile):
    file = open(inFile)
    output = open(outFile,"w")
    dirty = open(dirtyFile,"w")
    items = [
        r' (?P<ip>\S+)',   # ip
        r'\S+',
        r' (?P<user>\S+)',
        r'\[(?P<time>.+)\]',
        r'"(?P<request>.*)"',
        r' (?P<status>[0-9]+) ',
        r' (?P<size>[0-9-]+)',
        r'"(?P<referer>.*)"',
        r'"(?P<agent>.*)"',
        r' (.*)',
    ]
    pattern = re.compile(r'\s+'.join(items)+r'\s*\Z')
    for line in file:
        m = pattern.match(line)
        if not m:
            dirty.write(line)
        else:
            dict = m.groupdict()
            dict["time"] = convertTime(dict["time"])
            if dict["size"] == "-":
                dict["size"] = "0"
            for key in dict:
                if dict[key] == "-":
                    dict[key] = ""
            output.write("%s\n" % (COL_DELIMITER.join(
                    (dict["ip"],
                     dict["user"],
                     dict["time"],
                     dict["request"],
                     dict["status"],
                     dict["size"],
                     dict["referer"],
                     dict["agent"] ))))
            output.close()
            dirty.close()
            file.close()
            
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print ("Usage: %s <input_file> <output_file> <dirty_file>" %sys.argv[0])
        sys.exit(1)
    parseLog(sys.argv[1],sys.argv[2],sys.argv[3])