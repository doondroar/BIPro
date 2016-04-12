# -*- coding:UTF-8 -*- #

import subprocess
from com.utls.pro_env import HADOOP_PATH
from com.utls.exception import HadoopError

class HdfsUtil(object):
    def __init__(self):
        pass
    
    @staticmethod
    def execute_shell(fl):
        fl = fl.replace("\"","'")
        status, output = subprocess.getstatusoutput(HADOOP_PATH + "hdfs dfs -getmerge " + fl )
        if status != 0:
#            print("failed!")
            output = str(output).split("\n")            
            raise HadoopError(output)
        else:
            pass
#            print("success")    
#            return output