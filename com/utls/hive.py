# -*- coding:UTF-8 -*- #

import subprocess
from com.utls.pro_env import HIVE_PATH
from com.utls.exception import HiveError

class HiveUtil(object):
    def __init__(self):
        pass
    
    @staticmethod
    def execute_shell(hql):
        hql = hql.replace("\"","'")
        status, output = subprocess.getstatusoutput(HIVE_PATH + "hive -S -e \"" + hql + "\"")
        if status != 0:
#            print("failed!")
            output = str(output).split("\n")            
            raise HiveError(output)
        else:
            pass
#            print("success")    
#            return output