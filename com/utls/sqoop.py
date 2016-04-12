# -*- coding:UTF-8 -*- #

import subprocess
from com.utls.pro_env import SQOOP_PATH
from com.utls.exception import SqoopError

class SqoopUtil(object):
    '''
    sqoop operation
    '''
    def __init__(self):
        pass
    
    @staticmethod
    def execute_shell(shell,sqoop_path=SQOOP_PATH):
        status, output = subprocess.getstatusoutput(sqoop_path + shell)
        if status != 0:
#            print("failed!")
            output = str(output).split("\n")
            raise SqoopError(output)
        else:
            pass
#            print("success")
#            return output