# -*- coding:UTF-8 -*- #

import sys
import datetime
from com.utls.sqoop import SqoopUtil
from com.utls.hive import HiveUtil
from com.utls.hdfs import HdfsUtil
from com.utls.exception import HiveError,SqoopError,HadoopError
from com.utls.pro_env import PROJECT_LOG_DIR
from com.cal import impt
from com.cal import exe_hql
from com.cal import export
#from com.cal.impt import resolve_conf as impt_rf
#from com.cal.exe_hql import resolve_conf1 as exe_hql_rc1
#from com.cal.exe_hql import resolve_conf2 as exe_hql_rc2
#from com.cal.export import resolve_conf as expt_rc

if __name__ == '__main__':
    RUN_LOG_FILE = PROJECT_LOG_DIR + "run.log"
    HIVE_ERR_FILE = PROJECT_LOG_DIR + "hive.log"
    SQOOP_ERR_FILE = PROJECT_LOG_DIR + "sqoop.log"
    HDFS_ERR_FILE = PROJECT_LOG_DIR + "hdfs.log"
    dts = sys.argv[1]
    dt = datetime.datetime.strptime(dts,'%Y%m%d')
    try:
        cmds = exe_hql.resolve_conf1("create_tbl")
        for i in range(len(cmds)):
            cmd = cmds[i]
            HiveUtil.execute_shell(cmd)
            with open(RUN_LOG_FILE, 'a') as fd:
                fd.write("\" %s \" process is successful.\n" % (cmd))
          
        cmds = impt.resolve_conf(dt) 
        for i in range(len(cmds)):
            cmd = cmds[i]
            SqoopUtil.execute_shell(cmd)
            with open(RUN_LOG_FILE, 'a') as fd:
                fd.write("\" %s \" process is successful.\n" % (cmd))
                
        cmds = exe_hql.resolve_conf2("create_prt",dt)
        for i in range(len(cmds)):
            cmd = cmds[i]
            HiveUtil.execute_shell(cmd)
            with open(RUN_LOG_FILE, 'a') as fd:
                fd.write("\" %s \" process is successful.\n" % (cmd))
                
        cmds = exe_hql.resolve_conf2("etl_pro",dt)
        for i in range(len(cmds)):
            cmd = cmds[i]
            HiveUtil.execute_shell(cmd)
            with open(RUN_LOG_FILE, 'a') as fd:
                fd.write("\" %s \" process is successful.\n" % (cmd))
                
        cmds = export.resolve_conf() 
        for i in range(len(cmds)):
            cmd = cmds[i]
            SqoopUtil.execute_shell(cmd)
            with open(RUN_LOG_FILE, 'a') as fd:
                fd.write("\" %s \" process is successful.\n" % (cmd))
       
        fls = export.resolve_conf2(dt) 
        for i in range(len(fls)):
            fl = fls[i]
            HdfsUtil.execute_shell(fl)
            with open(RUN_LOG_FILE, 'a') as fd:
                fd.write("\" %s \" process is successful.\n" % (fl))
                
    except HiveError as err:
            print("Error has happened:",err)
            with open(HIVE_ERR_FILE, 'a') as fd:
                fd.write("Error has happened: %s \n" % err)
                
    except SqoopError as err:
            print("Error has happened:",err)
            with open(SQOOP_ERR_FILE, 'a') as fd:
                fd.write("Error has happened: %s \n" % err)
                
    except HadoopError as err:
            print("Error has happened:",err)
            with open(HDFS_ERR_FILE, 'a') as fd:
                fd.write("Error has happened: %s \n" % err)
