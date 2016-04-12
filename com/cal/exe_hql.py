# -*- coding:UTF-8 -*- #

import sys
import re
import datetime
from com.utls.pro_env import PROJECT_CONF_DIR
import xml.etree.ElementTree as ET
from com.utls.hive import HiveUtil

def resolve_conf1(type):
    conf_file = PROJECT_CONF_DIR + "HiveJob.xml"
    xml_tree = ET.parse(conf_file)
    jobs = xml_tree.findall('./Job')
    hqls = []
    
    for job in jobs:
        if job.attrib["type"] == type:
            for hql in job.getchildren():
                hql = hql.text.strip()
                if len(hql) == 0 or hql == "" or hql == None:
                    raise Exception('配置文件参数有误，程序终止运行！')
                hqls.append(hql)                  
    return hqls

def resolve_conf2(type,dt):
    conf_file = PROJECT_CONF_DIR + "HiveJob.xml"
    dts = dt.strftime('%Y%m%d')
    xml_tree = ET.parse(conf_file)
    jobs = xml_tree.findall('./Job')
    hqls = []
    
    for job in jobs:
        if job.attrib["type"] == type:
            for hql in job.getchildren():
                hql = hql.text.strip()
                if len(hql) == 0 or hql == "" or hql == None:
                    raise Exception('配置文件参数有误，程序终止运行！')
                elif re.search("\$someday\$",hql) :
                    hql = hql.replace("$someday$",dts)
                hqls.append(hql)
    return hqls

if __name__ == '__main__':
    pn = len(sys.argv)
    if pn == 2:
        hqls = resolve_conf1(sys.argv[1])
        for hql in hqls:
            print(hql)
#            HiveUtil.execute_shell(hql)
    elif pn == 3:
        dts = sys.argv[2]
        dt = datetime.datetime.strptime(dts,'%Y%m%d')
        hqls = resolve_conf2(sys.argv[1],dt)
        for hql in hqls:
            print(hql)
#            HiveUtil.execute_shell(hql)
    else:
        print("Wrong input! Please input: %s parameter1 or parameter2." % sys.argv[0])