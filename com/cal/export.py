# -*- coding:UTF-8 -*- #

import sys
import datetime
import xml.etree.ElementTree as ET
from com.utls.sqoop import SqoopUtil
from com.utls.hdfs import HdfsUtil
from com.utls.pro_env import PROJECT_CONF_DIR,WAREHOUSE_DB_DIR,PROJECT_TMP_DIR

def resolve_conf():
    conf_file = PROJECT_CONF_DIR + "Export.xml"
    xml_tree = ET.parse(conf_file)
    tasks = xml_tree.findall('./task')
    cmds = []
    
    for task in tasks:
        tables = task.findall('./table')
        for i in range(len(tables)):
            table_name = tables[i].text
            table_conf_file = PROJECT_CONF_DIR + table_name + ".xml"
            xmlTree = ET.parse(table_conf_file)
            sqoopNodes = xmlTree.find("./sqoop-shell")
            sqoop_cmd_type = sqoopNodes.attrib["type"]
            praNodes = sqoopNodes.findall("./param")
            cmap = {}
            for i in range(len(praNodes)):
                key = praNodes[i].attrib["key"]
                value = praNodes[i].text
                cmap[key] = value
            command = "sqoop " + sqoop_cmd_type
            for key in cmap.keys():
                value = cmap[key]
                if (value == None or value == "" or value == " "):
                    value = ""
                command += " --" + key + " " + value                  
            cmds.append(command)
    return cmds

def resolve_conf2(dt):
    conf_file = PROJECT_CONF_DIR + "Export.xml"
    dts = dt.strftime('%Y%m%d')
    xml_tree = ET.parse(conf_file)
    tasks = xml_tree.findall('./task')
    files = []
    
    for task in tasks:
        tables = task.findall('./table')
        for i in range(len(tables)):
            table_name = tables[i].text
            table_file = WAREHOUSE_DB_DIR + table_name + " " + PROJECT_TMP_DIR + table_name + "_" + dts + ".txt"
            files.append(table_file)
    return files

if __name__ == '__main__':
    dts = sys.argv[1]
    dt = datetime.datetime.strptime(dts,'%Y%m%d')

    cmds = resolve_conf()
    for i in range(len(cmds)):
        cmd = cmds[i]
        print(cmd)
#        SqoopUtil.execute_shell(cmd)

    files = resolve_conf2(dt)
    for i in range(len(files)):
        fl = files[i]
        print(fl)
#        HdfsUtil.execute_shell(fl)
