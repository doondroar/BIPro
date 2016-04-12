# -*- coding:UTF-8 -*- #

import time as tm
from datetime import datetime,time,date,timedelta
import xml.etree.ElementTree as ET
from com.utls.pro_env import PROJECT_CONF_DIR
from com.utls.sqoop import SqoopUtil

def resolve_conf(dt):
#    startstp = int(time.mktime(datetime.datetime.combine(dt, datetime.time.min).timetuple()))
#    endstp = int(time.mktime(datetime.datetime.combine(dt, datetime.time.max).timetuple()))
    startstp = int(tm.mktime(datetime.combine(dt, time.min).timetuple()))
    endstp = int(tm.mktime(datetime.combine(dt, time.max).timetuple()))
    dts = dt.strftime('%Y%m%d')
    conf_file = PROJECT_CONF_DIR + "Import.xml"
    xml_tree = ET.parse(conf_file)
    tasks = xml_tree.findall('./task')
    cmds = []
    for task in tasks:
        import_type = task.attrib["type"]
        if import_type == 'all' :
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
                command = "sqoop "+ sqoop_cmd_type
                for key in cmap.keys():
                    value = cmap[key]
                    if (value == None or value == "" or value == " "):
                        value = ""
                    command += " --" + key + " " + value
                cmds.append(command)
                
        elif import_type == 'add' :
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
                command = "sqoop "+ sqoop_cmd_type
                for key in cmap.keys():
                    value = cmap[key]
                    if (value == None or value == "" or value == " "):
                        value = ""
                    if (key == "connect"):
                        value = value.replace("$someday$", dts)
                    if (key == "query"):
                        value = value % (startstp,endstp)
                    if (key == "target-dir"):
                        value = value.replace("$someday$", dts)
                    command += " --" + key + " " + value
                cmds.append(command)
    return cmds            
      
if __name__ == '__main__':
#    dt = sys.argv[0]
    dt = date.today() + timedelta(days=-1)
    cmds = resolve_conf(dt)
    for i in range(len(cmds)):
        cmd = cmds[i]
        print(cmd)
#        SqoopUtil.execute_shell(cmd)  