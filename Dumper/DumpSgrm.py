#!/usr/bin/env python
#coding=utf-8

import mysql.connector as mysql
import configparser
import os
from obspy.core import UTCDateTime
#from obspy.clients.arclink import Client
from obspy.core.stream import Stream
from obspy.clients.fdsn import Client


def CreateListFile(fil,ListSta,t1,t2):
    frm="{};{};{}\n"
    with open(fil,'w') as f:
        for i in ListSta:
            f.write(frm.format(t1.strftime("%Y-%m-%d %H:%M:%S"),t2.strftime("%Y-%m-%d %H:%M:%S"),i))

#создание каталога
def CreateCat(Archive,nw):
    year=str(nw.year)
    mon=str(nw.month)
    day=str(nw.day)
    os.chdir(Archive)
    try:
        os.mkdir(year)
    except:
        pass
    os.chdir(year)
    try:
        os.mkdir(mon)
    except:
        pass
    os.chdir(mon)
    try:
        os.mkdir(day)
    except:
        pass
    #os.chdir('/home/aleksei/dump/')

#класс для чтения конфига
class DumpConfig:
    def __init__(self):
        self.host=""
        self.user=""
        self.pwd=""
        self.dbase=""
        self.archost=""
        self.arcport=18001
        self.arcuser=""
        self.arch=""
        self.dt=15
        self.flist=""
        self.obj=""
        self.fdsn=False

    def readini(self,pth):
        config=configparser.ConfigParser()
        config.read(pth)
        self.obj=config.get('Main','Object')
        self.host=config.get('Main','host')
        self.user=config.get('Main','user')
        self.pwd=config.get('Main','password')
        self.dbase=config.get('Main','dbase')
        self.archost=config.get('Main','archost')
        self.arcport=int(config.get('Main','arcport'))
        self.arcuser=config.get('Main','arcuser')
        self.arch=config.get('Main','Archive')
        self.dt=int(config.get('Main','Archive_dt'))
        self.flist=config.get('Main','StaList')
        self.start_uts=UTCDateTime(config.get('Main','start'))
        self.end_uts=UTCDateTime(config.get('Main','end'))
        self.start=self.start_uts.isoformat()
        self.end=self.end_uts.isoformat()
        self.fdsn=bool(int(config.get('Main','FDSNMode')))
        self.listfile=config.get('Main','ListFile')

    def report(self):
        print("=====Config info======")
        print("Object: %s" % self.obj)
        print("Data base")
        print("Host: %s" % self.host)
        print("User: %s" % self.user)
        print("DBase: %s" % self.dbase)
        print("ArcLink")
        print("Host: %s" % self.archost)
        print("Port: %s" % self.arcport)
        print("User: %s" % self.arcuser)
        print("Archive")
        #print("Start: %s" % self.start)
        #print("End: %s" % self.end)
        print("Path: %s" % self.arch)
        print("DT, s: %s" % self.dt)
        print("Station info: %s" % self.flist)
        print("")

#класс для хранения информации по станциям
class Station:
    def __init__(self,args):
        self.net=args[0].strip()
        self.code=args[1].strip()
        self.lc=args[2].strip()
        self.ch=args[3].strip()

    def report(self):
        print("=====Station info======")
        print("Network: %s" % self.net)
        print("Code: %s" % self.code)
        print("Location: %s" % self.lc)    
        print("Channel: %s" % self.ch)
        print("")    

#чтение конфига и вывод информации по исходным данным
dump=DumpConfig()
dump.readini("dump.ini")
dump.report()

with open(dump.flist) as f:
    text=f.readlines()

StaList=[]
for i in text:
    try:
        stat=i.split(".")
        StaList.append(Station(stat))
    except:
        pass

for stat in StaList:
    stat.report()

#подключение к БД, запрос осуществляется к времени ОБНОВЛЕНИЯ информации о событии, а возвращает время события
db = mysql.connect(host=dump.host, user=dump.user, passwd=dump.pwd, database=dump.dbase)
cur = db.cursor()
t_start_str=str(dump.start).replace('Z','').replace('T',' ')
t_end_str=str(dump.end).replace('Z','').replace('T',' ')
#t_start_str=str(sys.argv[1]).replace('Z','').replace('T',' ')
#t_end_str=str(sys.argv[2]).replace('Z','').replace('T',' ')
cur.execute("Select time_uts From MyOrigin WHERE obj=%s AND time_upd BETWEEN %s AND %s",[dump.obj,t_start_str,t_end_str])
rows=cur.fetchall()

if dump.fdsn:
    #подключение к FDSN
    client = Client(f"http://{dump.archost}")
else:
    #подключаемся к ArcLink
    client=Client(host=dump.archost,port=dump.arcport,user=dump.arcuser)

prog_start=UTCDateTime.now()
#цикл по событиям из БД
for row in rows:
    t0=UTCDateTime(row[0])
    """
    try:
        if (t_last-t0)<=0.8:
            continue
    except:
        t_last=t0
    """
    t1=t0-dump.dt
    t2=t0+dump.dt
    st=Stream()
    station_for_list_file=[]
    frm="{}.{}.{}.{}"
    for i in StaList:
        station_for_list_file.append(frm.format(i.net,i.code,i.lc,i.ch))
    #цикл по станциям из списка
    suc=True
    for stat in StaList:
        try:
            sgrm = client.get_waveforms(network=stat.net, station=stat.code, location=stat.lc, channel=stat.ch, starttime=t1, endtime=t2)
            st+=sgrm
        except Exception as e:
            CreateListFile(dump.listfile,station_for_list_file,t1,t2)
            suc=False
            print(e)
            print("WARN!"+str(t0)+stat.code)

    #создание каталога и сохранение сейсмограмм в файл
    CreateCat(dump.arch,t0)
    frm='{}_{}.msd'
    fname=frm.format(t1.strftime('%Y%m%d-%H%M%S'),StaList[0].net)
    year=str(t0.year)
    mon=str(t0.month)
    day=str(t0.day)
    fname=dump.arch+'/'+year+'/'+mon+'/'+day+'/'+fname
    if not suc:
        cmd="/home/sysop/seiscomp/bin/seiscomp exec scart -dsvE -I 'sdsarchive:///home/sysop/seiscomp/var/lib/archive/' --list {} --stdout> {}"
        os.system(cmd.format(dump.listfile,fname))
    else:
        st.write(fname,format='MSEED')
    print("Success! %s" % str(t0))

db.close()

prog_end=UTCDateTime.now()
delt=prog_end-prog_start

print("Delt, sec %s" % str(delt))
print("Delt, min %s" % str(delt/60))
#print("Sec per record %s" % str(delt/len(rows)))
print("Number of records %s" % str(len(rows)))
