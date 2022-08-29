#!/usr/bin/python3
#coding=utf-8

import os, sys, subprocess, traceback
import seiscomp.client
import mysql.connector as mysql
import timestring
import time
import datetime as dt
import configparser
from obspy.clients.seedlink.basic_client import Client
from obspy.core import UTCDateTime
from obspy.core import Trace
from obspy.core import Stream
from obspy.signal.trigger import coincidence_trigger
import numpy as np
from pprint import pprint


def ComplexFilter(st,filtstr):
    filt=filtstr.strip().split("+")
    nfilt=len(filt)
    type_filt=[]
    param_filt=[]
    for f in filt:
        type_filt.append(f[:2])
        param_filt.append(f[3:len(f)-1])
    for i in range(nfilt):
        params=param_filt[i].split(",")
        if "BP" in type_filt[i]:
            st.filter('bandpass', freqmin=float(params[0]), freqmax=float(params[1]),corners=int(params[2]))
        else:
            st.filter('bandstop', freqmin=float(params[0]), freqmax=float(params[1]),corners=int(params[2]))
    return st


def LogAppend(s,fil):
    with open(fil,'a') as f:
        dct=[]
        now=dt.datetime.now()
        frm='{} - {}\n'
        f.write(frm.format(now,s))


class StaLta():
    def __init__(self,sta=1,lta=10,trigon=3,trigoff=1.5,name="ARTI",filt="BP(1,50,8)"):
        self.sta=sta
        self.lta=lta
        self.trigon=trigon
        self.trigoff=trigoff
        self.station=name
        self.filt=filt

    def report(self):
        print("===========================")
        print("Station: %s" % self.station)
        print("STA, s: %s" %self.sta)
        print("LTA, s: %s" %self.lta)
        print("TrigOn: %s" %self.trigon)
        print("TrigOff: %s" %self.trigoff)
        print("Filt: %s" %self.filt)


class PickConfig():
    def __init__(self):
        self.host='localhost'
        self.user='sysop'
        self.password='pass'
        self.dbase='seiscomp'
        self.Sta=''
        self.ListSta=[]
        self.dt=2
        self.NSta=3
        self.code=""
        self.islist=0
        self.log=0
        self.logfile="1.log"
        self.obj=""

    def otchet(self):
        print("Config parametrs")
        print("Object: %s" % self.obj)
        print("host: %s" % self.host)
        print("user: %s" % self.user)
        print("password: %s" % self.password)
        print("DataBase: %s" % self.dbase)
        print("Code: %s" % self.code)
        print("ListMode: %s" % self.islist)
        if self.islist==1:
            print("List of st.: %s" % self.Sta)
        else:
            print("List of st.: -")
        print("NSta: %s" % self.NSta)
        print("DT, s: %s" % self.dt)
        #print("F1, Hz: %s" % self.f1)
        #print("F2, Hz: %s" % self.f2)
        #print("Order: %s" % self.order)
        print("Buffer, s: %s" % self.buffer)
        print("STALTA file: %s" % self.stafile)
        print("LogMode: %s" % self.log)
        print("LogFile: %s" % self.logfile)
        print("\n")

    def readini(self,pth):
        config=configparser.ConfigParser()
        config.read(pth)
        self.obj=config.get('Main','Object')
        self.host=config.get('Main','host')
        self.user=config.get('Main','user')
        self.password=config.get('Main','password')
        self.dbase=config.get('Main','dbase')
        self.code=config.get('Main','Code')
        self.NSta=int(config.get('Main','NSta'))
        self.dt=float(config.get('Main','dt'))
        self.Sta=config.get('Main','ListSta')
        self.islist=int(config.get('Main','ListMode'))
        self.log=int(config.get('Main','LogMode'))
        self.ListSta=self.Sta.split(',')
        self.logfile=config.get('Main','LogFile')
        #self.f1=float(config.get('Main','F1'))
        #self.f2=float(config.get('Main','F2'))
        self.buffer=int(config.get('Main','Buffer'))
        #self.order=int(config.get('Main','Order'))
        self.stafile=config.get('Main','STALTAfile')

class App(seiscomp.client.StreamApplication):

    def __init__(self, argc, argv):
        seiscomp.client.StreamApplication.__init__(self, argc, argv)
        # Do not connect to messaging and do not use database at all
        self.setMessagingEnabled(False)
        self.setDatabaseEnabled(False, False)

    def init(self):
        global Myconfig
        if seiscomp.client.StreamApplication.init(self) == False:
            return False

        # For testing purposes we subscribe to the last 5 minutes of data.
        # To use real-time data, do not define an end time and configure
        # a real-time capable backend such as Seedlink.

        # First, query now
        #now = seiscomp.core.Time.GMT()
        # Substract 5 minutes for the start time
        #start = now - seiscomp.core.TimeSpan(300,0)
        # Set the start time in our RecordStream
        #self.recordStream().setStartTime(start)
        # And the end time
        #self.recordStream().setEndTime(now)

        # Now add some streams to fetch
        #self.recordStream().addStream("BR", "B208", "", "EHZ")
        for i in Myconfig.ListSta:
            self.recordStream().addStream(str(Myconfig.code), str(i), "", "*")

        return True

    def handleRecord(self, rec):
        global myData
        global Myconfig
        global glob_station
        global stalta
        # Print the streamID which is a join of NSLC separated with '.'
        stat=rec.streamID().split(".")
        sta=stat[1]

        # Print the records start time in ISO format
        start=UTCDateTime(rec.startTime().iso())
        head={}
        head['sampling_rate']=rec.samplingFrequency()
        head['network']=stat[0]
        head['station']=stat[1]
        head['location']=stat[2]
        head['channel']=stat[3]
        head['starttime']=start
        if rec.data():
            # Try to extract a float array. If the samples are of other
            # data types, use rec.dataType() to query the type and use
            # the appropriate array classes.
            data = seiscomp.core.FloatArray.Cast(rec.data())
            if data:
                #заполняем буффер данными от сейскомпа
                try:
                    myData[sta].append(Trace(np.array([data.get(i) for i in range(data.size())]),header=head))
                    myData[sta].merge(fill_value="interpolate")
                except:
                    myData[sta]=Stream(Trace(np.array([data.get(i) for i in range(data.size())]),header=head))
                tr=myData[sta][0]
                stat=tr.stats
                #как только буффер заполнен запускаем пикер
                if (stat['endtime']-stat['starttime'])>Myconfig.buffer:
                     #детренд и фильтрация записи перед запуском пикера
                     try:
                         myData[sta].detrend()
                         myData[sta]=ComplexFilter(myData[sta],stalta[sta].filt)
                         #myData[sta].filter('bandpass', freqmin=stalta[sta].f1, freqmax=stalta[sta].f2,corners=stalta[sta].ord)
                     except:
                         msg=" ".join(map(str,["Filter error!",sta,stat['starttime'],stat['endtime'],len(myData[sta]),len(myData[sta][0].data)]))
                         print("Filter error!",sta,stat['starttime'],stat['endtime'])
                         LogAppend(msg,Myconfig.logfile)
                     trig = coincidence_trigger("recstalta", stalta[sta].trigon, stalta[sta].trigoff, myData[sta], 1, sta=stalta[sta].sta, lta=stalta[sta].lta)
                     if len(trig)>0:
                         for i in trig:
                             #подключаемся к БД и вносим информацию о найденных пиках
                             db = mysql.connect(host=Myconfig.host, user=Myconfig.user, passwd=Myconfig.password, database=Myconfig.dbase)
                             cur = db.cursor()
                             pick=seiscomp.datamodel.Pick.Create()
                             pick_time_str=str(i["time"].isoformat()).replace('T',' ')
                             now = str(seiscomp.core.Time.GMT())
                             insrt=[pick.publicID(),i["stations"][0],i["trace_ids"][0],pick_time_str,Myconfig.obj,now]
                             print(i["trace_ids"][0],i["time"].isoformat())
                             LogAppend(f'{i["trace_ids"][0]}, {i["time"].isoformat()}',Myconfig.logfile)
                             cur.execute("INSERT INTO MyPick(PublicID,Station,TraceID,time_uts,obj,time_upd) VALUES(%s,%s,%s,%s,%s,%s)",insrt)
                             db.commit()
                             # временное окно и запрос для выгрузки пиков
                             MyTime=i["time"].isoformat()
                             t_start=i["time"]-Myconfig.dt
                             t_end=i["time"]+Myconfig.dt
                             t_start=t_start.isoformat()
                             t_start_str=str(t_start).replace('Z','').replace('T',' ')
                             t_end_str=str(t_end).replace('Z','').replace('T',' ')
                             try:
                                 LogAppend(f"Select * From seiscomp.MyPick WHERE time_uts BETWEEN \"{t_start_str}\" AND \"{t_end_str}\";",Myconfig.logfile)
                                 #cur.execute(f"Select * From seiscomp.MyPick WHERE time_uts BETWEEN \"{t_start_str}\" AND \"{t_end_str}\"")
                                 cur.execute(f"Select * From seiscomp.MyPick WHERE time_uts>=\"{t_start_str}\" AND time_uts<=\"{t_end_str}\"")
                                 if Myconfig.log==1:
                                     LogAppend('Select is made success',Myconfig.logfile)
                             except Exception as e:
                                 LogAppend('Disconnect with DateBase - Timeout',Myconfig.logfile)
                                 LogAppend(str(e),Myconfig.logfile)
                             
                             rows = cur.fetchall()
                             pickID=[str(j[0].decode("utf-8")) for j in rows]
                             staID=[str(j[1].decode("utf-8")) for j in rows]
                             traceID=[str(j[2].decode("utf-8")) for j in rows]
                             stations=[]
                             #фильтр по станциям
                             for j in range(len(staID)):
                                 if Myconfig.islist==1:
                                     if staID[j] in Myconfig.ListSta:
                                         stations.append(traceID[j])
                             #количество уникальных станций в выборке
                             stations=list(set(stations))
                             if Myconfig.log==1:
                                 LogAppend('Uniqe stations in select: '+str(len(stations)),Myconfig.logfile)

                             if len(stations)>=Myconfig.NSta:
                                 if Myconfig.log==1:
                                     LogAppend('NSta - %s' % len(stations),Myconfig.logfile)
                                     LogAppend('List of stations: %s' % ','.join(stations),Myconfig.logfile)
                                 try:
                                     t_start_sel=UTCDateTime(t_start)-Myconfig.dt*100
                                     t_start_sel=str(t_start_sel).replace("T"," ").replace("Z","")
                                     t_end_sel=UTCDateTime(t_end)-Myconfig.dt*100
                                     t_end_sel=str(t_end_sel).replace("T"," ").replace("Z","")
                                
                                     cur.execute("SELECT PickID FROM MyOrigin WHERE time_uts>=%s AND time_uts<=%s",[t_start_sel,t_end_sel])
                                     fact_id=cur.fetchall()
                                     fact=[str(i[0]) for i in fact_id]
                                 except Exception as e:
                                     LogAppend(str(e),Myconfig.logfile)
                                 old_origin=False
                                 n=0
                                 #определяем новое ли это событие или старое
                                 
                                 for k in range(len(pickID)):
                                     for j in range(len(fact)):
                                         if fact[j].find(pickID[k])>-1:
                                             old_origin=True
                                             n=j
                                             break
                                 LogAppend("Pigon",Myconfig.logfile)
                                 if not old_origin:
                                     origin=seiscomp.datamodel.Origin.Create()
                                     Pid=','.join(pickID)

                                     for k in pickID:
                                         arrival=seiscomp.datamodel.Arrival()
                                         seiscomp.datamodel.Arrival.setPickID(arrival,k)
                                         origin.add(arrival)  
                                      
                                     print ("New Origin ID - %s" % origin.publicID())
                                     LogAppend("New Origin ID - %s" % origin.publicID(),Myconfig.logfile)
                                     if Myconfig.log==1:
                                         LogAppend('List of picks: %s' % Pid,Myconfig.logfile)
                                     #вносим в БД информацию о новом событии
                                     now = str(seiscomp.core.Time.GMT())
                                     try:
                                         insrt=[origin.publicID(),Pid,MyTime,now,Myconfig.obj]
                                         cur.execute("INSERT INTO MyOrigin(PublicID,PickID,time_uts,time_upd,obj) VALUES(%s,%s,%s,%s,%s)",insrt)
                                     except Exception as e:
                                         LogAppend(str(e),Myconfig.logfile)
                                 else:
                                     #обновляем информацию о старом событии
                                     #cur.execute("SELECT PublicID FROM MyOrigin")
                                     #fact_or=cur.fetchall()
                                     #fact_orig=[str(i[0]) for i in fact_or]
                                     LogAppend("Cat",Myconfig.logfile)
                                     t_start_sel=UTCDateTime(t_start)-Myconfig.dt*100
                                     t_start_sel=str(t_start_sel).replace("T"," ").replace("Z","")
                                     t_end_sel=UTCDateTime(t_end)-Myconfig.dt*100
                                     t_end_sel=str(t_end_sel).replace("T"," ").replace("Z","")
                                     cur.execute("SELECT PublicID,time_uts FROM MyOrigin WHERE time_uts>=%s AND time_uts<=%s",[t_start_sel,t_end_sel])
                                     fact_t=cur.fetchall()
                                     fact_orig=[str(i[0]) for i in fact_t]
                                     fact_time=[str(i[1]) for i in fact_t]
                                     LogAppend("Dog",Myconfig.logfile)
                                     t=fact_time[n]
                                     orig=fact_orig[n]
                                     Pid=','.join(pickID)
                                     now = str(seiscomp.core.Time.GMT())
                                     updt=[Pid,now,orig]
                                     orig_lst=[orig]
                                     frm=r"{}_{}.msd"
                                     cur.execute("UPDATE MyOrigin SET PickID=%s, time_upd=%s WHERE MyOrigin.PublicID=%s",updt)
                                     print("Update PickID list - %s" % orig_lst[0])
                                     LogAppend("Update PickID list - %s" % orig_lst[0],Myconfig.logfile)

                         #коммитим изменения в БД, в случе потери связи с БД переподключаемся
                         try:
                             db.commit()
                         except:
                             db.reconnect(attempts=3,delay=3)
                             db.commit()
                             LogAppend('Reconnect DB',Myconfig.logfile)

                         db.close()
                     #обнуляем буффер по обработанной станции
                     myData[sta]=Stream()
            else:
                print ("  no data")


def main():
    app = App(len(sys.argv), sys.argv)
    return app()

print("Start")

stalta={}
myData={}

#pth='config_sta.ini'

Myconfig=PickConfig()
Myconfig.readini(sys.argv[1])
#Myconfig.readini(pth)
Myconfig.otchet()

with open(Myconfig.stafile) as f:
    text=f.readlines()
    for i in text:
        try:
            s=i.strip().split('\t')
            param=list(map(float,s[:4]))
            stalta[s[4]]=StaLta(sta=param[0],lta=param[1],trigon=param[2],trigoff=param[3],name=s[4],filt=s[5])
            stalta[s[4]].report()
        except:
            pass


glob_station=[]

for i in Myconfig.ListSta:
    glob_station.append(str(Myconfig.code+"."+i+".*.*"))

with open(Myconfig.logfile,'w') as f:
    now=dt.datetime.now()
    f.write('%s Start\n' % now)


if __name__ == "__main__":
    sys.exit(main())
