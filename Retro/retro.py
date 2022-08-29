from obspy import read
from obspy.signal.trigger import classic_sta_lta
from obspy.signal.trigger import plot_trigger
from obspy.signal.trigger import coincidence_trigger
from obspy.core.stream import Stream
from obspy.clients.fdsn import Client
from obspy.core import UTCDateTime
from pprint import pprint
import os

##################################################
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
###################################################
def isNew(eq_list,t,tass):
    t1=t-2*tass
    t2=t+2*tass
    seleq=list(filter(lambda x: x>=t1,eq_list))
    seleq=list(filter(lambda x: x<=t2, seleq))
    if len(seleq)>0:
        return False
    else:
        return True

###################################################
print("Start")

#параметры ассоциаторова и детектора
tass=0.8
nsta=3
sta=0.5
lta=10
trigon=3
trigoff=trigon*0.8

#временное окно, список создается, чтобы не перегружать разом всю память
t1=UTCDateTime("2022-08-20T00:00")
t2=UTCDateTime("2022-08-23T00:00")
delt=2*60*60
time_list=[]
while t2>t1:
    time_list.append(t1)
    t1=t1+delt

#параметры сети и станций, параметры фильтрации, BP - полосовой фильтр, RJ - режекторный
net="UK"
stalist=["S3*","S1B04"]
loc="00"
chan="DHZ"
stadict={"S3*":"BP(6,45,8)+RJ(8.1,8.5,8)+RJ(24,26,8)","S1B04":"BP(15,40,8)"}

with open('df.csv','w') as f:
    f.write("Time,StaList\n")

client=Client("http://0.0.0.0:8080")


print("select waveforms")
eq_list=[]
for ti in range(1,len(time_list)):
    print(time_list[ti])
    st=Stream()
    for stat in stalist:
        st2=client.get_waveforms(net,stat,loc,chan,time_list[ti-1],time_list[ti])
        st2.merge(fill_value='interpolate')
        st2=ComplexFilter(st2,stadict[stat])
        #st2=st2.filter('bandpass',freqmin=6,freqmax=45,corners=8,zerophase=True)
        st+=st2

    st.merge(fill_value='interpolate')
    print("trigger")
    res=[]
    for tr in st:
        try:
            st_tr=Stream(traces=[tr])
            trig = coincidence_trigger(trigger_type="recstalta", thr_on=trigon, thr_off=trigoff ,stream=st_tr,thr_coincidence_sum=1, sta=sta, lta=lta)
            with open('df.csv','a') as f:
                for i in trig:
                    t=i["time"]
                    t1=t-10
                    t2=t+20
                    res.append([t,",".join(i["trace_ids"])])
                    #f.write(f'{t.strftime("%Y-%m-%d %H:%M:%S.%f")},{",".join(i["stations"])}\n')
        except Exception as e:
            print(e)

    #res=res[:50]

    for i in res:
        t=i[0]
        t1=t-tass
        t2=t+tass
        selpick=list(filter(lambda x: x[0]<t2,res))
        selpick=list(filter(lambda x: x[0]>t1, selpick))
        stations=list(set([x[1] for x in selpick]))
        if len(stations)>=nsta:
            if isNew(eq_list,t,tass):
                eq_list.append(t)

                st=Stream()
                for stat in stalist:
                    t_start=t-10
                    t_end=t+20
                    st+=client.get_waveforms(net,stat,loc,chan,t_start,t_end)
                try:
                    os.makedirs(f"trig_retro/{t.year}/{t.month}/{t.day}")
                except:
                    pass

                with open(f"trig_retro/{t.year}/{t.month}/{t.day}/{t}.txt",'w') as f2:
                    f2.write("Sta\tTime\tms\n")
                    for tr in selpick:
                        f2.write(f"{tr[1]}\t{tr[0].strftime('%Y-%m-%d %H:%M:%S')}\t{tr[0].microsecond/1000}\n")

                st.write(f"trig_retro/{t.year}/{t.month}/{t.day}/{t}.msd",format="MSEED")  
                print(f"trig_retro/{t}.msd")       
 

print("end")
