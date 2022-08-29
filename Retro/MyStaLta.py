from obspy import read
from obspy.signal.trigger import classic_sta_lta
from obspy.signal.trigger import plot_trigger
from obspy.signal.trigger import coincidence_trigger
from obspy.core.stream import Stream
from obspy.clients.filesystem.sds import Client
from obspy.core import UTCDateTime
from pprint import pprint
import os

print("Start")

t1=UTCDateTime("2022-08-20T00:00")
t2=UTCDateTime("2022-08-23T00:00")
delt=2*60*60
time_list=[]

while t2>t1:
    time_list.append(t1)
    t1=t1+delt

sta=0.5
lta=10
trigon=3
trigoff=trigon*0.8
nsta=3
tass=0.8

stalist=["S3*","S1B04"]
stadict={"S3*":"BP(6,45,8)+RJ(8.1,8.5,8)+RJ(24,26,8)","S1B04":"BP(15,40,8)"}
net="UK"
chan="DHZ"
loc="00"

client=Client("/home/sysop/seiscomp/var/lib/archive/")

with open('res.txt','w') as f:
    f.write("Time\tNSta\tStaList\n")

####################################################
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
            st.filter('bandpass', freqmin=float(params[0]), freqmax=float(params[1]),corners=int(params[2]),zerophase=True)
        else:
            st.filter('bandstop', freqmin=float(params[0]), freqmax=float(params[1]),corners=int(params[2]),zerophase=True)
    return st

#####################################################


for i in range(1,len(time_list)):
    print(time_list[i])
    st=Stream()
    for stat in stalist:
        st2=client.get_waveforms(net,stat,loc,chan,time_list[i-1],time_list[i])
        st2.merge(fill_value='interpolate') 
        st2=ComplexFilter(st2,stadict[stat])
        st+=st2
    
    st.merge(fill_value='interpolate')
    trig = coincidence_trigger(trigger_type="classicstalta", thr_on=trigon, thr_off=trigoff ,stream=st,thr_coincidence_sum=nsta, sta=sta, lta=lta)
    
    with open('res.txt','a') as f:
        for j in trig:
            t=j["time"]
            try:
                d=t-t_last
                t_last=t
                if d<=2*tass:
                    continue
            except:
                t_last=t
            t1=t-10
            t2=t+20
            st2=Stream()
            for stat in stalist:
                st2+=client.get_waveforms(net,stat,loc,chan,t1,t2)
            try:
                os.makedirs(f"trig/{t.year}/{t.month}/{t.day}")
            except:
                pass
            st2.write(f"trig/{t.year}/{t.month}/{t.day}/{t}.msd",format="MSEED")

            #print(f'{t.strftime("%Y-%m-%d %H:%M:%S.%f")}\t{len(j["stations"])}\t{",".join(j["stations"])}')
            f.write(f'{t.strftime("%Y-%m-%d %H:%M:%S.%f")}\t{len(j["stations"])}\t{",".join(j["stations"])}\n')
            st2=Stream()
            for stat in stalist:
                st3=client.get_waveforms(net,stat,loc,chan,t1-lta,t2)
                st3.merge(fill_value='interpolate') 
                st3=ComplexFilter(st3,stadict[stat])
                st2+=st3
            res=[]
            with open(f"trig/{t.year}/{t.month}/{t.day}/{t}.txt",'w') as f2:
                f2.write("Sta\tTime\tms\n")
                for tr in st2:
                    st_tr=Stream(traces=[tr])
                    trig = coincidence_trigger(trigger_type="classicstalta", thr_on=trigon, thr_off=trigoff ,stream=st_tr,thr_coincidence_sum=1, sta=sta, lta=lta)
                    for tr in trig:
                        if abs(tr['time']-t)<=tass:
                            f2.write(f"{''.join(tr['trace_ids'])}\t{tr['time'].strftime('%Y-%m-%d %H:%M:%S')}\t{tr['time'].microsecond/1000}\n")


print("end")


