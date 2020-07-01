from prometheus_client import start_http_server, Gauge
import json
import requests
import time
import urllib
from urllib.request import urlopen
from bs4 import BeautifulSoup

def getFileSystemJson(zone):

       

    url = 'http://tpysydhdpm101.sl.bluecloud.ibm.com:50070/webhdfs/v1/tmp/dhasin29/KPI7/%s?op=GETCONTENTSUMMARY' % zone
    r = requests.get(url = url)
    
    jsonobj1 = r.json()
    #print(json.dumps(jsonobj1, indent = 4))
    
    return jsonobj1
    

def val():

    jsonobj = getClusterAppsJson()    
    for app in jsonobj['apps']['app']:
            if (app['state'] == 'RUNNING'and app['applicationType'] == 'SPARK') :

             urx =( app['trackingUrl'] + 'environment/')
#             print(urx)
#             print(app['trackingUI'])
             response = urllib.request.urlopen(urx)
             html_doc = response.read()
             soup = BeautifulSoup(html_doc, 'html.parser')
             try:
               value = soup.find("td", text="spark.dynamicAllocation.enabled").find_next_sibling("td").text
#               print('xxx',value)
               return value
             except AttributeError:
#               print('yyy', 'False')
               return 'False'

def getClusterAppsJson():
    url = 'http://tpysydhdpm201.sl.bluecloud.ibm.com:8088/ws/v1/cluster/apps'
    r = requests.get(url = url)
    jsonobj = r.json()
    

#    print(json.dumps(jsonobj, indent = 4))
    return jsonobj
    

def getClusterMetricsJson():
   url = 'http://tpysydhdpm201.sl.bluecloud.ibm.com:8088/ws/v1/cluster/metrics'
   r = requests.get(url = url)
   jsonobj = r.json()
   #print(json.dumps(jsonobj, indent = 4))
   return jsonobj

def getTotalVCores():
    jsonobj = getClusterMetricsJson()
    return jsonobj['clusterMetrics']['totalVirtualCores']

def jobAvgTime():
    jsonobj = getClusterAppsJson()
    job_avg_time = {}
    job_total_time = {}
    job_run_count = {}
    for app in jsonobj['apps']['app']:
        if app['name'] not in job_total_time:
            job_total_time[app['name']] = 0
            job_run_count[app['name']] = 0
            job_avg_time[app['name']] = 0
        if app['state'] == 'FINISHED':
            job_total_time[app['name']] += app['finishedTime'] - app['startedTime']
            job_run_count[app['name']] += 1
            if job_run_count[app['name']] >= 10:
                job_avg_time[app['name']] = job_total_time[app['name']]/job_run_count[app['name']]
    return job_avg_time


if __name__ == '__main__':
    start_http_server(4242)
    g = Gauge('job_memory_usage', 'Application Memory', ['queue', 'appname'])
    gx = Gauge('job_CPU_usage', 'Application CPU', ['queue', 'appname'])
    gxv = Gauge('job_apptype_usage', 'Application TYPE', ['queue','appname','apptype'])
    gxu = Gauge('job_executiontime_usage', 'Execution Time', ['queue','appname'])
    gxz = Gauge('job_executiontimehistory_usage', 'executiontimehistory Time', ['queue','appname'])
    go = Gauge('job_resourceallocationtype_usage', 'allocation Type', ['queue', 'appname','allocationType'])
    gc = Gauge('Space_Consumed' ,'Memory consumed', ['zone'])
    gs = Gauge('SVP_Space_Consumed' ,'Memory consumed', ['zone','SVP'])



    # Generate some requests.
    while True:
        jsonobj = getClusterAppsJson()
        total_vcores = getTotalVCores()
        job_avg_time = jobAvgTime()
        values = val()
        i  = ['CRZ','EZ','OFZ','RAW','TSZ']
        S ={ 'RAW': 'RAZAVI' , 'TSZ': 'CRAIG' , 'OFZ': 'WONG' , 'CRZ': 'GASTON' }
        for zone in (i):
          jsonobj1 =  getFileSystemJson(zone) 
 
          gc.labels(zone=zone).set(jsonobj1['ContentSummary']['spaceConsumed'])
          #print(jsonobj1['ContentSummary'])
        for zone in (S):
          jsonobj1 = getFileSystemJson(zone)
          gs.labels(SVP = S[zone] , zone=zone).set(jsonobj1['ContentSummary']['spaceConsumed'])
        
        outlist = []
        for app in jsonobj['apps']['app']:
            if (app['state'] == 'RUNNING'and app['allocatedMB'] >1):
                g.labels(queue=app['queue'], appname=app['name']).set(app['allocatedMB'])
                
            if (app['state'] == 'RUNNING'):
                gxv.labels(appname=app['name'],queue=app['queue'], apptype=app['applicationType']).set(app['allocatedMB'])
                
                
        
            if (app['state'] == 'RUNNING'):
                gxu.labels(queue=app['queue'], appname=app['name']).set(app['elapsedTime'])    
                

        
            if (app['state'] == 'RUNNING') :
                gxz.labels(queue=app['queue'], appname=app['name']).set(job_avg_time[app['name']])
               

        
            if (app['state'] == 'RUNNING' and app['applicationType'] == 'SPARK') :
              if values == "true":
                go.labels(queue=app['queue'], allocationType = 'Dynamic',appname=app['name']).set(app['allocatedMB'])
                print(app['name'])
                print('Dynamic')
                print(app['allocatedMB'])
                print(values)
              else:
                go.labels(queue=app['queue'], allocationType = 'Static',appname=app['name']).set(app['allocatedMB'])
                print(app['name'])
                print('Static')
                print(app['allocatedMB'])
                print(values)
                

       
            if (app['state'] == 'RUNNING' and ((app['allocatedVCores']/total_vcores) * 100) > 1):
                gx.labels(queue=app['queue'], appname=app['name']).set((app['allocatedVCores']/total_vcores) * 100)
            
               
        time.sleep(10)