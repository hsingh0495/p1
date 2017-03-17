#!/usr/bin/python

import argparse
import collections
import smtplib
import csv
import time
from glob import glob; from os.path import expanduser
from pyhive import presto
import re
import sys
import os
import logging
from datetime import date, timedelta
from insert_data_mongo import PublishThirdPartyPatterns

arg=sys.argv[1]


# path = '/home/users/hsingh/mongo/presto_query/output/'

def PrestoQuery(query):
  print(query)
  db = presto.connect('presto.vpn.insnw.net', 12800)
  # print("connection done")
  cursor = db.cursor()
  try:
    cursor.execute(query)
    print("query executed ",query)
    results = cursor.fetchall()
    # print("results are ", results)
    return results

  except:
    print("query cant be execute")  
    # print("query executed")
    cursor.close()
    

def WaitQueryExec():
  time.sleep(500000) 

def GetData(qtype,yesterday):
  how_to=1
  qtype=int(qtype)

  if(qtype==1):
    print("inside 1")
    query = "select http_status_code , count(*) from access_logs  where date=timestamp'{0}' and hourofday=hour(current_timestamp)-1  group by http_status_code order by count() desc ".format(yesterday)
    col_header=['date','http_status_code','count']
    label='http_status_code'
    # print("executing query",query)
    
    # print("query executed")
    data=PrestoQuery(query)
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")    
    # print data
    

  elif (qtype==2):
    query="SELECT count(request_url) as number_of_hits , (sum(LAST_MILE_BANDWIDTH_BYTES)*1.0)/1073741824 AS No_of_Bytes_in_GB FROM access_logs where date=timestamp'{0}' and hourofday=hour(current_timestamp)-1".format(yesterday)  
    col_header=['number_of_hits','No_of_Bytes_Saved_in_GB']
    label='number_of_hits'
    data=PrestoQuery(query)
    print(data)
    try:
      print("inside 2 is ")
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        print("obj for 2 is ",obj)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")

  elif (qtype==3):
    query="select content_type , count( content_type) as number_of_images FROM access_logs where date=timestamp '{0}' AND  hourofday=hour(current_timestamp)-1 AND content_type not LIKE '%text%' and hourofday=hour(current_timestamp)-1 AND content_type in ('image/vnd.ms-photo', 'image/webp', 'image/png', 'image/gif', 'image/jpg', 'image/jpeg', 'image/png', 'image/gif') and request_url like '%i10c=img.%' group by content_type".format(yesterday)
    col_header=['content_type','number_of_images']
    label='number_of_images'
    data=PrestoQuery(query)
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")

  elif (qtype==4):
    query="select content_type , count(*) , (sum(ORIGINAL_BANDWIDTH_BYTES)*1.0)/1073741824 , (sum(LAST_MILE_BANDWIDTH_BYTES)*1.0)/1073741824 , (sum(ORIGINAL_BANDWIDTH_BYTES-LAST_MILE_BANDWIDTH_BYTES)*1.0)/1073741824 AS No_of_Bytes_Saved_in_GB FROM access_logs WHERE ORIGINAL_BANDWIDTH_BYTES > LAST_MILE_BANDWIDTH_BYTES AND IMAGE_RESPONSE_TYPE IS NOT NULL AND CONTENT_TYPE not LIKE '%text%' AND content_type in ('image/vnd.ms-photo', 'image/webp', 'image/png', 'image/gif', 'image/jpg', 'image/jpeg', 'image/png', 'image/gif')  and cache_status = 'hit' and date =timestamp '{0}' and hourofday=hour(current_timestamp)-1 GROUP BY content_type".format(yesterday)
    col_header=['content_type','ORIGINAL_BANDWIDTH_BYTES','LAST_MILE_BANDWIDTH_BYTES','column3','number_of_bytes_saved_in_GB']
    label="LAST_MILE_BANDWIDTH_BYTES_SAVED"
    data=PrestoQuery(query) 
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")    
  elif (qtype==5):
    query="SELECT avg(time_to_first_byte_ms) as average_ttfb, avg(md_origin_timers_send_req_last_byte_to_recv_resp_first_byte_time_usec/1000) as average_origin_response,approx_percentile(time_to_first_byte_ms, 0.95) As ttfb_95th_percentile,approx_percentile(time_to_first_byte_ms, 0.5) As ttfb_median , approx_percentile(md_origin_timers_send_req_last_byte_to_recv_resp_first_byte_time_usec/1000, 0.5) As origin_response_median FROM access_logs WHERE   date = timestamp '{0}' AND hourofday=hour(current_timestamp)-1 AND request_type = 'GET' AND content_type = 'text/html' AND (NOT regexp_like(request_url, 'instart_disable_injection')) AND http_status_code = 200 AND md_html_streaming_action = 'ok'".format(yesterday)
    col_header=['average_ttfb','average_origin_response','ttfb_95th_percentile','ttfb_median','origin_response_median']
    label="average_ttfb"
    data=PrestoQuery(query) 
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")

  elif (qtype==6):
    query="select  count(1) as jss_optimize_requests from access_logs where date = timestamp '{0}' and hourofday=hour(current_timestamp)-1 and request_url like '%.js%' and jss_service_request = 'svc_request_profiling'".format(yesterday)
    col_header=['number_of_js_optimized']
    label="number_of_js_optimized"
    data=PrestoQuery(query) 
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted") 
    except:
      print("query didnt fetch any result")       
  elif (qtype==7):
    query="select approx_distinct(client_ip) from access_logs where date= timestamp '{0}' and hourofday=hour(current_timestamp)-1".format(yesterday)
    col_header=['Distince_IP_addresses']
    label="Distince_IP_addresses"
    data=PrestoQuery(query)
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")    

  elif (qtype==8):
    query="select content_type, count(content_type) as count_content_type, sum(last_mile_bandwidth_bytes) as sum_of_last_mile_bandwidth_bytes, sum(original_bandwidth_bytes) as sum_of_original_bandwidth_bytes from access_logs where date = timestamp'{0}' and hourofday=hour(current_timestamp)-1 group by content_type".format(yesterday)
    col_header=['content_type','count_content_type,sum_of_last_mile_bandwidth_bytes,sum_of_original_bandwidth_bytes']
    label="different_mime_types"
    data=PrestoQuery(query) 
    print(data)
    try:
      print("inside try")
      for i in range(len(data)):
        print("i is ",i)
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        print("object is ",obj)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")

  elif (qtype==9):
    query="select client_browser_name, client_device_os_name , approx_distinct(client_ip||client_user_agent) as count FROM access_logs WHERE client_device_type IS NOT NULL AND client_device_type NOT LIKE '%unknown%'   AND date = timestamp'{0}' and hourofday=hour(current_timestamp)-1 GROUP BY client_browser_name, client_device_os_name order by count desc".format(yesterday)

    col_header=['browser_name','device_os','count']
    label="different_devices"
    data=PrestoQuery(query) 
    try:
      for i in range(len(data)):
        obj=MakeJson(label,qtype,data[i],yesterday,col_header)
        publish.add_new_entry(obj)
        print("inserted")
    except:
      print("query didnt fetch any result")      
  # elif (qtype==10):
  #   query="select count(*),request_type from access_logs cross join unnest (cast(json_extract(regexp_replace(mod_security_info,',"tag":\[.*?\]'),'$.alerts') as array<map<varchar, varchar>>)) as x(n) where date=timestamp'{0}'  group by request_type order by count(*) desc limit 10".format(yesterday)
  #   col_header=['count']
  #   label="number of attacks by event"
      #   data=PrestoQuery(query) 

  #   for i in range(len(data)):
  #     obj=MakeJson(label,qtype,data[i],yesterday,col_header)
  #     publish.add_new_entry(obj)
  #     print("inserted")      




def MakeJson(label,qtype,data,date,column):
    dic={}
    print("inside json ")
    for j in range(len(data)):
      dic[column[j]]=data[j]

    dic['date']=date
    dic['query']=qtype
    dic['label']=label 

    # print(dic)
    return dic 

db_location='172.19.0.146'
db_port='27017'
db_name='Scott'
yesterday = date.today() - timedelta(1)
yesterday=yesterday.strftime('%Y-%m-%d')
publish =  PublishThirdPartyPatterns(db_location,db_port,db_name)
print("arg is ",arg)
print("calling the function")
GetData(arg,yesterday)













