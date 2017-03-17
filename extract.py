from pymongo import MongoClient
import time
import datetime
import sys,gflags
from urlparse import urlparse
import json,os
import re
import sys

FLAGS = gflags.FLAGS
query_type=int(sys.argv[1])
gflags.DEFINE_string('query_type', query_type, 'type of the query')
gflags.DEFINE_string('db_location', '172.19.0.146', 'IP address of the database location')
gflags.DEFINE_string('db_port', '27017', 'port on which database is listening, default value is 27017')
gflags.DEFINE_string('db_name', 'Scott', 'name of the database, default value is temp')
json_path="/home/hsingh/Desktop/mongo/jsons/"
USAGE_STRING = \
        """

        use below command to publish the data to default database
            `python publish2db.py`

        use below command to publish the data to desired database (prerequisite is to setup the mongodb on the machine)
            `python publish2db.py --db_location <DB IPADDRESS> --db_port <DB PORT> --db_name <DB NAME>`

        """

class getData:
    def __init__(self, db_location, db_port, db_name,query_type):
        self.db_location = db_location
        self.db_port = db_port
        self.db_name = db_name

        self.client = MongoClient('mongodb://'+ self.db_location + ':' + self.db_port)
        
        self.db = self.client[self.db_name]
        self.collection = self.db.c2
        self.query_type= int(query_type)

    def get_entry(self):
        # i have to add the date from the user and then write pipe query again
        if (query_type==1):
            data=[]
            col_header=['count','http_status_code']
            pipe=[{'$match': { 'query': self.query_type }},{'$group':{'_id':{'date':"$date",'http_status_code':'$http_status_code'},'total':{'$sum':"$count"}}},{'$project' : { '_id':0,'date':'$_id.date','http_status_code': '$_id.http_status_code','count_error':'$total'}} ]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                    # print()
                    data.append(rows)
                    # print((rows))
                # print(data)   
                return data 
            except:
                print("couldnt fetch the data")
        elif (query_type==2):
            col_header=['number_of_request_url','total_number_of_hits']
            pipe=[{'$match': { 'query': self.query_type }},{'$group':{'_id':"$date",'total_number_of_hits':{'$sum':"$number_of_hits"},'total_number_of_urls':{'$sum':"$count_request_url"}}},{'$project' : { '_id':0,'date':'$_id','number_of_request_url':"$total_number_of_urls",'number_of_hits':'$total_number_of_hits'}} ]
            data=[]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                       # print()
                    data.append(rows)

                # print((data))
                return data
            except:
                print("couldnt fetch the data") 
        elif (query_type==3):
            col_header=['number_of_images','content_type']
            
            final=[]
            pipe=[{'$match':{ 'query': self.query_type }},{'$group':{'_id':{'date':'$date','content_type':"$content_type"},'total_number_of_images':{'$sum':"$number_of_images"}}},{'$project' : { '_id':0,'content_type':'$_id','number_of_images':'$total_number_of_images'}}]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                    # print(rows)
                    data={}
                    data['number_of_images']=rows['number_of_images']
                    data['content_type']=rows['content_type']['content_type']
                    data['date']=rows['content_type']['date'] 
                    # print(data)
                    final.append(data)   
                # print(final)
                return final
            except:
                print("couldnt fetch the data") 
        elif (query_type==4):
            col_header=['number_of_request_url','total_number_of_hits']
            pipe=[{'$match': { 'query': self.query_type }},{'$group':{'_id':{'date':"$date",'content_type':'$content_type'},'total_number_of_last_mile_bytes_saved_in_GB':{'$sum':"$LAST_MILE_BANDWIDTH_BYTES"}}},{'$project' : { '_id':0,'date':'$_id','total_bytes_saved':"$total_number_of_last_mile_bytes_saved_in_GB"}} ]
            
            final=[]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                    data={}
                    data['total_bytes_saved']=rows['total_bytes_saved']
                    data['date']=rows['date']['date']
                    data['content_type']=rows['date']['content_type']
                    # print(rows)
                    # data.append(rows)
                    final.append(data)

                print(final)
                # return data
            except:
                print("couldnt fetch the data")  
        elif (query_type==5):
            col_header=['average_origin_response','origin_response_median','average_ttfb','ttfb_median']
            pipe=[{'$match': { 'query': self.query_type }},{'$group':{'_id':{'date':"$date",'average_ttfb':'$average_ttfb','average_origin_response':'$average_origin_response','ttfb_95th_percentile':'$ttfb_95th_percentile'}}},{'$project' : { '_id':'$_id','average_origin_response':'$average_origin_response','ttfb_median':"$ttfb_median","ttfb_95th_percentile":"$ttfb_95th_percentile"}} ]
            
            final=[]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                    # print(rows)
                    data={}
                    data['date']=rows['_id']['date']
                    data['average_origin_response']=rows['_id']['average_origin_response']                    
                    data['average_ttfb']=rows['_id']['average_ttfb']
                    data['ttfb_95th_percentile']=rows['_id']['ttfb_95th_percentile']
                    # print(rows)
                    # data.append(rows)
                    final.append(data)

                print(final)   
                return final
            except:
                print("couldnt fetch the data")  
        elif (query_type==6):
            data=[]
            col_header=['date','number_of_js_optimized']
            pipe=[{'$match': { 'query': self.query_type }},{'$group':{'_id':"$date",'total':{'$sum':"$number_of_js_optimized"}}},{'$project' : { '_id':0,'date':"$_id",'total_js_optimized':'$total'}} ]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                    # print()
                    data.append(rows)
                    # print((rows))
                print(data)   
                return data 
            except:
                print("couldnt fetch the data")    
        elif (query_type==7):
            data=[]
            col_header=['date','distinct_ip_addresses']
            pipe=[{'$match': { 'query': self.query_type }},{'$group':{'_id':"$date",'total':{'$sum':"$distinct_ip_addresses"}}},{'$project' : { '_id':0,'date':"$_id",'total_distinct_ip_addresses':'$total'}} ]
            try:
                document=self.collection.aggregate(pipeline=pipe)
                for rows in document:
                    # print()
                    data.append(rows)
                    # print((rows))
                print(data)   
                return data 
            except:
                print("couldnt fetch the data")                                  

    
if __name__ == '__main__':
    try:
        sys.argv = FLAGS(sys.argv)
    except gflags.FlagsError, e:
        print('%s' % str(e))
        print USAGE_STRING
        sys.exit(1)

    #The below code to specify the example usage of the above module
    publish = getData(FLAGS.db_location, FLAGS.db_port, FLAGS.db_name,FLAGS.query_type)
    if publish is None:
        sys.exit(1)
    timestamp=""   
    db_location='172.19.0.146'
    db_port='27017'
    db_name='Scott' 
    A=getData(db_location,db_port,db_name,query_type)
    A.get_entry()