'''
This script takes data in json format and then puts that json data in to RUNNING MONGO DB
the structure of mongo file is 
{
"records":[output obtained from the presto query, i.e it is the merged csv data ], 
"timestamp":"the time at which the csv was merged(it is the day when query ran)",
"query":"it is a integer value to identify the query type"
}
'''


from pymongo import MongoClient
import time
import datetime
import sys,gflags
from urlparse import urlparse
import json,os
FLAGS = gflags.FLAGS


argument = sys.argv[1]
gflags.DEFINE_string('db_location', '172.19.0.146', 'IP address of the database location')
gflags.DEFINE_string('db_port', '27017', 'port on which database is listening, default value is 27017')
gflags.DEFINE_string('db_name', 'Scott', 'name of the database, default value is temp')
# json_path="/home/hsingh/Desktop/mongo/jsons/"+argument+"/"
USAGE_STRING = \
        """

        use below command to publish the data to default database
            `python publish2db.py`

        use below command to publish the data to desired database (prerequisite is to setup the mongodb on the machine)
            `python publish2db.py --db_location <DB IPADDRESS> --db_port <DB PORT> --db_name <DB NAME>`

        """

class PublishThirdPartyPatterns:
    def __init__(self, db_location, db_port, db_name):
        self.db_location = db_location
        self.db_port = db_port
        self.db_name = db_name

        self.client = MongoClient('mongodb://'+ self.db_location + ':' + self.db_port)
        
        self.db = self.client[self.db_name]
        self.collection = self.db.c2

    def add_new_entry(self,entry):
        # page=open(os.path.join(json_path,f_name),"r")
        # line = page.readline()
        # result=self.collection.insert_one(json.loads(line))
        print(entry)
        result=self.collection.insert_one(entry)
        print("done")

        # result = self.collection.insert_one(entry)
        return result.acknowledged

    

if __name__ == '__main__':
    try:
        sys.argv = FLAGS(sys.argv)
    except gflags.FlagsError, e:
        print('%s' % str(e))
        print USAGE_STRING
        sys.exit(1)

    #The below code to specify the example usage of the above module
    publish = PublishThirdPartyPatterns(FLAGS.db_location, FLAGS.db_port, FLAGS.db_name)
    if publish is None:
        sys.exit(1)
    else:
        add_new_entry()   
    
    # files=os.listdir(json_path)
    # # print(len(files))
    # # print(files)
    # for i in range(len(files)): 
    #     # print(i)   
    #     publish.add_new_entry(files[i])
