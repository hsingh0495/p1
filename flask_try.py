from extract import getData
from flask import Flask, flash, redirect, render_template, request, session, abort
import os,sys
import json
import urllib2

query_type=sys.argv[1]
tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)
print(tmpl_dir)
print(app)

class Plot:
    def __init__(self,data):
        self.data=data

    def getExchangeRates(self):
        # A=getData(db_location,db_port,db_name,query_type)
        
        rdata = json.loads(y)
        return rdata
     
    @app.route("/")
    def index():
        # print "insde insdex\n",data
        rates = B.getExchangeRates()
        x=query_type
        print(type(rates[0]['http_status_code']),type(rates[0]['count_error']))
        print(rates)
        return render_template('test2.html',**locals())      
     
    @app.route("/hello")
    def hello():
        return "Hello World!"
     
 
if __name__ == "__main__":
    db_location='172.19.0.146'
    db_port='27017'
    db_name='Scott' 

    A=getData(db_location,db_port,db_name,query_type)
    data=A.get_entry()
    y=json.dumps(data)
    # print(y)
    B=Plot(y)
    
    app.run(debug=True)