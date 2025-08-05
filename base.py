from flask import *
from functools import wraps
from threading import Thread
from dateutil.relativedelta import relativedelta
import requests
import re
import os
from werkzeug.datastructures import ImmutableMultiDict
import jwt
import json
from datetime import datetime,timedelta
from bson.objectid import ObjectId
from bson.errors import InvalidId
from flask_cors import CORS
from sqlalchemy import create_engine, MetaData, Table, text, exc,DateTime
from sqlalchemy.orm import sessionmaker
import faker
import copy
import pandas as pd
import shutil
import json
import sys
import traceback
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
import seaborn as sns
import common.form as cform
from urllib.parse import quote
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import random
from dateutil import parser
import pytz
import numpy as np
import schedule
import time
import threading
import common.dataValidator as cdv
import common.config as ctm
import common.fileCreation as cfc
import common.mongo_operations as cmo
import common.aggregation as cagg
# import common.sql_query as csq
import common.mailer as cmailer
import common.zipCreation as czc
import common.graph as cgraph
import common.apiReq as apireq
import common.data_col_manipulate as cdcm
import common.fsocket as cfsocket
import common.mail_template as cmt
import staticData.staticProcedure as staticProcedure
import common.datacreator as cdc
import common.forms as cfm
import common.eventlogs as evl
from PIL import Image


env_file_name = ".env"
env_path = os.path.join(os.getcwd(),env_file_name)
load_dotenv(dotenv_path=env_path)



def get_adjusted_current_month():
    today = datetime.today()
    current_month = today.month
    current_year = today.year
    if today.day >= 26:
        if current_month==12:
            current_year=current_year+1
            current_month=1
        else:
            current_month =current_month+ 1  # February
    return current_month, current_year

# def get_previous_months_mongodb_query():
#     current_month, current_year = get_adjusted_current_month()
    
#     if current_month == 1:
#         months = [(11, current_year - 1), (12, current_year - 1), (1, current_year)]
#     elif current_month == 2:
#         months = [(12, current_year - 1), (1, current_year), (2, current_year)]
#     else:
#         months = [(current_month , current_year),(current_month - 1, current_year),(current_month - 2, current_year)]
#     query = {"$or": [{"month": m, "year": y} for m, y in months]}
#     print('queryqueryquery',query)
#     return query


def get_previous_months_mongodb_query():
    current_month, current_year = get_adjusted_current_month()
    
    if current_month == 1:
        months = [(12, current_year - 1)]
    elif current_month == 2:
        months = [ (1, current_year)]
    else:
        months = [(current_month - 1, current_year)]
    query = {"$or": [{"month": m, "year": y} for m, y in months]}
    return query



def get_previous_months_Arrey():
    current_month, current_year = get_adjusted_current_month()
    
    if current_month == 1:
        months = [1,12,11]
    elif current_month == 2:
        months =[12,1,2]
    else:
        months =[ i for i in range(current_month,current_month-3,-1)]
    
    return months
def del_itm(data,remover):

    for i in ["overall_table_count","sid"]:
        if(i in data):
            del data[i]

    for i in remover:
      
        if(i in data):
        
            del data[i]

    return data
    

def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization']
        if not token:
            return jsonify({'message': 'a valid token is missing'}),401
        try:
            data=jwt.decode(token.split("Bearer ")[1], algorithms='HS512', verify=True, key=os.environ.get("SECRET_KEY"))
            loginAggr=[
                {
                    '$lookup': {
                        'from': 'userRole', 
                        'localField': 'userRole', 
                        'foreignField': '_id', 
                        'as': 'userRole'
                    }
                }, {
                    '$unwind': {
                        'path': '$userRole', 
                        'preserveNullAndEmptyArrays': True
                    }
                },  {
                    '$match': {
                        '_id':ObjectId(data['uniqueid'])
                    }
                }, {
                    '$addFields': {
                        'roleName': '$userRole.roleName', 
                        'permission': '$userRole.permission', 
                        'roleId': {
                            '$toString': '$userRole._id'
                        }, 
                        'userUniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'CurrentBalance', 
                        'localField': '_id', 
                        'foreignField': 'addedFor', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }
                                }
                            }, {
                                '$project': {
                                    'Amount': 1, 
                                    '_id': 0
                                }
                            }
                        ], 
                        'as': 'CurrentBalance'
                    }
                }, {
                    '$unwind': {
                        'path': '$CurrentBalance', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'CurrentBalance': '$CurrentBalance.Amount'
                    }
                }, {
                    '$project': {
                        'userRole': 0, 
                        '_id': 0
                    }
                }
            ]
            userData=cmo.finding_aggregate("userRegister",loginAggr)
            current_data=userData["data"]
            if(len(current_data)==0):
                return jsonify({'message': 'token is invalid'}),401
            current_user=current_data[0]

        except jwt.ExpiredSignatureError:
            return jsonify({"msg": "Your Session is Expired."}), 401
        except jwt.exceptions.InvalidSignatureError:
            return jsonify({"msg": "Invalid Token"}), 401
        except Exception as e:
            return jsonify({'message': 'token is invalid'}),401
        return f(current_user, *args, **kwargs)
    return decorator



def respond(userRole,data=None,typeuser=None):
    if('data' in userRole):
        if("password" in userRole['data']):
            del userRole['data']['password']
    myResponse=0
    # print(data)
    # if(data=="Login"):
    #     if(userRole["state"]==4):
    #         if(len(userRole["data"])!=0):
    #             myResponse=200
    #             uniqueid=userRole["data"][0]["uniqueid"]
    #             expisre=datetime.utcnow() + timedelta(days=7)
    #             userRole["data"][0]["expiresIn"]="36000" #10 Hour session expire 

                
    #             roleName=userRole["data"][0]["rolename"]
    #             print(userRole,"userRole")
    #             cmo.updating(typeuser,{"_id":ObjectId(uniqueid)},{"isLogin":True,"expiresIn":"36000"},False)
    #             token = jwt.encode({'uniqueid': uniqueid, 'exp' : expisre},key=os.environ.get("SECRET_KEY"),algorithm="HS512")  
    #             # return jsonify({'token' : token.decode('UTF-8')})
    #             userRole["data"][0]["idToken"]=token
                
    #             print(userRole,"userRole")
    #             response = make_response()
    #             response.headers['Authorization'] = "Bearer "+token
    #             response.data = json.dumps(userRole["data"][0])
    #             print(response,"responseresponse")
    #             log_manager.response(response)

    #             print(response,roleName,"response")

    #             if(roleName=="Tower Crew"):
    #                 userFinal={
    #                     "state":2,
    #                     "msg":"You Have No Access to use Web Portal",
    #                     "data":[],
    #                 }
    #                 return jsonify(userFinal), 401
    #             return response
    #             return jsonify(token), myResponse

    #         else:
    #             userFinal={
    #                 "state":2,
    #                 "msg":"Please Use Valid Credentials",
    #                 "data":[],
    #             }
    #             return jsonify(userFinal), 401



    if(userRole["status"]==201):
        myResponse=201
    elif(userRole["status"]==400):
        myResponse=400
    elif(userRole["status"]==500):
        myResponse=500
    elif(userRole["status"]==404):
        myResponse=404
    elif(userRole["status"]==422):
        myResponse=422
    elif(userRole["status"]==200):
        myResponse=200
    elif(userRole["status"]==401):
        myResponse=401
    elif(userRole["status"]==1):
        myResponse=201
    elif(userRole["state"]==2):
        myResponse=409

    elif(userRole["state"]==3):
        myResponse=422
    elif(userRole["state"]==4):
        if(len(userRole["data"])!=0):
            myResponse=200
        else:
            myResponse=204
    elif(userRole["state"]==22):
        myResponse=401

    return jsonify(userRole), myResponse


complianceApproverstatus = {
    'user':"In Process  --> Submit",
    "L1":"Approve --> Reject",
    "L2":"Submit to Airtel --> Reject"
}