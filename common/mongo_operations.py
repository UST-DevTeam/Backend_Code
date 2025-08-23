import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pymongo import errors
import traceback
import bson
from bson import json_util
from jsonschema import validate
from jsonschema.exceptions import ValidationError
import json
from bson.objectid import ObjectId
from common.config import unique_timestamp,timestamp,mdy_timestamp
from datetime import datetime,timedelta,date

print("os.environ.get()", os.environ.get("MONGODB_DATABASE"))

env_file_name = ".env"
env_path = os.path.join(os.getcwd(),env_file_name)
load_dotenv(dotenv_path=env_path)
mongo_client = MongoClient(host=os.environ.get("MONGODB_HOST"),port=int(os.environ.get("MONGODB_PORT")),connect=bool(os.environ.get("MONGODB_connect")), )
mongo_client.server_info()
db = mongo_client[os.environ.get("MONGODB_DATABASE")]
status={"state":True,"msg":"","data":[]}

def timestamp():
    return '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now())


def status_log_adder(database,log_inset):

    log_inset["log_add_date"]=timestamp()
    log_inset["log_add_timestamp"]=unique_timestamp()
    log_insert=insertion("log_"+database,log_inset)
    
    return "ok"


def common_status_log_adder(database,logjson):
    logdata=finding(database,logjson)["data"][0]
    logdata["parent_id"]=logdata["_id"]["$oid"]
    logdata["currentTimeStampLog"]=timestamp()
    logdata["maindata"]=database
    del logdata["_id"]
    col=db["log_"+database]
    resp=col.insert_one(logdata)
    return logdata





def bulk_update(dbname,operations):
    
    status = {}
    try:
        col=db[dbname]
        resp=col.bulk_write(operations, ordered=False)
        status["status"]=200
        status["msg"]="Data Updated Successfully"
        status["icon"]="success"
        status["data"]=[]
        status['updateData'] = resp.modified_count
        
    except Exception as e:
        print(e)
        status["status"]=400
        status["data"]=[]
        status["icon"]="error"
        status["msg"]="Error"
     
    return status

# def find_one(dbName, sort, filter=None):
#     col = db[dbName]
#     if filter is None:
#         filter = {}
#     resp = col.find_one(filter, sort=sort)
#     return 

def find_one(dbName, sort, filter=None):
    col = db[dbName]
    if filter is None:
        filter = {}
    # resp = col.find(filter).sort(sort).limit(1)
    # return resp.next() if resp.alive else None
    resp = col.find_one(filter, sort=sort)
    return  resp


def insertion(dbname,data):
    status={}
    try:
        col=db[dbname]
        resp=col.insert_one(data)

        status["status"]=201
        status["operation_id"]=str(resp.inserted_id)
        status["data"]=[]
        status["icon"]="success"
        status["msg"]="Data Added Successfully"

    except errors.DuplicateKeyError as dke:

        col=db[dbname]
        dictdata=dke.details["keyValue"]
        dictdata["deleteStatus"]=1
        finsDatadps=list(col.find(dictdata))
        removeThis={
            "deleated_at":unique_timestamp(),
            "deleteStatus":1
        }
        if(len(finsDatadps)>0):
            resp=col.update_one(dke.details["keyValue"],{'$unset':removeThis})
            status["state"]=1
            status["operation_id"]=finsDatadps[0]["_id"]
            status["data"]=[]
            status["msg"]="Data Added Successfully"
        else:
            msg=dke.details["keyPattern"].keys()
            status["state"]=2
            status["data"]=[]
            status["msg"]=list(msg)[0]+" Already Exist"
    
    except errors.WriteError as we:
        # print(we.details)
        msg=""
        for i in we.details["errInfo"]["details"]["schemaRulesNotSatisfied"]:

            if("propertiesNotSatisfied" in i):
                for j in i["propertiesNotSatisfied"]:
                    for k in j["details"]:
                        msg=msg+str(j["description"])+","

            if("missingProperties" in i):
                for j in i["missingProperties"]:
                    msg=msg+str(j)+" is missing"+","
        
        status["state"]=3
        status["data"]=[]
        status["msg"]=msg

    return status



def checkAdvanceNo(lastNo):
    arr=[
        {
            '$match': {
                'AdvanceNo':lastNo,
                
            }
        }
    ]
    response=finding_aggregate_with_deleteStatus('Advance',arr)['data']
    if len(response):
        return True
    else:
        return False
def checkExpenseNo(lastNo):
    arr=[
        {
            '$match': {
                'ExpenseNo':lastNo,
            }
        }
    ]
    response=finding_aggregate_with_deleteStatus('Expenses',arr)['data']
    if len(response):
        
        return True
    else:
        return False
    
def checkSettlementID(lastNo):
    arr=[
        {
            '$match': {
                'SettlementID':lastNo,
            }
        }
    ]
    response=finding_aggregate_with_deleteStatus('Settlement',arr)['data']
    if len(response):
        
        return True
    else:
        return False

def insertion_all(dbname,data):

    status={}
    try:
        col=db[dbname]
        if(len(data)>0):
            resp=col.insert_many(data,ordered=False)
            print(resp)
            status["status"]=201
            status["data"]=[]
            status["msg"]="Data Added Successfully"
        else:
            status["status"]=400
            status["data"]=[]
            status["msg"]="Empty Data Not Allowed"
        

    except errors.DuplicateKeyError as dke:
        msg=dke.details["keyPattern"].keys()
        status["status"]=400
        status["data"]=[]
        status["msg"]=list(msg)[0]+" Already Exist"
    

    

    # except errors.BulkWriteError as bwe:
    #     msg=bwe.details["writeErrors"]
    #     print(msg)
    #     status["state"]=2
    #     status["data"]=[]
    #     status["msg"]=list(msg)[0]+" Already Exist"


    
    except errors.WriteError as we:

        print(we)
        msg=bwe.details["writeErrors"]
        print(msg)
        status["status"]=400
        status["data"]=[]
        status["msg"]=list(msg)[0]+" Already Exist"

    except errors.BulkWriteError as bwe:
        msg=bwe.details["writeErrors"]

        print(msg)
        col=db[dbname]
        gameChanger=0
        newFinalMsg=""
        for i in msg:
            print(i["keyValue"])
            refkeVal={}
            for jj in i["keyValue"]:
                refkeVal[jj]=i["keyValue"][jj]
            dictdata=refkeVal
            dictdata["deleteStatus"]=1
            finsDatadps=list(col.find(dictdata))
            print(finsDatadps,refkeVal,dictdata,"finsDatadps")
            removeThis={
                "deleated_at":unique_timestamp(),
                "deleteStatus":1
            }

            if(len(finsDatadps)>0):
                resp=col.update_one(i["keyValue"],{'$unset':removeThis})
                print(finsDatadps[0]["_id"])
                # status["state"]=1
                # status["operation_id"]=finsDatadps[0]["_id"]
                # status["data"]=[]
                # status["msg"]="Data Added Successfully"

            else:    
                for j in i["keyValue"]:
                    print(j,i["keyValue"][j],"""i["keyValue"]""")
                    gameChanger=1
                    newFinalMsg=newFinalMsg+j+" "+i["keyValue"][j]+" "

        if(gameChanger==1):
            status["status"]=400
            status["data"]=[]
            status["msg"]=newFinalMsg+" Already Exist"
        else:
            status["status"]=201
            status["data"]=[]
            status["msg"]="Data Added Successfully"
        


    except errors.WriteError as we:
        print(we.details)
        msg=""
        for i in we.details["errInfo"]["details"]["schemaRulesNotSatisfied"]:
            if("propertiesNotSatisfied" in i):
                for j in i["propertiesNotSatisfied"]:
                    for k in j["details"]:
                        msg=msg+str(j["description"])+","

            if("missingProperties" in i):
                for j in i["missingProperties"]:
                    msg=msg+str(j)+" is missing, "
        
        status["state"]=400
        status["data"]=[]
        status["msg"]=msg

    return status



def smart_insertion_all(dbname,data):
    
    status={}
    try:
        
        if(len(data)>0):
            col=db[dbname]
            resp=col.insert_many(data,ordered=False)
            print(resp)
            status["status"]=201
            status["data"]=[]
            status["msg"]="Data Added Successfully"
        else:
            status["status"]=400
            status["data"]=[]
            status["msg"]="Empty Data Not Allowed"

        

    except errors.DuplicateKeyError as dke:
        msg=dke.details["keyPattern"].keys()
        status["status"]=400
        status["data"]=[]
        status["msg"]=list(msg)[0]+" Already Exist"
    

    

    except errors.BulkWriteError as bwe:
        # msg=bwe.details["writeErrors"]
        # print(msg)
        # newFinalMsg=""
        # for i in msg:
        #     print(i["keyValue"])

        #     for j in i["keyValue"]:
        #         newFinalMsg=newFinalMsg+j+" "+i["keyValue"][j]+" "
        # status["state"]=2
        # status["data"]=[]
        # status["msg"]=newFinalMsg+" Already Exist"

        # print(dke.details["keyValue"])
        msg=bwe.details["writeErrors"]

        print(msg)
        col=db[dbname]
        gameChanger=0
        newFinalMsg=""
        for i in msg:
            print(i["keyValue"])
            refkeVal={}
            for jj in i["keyValue"]:
                refkeVal[jj]=i["keyValue"][jj]
            dictdata=refkeVal
            dictdata["deleteStatus"]=1
            finsDatadps=list(col.find(dictdata))
            print(finsDatadps,refkeVal,dictdata,"finsDatadps")
            removeThis={
                "deleated_at":unique_timestamp(),
                "deleteStatus":1
            }

            if(len(finsDatadps)>0):
                resp=col.update_one(i["keyValue"],{'$unset':removeThis})
                print(finsDatadps[0]["_id"])
                # status["state"]=1
                # status["operation_id"]=finsDatadps[0]["_id"]
                # status["data"]=[]
                # status["msg"]="Data Added Successfully"

            else:    
                for j in i["keyValue"]:
                    print(j,i["keyValue"][j],"""i["keyValue"]""")
                    gameChanger=1
                    newFinalMsg=newFinalMsg+j+" "+i["keyValue"][j]+" "

        if(gameChanger==1):
            status["status"]=400
            status["data"]=[]
            status["msg"]=newFinalMsg+" Already Exist"
        else:
            status["status"]=201
            status["data"]=[]
            status["msg"]="Data Added Successfully"
        
        # dictdata=bwe.details["writeErrors"]
        # dictdata["deleteStatus"]=1
        # finsDatadps=list(col.find(dictdata))
        # removeThis={
        #     "deleated_at":unique_timestamp(),
        #     "deleteStatus":1
        # }
        # if(len(finsDatadps)>0):
        #     resp=col.update_one(dke.details["keyValue"],{'$unset':removeThis})
        #     print(finsDatadps[0]["_id"])
        #     status["state"]=1
        #     status["operation_id"]=finsDatadps[0]["_id"]
        #     status["data"]=[]
        #     status["msg"]="Data Added Successfully"
        # else:
        #     print(finsDatadps,"finsDatadps")
        #     # if()
        #     msg=dke.details["keyPattern"].keys()
        #     print(msg)
        #     status["state"]=2
        #     status["data"]=[]
        #     status["msg"]=list(msg)[0]+" Already Exist"


    except errors.WriteError as we:
        print(we.details)
        msg=""
        for i in we.details["errInfo"]["details"]["schemaRulesNotSatisfied"]:
            if("propertiesNotSatisfied" in i):
                for j in i["propertiesNotSatisfied"]:
                    for k in j["details"]:
                        msg=msg+str(j["description"])+","

            if("missingProperties" in i):
                for j in i["missingProperties"]:
                    msg=msg+str(j)+" is missing, "
        
        status["status"]=400
        status["data"]=[]
        status["msg"]=msg

    return status





def finding(dbname,data,data1=None):
    
    status={}
    col=db[dbname]
    datafetch=[]

    
    if(data!=None):
        if(data1!=None):
            datafetch=list(col.find(data,data1))
        else:
            datafetch=list(col.find(data))

    else:
        
        if(data1!=None):
            datafetch=list(col.find(data1))
        else:
            datafetch=list(col.find())

    
    # print(datafetch)
    status["status"]=200
    status["msg"]="Data Get Successfully"
    status["data"]=json.loads(json_util.dumps(datafetch))

    
    return status


def object_id_validate(dbname,data):
    
    status={}
    try:
        print("object_id_validate",dbname,data)
        col=db[dbname]
        datafetch=list(col.find({"_id":ObjectId(data)}))
        if(len(datafetch)>0):
            return ("Data Exist",1)
        else:
            return ("ObjectId Not Exist",2)
    except Exception as e:
        error=str(e)
        return (error,3)


def get_object_id(dbname,data):
    
    status={}
    print("object_id_data",dbname,data)
    col=db[dbname]
    datafetch=list(col.find(data))
    status["status"]=200
    status["msg"]="Data Get Successfully"
    status["data"]=json.loads(json_util.dumps(datafetch))
    
    return status




def keyRemover(dbname,data,removeThis):
    
    status={}
    try:
        col=db[dbname]
        lookdata={"_id":ObjectId(data)}
        setdata={
            "deleated_at":unique_timestamp(),
            "deleteStatus":1
        }
        print(lookdata,{"$set":setdata})
        col.update_one(lookdata,{'$unset':removeThis})
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]
    except Exception as e:
        print(e)




def keyRemoverWithUpdate(dbname,lookdata,data,removeThis):
    
    try:
        status={}
        col=db[dbname]
        print(lookdata,{"$set":data},{'$unset':removeThis},"keyRemoverWithUpdate")
        col.update_one(lookdata,{"$set":data,'$unset':removeThis},True)
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]

        return status
    except Exception as e:
        print(e)


def object_id_data(dbname,data):
    print("object_id_data",dbname,data)
    col=db[dbname]
    datafetch=list(col.find({"_id":ObjectId(data)}))
    status={}
    status["status"]=200
    status["msg"]="Data Get Successfully"
    status["data"]=json.loads(json_util.dumps(datafetch))
    
    return status

def finding_aggregate(dbname,data):
    status={}
    col=db[dbname]
    datadelarr=[{"$match":{"deleteStatus":{"$ne":1}}}]
    datafetch=list(col.aggregate(datadelarr+data,allowDiskUse=True))
    
    status["status"]=200
    status["msg"]="Data Get Successfully"
    status["data"]=datafetch
    
    return status

def finding_aggregate_with_deleteStatus(dbname,data):
    status={}
    col=db[dbname]
    datafetch=list(col.aggregate(data))

    status["status"]=200
    status["msg"]="Data Get Successfully"
    status["data"]=datafetch
    
    return status


def finding_aggregate_no_all(dbname,data):
    status={}
    col=db[dbname]
    datadelarr=[{"$match":{"deleteStatus":{"$ne":1}}}]

    datafetch=list(col.aggregate(data+datadelarr))
    status["status"]=200
    status["msg"]="Data Get Successfully"
    status["data"]=json.loads(bson.json_util.dumps(datafetch))
    
    return status

    
def updating(dbname,lookdata,setdata,upse=False,unset={}):
    status={}
    try:
        col=db[dbname]
        resp=col.update_one(lookdata,{"$set":setdata,"$unset":unset},upsert=upse)
        status["status"]=200
        status["msg"]="Data Updated Successfully"
        status["icon"]="success"
        status["data"]=[]
        status['updateData'] = resp.modified_count

    except Exception as e:
        print(e)
        status["status"]=400
        status["data"]=[]
        status["icon"]="error"
        status["msg"]="Error"

    return status

def advancelookup(localkey,result,collection):
    arr=[{
        '$lookup': {
            'from': 'userRegister', 
            'localField': result, 
            'foreignField': '_id', 
            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
            'as': result
        }
    }, {
        '$unwind': {
            'path': "$"+result
        }
    }]
    return arr

# def updating_m(dbname,lookdata,setdata,upse=False):
#     status={}
#     try:
#         col=db[dbname]
#         resp=col.update_many(lookdata,{"$set":setdata},upsert=upse)
#         status['updateStatus']={"matched_count":resp.matched_count,"modified_count":resp.modified_count}
#         status["status"]=200
#         status["msg"]="Data Updated Successfully"
#         status["icon"]="success"
#         status["data"]=[]

#     except Exception as e:
#         print(e)
#         print(traceback.print_exc())
#         status["status"]=400
#         status["data"]=[]
#         status["icon"]="error"
#         status["msg"]="Error"


#     return status


def updating_m(dbname,lookdata,setdata,upse=False):
    status={}
    try:
        col=db[dbname]
        resp=col.update_many(lookdata,{"$set":setdata},upsert=upse)
        status['updateStatus']={"matched_count":resp.matched_count,"modified_count":resp.modified_count}
        status["status"]=200
        status["msg"]="Data Updated Successfully"
        status["icon"]="success"
        status["data"]=[]

    except Exception as e:
        print(e)
        print(traceback.print_exc())
        status["status"]=400
        status["data"]=[]
        status["icon"]="error"
        status["msg"]="Error"


    return status


# def olddeleting(dbname,id):
#     try:
#         col=db[dbname]
#         lookdata={"_id":ObjectId(id)}
#         setdata={
#             "deleated_at":unique_timestamp(),
#             "deleteStatus":1
#         }
#         print(lookdata,{"$set":setdata})
#         col.update_one(lookdata,{"$set":setdata},upsert=False)
#         status["state"]=1
#         status["msg"]="Data Deleted Successfully"
#         status["data"]=[]
#     except Exception as e:
#         print(e)
#     return status

# def deleting(dbname,id):
#     try:
#         col=db[dbname]
#         lookdata={"_id":ObjectId(id)}

#         all_data=list(col.find(lookdata))
#         print(all_data,"all_data")
#         # insertion_all("deleted_data",all_data)
          
#         # col.delete_one(lookdata)
        

#         col.update_one(lookdata,{"$set":setdata},upsert=False)
#         status["state"]=1
#         status["msg"]="Data Deleted Successfully"
#         status["data"]=[]
#     except Exception as e:
#         print(e)
#     return status


def deleting(dbname,id,userId=""):
    
    status={}
    try:
        col=db[dbname]
        lookdata={"_id":ObjectId(id)}
        setdata={
            "deleated_at":unique_timestamp(),
            "deleteStatus":1,
            "deleteByuserId":userId
        }
        col.update_one(lookdata,{"$set":setdata},upsert=False)
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]
    except Exception as e:
        print(e)
    return status

def deleting_m(dbname,lookdata,userId=""):
    
    status={}
    try:
        col=db[dbname]
        setdata={
            "deleated_at":unique_timestamp(),
            "deleteStatus":1,
            "deleteByuserId":userId
        }
        col.update_many(lookdata,{"$set":setdata},upsert=False)
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]
    except Exception as e:
        print(e)
    return status

# def real_deleting(dbname,id):
    
#     status={}
#     try:
#         col=db[dbname]
#         lookdata={"_id":ObjectId(id)}

#         data=finding(dbname,lookdata)["data"][0]
#         data["del_parent_id"]=data["_id"]["$oid"]
#         del data["_id"]

#         insertion("deleted_data_"+dbname,data)
        

#         col.delete_one(lookdata)
#         status["status"]=201
#         status["msg"]="Data Deleted Successfully"
#         status["data"]=[]
#     except Exception as e:
#         print(e)
#     return status


def real_deleting(dbname,lookdata):
    status={}
    try:
        col=db[dbname]
        # lookdata={"_id":ObjectId(id)}
        data=finding(dbname,lookdata)["data"][0]
        data["del_parent_id"]=data["_id"]["$oid"]
        del data["_id"]
        insertion("deleted_data_"+dbname,data)
        col.delete_one(lookdata)
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]
    except Exception as e:
        print(e)
    return status

def real_bulk_deleting(dbname,looker):
    
    status={}
    try:
        col=db[dbname]
        col.delete_many(looker)
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]
    except Exception as e:
        print(e)
    return status

def updating_more(dbname,lookdata,setdata,upse):
    
    col=db[dbname]
    resp=col.update_one(lookdata,{"$set":setdata},upsert=upse)
    return True


def updating_aller(dbname,lookdata,setdata,upse):
    
    col=db[dbname]
    resp=col.update_many(lookdata,{"$set":setdata},upsert=upse)
    return True



def finding_aggregate_without_code(dbname,data):
    status={}
    col=db[dbname]
    datadelarr=[{"$match":{"deleteStatus":{"$ne":1}}}]
    # data.append({"$match":{"deleteStatus":{"$ne":1}}})
    datafetch=list(col.aggregate(datadelarr+data))
    # print(datafetch)
    
    return datafetch



def finding_db_count(dbname):
    status={}
    col=db[dbname]
    datafetch=col.find().count()
    # print(datafetch)
    
    return datafetch
    

# print(object_id_validate("MasterData","63a940a17ecdc21cdf0cbb48"))

# print(object_id_data("MasterData","63a940a17ecdc21cdf0cbb48"))

def deleting_m2(dbname,lookdata,userId):
    status={}
    try:
        col=db[dbname]
        setdata={
            "deleated_at":unique_timestamp(),
            "deleteStatus":1,
            "deleteByuserId":str(userId)
        }
        col.update_many(lookdata,{"$set":setdata},upsert=False)
        status["status"]=201
        status["msg"]="Data Deleted Successfully"
        status["data"]=[]
    except Exception as e:
        print(e)
    return status

def insertion_many_Adv(dbnane, data):
    col = db[dbnane]
    resp = col.insert_many(data)
    return resp


def UpdateSiteEngColl():
    aggr=[
        {
            '$group': {
                '_id': {
                    '$toString': '$projectGroup'
                }, 
                'count': {
                    '$sum': 1
                }, 
                'projectId': {
                    '$addToSet': {
                        '$toString': '$_id'
                    }
                }
            }
        }
    ]
    fetchData = finding_aggregate("project",aggr)
    for item in fetchData['data']:
        updateStatus= updating_m("milestone",{"projectuniqueId":{"$in":item['projectId']}},{"projectGropupId":item['_id']})
        print(updateStatus['updateStatus'])
    else:
        print("++++++Completed+++++")
        
def UpdateMilestoneColl():
    aggr=[
        {
            '$group': {
                '_id': {
                    '$toString': '$circle'
                }, 
                'count': {
                    '$sum': 1
                }, 
                'projectId': {
                    '$addToSet': {
                        '$toString': '$_id'
                    }
                }
            }
        }
    ]
    fetchData = finding_aggregate("project",aggr)
    for item in fetchData['data']:
        updateStatus= updating_m("SiteEngineer",{"projectuniqueId":{"$in":item['projectId']}},{"circleId":item['_id']})
        print(updateStatus['updateStatus'])
    else:
        print("++++++Completed+++++")

        
# UpdateSiteEngColl()
#UpdateMilestoneColl()