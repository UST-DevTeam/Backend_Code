from base import *
from bson import ObjectId
from common.config import *
from datetime import datetime
import pytz
def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time



def siteEngineerLogs(id,new_data,useruniqueId,collection,type=""):

    # print(type,"type")
    if id is not None:
        # print("ObjectId(id)",id, ObjectId(id))
        if 'projectuniqueId' in new_data:
            projectId = new_data['projectuniqueId']
        else:
            projectId=""
        new_data=new_data['data']
        arr = [{"$match": {"_id": ObjectId(id)}}]
        old_data = cmo.finding_aggregate(collection, arr)
        # print(old_data['data'],"old_data['data']")
        if len(old_data['data']):
            old_data=old_data['data'][0]
            # print("old_data", old_data, "hhdhdhhd",)
            # print("new_data", new_data)
            UpdatedAt=current_time()
            UpdatedBy=useruniqueId
            updatedData=[]
            for key in new_data.keys():
                if key in old_data:   
                    if old_data[key] != new_data[key]:
                        if type =="Mile":
                            newAssignData = new_data['assignerId']
                            newAssignData = newAssignData.split(",")
                            arra = [
                                {
                                    '$addFields': {
                                        '_id': {
                                            '$toString': '$_id'
                                        }
                                    }
                                }, {
                                    '$match': {
                                        '_id': {
                                            '$in': newAssignData
                                        }
                                    }
                                }, {
                                    '$project': {
                                        'empName': 1, 
                                        '_id': 0
                                    }
                                }
                            ]
                            response = cmo.finding_aggregate("userRegister",arra)
                            response = response['data']

                            listEmp=[]
                            for i in response:
                                listEmp.append(i['empName'])
                            
                            updates_data=(f"Task to Re-allocate to '{','.join(listEmp)}'")
                            updatedData.append(updates_data)
                        else:
                            updates_data=(f"Data on key '{key}' has changed from '{old_data[key]}' to '{new_data[key]}'")
                            updatedData.append(updates_data)        
                else:
                    # print(f"New data found on key '{key}' with value '{new_data[key]}'")
                    print("")
            # print('updatedData',updatedData)        
            data={
                'UpdatedBy':UpdatedBy,
                'UpdatedAt':UpdatedAt,
                collection+'Id':id,
                'projectuniqueId':projectId,
                'updatedData':updatedData
            }
            response=cmo.insertion(collection+"EventLogs",data)
            # print('datatattata',data)
    else:
        new_data['addedBy']=useruniqueId
        new_data['addedAt']=current_time()
        response=cmo.insertion("SiteEngineerEventLogs",new_data)


def siteEngineerLogsDirect(id,new_data,useruniqueId,collection):
    if id is not None:
        # print("ObjectId(id)",id, ObjectId(id))
        # new_data=new_data['data']
        # arr = [{"$match": {"_id": ObjectId(id)}}]
        # old_data = cmo.finding_aggregate(collection, arr)['data'][0]
        # print("old_data", old_data, "hhdhdhhd", type(old_data))
        # print("new_data", new_data)
        UpdatedAt=current_time()
        UpdatedBy=useruniqueId
        updatedData=new_data
        # for key in new_data.keys():
        #     if key in old_data:   
        #         if old_data[key] != new_data[key]:
        #             print(f"Data on key '{key}' has changed from '{old_data[key]}' to '{new_data[key]}'")
        #             updates_data=(f"Data on key '{key}' has changed from '{old_data[key]}' to '{new_data[key]}'")
        #             updatedData.append(updates_data)        
        #     else:
        #         print(f"New data found on key '{key}' with value '{new_data[key]}'")
        
        # print('updatedData',updatedData)    
        data={
            'UpdatedBy':UpdatedBy,
            'UpdatedAt':UpdatedAt,
            collection+'Id':id,
            'updatedData':updatedData,
        }
        response=cmo.insertion(collection+"EventLogs",data)
        # print('datatattata',data)



def siteDeleteLogs(id,useruniqueId,projectuniqueId,collection):
    if id is not None:
        UpdatedAt=current_time()
        UpdatedBy=useruniqueId     
        data={
            'UpdatedBy':UpdatedBy,
            'UpdatedAt':UpdatedAt,
            collection+'Id':id,
            'updatedData':'Deleted',
            "projectuniqueId":projectuniqueId
        }
        response=cmo.insertion(collection+"EventLogs",data)

def siteBilledLog(id,useruniqueId,collection):
    if id is not None:
        UpdatedAt=current_time()
        UpdatedBy=useruniqueId 
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$project': {
                    'projectuniqueId': 1, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        projectuniqueId=response['data'][0]['projectuniqueId']
        data={
            'UpdatedBy':UpdatedBy,
            'UpdatedAt':UpdatedAt,
             collection+'Id':id,
            'updatedData':'Billed',
            "projectuniqueId":projectuniqueId
        }
        response=cmo.insertion(collection+"EventLogs",data)


def milestoneCloseLogs(id,useruniqueId,collection,msg):
    if id is not None:
        UpdatedAt=current_time()
        UpdatedBy=useruniqueId  
          
        data={
            'UpdatedBy':UpdatedBy,
            'UpdatedAt':UpdatedAt,
                collection+'Id':id,
            'updatedData':msg,
        }
        response=cmo.insertion(collection+"EventLogs",data)






        
def newSitewithmilestone(siteid,useruniqueId,projectuniqueId,collection,milestoneName):
    if id is not None:
        UpdatedAt=current_time()
        UpdatedBy=useruniqueId 
        data={
            'UpdatedBy':UpdatedBy,
            'UpdatedAt':UpdatedAt,
                collection+'Id':siteid,
            'updatedData':milestoneName+ " " + "Closed",
            'projectuniqueId':projectuniqueId,
            "milestoneName":milestoneName
        }
        response=cmo.insertion(collection+"EventLogs",data)
        
        
        
        
def newproject(projectuid,Name,useruniqueId,collection="projectEventlog"):
    UpdatedAt=current_time()
    UpdatedBy=useruniqueId 
    data={
        'UpdatedBy':UpdatedBy,
        'UpdatedAt':UpdatedAt,
        'updatedData':"New Project Created",
        'projectuid':projectuid,
        "Name":Name
    }
    response=cmo.insertion(collection,data)

def newSite(siteid,useruniqueId,projectuniqueId,Name,collection="projectEventlog"):
        UpdatedAt=current_time()
        UpdatedBy=useruniqueId 
        data={
            'UpdatedBy':UpdatedBy,
            'UpdatedAt':UpdatedAt,
            'updatedData':"New Site Created",
            'projectuid':projectuniqueId,
            "siteuid":siteid,
            "Name":Name
        }
        response=cmo.insertion(collection,data)
        
def newMileStone(milestoneId,useruniqueId,projectuniqueId,Name,siteid,msg,collection="projectEventlog"):
    UpdatedAt=current_time()
    UpdatedBy=useruniqueId  
    data={
        'UpdatedBy':UpdatedBy,
        'UpdatedAt':UpdatedAt,
        'updatedData':msg,
        'projectuid':projectuniqueId,
        "siteuid":siteid,
        "milestoneuid":milestoneId,
        "Name":Name
    }
    response=cmo.insertion(collection,data)

