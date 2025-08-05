from base import *
from datetime import datetime
import pytz
from pymongo import UpdateOne
from common.config import check_and_convert_date as check_and_convert_date
from concurrent.futures import ThreadPoolExecutor

upgrade_blueprint = Blueprint("upgrade_blueprint", __name__)

def df_current_date():
    df_current_date = pd.to_datetime(datetime.now(tz=pytz.timezone("Asia/Kolkata")))
    current_date = df_current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    df_current_date = pd.to_datetime(current_date)
    return df_current_date

def current_date():
    utc_now = datetime.utcnow()
    ist_timezone = pytz.timezone("Asia/Kolkata")
    current_date = utc_now.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
    current_date_with_str = current_date.strftime("%d-%m-%Y")
    current_date = current_date.date()
    return current_date

def current_date_str():
    utc_now = datetime.utcnow()
    ist_timezone = pytz.timezone("Asia/Kolkata")
    current_date = utc_now.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
    current_date_with_str = current_date.strftime("%d-%m-%Y %H:%M:%S")
    return current_date_with_str

def my_function(row, userDatadp):
    dteq = []
    error = []
    for kk in row["assignerId"].split(","):
        datewq = userDatadp[userDatadp["assignerId"] == kk]
        if len(datewq) > 0:
            dteq.append(ObjectId(datewq["userUniqueId"].iloc[0]))
        else:
            error.append(kk)

    if len(error) > 0:
        return {"status": 400, "assignerId": error}
    row["assignerId"] = dteq
    row["status"] = 200
    return row

def create_unique_id(row):
    columns = row['value'].split(',')
    unique_id_parts = [str(row[col]) for col in columns]
    return '-'.join(unique_id_parts) 

def background_task_upgrade(dict_data,pathing):
    cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Upgrade Milestone Start On","typem":"old","time":current_date_str()})
    def update_chunk(chunk):
        bulk_updates = [] 
        bulk_sites = []
        for i in chunk:
            update_query1 = {  
                '_id':ObjectId(i['uid'])
            }
            update_query2 = {  
                '_id':ObjectId(i['siteId'])
            }
            if "CC_Completion Date" in dict_data[0]:
                update_data1 = {  
                    "$set": {
                        "Task Closure": current_date().strftime("%Y-%m-%d") + "T00:00:00",
                        "mileStoneStatus": "Closed",
                        'assignerId':i['assignerId'],
                        'CC_Completion Date':i['CC_Completion Date'],
                    }
                }
                if i['headerName']!=None:
                    update_data2 = {  
                        "$set": {
                            i['headerName']:i['CC_Completion Date']
                        }
                    }
            if "CC_Completion Date" not in dict_data[0] and "mileStoneStatus" in dict_data[0]:
                update_data1 = {  
                    "$set": {
                        "Task Closure": None,
                        'CC_Completion Date':None,
                        "mileStoneStatus": "Open",
                        'assignerId':i['assignerId'],
                    }
                }
                if i['headerName']!=None:
                    update_data2 = {  
                        "$set": {
                            i['headerName']: None,
                            'siteStatus' : "Open",
                            'Site_Completion Date':None,
                        }
                    }
            if "CC_Completion Date" not in dict_data[0] and "mileStoneStatus" not in dict_data[0]:
                update_data1 = {  
                    "$set": {
                        'assignerId':i['assignerId'],
                    }
                }
                
            bulk_updates.append(UpdateOne(update_query1, update_data1))
            if "CC_Completion Date"  in dict_data[0] or "mileStoneStatus"  in dict_data[0]:
                if i['headerName']!=None:
                    bulk_sites.append(UpdateOne(update_query2, update_data2,True))
        cmo.bulk_update("milestone",bulk_updates)
        if "CC_Completion Date"  in dict_data[0] or "mileStoneStatus"  in dict_data[0]:
            if len(bulk_sites):
                cmo.bulk_update("SiteEngineer",bulk_sites)
                
    chunk_size = 5000
    chunks = [dict_data[i:i + chunk_size] for i in range(0, len(dict_data), chunk_size)]
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(update_chunk, chunks)
    cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Upgrade Milestone Complete","typem":"old","time":current_date_str()})

def background_old_task_upgrade(dict_data,pathing):
    cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Upgrade Old Milestone Start On","typem":"old","time":current_date_str()})
    def update_chunk(chunk):
        bulk_updates = [] 
        for i in chunk:
            update_query = {
                '_id':ObjectId(i['uid'])
            }
            update_data = {'$set':{}}
            if "Completion Criteria" in i:
                update_data['$set']['Completion Criteria'] = i['Completion Criteria']
            if "Predecessor" in i:
                update_data['$set']['Predecessor'] = i['Predecessor']
            bulk_updates.append(UpdateOne(update_query, update_data))
        cmo.bulk_update("milestone",bulk_updates)    
    chunk_size = 5000
    chunks = [dict_data[i:i + chunk_size] for i in range(0, len(dict_data), chunk_size)]
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(update_chunk, chunks)
    cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Upgrade Old Milestone Complete","typem":"old","time":current_date_str()})

def background_site_upgrade(dict_data,pathing):
    cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Site Upgrade Start On","typem":"old","time":current_date_str()})
    def update_chunk(chunk):
        bulk_updates = []
        for i in chunk:
            update_query = {
                '_id':ObjectId(i['uid'])
            }
            i.pop('uid', None)
            update_data = {
                '$set':i
            }
            bulk_updates.append(UpdateOne(update_query, update_data))
        cmo.bulk_update("SiteEngineer",bulk_updates)    
    chunk_size = 5000
    chunks = [dict_data[i:i + chunk_size] for i in range(0, len(dict_data), chunk_size)]
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(update_chunk, chunks)
    cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Site Upgrade Complete","typem":"old","time":current_date_str()})    


@upgrade_blueprint.route("/commonUpdate", methods=["GET", "POST"])
@upgrade_blueprint.route("/commonUpdate/<id>", methods=["GET", "POST"])
@token_required
def site_update(current_user,id=None):
    if request.method == "POST":

        uploadedFile = request.files.get("uploadedFile[]")
        fileType = request.form.get("fileType")

        allData = {}
        supportFile = ["xlsx", "xls"]
        pathing = cform.singleFileSaver(uploadedFile, "", supportFile)
        if pathing["status"] == 200:
            allData["filePath"] = pathing["msg"]
        elif pathing["status"] == 422:
            return respond(pathing)

        fileTeamCheck = {
            "updateSite": {
                "validate": cdcm.validate_site_upload,
                "rename": cdcm.rename_site_upload,
            },
            "updateMilestone": {
                "validate": cdcm.validate_task_upload,
                "rename": cdcm.rename_task_upload,
            },
            "updateSiteOneProject": {
                "validate": cdcm.validate_site_upload,
                "rename": cdcm.rename_site_upload,
            },
            "updateMilestoneOneProject": {
                "validate": cdcm.validate_task_upload,
                "rename": cdcm.rename_task_upload,
            },
            "updateOldMilestone": {
                "validate": cdcm.validate_old_task,
                "rename": cdcm.rename_old_task,
            },
        }
        if fileType in fileTeamCheck:

            excel_file_path = os.path.join(os.getcwd(), allData["filePath"])
            rename = fileTeamCheck[fileType]["rename"]
            validate = fileTeamCheck[fileType]["validate"]
            data = cfc.exceltodfnoval(excel_file_path, rename, validate)
            if data["status"] != 400:
                exceldata = data["data"]

                if exceldata.empty:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "The uploaded Excel file is empty. Please check the file and try again.",
                        }
                    )

                if fileType == "updateSite":
                    if "Unique ID" in exceldata.columns:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":'Please remove "Unique ID" header from your excel file'
                        })
                    if "RFAI Date" in exceldata.columns:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":'Please remove "RFAI Date" header from your excel file'
                        })
                    dbData = [
                        {
                            '$match':{
                                'deleteStatus':{"$ne":1}
                            }
                        }, {
                            "$project": {
                                "systemId": 1,
                                "uid": {"$toString": "$_id"},
                                "SubProjectId":1,
                                "_id": 0,
                            }
                        }
                    ]
                    systemId = cmo.finding_aggregate("SiteEngineer", dbData)
                    systemIddf = pd.DataFrame.from_dict(systemId["data"])
                    systemIdmerged = exceldata.merge(systemIddf, on=["systemId"], how="left")
                    cresult2 = systemIdmerged[systemIdmerged["uid"].isna()]
                    unique_c = cresult2["systemId"].unique()
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "System Id is not found in DataBase " + ", ".join(unique_c),
                            }
                        )
                    else:
                        exceldata['uid'] = systemIdmerged['uid']
                        exceldata['SubProjectId'] = systemIdmerged['SubProjectId']
                        subprojectObject = []
                        unique_sub_projects = exceldata['SubProjectId'].unique()
                        for i in unique_sub_projects:
                            subprojectObject.append(ObjectId(i))
                        arra = [{"$match": {"_id":{'$in': subprojectObject}}}]
                        response = cmo.finding_aggregate("projectType", arra)["data"]
                        tempData = ['systemId', 'uid', 'SubProjectId',"siteStatus"]
                        temp_Date_Data = []
                        for response in response:
                            if 't_sengg' in response:
                                for i in response['t_sengg']:
                                    if i['dataType'] == "Date":
                                        if i["fieldName"] not in temp_Date_Data:
                                            temp_Date_Data.append(i['fieldName'])
                                    if i['fieldName'] not in tempData:
                                        tempData.append(i['fieldName'])  
                            if 't_tracking' in response:
                                for i in response['t_tracking']:
                                    if i['dataType'] == "Date":
                                        if i["fieldName"] not in temp_Date_Data:
                                            temp_Date_Data.append(i['fieldName'])
                                    if i['fieldName'] not in tempData:
                                        tempData.append(i['fieldName'])   
                            if 't_issues' in response:
                                for i in response['t_issues']:
                                    if i['dataType'] == "Date":
                                        if i["fieldName"] not in temp_Date_Data:
                                            temp_Date_Data.append(i['fieldName'])
                                    if i['fieldName'] not in tempData:
                                        tempData.append(i['fieldName'])
                        notfoundColumn = []
                        column = exceldata.columns
                        for i in column:
                            if i not in tempData:
                                notfoundColumn.append(i)
                        if  notfoundColumn:
                            return respond({
                                'status':400,
                                "icon":"error",
                                "msg":f'This Column is not found in Template,Please remove this header:- {notfoundColumn}'
                            })
                        dateField = []
                        for i in column:
                            if i in temp_Date_Data:
                               dateField.append(i) 
                        
                        if dateField:
                            invalidDate = []
                            expected_format = '%d-%m-%Y'
                            for i in dateField:
                                for index, row in exceldata.iterrows():
                                    if pd.isna(row[i]) or row[i] == '':
                                        row[i] = None
                                    else:
                                        try:
                                            row[i] = pd.to_datetime(row[i], format=expected_format, errors='raise')
                                        except Exception as e:
                                            invalidDate.append(index+2)
                            if invalidDate:
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f'These rows do not contain Valid Date Format - {invalidDate}'
                                })
                        fieldName = []
                        arra = [{"$match": {"_id":{'$in': subprojectObject}}}]
                        response = cmo.finding_aggregate("projectType", arra)["data"]
                        for i in response:
                            data = i['t_sengg']
                            unique_id_object = next((item for item in data if item["fieldName"] == "Unique ID"),None)
                            if unique_id_object:
                                if unique_id_object["dataType"] == "Auto Created":
                                    keys = unique_id_object["dropdownValue"].split(",")
                                    for i in keys:
                                        if i not in fieldName:
                                            fieldName.append(i)
                        common_columns = [col for col in exceldata.columns if col in fieldName]
                        if common_columns:
                            columns_presence = all(col in exceldata.columns for col in fieldName)
                            if columns_presence == False:
                                return respond({
                                    'status':400,
                                    "msg":f"These Column {fieldName} is must Required for Unique ID combination",
                                    "icon":"error"
                                })
                            any_empty_cells = exceldata[fieldName].isna().any().any()
                            if any_empty_cells:
                                return respond({
                                    "status":400,
                                    "icon":"error",
                                    "msg":f"These column {fieldName} contains Empty or None Value.Please check the file"
                                })
                            arra = [
                                {
                                    '$match':{
                                        'deleteStatus':{'$ne':1}
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$t_sengg', 
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$addFields': {
                                        'newField': '$t_sengg.fieldName'
                                    }
                                }, {
                                    '$match': {
                                        'newField': 'Unique ID'
                                    }
                                }, {
                                    '$project': {
                                        'SubProjectId': {
                                            '$toString': '$_id'
                                        }, 
                                        'value': '$t_sengg.dropdownValue', 
                                        '_id': 0
                                    }
                                }
                            ]
                            response = cmo.finding_aggregate("projectType",arra)['data']
                            df1 = pd.DataFrame(response)
                            mergedData1 = exceldata.merge(df1,on='SubProjectId',how='left')
                            exceldata['Unique ID'] = mergedData1.apply(create_unique_id, axis=1)
                            duplicated_rows = exceldata[exceldata['Unique ID'].duplicated(keep=False)].index.to_list()
                            if (duplicated_rows):
                                duplicated_rows = [x+2 for x in duplicated_rows]
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"These Rows Contains Same Unique ID :- {duplicated_rows}"
                                })
                            arra = [
                                {
                                    '$match':{
                                        'deleteStatus':{'$ne':1}
                                    }
                                }, {
                                    '$project': {
                                        'SubProjectId': 1, 
                                        'Unique ID': 1, 
                                        '_id': 0
                                    }
                                }
                            ]
                            response = cmo.finding_aggregate("SiteEngineer",arra)['data']
                            siteEnggdf = pd.DataFrame(response)
                            common_unique_ids = exceldata['Unique ID'].isin(siteEnggdf['Unique ID'])
                            common_rows_df1 = exceldata[common_unique_ids]
                            common_unique_ids_list = exceldata['Unique ID'][common_unique_ids].tolist()
                            site_list = exceldata['Site Id'][common_unique_ids].tolist()
                            if len(common_unique_ids_list):
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Unique ID Combination {common_unique_ids_list} \n for Site Id {site_list} is already found in Database"
                                })
                        if "Site Id" in exceldata.columns:
                            exceldata['Site Id'] = exceldata['Site Id'].astype(str)
                        if "SubProjectId" in exceldata.columns:
                            del exceldata['SubProjectId']
                        new_df = exceldata
                        new_df = new_df.replace({pd.NaT: None})
                        dict_data = new_df.to_dict(orient="records")
                        if len(dateField)>0:
                            for i in dateField:
                                for k in dict_data:
                                    k[i] = check_and_convert_date(k[i])
                        thread = Thread(target=background_site_upgrade, args=(dict_data,pathing))
                        thread.start()
                        return respond({
                                "status": 200,
                                "icon": "success",
                                "msg": f"Your file is in process when its completed we will notify",
                            })
                
                if fileType == "updateSiteOneProject":
                    if "Unique ID" in exceldata.columns:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":'Please remove "Unique ID" header from your excel file'
                        })
                    if "RFAI Date" in exceldata.columns:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":'Please remove "RFAI Date" header from your excel file'
                        })
                    dbData = [
                        {
                            '$match':{
                                'deleteStatus':{"$ne":1},
                                'projectuniqueId':id
                            }
                        }, {
                            "$project": {
                                "systemId": 1,
                                "uniqueId": {"$toString": "$_id"},
                                "_id": 0,
                            }
                        }
                    ]
                    systemId = cmo.finding_aggregate("SiteEngineer", dbData)
                    systemIddf = pd.DataFrame.from_dict(systemId["data"])
                    systemIdmerged = exceldata.merge(systemIddf, on=["systemId"], how="left")
                    cresult2 = systemIdmerged[systemIdmerged["uid"].isna()]
                    unique_c = cresult2["systemId"].unique()
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "System Id is not found in DataBase " + ", ".join(unique_c),
                            }
                        )
                    else:
                        exceldata['uid'] = systemIdmerged['uid']
                        exceldata['SubProjectId'] = systemIdmerged['SubProjectId']
                        subprojectObject = []
                        unique_sub_projects = exceldata['SubProjectId'].unique()
                        for i in unique_sub_projects:
                            subprojectObject.append(ObjectId(i))
                        arra = [{"$match": {"_id":{'$in': subprojectObject}}}]
                        response = cmo.finding_aggregate("projectType", arra)["data"]
                        fieldName = []
                        for i in response:
                            data = i['t_sengg']
                            unique_id_object = next((item for item in data if item["fieldName"] == "Unique ID"),None)
                            if unique_id_object:
                                if unique_id_object["dataType"] == "Auto Created":
                                    keys = unique_id_object["dropdownValue"].split(",")
                                    for i in keys:
                                        if i not in fieldName:
                                            fieldName.append(i)
                        common_columns = [col for col in exceldata.columns if col in fieldName]
                        if common_columns:
                            columns_presence = all(col in exceldata.columns for col in fieldName)
                            if columns_presence == False:
                                return respond({
                                    'status':400,
                                    "msg":f"These Column {fieldName} is must Required for Unique ID combination",
                                    "icon":"error"
                                })
                            any_empty_cells = exceldata[fieldName].isna().any().any()
                            if any_empty_cells:
                                return respond({
                                    "status":400,
                                    "icon":"error",
                                    "msg":f"These column {fieldName} contains Empty or None Value.Please check the file"
                                })
                            arra = [
                                {
                                    '$match':{
                                        'deleteStatus':{'$ne':1}
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$t_sengg', 
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$addFields': {
                                        'newField': '$t_sengg.fieldName'
                                    }
                                }, {
                                    '$match': {
                                        'newField': 'Unique ID'
                                    }
                                }, {
                                    '$project': {
                                        'SubProjectId': {
                                            '$toString': '$_id'
                                        }, 
                                        'value': '$t_sengg.dropdownValue', 
                                        '_id': 0
                                    }
                                }
                            ]
                            response = cmo.finding_aggregate("projectType",arra)['data']
                            df1 = pd.DataFrame(response)
                            mergedData1 = exceldata.merge(df1,on='SubProjectId',how='left')
                            exceldata['Unique ID'] = mergedData1.apply(create_unique_id, axis=1)
                            duplicated_rows = exceldata[exceldata['Unique ID'].duplicated(keep=False)].index.to_list()
                            if (duplicated_rows):
                                duplicated_rows = [x+2 for x in duplicated_rows]
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"These Rows Contains Same Unique ID :- {duplicated_rows}"
                                })
                            arra = [
                                {
                                    '$match':{
                                        'deleteStatus':{'$ne':1}
                                    }
                                }, {
                                    '$project': {
                                        'SubProjectId': 1, 
                                        'Unique ID': 1, 
                                        '_id': 0
                                    }
                                }
                            ]
                            response = cmo.finding_aggregate("SiteEngineer",arra)['data']
                            siteEnggdf = pd.DataFrame(response)
                            common_unique_ids = exceldata['Unique ID'].isin(siteEnggdf['Unique ID'])
                            common_rows_df1 = exceldata[common_unique_ids]
                            common_unique_ids_list = exceldata['Unique ID'][common_unique_ids].tolist()
                            site_list = exceldata['Site Id'][common_unique_ids].tolist()
                            if len(common_unique_ids_list):
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Unique ID Combination {common_unique_ids_list} \n for Site Id {site_list} is already found in Database"
                                })
                        if "Site Id" in exceldata.columns:
                            exceldata['Site Id'] = exceldata['Site Id'].astype(str)
                        if "SubProjectId" in exceldata.columns:
                            del exceldata['SubProjectId']
                        new_df = exceldata
                        dict_data = new_df.to_dict(orient="records")
                        thread = Thread(target=background_site_upgrade, args=(dict_data,pathing))
                        thread.start()
                        return respond({
                                "status": 200,
                                "icon": "success",
                                "msg": f"Your file is in process when its completed we will notify",
                            })
                        
                if fileType == "updateMilestone":
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                },
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'systemId': 1, 
                                'Name': 1, 
                                'SubProjectId':1,
                                'siteId':{'$toString': '$siteId'},
                                'uniqueId': {'$toString': '$_id'}, 
                            }
                        }
                    ]
                    systemId = cmo.finding_aggregate("milestone", dbData)
                    systemIddf = pd.DataFrame.from_dict(systemId["data"])
                    result = exceldata.merge(systemIddf, on=["systemId", "Name"], how="left")
                    cresult2 = result[result["uniqueId"].isna()]
                    unique_combinations = cresult2[["systemId", "Name"]]
                    unique_c_with_index = cresult2[["systemId", "Name"]].reset_index()
                    unique_c = unique_combinations.values

                    if len(unique_c) > 0:
                        unique_c_list = unique_c_with_index.apply(lambda row: f"RowNo:-{row['index']+2}: {row['systemId']}, {row['Name']}",axis=1,)
                        unique_c = np.array(unique_c_list)
                        unique_c_str = unique_c.flatten().astype(str)
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "System ID And Task Name Pair Are Not found In Database\n"
                                + "\n".join(unique_c_str),
                            }
                        )
                    else:
                        exceldata['uid'] = result['uniqueId']  
                        exceldata['SubProjectId'] = result['SubProjectId']
                        exceldata['siteId'] = result['siteId']
                        
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                },
                                # "type":{'$ne':'Partner'},
                                "status":"Active"
                            }
                        }, {
                            '$project': {
                                'assignerId': '$email', 
                                'userUniqueId': '$_id', 
                                '_id': 0
                            }
                        }
                    ]
                    user = cmo.finding_aggregate("userRegister", dbData)
                    userDatadf = pd.DataFrame.from_dict(user["data"])
                    result = exceldata.apply(my_function, args=(userDatadf,), axis=1)
                    
                    if isinstance(result, pd.Series):
                        resulting = []
                        for i in result:
                            if i["status"] != 200:
                                resulting.append(i["assignerId"])
                        assigner_ids = [
                            assignerId
                            for sublist in resulting
                            for assignerId in sublist
                        ]
                        if len(resulting) > 0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "This Email id Not Found in Database:-"
                                    + ", ".join(assigner_ids),
                                }
                            )
                    elif isinstance(result, pd.DataFrame):
                        resulting = []
                        for index, row in result.iterrows():
                            if row["status"] != 200:
                                resulting.append(row["assignerId"])
                        assigner_ids = [
                            assignerId
                            for sublist in resulting
                            for assignerId in sublist
                        ]
                        if len(resulting) > 0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "This Email id Not Found in Database:-"
                                    + ", ".join(assigner_ids),
                                }
                            )
                    new_df = result
                    
                    if "mileStoneStatus" in new_df:
                        invalidStatus = new_df[~new_df['mileStoneStatus'].isin(['Closed', 'Open'])].index.tolist()
                        if (len(invalidStatus)):
                            return respond({
                                "status":400,
                                "icon":"error",
                                "msg":'The "Status" column contains only the values "Closed" or "Open".'
                            })
                            
                        unique_statuses = new_df['mileStoneStatus'].unique()
                        if len(unique_statuses) > 1:
                            return respond({
                                "status": 400,
                                "icon": "error",
                                "msg": 'All rows in the "Custom Status" column must be the same.'
                            })
                    
                    if "CC_Completion Date" in new_df:
                        invalidDate = []
                        expected_format = '%d-%m-%Y'
                        for index, row in new_df.iterrows():
                            if pd.isna(row['CC_Completion Date']) or row['CC_Completion Date'] == '':
                                invalidDate.append(index+2)
                            else:
                                try:
                                    row['CC_Completion Date'] = pd.to_datetime(row['CC_Completion Date'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidDate.append(index+2)
                        
                        if len(invalidDate):
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These rows have incorrect MS Completion Date format or the MS Completion Date may be empty:- {invalidDate}",
                                }
                            )
                    arra = [
                        {
                            '$project': {
                                'SubProjectId': {
                                    '$toString': '$_id'
                                }, 
                                'projectType': 1, 
                                '_id': 0
                            }
                        }, {
                            '$sort':{
                                'projectType':1
                            }
                        }
                    ]
                    projectType = cmo.finding_aggregate("projectType",arra)['data']
                    if len(projectType):
                        projectTypedf = pd.DataFrame.from_dict(projectType)
                        mergedPT = result.merge(projectTypedf,on='SubProjectId',how='left')
                        result['projectType'] = mergedPT['projectType']
                    
                    if "projectType" in result:
                        arra = [
                            {
                                '$project': {
                                    'projectType': '$projectTypeName', 
                                    'Name': '$milestoneName', 
                                    'headerName': '$headerName', 
                                    '_id': 0
                                }
                            }
                        ]
                        mappedData = cmo.finding_aggregate("mappedData",arra)['data']
                        if len(mappedData):
                            mappedDatadf = pd.DataFrame.from_dict(mappedData)
                            result = result.merge(mappedDatadf,on=['projectType','Name'],how='left')
                    new_df = result
                    new_df.replace({np.nan: None}, inplace=True)
                    
                        # # new_df["CC_Completion Date"] = pd.to_datetime(new_df["CC_Completion Date"]).dt.tz_localize("Asia/Kolkata")
                        # future_date_rows = new_df[new_df["CC_Completion Date"] > df_current_date()]
                        # future_date_indices = future_date_rows.index
                        # future_date_index_strings = ((future_date_indices + 2).astype(str).tolist())
                        # if future_date_index_strings:
                        #     return respond(
                        #         {
                        #             "status": 400,
                        #             "icon": "error",
                        #             "msg": f"Rows where MS Completion Date is greater than the current date: {', '.join(future_date_index_strings)}",
                        #         }
                        #     )
                    
                    for i in ['status','SubProjectId','projectType']:
                        if i in new_df:
                            del new_df[i]
                        
                    dict_data = new_df.to_dict(orient="records")
                    thread = Thread(target=background_task_upgrade, args=(dict_data,pathing))
                    thread.start()
                    return respond({
                        "status": 200,
                        "icon": "success",
                        "msg": f"Your file is in process when its completed we will notify",
                    })
                    
                if fileType == "updateMilestoneOneProject":
                    arra = [
                        {
                            '$match': {
                                'projectuniqueId': id, 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'Name': 1, 
                                'systemId': 1, 
                                'uniqueId': {'$toString': '$_id'},
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("milestone",arra)['data']
                    if len(response):
                        milestonedf = pd.DataFrame.from_dict(response)
                        mergeddf = exceldata.merge(milestonedf,on=['Name','systemId'],how='left')
                        cresult2 = mergeddf[mergeddf["uniqueId"].isna()]
                        unique_combinations = cresult2[["systemId", "Name"]]
                        unique_c_with_index = cresult2[["systemId", "Name"]].reset_index()
                        unique_c = unique_combinations.values
                        
                        if len(unique_c) > 0:
                            unique_c_list = unique_c_with_index.apply(lambda row: f"RowNo:-{row['index']+2}: {row['systemId']}, {row['Name']}",axis=1,)
                            unique_c = np.array(unique_c_list)
                            unique_c_str = unique_c.flatten().astype(str)
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "System ID And Task Name Pair Are Not found In Database\n"
                                    + "\n".join(unique_c_str),
                                }
                            )
                        dbData = [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }
                                }
                            }, {
                                '$project': {
                                    'assignerId': '$email', 
                                    'userUniqueId': '$_id', 
                                    '_id': 0
                                }
                            }
                        ]
                        user = cmo.finding_aggregate("userRegister", dbData)
                        userDatadf = pd.DataFrame.from_dict(user["data"])
                        result = exceldata.apply(my_function, args=(userDatadf,), axis=1)
                        
                        if isinstance(result, pd.Series):
                            resulting = []
                            for i in result:
                                if i["status"] != 200:
                                    resulting.append(i["assignerId"])
                            assigner_ids = [
                                assignerId
                                for sublist in resulting
                                for assignerId in sublist
                            ]
                            if len(resulting) > 0:
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": "This Email id Not Found in Database:-"
                                        + ", ".join(assigner_ids),
                                    }
                                )

                        elif isinstance(result, pd.DataFrame):
                            resulting = []
                            for index, row in result.iterrows():
                                if row["status"] != 200:
                                    resulting.append(row["assignerId"])
                            assigner_ids = [
                                assignerId
                                for sublist in resulting
                                for assignerId in sublist
                            ]
                            if len(resulting) > 0:
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": "This Email id Not Found in Database:-"
                                        + ", ".join(assigner_ids),
                                    }
                                )
                                
                        new_df = result
                        if "CC_Completion Date" in new_df:
                            invalidDate = []
                            expected_format = '%d-%m-%Y'
                            for index, row in new_df.iterrows():
                                if pd.isna(row['CC_Completion Date']) or row['CC_Completion Date'] == '':
                                    invalidDate.append(index+2)
                                else:
                                    try:
                                        row['CC_Completion Date'] = pd.to_datetime(row['CC_Completion Date'], format=expected_format, errors='raise')
                                    except Exception as e:
                                        invalidDate.append(index+2)
                            
                            if len(invalidDate):
                                
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": f"These rows have incorrect MS Completion Date format or the MS Completion Date may be empty:- {invalidDate}",
                                    }
                                )
                                
                        if "mileStoneStatus" in new_df:
                            invalidStatus = new_df[~new_df['mileStoneStatus'].isin(['Closed', 'Open'])].index.tolist()
                            if (len(invalidStatus)):
                                return respond({
                                    "status":400,
                                    "icon":"error",
                                    "msg":'The "Status" column contains only the values "Closed" or "Open".'
                                })
                        
                        if "uniqueId" in new_df:
                            del new_df['uniqueId']
                            
                        if "status" in new_df:
                            del new_df['status']
                        dict_data = new_df.to_dict(orient="records")
                        thread = Thread(target=background_task_upgrade, args=(dict_data,pathing))
                        thread.start()
                        return respond({
                            "status": 200,
                            "icon": "success",
                            "msg": f"Your file is in process when its completed we will notify",
                        })
                        
                    else:
                        return respond({
                            'icon':'warning',
                            'status':400,
                            "msg":"This project does not contain any milestones."
                        })
                
                if fileType == "updateOldMilestone":
                    expected_columns = ['systemId', 'Name', 'Completion Criteria', 'Predecessor']
                    extra_columns = [col for col in exceldata.columns if col not in expected_columns]
                    if extra_columns:
                        return respond({
                            'status':400,
                            "msg":f'Extraa Column is found in Excel file, please remove the Extraa column :- {extra_columns}',
                            'icon':'error'
                        })
                        
                    required_columns = ['Completion Criteria', 'Predecessor']
                    if not any(col in exceldata.columns for col in required_columns):
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":'Please Provide At least one of the required columns "MS Completion Criteria" , "MS Predecessor"'
                        })
                        
                    if "Completion Criteria" in exceldata.columns:
                        empty_index = exceldata[(exceldata['Completion Criteria'] == "") | (exceldata['Completion Criteria'].isna())].index.tolist()
                        if empty_index:
                            empty_index = [x+2 for x in empty_index]
                            return respond({
                                'status':400,
                                "icon":"error",
                                "msg":f'These rows found Empty Data for MS Completion Criteria field :- {empty_index}'
                            })
                            
                    if "Predecessor" in exceldata.columns:
                        empty_index = exceldata[(exceldata['Predecessor'] == "") & (exceldata['Name'] != "Survey") & (exceldata['Name'] != "SACFA Check")].index.tolist()
                        if empty_index:
                            empty_index = [x+2 for x in empty_index]
                            return respond({
                                'status':400,
                                "icon":"error",
                                "msg":f'These rows found Empty Data for MS Predecessor field :- {empty_index}'
                            })
                    
                    
                        
                    arra = [
                        {
                            '$match': {
                                'mileStoneStatus': 'Open', 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'systemId': 1, 
                                'Name': 1, 
                                'uid': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("milestone",arra)
                    if len(response['data']):
                        df = pd.DataFrame.from_dict(response['data'])
                        mergedData = df.merge(exceldata,on=['systemId','Name'],how='right')
                        cresult2 = mergedData[mergedData["uid"].isna()]
                        unique_combinations = cresult2[["systemId", "Name"]]
                        unique_c_with_index = cresult2[["systemId", "Name"]].reset_index()
                        unique_c = unique_combinations.values
                        if len(unique_c) > 0:
                            unique_c_list = unique_c_with_index.apply(lambda row: f"RowNo:-{row['index']+2}: {row['systemId']}, {row['Name']}",axis=1,)
                            unique_c = np.array(unique_c_list)
                            unique_c_str = unique_c.flatten().astype(str)
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "System ID And Task Name Pair Are Not found In Database\n"
                                    + "\n".join(unique_c_str),
                                }
                            )
                        else:
                            exceldata['uid'] = mergedData['uid']
                            if "Completion Criteria" in exceldata.columns:
                                arra = [
                                    {
                                        '$project': {
                                            'Completion Criteria': '$completion', 
                                            '_id': 0
                                        }
                                    }
                                ]
                                CC = cmo.finding_aggregate("complectionCriteria",arra)
                                if len(CC['data']):
                                    CCdf = pd.DataFrame.from_dict(CC['data'])
                                    exceldata['Completion'] = exceldata['Completion Criteria'].apply(lambda x: x.split(','))
                                    unmatched_cc = []
                                    for idx, criteria_list in enumerate(exceldata['Completion']):
                                         if not all(item in CCdf['Completion Criteria'].values for item in criteria_list):
                                             unmatched_cc.append(idx)
                                    
                                    if unmatched_cc:
                                        unmatched_cc = [x+2 for x in unmatched_cc]
                                        return respond({
                                            'status':400,
                                            "icon":'error',
                                            "msg":f'These Rows Contain Invalid MS Completion Criteria :- {unmatched_cc}'
                                        })
                                    if "Completion" in exceldata:
                                        del exceldata['Completion']
                                else:
                                    return respond({
                                        'status':400,
                                        "icon":"error",
                                        "msg":'Firstly,Please Create Completion Criteria in HR Module'
                                    })    
                            dict_data = exceldata.to_dict(orient="records")
                            thread = Thread(target=background_old_task_upgrade, args=(dict_data,pathing))
                            thread.start()
                            return respond({
                                "status": 200,
                                "icon": "success",
                                "msg": f"Your file is in process when its completed we will notify",
                            })
                    else:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":'There is no open Task found in the Database...'
                        })
                        
            else:
                return respond(data)