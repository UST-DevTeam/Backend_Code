from base import *
import common.excel_write as excelWriteFunc
from common.config import convertToDateBulkExport as convertToDateBulkExport
from datetime import datetime
import pytz
from datetime import datetime as dtt
from blueprint_routes.currentuser_blueprint import projectId_Object, projectId_str

def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time

sample_blueprint = Blueprint('sample_blueprint', __name__)

def invoiceaccess(roleId):
    mappingarra = [
        {
            '$match':{
                'moduleName':"Financial button under Template",
                'roleId':roleId
            }
        }
    ]
    mapingResponse = cmo.finding_aggregate("uamManagement",mappingarra)['data']
    accessType = "H"
    if len(mapingResponse):
        accessType = mapingResponse[0]['accessType']
    return accessType
  
  
def sample_pg():
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$lookup': {
                'from': 'customer', 
                'localField': 'customerId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'customer'
            }
        }, {
            '$unwind': {
                'path': '$customer', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'zone', 
                'localField': 'zoneId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'zone'
            }
        }, {
            '$unwind': {
                'path': '$zone', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'costCenter', 
                'localField': 'costCenterId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'costCenter'
            }
        }, {
            '$unwind': {
                'path': '$costCenter', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'costCenter': '$costCenter.costCenter', 
                'zone': '$zone.shortCode', 
                'customer': '$customer.shortName'
            }
        }, {
            '$addFields': {
                'projectGroupName': {
                    '$concat': [
                        '$customer', '-', '$zone', '-', '$costCenter'
                    ]
                }, 
                'Project Group': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'projectGroupName': 1, 
                'Project Group': 1
            }
        }
    ]
    response = cmo.finding_aggregate("projectGroup",arra)['data']
    projectGroupdf = []
    if len(response):
        projectGroupdf = pd.DataFrame.from_dict(response)
    return projectGroupdf
        
        

def sample_PT():
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'Sub Project': {
                    '$toString': '$_id'
                }, 
                '_id': 0, 
                'Project Type': '$projectType', 
                'subProjectName': '$subProject'
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)['data']
    projectTypedf = []
    if len(response):
        projectTypedf =  pd.DataFrame.from_dict(response)  
    return projectTypedf
        
        
def sample_circle():
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'Circle': {
                    '$toString': '$_id'
                }, 
                'circleName': '$circleCode', 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("circle",arra)['data']
    circledf = pd.DataFrame(response)
    return circledf
    

def sample_pmName():
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }, 
                'type': {
                    '$ne': 'Partner'
                }
            }
        }, {
            '$project': {
                'NamePM': '$empName', 
                'PM Name': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("userRegister",arra)['data']
    pmNamedf = pd.DataFrame.from_dict(response)
    return pmNamedf
        


def sample_projectId(userRole,empId):
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }
    ]
    if userRole not in ['Admin','PMO']:
        arra = arra + [
            {
                '$match':{
                    '_id':{
                        '$in':projectId_Object(empId)
                    }
                }
            }
        ]
    
    arra = arra + [
        {
            '$addFields': {
                'custId': {
                    '$toObjectId': '$custId'
                }
            }
        }, {
            '$lookup': {
                'from': 'customer', 
                'localField': 'custId', 
                'foreignField': '_id', 
                'as': 'result'
            }
        }, {
            '$project': {
                'Project Group': {
                    '$toString': '$projectGroup'
                }, 
                'Circle': '$circle', 
                'Customer': {
                    '$arrayElemAt': [
                        '$result.customerName', 0
                    ]
                }, 
                'PM Name': '$PMId', 
                'Project ID': {
                    '$toString': '$_id'
                }, 
                'projectId': 1, 
                '_id': 0,
                'customeruid':{
                    '$toString':'$custId'
                }
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)['data']
    projectdf = pd.DataFrame(response)
    return projectdf


def sample_siteid(subProjectIdswithoutObjectId,userRole,empId,id,siteId,siteStatus,siteBillingStatus):
    arra=[
        {
            '$match':{
                'deleteStatus': {'$ne': 1},
            }
        }
    ]
    if siteId != None and siteId != 'undefined':
        arra = arra+[
            {
                '$match':{
                    'Site Id':{
                        '$regex':siteId,
                        '$options':'i'
                    }
                }
            }
        ]
    if siteStatus != None and siteStatus != 'undefined':  
        arra = arra + [
            {
                '$match':{
                    'siteStatus':siteStatus
                }
            }
        ] 
    if siteBillingStatus != None and siteBillingStatus != 'undefined': 
        arra = arra + [
            {
                '$match':{
                    'siteBillingStatus':siteBillingStatus
                }
            }
        ]
    if id != None and id != 'undefined':
        arra=arra+[
                {
                    '$match': {
                        'projectuniqueId': id
                    }
                }
            ]
    if userRole not in ['Admin','PMO']:
        arra = arra + [
            {
                '$match':{
                    'projectuniqueId':{
                        '$in':projectId_str(empId)
                    }
                }
            }
        ]
    arra = arra+ [
        {
            '$match': {
                'SubProjectId':{
                    '$in':subProjectIdswithoutObjectId
                }
            }
        }, {
            '$addFields': {
                'Site ID': '$Site Id', 
                'Unique ID': '$Unique ID', 
                'System ID': '$systemId',
                "Sub Project":'$SubProjectId',
                'Project ID':'$projectuniqueId',
                'siteuid':{
                    '$toString':'$_id'
                }
            }
        }, {
            '$project': {
                'addedAt': 0, 
                'projectuniqueId':0,
                'SubProjectId':0,
                'Project Type': 0, 
                'Customer': 0, 
                'customerId': 0, 
                'siteEndDate': 0, 
                'siteStartDate': 0, 
                'siteStatus': 0, 
                'siteBillingStatus': 0, 
                'systemId': 0, 
                'projectGroupid': 0, 
                'projectType': 0, 
                'Site Id': 0, 
                'Project Group': 0, 
                '_id':0,
                'Task Name':0,
                'Task Owner':0,
                'MS Completion Date':0,
                'Custom Status':0,
                "Task Closure":0,
                'Start Date':0,
                "Due Date":0,
                'PM Name':0,
                'Circle':0,
                "circleId":0
            }
        }
    ]
    response = cmo.finding_aggregate("SiteEngineer",arra)['data']
    siteIddf = []
    if len(response):
        siteIddf = pd.DataFrame(response)
    return siteIddf

def sample_task(subProjectIdswithoutObjectId,userRole,empId,id):
    arra=[]
    if id != None and id != 'undefined':
        arra=[
            {
                '$match': {
                    'projectuniqueId': id
                }
            }
        ]
    if userRole not in ['Admin','PMO']:
        arra = arra + [
            {
                '$match':{
                    'projectuniqueId':{
                        '$in':projectId_str(empId)
                    }
                }
            }
        ]
    arra =arra+ [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }, 
                'SubProjectId': {
                    '$in':subProjectIdswithoutObjectId
                }
            }
        }, {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'assignerId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'Task Owner': '$result.empName'
            }
        }, {
            '$addFields': {
                'Task Owner': {
                    '$reduce': {
                        'input': '$Task Owner', 
                        'initialValue': '', 
                        'in': {
                            '$concat': [
                                '$$value', {
                                    '$cond': [
                                        {
                                            '$eq': [
                                                '$$value', ''
                                            ]
                                        }, '', ','
                                    ]
                                }, '$$this'
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                'Task Name': '$Name', 
                'Task Owner': 1, 
                'Start Date': '$mileStoneStartDate', 
                'Due Date': '$mileStoneEndDate', 
                'MS Completion Date': '$CC_Completion Date', 
                'Task Closure': 1, 
                'Custom Status': '$mileStoneStatus', 
                'siteuid': {
                    '$toString': '$siteId'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("milestone",arra)['data']

    taskdf = []
    if len(response):
        taskdf = pd.DataFrame(response)
    return taskdf


def sample_template(subProjectIds,roleId="",T_D=""):
    mappingarra = [
        {
            '$match':{
                'moduleName':"Financial button under Template",
                'roleId':roleId
            }
        }
    ]
    mapingResponse = cmo.finding_aggregate("uamManagement",mappingarra)['data']
    accessType = "H"
    if len(mapingResponse):
        accessType = mapingResponse[0]['accessType']
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                },
                '_id':{
                    '$in':subProjectIds
                }
            }
        }, {
            '$project': {
                'projectType': 0, 
                'subProject': 0, 
                'status': 0, 
                'custId': 0, 
                '_id': 0, 
                'MileStone': 0, 
                'Commercial': 0
            }
        }, {
            '$match': {
                '$expr': {
                    '$gt': [
                        {
                            '$size': {
                                '$filter': {
                                    'input': {
                                        '$objectToArray': '$$ROOT'
                                    }, 
                                    'as': 'field', 
                                    'cond': {
                                        '$ne': [
                                            '$$field.v', None
                                        ]
                                    }
                                }
                            }
                        }, 0
                    ]
                }
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)['data']
    T_sengg = []
    T_tarcking = []
    T_issues = []
    T_financial = []
    T_Date = []
    
    for response in response:
        
        
    
        if 't_sengg' in response:
            for i in response['t_sengg']:
                if i['dataType'] == "Date":
                    if i["fieldName"] not in T_Date:
                        T_Date.append(i['fieldName'])
                if i['fieldName'] not in T_sengg:
                    T_sengg.append(i['fieldName'])
                
        if "t_tracking" in response:
            for i in response['t_tracking']:
                if i['dataType'] == "Date":
                    if i["fieldName"] not in T_Date:
                        T_Date.append(i['fieldName'])
                if i['fieldName'] not in T_tarcking:
                    T_tarcking.append(i['fieldName'])
                
        if "t_issues" in response:
            for i in response['t_issues']:
                if i['dataType'] == "Date":
                    if i["fieldName"] not in T_Date:
                        T_Date.append(i['fieldName'])
                if i['fieldName'] not in T_issues:
                    T_issues.append(i['fieldName'])
        if accessType == "W":        
            if "t_sFinancials" in response:
                for i in response['t_sFinancials']:
                    if i['fieldName'] not in T_financial:
                        T_financial.append(i['fieldName'])
                    
        if 'Site Id' in T_sengg:
            T_sengg.remove('Site Id')
        if "RFAI Date" in T_sengg:
            T_sengg.remove('RFAI Date')
        if "Unique ID" in T_sengg:
            T_sengg.remove('Unique ID')
        
    if T_D == "":
        return T_sengg+T_tarcking+T_issues+T_financial
    else:
        return T_Date


def sample_invoice(projectId):
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }]
    if projectId!=None:
        arra = arra + [
            {
                '$match':{
                    'projectId':{
                        '$in':projectId
                    }
                }
            }
        ]
    arra = arra + [
        {
            '$addFields': {
                'wccSignOffdate': {
                    '$cond': [
                        {
                            '$eq': [
                                '$wccSignOffdate', ''
                            ]
                        }, None, {
                            '$toDate': '$wccSignOffdate'
                        }
                    ]
                }, 
                'invoiceDate': {
                    '$cond': [
                        {
                            '$eq': [
                                '$invoiceDate', ''
                            ]
                        }, None, {
                            '$toDate': '$invoiceDate'
                        }
                    ]
                }
            }
        }, {
            '$addFields': {
                'invoiceDate': {
                    '$dateAdd': {
                        'startDate': '$invoiceDate', 
                        'unit': 'minute', 
                        'amount': 330
                    }
                }, 
                'wccSignOffdate': {
                    '$dateAdd': {
                        'startDate': '$wccSignOffdate', 
                        'unit': 'minute', 
                        'amount': 330
                    }
                }
            }
        }, {
            '$addFields': {
                'invoiceDate': {
                    '$dateToString': {
                        'date': {
                            '$toDate': '$invoiceDate'
                        }, 
                        'format': '%d-%m-%Y', 
                        'timezone': 'Asia/Kolkata'
                    }
                }, 
                'wccSignOffdate': {
                    '$dateToString': {
                        'date': {
                            '$toDate': '$wccSignOffdate'
                        }, 
                        'format': '%d-%m-%Y', 
                        'timezone': 'Asia/Kolkata'
                    }
                }
            }
        }, {
            '$project': {
                "siteuid":'$siteId', 
                'WCC No': '$wccNumber', 
                'WCC SignOff Date': '$wccSignOffdate', 
                'PO Number': '$poNumber', 
                'Item Code': '$itemCode', 
                'Invoiced Quantity': '$qty', 
                'Invoice Number': '$invoiceNumber', 
                'Invoice Date': '$invoiceDate', 
                'Unit Rate': '$unitRate', 
                'Amount': {
                    '$multiply': [
                        '$unitRate', '$qty'
                    ]
                },
                'Status': '$status', 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("invoice",arra)
    invoicedf = pd.DataFrame.from_dict(response['data'])
    if not invoicedf.empty:
        invoicedf = invoicedf.groupby('siteuid').agg(lambda x: ', '.join(x.astype(str))).reset_index()
        invoicedf['Status'] = invoicedf['Status'].apply(lambda x: x.split(',')[-1].strip())
        
        invoicedf = invoicedf.groupby('siteuid').agg(lambda x: ', '.join(map(str, x))).reset_index()
        invoicedf['Status'] = invoicedf.groupby('siteuid')['Status'].apply(lambda x: x.iloc[-1]).reset_index(drop=True)

    return invoicedf
    



@sample_blueprint.route("/export/siteWithOutTask2/<customeruniqueId>",methods=['GET','POST'])
@token_required
def sample_api(current_user,customeruniqueId=None):
    
    subProjectIdswithoutObjectId=request.get_json()
    subProjectIdswithoutObjectId=subProjectIdswithoutObjectId['subproject']
    subProjectIds=[]
    for i in subProjectIdswithoutObjectId:
        subProjectIds.append(ObjectId(i))
    projectgroupdf = sample_pg()
    circledf = sample_circle()
    pmNamedf = sample_pmName()
    projectdf = sample_projectId(current_user['roleName'],current_user['userUniqueId'])
    projectTypedf = sample_PT()
    siteiddf = sample_siteid(subProjectIdswithoutObjectId,current_user['roleName'],current_user['userUniqueId'],id=None,siteId=None,siteStatus=None,siteBillingStatus=None)
    mergeData = circledf.merge(projectdf,on='Circle',how='right')
    projectdf['Circle'] = mergeData['circleName']
    mergeData = projectgroupdf.merge(projectdf,on='Project Group',how='right')
    projectdf['Project Group'] = mergeData['projectGroupName']
    mergeData = pmNamedf.merge(projectdf,on='PM Name',how='right')
    projectdf['PM Name'] = mergeData['NamePM']
    mergeData = projectTypedf.merge(siteiddf,on='Sub Project',how='right')
    siteiddf['Sub Project'] = mergeData['subProjectName']
    siteiddf['Project Type'] = mergeData['Project Type']
    mergeData = projectdf.merge(siteiddf,on='Project ID',how='right')
    siteiddf['Project ID'] = mergeData['projectId']
    siteiddf['Customer']  = mergeData['Customer']
    siteiddf['Project Group']  = mergeData['Project Group']
    siteiddf['PM Name']  = mergeData['PM Name']
    siteiddf['Circle']  = mergeData['Circle']
    
    project_id_list = list(set(mergeData['Project ID'].tolist()))
    
    
    permission = invoiceaccess(current_user['roleId'])
    if permission == "W":
        invoicedf = sample_invoice(project_id_list)
        if not invoicedf.empty:
            siteiddf = siteiddf.merge(invoicedf,on="siteuid",how='left')
            
    Template = sample_template(subProjectIds,current_user['roleId'],T_D="")
    T_Date = sample_template(subProjectIds,current_user['roleId'],"Vishal")
    if "RFAI Date" not in T_Date:
        T_Date.append("RFAI Date")
    columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date"]+Template
    
    for col in columns_to_start:
        if col not in siteiddf.columns:
            siteiddf[col] = '' 
    for col in T_Date:
        siteiddf[col] = siteiddf[col].apply(convertToDateBulkExport)
    siteiddf = siteiddf[columns_to_start]
    
    fullPath = excelWriteFunc.excelFileWriter(siteiddf, "Export_Site", "Site")
    return send_file(fullPath)

@sample_blueprint.route("/export/siteWithTask2/<customeruniqueId>",methods=['POST'])
@token_required
def sample_api_Task(current_user,customeruniqueId=None):
    
    subProjectIdswithoutObjectId=request.get_json()
    subProjectIdswithoutObjectId=subProjectIdswithoutObjectId['subproject']
    subProjectIds=[]
    for i in subProjectIdswithoutObjectId:
        subProjectIds.append(ObjectId(i))
    
    projectgroupdf = sample_pg()
    circledf = sample_circle()
    pmNamedf = sample_pmName()
    projectdf = sample_projectId(current_user['roleName'],current_user['userUniqueId'])
    projectTypedf = sample_PT()
    siteiddf = sample_siteid(subProjectIdswithoutObjectId,current_user['roleName'],current_user['userUniqueId'],id=None,siteId=None,siteStatus=None,siteBillingStatus=None)
    taskdf = sample_task(subProjectIdswithoutObjectId,current_user['roleName'],current_user['userUniqueId'],id=None)
    
    if len(siteiddf)<1:
        return respond({
            'icon':"error",
            "status":400,
            "msg":"There is no One stie in this Project Type"
        })
    
    
    mergeData = circledf.merge(projectdf,on='Circle',how='right')
    projectdf['Circle'] = mergeData['circleName']
    mergeData = projectgroupdf.merge(projectdf,on='Project Group',how='right')
    projectdf['Project Group'] = mergeData['projectGroupName']
    mergeData = pmNamedf.merge(projectdf,on='PM Name',how='right')
    projectdf['PM Name'] = mergeData['NamePM']
    mergeData = projectTypedf.merge(siteiddf,on='Sub Project',how='right')

    siteiddf['Sub Project'] = mergeData['subProjectName']
    siteiddf['Project Type'] = mergeData['Project Type']
    
    mergeData = projectdf.merge(siteiddf,on='Project ID',how='right')
    siteiddf['Project ID'] = mergeData['projectId']
    siteiddf['Customer']  = mergeData['Customer']
    siteiddf['Project Group']  = mergeData['Project Group']
    siteiddf['PM Name']  = mergeData['PM Name']
    siteiddf['Circle']  = mergeData['Circle']
    
    project_id_list = list(set(mergeData['Project ID'].tolist()))
    
    permission = invoiceaccess(current_user['roleId'])
    if permission == "W":
        invoicedf = sample_invoice(project_id_list)
        if not invoicedf.empty:
            siteiddf = siteiddf.merge(invoicedf,on="siteuid",how='left')
            
    siteiddf = siteiddf.merge(taskdf,on=['siteuid'],how='left')
    
    Template = sample_template(subProjectIds,current_user['roleId'],T_D="")
    T_Date = sample_template(subProjectIds,current_user['roleId'],"Vishal")
    if "RFAI Date" not in T_Date:
        T_Date.append("RFAI Date")
    T_Date = T_Date+["Start Date","Due Date","MS Completion Date","Task Closure"]
    
    
    columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date",'Task Name',"Task Owner","Start Date","Due Date","MS Completion Date","Task Closure","Custom Status"]+Template
    for col in columns_to_start:
        if col not in siteiddf.columns:
            siteiddf[col] = ''
            
    for col in T_Date:
        siteiddf[col] = siteiddf[col].apply(convertToDateBulkExport)
    
    siteiddf = siteiddf[columns_to_start]
    
    fullPath = excelWriteFunc.excelFileWriter(siteiddf, "Export_Task", "Task")
    return send_file(fullPath)
    
    
@sample_blueprint.route("/export/siteId/<id>",methods=['GET','POST'])
@token_required
def sample_api2(current_user,id=None):
    sub_project_ids=[]
    if id != None and id != "undefined":
        arr3 =[
            {
                '$match': {
                    'projectuniqueId': id,
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if request.args.get("subProject")!=None and request.args.get("subProject")!='undefined':
            arr3 = arr3 + [
                {
                    '$match':{
                        'SubProjectId':request.args.get("subProject")
                    }
                }
            ]
        arr3 = arr3 + [
             {
                '$project': {
                    'SubProjectId': 1, 
                    '_id': 0
                }
            },
        ]
        dataf = cmo.finding_aggregate("SiteEngineer", arr3)["data"]
        sub_project_ids = [d["SubProjectId"] for d in dataf]
    siteId=None
    siteStatus=None
    siteBillingStatus=None
    if request.args.get("siteId")!=None and request.args.get("siteId")!='undefined':
        siteId = request.args.get("siteId").strip()
    if request.args.get("siteStatus")!=None and request.args.get("siteStatus")!='undefined':
        siteStatus = request.args.get("siteStatus")
    if request.args.get("siteBillingStatus")!=None and request.args.get("siteBillingStatus")!='undefined':
        siteBillingStatus = request.args.get("siteBillingStatus")
    subProjectIdswithoutObjectId=sub_project_ids
    subProjectIds=[]
    for i in subProjectIdswithoutObjectId:
        subProjectIds.append(ObjectId(i))
    projectgroupdf = sample_pg()
    circledf = sample_circle()
    pmNamedf = sample_pmName()
    projectdf = sample_projectId(current_user['roleName'],current_user['userUniqueId'])
    projectTypedf = sample_PT()
    siteiddf = sample_siteid(subProjectIdswithoutObjectId,current_user['roleName'],current_user['userUniqueId'],id,siteId,siteStatus,siteBillingStatus)
    mergeData = circledf.merge(projectdf,on='Circle',how='right')
    projectdf['Circle'] = mergeData['circleName']
    mergeData = projectgroupdf.merge(projectdf,on='Project Group',how='right')
    projectdf['Project Group'] = mergeData['projectGroupName']
    mergeData = pmNamedf.merge(projectdf,on='PM Name',how='right')
    projectdf['PM Name'] = mergeData['NamePM']
    mergeData = projectTypedf.merge(siteiddf,on='Sub Project',how='right')

    siteiddf['Sub Project'] = mergeData['subProjectName']
    siteiddf['Project Type'] = mergeData['Project Type']
    
    mergeData = projectdf.merge(siteiddf,on='Project ID',how='right')
    siteiddf['Project ID'] = mergeData['projectId']
    siteiddf['Customer']  = mergeData['Customer']
    siteiddf['Project Group']  = mergeData['Project Group']
    siteiddf['PM Name']  = mergeData['PM Name']
    siteiddf['Circle']  = mergeData['Circle']
    
    
    permission = invoiceaccess(current_user['roleId'])
    if permission == "W":
        project_id_list = [id]
        invoicedf = sample_invoice(project_id_list)
        if not invoicedf.empty:
            siteiddf = siteiddf.merge(invoicedf,on="siteuid",how='left')
    
    Template = sample_template(subProjectIds,current_user['roleId'],T_D="")
    T_Date = sample_template(subProjectIds,current_user['roleId'],"Vishal")
    if "RFAI Date" not in T_Date:
        T_Date.append("RFAI Date")
    
    
    
    columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date"]+Template
    
    for col in columns_to_start:
        if col not in siteiddf.columns:
            siteiddf[col] = ''
    
    for col in T_Date:
        siteiddf[col] = siteiddf[col].apply(convertToDateBulkExport)
        
    siteiddf = siteiddf[columns_to_start]
    fullPath = excelWriteFunc.excelFileWriter(siteiddf, "Export_Site", "Site")
    return send_file(fullPath)

@sample_blueprint.route("/export/siteIdwithMilestone/<id>",methods=['POST','GET'])
@token_required
def sample_api_Task2(current_user,id=None):
    sub_project_ids=[]
    if id != None and id != "undefined":
        arr3 =[
            {
                '$match': {
                    'projectuniqueId': id,
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if request.args.get("subProject")!=None and request.args.get("subProject")!='undefined':
            arr3 = arr3 + [
                {
                    '$match':{
                        'SubProjectId':request.args.get("subProject")
                    }
                }
            ]
        arr3 = arr3 + [
             {
                '$project': {
                    'SubProjectId': 1, 
                    '_id': 0
                }
            },
        ]
        dataf = cmo.finding_aggregate("SiteEngineer", arr3)["data"]
        sub_project_ids = [d["SubProjectId"] for d in dataf]
    siteId=None
    siteStatus=None
    siteBillingStatus=None
    if request.args.get("siteId")!=None and request.args.get("siteId")!='undefined':
        siteId = request.args.get("siteId").strip()
    if request.args.get("siteStatus")!=None and request.args.get("siteStatus")!='undefined':
        siteStatus = request.args.get("siteStatus")
    if request.args.get("siteBillingStatus")!=None and request.args.get("siteBillingStatus")!='undefined':
        siteBillingStatus = request.args.get("siteBillingStatus")
    
    subProjectIdswithoutObjectId=sub_project_ids
    subProjectIds=[]
    for i in subProjectIdswithoutObjectId:
        subProjectIds.append(ObjectId(i))
    
    projectgroupdf = sample_pg()
    circledf = sample_circle()
    pmNamedf = sample_pmName()
    projectdf = sample_projectId(current_user['roleName'],current_user['userUniqueId'])
    projectTypedf = sample_PT()
    siteiddf = sample_siteid(subProjectIdswithoutObjectId,current_user['roleName'],current_user['userUniqueId'],id,siteId,siteStatus,siteBillingStatus)
    taskdf = sample_task(subProjectIdswithoutObjectId,current_user['roleName'],current_user['userUniqueId'],id)
    
    # if siteiddf == []:
    #     return respond({
    #         'icon':"error",
    #         "status":400,
    #         "msg":"There is no One stie in this Project Type"
    #     })
    if len(siteiddf) <1:
        return respond({
            'icon':"error",
            "status":400,
            "msg":"There is no One site in this Project Type"
        })
    
    
    mergeData = circledf.merge(projectdf,on='Circle',how='right')
    projectdf['Circle'] = mergeData['circleName']
    mergeData = projectgroupdf.merge(projectdf,on='Project Group',how='right')
    projectdf['Project Group'] = mergeData['projectGroupName']
    mergeData = pmNamedf.merge(projectdf,on='PM Name',how='right')
    projectdf['PM Name'] = mergeData['NamePM']
    mergeData = projectTypedf.merge(siteiddf,on='Sub Project',how='right')

    siteiddf['Sub Project'] = mergeData['subProjectName']
    siteiddf['Project Type'] = mergeData['Project Type']
    
    mergeData = projectdf.merge(siteiddf,on='Project ID',how='right')
    siteiddf['Project ID'] = mergeData['projectId']
    siteiddf['Customer']  = mergeData['Customer']
    siteiddf['Project Group']  = mergeData['Project Group']
    siteiddf['PM Name']  = mergeData['PM Name']
    siteiddf['Circle']  = mergeData['Circle']
    
    permission = invoiceaccess(current_user['roleId'])
    if permission == "W":
        project_id_list = [id]
        invoicedf = sample_invoice(project_id_list)
        if not invoicedf.empty:
            siteiddf = siteiddf.merge(invoicedf,on="siteuid",how='left')
    
    siteiddf = siteiddf.merge(taskdf,on=['siteuid'],how='left')
    
    Template = sample_template(subProjectIds,current_user['roleId'],T_D="")
    T_Date = sample_template(subProjectIds,current_user['roleId'],"Vishal")
    T_Date = T_Date+["Start Date","Due Date","MS Completion Date","Task Closure"]
    if "RFAI Date" not in T_Date:
        T_Date.append("RFAI Date")
    
    
    columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date",'Task Name',"Task Owner","Start Date","Due Date","MS Completion Date","Task Closure","Custom Status"]+Template
    for col in columns_to_start:
        if col not in siteiddf.columns:
            siteiddf[col] = ''

    for col in T_Date:
        siteiddf[col] = siteiddf[col].apply(convertToDateBulkExport)
    siteiddf = siteiddf[columns_to_start]
    fullPath = excelWriteFunc.excelFileWriter(siteiddf, "Export_Task", "Task")
    return send_file(fullPath)




@sample_blueprint.route("/export/myTask",methods=['GET'])
@token_required
def sample_my_task_export(current_user):
    arra = [
        {
            '$match':{
                'deleteStatus':{'$ne':1},
                "assignerId":ObjectId(current_user['userUniqueId']),
            }
        }
    ]
    if request.args.get("mileStoneName") != None and request.args.get("mileStoneName") != "undefined":
        arra = arra + [
            {
                '$match': {
                    'Name': {
                        '$regex': re.escape(request.args.get("mileStoneName")), 
                        '$options': 'i'
                    }
                }
            }
        ]
    if request.args.get("subProject") != None and request.args.get("subProject") != "undefined":
        arra = arra + [
            {
                '$match': {
                    'SubProjectId':request.args.get("subProject")
                }
            }
        ]
    if request.args.get("customer")!=None and request.args.get("customer")!="undefined":
        arra = arra + [
            {
                '$match':{
                    'customerId':request.args.get('customer')
                }
            }
        ]
    arra = arra +[
        {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'assignerId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'Task Owner': '$result.empName'
            }
        }, {
            '$addFields': {
                'Task Owner': {
                    '$reduce': {
                        'input': '$Task Owner', 
                        'initialValue': '', 
                        'in': {
                            '$concat': [
                                '$$value', {
                                    '$cond': [
                                        {
                                            '$eq': [
                                                '$$value', ''
                                            ]
                                        }, '', ','
                                    ]
                                }, '$$this'
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                'Task Name': '$Name', 
                'Task Owner': 1, 
                'Start Date': '$mileStoneStartDate', 
                'Due Date': '$mileStoneEndDate', 
                'MS Completion Date': '$CC_Completion Date', 
                'Task Closure': 1, 
                'Custom Status': '$mileStoneStatus', 
                'siteuid': {
                    '$toString': '$siteId'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("milestone",arra)['data']
    milestonedf = pd.DataFrame.from_dict(response)
    if not milestonedf.empty:
        uid_list = milestonedf['siteuid'].tolist()
        uid_list_objectId = []
        for i in uid_list:
            uid_list_objectId.append(ObjectId(i))
        arra = [
            {
                '$match':{
                    '_id':{'$in':uid_list_objectId},
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if request.args.get("siteName") != None and request.args.get("siteName") != "undefined":
            arra = arra + [
                {
                    '$match': {
                        'Site Id':{
                            '$regex':re.escape(request.args.get("siteName").strip()),
                            '$options':"i"
                        }
                    }
                }
            ]
        arra = arra +[{
                '$addFields': {
                    'Site ID': '$Site Id', 
                    'Unique ID': '$Unique ID', 
                    'System ID': '$systemId',
                    "Sub Project":'$SubProjectId',
                    'Project ID':'$projectuniqueId',
                    'siteuid':{
                        '$toString':'$_id'
                    }
                }
            }, {
                '$project': {
                    'addedAt': 0, 
                    'projectuniqueId':0,
                    'SubProjectId':0,
                    'Project Type': 0, 
                    'Customer': 0, 
                    'customerId': 0, 
                    'siteEndDate': 0, 
                    'siteStartDate': 0, 
                    'siteStatus': 0, 
                    'siteBillingStatus': 0, 
                    'systemId': 0, 
                    'projectGroupid': 0, 
                    'projectType': 0, 
                    'Site Id': 0, 
                    'Project Group': 0, 
                    '_id':0,
                    'Task Name':0,
                    'Task Owner':0,
                    'MS Completion Date':0,
                    'Custom Status':0,
                    "Task Closure":0,
                    'Start Date':0,
                    "Due Date":0,
                    'PM Name':0,
                    'Circle':0
                }
            }
        ]
        siteiddf = pd.DataFrame.from_dict(cmo.finding_aggregate("SiteEngineer",arra)['data'])
        projectgroupdf = sample_pg()
        circledf = sample_circle()
        pmNamedf = sample_pmName()
        projectdf = sample_projectId(current_user['roleName'],current_user['userUniqueId'])
        projectTypedf = sample_PT()
        mergeData = circledf.merge(projectdf,on='Circle',how='right')
        projectdf['Circle'] = mergeData['circleName']
        mergeData = projectgroupdf.merge(projectdf,on='Project Group',how='right')
        projectdf['Project Group'] = mergeData['projectGroupName']
        mergeData = pmNamedf.merge(projectdf,on='PM Name',how='right')
        projectdf['PM Name'] = mergeData['NamePM']
        mergeData = projectTypedf.merge(siteiddf,on='Sub Project',how='right')
        siteiddf['Sub Project'] = mergeData['subProjectName']
        siteiddf['Project Type'] = mergeData['Project Type']
        sub_project_list = list(set(mergeData['Sub Project'].tolist()))
        mergeData = projectdf.merge(siteiddf,on='Project ID',how='right')
        siteiddf['Project ID'] = mergeData['projectId']
        siteiddf['Customer']  = mergeData['Customer']
        siteiddf['Project Group']  = mergeData['Project Group']
        siteiddf['PM Name']  = mergeData['PM Name']
        siteiddf['Circle']  = mergeData['Circle']
        
        project_id_list = list(set(mergeData['Project ID'].tolist()))
        subProjectIds = []
        for i in sub_project_list:
            subProjectIds.append(ObjectId(i))
            
        permission = invoiceaccess(current_user['roleId'])
        if permission == "W":
            invoicedf = sample_invoice(project_id_list)
            if not invoicedf.empty:
                siteiddf = siteiddf.merge(invoicedf,on="siteuid",how='left')
                
        siteiddf = siteiddf.merge(milestonedf,on=['siteuid'],how='left')
            
        Template = sample_template(subProjectIds,current_user['roleId'],T_D="")
        T_Date = sample_template(subProjectIds,current_user['roleId'],"Vishal")
        if "RFAI Date" not in T_Date:
            T_Date.append("RFAI Date")
        T_Date = T_Date+["Start Date","Due Date","MS Completion Date","Task Closure"]
        columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date",'Task Name',"Task Owner","Start Date","Due Date","MS Completion Date","Task Closure","Custom Status"]+Template
        missing_columns = {col: '' for col in columns_to_start if col not in siteiddf.columns}
        if missing_columns:
            siteiddf = pd.concat([siteiddf, pd.DataFrame(missing_columns, index=siteiddf.index)], axis=1)
                
        for col in T_Date:
            siteiddf[col] = siteiddf[col].apply(convertToDateBulkExport)
        
        siteiddf = siteiddf[columns_to_start]
        
        fullPath = excelWriteFunc.excelFileWriter(siteiddf, "Export_My_Task", "Task")
        return send_file(fullPath)
    else:
        siteiddf = pd.DataFrame()
        fullPath = excelWriteFunc.excelFileWriter(siteiddf, "Export_My_Task", "Task")
        return send_file(fullPath)

def sub_project3():
    arra = [
        {
            "$match": {
                "deleteStatus": {"$ne": 1},
                # '_id': {
                #     '$in': projectId_Object(empId)
                # }
            }
        },
        {"$project": {"projectType": 1, "_id": 0}},
        {
            "$lookup": {
                "from": "projectType",
                "localField": "projectType",
                "foreignField": "_id",
                "as": "result",
            }
        },
        {"$addFields": {"projectType": {
            "$arrayElemAt": ["$result.projectType", 0]}}},
        {"$group": {"_id": "$projectType", "projectType": {"$first": "$projectType"}}},
        {
            "$lookup": {
                "from": "projectType",
                "localField": "projectType",
                "foreignField": "projectType",
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {"$replaceRoot": {"newRoot": "$result"}},
        {"$addFields": {"uid": {"$toString": "$_id"}}},
        {
            "$group": {
                "_id": "$projectType",
                "projectType": {"$first": "$projectType"},
                "uid": {"$push": "$uid"},
            }
        },
        {"$sort": {"projectType": 1}},
        {"$project": {"projectType": 1, "uid": 1, "_id": 0}},
    ]
    response = cmo.finding_aggregate("project", arra)["data"]
    if len(response):
        return response
    else:
        return [{"projectType": "", "uid": []}]

def getActualMonthsQuery(months, year):
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    today = dtt.now(kolkata_tz)
    months_query = []
    
    for month in months:
        if month == 1:
            if today.day >= 26:
                months_query.append({
                    "$expr": {
                        "$and": [
                            {"$gte": ["$newAssigndate", f"{year}-01-26"]},
                            {"$lte": ["$newAssigndate", f"{year}-02-25"]}
                        ]
                    }
                })
            else:
                months_query.append({
                    "$expr": {
                        "$and": [
                            {"$gte": ["$newAssigndate", f"{year-1}-12-26"]},
                            {"$lte": ["$newAssigndate", f"{year}-01-25"]}
                        ]
                    }
                })
        else:
            prev_month = month - 1
            if today.day >= 26:
                next_month = 1 if month == 12 else month + 1  # Handle December → January transition
                months_query.append({
                    "$expr": {
                        "$and": [
                            {"$gte": ["$newAssigndate", f"{year}-{month:02d}-26"]},
                            {"$lte": ["$newAssigndate", f"{year if month != 12 else year + 1}-{next_month:02d}-25"]}
                        ]
                    }
                })
            else:
                months_query.append({
                    "$expr": {
                        "$and": [
                            {"$gte": ["$newAssigndate", f"{year}-{prev_month:02d}-26"]},
                            {"$lte": ["$newAssigndate", f"{year}-{month:02d}-25"]}
                        ]
                    }
                })
    
    return {
        "$match": {
            "$or": months_query
        }
    }     


def getSiteIDByName(value):
    query = [
    {
        '$match': {
            'Site Id': {
                '$regex': value, 
                '$options': 'i'
            }
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            }
        }
    }
]
    response = cmo.finding_aggregate("SiteEngineer", query)
    return [ObjectId(x["_id"]) for x in response["data"]]

def getVendorByNameAndCode(value, key):
    query = [
        {
            "$match" : {
                key : value if key == "vendorCode"  else {
                    "$regex" : value, "$options": "i"
                }
            }
        },
        {
        '$project': {
            '_id': {
                '$toString': '$_id'
            }
        }
    }
    ]
    response = cmo.finding_aggregate("userRegister",query)
    return [ObjectId(x["_id"]) for x in response["data"]]


def sub_project():
    arra = [
        {
            "$match": {
                "deleteStatus": {"$ne": 1},
                # '_id': {
                #     '$in': projectId_Object(empId)
                # }
            }
        },
        {"$project": {"projectType": 1, "_id": 0}},
        {
            "$lookup": {
                "from": "projectType",
                "localField": "projectType",
                "foreignField": "_id",
                "as": "result",
            }
        },
        {"$addFields": {"projectType": {
            "$arrayElemAt": ["$result.projectType", 0]}}},
        {"$group": {"_id": "$projectType", "projectType": {"$first": "$projectType"}}},
        {
            "$lookup": {
                "from": "projectType",
                "localField": "projectType",
                "foreignField": "projectType",
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {"$replaceRoot": {"newRoot": "$result"}},
        {"$addFields": {"uid": {"$toString": "$_id"}}},
        {
            "$group": {
                "_id": "$projectType",
                "projectType": {"$first": "$projectType"},
                "uid": {"$push": "$uid"},
            }
        },
        {"$sort": {"projectType": 1}},
        {"$project": {"projectType": 1, "uid": 1, "_id": 0}},
    ]
    response = cmo.finding_aggregate("project", arra)["data"]
    if len(response):
        return response
    else:
        return [{"projectType": "", "uid": []}]

def current_year():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_year = dtt.now(kolkata_tz).year
    return current_year

def current_month():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_month = dtt.now(kolkata_tz).month
    return current_month


def getActualMonthsQuery(months, year):
    months_query = []
    
    for month in months:
        if month == 1:
            months_query.append({
                "$expr": {
                    "$and": [
                        {"$gte": ["$newAssigndate", f"{year-1}-12-26"]},
                        {"$lte": ["$newAssigndate", f"{year}-01-25"]}
                    ]
                }
            })
        else:
            prev_month = month - 1
            months_query.append({
                "$expr": {
                    "$and": [
                        {"$gte": ["$newAssigndate", f"{year}-{prev_month:02d}-26"]},
                        {"$lte": ["$newAssigndate", f"{year}-{month:02d}-25"]}
                    ]
                }
            })
    
    return {
        "$match": {
            "$or": months_query
        }
    }     

@sample_blueprint.route("/export/vendor-project-tracking",methods=['GET'])

@token_required
def vendor_project_tracking_export(current_user):
    taskStatus = ["Open", "In Process", "Submit", "Approve", "Reject","Submit to Airtel", "Closed"]
    subProjectArray = (sub_project())[0]["uid"]

    year = request.args.get("year")
    months = [int(x) for x in request.args.get("Month").split(",")] if request.args.get("Month") else []
    customerId = request.args.get("customerId")
    siteId = getSiteIDByName(request.args.get("siteId")) if request.args.get("siteId") else None
    vendorCode = getVendorByNameAndCode(int(request.args.get("vendorCode")), "vendorCode") if request.args.get("vendorCode") else None
    vendorName = getVendorByNameAndCode(request.args.get("vendorName"), "vendorName") if request.args.get("vendorName") else None
    milestone = request.args.get("milestone")
    start = request.args.get("start")
    end = request.args.get("end")        
    projectType = request.args.get("projectType").split(",") if request.args.get("projectType") else None

    if (request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined"):
        taskStatus = ["Open", "In Process", "Submit", "Approve", "Reject", "Closed"]
    if (request.args.get("mileStoneStatus") != None and request.args.get("mileStoneStatus") != "undefined"):
        taskStatus = [request.args.get("mileStoneStatus")]
        if taskStatus == ["All"]:
            taskStatus = ["Open", "In Process", "Submit", "Approve", "Reject", "Closed"]
    arra = [
        {
            '$addFields': {
                'newAssigndate': {
                    '$dateToString': {
                        "format": "%Y-%m-%d",
                        'date': {
                            '$dateFromString': {
                                'dateString': '$assignDate', 
                                'format': '%d-%m-%Y'
                            }
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "typeAssigned": "Partner",
            }
        },   
    ]
    
    if customerId:
        arra[1]["$match"]["customerId"] = customerId
    if siteId:
        arra[1]["$match"]["siteId"] = {
            "$in": siteId
        }
    if vendorCode:
        arra[1]["$match"]["assignerId"] = {
            "$in": vendorCode
        }
    if vendorName:
        arra[1]["$match"]["assignerId"] = {
            "$in": vendorName
        }
    if milestone:
        arra[1]["$match"]["Name"] = { "$regex": milestone, "$options": "i" }
        
    if projectType:
        arra[1]["$match"]["SubProjectId"] = {
            "$in": projectType
        }
        
    andCond= []  
    
    # if  start and end:
    #     if months:
    #         return jsonify({ "status" : 400, "msg": "You can't apply year or month filter with date range filter"}), 400
    
    if start and end :
        
        
        arra[1]["$match"]["newAssigndate"] = {
                "$gte": start,
                "$lte": end
        }
          
    if year:
        andCond.append(
            {
        "$eq": [
        { "$year": { "$dateFromString": { "dateString": "$newAssigndate", "format": "%Y-%m-%d" } } },
        int(year)
        ]
    }
        )
    
    if months: 
        monthQuery = getActualMonthsQuery(months, int(year))
        arra.insert(2, monthQuery)
    
        
    if len(andCond):
       arra[1]["$match"]['$expr'] = {"$and":andCond}
    
    if (request.args.get("mileStoneName") != None and request.args.get("mileStoneName") != "undefined"):
        arra = arra + [
            {
                "$match": {
                    "Name": {
                        "$regex": re.escape(request.args.get("mileStoneName")),
                        "$options": "i",
                    }
                }
            }
        ]
    if (request.args.get("subProject") != None and request.args.get("subProject") != "undefined"):
        subProjectArray = request.args.get("subProject").split(",")
        arra = [{"$match": {"SubProjectId": {"$in": subProjectArray}}}] + arra
    # else:
    #     arra = [{"$match": {"SubProjectId": {"$in": subProjectArray}}}] + arra
    arra = arra + [
        
        {
            "$addFields": {
                "_id": {"$toString": "$_id"},
                "uniqueId": {"$toString": "$_id"},
                "mileStoneStartDate1": {"$toDate": "$mileStoneStartDate"},
                "mileStoneEndtDate1": {"$toDate": "$mileStoneEndDate"},
                "CC_Completion Date1": {"$toDate": "$CC_Completion Date"},
                'Task Closure': {
                '$toDate': '$Task Closure'
            }
            }
        },
        {
            "$addFields": {
               'Task Closure': {
                '$cond': {
                    'if': {
                        '$eq': [
                            {
                                '$type': '$Task Closure'
                            }, 'date'
                        ]
                    }, 
                    'then': {
                        '$dateToString': {
                            'date': '$Task Closure', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'else': ''
                }
            },
                "CC_Completion Date": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$CC_Completion Date1"}, "date"]},
                        "then": {
                            "$dateToString": {
                                "date": "$CC_Completion Date1",
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "else": "",
                    }
                },
                "taskageing": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$CC_Completion Date1"}, "date"]},
                        "then": {
                            "$round": {
                                "$divide": [
                                    {
                                        "$subtract": [
                                            "$mileStoneEndtDate1",
                                            "$CC_Completion Date1",
                                        ]
                                    },
                                    86400000,
                                ]
                            }
                        },
                        "else": {
                            "$round": {
                                "$divide": [
                                    {"$subtract": ["$mileStoneEndtDate1", "$$NOW"]},
                                    86400000,
                                ]
                            }
                        },
                    }
                },
                "mileStoneStartDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$mileStoneStartDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "mileStoneEndDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$mileStoneEndDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
            }
        },
        
        {
            "$lookup": {
                "from": "userRegister",
                "localField": "assignerId",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}, "type": "Partner"}},
                    {
                        "$project": {
                            "_id": 0,
                            "assignerName": "$vendorName",
                            "vendorCode": {
                                '$toString':"$vendorCode"},
                            "assignerId": {"$toString": "$_id"},
                        }
                    },
                ],
                "as": "assignerResult",
            }
        },
        {"$project": {"assignerId": 0}},
        {
            "$group": {
                "_id": "$siteId",
                "milestoneArray": {"$addToSet": "$$ROOT"},
                "siteId": {"$first": "$siteId"},
            }
        },
        {
            "$lookup": {
                "from": "SiteEngineer",
                "localField": "siteId",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$addFields": {"SubProjectId": {"$toObjectId": "$SubProjectId"}}},
                    {
                        "$lookup": {
                            "from": "projectType",
                            "localField": "SubProjectId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "SubTypeResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$SubTypeResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "SubProject": "$SubTypeResult.subProject",
                            "projectuniqueId": {"$toObjectId": "$projectuniqueId"},
                            "SubProjectId": {"$toString": "$SubProjectId"},
                            "projectType": "$SubTypeResult.projectType",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "project",
                            "localField": "projectuniqueId",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$addFields": {"PMId": {"$toObjectId": "$PMId"}}},
                                {
                                    "$addFields": {
                                        "customerId": {"$toObjectId": "$custId"}
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "projectGroup",
                                        "localField": "projectGroup",
                                        "foreignField": "_id",
                                        "pipeline": [
                                            {"$match": {"deleteStatus": {"$ne": 1}}},
                                            {
                                                "$lookup": {
                                                    "from": "customer",
                                                    "localField": "customerId",
                                                    "foreignField": "_id",
                                                    "pipeline": [
                                                        {
                                                            "$match": {
                                                                "deleteStatus": {
                                                                    "$ne": 1
                                                                }
                                                            }
                                                        }
                                                    ],
                                                    "as": "customer",
                                                }
                                            },
                                            {
                                                "$unwind": {
                                                    "path": "$customer",
                                                    "preserveNullAndEmptyArrays": True,
                                                }
                                            },
                                            {
                                                "$lookup": {
                                                    "from": "zone",
                                                    "localField": "zoneId",
                                                    "foreignField": "_id",
                                                    "pipeline": [
                                                        {
                                                            "$match": {
                                                                "deleteStatus": {
                                                                    "$ne": 1
                                                                }
                                                            }
                                                        }
                                                    ],
                                                    "as": "zone",
                                                }
                                            },
                                            {
                                                "$unwind": {
                                                    "path": "$zone",
                                                    "preserveNullAndEmptyArrays": True,
                                                }
                                            },
                                            {
                                                "$lookup": {
                                                    "from": "costCenter",
                                                    "localField": "costCenterId",
                                                    "foreignField": "_id",
                                                    "pipeline": [
                                                        {
                                                            "$match": {
                                                                "deleteStatus": {
                                                                    "$ne": 1
                                                                }
                                                            }
                                                        }
                                                    ],
                                                    "as": "costCenter",
                                                }
                                            },
                                            {
                                                "$unwind": {
                                                    "path": "$costCenter",
                                                    "preserveNullAndEmptyArrays": True,
                                                }
                                            },
                                            {
                                                "$addFields": {
                                                    "costCenter": "$costCenter.costCenter",
                                                    "zone": "$zone.shortCode",
                                                    "customer": "$customer.shortName",
                                                    "Customer": "$customer.customerName",
                                                }
                                            },
                                            {
                                                "$addFields": {
                                                    "projectGroupName": {
                                                        "$concat": [
                                                            "$customer",
                                                            "-",
                                                            "$zone",
                                                            "-",
                                                            "$costCenter",
                                                        ]
                                                    },
                                                    "projectGroup": {
                                                        "$toString": "$_id"
                                                    },
                                                }
                                            },
                                            {
                                                "$project": {
                                                    "_id": 0,
                                                    "projectGroupName": 1,
                                                    "projectGroup": 1,
                                                    "Customer": 1,
                                                    "costCenter": 1,
                                                }
                                            },
                                        ],
                                        "as": "projectGroupResult",
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "userRegister",
                                        "localField": "PMId",
                                        "foreignField": "_id",
                                        "pipeline": [
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
                                        ],
                                        "as": "PMarray",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$PMarray",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "PMName": "$PMarray.empName",
                                        "projectGroupName": {
                                            "$arrayElemAt": [
                                                "$projectGroupResult.projectGroupName",
                                                0,
                                            ]
                                        },
                                        "Customer": {
                                            "$arrayElemAt": [
                                                "$projectGroupResult.Customer",
                                                0,
                                            ]
                                        },
                                    }
                                },
                                {
                                    "$project": {
                                        "PMName": 1,
                                        "projectId": 1,
                                        "_id": 0,
                                        "Customer": 1,
                                        "projectGroupName": 1,
                                        'projectGroupId': '$projectGroup', 
                                        'customerId': 1
                                    }
                                },
                            ],
                            "as": "projectArray",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$projectArray",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                ],
                "as": "siteResult",
            }
        },
        {"$unwind": "$siteResult"},
        {"$addFields": {"siteResult.milestoneArray": "$milestoneArray"}},
        {"$replaceRoot": {"newRoot": "$siteResult"}},
    ]
    if (request.args.get("siteName") != None and request.args.get("siteName") != "undefined"):
        arra = arra + [
            {
                "$match": {
                    "Site Id": {
                        "$regex": re.escape(request.args.get("siteName").strip()),
                        "$options": "i",
                    }
                }
            }
        ]
    if (request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined"):
        siteStatus = [request.args.get("siteStatus")]
        if siteStatus == ["all"]:
            siteStatus = ["Open", "Close", "Drop"]
        arra = arra + [{"$match": {"siteStatus": {"$in": siteStatus}}}]
    # arra = (arra+ apireq.countarra("milestone", arra) + apireq.args_pagination(request.args))
    arra = arra + [
        {
            "$addFields": {
                "siteStartDate": {"$toDate": "$siteStartDate"},
                "siteEndDate": {"$toDate": "$siteStartDate"},
                "Site_Completion Date": {"$toDate": "$siteStartDate"},
            }
        },
        {
            "$addFields": {
                "uniqueId": {"$toString": "$_id"},
                "projectuniqueId": {"$toString": "$projectuniqueId"},
                "siteStartDate": {
                    "$dateToString": {
                        "date": "$siteStartDate",
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "siteEndDate": {
                    "$dateToString": {
                        "date": "$siteEndDate",
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "Site_Completion Date": {
                    "$dateToString": {
                        "date": "$Site_Completion Date",
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "PMName": "$projectArray.PMName",
                "projectId": "$projectArray.projectId",
                "subProject": "$SubProject",
                "siteageing": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date"}, "date"]},
                        "then": {
                            "$round": {
                                "$divide": [
                                    {
                                        "$subtract": [
                                            "$siteEndDate",
                                            "$Site_Completion Date",
                                        ]
                                    },
                                    86400000,
                                ]
                            }
                        },
                        "else": "",
                    }
                },
            }
        },
        {
            "$addFields": {
                "milestoneArray": {
                    "$sortArray": {"input": "$milestoneArray", "sortBy": {"_id": 1}}
                },
                "_id": {"$toString": "$_id"},
                "projectGroupName": "$projectArray.projectGroupName",
                "Customer": "$projectArray.Customer",
                # 'milestoneNames': {
                #             '$reduce': {
                #             'input': '$milestoneArray', 
                #             'initialValue': '', 
                #             'in': {
                #                 '$cond': [
                #                     {
                #                         '$eq': [
                #                             '$$value', ''
                #                         ]
                #                     }, '$$this.Name', {
                #                         '$concat': [
                #                             '$$value', ',', '$$this.Name'
                #                         ]
                #                     }
                #                 ]
                #             }
                #         }
                #     }, 
                # 'milestoneNames': {
                #         '$map': {
                #             'input': {
                #                 '$filter': {
                #                     'input': '$milestoneArray', 
                #                     'as': 'milestone', 
                #                     'cond': {
                #                         '$eq': [
                #                             '$$milestone.mileStoneStatus', 'Closed'
                #                         ]
                #                     }
                #                 }
                #             }, 
                #             'as': 'closed', 
                #             'in': '$$closed.Name'
                #         }
                #     }, 
                # 'assignerIdResult': {
                #             '$let': {
                #                 'vars': {
                #                     'allAssignerIds': {
                #                         '$reduce': {
                #                             'input': {
                #                                 '$filter': {
                #                                     'input': '$milestoneArray', 
                #                                     'as': 'milestone', 
                #                                     'cond': {
                #                                         '$eq': [
                #                                             '$$milestone.mileStoneStatus', 'Closed'
                #                                         ]
                #                                     }
                #                                 }
                #                             }, 
                #                             'initialValue': [], 
                #                             'in': {
                #                                 '$concatArrays': [
                #                                     '$$value', {
                #                                         '$map': {
                #                                             'input': '$$this.assignerResult', 
                #                                             'as': 'result', 
                #                                             'in': '$$result.assignerId'
                #                                         }
                #                                     }
                #                                 ]
                #                             }
                #                         }
                #                     }
                #                 }, 
                #                 'in': {
                #                     '$cond': [
                #                         {
                #                             '$eq': [
                #                                 {
                #                                     '$size': {
                #                                         '$setUnion': [
                #                                             '$$allAssignerIds'
                #                                         ]
                #                                     }
                #                                 }, 1
                #                             ]
                #                         }, {
                #                             '$arrayElemAt': [
                #                                 '$$allAssignerIds', 0
                #                             ]
                #                         }, None
                #                     ]
                #                 }
                #             }
                #         }
                
                }
                },
                 {
                    '$unwind': {
                        'path': '$milestoneArray'
                    }
                }, {
                    '$group': {
                        '_id': {
                            "workDescription" :"$milestoneArray.workDescription",
                            "siteId": "$milestoneArray.siteId"
                        },
                    'items': {
                        '$addToSet': '$$ROOT'
                    },
                    'milestones': {
                        '$addToSet': '$$ROOT.milestoneArray.Name'
                    },
                    'data': {
                        '$addToSet': {
                    'assignerIdResult': {
                        '$first': '$$ROOT.milestoneArray.assignerResult.assignerId'
                    },
                    'SubProjectId': '$$ROOT.milestoneArray.SubProjectId',
                    'siteId': '$$ROOT.milestoneArray.siteId',
                    'customerId': '$$ROOT.milestoneArray.customerId',
                    'projectGropupId': '$$ROOT.milestoneArray.projectGropupId'
                }
            }
        }
    }, {
        '$addFields': {
            'subProjectId': {
                '$toObjectId': '$SubProjectId'
            },
            'POEligibility': {
                '$cond': {
                    'if': {
                        '$eq': [
                            {
                                '$size': {
                                    '$filter': {
                                        'input': '$items', 
                                        'as': 'item', 
                                        'cond': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$$item.milestoneArray.mileStoneStatus', 'Closed'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$$item.milestoneArray.workDescription', None
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$$item.milestoneArray.workDescription', ''
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            }, {
                                '$size': '$items'
                            }
                        ]
                    }, 
                    'then': 'Yes', 
                    'else': 'No'
                }
            },
            'assignerIdResult': {
                '$toObjectId': {
                    '$arrayElemAt': [
                        '$data.assignerIdResult', 0
                    ]
                }
            },
            'subProjectId': {
                '$toObjectId': {
                    '$arrayElemAt': [
                        '$data.SubProjectId', 0
                    ]
                }
            },
            'siteId': {
                '$toObjectId': {
                    '$arrayElemAt': [
                        '$data.siteId', 0
                    ]
                }
            },
            'projectGroupId': {
                '$toObjectId': {
                    '$arrayElemAt': [
                        '$data.projectGropupId', 0
                    ]
                }
            },
            'customerId': {
                '$toObjectId': {
                    '$arrayElemAt': [
                        '$data.customerId', 0
                    ]
                }
            }
        }
    }, {
        '$lookup': {
            'let': {
                'milestoneNames': '$milestones',
                'subProjectId': '$subProjectId',
                'projectGroupId': '$projectGroupId',
                'customerId': '$customerId'
            },
            'from': 'vendorCostMilestone',
            'localField': 'assignerIdResult',
            'foreignField': 'vendorId',
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$setIsSubset': [
                                        '$milestoneList', '$$milestoneNames'
                                    ]
                                }, {
                                    '$setIsSubset': [
                                        '$$milestoneNames', '$milestoneList'
                                    ]
                                }, {
                                    '$eq': [
                                        '$subProjectId', '$$subProjectId'
                                    ]
                                }, {
                                    '$eq': [
                                        '$projectGroup', '$$projectGroupId'
                                    ]
                                }, {
                                    '$eq': [
                                        '$customerId', '$$customerId'
                                    ]
                                }
                            ]
                        }
                    }
                }
            ],
            'as': 'itemCodeResults'
        }
    }, {
        '$addFields': {
            'subProjectId': {
                '$toString': '$subProjectId'
            },
            'vendorItemCode': {
                '$arrayElemAt': [
                    '$itemCodeResults.itemCode', {
                        '$indexOfArray': [
                            '$itemCodeResults.rate', {
                                '$min': '$itemCodeResults.rate'
                            }
                        ]
                    }
                ]
            }, 
            'vendorRate': {
                '$min': '$itemCodeResults.rate'
            },
            'MS1Date': {
                '$let': {
                    'vars': {
                        'ms1': {
                            '$arrayElemAt': [
                                {
                                    '$filter': {
                                        'input': '$milestoneArray',
                                        'as': 'item',
                                        'cond': {
                                            '$eq': [
                                                '$$item.Name', 'MS1'
                                            ]
                                        }
                                    }
                                }, 0
                            ]
                        }
                    },
                    'in': {
                        '$ifNull': [
                            '$$ms1.CC_Completion Date', ''
                        ]
                    }
                }
            },
            'MS2Date': {
                '$let': {
                    'vars': {
                        'ms1': {
                            '$arrayElemAt': [
                                {
                                    '$filter': {
                                        'input': '$milestoneArray',
                                        'as': 'item',
                                        'cond': {
                                            '$eq': [
                                                '$$item.Name', 'MS2'
                                            ]
                                        }
                                    }
                                }, 0
                            ]
                        }
                    },
                    'in': {
                        '$ifNull': [
                            '$$ms1.CC_Completion Date', ''
                        ]
                    }
                }
            }
        }
    }, {
        '$unwind': {
            'path': '$items'
        }
    }, {
        '$addFields': {
            'items.rate': {
                '$cond': {
                    'if': {
                        '$gt': [
                            {
                                '$size': '$itemCodeResults'
                            }, 0
                        ]
                    }, 
                    'then': {
                        '$arrayElemAt': [
                            '$itemCodeResults.rate', {
                                '$indexOfArray': [
                                    '$itemCodeResults.rate', {
                                        '$min': '$itemCodeResults.rate'
                                    }
                                ]
                            }
                        ]
                    }, 
                    'else': None
                }
            }, 
            'items.itemCode': {
                '$cond': {
                    'if': {
                        '$gt': [
                            {
                                '$size': '$itemCodeResults'
                            }, 0
                        ]
                    }, 
                    'then': {
                        '$arrayElemAt': [
                            '$itemCodeResults.itemCode', {
                                '$indexOfArray': [
                                    '$itemCodeResults.rate', {
                                        '$min': '$itemCodeResults.rate'
                                    }
                                ]
                            }
                        ]
                    }, 
                    'else': None
                }
            },
            'items.POEligibility': '$POEligibility'
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$items'
        }
    }, {
        '$addFields': {
            'milestoneArray.siteId': {
                '$toString': '$milestoneArray.siteId'
            }
        }
    },
    {
        '$group': {
            '_id': '$_id',
            'items': {
                '$addToSet': {
                    '$mergeObjects': [
                        '$$ROOT.milestoneArray', {
                            'itemCode': '$$ROOT.itemCode',
                            'rate': '$$ROOT.rate',
                            'POEligibility': '$$ROOT.POEligibility'
                        }
                    ]
                }
            },
            'realData': {
                '$first': '$$ROOT'
            }
        }
    }, {
        '$addFields': {
            'realData.milestoneArray': '$items'
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$realData',
        }
    },
    {   
     '$addFields': {
        "milestoneArray": {
            "$sortArray": {
            "input": "$milestoneArray",
            "sortBy": {
                "_id": 1
            }
            }
        }
        }
    },
    {
        '$addFields': {
            'milestoneArray': {
                '$map': {
                    'input': '$milestoneArray', 
                    'as': 'item', 
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'ccDateParsed': {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$ne': [
                                                        '$$item.CC_Completion Date', None
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$$item.CC_Completion Date', ''
                                                    ]
                                                },
                                                 {
                                                    '$eq': [
                                                        '$$item.mileStoneStatus','Closed'
                                                    ]
                                                 }
                                            ]
                                        }, {
                                            '$dateFromString': {
                                                'dateString': '$$item.CC_Completion Date', 
                                                'format': '%d-%m-%Y'
                                            }
                                        }, None
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'lastDate': {
                '$let': {
                    'vars': {
                        'lastObj': {
                            '$arrayElemAt': [
                                '$milestoneArray', -1
                            ]
                        }
                    }, 
                    'in': '$$lastObj.ccDateParsed'
                }
            }
        }
    }, {
        '$addFields': {
            'customMonth': {
                '$let': {
                    'vars': {
                        'day': {
                            '$dayOfMonth': '$lastDate'
                        }, 
                        'month': {
                            '$month': '$lastDate'
                        }
                    }, 
                    'in': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$$day', 26
                                ]
                            }, {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$$month', 12
                                        ]
                                    }, 1, {
                                        '$add': [
                                            '$$month', 1
                                        ]
                                    }
                                ]
                            }, '$$month'
                        ]
                    }
                }
            }, 
            'customYear': {
                '$let': {
                    'vars': {
                        'day': {
                            '$dayOfMonth': '$lastDate'
                        }, 
                        'month': {
                            '$month': '$lastDate'
                        }, 
                        'year': {
                            '$year': '$lastDate'
                        }
                    }, 
                    'in': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$month', 12
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$day', 26
                                        ]
                                    }
                                ]
                            }, {
                                '$add': [
                                    '$$year', 1
                                ]
                            }, '$$year'
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'milestoneArray': {
                '$map': {
                    'input': '$milestoneArray', 
                    'as': 'item', 
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'completionMonth': '$customMonth', 
                                'completionYear': '$customYear'
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$project': {
            'lastDate': 0, 
            'customMonth': 0, 
            'customYear': 0, 
            'milestoneArray.ccDateParsed': 0
        }
    },
    {
        '$project': {
            'projectArray': 0, 
            'SubTypeResult': 0
        }
    },
        {"$sort": {"_id": 1}},
    ]
    print(arra,'arraarraarraarraarraarra')
    
    # response = cmo.finding_aggregate("milestone", arra)
    response = cmo.finding_aggregate("milestone", arra)["data"]
    sites_data = []
    month_map = {
        1: 'Jan',
        2: 'Feb',
        3: 'Mar',
        4: 'Apr',
        5: 'May',
        6: 'Jun',
        7: 'Jul',
        8: 'Aug',
        9: 'Sep',
        10: 'Oct',
        11: 'Nov',
        12: 'Dec'
    }
    for row in response:
        for milestone in row["milestoneArray"]:
            data = {
                "Site Id": row["Site Id"],
                "System Id" : row["systemId"],
                "Customer": row["Customer"],
                "Project Group": row["projectGroupName"],
                "Project ID": row["projectId"],
                "Project Type": row["projectType"],
                "Sub Project": row["subProject"],
                "Ageing": milestone["taskageing"],
                "MS1 Completion Date" : "",
                "MS2 Completion Date":"",
                "Milestone Name": milestone["Name"],
                "Vendor Name": milestone["assignerResult"][0]["assignerName"] if len(milestone["assignerResult"]) else "",
                "Vendor ID": milestone["assignerResult"][0]["vendorCode"] if milestone["assignerResult"] else "",
                "Task Allocation Date": milestone["assignDate"],
                "MS Completition Date": milestone["CC_Completion Date"],
                "MS Completion Month":milestone["completionMonth"],
                "MS Completion Year":milestone["completionYear"],
                "Task Closure Date": milestone["Task Closure"],
                "Predecessor": milestone["Predecessor"],
                "MS Status" : milestone["mileStoneStatus"],
                "Vendor Item Code": milestone["itemCode"],
                "Vendor Rate" : milestone["rate"],
                "PO eligibility (Yes/No)": milestone["POEligibility"]
            }
            sites_data.append(data)
    dateList=["Task Allocation Date",'MS Completition Date','Task Closure Date']
    
    sites_df = pd.DataFrame(sites_data)
    for col in dateList:
        if col in sites_df.columns:
            sites_df[col] = sites_df[col].apply(convertToDateBulkExport)
    sites_df['MS Completion Month'] = sites_df['MS Completion Month'].map(month_map)
    fullPath = excelWriteFunc.excelFileWriter(sites_df, "Export_vendor_project_tracking", "Vendor Project Tracking")
    return send_file(fullPath)


@sample_blueprint.route("/export/dateRange",methods=['GET'])
def export_dataRange():
    milestoneArra = [
        # {
        #     '$match': {
        #         'SubProjectId': {
        #             '$in': [
        #                 '66a8c947e2e6dc3e9e6b486b', '66a8c99fe2e6dc3e9e6b486c', '66a8c9ede2e6dc3e9e6b486d', '676e8be42194233723e123b3'
        #             ]
        #         }
        #     }
        # },
        {
            '$match': {
                'customerId': {
                    "$in":["667d593927f39f1ac03d7863","6735cb7c0e28f3e385b7f0c4"]
                }
            }
        },
        {
            '$addFields': {
                'CC_Completion Date': {
                    '$toDate': '$CC_Completion Date'
                }
            }
        }, {
            '$addFields': {
                'CC_Completion Date': {
                    '$dateAdd': {
                        'startDate': '$CC_Completion Date', 
                        'unit': 'minute', 
                        'amount': 330
                    }
                }, 
                'startDate': {
                    '$dateFromString': {
                        'dateString': '2025-06-26'
                    }
                }, 
                'endDate': {
                    '$dateFromString': {
                        'dateString': '2025-07-25T05:30:00.000+00:00'
                    }
                }
            }
        }, {
            '$match': {
                '$expr': {
                    '$and': [
                        {
                            '$gte': [
                                '$CC_Completion Date', '$startDate'
                            ]
                        }, {
                            '$lte': [
                                '$CC_Completion Date', '$endDate'
                            ]
                        }
                    ]
                }
            }
        }, {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'assignerId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }, {
                        '$addFields':{
                            'empCode':{
                                '$ifNull':['$empCode','$vendorCode']
                            },
                            'empName':{
                                '$ifNull':['$empName','$vendorName']
                            },
                        }
                    }, {
                        '$addFields': {
                            'empCode': {
                                '$toString': '$empCode'
                            },
                            'empName': {
                                '$toString': '$empName'
                            }
                        }
                    }, {
                        '$addFields': {
                            'empName': {
                                '$concat': [
                                    '$empName', '(', '$empCode', ')'
                                ]
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'Task Owner': '$result.empName'
            }
        }, {
            '$addFields': {
                'Task Owner': {
                    '$reduce': {
                        'input': '$Task Owner', 
                        'initialValue': '', 
                        'in': {
                            '$concat': [
                                '$$value', {
                                    '$cond': [
                                        {
                                            '$eq': [
                                                '$$value', ''
                                            ]
                                        }, '', ','
                                    ]
                                }, '$$this'
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                'Task Name': '$Name', 
                'Task Owner': 1, 
                'Start Date': '$mileStoneStartDate', 
                'Due Date': '$mileStoneEndDate', 
                'MS Completion Date': '$CC_Completion Date', 
                'Task Closure': 1, 
                'Custom Status': '$mileStoneStatus', 
                'siteuid': {
                    '$toString': '$siteId'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("milestone",milestoneArra)['data']
    if response:
        siteuid = []
        siteuidArra = []
        for i in response:
            siteuid.append(ObjectId(i['siteuid']))
            siteuidArra.append(i['siteuid'])
        df1 = pd.DataFrame(response)

        siteArra = [
            {
                '$match':{
                    '_id':{
                        '$in':siteuid
                    }
                }
            }, {
                '$project': {
                    'Site ID': '$Site Id', 
                    'Unique ID': '$Unique ID', 
                    'System ID': '$systemId', 
                    'siteuid': {
                        '$toString': '$_id'
                    }, 
                    'RFAI Date': 1, 
                    '_id': 0, 
                    'projectGropupId': 1, 
                    'circleId': 1, 
                    'SubProjectId': 1, 
                    'customerId': 1, 
                    'projectuniqueId': 1
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",siteArra)['data']
        df2 = pd.DataFrame(response)

        pgArra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    },
                }
            }, {
                '$lookup': {
                    'from': 'customer', 
                    'localField': 'customerId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }
                    ], 
                    'as': 'customer'
                }
            }, {
                '$unwind': {
                    'path': '$customer', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'zone', 
                    'localField': 'zoneId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }
                    ], 
                    'as': 'zone'
                }
            }, {
                '$unwind': {
                    'path': '$zone', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'costCenter', 
                    'localField': 'costCenterId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }
                    ], 
                    'as': 'costCenter'
                }
            }, {
                '$unwind': {
                    'path': '$costCenter', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'costCenter': '$costCenter.costCenter', 
                    'zone': '$zone.shortCode', 
                    'customer': '$customer.shortName'
                }
            }, {
                '$addFields': {
                    'Project Group': {
                        '$concat': [
                            '$customer', '-', '$zone', '-', '$costCenter'
                        ]
                    }, 
                    'projectGropupId': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'Project Group': 1, 
                    'projectGropupId': 1
                }
            }
        ]
        response = cmo.finding_aggregate("projectGroup",pgArra)['data']
        df3 = pd.DataFrame(response)


        circleArra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'circleId': {
                        '$toString': '$_id'
                    }, 
                    'Circle': '$circleCode', 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("circle",circleArra)['data']
        df4 = pd.DataFrame(response)

        mergedDF = df2.merge(df4,on="circleId",how="left")

        subProjectArra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'SubProjectId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0, 
                    'Project Type': '$projectType', 
                    'Sub Project': '$subProject'
                }
            }
        ]
        response = cmo.finding_aggregate("projectType",subProjectArra)['data']
        df5 = pd.DataFrame(response)

        mergedDF = mergedDF.merge(df5,on="SubProjectId",how="left")

        projectIdArra = [
            {
                '$addFields': {
                    'PMId': {
                        '$toObjectId': '$PMId'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'PMId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$addFields': {
                                'empCode': {
                                    '$toString': '$empCode'
                                }
                            }
                        }, {
                            '$addFields': {
                                'empName': {
                                    '$concat': [
                                        '$empName', '(', '$empCode', ')'
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'projectuniqueId': {
                        '$toString': '$_id'
                    }, 
                    'Project ID': '$projectId', 
                    'PM Name': {
                        '$arrayElemAt': [
                            '$result.empName', 0
                        ]
                    },
                    'projectGropupId': {
                        '$toString': '$projectGroup'
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("project",projectIdArra)['data']
        df6 = pd.DataFrame(response)

        df6 = df3.merge(df6,on="projectGropupId",how='right')

        mergedDF = mergedDF.merge(df6,on="projectuniqueId",how="left")

        customerArra = [
            {
                '$project': {
                    'Customer': '$customerName', 
                    'customerId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("customer",customerArra)['data']
        df7 = pd.DataFrame(response)

        mergedDF = mergedDF.merge(df7,on="customerId",how="left")

        mergedDF = mergedDF.merge(df1,on="siteuid",how="left")



        invoiceArra = [
            {
                '$match': {
                    'siteId': {
                        '$in': siteuidArra
                    }
                }
            }, {
                '$project': {
                    "siteuid":'$siteId',  
                    'Item Code': '$itemCode', 
                    'Billing Status': '$status', 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("invoice",invoiceArra)
        invoicedf = pd.DataFrame.from_dict(response['data'])
        if not invoicedf.empty:
            invoicedf = invoicedf.groupby('siteuid').agg(lambda x: ', '.join(x.astype(str))).reset_index()
            invoicedf['Billing Status'] = invoicedf['Billing Status'].apply(lambda x: x.split(',')[-1].strip())
            
            invoicedf = invoicedf.groupby('siteuid').agg(lambda x: ', '.join(map(str, x))).reset_index()
            invoicedf['Billing Status'] = invoicedf.groupby('siteuid')['Billing Status'].apply(lambda x: x.iloc[-1]).reset_index(drop=True)

            mergedDF = mergedDF.merge(invoicedf,on="siteuid",how="left")


        columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date",'Task Name',"Task Owner","Start Date","Due Date","MS Completion Date","Task Closure","Custom Status","Item Code","Billing Status"]
        for col in columns_to_start:
            if col not in mergedDF.columns:
                mergedDF[col] = ''

        for col in ['RFAI Date',"Start Date","Due Date","MS Completion Date","Task Closure"]:
            mergedDF[col] = mergedDF[col].apply(convertToDateBulkExport)

        mergedDF = mergedDF[columns_to_start]
        fullPath = excelWriteFunc.excelFileWriter(mergedDF, "Export_Task", "Task")
        return send_file(fullPath)
    else:
        df = pd.DataFrame()
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Task", "Task")
        return send_file(fullPath)
    



@sample_blueprint.route("/export/fullDump",methods=['GET'])
def export_fullDump():
    

    siteArra = [
        {
            '$project': {
                'Site ID': '$Site Id', 
                'Unique ID': '$Unique ID', 
                'System ID': '$systemId', 
                'siteuid': {
                    '$toString': '$_id'
                }, 
                'RFAI Date': 1, 
                '_id': 0, 
                'projectGropupId': 1, 
                'circleId': 1, 
                'SubProjectId': 1, 
                'customerId': 1, 
                'projectuniqueId': 1,
                "OA_(COMMERCIAL_TRAFFIC_PUT_ON_AIR)_(MS1)_DATE":1,
                "MAPA_INCLUSION_DATE":1
            }
        }
    ]
    response = cmo.finding_aggregate("SiteEngineer",siteArra)['data']
    df2 = pd.DataFrame(response)

    pgArra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                },
            }
        }, {
            '$lookup': {
                'from': 'customer', 
                'localField': 'customerId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'customer'
            }
        }, {
            '$unwind': {
                'path': '$customer', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'zone', 
                'localField': 'zoneId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'zone'
            }
        }, {
            '$unwind': {
                'path': '$zone', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'costCenter', 
                'localField': 'costCenterId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ], 
                'as': 'costCenter'
            }
        }, {
            '$unwind': {
                'path': '$costCenter', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'costCenter': '$costCenter.costCenter', 
                'zone': '$zone.shortCode', 
                'customer': '$customer.shortName'
            }
        }, {
            '$addFields': {
                'Project Group': {
                    '$concat': [
                        '$customer', '-', '$zone', '-', '$costCenter'
                    ]
                }, 
                'projectGropupId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'Project Group': 1, 
                'projectGropupId': 1
            }
        }
    ]
    response = cmo.finding_aggregate("projectGroup",pgArra)['data']
    df3 = pd.DataFrame(response)


    circleArra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'circleId': {
                    '$toString': '$_id'
                }, 
                'Circle': '$circleCode', 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("circle",circleArra)['data']
    df4 = pd.DataFrame(response)

    mergedDF = df2.merge(df4,on="circleId",how="left")

    subProjectArra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'SubProjectId': {
                    '$toString': '$_id'
                }, 
                '_id': 0, 
                'Project Type': '$projectType', 
                'Sub Project': '$subProject'
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",subProjectArra)['data']
    df5 = pd.DataFrame(response)

    mergedDF = mergedDF.merge(df5,on="SubProjectId",how="left")

    projectIdArra = [
        {
            '$addFields': {
                'PMId': {
                    '$toObjectId': '$PMId'
                }
            }
        }, {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'PMId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }, {
                        '$addFields': {
                            'empCode': {
                                '$toString': '$empCode'
                            }
                        }
                    }, {
                        '$addFields': {
                            'empName': {
                                '$concat': [
                                    '$empName', '(', '$empCode', ')'
                                ]
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$project': {
                '_id': 0, 
                'projectuniqueId': {
                    '$toString': '$_id'
                }, 
                'Project ID': '$projectId', 
                'PM Name': {
                    '$arrayElemAt': [
                        '$result.empName', 0
                    ]
                },
                'projectGropupId': {
                    '$toString': '$projectGroup'
                }
            }
        }
    ]
    response = cmo.finding_aggregate("project",projectIdArra)['data']
    df6 = pd.DataFrame(response)

    df6 = df3.merge(df6,on="projectGropupId",how='right')

    mergedDF = mergedDF.merge(df6,on="projectuniqueId",how="left")

    customerArra = [
        {
            '$project': {
                'Customer': '$customerName', 
                'customerId': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("customer",customerArra)['data']
    df7 = pd.DataFrame(response)

    mergedDF = mergedDF.merge(df7,on="customerId",how="left")



    invoiceArra = [
        {
            '$project': {
                "siteuid":'$siteId',  
                'Item Code': '$itemCode', 
                'Billing Status': '$status', 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("invoice",invoiceArra)
    invoicedf = pd.DataFrame.from_dict(response['data'])
    if not invoicedf.empty:
        invoicedf = invoicedf.groupby('siteuid').agg(lambda x: ', '.join(x.astype(str))).reset_index()
        invoicedf['Billing Status'] = invoicedf['Billing Status'].apply(lambda x: x.split(',')[-1].strip())
        
        invoicedf = invoicedf.groupby('siteuid').agg(lambda x: ', '.join(map(str, x))).reset_index()
        invoicedf['Billing Status'] = invoicedf.groupby('siteuid')['Billing Status'].apply(lambda x: x.iloc[-1]).reset_index(drop=True)

        mergedDF = mergedDF.merge(invoicedf,on="siteuid",how="left")


    columns_to_start = ['Customer', "Project Group","Project ID","Project Type","Sub Project","PM Name","Circle","Site ID","Unique ID","System ID","RFAI Date","OA_(COMMERCIAL_TRAFFIC_PUT_ON_AIR)_(MS1)_DATE","MAPA_INCLUSION_DATE","Item Code","Billing Status"]
    for col in columns_to_start:
        if col not in mergedDF.columns:
            mergedDF[col] = ''

    for col in ['RFAI Date',"OA_(COMMERCIAL_TRAFFIC_PUT_ON_AIR)_(MS1)_DATE","MAPA_INCLUSION_DATE"]:
        mergedDF[col] = mergedDF[col].apply(convertToDateBulkExport)

    mergedDF = mergedDF[columns_to_start]
    fullPath = excelWriteFunc.excelFileWriter(mergedDF, "Export_Sites", "Sites")
    return send_file(fullPath)



def apnafunction():
    explist = [
        {"ExpenseNo": "EXP2025000059765", "UniqueId": "688c7a326bb7683412ce141f"},
        {"ExpenseNo": "EXP2025000059766", "UniqueId": "688c7b476bb7683412ce1422"},
        {"ExpenseNo": "EXP2025000059769", "UniqueId": "688cae5d6bb7683412ce14d7"},
        {"ExpenseNo": "EXP2025000059770", "UniqueId": "688d7c056bb7683412ce16fd"},
        {"ExpenseNo": "EXP2025000059771", "UniqueId": "688f8c86ff2e213f8fc3a09a"},
        {"ExpenseNo": "EXP2025000059772", "UniqueId": "689068b4ff2e213f8fc3a69e"},
        {"ExpenseNo": "EXP2025000059773", "UniqueId": "689086a0ff2e213f8fc3aa23"},
        {"ExpenseNo": "EXP2025000059774", "UniqueId": "6890d6a7e964f9bd3cf3c29e"},
        {"ExpenseNo": "EXP2025000059775", "UniqueId": "68919e75e964f9bd3cf3c6d4"},
        {"ExpenseNo": "EXP2025000059776", "UniqueId": "6891b3d0e964f9bd3cf3cd65"},
        {"ExpenseNo": "EXP2025000059777", "UniqueId": "6891bfbde964f9bd3cf3cea0"},
        {"ExpenseNo": "EXP2025000059778", "UniqueId": "68920909e964f9bd3cf3dd05"},
        {"ExpenseNo": "EXP2025000059779", "UniqueId": "689209a2e964f9bd3cf3dd07"},
        {"ExpenseNo": "EXP2025000059780", "UniqueId": "68920be3e964f9bd3cf3dd0b"},
        {"ExpenseNo": "EXP2025000059781", "UniqueId": "68920ce5e964f9bd3cf3dd0e"},
        {"ExpenseNo": "EXP2025000059782", "UniqueId": "68920daae964f9bd3cf3dd11"},
        {"ExpenseNo": "EXP2025000059783", "UniqueId": "68920e9ce964f9bd3cf3dd14"},
        {"ExpenseNo": "EXP2025000059784", "UniqueId": "68920f43e964f9bd3cf3dd17"},
        {"ExpenseNo": "EXP2025000059785", "UniqueId": "68920fbce964f9bd3cf3dd1a"},
        {"ExpenseNo": "EXP2025000059786", "UniqueId": "68921099e964f9bd3cf3dd1d"},
        {"ExpenseNo": "EXP2025000059787", "UniqueId": "68921165e964f9bd3cf3dd20"},
        {"ExpenseNo": "EXP2025000059788", "UniqueId": "689211f7e964f9bd3cf3dd23"},
        {"ExpenseNo": "EXP2025000059789", "UniqueId": "6892135fe964f9bd3cf3dd26"},
        {"ExpenseNo": "EXP2025000059790", "UniqueId": "6892141ae964f9bd3cf3dd29"},
        {"ExpenseNo": "EXP2025000059791", "UniqueId": "689214e7e964f9bd3cf3dd2c"},
        {"ExpenseNo": "EXP2025000059792", "UniqueId": "68921571e964f9bd3cf3dd2f"},
        {"ExpenseNo": "EXP2025000059793", "UniqueId": "689215ede964f9bd3cf3dd32"},
        {"ExpenseNo": "EXP2025000059794", "UniqueId": "689216b6e964f9bd3cf3dd35"},
        {"ExpenseNo": "EXP2025000059795", "UniqueId": "689217f7e964f9bd3cf3dd38"},
        {"ExpenseNo": "EXP2025000059796", "UniqueId": "689218cfe964f9bd3cf3dd3b"},
        {"ExpenseNo": "EXP2025000059797", "UniqueId": "689219bde964f9bd3cf3dd3e"},
        {"ExpenseNo": "EXP2025000059799", "UniqueId": "68922d1be964f9bd3cf3dd43"},
        {"ExpenseNo": "EXP2025000059800", "UniqueId": "6892350088b37699afe08c2e"},
        {"ExpenseNo": "EXP2025000059801", "UniqueId": "689236a8a6c4c2958c6cddc3"}
    ]
    for item in explist:
        cmo.updating("Expenses",{'_id':ObjectId(item['UniqueId'])},{'ExpenseNo':item['ExpenseNo']},False)
        print("====Complete====")


