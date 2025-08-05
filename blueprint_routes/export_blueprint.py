from base import *
from bson import ObjectId
import io
import common.excel_write as excelWriteFunc
from datetime import datetime as dtt
from common.config import check_and_convert_date as check_and_convert_date
from common.config import convert_datetime_to_custom_format as custumdatetime
import numpy as np
from common.config import is_valid_mongodb_objectid as is_valid_mongodb_objectid
from common.config import strtodate as strtodate
from common.config import convertToDateBulkExport as convertToDateBulkExport
from blueprint_routes.currentuser_blueprint import projectId_str,projectGroup_str,sub_project,costCenter_str
from blueprint_routes.poLifeCycle_blueprint import projectGroup
from blueprint_routes.poLifeCycle_blueprint import projectid
from blueprint_routes.poLifeCycle_blueprint import subproject
from blueprint_routes.poLifeCycle_blueprint import masterunitRate
from common.mongo_operations import db as database
from common.commonDateFilter import dateFilter
from common.config import get_current_date_timestamp as get_current_date_timestamp
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle, Paragraph,PageBreak, Spacer, Image as PlatypusImage)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet

import datetime
import pytz
def current_date_str():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%d-%m-%Y")
    current_time = asia_now.strftime("%d/%m/%Y, %H:%M:%S")
    return current_time

def current_date():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%Y-%m-%d"+"T00:00:00")
    # current_time = asia_now.strftime("%d/%m/%Y, %H:%M:%S")
    return current_date

def current_time1():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%d-%m-%Y")
    return current_date

def current_month():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    return asia_now.month

def rename_columns(col):
    match = re.match(r'(pv|amount)-(\d+)-(\d+)', col)
    if match:
        prefix, month, year = match.groups()
        month_names = {
            '1': 'Jan', '2': 'Feb', '3': 'Mar', '4': 'Apr', '5': 'May', '6': 'Jun',
            '7': 'Jul', '8': 'Aug', '9': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
        }
        month_name = month_names.get(month, month)
        if prefix == 'pv':
            return f'PV Target({month_name} {year})'
        elif prefix == 'amount':
            return f'Achievement ({month_name} {year})'
    elif col == "customer":
        return "Customer"
    elif col == "costCenter":
        return "Cost Center"
    elif col == "businessUnit":
        return "Business Unit"
    else:
        return col
    
def custom_sort(col):
    match = re.match(r'(pv|amount)-(\d+)-(\d+)', col)
    if match:
        prefix, month, year = match.groups()
        month = int(month)  
        year = int(year)
        order = 0 if prefix == 'pv' else 1  
        return (year, month, order) 
    return (9999, 9999, 9999) 

export_blueprint = Blueprint("export_blueprint", __name__)

@export_blueprint.route("/template/<filePath>", methods=["GET"])
@token_required
def template_download(current_user, filePath):
    print(filePath,"skjdhfgkjfd")
    if filePath in ["Site Engg", "Financials", "Issues", "Tracking","Template","Planning Details","Site Details","Checklist","Snap","Acceptance Log"]:
        filePath = "Template.xlsx"
    if filePath == "MileStone":
        filePath = "MileStone.xlsx"
    if filePath == "Commercial":
        filePath = "Commercial.xlsx"
    if filePath == 'MDB_Approver.xlsx':
        filePath='MDB_Approver.xlsx'

    return send_file(os.path.join(os.getcwd(), "templateList", filePath))


@export_blueprint.route("/export/deliveryPva/template/<id>", methods=["GET"])
def export_delivery_pva_tempalte(id=None):
    arra = [
        {
            '$match': {
                'customer': ObjectId(id)
            }
        }, {
            '$unwind': {
                'path': '$subProjectIds', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'subProjectIds', 
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
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$project': {
                'subProject': {
                    '$concat': [
                        '$result.subProject', '(', '$projectType', ')'
                    ]
                }, 
                '_id': 0
            }
        }
    ]
    res = cmo.finding_aggregate("masterSubProject",arra)['data']
    Headers = ["Customer","Year","Month","Circle"]
    if len(res):
        for i in res:
            Headers.append(i['subProject'])
    df = pd.DataFrame(columns=Headers)
    fullPath = excelWriteFunc.excelFileWriter(df, "Export_Delivery_PVA_Template", "Delivery_PVA")
    return send_file(fullPath)

@export_blueprint.route("/export/partnerWorkDescription", methods=["GET"])
@token_required
def export_partner_work_description(current_user):
    arra = [
        {
            '$lookup': {
                'from': 'customer', 
                'localField': 'customer', 
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
                'as': 'customerResult'
            }
        }, {
            '$addFields': {
                'customerName': {
                    '$arrayElemAt': [
                        '$customerResult.customerName', 0
                    ]
                }, 
                'customer': {
                    '$toString': '$customer'
                }, 
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                "Customer":'$customerName',
                "Work Description":'$workDescription'
            }
        }
    ]
    response = cmo.finding_aggregate("partnerWorkDescription",arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Partner_Work_Description", "Partner_Work_Description")
    return send_file(fullPath)

@export_blueprint.route("/export/partnerActivity", methods=["GET"])
def export_partner_activity():
    arra = [
        {
            '$lookup': {
                'from': 'customer', 
                'localField': 'customer', 
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
                'as': 'customerResult'
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'subProject', 
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
                'as': 'subResult'
            }
        }, {
            '$lookup': {
                'from': 'partnerWorkDescription', 
                'localField': 'workDescription', 
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
                'as': 'workResult'
            }
        }, {
            '$addFields': {
                'Customer': {
                    '$arrayElemAt': [
                        '$customerResult.customerName', 0
                    ]
                }, 
                'Project Type': '$projectType', 
                'Sub Project': {
                    '$arrayElemAt': [
                        '$subResult.subProject', 0
                    ]
                }, 
                'Work Description': {
                    '$arrayElemAt': [
                        '$workResult.workDescription', 0
                    ]
                }
            }
        }, {
            '$project': {
                'Customer': 1, 
                'Project Type': 1, 
                'Sub Project': 1, 
                'Work Description': 1, 
                'MS List': '$milestone', 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("partnerActivity",arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Partner_Activity", "Partner_Activity")
    return send_file(fullPath)

@export_blueprint.route("/ProjectIDBulkUploadTemplate/<custId>", methods=["GET"])
@export_blueprint.route("/ProjectIDBulkUploadTemplate/<custId>/<projectTypeUniqueId>",methods=["GET"])
@token_required
def ProjectIDBulkUploadTemplate(current_user,custId=None, projectTypeUniqueId=None):
    arra = [
        {
            '$match':{
                'custId':custId
            }
        }
    ]
    if projectTypeUniqueId != None and projectTypeUniqueId != "undefined":
        checkarra = [
            {
                '$match':{
                    '_id':ObjectId(projectTypeUniqueId),
                }
            }
        ]
        data = cmo.finding_aggregate("projectType", checkarra)["data"]
        projectType = data[0]["projectType"]
        customerId = data[0]['custId']
        arra = arra + [
            {
                '$match':{
                    'projectType':projectType,
                    "custId":customerId
                }
            }
        ]
    response = cmo.finding_aggregate("projectType", arra)["data"]
    if len(response)>0:
        T_sengg = []
        T_tarcking = []
        T_issues = []
        
        for response in response:
            if 't_sengg' in response:
                for i in response['t_sengg']:
                    if i['fieldName'] not in T_sengg:
                        T_sengg.append(i['fieldName'])
                    
            if "t_tracking" in response:
                for i in response['t_tracking']:
                    if i['fieldName'] not in T_tarcking:
                        T_tarcking.append(i['fieldName'])
                    
            if "t_issues" in response:
                for i in response['t_issues']:
                    if i['fieldName'] not in T_issues:
                        T_issues.append(i['fieldName'])
                    
        elements_to_remove = ['Site Id', 'RFAI Date', 'Unique ID']       
        T_sengg = [item for item in T_sengg if item not in elements_to_remove]    
            
        sortCol = ["Customer","Project Group","Project ID","Project Type","Sub Project","Circle","Site Id","Unique ID","RFAI Date"]
            
        headers = sortCol+T_sengg+T_tarcking+T_issues
        df = pd.DataFrame(columns=headers)
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Template", "Template")
        return send_file(fullPath)
    else:
        df = pd.DataFrame()
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Template", "Template")
        return send_file(fullPath)
        


@export_blueprint.route("/template/OneProject/<id>",methods=['GET'])
@token_required
def template_one_project(current_user,id=None):
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }, 
                '_id': ObjectId(id)
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'projectType', 
                'foreignField': '_id', 
                'as': 'result'
            }
        }, {
            '$project': {
                'projectType': {
                    '$arrayElemAt': [
                        '$result.projectType', 0
                    ]
                }
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'projectType', 
                'foreignField': 'projectType', 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$replaceRoot': {
                'newRoot': '$result'
            }
        }, {
            '$project': {
                't_sengg': 1, 
                't_tracking': 1, 
                't_issues': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)['data']
    if len(response)>0:
        T_sengg = []
        T_tarcking = []
        T_issues = []
        
        for response in response:
            if 't_sengg' in response:
                for i in response['t_sengg']:
                    if i['fieldName'] not in T_sengg:
                        T_sengg.append(i['fieldName'])
                    
            if "t_tracking" in response:
                for i in response['t_tracking']:
                    if i['fieldName'] not in T_tarcking:
                        T_tarcking.append(i['fieldName'])
                    
            if "t_issues" in response:
                for i in response['t_issues']:
                    if i['fieldName'] not in T_issues:
                        T_issues.append(i['fieldName'])
                    
        elements_to_remove = ['Site Id', 'RFAI Date', 'Unique ID']       
        T_sengg = [item for item in T_sengg if item not in elements_to_remove]    
            
        sortCol = ["Customer","Project Group","Project ID","Project Type","Sub Project","Circle","Site Id","Unique ID","RFAI Date"]
            
        headers = sortCol+T_sengg+T_tarcking+T_issues
        df = pd.DataFrame(columns=headers)
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Template", "Template")
        return send_file(fullPath)
    else:
        df = pd.DataFrame()
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Template", "Template")
        return send_file(fullPath)
    
def create_unique_id(row):
    columns = row['value'].split(',')
    unique_id_parts = [str(row[col]) for col in columns]
    return '-'.join(unique_id_parts)   

def background_site_upload(dict_data,pathing):
    try:
        cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Start On","typem":"old","time":current_date_str()})
        for datauploaded in dict_data:
            datauploaded["RFAI Date"] = check_and_convert_date((datauploaded["RFAI Date"]))
            datauploaded['siteStartDate'] = datauploaded['RFAI Date']
            datauploaded["addedAt"] = current_date_str()
            Response = cmo.insertion("SiteEngineer", datauploaded)
            uId = Response["operation_id"]
            arra = [
                {
                    '$match': {
                        '_id': ObjectId(datauploaded['SubProjectId']),
                        'deleteStatus':{"$ne":1},
                        "custId":datauploaded["customerId"]
                    }
                }, {
                    '$project': {
                        'MileStone': 1, 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType",arra)['data']
            milestone = response[0]["MileStone"]
            finalerEnd = None
            finalerStart = ctm.strtodate((datauploaded["siteStartDate"]))
            for i in milestone:
                finalerEnd = ctm.add_hour_in_udate(finalerStart, int(i["Estimated Time (Days)"]))
                data = {
                    "Name": i["fieldName"],
                    "Estimated Time (Days)": i["Estimated Time (Days)"],
                    "WCC Sign Off": i["WCC Sign off"],
                    "Predecessor": i["Predecessor"],
                    "Completion Criteria": i["Completion Criteria"],
                    "SubProjectId": datauploaded["SubProjectId"],
                    "projectuniqueId": datauploaded["projectuniqueId"],
                    "siteId": ObjectId(uId),
                    "systemId": datauploaded['systemId'],
                    "estimateDay": int(i["Estimated Time (Days)"]),
                    "mileStoneStatus": "Open",
                    "mileStoneStartDate": finalerStart.strftime("%Y-%m-%d")+"T00:00:00",
                    "mileStoneEndDate": finalerEnd.strftime("%Y-%m-%d")+"T00:00:00",
                    "customerId":datauploaded["customerId"],
                    "projectGropupId":datauploaded["projectGroupId"],
                    "projectGroupId":datauploaded["projectGroupId"],
                    "circleId":datauploaded["circleId"],
                }
                finalerStart = finalerEnd
                cmo.insertion("milestone", data)
            upby = {"_id": ObjectId(uId)}
            upAt = {"siteEndDate": finalerEnd.strftime("%Y-%m-%d")+"T00:00:00"}
            cmo.updating("SiteEngineer", upby, upAt, False)
        cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Complete","typem":"old","time":current_date_str()})
    except Exception as e:
        cmo.insertion("notification",{"msg":f"{pathing['fnamemsg']}","status":"Failed","typem":"old","time":current_date_str()})


@export_blueprint.route("/uploadSiteOneProject/<id>",methods=['GET','POST'])
@token_required
def uploadsite_oneProject(current_user,id=None):
    if request.method == "POST":
        upload = request.files.get("uploadedFile[]")
        allData = {}
        supportFile = ["xlsx", "xls"]
        pathing = cform.singleFileSaver(upload, "", supportFile)
        if pathing["status"] == 200:
            allData["pathing"] = pathing["msg"]
        elif pathing["status"] == 422:
            return respond(pathing)
        excel_file_path = os.path.join(os.getcwd(), allData["pathing"])
        exceldata = pd.read_excel(excel_file_path)
        if exceldata.empty:
            return respond({
                'status':400,
                "icon":"error",
                "msg":"Your Excel File is Empty"
            })
        required_columns = {"Customer", "Project Group", "Project ID", "Project Type", "Sub Project", "Circle", "Site Id", "Unique ID", "RFAI Date"}
        if not required_columns.issubset(exceldata.columns):
            return respond({
                'status': 400,
                "icon": 'error',
                "msg": 'The following columns are required: Customer, Project Group, Project ID, Project Type, Sub Project, Circle, Site Id, Unique ID, and RFAI Date.'
            })
        arra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    '_id': ObjectId(id)
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'projectType', 
                    'foreignField': '_id', 
                    'pipeline': [
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
                            '$addFields': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customer.customerName', 0
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'projectType': 1, 
                                'customer': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$lookup': {
                    'from': 'projectGroup', 
                    'localField': 'projectGroup', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
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
                                    }, {
                                        '$match': {
                                            'deleteStatus': {
                                                '$ne': 1
                                            }
                                        }
                                    }, {
                                        '$lookup': {
                                            'from': 'circle', 
                                            'localField': 'circle', 
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
                                            'as': 'CircleResult'
                                        }
                                    }, {
                                        '$unwind': {
                                            'path': '$CircleResult', 
                                            'preserveNullAndEmptyArrays': True
                                        }
                                    }, {
                                        '$addFields': {
                                            'circle': '$CircleResult.circleCode'
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
                                'customer': '$customer.shortName', 
                                'circle': '$zone.circle'
                            }
                        }, {
                            '$addFields': {
                                'projectGroup': {
                                    '$concat': [
                                        '$customer', '-', '$zone', '-', '$costCenter'
                                    ]
                                },
                                "_id":{
                                    '$toString':"$_id"
                                }
                            }
                        }, {
                            '$project': {
                                'projectGroup': 1, 
                                'circle': 1
                            }
                        }
                    ], 
                    'as': 'PGResult'
                }
            }, {
                '$unwind': {
                    'path': '$PGResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    'Project Type': {
                        '$arrayElemAt': [
                            '$result.projectType', 0
                        ]
                    }, 
                    'Project Group': '$PGResult.projectGroup', 
                    'Circle': '$PGResult.circle', 
                    'Customer': {
                        '$arrayElemAt': [
                            '$result.customer', 0
                        ]
                    }, 
                    'Project ID': '$projectId', 
                    'projectuniqueId': {
                        '$toString': '$_id'
                    },
                    "custId":1,
                    'projectGroupId': '$PGResult._id',
                    'circleId': '$circle'
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'Project Type', 
                    'foreignField': 'projectType', 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'Sub Project': '$result.subProject', 
                    'SubProjectId': {
                        '$toString': '$result._id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'result': 0
                }
            }
        ]
        response = cmo.finding_aggregate("project",arra)['data']
        if len(response)>0:
            projectDatadf = pd.DataFrame.from_dict(response)
            mergedData = exceldata.merge(projectDatadf,on=['Customer','Project Group',"Project ID","Project Type","Sub Project","Circle"],how='left')
            result = mergedData[mergedData["projectuniqueId"].isna()]
            unique_c = result[['Customer','Project Group',"Project ID","Project Type","Sub Project","Circle"]].drop_duplicates()
            if len(unique_c) > 0:
                pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "The combination of Customer, Project Group, Project Id, Project Type, Sub Project and Circle was not found in the database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                    }
                )
            else:
                exceldata['projectuniqueId'] = mergedData['projectuniqueId']
                exceldata['SubProjectId'] = mergedData['SubProjectId']
                exceldata['customerId'] = mergedData['custId']
                exceldata['projectGroupId'] = mergedData['projectGroupId']
                exceldata['circleId'] = mergedData['circleId']
                exceldata.drop(columns=['Customer','Project Group',"Project ID","Project Type","Sub Project","Circle"],inplace=True)
                if "Site ID" in exceldata:
                    del exceldata['Site ID']
                exceldata['Site Id'] = exceldata['Site Id'].astype(str)
                invalidRfaiDate = []
                expected_format = '%d-%m-%Y'
                if "RFAI Date" in exceldata:
                    for index, row in exceldata.iterrows():
                        if pd.isna(row['RFAI Date']) or row['RFAI Date'] == '':
                            invalidRfaiDate.append(index+2)
                        else:
                            try:
                                row['RFAI Date'] = pd.to_datetime(row['RFAI Date'], format=expected_format, errors='raise')
                            except Exception as e:
                                invalidRfaiDate.append(index+2)
                                   
                if (len(invalidRfaiDate)):
                    return respond({
                        "status":400,
                        "icon":"error",
                        "msg":f"These rows have incorrect RFAI Date format or the RFAI Date may be empty:-{invalidRfaiDate}"
                    })
                    
                invalidAllocDate = []
                if "ALLOCATION DATE" in exceldata:
                    for index, row in exceldata.iterrows():
                        if pd.isna(row['ALLOCATION DATE']) or row['ALLOCATION DATE'] == '':
                            row['ALLOCATION DATE'] = None
                        else:
                            try:
                                row['ALLOCATION DATE'] = pd.to_datetime(row['ALLOCATION DATE'], format=expected_format, errors='raise')
                            except Exception as e:
                                invalidAllocDate.append(index+2)
                                
                if (len(invalidAllocDate)):
                    return respond({
                        "status":400,
                        "icon":"error",
                        "msg":f"These rows have incorrect ALLOCATION DATE format or the ALLOCATION DATE may be empty:-{invalidAllocDate}"
                    })
                subprojectObject = []
                unique_sub_projects = exceldata['SubProjectId'].unique()
                for i in unique_sub_projects:
                    subprojectObject.append(ObjectId(i))
                arra = [{"$match": {"_id":{'$in': subprojectObject}}}]
                respone = cmo.finding_aggregate("projectType", arra)["data"]
                fieldName = []
                dropdownField = []
                for i in respone:
                    data = i['t_sengg']
                    for item in data:
                        if item["dataType"] == "Dropdown":
                            # item['subId'] = i['subId']
                            item['fieldName'] = item['fieldName'].strip()
                            item['dropdownValue'] = [value.strip() for value in item['dropdownValue'].split(',')]
                            dropdownField.append(item)
                    unique_id_object = next((item for item in data if item["fieldName"] == "Unique ID"),None)
                    if unique_id_object:
                            if unique_id_object["dataType"] == "Auto Created":
                                keys = unique_id_object["dropdownValue"].split(",")
                                for i in keys:
                                    if i not in fieldName:
                                        fieldName.append(i)
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
                for item in dropdownField:
                    field_name = item['fieldName']
                    valid_values = item['dropdownValue']
                    print(valid_values,field_name,'field_namefield_namefield_namefield_name')
                    
                    required = item['required']
                    if field_name in exceldata.columns:
                        if required == 'No':
                            exceldata[field_name] = exceldata[field_name].apply(lambda x: x if x in valid_values or pd.isna(x) else "")
                        else:
                            if field_name in ['CELL ID',"BAND"]:
                                exceldata[field_name] = exceldata[field_name].str.split(",")
                                print(exceldata[field_name],"1387")
                                invalid_values = (exceldata[field_name].dropna().apply(lambda lst: [item for item in lst if item not in valid_values]).explode() .dropna().unique())
                                exceldata[field_name] = exceldata[field_name].apply(lambda x: ','.join(map(str, x)) if len(x) > 1 else str(x[0]))
                            else:
                                invalid_values = exceldata[~exceldata[field_name].isin(valid_values)][field_name].dropna().unique()
                            if len(invalid_values) > 0:
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Field '{field_name}' has invalid values: {invalid_values}"
                                })    
                # for item in dropdownField:
                #     field_name = item['fieldName']
                #     valid_values = item['dropdownValue']
                #     required = item['required']
                #     if field_name in exceldata.columns:
                #         if required == 'No':
                #             exceldata[field_name] = exceldata[field_name].apply(lambda x: x if x in valid_values or pd.isna(x) else "")
                #         else:
                #             invalid_values = exceldata[~exceldata[field_name].isin(valid_values)][field_name].dropna().unique()
                #             if len(invalid_values) > 0:
                #                 return respond({
                #                     'status':400,
                #                     "icon":"error",
                #                     "msg":f"Field '{field_name}' has invalid values: {invalid_values}"
                #                 })
                arra = [
                    {
                        '$match':{
                            'deleteStatus':{'$ne':1},
                            "custId":exceldata['customerId'].iloc[0]
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
                            'deleteStatus':{'$ne':1},
                            "customerId":exceldata['customerId'].iloc[0]
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
                number_of_records = len(exceldata)
                counter = database.fileDetail.find_one_and_update({"id": "systemIdCounter"},{"$inc": {"sequence_value": number_of_records}},return_document=True)
                starting_number = counter["sequence_value"] - number_of_records+1
                exceldata['systemId'] = ["SSID" + str(starting_number +i).zfill(8) for i in range(len(exceldata))]
                exceldata['siteBillingStatus'] = "Unbilled"
                exceldata['siteStatus'] = "Open"
                exceldata['Site Id'] = exceldata['Site Id'].astype(str)
                dict_data = cfc.dfjson(exceldata)
                thread = Thread(target=background_site_upload, args=(dict_data,pathing))
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
                "msg":"No Active Project found in Database"
            })
            




#     cfsocket.socketio.emit("notification",{"msg":f"{pathing['fnamemsg']} completed","typem":"new","time":ctm.unique_timestamp()})
    

@export_blueprint.route("/uploadBulkSite/<customerUniqueId>", methods=["POST"])
@export_blueprint.route("/uploadBulkSite/<customerUniqueId>/<projectTypeIds>", methods=["POST"])
@token_required
def uploadBulkSite(current_user, customerUniqueId=None, projectTypeIds=None):
    if request.method == "POST":
        upload = request.files.get("uploadedFile[]")
        allData = {}
        supportFile = ["xlsx", "xls"]
        pathing = cform.singleFileSaver(upload, "", supportFile)
        if pathing["status"] == 200:
            allData["pathing"] = pathing["msg"]
        elif pathing["status"] == 422:
            return respond(pathing)
        excel_file_path = os.path.join(os.getcwd(), allData["pathing"])
        exceldata = pd.read_excel(excel_file_path)
        if exceldata.empty:
            return respond({
                'status':400,
                "icon":"error",
                "msg":"Your Excel File is Empty"
            })
        required_columns = {"Customer", "Project Group", "Project ID", "Project Type", "Sub Project", "Circle", "Site Id", "Unique ID", "RFAI Date"}
        if not required_columns.issubset(exceldata.columns):
            return respond({
                'status': 400,
                "icon": 'error',
                "msg": 'The following columns are required: Customer, Project Group, Project ID, Project Type, Sub Project, Circle, Site Id, Unique ID, and RFAI Date.'
            })
            
        arra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    },
                    "custId":customerUniqueId
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'projectType', 
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
                                'custId': {
                                    '$toObjectId': '$custId'
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'customer', 
                                'localField': 'custId', 
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
                            '$addFields': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customer.customerName', 0
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'projectType': 1, 
                                'customer': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$lookup': {
                    'from': 'projectGroup', 
                    'localField': 'projectGroup', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
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
                                    }, {
                                        '$match': {
                                            'deleteStatus': {
                                                '$ne': 1
                                            }
                                        }
                                    }, {
                                        '$lookup': {
                                            'from': 'circle', 
                                            'localField': 'circle', 
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
                                            'as': 'CircleResult'
                                        }
                                    }, {
                                        '$unwind': {
                                            'path': '$CircleResult', 
                                            'preserveNullAndEmptyArrays': True
                                        }
                                    }, {
                                        '$addFields': {
                                            'circle': '$CircleResult.circleCode'
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
                                'customer': '$customer.shortName', 
                                'circle': '$zone.circle'
                            }
                        }, {
                            '$addFields': {
                                'projectGroup': {
                                    '$concat': [
                                        '$customer', '-', '$zone', '-', '$costCenter'
                                    ]
                                },
                                "_id":{
                                    '$toString':"$_id"
                                }
                            }
                        }, {
                            '$project': {
                                'projectGroup': 1, 
                                'circle': 1
                            }
                        }
                    ], 
                    'as': 'PGResult'
                }
            }, {
                '$unwind': {
                    'path': '$PGResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    'Project Type': {
                        '$arrayElemAt': [
                            '$result.projectType', 0
                        ]
                    }, 
                    'Project Group': '$PGResult.projectGroup', 
                    'Circle': '$PGResult.circle', 
                    'Customer': {
                        '$arrayElemAt': [
                            '$result.customer', 0
                        ]
                    }, 
                    'Project ID': '$projectId', 
                    'projectuniqueId': {
                        '$toString': '$_id'
                    },
                    "custId":1,
                    'projectGroupId': '$PGResult._id',
                    'circleId': '$circle'
                }
            },  {
                '$lookup': {
                    'from': 'projectType', 
                    'let': {
                        'projectType': '$Project Type', 
                        'custId': '$custId'
                    }, 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$projectType', '$$projectType'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$custId', '$$custId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'Sub Project': '$result.subProject', 
                    'SubProjectId': {
                        '$toString': '$result._id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'result': 0,
                   
                }
            }
        ]
        response = cmo.finding_aggregate("project",arra)['data']
        if len(response)>0:
            projectDatadf = pd.DataFrame.from_dict(response)
            mergedData = exceldata.merge(projectDatadf,on=['Customer','Project Group',"Project ID","Project Type","Sub Project","Circle"],how='left')
            result = mergedData[mergedData["projectuniqueId"].isna()]
            unique_c = result[['Customer','Project Group',"Project ID","Project Type","Sub Project","Circle"]].drop_duplicates()
            if len(unique_c) > 0:
                pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "The combination of Customer, Project Group, Project Id, Project Type, Sub Project and Circle was not found in the database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                    }
                )
            else:
                exceldata['projectuniqueId'] = mergedData['projectuniqueId']
                exceldata['SubProjectId'] = mergedData['SubProjectId']
                exceldata['customerId'] = mergedData['custId']
                exceldata['projectGroupId'] = mergedData['projectGroupId']
                exceldata['circleId'] = mergedData['circleId']
                
                exceldata.drop(columns=['Customer','Project Group',"Project ID","Project Type","Sub Project","Circle"],inplace=True)
                if "Site ID" in exceldata:
                    del exceldata['Site ID']
                emptySite = []
                for index, row in exceldata.iterrows():
                    if pd.isna(row['Site Id']) or row['Site Id'] == '':
                        emptySite.append(index+2)
                if emptySite:
                    return respond({
                        'status':400,
                        "icon":"error",
                        "msg":f"These Rows are contain Empty Site Id Field-{emptySite}"
                    })
                exceldata['Site Id'] = exceldata['Site Id'].astype(str)
                invalidRfaiDate = []
                expected_format = '%d-%m-%Y'
                if "RFAI Date" in exceldata:
                    for index, row in exceldata.iterrows():
                        if pd.isna(row['RFAI Date']) or row['RFAI Date'] == '':
                            invalidRfaiDate.append(index+2)
                        else:
                            try:
                                row['RFAI Date'] = pd.to_datetime(row['RFAI Date'], format=expected_format, errors='raise')
                            except Exception as e:
                                invalidRfaiDate.append(index+2)
                                
                if (len(invalidRfaiDate)):
                    return respond({
                        "status":400,
                        "icon":"error",
                        "msg":f"These rows have incorrect RFAI Date format or the RFAI Date may be empty:-{invalidRfaiDate}"
                    })
                    
                invalidAllocDate = []   
                if "ALLOCATION DATE" in exceldata:
                    for index, row in exceldata.iterrows():
                        if pd.isna(row['ALLOCATION DATE']) or row['ALLOCATION DATE'] == '':
                            row['ALLOCATION DATE'] = None
                        else:
                            try:
                                row['ALLOCATION DATE'] = pd.to_datetime(row['ALLOCATION DATE'], format=expected_format, errors='raise')
                            except Exception as e:
                                invalidAllocDate.append(index+2)
                                
                if (len(invalidAllocDate)):
                    return respond({
                        "status":400,
                        "icon":"error",
                        "msg":f"These rows have incorrect ALLOCATION DATE format or the ALLOCATION DATE may be empty:-{invalidAllocDate}"
                    })
                    
                subprojectObject = []
                unique_sub_projects = exceldata['SubProjectId'].unique()
                for i in unique_sub_projects:
                    subprojectObject.append(ObjectId(i))
                arra = [{"$match": {"_id":{'$in': subprojectObject}}},{'$addFields': {'subId': {'$toString': '$_id'}}}]
                respone = cmo.finding_aggregate("projectType", arra)["data"]
                fieldName = []
                dropdownField = []
                for i in respone:
                    data = i['t_sengg']
                    unique_id_object = next((item for item in data if item["fieldName"] == "Unique ID"),None)
                    for item in data:
                        if item["dataType"] == "Dropdown":
                            item['subId'] = i['subId']
                            item['fieldName'] = item['fieldName'].strip()
                            item['dropdownValue'] = [value.strip() for value in item['dropdownValue'].split(',')]
                            dropdownField.append(item)
                    
                    if unique_id_object:
                            if unique_id_object["dataType"] == "Auto Created":
                                keys = unique_id_object["dropdownValue"].split(",")
                                for i in keys:
                                    if i not in fieldName:
                                        fieldName.append(i)
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
                for item in dropdownField:
                    field_name = item['fieldName']
                    valid_values = item['dropdownValue']
                    
                    required = item['required']
                    if field_name in exceldata.columns:
                        if required == 'No':
                            exceldata[field_name] = exceldata[field_name].apply(lambda x: x if x in valid_values or pd.isna(x) else "")
                        else:
                            if field_name in ['CELL ID',"BAND"]:
                                exceldata[field_name] = exceldata[field_name].str.split(",")
                                invalid_values = (exceldata[field_name].dropna().apply(lambda lst: [item for item in lst if item not in valid_values]).explode() .dropna().unique())
                                exceldata[field_name] = exceldata[field_name].apply(lambda x: ','.join(map(str, x)) if len(x) > 1 else str(x[0]))
                            else:
                                exceldata[field_name] = exceldata[field_name].astype(str)
                                invalid_values = exceldata[~exceldata[field_name].isin(valid_values)][field_name].dropna().unique()
                            if len(invalid_values) > 0:
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Field '{field_name}' has invalid values: {invalid_values}"
                                })
                    
                # for item in dropdownField:
                #     field_name = item['fieldName']
                #     valid_values = item['dropdownValue']
                #     required = item['required']
                #     if field_name in exceldata.columns:
                #         if required == 'No':
                #             exceldata[field_name] = exceldata[field_name].apply(lambda x: x if x in valid_values or pd.isna(x) else "")
                #         else:
                #             invalid_values = exceldata[~exceldata[field_name].isin(valid_values)][field_name].dropna().unique()
                #             if len(invalid_values) > 0:
                #                 return respond({
                #                     'status':400,
                #                     "icon":"error",
                #                     "msg":f"Field '{field_name}' has invalid values: {invalid_values}"
                #                 })
                
                arra = [
                    {
                        '$match':{
                            'deleteStatus':{'$ne':1},
                            "custId":customerUniqueId
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
                            'deleteStatus':{'$ne':1},
                            "customerId":customerUniqueId
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
                
                number_of_records = len(exceldata)
                counter = database.fileDetail.find_one_and_update({"id": "systemIdCounter"},{"$inc": {"sequence_value": number_of_records}},return_document=True)
                starting_number = counter["sequence_value"] - number_of_records+1
                exceldata['systemId'] = ["SSID" + str(starting_number +i).zfill(8) for i in range(len(exceldata))]
                exceldata['siteBillingStatus'] = "Unbilled"
                exceldata['siteStatus'] = "Open"
                dict_data = cfc.dfjson(exceldata)
                thread = Thread(target=background_site_upload, args=(dict_data,pathing))
                thread.start()
                return respond({
                        "status": 200,
                        "icon": "success",
                        "msg": f"Your file is in process when its completed we will notify",
                    })
                    




@export_blueprint.route("/export/manageCircle", methods=["GET"])
@token_required
def export_manageCircle_download(current_user):

    arra = [
        {
            "$lookup": {
                "from": "customer",
                "localField": "customer",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "Customer": "$result.customerName",
                "Circle Name": "$circleName",
                "Circle Code": "$circleCode",
            }
        },
        {"$project": {"_id": 0, "Customer": 1, "Circle Name": 1, "Circle Code": 1}},
    ]
    response = cmo.finding_aggregate("circle", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)

    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_circle", "Circle")
    return send_file(fullPath)


@export_blueprint.route("/export/vendor", methods=["GET"])
@token_required
def export_Partner(current_user):
    arra = []
        
    if (request.args.get("vendorName") != None and request.args.get("vendorName") != "undefined"):
        arra = arra + [
            {"$match": {"vendorName": {"$regex": request.args.get("vendorName"), '$options': 'i' }}}
        ]
    if (request.args.get("vendorCode") != None and request.args.get("vendorCode") != "undefined"):
        arra = arra + [{"$match": {"vendorCode": {"$regex": request.args.get("vendorCode"), '$options': 'i' }}}]
    if (request.args.get("status") != None and request.args.get("status") != "undefined"):
        arra = arra + [{"$match": {"status": request.args.get("status")}}]

    arra = arra+ [
        {"$match": {"type": "Partner"}},
        {
            "$addFields": {
                "uniqueId": {"$toString": "$_id"},
                "dateOfRegistration": {
                    "$cond": {
                        "if": {"$eq": ["$dateOfRegistration", ""]},
                        "then": "",
                        "else": {
                            "$dateToString": {
                                "format": "%d-%m-%Y",
                                "date": {
                                    "$dateFromString": {
                                        "dateString": "$dateOfRegistration"
                                    }
                                },
                            }
                        },
                    }
                },
                "validityUpto": {
                    "$cond": {
                        "if": {"$eq": ["$validityUpto", ""]},
                        "then": "",
                        "else": {
                            "$dateToString": {
                                "format": "%d-%m-%Y",
                                "date": {
                                    "$dateFromString": {"dateString": "$validityUpto"}
                                },
                            }
                        },
                    }
                },
                "Circle": {
                    "$cond": [{"$eq": ["$Circle", ""]}, "", {"$toObjectId": "$Circle"}]
                },
            }
        },
        {"$project": {"_id": 0}},
        {
            "$lookup": {
                "from": "circle",
                "localField": "Circle",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$project": {"circleCode": 1, "circleName": 1, "_id": 0}},
                ],
                "as": "Circle",
            }
        },
        {
            "$lookup": {
                "from": "userRole",
                "localField": "userRole",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "userRole",
            }
        },
        {
            "$addFields": {
                "Circle": {"$arrayElemAt": ["$Circle.circleName", 0]},
                "userRole": {"$arrayElemAt": ["$userRole.roleName", 0]},
            }
        },
        {
            "$project": {
                "Partner Code": {"$ifNull": ["$vendorCode", ""]},
                "Partner Name": {"$ifNull": ["$vendorName", ""]},
                "UST Partner Code": {"$ifNull": ["$ustCode", ""]},
                "e-mail Address": {"$ifNull": ["$email", ""]},
                "Partner Category": {"$ifNull": ["$vendorCategory", ""]},
                "Partner Subcategory": {"$ifNull": ["$vendorSubCategory", ""]},
                "Role": {"$ifNull": ["$userRole", ""]},
                "Date of Registration": {"$ifNull": ["$dateOfRegistration", ""]},
                "Validity Upto": {"$ifNull": ["$validityUpto", ""]},
                "Partner Ranking": {"$ifNull": ["$ranking", ""]},
                "Payment Terms (Days)": {"$ifNull": ["$paymentTerms", ""]},
                "Working Circle": {"$ifNull": ["$Circle", ""]},
                "Partner Registered with GST (Y/N)": {
                    "$ifNull": ["$vendorRegistered", ""]
                },
                "GST Number": {"$ifNull": ["$gstNumber", ""]},
                "Current Status": {"$ifNull": ["$status", ""]},
                "Password": {"$ifNull": ["$password", ""]},
                "PAN Number": {"$ifNull": ["$panNumber", ""]},
                "TAN Number": {"$ifNull": ["$tanNumber", ""]},
                "ESI Number": {"$ifNull": ["$esiNumber", ""]},
                "EPF Number": {"$ifNull": ["$epfNumber", ""]},
                "STN Number": {"$ifNull": ["$stnNumber", ""]},
                "Bank Account Number": {"$ifNull": ["$accountNumber", ""]},
                "Bank Name": {"$ifNull": ["$bankName", ""]},
                "IFSC Code": {"$ifNull": ["$ifscCode", ""]},
                "Branch Address": {"$ifNull": ["$bankAddress", ""]},
                "Financial Turnover": {"$ifNull": ["$financialTurnover", ""]},
                "CBTHR Certified (Y/N)": {"$ifNull": ["$cbt", ""]},
                "Form Toclii": {"$ifNull": ["$formToci", ""]},
                "Contact Person": {"$ifNull": ["$contactPerson", ""]},
                "Registered Address": {"$ifNull": ["$registeredAddress", ""]},
                "Contact Details": {"$ifNull": ["$contactDetails", ""]},
                "Secondary Contact details": {"$ifNull": ["$SecContactDetails", ""]},
                "Company Type": {"$ifNull": ["$companyType", ""]},
                "Team Capacity": {"$ifNull": ["$technology", ""]},
                "Other Information": {"$ifNull": ["$otherInfo", ""]},
            }
        },
    ]

    response = cmo.finding_aggregate("userRegister", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    for col in ['Date of Registration','Validity Upto']:
        dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Vendor", "Vendor")
    return send_file(fullPath)


@export_blueprint.route("/export/manageZone", methods=["GET"])
@token_required
def export_manageZone_download(current_user):

    arra = [
        {
            "$lookup": {
                "from": "customer",
                "localField": "customer",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "Customer": "$result.customerName",
                "Zone Name": "$zoneName",
                "Zone Code": "$shortCode",
            }
        },
        {
            "$lookup": {
                "from": "circle",
                "localField": "circle",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deletestatus": {"$ne": 1}}},
                    {"$project": {"_id": 0, "circleName": 1}},
                ],
                "as": "circle",
            }
        },
        {
            "$project": {
                "_id": 0,
                "result": 0,
                "customer": 0,
                "zoneName": 0,
                "shortCode": 0,
            }
        },
        {"$addFields": {"circle": "$circle.circleName"}},
        {
            "$addFields": {
                "Circle": {
                    "$reduce": {
                        "input": "$circle",
                        "initialValue": "",
                        "in": {
                            "$concat": [
                                "$$value",
                                {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                "$$this",
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"circle": 0}},
    ]
    response = cmo.finding_aggregate("zone", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)

    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Zone", "Zone")
    return send_file(fullPath)


@export_blueprint.route("/testlookup")
def testing_function():
    globalArr = []
    all = [
        "designation",
        "L1Approver",
        "L1Commercial",
        "L1Commercial",
        "L1Sales",
        "L1Vendor",
        "L2Approver",
        "L2Combliance",
        "L2Sales",
        "L2Vendor",
        "assetManager",
        "compliance",
        "financeApprover",
        "reportingManager",
        "reportingHrManager",
    ]
    for i in all:
        # print(i,'iiiii')
        globalArr.append(cmo.advancelookup(i, i, "userRegister"))

    return globalArr


@export_blueprint.route("/export/manageEmployee", methods=["GET"])
@export_blueprint.route("/export/manageEmployee/<id>", methods=["GET"])
@token_required
def export_manageEmployee_download(current_user,id=None):
    arraa_name = [
        {"$addFields": {"uId": {"$toString": "$_id"}}},
        {"$project": {"_id": 0, "uId": 1, "empName": 1}},
    ]
    response_one_df = cmo.finding_aggregate("userRegister", arraa_name)
    arraa = []  
    if (request.args.get("empName") != None and request.args.get("empName") != "undefined"):
            arraa = arraa + [
                {"$match": {"empName": {"$regex": request.args.get("empName"), '$options': 'i' }}}
            ]
    if (request.args.get("empCode") != None and request.args.get("empCode") != "undefined"):
        arraa = arraa + [{"$match": {"empCode": {"$regex": request.args.get("empCode"), '$options': 'i' }}}]
    if (request.args.get("pmisRole") != None and request.args.get("pmisRole") != "undefined"):
        arraa = arraa + [{"$match": {"userRole": ObjectId(request.args.get("pmisRole"))}}]
    if (request.args.get("status") != None and request.args.get("status") != "undefined"):
        arraa = arraa + [{"$match": {"status": request.args.get("status")}}]
    arraa = arraa + [
        {"$match": {"deletestatus": {"$ne": 1}, "type": {"$ne": "Partner"}}},
        
        {
        '$addFields': {
            'dob': {
                '$cond': {
                    'if': {
                        '$ne': [
                            '$dob', ''
                        ]
                    }, 
                    'then': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'date': {
                                '$dateFromParts': {
                                    'year': {
                                        '$year': {
                                            'date': {
                                                '$toDate': '$dob'
                                            }
                                        }
                                    }, 
                                    'month': {
                                        '$month': {
                                            'date': {
                                                '$toDate': '$dob'
                                            }
                                        }
                                    }, 
                                    'day': {
                                        '$dayOfMonth': {
                                            'date': {
                                                '$toDate': '$dob'
                                            }
                                        }
                                    }, 
                                    'hour': {
                                        '$hour': {
                                            'date': {
                                                '$toDate': '$dob'
                                            }
                                        }
                                    }, 
                                    'minute': {
                                        '$minute': {
                                            'date': {
                                                '$toDate': '$dob'
                                            }
                                        }
                                    }, 
                                    'second': {
                                        '$second': {
                                            'date': {
                                                '$toDate': '$dob'
                                            }
                                        }
                                    }, 
                                    'timezone': '+05:30'
                                }
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'else': '$dob'
                }
            }, 
            'joiningDate': {
                '$cond': {
                    'if': {
                        '$ne': [
                            '$joiningDate', ''
                        ]
                    }, 
                    'then': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'date': {
                                '$dateFromParts': {
                                    'year': {
                                        '$year': {
                                            'date': {
                                                '$toDate': '$joiningDate'
                                            }
                                        }
                                    }, 
                                    'month': {
                                        '$month': {
                                            'date': {
                                                '$toDate': '$joiningDate'
                                            }
                                        }
                                    }, 
                                    'day': {
                                        '$dayOfMonth': {
                                            'date': {
                                                '$toDate': '$joiningDate'
                                            }
                                        }
                                    }, 
                                    'hour': {
                                        '$hour': {
                                            'date': {
                                                '$toDate': '$joiningDate'
                                            }
                                        }
                                    }, 
                                    'minute': {
                                        '$minute': {
                                            'date': {
                                                '$toDate': '$joiningDate'
                                            }
                                        }
                                    }, 
                                    'second': {
                                        '$second': {
                                            'date': {
                                                '$toDate': '$joiningDate'
                                            }
                                        }
                                    }, 
                                    'timezone': '+05:30'
                                }
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'else': '$joiningDate'
                }
            }, 
            'lastWorkingDay': {
                '$cond': {
                    'if': {
                        '$ne': [
                            '$lastWorkingDay', ''
                        ]
                    }, 
                    'then': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'date': {
                                '$dateFromParts': {
                                    'year': {
                                        '$year': {
                                            'date': {
                                                '$toDate': '$lastWorkingDay'
                                            }
                                        }
                                    }, 
                                    'month': {
                                        '$month': {
                                            'date': {
                                                '$toDate': '$lastWorkingDay'
                                            }
                                        }
                                    }, 
                                    'day': {
                                        '$dayOfMonth': {
                                            'date': {
                                                '$toDate': '$lastWorkingDay'
                                            }
                                        }
                                    }, 
                                    'hour': {
                                        '$hour': {
                                            'date': {
                                                '$toDate': '$lastWorkingDay'
                                            }
                                        }
                                    }, 
                                    'minute': {
                                        '$minute': {
                                            'date': {
                                                '$toDate': '$lastWorkingDay'
                                            }
                                        }
                                    }, 
                                    'second': {
                                        '$second': {
                                            'date': {
                                                '$toDate': '$lastWorkingDay'
                                            }
                                        }
                                    }, 
                                    'timezone': '+05:30'
                                }
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'else': '$lastWorkingDay'
                }
            }, 
            'resignDate': {
                '$cond': {
                    'if': {
                        '$ne': [
                            '$resignDate', ''
                        ]
                    }, 
                    'then': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'date': {
                                '$dateFromParts': {
                                    'year': {
                                        '$year': {
                                            'date': {
                                                '$toDate': '$resignDate'
                                            }
                                        }
                                    }, 
                                    'month': {
                                        '$month': {
                                            'date': {
                                                '$toDate': '$resignDate'
                                            }
                                        }
                                    }, 
                                    'day': {
                                        '$dayOfMonth': {
                                            'date': {
                                                '$toDate': '$resignDate'
                                            }
                                        }
                                    }, 
                                    'hour': {
                                        '$hour': {
                                            'date': {
                                                '$toDate': '$resignDate'
                                            }
                                        }
                                    }, 
                                    'minute': {
                                        '$minute': {
                                            'date': {
                                                '$toDate': '$resignDate'
                                            }
                                        }
                                    }, 
                                    'second': {
                                        '$second': {
                                            'date': {
                                                '$toDate': '$resignDate'
                                            }
                                        }
                                    }, 
                                    'timezone': '+05:30'
                                }
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'else': '$resignDate'
                }
            }
        }
    },
        {
            "$addFields": {
                "department": {
                    "$cond": [
                        {"$eq": ["$department", ""]},
                        "",
                        {"$toObjectId": "$department"},
                    ]
                },
                "customer": {
                    "$cond": [
                        {"$eq": ["$customer", ""]},
                        "",
                        {"$toObjectId": "$customer"},
                    ]
                },
                "role": {
                    "$cond": [{"$eq": ["$role", ""]}, "", {"$toObjectId": "$role"}]
                },
                "designation": {
                    "$cond": [
                        {"$eq": ["$designation", ""]},
                        "",
                        {"$toObjectId": "$designation"},
                    ]
                },
                'costCenter': {
                '$cond': [
                    {
                        '$eq': [
                            '$costCenter', ''
                        ]
                    }, '', {
                        '$toObjectId': '$costCenter'
                    }
                ]
            },
                
                "circle": {
                    "$cond": [{"$eq": ["$circle", ""]}, "", {"$toObjectId": "$circle"}]
                },
            }
        },
        {
        '$lookup': {
            'from': 'costCenter', 
            'localField': 'costCenter', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'costCenter': 1, 
                        '_id': 0
                    }
                }
            ], 
            'as': 'costCenterResult'
        }
    },
        {
        '$lookup': {
            'from': 'customer', 
            'localField': 'customer', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'customer': '$customerName', 
                        '_id': 0
                    }
                }
            ], 
            'as': 'customerResults'
        }
    },
        {
            "$lookup": {
                "from": "designation",
                "localField": "designation",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$project": {"_id": 0}},
                ],
                "as": "designation",
            }
        },
        {
            "$lookup": {
                "from": "department",
                "localField": "department",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$project": {"_id": 0}},
                ],
                "as": "department",
            }
        },
        {
            "$lookup": {
                "from": "userRole",
                "localField": "userRole",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$project": {"_id": 0}},
                ],
                "as": "userRoleResult",
            }
        },
        {
            "$lookup": {
                "from": "userRole",
                "localField": "role",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$project": {"_id": 0}},
                ],
                "as": "RoleResult",
            }
        },
        {
            "$lookup": {
                "from": "circle",
                "localField": "circle",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$project": {"_id": 0}},
                ],
                "as": "circle",
            }
        },
        {
            "$addFields": {
                "department": {"$arrayElemAt": ["$department.department", 0]},
                "designation": {"$arrayElemAt": ["$designation.designation", 0]},
                "circle": {"$arrayElemAt": ["$circle.circleName", 0]},
                "roleName": {"$arrayElemAt": ["$userRoleResult.roleName", 0]},
                "PmisRole": {"$arrayElemAt": ["$RoleResult.roleName", 0]},
                "customer": {"$arrayElemAt": ["$customerResults.customer", 0]},
                "costCenter":{"$arrayElemAt": ["$costCenterResult.costCenter", 0]},
            }
        },
        {
            "$addFields": {
                "monthlySalary": {"$toString": "$monthlySalary"},
                "salaryCurrency": {"$toString": "$salaryCurrency"},
                "accountNumber": {"$toString": "$accountNumber"},
                "allocationPercentage": {"$toString": "$allocationPercentage"},
                "grossCtc": {"$toString": "$grossCtc"},
            }
        },
        {
        '$addFields': {
            'uId': {
                '$toString': '$_id'
            }, 
            'RoleName': '$RoleName.roleName', 
            'Title': '$title', 
            'Employee Code': '$empCode', 
            'UST Emp Code': '$ustCode', 
            'UST Project ID': '$ustProjectId', 
            'UST Job Code': '$ustJobCode', 
            'Employee Name': '$empName', 
            "Father's Name": '$fatherName', 
            "Mother's Name": '$motherName', 
            'Marital Status': '$martialStatus', 
            'Official Email-ID': '$email', 
            'Personal Email-ID': '$personalEmailId', 
            'Date Of Birth(as Per doc)': '$dob', 
            'Contact Number': '$mobile', 
            'Blood Group': '$blood', 
            'Country': '$country', 
            'State': '$state', 
            'City': '$city', 
            'PinCode': '$pincode', 
            'Address': '$address', 
            'Permanent Country': '$pcountry', 
            'Permanent State': '$pstate', 
            'Permanent City': '$pcity', 
            'Permanent PinCode': '$ppincode', 
            'Permanent Address': '$paddress', 
            'PAN Number': '$panNumber', 
            'Aadhar Number': '$adharNumber', 
            'Circle': '$circle', 
            'Experience': '$experience', 
            'Salary': '$monthlySalary', 
            'Gross CTC': '$grossCtc', 
            'Joining Date': '$joiningDate', 
            'Last Working Day': '$lastWorkingDay', 
            'Resign Date': '$resignDate', 
            'Passport (Y/N)': '$passport', 
            'Passport Number': '$passportNumber', 
            'Bank Name': '$bankName', 
            'Bank Account Number': '$accountNumber', 
            'IFSC Code': '$ifscCode', 
            'Beneficiary Name': '$benificiaryname', 
            'Organization Level': '$orgLevel', 
            'Allocation Percentage': '$allocationPercentage', 
            'Business Unit': '$businesssUnit', 
            'Customer Name': '$customer', 
            'Role': '$roleName', 
            'Grade': '$designation', 
            'PMIS Role': '$PmisRole', 
            'Cost Center': '$costCenter', 
            'Designation': '$band', 
            'Department': '$department', 
            'Reporting Manager': '$reportingManager', 
            'L1 Approver': '$L1Approver.empName', 
            'L2 Approver': '$L2Approver.empName', 
            'Finance Approver': '$financeApprover', 
            'Reporting HR Manager': '$reportingHrManager', 
            'Asset Manager': '$assetManager.empName', 
            'L1 Vendor': '$L1Vendor.empName', 
            'L2 Vendor': '$L2Vendor.empName', 
            'Compliance': '$compliance.empName', 
            'L1 Compliance': '$L1Compliance.empName', 
            'L2 Compliance': '$L2Compliance.empName', 
            'L1 Commercial': '$L1Commercial.empName', 
            'L1 Sales': '$L1Sales.empName', 
            'L2 Sales': '$L2Sales.empName', 
            'Status': '$status', 
            'Password': '$password'
        }
    },
        {
            "$project": {
                "_id": 0,
                "userRole": 0,
                "fatherName": 0,
                "motherName": 0,
                "martialStatus": 0,
                "email": 0,
                "personalEmailId": 0,
                "dob": 0,
                "mobile": 0,
                "blood": 0,
                "country": 0,
                "state": 0,
                "city": 0,
                "pincode": 0,
                "address": 0,
                "pcountry": 0,
                "pstate": 0,
                "pcity": 0,
                "ppincode": 0,
                "paddress": 0,
                "empCode": 0,
                "panNumber": 0,
                "adharNumber": 0,
                "circle": 0,
                "experience": 0,
                "datetime": 0,
                "passportNumber": 0,
                "bankName": 0,
                "accountNumber": 0,
                "ifscCode": 0,
                "benificiaryname": 0,
                "orgLevel": 0,
                "designation": 0,
                "band": 0,
                "department": 0,
                "costCenter":0,
                "costCenterResult":0,
                # 'L1Compliance': 0,
                # 'L1Approver': 0,
                # 'L1Commercial': 0,
                # 'L1Sales': 0,
                # 'L1Vendor': 0,
                # 'L2Approver': 0,
                # 'L2Approver': 0,
                # 'L2Compliance': 0,
                "salaryCurrency": 0,
                "monthlySalary": 0,
                "grossCtc": 0,
                # 'L2Sales': 0,
                # 'L2Vendor': 0,
                # 'assetManager': 0,
                "customerResults":0,
                "compliance": 0,
                "financeApprover": 0,
                "reportingHrManager": 0,
                "reportingManager": 0,
                "img": 0,
                "img[]": 0,
                "cv": 0,
                "qw": 0,
                "benificiaryname": 0,
                "fillAddress": 0,
                "passport": 0,
                "cv[]": 0,
                "samePerAdd": 0,
                "empName": 0,
                "role": 0,
                "empName": 0,
                "title": 0,
                "uniqueId": 0,
                "status": 0,
                "userRoleName": 0,
                "password": 0,
                "RoleResult": 0,
                "userRoleResult": 0,
                # "L2Combliance":0
            }
        },
    ]
    print('arraaarraaarraaarraaarraa',arraa)
    response = cmo.finding_aggregate("userRegister", arraa)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    dataframe_resp = pd.DataFrame(response_one_df["data"])  # Assuming response_one_df is defined elsewhere

    # List of columns to merge
    columns_to_merge = [
        "L1Approver", "L1Commercial", "L1Compliance", "L1Sales", "L1Vendor", "L2Approver", 
        "L2Compliance", "L2Sales", "L2Vendor", "Finance Approver", "Reporting HR Manager", 
        "Reporting Manager", "assetManager"
    ]

    merged_dfs = []

    for col in columns_to_merge:
        if col in dataframe.columns:
            selected_columns = dataframe[["uId", col]]
            merged_df = pd.merge(
                selected_columns, dataframe_resp, left_on=col, right_on="uId", how="left"
            )
            merged_df.drop(columns=[col, "uId_x", "uId_y"], inplace=True)
            merged_df.rename(columns={"empName": col}, inplace=True)
            merged_dfs.append(merged_df)

    if merged_dfs:
        final_merged_df = pd.concat(merged_dfs, axis=1)
    else:
        final_merged_df = pd.DataFrame()

    dataframe.drop(columns=[col for col in columns_to_merge if col in dataframe.columns], inplace=True)
    final_merged_df = pd.concat([final_merged_df, dataframe], axis=1)

    # Reordering and renaming columns
    columns_order = [
        "Employee Code", "UST Emp Code","UST Project ID","UST Job Code","Title", "Employee Name", "Official Email-ID", 
        "Father's Name", "Mother's Name", "Marital Status", "Personal Email-ID", 
        "Date Of Birth(as Per doc)", "Contact Number", "Blood Group", "Country", "State", 
        "City", "PinCode", "Address", "Permanent Country", "Permanent State", 
        "Permanent City", "Permanent PinCode", "Permanent Address", "PAN Number", 
        "Aadhar Number", "Circle", "Experience", "Salary", "Gross CTC", "Joining Date", 
        "Last Working Day","Resign Date", "Passport (Y/N)", "Passport Number", "Bank Name", 
        "Bank Account Number", "IFSC Code", "Beneficiary Name", "Organization Level","Allocation Percentage","Business Unit","Customer Name",

        "Grade", "Department", "PMIS Role","Cost Center", "Role", "Designation", "Reporting Manager", 
        "Reporting HR Manager", "L1Approver", "L2Approver", "Finance Approver", 
        "L1Commercial", "L1Vendor", "L2Vendor", "L1Compliance", "L2Compliance", 
        "L1Sales", "L2Sales", "Status", "Password"
    ]

    # Create an empty DataFrame with all columns in columns_order
    empty_df = pd.DataFrame(columns=columns_order)

    # Merge the empty DataFrame with final_merged_df to ensure all columns are present
    final_merged_df = pd.concat([empty_df, final_merged_df], axis=0, ignore_index=True)

    # Ensure columns are in the correct order
    final_merged_df = final_merged_df[columns_order]

    # Formatting date column
    final_merged_df["Date Of Birth(as Per doc)"] = pd.to_datetime(
        final_merged_df["Date Of Birth(as Per doc)"], errors="coerce"
    )
    mask = final_merged_df["Date Of Birth(as Per doc)"].notnull() & ~final_merged_df["Date Of Birth(as Per doc)"].isna()
    final_merged_df["Date Of Birth(as Per doc)"] = np.where(
        mask, final_merged_df["Date Of Birth(as Per doc)"].dt.strftime("%d-%m-%Y"), ""
    )

    # Converting Bank Account Number and Aadhar Number to string if they are not null
    try:
        final_merged_df["Bank Account Number"] = final_merged_df["Bank Account Number"].apply(
            lambda x: str(x) if pd.notna(x) and x != "" else x
        )
    except TypeError:
        pass

    try:
        final_merged_df["Aadhar Number"] = final_merged_df["Aadhar Number"].apply(
            lambda x: str(x) if pd.notna(x) and x != "" else x
        )
    except TypeError:
        pass
    dateColumns=['Date Of Birth(as Per doc)','Joining Date','Last Working Day','Resign Date']
    for col in dateColumns:
        final_merged_df[col] = final_merged_df[col].apply(convertToDateBulkExport)
    
    fullPath = excelWriteFunc.excelFileWriter(final_merged_df, "Export_Employees", "Employees")
    return send_file(fullPath)


@export_blueprint.route("/export/manageCostCenter", methods=["GET"])
@token_required
def export_manageCostCenter_download(current_user):

    arra = [
        {
            "$lookup": {
                "from": "customer",
                "localField": "customer",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"Customer": "$result.customerName"}},
        {
            "$lookup": {
                "from": "zone",
                "localField": "zone",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "Zone Code": "$result.shortCode",
                "Cost Center": "$costCenter",
            }
        },
        {"$project": {"_id": 0, "Customer": 1, "Zone Code": 1, "Cost Center": 1,"Description":"$description","Business Unit":"$businessUnit","UST Project ID":"$ustProjectId"}},
    ]
    response = cmo.finding_aggregate("costCenter", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(
        dataframe, "Export_Cost_Center", "Cost Center"
    )
    return send_file(fullPath)


@export_blueprint.route("/export/manageProjectGroup", methods=["GET"])
@token_required
def export_manageProjectGroup_download(current_user):
    arra = [
        {
            "$lookup": {
                "from": "customer",
                "localField": "customerId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "customer",
            }
        },
        {
            "$lookup": {
                "from": "zone",
                "localField": "zoneId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "zone",
            }
        },
        {
            "$lookup": {
                "from": "costCenter",
                "localField": "costCenterId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "costcenter",
            }
        },
        {
            "$addFields": {
                "customer": {"$arrayElemAt": ["$customer.shortName", 0]},
                "zone": {"$arrayElemAt": ["$zone.shortCode", 0]},
                "costcenter": {"$arrayElemAt": ["$costcenter.costCenter", 0]},
            }
        },
        {
            "$addFields": {
                "Project Group ID": {
                    "$concat": ["$customer", "-", "$zone", "-", "$costcenter"]
                }
            }
        },
        {"$project": {"Project Group ID": 1, "_id": 0}},
    ]
    response = cmo.finding_aggregate("projectGroup", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)

    fullPath = excelWriteFunc.excelFileWriter(
        dataframe, "Export_Project_Group", "Project_Group"
    )
    return send_file(fullPath)


@export_blueprint.route("/export/manageCustomer", methods=["GET"])
@token_required
def export_manageCustomer_download(current_user):

    if request.method == "GET":
        arra = [
            {
                "$project": {
                    "Customer": "$customerName",
                    "Short Name": "$shortName",
                    "Contact Person": "$personName",
                    "Email ID": "$email",
                    "Mobile": "$mobile",
                    "Address": "$address",
                    "Status": "$status",
                    "_id": 0,
                }
            }
        ]
        response = cmo.finding_aggregate("customer", arra)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Customer", "Customer")
        return send_file(fullPath)


@export_blueprint.route("/export/Template/<type>/<id>", methods=["GET"])
@token_required
def export_Template(current_user,type=None, id=None):
    if request.method == "GET":
        if type in ["Site Engg", "Tracking", "Issues", "Financials"]:
            arra = [
                {"$match": {"_id": ObjectId(id)}},
                {
                    "$project": {
                        "t_tracking": 0,
                        "t_issues": 0,
                        "t_sFinancials": 0,
                        "MileStone": 0,
                        "Commercial": 0,
                    }
                },
                {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                {
                    "$lookup": {
                        "from": "customer",
                        "localField": "custId",
                        "foreignField": "_id",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "customerResult",
                    }
                },
                {
                    "$addFields": {
                        "customer": {
                            "$arrayElemAt": ["$customerResult.customerName", 0]
                        }
                    }
                },
                {"$unwind": {"path": "$t_sengg", "preserveNullAndEmptyArrays": True}},
                {
                    "$addFields": {
                        "Status": "$t_sengg.Status",
                        "dataType": "$t_sengg.dataType",
                        "fieldName": "$t_sengg.fieldName",
                        "index": "$t_sengg.index",
                        "required": "$t_sengg.required",
                        "dropdownValue": {"$ifNull": ["$t_sengg.dropdownValue", ""]},
                    }
                },
                {"$sort": {"index": 1}},
                {
                    "$project": {
                        "Customer": "$customer",
                        "Project Type": "$projectType",
                        "Sub Project": "$subProject",
                        "Project Status": "$status",
                        "FieldName": "$fieldName",
                        "Mandatory(Y/N)": "$required",
                        "InputType": "$dataType",
                        "Dropdown Values": "$dropdownValue",
                        "Status": "$Status",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("projectType", arra)
            response = response["data"]
            dataframe1 = pd.DataFrame(response)

            arra = [
                {"$match": {"_id": ObjectId(id)}},
                {
                    "$project": {
                        "t_issues": 0,
                        "t_sFinancials": 0,
                        "t_sengg": 0,
                        "MileStone": 0,
                        "Commercial": 0,
                    }
                },
                {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                {
                    "$lookup": {
                        "from": "customer",
                        "localField": "custId",
                        "foreignField": "_id",
                        "as": "customerResult",
                    }
                },
                {
                    "$addFields": {
                        "customer": {
                            "$arrayElemAt": ["$customerResult.customerName", 0]
                        }
                    }
                },
                {
                    "$unwind": {
                        "path": "$t_tracking",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "Status": "$t_tracking.Status",
                        "dataType": "$t_tracking.dataType",
                        "fieldName": "$t_tracking.fieldName",
                        "index": "$t_tracking.index",
                        "required": "$t_tracking.required",
                        "dropdownValue": {"$ifNull": ["$t_tracking.dropdownValue", ""]},
                    }
                },
                {"$sort": {"index": 1}},
                {
                    "$project": {
                        "Customer": "$customer",
                        "Project Type": "$projectType",
                        "Sub Project": "$subProject",
                        "Project Status": "$status",
                        "FieldName": "$fieldName",
                        "Mandatory(Y/N)": "$required",
                        "InputType": "$dataType",
                        "Dropdown Values": "$dropdownValue",
                        "Status": "$Status",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("projectType", arra)
            response = response["data"]
            dataframe2 = pd.DataFrame(response)

            arra = [
                {"$match": {"_id": ObjectId(id)}},
                {
                    "$project": {
                        "t_sFinancials": 0,
                        "t_sengg": 0,
                        "t_tracking": 0,
                        "Commercial": 0,
                        "MileStone": 0,
                    }
                },
                {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                {
                    "$lookup": {
                        "from": "customer",
                        "localField": "custId",
                        "foreignField": "_id",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "customerResult",
                    }
                },
                {
                    "$addFields": {
                        "customer": {
                            "$arrayElemAt": ["$customerResult.customerName", 0]
                        }
                    }
                },
                {"$unwind": {"path": "$t_issues", "preserveNullAndEmptyArrays": True}},
                {
                    "$addFields": {
                        "Status": "$t_issues.Status",
                        "dataType": "$t_issues.dataType",
                        "fieldName": "$t_issues.fieldName",
                        "index": "$t_issues.index",
                        "required": "$t_issues.required",
                        "dropdownValue": {"$ifNull": ["$t_issues.dropdownValue", ""]},
                    }
                },
                {"$sort": {"index": 1}},
                {
                    "$project": {
                        "Customer": "$customer",
                        "Project Type": "$projectType",
                        "Sub Project": "$subProject",
                        "Project Status": "$status",
                        "FieldName": "$fieldName",
                        "Mandatory(Y/N)": "$required",
                        "InputType": "$dataType",
                        "Dropdown Values": "$dropdownValue",
                        "Status": "$Status",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("projectType", arra)
            response = response["data"]
            dataframe3 = pd.DataFrame(response)

            arra = [
                {"$match": {"_id": ObjectId(id)}},
                {
                    "$project": {
                        "t_sengg": 0,
                        "t_tracking": 0,
                        "t_issues": 0,
                        "MileStone": 0,
                        "Commercial": 0,
                    }
                },
                {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                {
                    "$lookup": {
                        "from": "customer",
                        "localField": "custId",
                        "foreignField": "_id",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "customerResult",
                    }
                },
                {
                    "$addFields": {
                        "customer": {
                            "$arrayElemAt": ["$customerResult.customerName", 0]
                        }
                    }
                },
                {
                    "$unwind": {
                        "path": "$t_sFinancials",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "Status": "$t_sFinancials.Status",
                        "dataType": "$t_sFinancials.dataType",
                        "fieldName": "$t_sFinancials.fieldName",
                        "index": "$t_sFinancials.index",
                        "required": "$t_sFinancials.required",
                        "dropdownValue": {
                            "$ifNull": ["$t_sFinancials.dropdownValue", ""]
                        },
                    }
                },
                {"$sort": {"index": 1}},
                {
                    "$project": {
                        "Customer": "$customer",
                        "Project Type": "$projectType",
                        "Sub Project": "$subProject",
                        "Project Status": "$status",
                        "FieldName": "$fieldName",
                        "Mandatory(Y/N)": "$required",
                        "InputType": "$dataType",
                        "Dropdown Values": "$dropdownValue",
                        "Status": "$Status",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("projectType", arra)
            response = response["data"]
            dataframe4 = pd.DataFrame(response)

            fullPath = os.path.join(os.getcwd(), "downloadFile", "Manage_Project_Type.xlsx")
            newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")

            dataframe1.to_excel(newExcelWriter, index=False, sheet_name="Site Engg")
            dataframe2.to_excel(newExcelWriter, index=False, sheet_name="Tracking")
            dataframe3.to_excel(newExcelWriter, index=False, sheet_name="Issues")
            dataframe4.to_excel(newExcelWriter, index=False, sheet_name="Financials")

            workbook = newExcelWriter.book
            worksheet = newExcelWriter.sheets["Site Engg"]
            worksheet2 = newExcelWriter.sheets["Tracking"]
            worksheet3 = newExcelWriter.sheets["Issues"]
            worksheet4 = newExcelWriter.sheets["Financials"]
            worksheet.set_tab_color("#92d050")
            worksheet2.set_tab_color("#00B0F0")
            worksheet3.set_tab_color("#FFFF00")
            worksheet4.set_tab_color("#FF0000")

            header_format = workbook.add_format(
                {
                    "bold": True,
                    "align": "center",
                    "valign": "vtop",
                    "bg_color": "#143b64",
                    "border": 1,
                    "border_color": "black",
                    "color": "white",
                }
            )

            for col_num, value in enumerate(dataframe1.columns.values):
                worksheet.write(0, col_num, value, header_format)

            for col_num, value in enumerate(dataframe2.columns.values):
                worksheet2.write(0, col_num, value, header_format)

            for col_num, value in enumerate(dataframe3.columns.values):
                worksheet3.write(0, col_num, value, header_format)

            for col_num, value in enumerate(dataframe4.columns.values):
                worksheet4.write(0, col_num, value, header_format)

            cell_format = workbook.add_format(
                {
                    "align": "center",
                    "valign": "vtop",
                }
            )

            cols = len(dataframe1.axes[1])
            start_column = 0  # Starting column index
            end_column = cols  # Ending column index
            width = 20
            for col in range(start_column, end_column):
                worksheet.set_column(col, col, width, cell_format)

            cols = len(dataframe2.axes[1])
            start_column = 0
            end_column = cols
            width = 20
            for col in range(start_column, end_column):
                worksheet2.set_column(col, col, width, cell_format)

            cols = len(dataframe3.axes[1])
            start_column = 0
            end_column = cols
            width = 20
            for col in range(start_column, end_column):
                worksheet3.set_column(col, col, width, cell_format)

            cols = len(dataframe4.axes[1])
            start_column = 0
            end_column = cols  # Ending column index
            width = 20
            for col in range(start_column, end_column):
                worksheet4.set_column(col, col, width, cell_format)

            newExcelWriter.close()
            return send_file(fullPath)
        
        elif type in ["Template","Planning Details","Site Details","Checklistt","Snap","Acceptance Log"]:
            arra = [
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }, {
                    '$lookup': {
                        'from': 'customer', 
                        'localField': 'customer', 
                        'foreignField': '_id', 
                        'as': 'customerResult'
                    }
                }, {
                    '$lookup': {
                        'from': 'projectType', 
                        'localField': 'subProject', 
                        'foreignField': '_id', 
                        'as': 'subTypeResult'
                    }
                }, {
                    '$addFields': {
                        'customerName': {
                            '$arrayElemAt': [
                                '$customerResult.customerName', 0
                            ]
                        }, 
                        'subProjectName': {
                            '$arrayElemAt': [
                                '$subTypeResult.subProject', 0
                            ]
                        }
                    }
                }, {
                    '$facet': {
                        'outputField1': [
                            {
                                '$unwind': {
                                    'path': '$Template', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'Customer': '$customerName', 
                                    'Project Type': '$projectType', 
                                    'Sub Project': '$subProjectName', 
                                    'Activity': '$activity', 
                                    'OEM': '$oem', 
                                    'Milestone': '$complianceMilestone', 
                                    'FieldName': '$Template.fieldName', 
                                    'Mandatory(Y/N)': '$Template.required', 
                                    'InputType': '$Template.dataType', 
                                    'Dropdown Values': {
                                        '$ifNull': [
                                            '$Template.dropdownValue', ''
                                        ]
                                    }, 
                                    'Status': '$Template.Status', 
                                    '_id': 0
                                }
                            }
                        ], 
                        'outputField2': [
                            {
                                '$unwind': {
                                    'path': '$planDetails', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'Customer': '$customerName', 
                                    'Project Type': '$projectType', 
                                    'Sub Project': '$subProjectName', 
                                    'Activity': '$activity', 
                                    'OEM': '$oem', 
                                    'Milestone': '$complianceMilestone', 
                                    'FieldName': '$planDetails.fieldName', 
                                    'Mandatory(Y/N)': '$planDetails.required', 
                                    'InputType': '$planDetails.dataType', 
                                    'Dropdown Values': {
                                        '$ifNull': [
                                            '$planDetails.dropdownValue', ''
                                        ]
                                    }, 
                                    'Status': '$planDetails.Status', 
                                    '_id': 0
                                }
                            }
                        ], 
                        'outputField3': [
                            {
                                '$unwind': {
                                    'path': '$siteDetails', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'Customer': '$customerName', 
                                    'Project Type': '$projectType', 
                                    'Sub Project': '$subProjectName', 
                                    'Activity': '$activity', 
                                    'OEM': '$oem', 
                                    'Milestone': '$complianceMilestone', 
                                    'FieldName': '$siteDetails.fieldName', 
                                    'Mandatory(Y/N)': '$siteDetails.required', 
                                    'InputType': '$siteDetails.dataType', 
                                    'Dropdown Values': {
                                        '$ifNull': [
                                            '$siteDetails.dropdownValue', ''
                                        ]
                                    }, 
                                    'Status': '$siteDetails.Status', 
                                    '_id': 0
                                }
                            }
                        ], 
                        'outputField4': [
                            {
                                '$unwind': {
                                    'path': '$ranChecklist', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'Customer': '$customerName', 
                                    'Project Type': '$projectType', 
                                    'Sub Project': '$subProjectName', 
                                    'Activity': '$activity', 
                                    'OEM': '$oem', 
                                    'Milestone': '$complianceMilestone', 
                                    'FieldName': '$ranChecklist.fieldName', 
                                    'Mandatory(Y/N)': '$ranChecklist.required', 
                                    'InputType': '$ranChecklist.dataType', 
                                    'Dropdown Values': {
                                        '$ifNull': [
                                            '$ranChecklist.dropdownValue', ''
                                        ]
                                    }, 
                                    'Status': '$ranChecklist.Status', 
                                    '_id': 0
                                }
                            }
                        ], 
                        'outputField5': [
                            {
                                '$unwind': {
                                    'path': '$snap', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'Customer': '$customerName', 
                                    'Project Type': '$projectType', 
                                    'Sub Project': '$subProjectName', 
                                    'Activity': '$activity', 
                                    'OEM': '$oem', 
                                    'Milestone': '$complianceMilestone', 
                                    'FieldName': '$snap.fieldName', 
                                    'Mandatory(Y/N)': '$snap.required', 
                                    'InputType': '$snap.dataType', 
                                    'Dropdown Values': {
                                        '$ifNull': [
                                            '$snap.dropdownValue', ''
                                        ]
                                    }, 
                                    'Status': '$snap.Status', 
                                    '_id': 0
                                }
                            }
                        ], 
                        'outputField6': [
                            {
                                '$unwind': {
                                    'path': '$acceptanceLog', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'Customer': '$customerName', 
                                    'Project Type': '$projectType', 
                                    'Sub Project': '$subProjectName', 
                                    'Activity': '$activity', 
                                    'OEM': '$oem', 
                                    'Milestone': '$complianceMilestone', 
                                    'FieldName': '$acceptanceLog.fieldName', 
                                    'Mandatory(Y/N)': '$acceptanceLog.required', 
                                    'InputType': '$acceptanceLog.dataType', 
                                    'Dropdown Values': {
                                        '$ifNull': [
                                            '$acceptanceLog.dropdownValue', ''
                                        ]
                                    }, 
                                    'Status': '$acceptanceLog.Status', 
                                    '_id': 0
                                }
                            }
                        ]
                    }
                }
            ]
            response = cmo.finding_aggregate("complianceForm",arra)
            dataframes = [pd.DataFrame(response['data'][0][f'outputField{i}']) for i in range(1, 7)]
            sheet_names = [
                "Template", 
                "Planning Details", 
                "Site Details", 
                "Checklist", 
                "Snap", 
                "Acceptance Log"
            ]
            tab_colors = [
                "#92d050", 
                "#00B0F0", 
                "#FFFF00", 
                "#FF0000", 
                "#8E44AD", 
                "#F39C12"
            ]

            fullPath = os.path.join(os.getcwd(), "downloadFile", "Forms_Checklist.xlsx")
            newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
            workbook = newExcelWriter.book
            
            header_format = workbook.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vtop',
                'bg_color': '#24292d', 
                'border':1,
                'border_color':'black',
                'color':'white'
            })
            
            cell_format = workbook.add_format({
                'align':'center',
                'valign':'vtop',
            })
            
            for df, sheet_name, color in zip(dataframes, sheet_names, tab_colors):
                df.to_excel(newExcelWriter, index=False, sheet_name=sheet_name)
                worksheet = newExcelWriter.sheets[sheet_name]
                worksheet.set_tab_color(color)
                worksheet.set_column(0, len(df.columns) - 1, 20, cell_format)

                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)   
                    
            newExcelWriter.close()
            return send_file(fullPath)
        
        
        elif type == "MileStone":
            arra = [
                {"$match": {"_id": ObjectId(id)}},
                {
                    "$project": {
                        "t_tracking": 0,
                        "t_issues": 0,
                        "t_sFinancials": 0,
                        "t_sengg": 0,
                        "Commercial": 0,
                    }
                },
                {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                {
                    "$lookup": {
                        "from": "customer",
                        "localField": "custId",
                        "foreignField": "_id",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "customerResult",
                    }
                },
                {
                    "$addFields": {
                        "customer": {
                            "$arrayElemAt": ["$customerResult.customerName", 0]
                        }
                    }
                },
                {"$unwind": {"path": "$MileStone", "preserveNullAndEmptyArrays": True}},
                {
                    "$addFields": {
                        "milestone": "$MileStone.fieldName",
                        "wcc": "$MileStone.WCC Sign off",
                        "time": "$MileStone.Estimated Time (Days)",
                        "criteria": "$MileStone.Completion Criteria",
                        "predecessor": {"$ifNull": ["$MileStone.Predecessor", ""]},
                        "status": "$MileStone.Status",
                        "index": "$MileStone.index",
                    }
                },
                {"$sort": {"index": 1}},
                {
                    "$project": {
                        "Customer": "$customer",
                        "Project Type": "$projectType",
                        "Sub Project": "$subProject",
                        "Project Status": "$status",
                        "Milestone": "$milestone",
                        "WCC Sign off": "$wcc",
                        "Estimated Time (Days)": "$time",
                        "Completion Criteria": "$criteria",
                        "Predecessor": "$predecessor",
                        "Status": "$status",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("projectType", arra)
            response = response["data"]
            dataframe = pd.DataFrame(response)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_MileStone", "Milestone"
            )
            return send_file(fullPath)

        else:
            arra = [
                {"$match": {"_id": ObjectId(id)}},
                {
                    "$project": {
                        "t_tracking": 0,
                        "t_issues": 0,
                        "t_sFinancials": 0,
                        "t_sengg": 0,
                        "MileStone": 0,
                    }
                },
                {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                {
                    "$lookup": {
                        "from": "customer",
                        "localField": "custId",
                        "foreignField": "_id",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "customerResult",
                    }
                },
                {
                    "$addFields": {
                        "customer": {
                            "$arrayElemAt": ["$customerResult.customerName", 0]
                        }
                    }
                },
                {
                    "$unwind": {
                        "path": "$Commercial",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "gbpa": {"$ifNull": ["$Commercial.GBPA", ""]},
                        "code": {"$ifNull": ["$Commercial.ItemCode", ""]},
                        "rate": {"$ifNull": ["$Commercial.UnitRate", ""]},
                        "description": {"$ifNull": ["$Commercial.Description", ""]},
                        "index": "$Commercial.index",
                    }
                },
                {"$sort": {"index": 1}},
                {
                    "$project": {
                        "Customer": "$customer",
                        "Project Type": "$projectType",
                        "Sub Project": "$subProject",
                        "Project Status": "$status",
                        "GBPA": "$gbpa",
                        "Item Code": "$code",
                        "Unit Rate": "$rate",
                        "Description": "$description",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("projectType", arra)
            response = response["data"]
            dataframe = pd.DataFrame(response)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Commercial", "Commercial"
            )
            return send_file(fullPath)


@export_blueprint.route("/export/Project/<customerId>/<ProjectId>", methods=["GET"])
@token_required
def export_project(current_user,customerId=None, ProjectId=None):

    if request.method == "GET":

        projectStatus = "Active"
        if (request.args.get("statusType") != None and request.args.get("statusType") != "undefined"):
            projectStatus = request.args.get("statusType")

        arra = [{"$match": {"custId": customerId, "status": projectStatus}}]
        if ProjectId != None and ProjectId != "undefined":
            arra = arra + [{"$match": {"projectType": ObjectId(ProjectId)}}]
        arra = arra + [
            {
                "$addFields": {
                    "custId": {"$toObjectId": "$custId"},
                    "PMId": {"$toObjectId": "$PMId"},
                    "circle": {"$toObjectId": "$circle"},
                }
            },
            {
                "$lookup": {
                    "from": "userRegister",
                    "localField": "PMId",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "empDetails",
                }
            },
            {
                "$lookup": {
                    "from": "customer",
                    "localField": "custId",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "customer",
                }
            },
            {
                "$lookup": {
                    "from": "projectType",
                    "localField": "projectType",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "projectTypeArray",
                }
            },
            {
                "$lookup": {
                    "from": "circle",
                    "localField": "circle",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "circle",
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
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                            }
                        },
                        {
                            "$addFields": {
                                "projectGroupId": {
                                    "$concat": [
                                        "$customer",
                                        "-",
                                        "$zone",
                                        "-",
                                        "$costCenter",
                                    ]
                                },
                                "zoneId": {"$toString": "$zoneId"},
                                "customerId": {"$toString": "$customerId"},
                                "costCenterId": {"$toString": "$costCenterId"},
                                "uniqueId": {"$toString": "$_id"},
                            }
                        },
                        {"$project": {"_id": 0, "projectGroupId": 1}},
                    ],
                    "as": "result",
                }
            },
            {
                "$addFields": {
                    "customer": {"$arrayElemAt": ["$customer.customerName", 0]},
                    "emp": {"$arrayElemAt": ["$empDetails.empName", 0]},
                    "projectType": {
                        "$arrayElemAt": ["$projectTypeArray.projectType", 0]
                    },
                    "subProject": {"$arrayElemAt": ["$projectTypeArray.subProject", 0]},
                    "circle": {"$arrayElemAt": ["$circle.circleName", 0]},
                    "projectGroup": {"$arrayElemAt": ["$result.projectGroupId", 0]},
                }
            },
            {
                "$project": {
                    "Customer": "$customer",
                    "Project ID": "$projectId",
                    "Project Group": "$projectGroup",
                    "Project Type": "$projectType",
                    "Sub Project": "$subProject",
                    "Project Manager": "$emp",
                    "Circle": "$circle",
                    "Start Date": "$startDate",
                    "End Date": "$endDate",
                    "Status": "$status",
                    "_id": 0,
                }
            },
        ]
        if (request.args.get("projectId") != None and request.args.get("projectId") != "undefined"):
            arra = arra + [{"$match": {"Project ID": request.args.get("projectId")}}]
        if (
            request.args.get("projectGroup") != None
            and request.args.get("projectGroup") != "undefined"
        ):
            arra = arra + [
                {"$match": {"Project Group": request.args.get("projectGroup")}}
            ]
        if (
            request.args.get("projectType") != None
            and request.args.get("projectType") != "undefined"
        ):
            arra = arra + [
                {"$match": {"Project Type": request.args.get("projectType")}}
            ]
        if (
            request.args.get("projectManager") != None
            and request.args.get("projectManager") != "undefined"
        ):
            arra = arra + [
                {"$match": {"Project Manager": request.args.get("projectManager")}}
            ]
        if (
            request.args.get("circle") != None
            and request.args.get("circle") != "undefined"
        ):
            arra = arra + [{"$match": {"Circle": request.args.get("circle")}}]
        response = cmo.finding_aggregate("project", arra)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(
            dataframe, "Export_Project", "Project"
        )
        return send_file(fullPath)


@export_blueprint.route("/export/siteIdwithProjectTye/<id>", methods=["GET"])
@token_required
def export_siteId_with_project_type(current_user,id=None):
    arra = [
        {
            "$addFields": {
                "SubProjectId": {"$toObjectId": "$SubProjectId"},
                'Site_Completion Date1': {
                '$cond': {
                    'if': {
                        ' $or': [
                            {
                                '$eq': [
                                    '$Site_Completion Date', None
                                ]
                            }, {
                                '$eq': [
                                    '$Site_Completion Date', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        ' $toDate': '$Site_Completion Date'
                    }
                }
            },
                # "siteEndDate1": {"$toDate": "$siteEndDate"},
                'siteEndDate1': {
                '$cond': {
                    'if': {
                        ' $or': [
                            {
                                '$eq': [
                                    '$siteEndDate', None
                                ]
                            }, {
                                '$eq': [
                                    '$siteEndDate', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        ' $toDate': '$siteEndDate'
                    }
                }
            },
            }
        },
        {
            "$addFields": {
                "Site_Completion Date": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                        "then": {
                            "$dateToString": {
                                "date": "$Site_Completion Date1",
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "else": "",
                    }
                },
                "siteageing": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                        "then": {
                            "$round": {
                                "$divide": [
                                    {
                                        "$subtract": [
                                            "$siteEndDate1",
                                            "$Site_Completion Date1",
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
            "$lookup": {
                "from": "projectType",
                "localField": "SubProjectId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "projectType": "$result.projectType",
                "subProject": "$result.subProject",
                "_id": {"$toString": "$_id"},
                "SubProjectId": {"$toString": "$SubProjectId"},
                "projectuniqueId": {"$toObjectId": "$projectuniqueId"},
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
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "PMId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "PMarray",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$PMarray",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {"$addFields": {"PMName": "$PMarray.empName"}},
                    {"$project": {"PMName": 1, "_id": 0}},
                ],
                "as": "projectArray",
            }
        },
        {"$unwind": {"path": "$projectArray", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "PMName": "$projectArray.PMName",
                "siteId": "$siteid",
                "projectuniqueId": {"$toString": "$projectuniqueId"},
                "siteStartDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$siteStartDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "siteEndDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$siteEndDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "rfidate": {
                    "$dateToString": {
                        "date": {"$toDate": "$rfidate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "Systen Unique ID": "$_id",
            }
        },
        {
            "$project": {
                "projectArray": 0,
                "assignerId": 0,
                "result": 0,
                "Site_Completion Date1": 0,
                "siteEndDate1": 0,
                "_id": 0,
                "SubProjectId": 0,
                "new_u_id": 0,
                "projectuniqueId": 0,
            }
        },
    ]
    response = cmo.finding_aggregate("SiteEngineer", arra)
    return respond(response)


@export_blueprint.route("/export/projectEventLog/<id>", methods=["GET"])
@token_required
def export_project_event_log(current_user,id=None):
    arra = [
        {"$match": {"projectuniqueId": id}},
        {"$addFields": {"SiteEngineerId": {"$toObjectId": "$SiteEngineerId"}}},
        {
            "$lookup": {
                "from": "SiteEngineer",
                "localField": "SiteEngineerId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "siteData",
            }
        },
        {"$unwind": {"path": "$siteData", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"UpdatedBy": {"$toObjectId": "$UpdatedBy"}}},
        {
            "$lookup": {
                "from": "userRegister",
                "localField": "UpdatedBy",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "SiteId": "$siteData.Site Id",
                "email": "$result.email",
                "UpdatedAt": {
                    "$dateToString": {
                        "format": "%d-%m-%Y %H:%M:%S",
                        "date": {
                            "$dateFromString": {
                                "dateString": "$UpdatedAt",
                                "format": "%Y-%m-%d %H:%M:%S",
                            }
                        },
                    }
                },
            }
        },
        {
            "$project": {
                "Site ID": "$SiteId",
                "User Email": "$email",
                "Date & Time": "$UpdatedAt",
                "Updated Data": "$updatedData",
                "_id": 0,
            }
        },
    ]
    response = cmo.finding_aggregate("SiteEngineerEventLogs", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Project_Event_log", "Event_log")
    return send_file(fullPath)


@export_blueprint.route("/export/siteEventLog/<id>", methods=["GET"])
@token_required
def export_site_event_log(current_user,id=None):
    arra = [
        {"$match": {"SiteEngineerId": id}},
        {"$addFields": {"SiteEngineerId": {"$toObjectId": "$SiteEngineerId"}}},
        {
            "$lookup": {
                "from": "SiteEngineer",
                "localField": "SiteEngineerId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "siteData",
            }
        },
        {"$unwind": {"path": "$siteData", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"UpdatedBy": {"$toObjectId": "$UpdatedBy"}}},
        {
            "$lookup": {
                "from": "userRegister",
                "localField": "UpdatedBy",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "SiteId": "$siteData.Site Id",
                "email": "$result.email",
                "UpdatedAt": {
                    "$dateToString": {
                        "format": "%d-%m-%Y %H:%M:%S",
                        "date": {
                            "$dateFromString": {
                                "dateString": "$UpdatedAt",
                                "format": "%Y-%m-%d %H:%M:%S",
                            }
                        },
                    }
                },
            }
        },
        {
            "$project": {
                "Site ID": "$SiteId",
                "User Email": "$email",
                "Date & Time": "$UpdatedAt",
                "Updated Data": "$updatedData",
                "_id": 0,
            }
        },
    ]
    response = cmo.finding_aggregate("SiteEngineerEventLogs", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Site_Event_log", "Event_log")
    return send_file(fullPath)


@export_blueprint.route("/export/milestoneEventLog/<id>", methods=["GET", "POST"])
@token_required
def export_milestone_event_log(current_user,id=None):
    if request.method == "GET":
        arra = [
            {"$match": {"milestoneId": id}},
            {
                "$addFields": {
                    "milestoneId": {"$toObjectId": "$milestoneId"},
                    "UpdatedAt": {
                        "$dateToString": {
                            "format": "%d-%m-%Y %H:%M:%S",
                            "date": {
                                "$dateFromString": {
                                    "dateString": "$UpdatedAt",
                                    "format": "%Y-%m-%d %H:%M:%S",
                                }
                            },
                        }
                    },
                }
            },
            {
                "$lookup": {
                    "from": "milestone",
                    "localField": "milestoneId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$lookup": {
                                "from": "SiteEngineer",
                                "localField": "siteId",
                                "foreignField": "_id",
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                                "as": "siteData",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$siteData",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {"$addFields": {"Site Id": "$siteData.Site Id"}},
                        {"$project": {"Name": 1, "Site Id": 1, "_id": 0}},
                    ],
                    "as": "result",
                }
            },
            {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
            {"$addFields": {"UpdatedBy": {"$toObjectId": "$UpdatedBy"}}},
            {
                "$lookup": {
                    "from": "userRegister",
                    "localField": "UpdatedBy",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "userData",
                }
            },
            {"$unwind": {"path": "$userData", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "Milestone": "$result.Name",
                    "Site ID": "$result.Site Id",
                    "User Email": "$userData.email",
                    "Time & Date": "$UpdatedAt",
                    "Updated Data": "$updatedData",
                    "_id": 0,
                }
            },
            {"$sort": {"_id": -1}},
        ]
        response = cmo.finding_aggregate("milestoneEventLogs", arra)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_milestone_Event_log", "Event_log")
        return send_file(fullPath)


@export_blueprint.route("/export/poInvoice/<id>", methods=["GET"])
@token_required
def export_poInvoice(current_user,id=None):
    poStatus = ["Open","Closed","Short Closed"]
    if (request.args.get("poStatus")!=None and request.args.get("poStatus")!='undefined'):
        poStatus= [request.args.get("poStatus")]
    arra = []
    if  id not in ['','undefined',None]:
        arra=arra+[
            {
            '$match': {
                'deleteStatus': {'$ne': 1}, 
                
                'customer':id
            }
        },
        ]        
    arra = arra+[
        {
            '$match': {
                'deleteStatus': {'$ne': 1}, 
                "poStatus":{'$in':poStatus},
                'projectGroup': {'$in': projectGroup_str(current_user['userUniqueId'])},
                
            }
        }, {
            '$sort':{
                '_id':-1
            }
        }
    ]
    if (request.args.get("itemCodeStatus")!=None and request.args.get("itemCodeStatus")!='undefined'):
        arra = arra + [
            {
                '$match':{
                    'itemCodeStatus': request.args.get("itemCodeStatus")
                }
            }
        ]
    if request.args.get("searvhView")!=None and request.args.get("searvhView")!='undefined':
        searvhView = request.args.get("searvhView").strip()
        arra = arra + [
            {
                '$match': {
                    '$or': [
                        {
                            'itemCode': {
                                '$regex': searvhView, 
                                '$options': 'i'
                            }
                        }, {
                            'poNumber': {
                                '$regex': searvhView, 
                                '$options': 'i'
                            }
                        }, {
                            'gbpa': {
                                '$regex': searvhView, 
                                '$options': 'i'
                            }
                        }, {
                            'description': {
                                '$regex': searvhView, 
                                '$options': 'i'
                            }
                        }, {
                            'itemCodeStatus': {
                                '$regex': searvhView, 
                                '$options': 'i'
                            }
                        }, {
                            'poStatus': {
                                '$regex': searvhView, 
                                '$options': 'i'
                            }
                        }
                    ]
                }
            }
        ]
    arra = arra + [
        {
            '$lookup': {
                'from': 'invoice', 
                'let': {
                    'projectGroup': '$projectGroup', 
                    'poNumber': '$poNumber', 
                    'itemCode': '$itemCode'
                }, 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }, {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$projectGroup', '$$projectGroup'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$poNumber', '$$poNumber'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$itemCode', '$$itemCode'
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'qty': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$qty', ''
                                        ]
                                    }, 
                                    'then': 0, 
                                    'else': '$qty'
                                }
                            }
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'qty': {
                                '$sum': '$qty'
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'invoicedQty': '$result.qty'
            }
        }, {
            '$addFields': {
                'customer': {
                    '$toObjectId': '$customer'
                },
                'projectGroup': {
                    '$toObjectId': '$projectGroup'
                },
                'invoicedQty': '$result.qty'
            }
        }, {
            '$lookup': {
                'from': 'customer', 
                'localField': 'customer', 
                'foreignField': '_id', 
                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                'as': 'customerResult'
            }
        }, {
            '$lookup': {
                'from': 'projectGroup', 
                'localField': 'projectGroup', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        "$match":{
                            'deleteStatus':{'$ne':1}
                        }
                    }, {
                        '$lookup': {
                            'from': 'customer', 
                            'localField': 'customerId', 
                            'foreignField': '_id', 
                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                            'projectGroupId': {
                                '$concat': [
                                    '$customer', '-', '$zone', '-', '$costCenter'
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'projectGroupId': 1, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'projectGroupIdResult'
            }
        }, {
            '$unwind': {
                'path': '$customerResult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$unwind': {
                'path': '$projectGroupIdResult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'Customer': '$customerResult.customerName',  
                'Project Group': '$projectGroupIdResult.projectGroupId', 
                'invoicedQty': {
                    '$ifNull': [
                        '$invoicedQty', 0
                    ]
                }
            }
        }, {
            '$addFields': {
                'poStartDate': {
                    '$cond': [
                        {
                            '$eq': [
                                '$poStartDate', ''
                            ]
                        }, '', {
                            '$dateToString': {
                                'date': {
                                    '$toDate': '$poStartDate'
                                }, 
                                'format': '%d-%m-%Y', 
                                'timezone': 'Asia/Kolkata'
                            }
                        }
                    ]
                }, 
                'poEndDate': {
                    '$cond': [
                        {
                            '$eq': [
                                '$poEndDate', ''
                            ]
                        }, '', {
                            '$dateToString': {
                                'date': {
                                    '$toDate': '$poEndDate'
                                }, 
                                'format': '%d-%m-%Y', 
                                'timezone': 'Asia/Kolkata'
                            }
                        }
                    ]
                }
            }
        }, {
            '$addFields': {
                'povalidity': {
                    '$cond': [
                        {
                            '$eq': [
                                '$poEndDate', ''
                            ]
                        }, '', {
                            '$divide': [
                                {
                                    '$subtract': [
                                        {
                                            '$toDate': '$poEndDate'
                                        }, {
                                            '$toDate': current_date()
                                        }
                                    ]
                                }, 86400000
                            ]
                        }
                    ]
                }
            }
        }, {
            '$addFields': {
                'openQty': {
                    '$subtract': [
                        '$initialPoQty', '$invoicedQty'
                    ]
                }
            }
        }, {
            '$addFields': {
                'OpenPoValue': {
                    '$multiply': [
                        '$unitRate(INR)', '$openQty'
                    ]
                }
            }
        }, {
            '$project': {
                'Customer':1,
                "Project Group":1,
                "GBPA":'$gbpa',
                "PO Number":'$poNumber',
                'PO Start Date':'$poStartDate',
                'PO End Date':'$poEndDate',
                "Validity(Days)":'$povalidity',
                "Item Code":'$itemCode',
                "Description":"$description",
                "Unit Rate(INR)":'$unitRate(INR)',
                "Initial PO Qty":'$initialPoQty',
                "Invoiced Quantity":"$invoicedQty",
                "Open Quantity(Post Invoice)":'$openQty',
                "Open PO Value(INR)-Invoiced":'$OpenPoValue',
                "Item Code Status":'$itemCodeStatus',
                "PO Status":'$poStatus',
                "_id":0
            }
        }
    ]
    response = cmo.finding_aggregate("PoInvoice", arra)['data']
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_POInvoice", "PO_Invoice")
    return send_file(fullPath)


@export_blueprint.route("/export/Invoice/<id>", methods=["GET", "POST"])
@token_required
def export_Invoice(current_user,id=None):
    arra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'projectGroup': {
                        '$in': projectGroup_str(current_user['userUniqueId'])
                    },
                    'customer':id
                }
            }
        ]
    if (request.args.get("wccNumber")!=None and request.args.get("wccNumber")!='undefined'):
        arra = arra + [
            {
                '$match':{
                    'wccNumber':{
                        '$regex':re.escape(request.args.get("wccNumber").strip()),
                        '$options':'i'
                    }
                }
            }
        ]
    if (request.args.get("invoiceNumber")!=None and request.args.get("invoiceNumber")!='undefined'):
        arra = arra + [
            {
                '$match':{
                    'invoiceNumber':{
                        '$regex':re.escape(request.args.get("invoiceNumber").strip()),
                        '$options':'i'
                    }
                }
            }
        ]
    
    arra = arra + [
        {
            '$sort':{
                '_id':-1
            }
        }, {
            '$addFields':{
                'uniqueId':{
                    '$toString':'$_id'
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
                }, 
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
                        'format': '%d-%m-%Y', 
                        'timezone': 'Asia/Kolkata', 
                        'date': '$invoiceDate'
                    }
                }, 
                'wccSignOffdate': {
                    '$dateToString': {
                        'format': '%d-%m-%Y', 
                        'timezone': 'Asia/Kolkata', 
                        'date': '$wccSignOffdate'
                    }
                }
            }
        }, {
            '$addFields': {
                'date': '$invoiceDate'
            }
        }, {
            '$addFields': {
                'date': {
                    '$dateFromString': {
                        'dateString': '$date', 
                        'format': '%d-%m-%Y',
                        'timezone':'Asia/Kolkata'
                    }
                }
            }
        }, {
            '$addFields': {
                'year': {
                    '$year': '$date'
                }, 
                'month':{
                    '$month':'$date'
                }
            }  
        }, {
            '$addFields': {
                'month': {
                    '$switch': {
                        'branches': [
                            {
                                'case': {
                                    '$eq': [
                                        '$month', 1
                                    ]
                                }, 
                                'then': 'Jan'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 2
                                    ]
                                }, 
                                'then': 'Feb'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 3
                                    ]
                                }, 
                                'then': 'Mar'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 4
                                    ]
                                }, 
                                'then': 'Apr'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 5
                                    ]
                                }, 
                                'then': 'May'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 6
                                    ]
                                }, 
                                'then': 'Jun'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 7
                                    ]
                                }, 
                                'then': 'Jul'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 8
                                    ]
                                }, 
                                'then': 'Aug'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 9
                                    ]
                                }, 
                                'then': 'Sep'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 10
                                    ]
                                }, 
                                'then': 'Oct'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 11
                                    ]
                                }, 
                                'then': 'Nov'
                            }, {
                                'case': {
                                    '$eq': [
                                        '$month', 12
                                    ]
                                }, 
                                'then': 'Dec'
                            }
                        ], 
                        'default': None
                    }
                }
            }
        }, {
            '$project':{
                '_id':0,
                'date':0,
            }
        }
    ]
    if (request.args.get("year")!=None and request.args.get("year")!='undefined'):
        arra = arra + [
            {
                '$match':{
                    'year':int(request.args.get("year"))
                }
            }
        ]
    if (request.args.get("month")!=None and request.args.get("month")!='undefined'):
        arra = arra + [
            {
                '$match':{
                    'month':request.args.get("month")
                }
            }
        ]
    response = cmo.finding_aggregate("invoice",arra)
    invoicedf = pd.DataFrame.from_dict(response['data'])
    if not invoicedf.empty:
    
        projectIddf = projectid()
        projectIddf.rename(columns={
            'projectId': 'projectIdName',
            'projectuniqueId': 'projectId'
        }, inplace=True)
        projectIddf.drop(columns=['projectGroup'],inplace=True)
        
        projectGroupdf = projectGroup()
        projectGroupdf.rename(columns={
            'Customer':'customerName',
            'projectGroupName':'projectGroupId'
        }, inplace=True)
    
        
        arra = [
            {
                '$project': {
                    'siteId': {'$toString': '$_id'}, 
                    'siteIdName': '$Site Id',
                    'ssidName': '$systemId',
                    '_id': 0
                }
            }
        ]
        
        sitedf = pd.DataFrame.from_dict(cmo.finding_aggregate("SiteEngineer",arra)['data'])
        mergedSite = invoicedf.merge(sitedf,on='siteId',how='left')
        mergedPG = invoicedf.merge(projectGroupdf,on='projectGroup',how='left')
        mergedPD = invoicedf.merge(projectIddf,on='projectId',how='left')
        
        invoicedf['siteIdName'] = mergedSite['siteIdName']
        invoicedf['ssidName'] = mergedSite['ssidName']
        invoicedf['projectIdName'] = mergedPD['projectIdName']
        invoicedf['projectGroupId'] = mergedPG['projectGroupId']
        invoicedf['customerName'] = mergedPG['customerName']
        invoicedf['amount'] = invoicedf['qty']*invoicedf['unitRate']
        
        
        if (request.args.get("siteId")!=None and request.args.get("siteId")!='undefined'):
            siteFilter = request.args.get("siteId").strip()
            invoicedf = invoicedf[invoicedf['siteIdName'].str.contains(siteFilter, regex=True, case=False, na=False)]
            
        if (request.args.get("projectGroup")!=None and request.args.get("ProjectGroup")!='undefined'):
            pgFilter = request.args.get("projectGroup").strip()
            invoicedf = invoicedf[invoicedf['projectGroupId'].str.contains(pgFilter, regex=True, case=False, na=False)]
        
        if (request.args.get("projectId")!=None and request.args.get("projectId")!='undefined'):
            pdFilter = request.args.get("projectId").strip()
            invoicedf = invoicedf[invoicedf['projectIdName'].str.contains(pdFilter, regex=True, case=False, na=False)]  
        
        invoicedf.rename(columns={
            "year":"Year",
            "month":"Month",
            'customerName':'Customer',
            "projectGroupId":"Project Group",
            "projectIdName":'Project ID',
            "ssidName":"SSID",
            "siteIdName":"Site Id",
            "wccNumber":"WCC No",
            "wccSignOffdate":"WCC SignOff Date",
            "poNumber":"PO Number",
            "itemCode":'Item Code',
            "qty":"Invoiced Quantity",
            "invoiceNumber":"Invoice Number",
            "invoiceDate":'Invoice Date',
            "unitRate":"Unit Rate",
            "amount":'Amount',
            "status":'Status',
        }, inplace=True)
        
        columns_to_start = ["Year","Month","Customer","Project Group","Project ID","SSID","Site Id","WCC No","WCC SignOff Date","PO Number","Item Code","Invoiced Quantity","Invoice Number","Invoice Date","Unit Rate","Amount","Status"]
        
        for col in columns_to_start:
            if col not in invoicedf.columns:
                invoicedf[col] = '' 
                
        invoicedf = invoicedf[columns_to_start]
            
        fullPath = excelWriteFunc.excelFileWriter(invoicedf, "Export_Invoice", "Invoice")
        return send_file(fullPath)
    else:
        invoicedf = pd.DataFrame()
        fullPath = excelWriteFunc.excelFileWriter(invoicedf, "Export_Invoice", "Invoice")
        return send_file(fullPath)
        
      
@export_blueprint.route("/export/trackingWorkDone/<id>", methods=["GET"])
@token_required
def export_tracking_workdone(current_user,id=None):
    arra = [
        {
            '$match':{
                'customer':id
            }
        }, {
            "$addFields": {
                "unitRate(INR)": {
                    "$cond": {
                        "if": {"$eq": ["$unitRate(INR)", ""]},
                        "then": 0,
                        "else": "$unitRate(INR)",
                    }
                },
                "initialPoQty": {
                    "$cond": {
                        "if": {"$eq": ["$initialPoQty", ""]},
                        "then": 0,
                        "else": "$initialPoQty",
                    }
                },
            }
        },
        {
            "$group": {
                "_id": {"projectGroup": "$projectGroup", "itemCode": "$itemCode"},
                "Data": {"$first": "$$ROOT"},
                "initialPoQty": {"$sum": "$initialPoQty"},
            }
        },
        {"$set": {"Data.initialPoQty": "$initialPoQty"}},
        {"$replaceRoot": {"newRoot": "$Data"}},
        {
            "$lookup": {
                "from": "invoice",
                "let": {"projectGroup": "$projectGroup", "itemCode": "$itemCode"},
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$projectGroup", "$$projectGroup"]},
                                    {"$eq": ["$itemCode", "$$itemCode"]},
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "qty": {
                                "$cond": {
                                    "if": {"$eq": ["$qty", ""]},
                                    "then": 0,
                                    "else": "$qty",
                                }
                            }
                        }
                    },
                    {"$group": {"_id": None, "qty": {"$sum": "$qty"}}},
                ],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"invoicedQty": {"$ifNull": ["$result.qty", 0]}}},
        {
            "$lookup": {
                "from": "workDone",
                "let": {"projectGroup": "$projectGroup", "itemCode": "$itemCode"},
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$projectGroup", "$$projectGroup"]},
                                    {"$eq": ["$itemCode", "$$itemCode"]},
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "qty": {
                                "$cond": {
                                    "if": {"$eq": ["$quantity", ""]},
                                    "then": 0,
                                    "else": "$quantity",
                                }
                            }
                        }
                    },
                    {"$group": {"_id": None, "quantity": {"$sum": "$quantity"}}},
                ],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"workDoneQty": {"$ifNull": ["$result.quantity", 0]}}},
        {"$addFields": {"totalQty": {"$sum": ["$invoicedQty", "$workDoneQty"]}}},
        {
            "$addFields": {
                "customer": {
                    "$cond": [
                        {"$eq": ["$customer", ""]},
                        "",
                        {"$toObjectId": "$customer"},
                    ]
                },
                "projectId": {
                    "$cond": [
                        {"$eq": ["$projectId", ""]},
                        "",
                        {"$toObjectId": "$projectId"},
                    ]
                },
                "projectGroup": {
                    "$cond": [
                        {"$eq": ["$projectGroupd", ""]},
                        "",
                        {"$toObjectId": "$projectGroup"},
                    ]
                },
                "projectType": {
                    "$cond": [
                        {"$eq": ["$projectType", ""]},
                        "",
                        {"$toObjectId": "$projectType"},
                    ]
                },
                "_id": {"$toString": "$_id"},
                "uniqueId": {"$toString": "$_id"},
            }
        },
        {
            "$lookup": {
                "from": "customer",
                "localField": "customer",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "customerResult",
            }
        },
        {
            "$lookup": {
                "from": "project",
                "localField": "projectId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "projectResult",
            }
        },
        {
            "$lookup": {
                "from": "projectType",
                "localField": "projectType",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "projectTypeResult",
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
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "zone",
                        }
                    },
                    {"$unwind": {"path": "$zone", "preserveNullAndEmptyArrays": True}},
                    {
                        "$lookup": {
                            "from": "costCenter",
                            "localField": "costCenterId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                        }
                    },
                    {
                        "$addFields": {
                            "projectGroupId": {
                                "$concat": [
                                    "$customer",
                                    "-",
                                    "$zone",
                                    "-",
                                    "$costCenter",
                                ]
                            }
                        }
                    },
                    {"$project": {"projectGroupId": 1, "_id": 0}},
                ],
                "as": "projectGroupIdResult",
            }
        },
        {"$unwind": {"path": "$customerResult", "preserveNullAndEmptyArrays": True}},
        {"$unwind": {"path": "$projectResult", "preserveNullAndEmptyArrays": True}},
        {"$unwind": {"path": "$projectTypeResult", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$projectGroupIdResult",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$addFields": {
                "customer": "$customerResult.customerName",
                "projectId": "$projectResult.projectId",
                "projectType": "$projectTypeResult.projectType",
                "projectGroup": "$projectGroupIdResult.projectGroupId",
            }
        },
        {"$addFields": {"openQty": {"$subtract": ["$initialPoQty", "$totalQty"]}}},
        {
            "$project": {
                "Customer": "$customer",
                "Project Group": "$projectGroup",
                "Project Type": "$projectType",
                "Project ID": "$projectId",
                "GBPA": "$gbpa",
                "Item Code": "$itemCode",
                "Description": "$description",
                "Unit Rate (INR)": "$unitRate(INR)",
                "Initial PO Qty (Sum of all Open PO)": "$initialPoQty",
                "Invoiced Quantity": "$invoicedQty",
                "Workdone Quantity": "$workDoneQty",
                "Open Qunatity": "$openQty",
                "_id": 0,
            }
        },
    ]
    if (
        request.args.get("customer") != None
        and request.args.get("customer") != "undefined"
    ):
        arra = arra + [{"$match": {"Customer": request.args.get("customer")}}]

    if (
        request.args.get("projectGroup") != None
        and request.args.get("projectGroup") != "undefined"
    ):
        arra = arra + [{"$match": {"Project Group": request.args.get("projectGroup")}}]

    if (
        request.args.get("projectId") != None
        and request.args.get("projectId") != "undefined"
    ):
        arra = arra + [{"$match": {"Project ID": request.args.get("projectId")}}]

    if (
        request.args.get("itemCode") != None
        and request.args.get("itemCode") != "undefined"
    ):
        arra = arra + [{"$match": {"Item Code": request.args.get("itemCode")}}]

    response = cmo.finding_aggregate("PoInvoice", arra)
    response = response["data"]
    dataframe = pd.DataFrame(response)

    fullPath = excelWriteFunc.excelFileWriter(
        dataframe, "Export_Tracking_WorkDone", "PO_Tracking_WorkDone"
    )
    return send_file(fullPath)


@export_blueprint.route("/export/poWorkDone/<id>", methods=["GET"])
@token_required
def export_accural_revenue(current_user,id=None):
    arra = [
        {
            '$match':{
                'deleteStatus': {'$ne': 1},
                "customerId":id,
                'projectuniqueId':{
                        '$in':projectId_str(current_user['userUniqueId'])
                }
            }
        }
    ]
    subProjectArray = []
    subArray = (sub_project(current_user['userUniqueId'],id))
    for i in subArray:
        for k in i['uid']:
            subProjectArray.append(k)
    if request.args.get("projectType")!=None and request.args.get("projectType")!="undefined":
            subProjectArray = request.args.get("projectType").split(',')
    if request.args.get("siteId")!=None and request.args.get("siteId")!='undefined':
        arra = arra + [
            {
                '$match': {
                    'Site Id':{
                        '$regex':request.args.get("siteId"),
                        '$options': 'i'
                    }
                }
            }
        ]
        subProjectArray = []
        subArray = (sub_project(current_user['userUniqueId'],id))
        for i in subArray:
            for k in i['uid']:
                subProjectArray.append(k)
    arra = arra + [
        {
            '$match':{
                'SubProjectId':{
                    '$in':subProjectArray
                }
            }
        }
    ]
    if request.args.get("siteBillingStatus")!=None and request.args.get("siteBillingStatus")!="undefined":
        siteStatus = request.args.get("siteBillingStatus")
        arra = arra + [
            {
                '$match':{
                    'siteBillingStatus':siteStatus
                }
            }
        ]
    arra = arra + [
        {
            '$lookup': {
                'from': 'milestone', 
                'localField': '_id', 
                'foreignField': 'siteId', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {'$ne': 1}, 
                            'Name': {'$in': ['MS1', 'MS2']}, 
                            'mileStoneStatus': 'Closed'
                        }
                    }, {
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
                            }
                        }
                    }, {
                        '$sort': {
                            'Name': 1
                        }
                    }
                ], 
                'as': 'Milestone'
            }
        }, {
            '$match': {
                'Milestone': {
                    '$ne': []
                }
            }
        }, {
            '$addFields': {
                'MS1': {
                    '$arrayElemAt': [
                        {
                            '$filter': {
                                'input': '$Milestone', 
                                'as': 'milestone', 
                                'cond': {
                                    '$eq': [
                                        '$$milestone.Name', 'MS1'
                                    ]
                                }
                            }
                        }, 0
                    ]
                }, 
                'MS2': {
                    '$arrayElemAt': [
                        {
                            '$filter': {
                                'input': '$Milestone', 
                                'as': 'milestone', 
                                'cond': {
                                    '$eq': [
                                        '$$milestone.Name', 'MS2'
                                    ]
                                }
                            }
                        }, 0
                    ]
                }
            }
        }, {
            '$addFields': {
                'MS1': '$MS1.CC_Completion Date',
                'MS2': '$MS2.CC_Completion Date',
            }
        }, {
            '$project':{
                'projectuniqueId': 1, 
                'SubProjectId': 1, 
                'siteBillingStatus': 1, 
                'BAND':{
                    '$ifNull':['$BAND',""]
                }, 
                'ACTIVITY':{
                    '$ifNull':['$ACTIVITY',""]
                },
                'siteId': {
                    '$toString': '$_id'
                }, 
                '_id': 0,
                'Site Id':1,
                "MS1":1,
                "MS2":1,
                "systemId":1,
            }
        }
    ]
    response = cmo.finding_aggregate("SiteEngineer",arra)
    if len(response['data']):
        pivotedData = pd.DataFrame(response['data'])
        pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(len(str(x).split('-'))))
        
        
        projectGroupdf = projectGroup()
        projectIddf = projectid()
        subProjectdf = subproject()
        masterunitRatedf = masterunitRate()
        
        mergedData = projectIddf.merge(projectGroupdf,on='projectGroup',how='left')
        projectIddf['projectGroup'] = mergedData['projectGroupName']
        projectIddf['customer'] = mergedData['Customer']
        
        mergedData = pivotedData.merge(projectIddf,on="projectuniqueId",how="left")
        pivotedData['projectId'] = mergedData['projectId']
        pivotedData['projectGroup'] = mergedData['projectGroup']
        pivotedData['customer'] = mergedData['customer']
        
        
        mergedData = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
        pivotedData['subProject'] = mergedData['subProject']
        pivotedData['projectType'] = mergedData['projectType']
        
        mergedData = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
        masterunitRatedf['projectId'] = mergedData['projectId']
        
        mergedData = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
        masterunitRatedf['subProject'] = mergedData['subProject']
        masterunitRatedf['projectType'] = mergedData['projectType']
        
        pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        masterunitRatedf.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        
        
        
        df_macro = pivotedData[pivotedData['projectType'] == 'MACRO']
        if "BAND" in df_macro:
            del df_macro['BAND']
        # df_ibs_operation = pivotedData[pivotedData['projectType'].isin(['IBS', 'OPERATION'])]
        df_dismantle_relocation = pivotedData[(pivotedData['projectType'] == 'RELOCATION') | (pivotedData['subProject'] == 'RELOCATION DISMANTLE')]
        if "ACTIVITY" in df_dismantle_relocation:
            del df_dismantle_relocation['ACTIVITY']
        # df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO', 'IBS', 'OPERATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
        df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO','RELOCATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
        if 'BAND' in df_other.columns:
            del df_other['BAND']
        
        if 'ACTIVITY' in df_other.columns:
            del df_other['ACTIVITY']
        
        
        df_macro_merged = pd.merge(df_macro, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'ACTIVITY'], how='left')
        df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        # df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
        final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
        
        
        def calculate_amounts(row):
            amount1 = row['rate'] * 0.65 if pd.notna(row['MS1']) else 0
            amount2 = row['rate'] * 0.35 if pd.notna(row['MS2']) else 0
            return pd.Series([amount1, amount2])

        final_df[['amount1', 'amount2']] = final_df.apply(calculate_amounts, axis=1)
        
        final_df['final_amount'] = final_df['amount1'] + final_df['amount2']
        final_df.rename(columns={
                'customer':'Customer',
                'projectGroup':'Project Group',
                'projectType': 'Project Type', 
                'projectId': 'Project ID', 
                'subProject': 'Sub Project', 
                'systemId':'SSID',
                'Site Id': 'Site Id', 
                'BAND': 'BAND', 
                'ACTIVITY': 'ACTIVITY', 
                'MS1': 'MS1 Completion Date', 
                'MS2': 'MS2 Completion Date', 
                'siteBillingStatus': 'Billing Status', 
                'amount1': 'Unbilled MS1 Done', 
                'amount2': 'Unbilled MS2 Done', 
                'final_amount': 'Total Unbilled', 
                'itemCode01': 'Item Code 1',
                'itemCode02': 'Item Code 2',
                'itemCode03': 'Item Code 3',
                'itemCode04': 'Item Code 4',
                'itemCode05': 'Item Code 5',
                'itemCode06': 'Item Code 6',
                'itemCode07': 'Item Code 7',
            }, inplace=True)
        columns_to_start = ['Customer','Project Group','Project Type', 'Project ID', 'Sub Project','SSID', 'Site Id', 'BAND', 'ACTIVITY', 'MS1 Completion Date', 'MS2 Completion Date', 'Billing Status', 'Unbilled MS1 Done', 'Unbilled MS2 Done', 'Total Unbilled', 'Item Code 1', 'Item Code 2', 'Item Code 3', 'Item Code 4', 'Item Code 5', 'Item Code 6', 'Item Code 7']
        for col in columns_to_start:
            if col not in final_df.columns:
                final_df[col] = ''
        final_df = final_df[columns_to_start]
        for col in ["MS1 Completion Date","MS2 Completion Date"]:
            final_df[col] = final_df[col].apply(convertToDateBulkExport)
        fullPath = excelWriteFunc.excelFileWriter(final_df, "Export_PO_WorkDone", "PO_WorkDone")
        return send_file(fullPath)
    else:
        final_df = pd.DataFrame(response['data'])
        fullPath = excelWriteFunc.excelFileWriter(final_df, "Export_PO_WorkDone", "PO_WorkDone")
        return send_file(fullPath)




@export_blueprint.route("/export/accrualRevenue", methods=["GET"])
@token_required
def Export_Accrual_Revenue(current_user):

    curr = datetime.datetime.now()
    prev_curr = datetime.datetime.now() - relativedelta(months=1)

    first_day_one = curr.replace(day=1)
    next_month_first = curr.replace(day=28) + datetime.timedelta(
        days=4
    )  # Adding 4 days compensates for months with less than 31 days
    last_day_first = next_month_first - datetime.timedelta(days=next_month_first.day)

    first_day_two = prev_curr.replace(day=1)
    next_month_second = prev_curr.replace(day=28) + datetime.timedelta(
        days=4
    )  # Adding 4 days compensates for months with less than 31 days
    last_day_two = next_month_second - datetime.timedelta(days=next_month_second.day)

    first_day_curr = first_day_one.strftime("%d/%m/%Y")
    last_day_curr = last_day_first.strftime("%d/%m/%Y")
    first_day_prev = first_day_two.strftime("%d/%m/%Y")
    last_day_prev = last_day_two.strftime("%d/%m/%Y")

    current_month = curr.strftime("%b")
    prev_month = prev_curr.strftime("%b")
    current_year = str(curr.year)

    print(current_month, "current_month")
    print(prev_month, "prev_month")

    arra = [
        {
            "$addFields": {
                "SubProjectId": {"$toObjectId": "$SubProjectId"},
                "projectuniqueId": {"$toObjectId": "$projectuniqueId"},
            }
        },
        {
            "$lookup": {
                "from": "projectType",
                "localField": "SubProjectId",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                    {
                        "$lookup": {
                            "from": "customer",
                            "localField": "custId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "customerResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$customerResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {"$addFields": {"customerName": "$customerResult.customerName"}},
                    {"$project": {"projectType": 1, "customerName": 1, "_id": 0}},
                ],
                "as": "ProjectTypeArray",
            }
        },
        {
            "$lookup": {
                "from": "project",
                "localField": "projectuniqueId",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                    }
                                },
                                {
                                    "$addFields": {
                                        "projectGroupId": {
                                            "$concat": [
                                                "$customer",
                                                "-",
                                                "$zone",
                                                "-",
                                                "$costCenter",
                                            ]
                                        },
                                        "zoneId": {"$toString": "$zoneId"},
                                        "customerId": {"$toString": "$customerId"},
                                        "costCenterId": {"$toString": "$costCenterId"},
                                        "uniqueId": {"$toString": "$_id"},
                                    }
                                },
                                {
                                    "$project": {
                                        "projectGroupUid": {"$toString": "$_id"},
                                        "_id": 0,
                                        "projectGroupId": 1,
                                    }
                                },
                            ],
                            "as": "result",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$result",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "projectGroup": "$result.projectGroupId",
                            "projectGroupUid": "$result.projectGroupUid",
                        }
                    },
                    {
                        "$project": {
                            "projectGroup": 1,
                            "projectId": 1,
                            "_id": 0,
                            "projectGroupUid": 1,
                        }
                    },
                ],
                "as": "projectArray",
            }
        },
        {"$unwind": {"path": "$ProjectTypeArray", "preserveNullAndEmptyArrays": True}},
        {"$unwind": {"path": "$projectArray", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "customer": "$ProjectTypeArray.customerName",
                "projectType": "$ProjectTypeArray.projectType",
                "subProject": "$ProjectTypeArray.subProject",
                "projectId": "$projectArray.projectId",
                "projectGroup": "$projectArray.projectGroup",
                "projectGroupUid": "$projectArray.projectGroupUid",
                "_id": {"$toString": "$_id"},
            }
        },
        {
            "$project": {
                "customer": 1,
                "projectGroup": 1,
                "projectType": 1,
                "projectId": 1,
                "systemId": 1,
                "_id": {"$toObjectId": "$_id"},
                "siteUid": {"$toObjectId": "$_id"},
            }
        },
        {
            "$lookup": {
                "from": "milestone",
                "localField": "siteUid",
                "foreignField": "siteId",
                "pipeline": [
                    {
                        "$match": {
                            "deleteStatus": {"$ne": 1},
                            "Name": {"$in": ["MS1", "MS2"]},
                        }
                    },
                    {
                        "$addFields": {
                            "CC_Completion Date": {
                                "$ifNull": ["$CC_Completion Date", None]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "CC_Completion Date": {"$toDate": "$CC_Completion Date"},
                            "checkGreater": {
                                "$dateFromString": {
                                    "dateString": first_day_prev,
                                    "format": "%d/%m/%Y",
                                }
                            },
                            "checklesser": {
                                "$dateFromString": {
                                    "dateString": last_day_prev,
                                    "format": "%d/%m/%Y",
                                }
                            },
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$CC_Completion Date", "$checkGreater"]},
                                    {"$lte": ["$CC_Completion Date", "$checklesser"]},
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "documentMonth": {"$month": "$CC_Completion Date"},
                            "documentYear": {"$year": "$CC_Completion Date"},
                        }
                    },
                ],
                "as": "Milestone_1",
            }
        },
        {
            "$lookup": {
                "from": "milestone",
                "localField": "siteUid",
                "foreignField": "siteId",
                "pipeline": [
                    {
                        "$match": {
                            "deleteStatus": {"$ne": 1},
                            "Name": {"$in": ["MS1", "MS2"]},
                        }
                    },
                    {
                        "$addFields": {
                            "CC_Completion Date": {
                                "$ifNull": ["$CC_Completion Date", None]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "CC_Completion Date": {"$toDate": "$CC_Completion Date"},
                            "checkGreater": {
                                "$dateFromString": {
                                    "dateString": first_day_curr,
                                    "format": "%d/%m/%Y",
                                }
                            },
                            "checklesser": {
                                "$dateFromString": {
                                    "dateString": last_day_curr,
                                    "format": "%d/%m/%Y",
                                }
                            },
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$CC_Completion Date", "$checkGreater"]},
                                    {"$lte": ["$CC_Completion Date", "$checklesser"]},
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "documentMonth": {"$month": "$CC_Completion Date"},
                            "documentYear": {"$year": "$CC_Completion Date"},
                        }
                    },
                ],
                "as": "Milestone_2",
            }
        },
        {
            "$addFields": {
                "length1": {"$size": "$Milestone_1"},
                "length2": {"$size": "$Milestone_2"},
            }
        },
        {
            "$match": {
                "$expr": {"$or": [{"$gte": ["$length1", 1]}, {"$gte": ["$length2", 1]}]}
            }
        },
        {"$addFields": {"uid": {"$toString": "$_id"}}},
        {
            "$lookup": {
                "from": "workDone",
                "localField": "uid",
                "foreignField": "systemId",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {"$match": {"itemCode": {"$ne": ""}}},
                ],
                "as": "workDoneresult",
            }
        },
        {"$addFields": {"workDoneresultIndex": {"$size": "$workDoneresult"}}},
        {"$match": {"workDoneresultIndex": {"$gte": 1}}},
        {"$unwind": {"path": "$workDoneresult", "preserveNullAndEmptyArrays": True}},
        {
            "$facet": {
                "outputFieldN1": [
                    {
                        "$unwind": {
                            "path": "$Milestone_1",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "itemCode": "$workDoneresult.itemCode",
                            "documentMonth": "$Milestone_1.documentMonth",
                            "documentYear": "$Milestone_1.documentYear",
                            "mName": "$Milestone_1.Name",
                            "qty": {"$ifNull": ["$workDoneresult.quantity", 0]},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "Milestone_1": 0,
                            "length": 0,
                            "workDoneresultIndex": 0,
                            "workDoneresult": 0,
                            "length1": 0,
                            "length2": 0,
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "documentMonth": "$documentMonth",
                                "documentYear": "$documentYear",
                                "itemCode": "$itemCode",
                                "mName": "$mName",
                                "projectGroup": "$projectGroup",
                                "projectId": "$projectId",
                            },
                            "qty": {"$sum": "$qty"},
                            "documentMonth": {"$first": "$documentMonth"},
                            "documentYear": {"$first": "$documentYear"},
                            "itemCode": {"$first": "$itemCode"},
                            "mName": {"$first": "$mName"},
                            "projectGroup": {"$first": "$projectGroup"},
                            "projectId": {"$first": "$projectId"},
                            "data": {"$push": "$$ROOT"},
                        }
                    },
                    {"$unwind": {"path": "$data", "preserveNullAndEmptyArrays": True}},
                    {
                        "$addFields": {
                            "data.documentMonth": "$documentMonth",
                            "data.documentYear": "$documentYear",
                            "data.qty": "$qty",
                            "data.itemCode": "$itemCode",
                            "data.mName": "$mName",
                        }
                    },
                    {"$replaceRoot": {"newRoot": "$data"}},
                    {"$project": {"siteUid": 0, "systemId": 0, "uid": 0}},
                    {
                        "$lookup": {
                            "from": "projectType",
                            "let": {"itemCode": "$itemCode"},
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$match": {"Commercial": {"$exists": {"$ne": None}}}},
                                {
                                    "$unwind": {
                                        "path": "$Commercial",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "UnitRate": "$Commercial.UnitRate",
                                        "ItemCode": "$Commercial.ItemCode",
                                    }
                                },
                                {"$project": {"UnitRate": 1, "ItemCode": 1, "_id": 0}},
                                {
                                    "$match": {
                                        "$expr": {"$eq": ["$ItemCode", "$$itemCode"]}
                                    }
                                },
                            ],
                            "as": "result",
                        }
                    },
                    {
                        "$addFields": {
                            "unitRate": {
                                "$ifNull": [
                                    {"$arrayElemAt": ["$result.UnitRate", 0]},
                                    0,
                                ]
                            }
                        }
                    },
                    {"$project": {"result": 0}},
                ],
                "outputFieldN2": [
                    {
                        "$unwind": {
                            "path": "$Milestone_2",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "itemCode": "$workDoneresult.itemCode",
                            "documentMonth": "$Milestone_2.documentMonth",
                            "documentYear": "$Milestone_2.documentYear",
                            "mName": "$Milestone_2.Name",
                            "qty": {"$ifNull": ["$workDoneresult.quantity", 0]},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "Milestone_2": 0,
                            "length": 0,
                            "workDoneresultIndex": 0,
                            "workDoneresult": 0,
                            "length1": 0,
                            "length2": 0,
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "documentMonth": "$documentMonth",
                                "documentYear": "$documentYear",
                                "itemCode": "$itemCode",
                                "mName": "$mName",
                                "projectGroup": "$projectGroup",
                                "projectId": "$projectId",
                            },
                            "qty": {"$sum": "$qty"},
                            "documentMonth": {"$first": "$documentMonth"},
                            "documentYear": {"$first": "$documentYear"},
                            "itemCode": {"$first": "$itemCode"},
                            "mName": {"$first": "$mName"},
                            "projectGroup": {"$first": "$projectGroup"},
                            "projectId": {"$first": "$projectId"},
                            "data": {"$push": "$$ROOT"},
                        }
                    },
                    {"$unwind": {"path": "$data", "preserveNullAndEmptyArrays": True}},
                    {
                        "$addFields": {
                            "data.documentMonth": "$documentMonth",
                            "data.documentYear": "$documentYear",
                            "data.qty": "$qty",
                            "data.itemCode": "$itemCode",
                            "data.mName": "$mName",
                        }
                    },
                    {"$replaceRoot": {"newRoot": "$data"}},
                    {"$project": {"siteUid": 0, "systemId": 0, "uid": 0}},
                    {
                        "$lookup": {
                            "from": "projectType",
                            "let": {"itemCode": "$itemCode"},
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$match": {"Commercial": {"$exists": {"$ne": None}}}},
                                {
                                    "$unwind": {
                                        "path": "$Commercial",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "UnitRate": "$Commercial.UnitRate",
                                        "ItemCode": "$Commercial.ItemCode",
                                    }
                                },
                                {"$project": {"UnitRate": 1, "ItemCode": 1, "_id": 0}},
                                {
                                    "$match": {
                                        "$expr": {"$eq": ["$ItemCode", "$$itemCode"]}
                                    }
                                },
                            ],
                            "as": "result",
                        }
                    },
                    {
                        "$addFields": {
                            "unitRate": {
                                "$ifNull": [
                                    {"$arrayElemAt": ["$result.UnitRate", 0]},
                                    0,
                                ]
                            }
                        }
                    },
                    {"$project": {"result": 0}},
                ],
            }
        },
    ]
    dataAggr = cmo.finding_aggregate("SiteEngineer", arra)

    respDf_1 = pd.DataFrame(dataAggr["data"][0]["outputFieldN1"])
    respDf_2 = pd.DataFrame(dataAggr["data"][0]["outputFieldN2"])

    pivot_df_1 = respDf_1.pivot_table(
        index=[
            "itemCode",
            "projectId",
            "projectGroup",
            "customer",
            "projectType",
            "unitRate",
        ],
        columns="mName",
        values="qty",
        aggfunc="sum",
        fill_value=0,
    )

    pivot_df_1 = pivot_df_1.reindex(columns=["MS1", "MS2"], fill_value=0)

    pivot_df_1["amount_p"] = (
        pivot_df_1["MS1"] * pivot_df_1.index.get_level_values("unitRate") * 0.65
        + pivot_df_1["MS2"] * pivot_df_1.index.get_level_values("unitRate") * 0.35
    )

    pivot_df_2 = respDf_2.pivot_table(
        index=[
            "itemCode",
            "projectId",
            "projectGroup",
            "customer",
            "projectType",
            "unitRate",
        ],
        columns="mName",
        values="qty",
        aggfunc="sum",
        fill_value=0,
    )

    pivot_df_2 = pivot_df_2.reindex(columns=["MS1", "MS2"], fill_value=0)

    pivot_df_2["amount_c"] = (
        pivot_df_2["MS1"] * pivot_df_2.index.get_level_values("unitRate") * 0.65
        + pivot_df_2["MS2"] * pivot_df_2.index.get_level_values("unitRate") * 0.35
    )

    listrespDf_1 = respDf_1[
        ["itemCode", "projectId", "projectGroup", "customer", "projectType"]
    ]
    listrespDf_2 = respDf_2[
        ["itemCode", "projectId", "projectGroup", "customer", "projectType"]
    ]

    listrespDf_1 = listrespDf_1.drop_duplicates()
    listrespDf_2 = listrespDf_2.drop_duplicates()

    final_data_1 = listrespDf_1.merge(
        pivot_df_1,
        on=["itemCode", "projectId", "projectGroup", "customer", "projectType"],
        how="right",
    )

    final_data_2 = listrespDf_2.merge(
        pivot_df_2,
        on=["itemCode", "projectId", "projectGroup", "customer", "projectType"],
        how="right",
    )

    final_data_1["MS1_p"] = final_data_1["MS1"]
    final_data_1["MS2_p"] = final_data_1["MS2"]
    final_data_2["MS1_c"] = final_data_2["MS1"]
    final_data_2["MS2_c"] = final_data_2["MS2"]

    final_data = pd.merge(
        final_data_1,
        final_data_2,
        on=["itemCode", "projectId", "projectGroup", "customer", "projectType"],
        how="outer",
    )

    final_data = final_data.replace(np.NaN, 0)

    final_data["MS1_p"] = final_data["MS1_p"].astype(int)
    final_data["MS2_p"] = final_data["MS2_p"].astype(int)
    final_data["MS1_c"] = final_data["MS1_c"].astype(int)
    final_data["MS2_c"] = final_data["MS2_c"].astype(int)

    if final_data.empty:
        return respond(
            {"icon": "error", "msg": "No Data Found In DataBase", "status": 400}
        )

    jsonD = final_data.to_json(orient="records")

    dataAggr["data"] = json.loads(jsonD)

    ordered_keys = [
        "Customer",
        "Project Group",
        "Project Type",
        "Project ID",
        "Item Code",
        "MS1 Quantity(" + prev_month + current_year + ")",
        "MS2 Quantity(" + prev_month + current_year + ")",
        "Accrual(" + prev_month + current_year + ")",
        "MS1 Quantity(" + current_month + current_year + ")",
        "MS2 Quantity(" + current_month + current_year + ")",
        "Accrual(" + current_month + current_year + ")",
    ]

    reordered_data = []

    # for item in dataAggr['data']:
    #     item['Customer'] = item.pop("customer")
    #     item['Project Group'] = item.pop('projectGroup')
    #     item['Project Type'] = item.pop('projectType')
    #     item['Project ID'] = item.pop("projectId")
    #     item['Item Code'] = item.pop('itemCode')
    #     item["MS1 Quantity("+prev_month+current_year+")"] = item.pop('MS1_p')
    #     item["MS2 Quantity("+prev_month+current_year+")"] = item.pop('MS2_p')
    #     item["Accrual("+prev_month+current_year+")"] = item.pop('amount_p')
    #     item["MS1 Quantity("+current_month+current_year+")"] = item.pop('MS1_c')
    #     item["MS2 Quantity("+current_month+current_year+")"] = item.pop('MS2_c')
    #     item["Accrual("+current_month+current_year+")"] = item.pop('amount_c')
    for item in dataAggr["data"]:
        new_data = {
            "Customer": item.pop("customer"),
            "Project Group": item.pop("projectGroup"),
            "Project Type": item.pop("projectType"),
            "Project ID": item.pop("projectId"),
            "Item Code": item.pop("itemCode"),
            "MS1 Quantity(" + prev_month + current_year + ")": item.pop("MS1_p"),
            "MS2 Quantity(" + prev_month + current_year + ")": item.pop("MS2_p"),
            "Accrual(" + prev_month + current_year + ")": item.pop("amount_p"),
            "MS1 Quantity(" + current_month + current_year + ")": item.pop("MS1_c"),
            "MS2 Quantity(" + current_month + current_year + ")": item.pop("MS2_c"),
            "Accrual(" + current_month + current_year + ")": item.pop("amount_c"),
        }
        reordered_item = {key: new_data[key] for key in ordered_keys}
        reordered_data.append(reordered_item)
        dataAggr["data"] = reordered_data
    response = dataAggr["data"]
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(
        dataframe, "Export_Accrual_Revenue", "Accrual_Revenue"
    )
    return send_file(fullPath)



@export_blueprint.route("/export/EvmFinancial", methods=["GET", "POST"])
@token_required
def export_evmfinancial(current_user):
    if request.method == "GET":
        allData = request.get_json()
        year = int(allData["year"])
        viewBy = allData["viewBy"].split(",")
        projection_stage = {
            '_id': 0, 
            "Customer":'$customer',
            "Cost Center":'$costCenter',
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}
        conversion = {
            '1': 'Jan',
            '2': 'Feb',
            '3': 'Mar',
            '4': 'Apr',
            '5': 'May',
            '6': 'Jun',
            '7': 'Jul',
            '8': 'Aug',
            '9': 'Sep',
            '10': 'Oct',
            '11': 'Nov',
            '12': 'Dec'
        }
        
        for field in viewBy:
            
            projection_stage['AOP Target('+conversion[field]+' '+str(year)+')'] = {'$ifNull':["$" + 'aop-'+field,""]}
            projection_stage['PV Target('+conversion[field]+' '+str(year)+')'] = {'$ifNull':["$" + 'pv-'+field,""]}
            # projection_stage['pv-'+field] = 1
            # add_fields_stage['AOP Target('+conversion[field]+' '+str(year)+')']={'$round':['AOP Target('+conversion[field]+' '+str(year)+')']}
            # add_fields_stage['PV Target('+conversion[field]+' '+str(year)+')']={'$round':['PV Target('+conversion[field]+' '+str(year)+')']}
        
        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'status': 'Active', 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
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
                                                }, {
                                                    '$addFields': {
                                                        'uniqueId': {
                                                            '$toString': '$_id'
                                                        }
                                                    }
                                                }
                                            ], 
                                            'as': 'costCenter'
                                        }
                                    }, {
                                        '$project': {
                                            'costCenter': {
                                                '$arrayElemAt': [
                                                    '$costCenter.costCenter', 0
                                                ]
                                            }, 
                                            'uniqueId': {
                                                '$arrayElemAt': [
                                                    '$costCenter.uniqueId', 0
                                                ]
                                            }, 
                                            'customer': {
                                                '$arrayElemAt': [
                                                    '$customer.customerName', 0
                                                ]
                                            }, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$project': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$result.customer', 0
                                    ]
                                }, 
                                'costCenter': {
                                    '$arrayElemAt': [
                                        '$result.costCenter', 0
                                    ]
                                }, 
                                'uniqueId': {
                                    '$arrayElemAt': [
                                        '$result.uniqueId', 0
                                    ]
                                }, 
                                '_id': 0, 
                                'projectgroupuid': {
                                    '$toString': '$projectGroup'
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$uniqueId', 
                                'data': {
                                    '$first': '$$ROOT'
                                }
                            }
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$data'
                            }
                        }, {
                            '$sort': {
                                'uniqueId': 1
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$lookup': {
                    'from': 'earnValue', 
                    'localField': 'uniqueId', 
                    'foreignField': 'uniqueId', 
                    'pipeline':[
                        {
                            '$match':{
                                'year':year
                            }
                        }
                    ],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'result.customer': '$customer', 
                    'result.costCenter': '$costCenter', 
                    'result.uniqueId': '$uniqueId', 
                    'result.projectgroupuid': '$projectgroupuid'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }, {
                '$sort':{
                    'uniqueId':1
                }
            }, 
            # {
            #    '$addFields': add_fields_stage
            # }
        ]
        zoneArray=[{
            '$addFields': {
                'projectgroupid': {
                    '$toObjectId': '$projectgroupuid'
                }
            }
            }, {
                '$lookup': {
                    'from': 'projectGroup', 
                    'localField': 'projectgroupid', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
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
                                    }, {
                                        '$project': {
                                            'shortCode': 1
                                        }
                                    }
                                ], 
                                'as': 'zoneResults'
                            }
                        }, {
                            '$addFields': {
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResults.shortCode', 0
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'zone'
                }
            }, {
                '$addFields': {
                    'zone': {
                        '$arrayElemAt': [
                            '$zone.zone', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'projectgroupid': 0
                }
            }, {
                '$sort': {
                    'uniqueId': 1
                }
            }]
        
        dataResp = cmo.finding_aggregate("projectAllocation", arra+zoneArray)['data']
        if len (dataResp):
            dataRespdf = pd.DataFrame.from_dict(dataResp)
            month = [int(x) for x in viewBy]
            year = int(year)
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$addFields': {
                        'invoiceDate': {
                            '$toDate': '$invoiceDate'
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
                        }
                    }
                }, {
                    '$addFields': {
                        'month': {
                            '$month': '$invoiceDate'
                        }, 
                        'year': {
                            '$year': '$invoiceDate'
                        }
                    }
                }, {
                    '$match':{
                    'month':{
                        '$in':month
                    } ,
                    'year':year
                    }
                },{
                    '$project': {
                        'projectgroupuid': '$projectGroup', 
                        'month': 1, 
                        'year': 1, 
                        'amount': {
                            '$multiply': [
                                '$qty', '$unitRate'
                            ]
                        }, 
                        '_id': 0
                    }
                }
            ]
            invoiceData = cmo.finding_aggregate("invoice",arra)['data']
            if len(invoiceData):
                invoicedf = pd.DataFrame.from_dict(invoiceData)
                result = invoicedf.groupby(['month', 'year', 'projectgroupuid'], as_index=False)['amount'].sum()
                pivot_df = result.pivot_table(index=['year', 'projectgroupuid'],columns='month',values='amount',fill_value=0  ).reset_index()
                pivot_df.columns = [f'amount-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                pivot_df.drop(columns=['year'], inplace=True)
                mergedDf = dataRespdf.merge(pivot_df,on='projectgroupuid',how='left')
                rename_dict = {
                    'amount-1': f'Achievement(Jan {year})',
                    'amount-2': f'Achievement(Feb {year})',
                    'amount-3': f'Achievement(Mar {year})',
                    'amount-4': f'Achievement(Apr {year})',
                    'amount-5': f'Achievement(May {year})',
                    'amount-6': f'Achievement(Jun {year})',
                    'amount-7': f'Achievement(Jul {year})',
                    'amount-8': f'Achievement(Aug {year})',
                    'amount-9': f'Achievement(Sep {year})',
                    'amount-10':f'Achievement(Oct {year})',
                    'amount-11':f'Achievement(Nov {year})',
                    'amount-12':f'Achievement(Dec {year})',
                }
                mergedDf.rename(columns=rename_dict, inplace=True)
                aop_targets = [f'AOP Target({conversion[month]} {year})' for month in viewBy]
                pv_targets = [f'PV Target({conversion[month]} {year})' for month in viewBy]
                achievements = [f'Achievement({conversion[month]} {year})' for month in viewBy]
                base_columns = ['Customer', 'Cost Center','Zone']
                new_order = base_columns
                for  aop, pv, ach in zip(aop_targets, pv_targets, achievements):
                    new_order.extend([aop, pv, ach])
                for col in new_order:
                    if col not in mergedDf.columns:
                        mergedDf[col] = ''
                mergedDf = mergedDf[new_order]
                dataResp = mergedDf
        dataframe = pd.DataFrame(dataResp)
        if "projectgroupuid" in dataframe:
            del dataframe['projectgroupuid']
        if "year" in dataframe:
            del dataframe['year']
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_EVMFinancial", "EVMFinancial")
        return send_file(fullPath)

    if request.method == "POST":
        allData = request.get_json()
        viewBy = allData["Monthly"].split(",")
        viewType = allData['viewType']
        amountType = allData['amountType']
        customer = allData['customer']
        costCenter = allData['costCenter']
        businessUnit = allData['businessUnit']
        month = []
        year = []
        value_vars = []
        acctual_pv = []
        acctual_amount = []
        for i in viewBy:
            if i.split("-")[0] not in month:
                month.append(i.split("-")[0])
            if i.split("-")[1] not in year:
                year.append(i.split("-")[1])
            acctual_pv.append(f'pv-{i}')
            acctual_amount.append(f'amount-{i}')
        
        Month = [int(i) for i in month]
        Year = [int(i) for i in year]
        projection_stage = {
            '_id': 0, 
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "businessUnit":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        for field in month:
            projection_stage[f'pv-{field}'] = {"$ifNull":[f'$pv-{field}',0]}
            value_vars.append(f'pv-{field}')
            
        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'status': 'Active', 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
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
                                                }, {
                                                    '$addFields': {
                                                        'uniqueId': {
                                                            '$toString': '$_id'
                                                        }
                                                    }
                                                }
                                            ], 
                                            'as': 'costCenter'
                                        }
                                    }, {
                                        '$project': {
                                            'costCenter': {
                                                '$arrayElemAt': [
                                                    '$costCenter.costCenter', 0
                                                ]
                                            }, 
                                            'uniqueId': {
                                                '$arrayElemAt': [
                                                    '$costCenter.uniqueId', 0
                                                ]
                                            },
                                            'businessUnit': {
                                                '$arrayElemAt': [
                                                    '$costCenter.businessUnit', 0
                                                ]
                                            }, 
                                            'customer': {
                                                '$arrayElemAt': [
                                                    '$customer.customerName', 0
                                                ]
                                            }, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$project': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$result.customer', 0
                                    ]
                                }, 
                                'costCenter': {
                                    '$arrayElemAt': [
                                        '$result.costCenter', 0
                                    ]
                                }, 
                                'uniqueId': {
                                    '$arrayElemAt': [
                                        '$result.uniqueId', 0
                                    ]
                                }, 
                                'businessUnit': {
                                    '$arrayElemAt': [
                                        '$result.businessUnit', 0
                                    ]
                                }, 
                                '_id': 0, 
                                'projectgroupuid': {
                                    '$toString': '$projectGroup'
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$uniqueId', 
                                'data': {
                                    '$first': '$$ROOT'
                                }
                            }
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$data'
                            }
                        }, {
                            '$sort': {
                                'uniqueId': 1
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    # 'preserveNoneAndEmptyArrays': True
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }
        ]
        if costCenter != [""]:
            arra = arra + [
                {
                    '$match':{
                        'uniqueId':{
                            '$in':costCenter
                        }
                    }
                }
            ]
        if businessUnit != [""]:
            arra = arra + [
                {
                    '$match':{
                        'businessUnit':{
                            '$in':businessUnit
                        }
                    }
                }
            ]
        arra = arra +[
            {
                '$lookup': {
                    'from': 'earnValue', 
                    'localField': 'uniqueId', 
                    'foreignField': 'uniqueId', 
                    'pipeline':[
                        {
                            '$match':{
                                'year':{
                                    '$in':Year
                                }
                            }
                        }
                    ],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    # 'preserveNoneAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'result.customer': '$customer', 
                    'result.costCenter': '$costCenter', 
                    'result.uniqueId': '$uniqueId', 
                    'result.businessUnit': '$businessUnit', 
                    'result.projectgroupuid': '$projectgroupuid'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }, {
                '$sort':{
                    'uniqueId':1
                }
            }
        ]
        if customer!=[""]:
            arra[1]['$lookup']['pipeline'].insert(0,{
                "$match":{
                    'custId':{
                        '$in':customer
                    }
                }
            })
        
        zoneArray=[{
            '$addFields': {
                'projectgroupid': {
                    '$toObjectId': '$projectgroupuid'
                }
            }
            }, {
                '$lookup': {
                    'from': 'projectGroup', 
                    'localField': 'projectgroupid', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
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
                                    }, {
                                        '$project': {
                                            'shortCode': 1
                                        }
                                    }
                                ], 
                                'as': 'zoneResults'
                            }
                        }, {
                            '$addFields': {
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResults.shortCode', 0
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'zone'
                }
            }, {
                '$addFields': {
                    'zone': {
                        '$arrayElemAt': [
                            '$zone.zone', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'projectgroupid': 0
                }
            }, {
                '$sort': {
                    'uniqueId': 1
                }
            }]
        
        dataResp = cmo.finding_aggregate("projectAllocation", arra+zoneArray)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            pd.set_option('display.max_columns', None)
            if amountType == "DOLLAR":
                arra = [
                    {
                        '$project': {
                            '_id': 0
                        }
                    }
                ]
                exRatedf = pd.DataFrame.from_dict(cmo.finding_aggregate("exChangeRate",arra)['data'])
                dataRespdf = dataRespdf.merge(exRatedf,on="year",how="left")
                dataRespdf[value_vars] = dataRespdf[value_vars].div(dataRespdf['rate'], axis=0)
            dataRespdf[value_vars] = dataRespdf[value_vars].div(1000000, axis=0).round(2)
            df_pivot = dataRespdf.melt(id_vars=["uniqueId", "projectgroupuid", "customer", "costCenter",'zone', "year"],value_vars=value_vars)
            df_pivot = df_pivot.pivot_table(index=["uniqueId", "projectgroupuid", "customer", "costCenter",'zone'],columns=["year", "variable"],values="value",aggfunc="sum").reset_index()
            df_pivot.columns = [f"{col[1]}-{col[0]}" if isinstance(col[0], int) else col[0] for col in df_pivot.columns]
            df_pivot = df_pivot[["uniqueId", "projectgroupuid", "customer", "costCenter",'zone']+acctual_pv]
            if viewType == "Cumulative":
                del acctual_pv[0]
                for i in range(1, len(acctual_pv) + 1):
                    df_pivot.iloc[:, 4 + i] += df_pivot.iloc[:, 4 + i - 1]
            # for business unit 
            arra = [
                {
                    '$project': {
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'businessUnit': 1, 
                        '_id': 0
                    }
                }
            ]
            businessUnitdf = pd.DataFrame.from_dict((cmo.finding_aggregate("costCenter",arra))['data'])
            df_pivot = df_pivot.merge(businessUnitdf,on='uniqueId',how='left')
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$addFields': {
                        'invoiceDate': {
                            '$toDate': '$invoiceDate'
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
                        }
                    }
                }, {
                    '$addFields': {
                        'month': {
                            '$month': '$invoiceDate'
                        }, 
                        'year': {
                            '$year': '$invoiceDate'
                        }
                    }
                }, {
                    '$match':{
                        'month':{
                            '$in':Month
                        },
                        'year':{
                            '$in':Year
                        }
                    }
                }, {
                    '$project': {
                        'projectgroupuid': '$projectGroup', 
                        'month': 1, 
                        'year': 1, 
                        'amount': {
                            '$multiply': [
                                '$qty', '$unitRate'
                            ]
                        }, 
                        '_id': 0
                    }
                }
            ]
            invoiceData = cmo.finding_aggregate("invoice",arra)['data']
            if len(invoiceData):
                invoicedf = pd.DataFrame.from_dict(invoiceData)
                result = invoicedf.groupby(['month', 'year', 'projectgroupuid'], as_index=False)['amount'].sum()
                if amountType == "DOLLAR":
                    arra = [
                        {
                            '$project': {
                                '_id': 0
                            }
                        }
                    ]
                    exRatedf = pd.DataFrame.from_dict(cmo.finding_aggregate("exChangeRate",arra)['data'])
                    result = result.merge(exRatedf,on="year",how="left")
                    result[['amount']] = result[['amount']].div(result['rate'], axis=0)
                # pivot_df = result.pivot_table(index=['year', 'projectgroupuid'],columns='month',values='amount',fill_value=0  ).reset_index()
                # pivot_df.columns = [f'amount-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                result[['amount']] = result[['amount']].div(1000000, axis=0).round(2)
                pivot_df = result.pivot_table(index=['projectgroupuid'], columns=['year', 'month'], values='amount', fill_value=0).reset_index()
                pivot_df.columns = [f"amount-{col[1]}-{col[0]}" if isinstance(col[0], int) else col[0] for col in pivot_df.columns]
                for col in acctual_amount:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0
                pivot_df = pivot_df[["projectgroupuid"]+acctual_amount]
                # pivot_df.iloc[:, 1:] = pivot_df.iloc[:, 1:].cumsum()
                if viewType == "Cumulative":
                    del acctual_amount[0]
                    for i in range(1, len(acctual_amount)+1):  
                        pivot_df.iloc[:, i + 1] += pivot_df.iloc[:, i]
                mergedDf = df_pivot.merge(pivot_df,on='projectgroupuid',how='left')
                mergedDf = mergedDf.drop(columns=['uniqueId', 'projectgroupuid'])
                fixed_columns = ['customer', 'costCenter','zone', 'businessUnit']
                pv_amount_columns = sorted([col for col in mergedDf.columns if col not in fixed_columns], key=custom_sort)
                sorted_columns = fixed_columns + pv_amount_columns
                mergedDf = mergedDf[sorted_columns]
                mergedDf = mergedDf.rename(columns=rename_columns)
                fullPath = excelWriteFunc.excelFileWriter(mergedDf, "Export_EVMFinancial", "EVMFinancial")
                return send_file(fullPath)
 
    
@export_blueprint.route("/export/EvmDelivery", methods=["GET", "POST"])
@token_required
def export_evmdelivery(current_user):

    if request.method == "POST":
        allData = request.get_json()
        year = int(allData["year"])
        typeSelectional = allData["typeSelectional"]
        viewBy = allData["viewBy"].split(",")
        projectType = allData['projectType']
        projectId = allData['projectId']
        viwq = []
        for i in viewBy:
            viwq.append(int(i.replace("WK#", "")))
        
        conversion = {
            '1': 'Jan',
            '2': 'Feb',
            '3': 'Mar',
            '4': 'Apr',
            '5': 'May',
            '6': 'Jun',
            '7': 'Jul',
            '8': 'Aug',
            '9': 'Sep',
            '10': 'Oct',
            '11': 'Nov',
            '12': 'Dec'
        }
            
        projection_stage = {
            '_id': 0,
            'Project Type':'$projectType',
            'Project ID':'$projectId',
            "projecttypeuid":1,
            "projectuid":1,
            'year':1,
        }
        if typeSelectional == "Monthly":
            for field in viewBy:
                projection_stage[f'Plan-{conversion[field]}({year})'] = {'$ifNull':[f'$M-{field}',""]}
        else:
            for field in viwq:
                projection_stage[f'Plan-WK#{field:02d}({year})'] = {'$ifNull':[f'$WK#{field:02d}',""]}
        
        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'status': 'Active', 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }
        ]
        if projectId !='':
            arra = arra + [
                {
                    '$match':{
                        '_id':ObjectId(projectId)
                    }
                }
            ]
        arra = arra +[
            {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'projectType', 
                    'foreignField': '_id', 
                    'as': 'result'
                }
            }, {
                '$project': {
                    'projectId': 1, 
                    'projectType': {
                        '$arrayElemAt': [
                            '$result.projectType', 0
                        ]
                    }, 
                    'projectuid': {
                        '$toString': '$_id'
                    }, 
                    'projecttypeuid': {
                        '$toString': '$projectType'
                    }, 
                    '_id': 0
                }
            }]
        if projectType != '':
            arra = arra + [
                {
                    '$match':{
                        'projecttypeuid':projectType
                    }
                }
            ]
        arra = arra + [
            {
                '$lookup': {
                    'from': 'evmActual', 
                    'localField': 'projectuid', 
                    'foreignField': 'projectuid', 
                    'pipeline':[
                        {
                            '$match':{
                                'year':year
                            }
                        }
                    ],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'result.projectId': '$projectId', 
                    'result.projectType': '$projectType', 
                    'result.projectuid': '$projectuid', 
                    'result.projecttypeuid': '$projecttypeuid'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project':projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)['data']
        if len(dataResp):
            dataRespdf = pd.DataFrame.from_dict(dataResp)
            year = int(year)
            if typeSelectional == "Monthly":
                month = [int(x) for x in viewBy]
                add_field = {
                    'month': {
                        '$month': '$CC_Completion Date'
                    }, 
                    'year': {
                        '$year': '$CC_Completion Date'
                    }
                }
                match_stage = {
                    'year': year, 
                    'month': {
                        '$in': month
                    }
                }
                project_stage = {
                    'projectuid': '$projectuniqueId', 
                    'month': 1, 
                    'year': 1, 
                    '_id': 0
                }
            else:
                week = viwq
                add_field = {
                    'week': {
                        '$week': '$CC_Completion Date'
                    }, 
                    'year': {
                        '$year': '$CC_Completion Date'
                    }
                }
                match_stage = {
                    'year': year, 
                    'week': {
                        '$in': week
                    }
                }
                project_stage = {
                    'projectuid': '$projectuniqueId', 
                    'week': 1, 
                    'year': 1, 
                    '_id': 0
                }
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {
                            '$ne': None
                        }
                    }
                }, {
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
                        }
                    }
                }, {
                    '$addFields': add_field
                }, {
                    '$match': match_stage
                }, {
                    '$project': project_stage
                }
            ]
            ms2Data = cmo.finding_aggregate("milestone",arra)['data']
            if len(ms2Data):
                ms2Datadf = pd.DataFrame.from_dict(ms2Data)
                if typeSelectional == "Monthly":
                    result = ms2Datadf.groupby(['month','year','projectuid']).size().reset_index(name='count')
                    pivot_df = result.pivot_table(index=['year','projectuid'],columns='month',values='count',fill_value=0 ).reset_index()
                    pivot_df.columns = [f'count-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                    rename_dict = {
                        'count-1': f'Actual-Jan({year})',
                        'count-2': f'Actual-Feb({year})',
                        'count-3': f'Actual-Mar({year})',
                        'count-4': f'Actual-Apr({year})',
                        'count-5': f'Actual-May({year})',
                        'count-6': f'Actual-Jun({year})',
                        'count-7': f'Actual-Jul({year})',
                        'count-8': f'Actual-Aug({year})',
                        'count-9': f'Actual-Sep({year})',
                        'count-10':f'Actual-Oct({year})',
                        'count-11':f'Actual-Nov({year})',
                        'count-12':f'Actual-Dec({year})',
                    }
                    pivot_df.rename(columns=rename_dict, inplace=True)
                    
                else:
                    result = ms2Datadf.groupby(['week','year','projectuid']).size().reset_index(name='count')
                    pivot_df = result.pivot_table(index=['year','projectuid'],columns='week',values='count',fill_value=0 ).reset_index()
                    pivot_df.columns = [f'Actual-WK#{col:02d}({year})' if isinstance(col, int) else col for col in pivot_df.columns]  
                pivot_df.drop(columns=['year'], inplace=True)
                mergedDf = dataRespdf.merge(pivot_df,on='projectuid',how='left')
                if typeSelectional == "Monthly":
                    plan_column = [f'Plan-{conversion[month]}({year})' for month in viewBy]
                    actual_column = [f'Actual-{conversion[month]}({year})' for month in viewBy]
                    base_columns = ['Project Type', 'Project ID']
                    new_order = base_columns
                    for  plan, acctual in zip(plan_column, actual_column):
                        new_order.extend([plan, acctual])
                else:
                    plan_column = [f'Plan-WK#{field:02d}({year})' for field in viwq]
                    actual_column = [f'Actual-WK#{field:02d}({year})' for field in viwq]
                    base_columns = ['Project Type', 'Project ID']
                    new_order = base_columns
                    for  plan, acctual in zip(plan_column, actual_column):
                        new_order.extend([plan, acctual])
                for col in new_order:
                    if col not in mergedDf.columns:
                        mergedDf[col] = ''
                mergedDf = mergedDf[new_order]
                dataResp = mergedDf
        if "projecttypeuid" in dataResp:
            del dataResp['projecttypeuid']
        if "projectuid" in dataResp:
            del dataResp['projectuid']
        if "year" in dataResp:
            del dataResp['year']
        dataframe = pd.DataFrame(dataResp)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_ActualWorkDone", "ActualWorkDone")
        return send_file(fullPath)





@export_blueprint.route('/export/profit&loss',methods=['GET','POST'])
@token_required
def export_profitandloss(current_user):
     if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        costCenter = costCenter_str(current_user['userUniqueId'])
        for i in costCenter:
            costCenterId.append(ObjectId(i['uniqueId']))
        year = int(allData["year"])
        month = [current_month(),current_month()-1]
        if allData['Month']!="" and allData['Month']!="undefined":
            month = allData["Month"].split(",")
            month = [int(x) for x in month]
            
        if allData['Cost Center'] !='' and allData['Cost Center']!='undefined':
            costCenterId = []
            costCenter = allData['Cost Center'].split(",")
            for i in costCenter:
                costCenterId.append(ObjectId(i))
        
        
        arra = [
            {
                '$match':{
                    'costCenter':{'$in':costCenterId},
                    'month':{'$in':month},
                    'year':year
                }
            },{
                '$lookup': {
                    'from': 'costCenter', 
                    'localField': 'costCenter', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'zone', 
                                'localField': 'zone', 
                                'foreignField': '_id', 
                                'as': 'zoneResult'
                            }
                        }, {
                            '$addFields': {
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResult.shortCode', 0
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'costCenterName': {
                        '$arrayElemAt': [
                            '$result.costCenter', 0
                        ]
                    }, 
                    'zone': {
                        '$arrayElemAt': [
                            '$result.zone', 0
                        ]
                    },  
                    'projectedGrossProfit': {
                        '$subtract': [
                            '$projectedRevenue', '$projectedCost'
                        ]
                    }, 
                    'actualGrossProfit': {
                        '$subtract': [
                            '$actualRevenue', '$actualCost'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'projectedGrossMargin': {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$projectedGrossProfit', '$projectedRevenue'
                                ]
                            }, 100
                        ]
                    }, 
                    'actualGrossMargin': {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$actualGrossProfit', '$actualRevenue'
                                ]
                            }, 100
                        ]
                    }, 
                    'netProfit': {
                        '$subtract': [
                            '$actualGrossProfit', '$sgna'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'projectedGrossMargin': {
                        '$round': [
                            '$projectedGrossMargin',1
                        ]
                    }, 
                    'actualGrossMargin': {
                        '$round': [
                            '$actualGrossMargin',1
                        ]
                    }, 
                    'netMargin': {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$netProfit', '$actualRevenue'
                                ]
                            }, 100
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'netMargin': {
                        '$round': [
                            '$netMargin',1
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'result': 0
                }
            }, {
                '$sort':{
                    'year':-1,
                    'month':-1,
                    'costCenterName':1
                }
            }, {
                '$addFields': {
                    'month': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$month', 1
                                        ]
                                    }, 
                                    'then': 'Jan'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 2
                                        ]
                                    }, 
                                    'then': 'Feb'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 3
                                        ]
                                    }, 
                                    'then': 'Mar'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 4
                                        ]
                                    }, 
                                    'then': 'Apr'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 5
                                        ]
                                    }, 
                                    'then': 'May'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 6
                                        ]
                                    }, 
                                    'then': 'Jun'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 7
                                        ]
                                    }, 
                                    'then': 'Jul'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 8
                                        ]
                                    }, 
                                    'then': 'Aug'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 9
                                        ]
                                    }, 
                                    'then': 'Sep'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 10
                                        ]
                                    }, 
                                    'then': 'Oct'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 11
                                        ]
                                    }, 
                                    'then': 'Nov'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 12
                                        ]
                                    }, 
                                    'then': 'Dec'
                                }
                            ], 
                            'default': None
                        }
                    }
                }
            },{
                '$project':{
                    'Year':'$year',
                    'Month':'$month',
                    'Cost Center':'$costCenterName',
                    'Zone':'$zone',
                    'Projected Revenue':{
                        '$ifNull':['$projectedRevenue',""]
                    },
                    'Actual Revenue':{
                        '$ifNull':['$actualRevenue','']
                    },
                    'Projected Cost':{
                        '$ifNull':['$projectedCost','']
                    },
                    'Actual Cost':{
                        '$ifNull':['$actualCost','']
                    },
                    'Projected Gross Profit':{
                        '$ifNull':['$projectedGrossProfit','']
                    },
                    'Actual Gross Profit':{
                        '$ifNull':['$actualGrossProfit','']
                    },
                    'Projected Gross Margin(%)':{
                        '$ifNull':['$projectedGrossMargin','']
                    },
                    'Actual Gross Margin(%)':{
                        '$ifNull':['$actualGrossMargin','']
                    },
                    'SGNA Cost':{
                        '$ifNull':['$sgna','']
                    },
                    'Net Profit':{
                        '$ifNull':['$netProfit','']
                    },
                    'Net Margin(%)':{
                        '$ifNull':['$netMargin','']
                    },
                }
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)['data']
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Profit_Loss", "Profit_Loss")
        return send_file(fullPath)




@export_blueprint.route('/export/salaryDB',methods=['GET','POST'])
@token_required
def export_salaryDB(current_user):
     if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        # costCenter = costCenter_str(current_user['userUniqueId'])
        # for i in costCenter:
        #     costCenterId.append(ObjectId(i['uniqueId']))
        year = int(allData["year"])
        # month = [current_month(),current_month()-1]
        month=[]
        if allData['Month']!="" and allData['Month']!="undefined":
            month = allData["Month"].split(",")
            month = [int(x) for x in month]
            
        if allData['Cost Center'] !='' and allData['Cost Center']!='undefined':
            costCenterId = []
            costCenter = allData['Cost Center'].split(",")
            for i in costCenter:
                costCenterId.append(ObjectId(i))
        filterArray=[]
        if len(costCenterId)>0 and len(month)>0:
            if year:
                filterArray=[{
                '$match':{
                    'projectGroup':{'$in':costCenterId},
                    'month':{'$in':month},
                    'year':year
                }
            }]
        
        arra = filterArray+[
            {
                '$lookup': {
                    'from': 'costCenter', 
                    'localField': 'costCenter', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'customer', 
                                'localField': 'customer', 
                                'foreignField': '_id', 
                                'pipeline': [
                                    {
                                        '$project': {
                                            'customerName': 1, 
                                            'shortName': 1, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'customerResult'
                            }
                        }, {
                            '$addFields': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customerResult.customerName', 0
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'customerResult': 0
                            }
                        }
                    ], 
                    'as': 'costCenterResults'
                }
            }, {
                '$addFields': {
                    'costCenter': {
                        '$arrayElemAt': [
                            '$costCenterResults.costCenter', 0
                        ]
                    }, 
                    'customer': {
                        '$arrayElemAt': [
                            '$costCenterResults.customer', 0
                        ]
                    }, 
                    'month': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$month', 1
                                        ]
                                    }, 
                                    'then': 'January'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 2
                                        ]
                                    }, 
                                    'then': 'February'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 3
                                        ]
                                    }, 
                                    'then': 'March'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 4
                                        ]
                                    }, 
                                    'then': 'April'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 5
                                        ]
                                    }, 
                                    'then': 'May'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 6
                                        ]
                                    }, 
                                    'then': 'June'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 7
                                        ]
                                    }, 
                                    'then': 'July'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 8
                                        ]
                                    }, 
                                    'then': 'August'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 9
                                        ]
                                    }, 
                                    'then': 'September'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 10
                                        ]
                                    }, 
                                    'then': 'October'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 11
                                        ]
                                    }, 
                                    'then': 'November'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 12
                                        ]
                                    }, 
                                    'then': 'December'
                                }
                            ], 
                            'default': None
                        }
                    }
                }
            }, {
                '$project': {
                    'cost': 1, 
                    'costCenter': 1, 
                    'year': 1, 
                    'month': 1, 
                    'customer': 1, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }, {
                '$project': {
                    'Year': '$year', 
                    'Month': '$month', 
                    'Customer': '$customer', 
                    'Cost Center': '$costCenter', 
                    'Cost': '$cost'
                }
            }
        ]
        response = cmo.finding_aggregate("SalaryDB",arra)['data']
        
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_salaryDB", "Export_salaryDB")
        return send_file(fullPath)




@export_blueprint.route('/export/OtherFixedCost',methods=['GET','POST'])
@token_required
def export_OtherFixedCost(current_user):
     if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        # costCenter = costCenter_str(current_user['userUniqueId'])
        # for i in costCenter:
        #     costCenterId.append(ObjectId(i['uniqueId']))
        year = int(allData["year"])
        # month = [current_month(),current_month()-1]
        month=[]
        if allData['Month']!="" and allData['Month']!="undefined":
            month = allData["Month"].split(",")
            month = [int(x) for x in month]
            
        if allData['Cost Center'] !='' and allData['Cost Center']!='undefined':
            costCenterId = []
            costCenter = allData['Cost Center'].split(",")
            for i in costCenter:
                costCenterId.append(ObjectId(i))
        filterArray=[]
        if len(costCenterId)>0 and len(month)>0:
            if year:
                filterArray=[{
                '$match':{
                    'projectGroup':{'$in':costCenterId},
                    'month':{'$in':month},
                    'year':year
                }
            }]
        
        arra = filterArray+[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'costCenter', 
                        'localField': 'costCenter', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customer', 
                                    'foreignField': '_id', 
                                    'as': 'customerResult'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'zone', 
                                    'localField': 'zone', 
                                    'foreignField': '_id', 
                                    'as': 'zoneResult'
                                }
                            }, {
                                '$project': {
                                    'zone': {
                                        '$arrayElemAt': [
                                            '$zoneResult.zoneName', 0
                                        ]
                                    }, 
                                    'zoneId': {
                                        '$toString': ''
                                    }, 
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$customerResult.customerName', 0
                                        ]
                                    }, 
                                    'costCenter': 1, 
                                    '_id': 0
                                }
                            }
                        ], 
                        'as': 'costCenterResult'
                    }
                }, {
                    '$addFields': {
                        'costCenter': {
                            '$arrayElemAt': [
                                '$costCenterResult.costCenter', 0
                            ]
                        }, 
                        'customer': {
                            '$arrayElemAt': [
                                '$costCenterResult.customer', 0
                            ]
                        }, 
                        'zone': {
                            '$arrayElemAt': [
                                '$costCenterResult.zone', 0
                            ]
                        }, 
                        'month': {
                            '$switch': {
                                'branches': [
                                    {
                                        'case': {
                                            '$eq': [
                                                '$month', 1
                                            ]
                                        }, 
                                        'then': 'January'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 2
                                            ]
                                        }, 
                                        'then': 'February'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 3
                                            ]
                                        }, 
                                        'then': 'March'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 4
                                            ]
                                        }, 
                                        'then': 'April'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 5
                                            ]
                                        }, 
                                        'then': 'May'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 6
                                            ]
                                        }, 
                                        'then': 'June'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 7
                                            ]
                                        }, 
                                        'then': 'July'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 8
                                            ]
                                        }, 
                                        'then': 'August'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 9
                                            ]
                                        }, 
                                        'then': 'September'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 10
                                            ]
                                        }, 
                                        'then': 'October'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 11
                                            ]
                                        }, 
                                        'then': 'November'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$month', 12
                                            ]
                                        }, 
                                        'then': 'December'
                                    }
                                ], 
                                'default': None
                            }
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'OtherCostTypes', 
                        'localField': 'costType', 
                        'foreignField': '_id', 
                        'as': 'costTypeResults'
                    }
                }, {
                    '$addFields': {
                        'costType': {
                            '$arrayElemAt': [
                                '$costTypeResults.costType', 0
                            ]
                        }, 
                        'costTypeId': {
                            '$toString': '$costTypeId'
                        }
                    }
                }, {
                    '$project': {
                        'cost': 1, 
                        'costCenter': 1, 
                        'year': 1, 
                        'month': 1, 
                        'customer': 1, 
                        'zone': 1, 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0, 
                        'costType': 1, 
                        'costTypeId': 1
                    }
                }, {
                    '$project': {
                        'Year': '$year', 
                        'Month': '$month', 
                        'Customer': '$customer', 
                        'Cost Center': '$costCenter', 
                        'Zone': '$zone', 
                        'Cost Type': '$costType', 
                        'Cost': '$cost'
                    }
                }
            ]
        response = cmo.finding_aggregate("OtherFixedCost",arra)['data']
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_OtherFixedCost", "Export_OtherFixedCost")
        return send_file(fullPath)




@export_blueprint.route('/export/vendorCost',methods=['GET','POST'])
@token_required
def export_VendorCost(current_user):
     if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        # costCenter = costCenter_str(current_user['userUniqueId'])
        # for i in costCenter:
        #     costCenterId.append(ObjectId(i['uniqueId']))
        if 'year' in allData:
            year = int(allData["year"])
        else:
            year = None
        # month = [current_month(),current_month()-1]
        month=[]
        if 'Month' in allData:
            if allData['Month']!="" and allData['Month']!="undefined":
                month = allData["Month"].split(",")
                month = [int(x) for x in month]
        
        if 'Cost Center' in allData:  
            if allData['Cost Center'] !='' and allData['Cost Center']!='undefined':
                costCenterId = []
                costCenter = allData['Cost Center'].split(",")
                for i in costCenter:
                    costCenterId.append(ObjectId(i))
        filterArray=[]
        if len(costCenterId)>0 and len(month)>0:
            if year:
                filterArray=[{
                '$match':{
                    'projectGroup':{'$in':costCenterId},
                    'month':{'$in':month},
                    'year':year
                }
            }]
        
        arra = filterArray+[
            {
                '$lookup': {
                    'from': 'projectGroup', 
                    'localField': 'projectGroup', 
                    'foreignField': '_id', 
                    'pipeline': [
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
                                'customer': '$customer.shortName', 
                                'Customer': '$customer.customerName'
                            }
                        }, {
                            '$addFields': {
                                'projectGroupName': {
                                    '$concat': [
                                        '$customer', '-', '$zone', '-', '$costCenter'
                                    ]
                                }, 
                                'projectGroup': {
                                    '$toString': '$_id'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'projectGroupName': 1, 
                                'projectGroup': 1, 
                                'Customer': 1, 
                                'costCenter': 1
                            }
                        }
                    ], 
                    'as': 'projectGroupResult'
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'subProjectId', 
                    'foreignField': '_id', 
                    'as': 'projectTypeResults'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'vendorId', 
                    'foreignField': '_id', 
                    'as': 'vendorResults'
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'customerId': {
                        '$toString': '$customerId'
                    }, 
                    'customer': {
                        '$arrayElemAt': [
                            '$projectGroupResult.Customer', 0
                        ]
                    }, 
                    'projectGroup': {
                        '$toString': '$projectGroup'
                    }, 
                    'projectGroupName': {
                        '$arrayElemAt': [
                            '$projectGroupResult.projectGroupName', 0
                        ]
                    }, 
                    'projectType': {
                        '$arrayElemAt': [
                            '$projectTypeResults.projectType', 0
                        ]
                    }, 
                    'subProject': {
                        '$arrayElemAt': [
                            '$projectTypeResults.subProject', 0
                        ]
                    }, 
                    'subProjectId': {
                        '$toString': '$subProjectId'
                    }, 
                    'vendorName': {
                        '$arrayElemAt': [
                            '$vendorResults.vendorName', 0
                        ]
                    }, 
                    'vendorCode': {
                        '$arrayElemAt': [
                            '$vendorResults.vendorCode', 0
                        ]
                    }, 
                    'vendorId': {
                        '$toString': '$vendorId'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'vendorResults': 0, 
                    'projectGroupResult': 0, 
                    'projectTypeResults': 0
                }
            },
            {
        '$project': {
            'Customer': '$customer', 
            'Project Group': '$projectGroupName', 
            'Project Type': '$projectType', 
            'Sub Project': '$subProject', 
            'Activity Name': '$activityName', 
            'Airtel Item Code': '$airtelItemCode', 
            'Vendor Item Code': '$itemCode', 
            'Item Code Description': '$itemCodeDescription', 
            'GBPA': '$GBPA', 
            'Milestone': '$milestone', 
            'Vendor Name': '$vendorName', 
            'Vendor Code': '$vendorCode', 
            'Rate': '$rate'
        }
    }
            # {
            #     '$set': {
            #         'milestone': {
            #             '$split': [
            #                 '$milestone', ','
            #             ]
            #         }
            #     }
            # }, {
            #     '$unwind': '$milestone'
            # }
        ]
        response = cmo.finding_aggregate("vendorCostMilestone",arra)['data']
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_VendorCost", "Export_VendorCost")
        return send_file(fullPath)




@export_blueprint.route("/export/accrualRevenueTrend",methods=["GET","POST"])
@token_required
def Export_Accrual_Revenue_Trend(current_user):
    
    if request.method == "POST":

        allData = request.get_json()
        viewBy = allData["Monthly"]
        projection_stage = {
            "_id": 0,
            "Customer": "$customer",
            "Cost Center": "$costCenter",
            "UST Project ID": {
                '$ifNull':["$ustProjectid",""]
            }
        }

        month_map = {
            "M-1Y": "Jan",
            "M-2Y": "Feb",
            "M-3Y": "Mar",
            "M-4Y": "Apr",
            "M-5Y": "May",
            "M-6Y": "Jun",
            "M-7Y": "Jul",
            "M-8Y": "Aug",
            "M-9Y": "Sep",
            "M-10Y": "Oct",
            "M-11Y": "Nov",
            "M-12Y": "Dec",
        }
        for item in viewBy:
            month_code, year = (
                item.split("-")[0] + "-" + item.split("-")[1],
                item.split("-")[-1],
            )
            month_name = month_map.get(month_code, "Unknown")
            projection_stage[f"{month_name}({year})"] = {'$ifNull':["$" + item,""]}
        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'status': 'Active', 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        },{
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
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
                                                }, {
                                                    '$addFields': {
                                                        'uniqueId': {
                                                            '$toString': '$_id'
                                                        }
                                                    }
                                                }
                                            ], 
                                            'as': 'costCenter'
                                        }
                                    }, {
                                        '$project': {
                                            'costCenter': {
                                                '$arrayElemAt': [
                                                    '$costCenter.costCenter', 0
                                                ]
                                            }, 
                                            'uniqueId': {
                                                '$arrayElemAt': [
                                                    '$costCenter.uniqueId', 0
                                                ]
                                            }, 
                                            'ustProjectId': {
                                                '$arrayElemAt': [
                                                    '$costCenter.ustProjectId', 0
                                                ]
                                            },
                                            'customer': {
                                                '$arrayElemAt': [
                                                    '$customer.customerName', 0
                                                ]
                                            }, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$project': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$result.customer', 0
                                    ]
                                }, 
                                'costCenter': {
                                    '$arrayElemAt': [
                                        '$result.costCenter', 0
                                    ]
                                }, 
                                'uniqueId': {
                                    '$arrayElemAt': [
                                        '$result.uniqueId', 0
                                    ]
                                }, 
                                'ustProjectId': {
                                    '$arrayElemAt': [
                                        '$result.ustProjectId', 0
                                    ]
                                },
                                '_id': 0
                            }
                        }, {
                            '$group': {
                                '_id': '$uniqueId', 
                                'data': {
                                    '$first': '$$ROOT'
                                }
                            }
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$data'
                            }
                        }, {
                            '$sort': {
                                'uniqueId': 1
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$lookup': {
                    'from': 'accrualRevenueTrend', 
                    'localField': 'uniqueId', 
                    'foreignField': 'costCenteruid', 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'result.costCenter': '$costCenter', 
                    'result.uniqueId': '$uniqueId',
                    'result.ustProjectid': '$ustProjectId',
                    'result.customer':'$customer'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        response = cmo.finding_aggregate("projectAllocation",arra)['data']
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Accrual_Revenue", "Accrual_Revenue_Trend")
        return send_file(fullPath)
    
@export_blueprint.route("/export/sobForms",methods=["GET","POST"])
@token_required
def sobForms(current_user):
    if request.method == "POST":
        allData = request.get_json()
        
        year = allData["year"]
        circle=allData["circle"]
        viewBy = allData["viewBy"]
        viewBy_string = ",".join(viewBy)

        

        arra = [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            },
              {
                '$lookup': {
                    'from': 'sobAction', 
                    'localField': 'uniqueId', 
                    'foreignField': 'uniqueId', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        },
                        {
                        '$match': {
                            'year': (year),
                            'viewBy':(viewBy_string),
                            'circle':(circle),
                            'roleId':current_user['userUniqueId']
                    }
                },
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
            '$addFields': {
                'result.Partner': '$competitor', 
                'result.uniqueId': '$uniqueId', 
                'result.circle': {
                    '$toObjectId': '$result.circle'
                }
            }
        }, {
            '$replaceRoot': {
                'newRoot': '$result'
            }
        }, {
            '$lookup': {
                'from': 'circle', 
                'localField': 'circle', 
                'foreignField': '_id', 
                'as': 'result1'
            }
        }, {
            '$addFields': {
                'Circle': {
                    '$arrayElemAt': [
                        '$result1.circleName', 0
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'uniqueId': 0, 
                'circleuid': 0, 
                'circle': 0, 
                'result1': 0,
                'competitor':0,
                'undefined':0,
                'roleId':0,
                'circleName':0,
                'cirlce':0
            }
        }
        ]
        response = cmo.finding_aggregate("sob",arra)['data']
        month_mapping = {
            '1': "Jan",
            '2': "Feb",
            '3': "Mar",
            '4': "Apr",
            '5': "May",
            '6': "Jun",
            '7': "Jul",
            '8': "Aug",
            '9': "Sep",
            '10': "Oct",
            '11': "Nov",
            '12': "Dec"
        }

        for entry in response:
            if 'viewBy' in entry:
                month = entry['viewBy']
                if month in month_mapping:
                    entry['viewBy'] = month_mapping[month]

        key_mapping = {
            'year': 'Year',
            'viewBy': 'Month', 
        }

        def rename_keys(d, key_mapping):
            return {key_mapping.get(k, k): v for k, v in d.items()}

        new_data = [rename_keys(d, key_mapping) for d in response]
        record = new_data
        reordered_data = [{'Partner': item.pop('Partner'), **item} if 'Partner' in item else item for item in record]
        dataframe = pd.DataFrame(reordered_data)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_sob_form", "sob_forms")
        return send_file(fullPath)


@export_blueprint.route("/export/userProjectAllocation",methods=['GET'])
@token_required
def export_user_project_allocation(Current_user):
    arra = [
        {
            '$match':{
                'type':{'$ne':'Partner'}
                }
        }, {
            "$addFields": {
                "emp": {"$concat": ["$empName", "(", "$empCode", ")"]},
                "uniqueId": {"$toString": "$_id"},
                "_id": {"$toString": "$_id"},
            }
        }, 
        ]
    if (request.args.get("empUniqueId") != None and request.args.get("empUniqueId") != ""):
            arra = arra + [
                {
                    "$match": {
                        "emp": {
                            "$regex": cdc.regexspecialchar(request.args.get("empUniqueId")),
                            "$options": "i",
                        }
                    }
                }
            ]
    if request.args.get("profile") != None and request.args.get("profile") != "":
            profile = ObjectId(request.args.get("profile"))
            arra = arra + [
                {
                    "$match": {
                        "userRole": profile
                    }
                }
            ]
    arra= arra + [        
        {
            '$project': {
                'emp': 1, 
                'userRole': 1, 
                'uniqueId': 1
            }
        }, {
            '$lookup': {
                'from': 'projectAllocation', 
                'localField': '_id', 
                'foreignField': 'empId', 
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
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'projectIds': '$result.projectIds'
            }
        }, {
            '$lookup': {
                'from': 'project', 
                'localField': 'projectIds', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0, 
                            'projectId': 1
                        }
                    }
                ], 
                'as': 'projectIdName'
            }
        }, {
            '$addFields': {
                'projectIdName': '$projectIdName.projectId'
            }
        }, {
            '$addFields': {
                'projectIdName': {
                    '$reduce': {
                        'input': '$projectIdName', 
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
            '$lookup': {
                'from': 'userRole', 
                'localField': 'userRole', 
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
                'as': 'role'
            }
        }, {
            '$unwind': {
                'path': '$role', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'userRole': '$role.roleName'
            }
        }, {
            '$addFields': {
                'projectIds': {
                    '$map': {
                        'input': '$projectIds', 
                        'as': 'objectId', 
                        'in': {
                            '$toString': '$$objectId'
                        }
                    }
                }
            }
        }, {
            '$addFields': {
                'projectIds': {
                    '$reduce': {
                        'input': '$projectIds', 
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
        }, 
        # {
        #         '$project': {
        #             'role': 0, 
        #             '_id': 0, 
        #             'result': 0,
        #             'uniqueId':0,
        #             'projectIds':0
        #         }
        #     }
        {
            '$project': {
                'Employee':{
                    '$ifNull':['$emp',None]
                },
                'Profile':{
                    '$ifNull':['$userRole',None]
                },
                'Project':{
                    '$ifNull':['$projectIdName',None]
                },
                '_id':0
            }
        }
    ]
    if request.args.get("project") != None and request.args.get("project") != "":
            arra = arra + [
                {
                    "$match": {
                        "Project": {
                            "$regex": request.args.get("project").strip(),
                            "$options": "i",
                        }
                    }
                }
            ]
    response = cmo.finding_aggregate("userRegister", arra)
    response = response['data']
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_User_Project_Allocation", "User_Project_Allocation")
    return send_file(fullPath)

@export_blueprint.route("/export/partnerProjectAllocation",methods=['GET'])
@token_required
def export_partner_project_allocation(Current_user):
    arra = [
        {
                '$match': {
                    'type': 'Partner',
                    'deleteStatus':{
                        '$ne':1
                    }
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    'vendorCode':{
                        '$toString': '$vendorCode'
                    }
                    
                }
            }, {
                '$addFields': {
                    'vendor': {
                        '$concat': [
                            '$vendorName', '(', '$vendorCode', ')'
                        ]
                    }
                }
            },
        ] 
    if (request.args.get("vendor") != None and request.args.get("vendor") != ""):
            arra = arra + [
                {
                    "$match": {
                        "vendor": {
                            "$regex": cdc.regexspecialchar(request.args.get("vendor")),
                            "$options": "i",
                        }
                    }
                }
            ]
    arra= arra + [
            {
                '$project': {
                    'vendor': 1, 
                    'uniqueId': 1, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'projectAllocation', 
                    'localField': 'uniqueId', 
                    'foreignField': 'empId', 
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
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'projectIds': '$result.projectIds'
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'projectId': 1
                            }
                        }
                    ], 
                    'as': 'projectIdName'
                }
            }, {
                '$addFields': {
                    'projectIdName': '$projectIdName.projectId'
                }
            }, {
                '$addFields': {
                    'projectIdName': {
                        '$reduce': {
                            'input': '$projectIdName', 
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
                "$addFields": {
                    "project": {
                        "$map": {
                            "input": "$projectIds",
                            "as": "objectId",
                            "in": {"$toString": "$$objectId"},
                        }
                    }
                }
            }, {
                "$addFields": {
                    "project": {
                        "$reduce": {
                            "input": "$project",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                }
            }, {
                '$project': {
                    "_id":0, 
                    "Partner":{
                        '$ifNull':['$vendor',None]
                    },
                    "Project":{
                        '$ifNull':['$projectIdName',None]
                    },
                }
            }
    ]
    if request.args.get("project") != None and request.args.get("project") != "":
            arra = arra + [
                {
                    "$match": {
                        "Project": {
                            "$regex": request.args.get("project"),
                            "$options": "i",
                        }
                    }
                }
            ]
    response = cmo.finding_aggregate("userRegister", arra)
    response = response['data']
    dataframe = pd.DataFrame(response)
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_Partner_Project_Allocation", "Partner_Project_Allocation")
    return send_file(fullPath)









@export_blueprint.route("/export/siteWithTask3/<customeruniqueId>", methods=["GET","POST"])
@token_required
def ExportSiteWithwithmilestoneAll3(current_user,customeruniqueId=None, projecttypeuniqueId=None):
    mappingarra = [
        {
            '$match':{
                'moduleName':"Financial button under Template",
                'roleId':current_user['roleId']
            }
        }
    ]
    mapingResponse = cmo.finding_aggregate("uamManagement",mappingarra)['data']
    accessType = "H"
    if len(mapingResponse):
        accessType = mapingResponse[0]['accessType']
    subProjectIdswithoutObjectId=request.get_json()
    subProjectIdswithoutObjectId=subProjectIdswithoutObjectId['subproject']
    subProjectIds=[]
    dataff=[]
    for i in subProjectIdswithoutObjectId:
        subProjectIds.append(ObjectId(i))
        if len(subProjectIds):
            arrr=[
{
    '$match': {
        '_id': {
            '$in': subProjectIds
        },
        'custId':customeruniqueId
    }
},
{
        '$project': {
            'MileStone': 0
        }
    }
]       
            dataff = cmo.finding_aggregate("projectType", arrr)["data"]
            if len(dataff) < 1:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "The Sub Project Type has been deleted",
                    }
                )
        else:
            return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "The Sub Project Type has been deleted",
                    }
                )
    list_arr = []
    for j in dataff:
        if "t_sengg" in j:
            list_arr.extend(j["t_sengg"])
        if "t_tracking" in j:
            list_arr.extend(j["t_tracking"])
        if "t_issues" in j:
            list_arr.extend(j["t_issues"])
        if accessType == "W":
            if "t_sFinancials" in j:
                list_arr.extend(j["t_sFinancials"])

    data = cmo.finding_aggregate("project", arrr)
    all_ids = [doc["_id"] for doc in data["data"]]
    
    arra = [
        {"$match": {"SubProjectId": {"$in": subProjectIdswithoutObjectId}, "deleteStatus": {"$ne": 1}}},
        {
            "$addFields": {
                "SubProjectId": {"$toObjectId": "$SubProjectId"},
                'Site_Completion Date1': {
                '$cond': {
                    'if': {
                        ' $or': [
                            {
                                '$eq': [
                                    '$Site_Completion Date', None
                                ]
                            }, {
                                '$eq': [
                                    '$Site_Completion Date', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        ' $toDate': '$Site_Completion Date'
                    }
                }
            },
                'siteEndDate1': {
                '$cond': {
                    'if': {
                        ' $or': [
                            {
                                '$eq': [
                                    '$siteEndDate', None
                                ]
                            }, {
                                '$eq': [
                                    '$siteEndDate', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        ' $toDate': '$siteEndDate'
                    }
                }
            },
            }
        },
        {
            "$addFields": {
                "Site_Completion Date": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                        "then": {
                            "$dateToString": {
                                "date": "$Site_Completion Date1",
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "else": "",
                    }
                },
                "siteageing": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                        "then": {
                            "$round": {
                                "$divide": [
                                    {
                                        "$subtract": [
                                            "$siteEndDate1",
                                            "$Site_Completion Date1",
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
            "$lookup": {
                "from": "projectType",
                "localField": "SubProjectId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "projectType": "$result.projectType",
                "subProject": "$result.subProject",
                "SubProjectId": {"$toString": "$SubProjectId"},
                "projectuniqueId": {"$toObjectId": "$projectuniqueId"},
            }
        },
        {
            "$lookup": {
                "from": "project",
                "localField": "projectuniqueId",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {
                        "$addFields": {
                            "PMId": {"$toObjectId": "$PMId"},
                            "circle": {"$toObjectId": "$circle"},
                        }
                    },
                    {
                        "$lookup": {
                            "from": "circle",
                            "localField": "circle",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "circleresult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$circleresult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "PMId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                    }
                                },
                                {
                                    "$addFields": {
                                        "projectGroupId": {
                                            "$concat": [
                                                "$customer",
                                                "-",
                                                "$zone",
                                                "-",
                                                "$costCenter",
                                            ]
                                        },
                                        "zoneId": {"$toString": "$zoneId"},
                                        "customerId": {"$toString": "$customerId"},
                                        "costCenterId": {"$toString": "$costCenterId"},
                                        "uniqueId": {"$toString": "$_id"},
                                    }
                                },
                                {
                                    "$project": {
                                        "_id": 0,
                                        "customerId": 1,
                                        "zoneId": 1,
                                        "costCenterId": 1,
                                        "projectGroupId": 1,
                                        "uniqueId": 1,
                                    }
                                },
                            ],
                            "as": "projectGroup",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$projectGroup",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "PMName": "$PMarray.empName",
                            "Project Group": "$projectGroup.projectGroupId",
                            "circle": "$circleresult.circleName",
                        }
                    },
                    {
                        "$project": {
                            "PMName": 1,
                            "projectId": 1,
                            "Project Group": 1,
                            "circle": 1,
                            "_id": 0,
                        }
                    },
                ],
                "as": "projectArray",
            }
        },
        {"$unwind": {"path": "$projectArray", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"_id": {"$toString": "$_id"}}}
    ]
    if accessType == "W":
        arra = arra +[{
                "$lookup": {
                    "from": "invoice",
                    "localField": "_id",
                    "foreignField": "siteId",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$addFields": {
                                "Invoiced Quantity": {
                                    "$cond": {
                                        "if": {"$eq": ["$qty", ""]},
                                        "then": 0,
                                        "else": "$qty",
                                    }
                                },
                                "Unit Rate": {
                                    "$cond": {
                                        "if": {"$eq": ["$unitRate", ""]},
                                        "then": 0,
                                        "else": "$unitRate",
                                    }
                                },
                                "WCC SignOff Date": {
                                    "$dateToString": {
                                        "date": {"$toDate": "$wccSignOffdate"},
                                        "format": "%d-%m-%Y",
                                        "timezone": "Asia/Kolkata",
                                    }
                                },
                                "Invoice Date": {
                                    "$dateToString": {
                                        "date": {"$toDate": "$invoiceDate"},
                                        "format": "%d-%m-%Y",
                                        "timezone": "Asia/Kolkata",
                                    }
                                },
                                "Qtyy": {"$toString": "$qty"},
                                "unitrates": {"$toString": "$unitRate"},
                                "Amounts": {
                                    "$toString": {
                                        "$cond": [
                                            {
                                                "$or": [
                                                    {"$eq": ["$qty", ""]},
                                                    {"$eq": ["$unitRate", ""]},
                                                ]
                                            },
                                            "",
                                            {"$multiply": ["$qty", "$unitRate"]},
                                        ]
                                    }
                                },
                                "Amount": {
                                    "$cond": [
                                        {
                                            "$or": [
                                                {"$eq": ["$qty", ""]},
                                                {"$eq": ["$unitRate", ""]},
                                            ]
                                        },
                                        "",
                                        {"$multiply": ["$qty", "$unitRate"]},
                                    ]
                                },
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "customer": 0,
                                "projectGroup": 0,
                                "projectId": 0,
                                "projectType": 0,
                                "subProject": 0,
                                "systemId": 0,
                                "siteId": 0,
                                "wccSignOffdate": 0,
                                "invoiceDate": 0,
                            }
                        },
                    ],
                    "as": "invoiceResult",
                }
            },
            {
                "$addFields": {
                    "WCC No": {
                        "$reduce": {
                            "input": "$invoiceResult.wccNumber",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Amount": {
                        "$reduce": {
                            "input": "$invoiceResult.Amount",
                            "initialValue": 0,
                            "in": {"$add": ["$$value", "$$this"]},
                        }
                    },
                    "PO Number": {
                        "$reduce": {
                            "input": "$invoiceResult.poNumber",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Item Code": {
                        "$reduce": {
                            "input": "$invoiceResult.itemCode",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Invoiced Quantity": {
                        "$reduce": {
                            "input": "$invoiceResult.Qtyy",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Invoice Number": {
                        "$reduce": {
                            "input": "$invoiceResult.invoiceNumber",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "WCC SignOff Date": {
                        "$reduce": {
                            "input": "$invoiceResult.WCC SignOff Date",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Invoice Date": {
                        "$reduce": {
                            "input": "$invoiceResult.Invoice Date",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                }
            }]
    arra = arra +[{
            "$addFields": {
                "PMName": "$projectArray.PMName",
                "siteId": "$siteid",
                "Project Group": "$projectArray.Project Group",
                "Project Id": "$projectArray.projectId",
                "projectuniqueId": {"$toString": "$projectuniqueId"},
                "circle": "$projectArray.circle",
                "uniqueId": "$_id",
                "siteStartDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$siteStartDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "siteEndDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$siteEndDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "rfidate": {
                    "$dateToString": {
                        "date": {"$toDate": "$rfidate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
            }
        },
        {
            "$project": {
                "projectArray": 0,
                "assignerId": 0,
                "result": 0,
                "Site_Completion Date1": 0,
                "siteEndDate1": 0,
            }
        },
        {"$addFields": {"data": "$$ROOT"}},
        {"$project": {"data": 1}},
        {"$addFields": {"_id": {"$toObjectId": "$_id"}}},
        {
            "$lookup": {
                "from": "milestone",
                "localField": "_id",
                "foreignField": "siteId",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {
                        "$addFields": {
                            "_id": {"$toString": "$_id"},
                            "siteId": {"$toString": "$siteId"},
                            "uniqueId": {"$toString": "$_id"},
                            "mileStoneStartDate1": {"$toDate": "$mileStoneStartDate"},
                            "mileStoneEndtDate1": {"$toDate": "$mileStoneEndDate"},
                            "CC_Completion Date1": {"$toDate": "$CC_Completion Date"},
                        }
                    },
                    {
                        "$addFields": {
                            "CC_Completion Date": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$type": "$CC_Completion Date1"},
                                            "date",
                                        ]
                                    },
                                    "then": {
                                        "$dateToString": {
                                            "date": "$CC_Completion Date1",
                                            "format": "%d-%m-%Y",
                                            "timezone": "Asia/Kolkata",
                                        }
                                    },
                                    "else": None,
                                }
                            },
                            "taskageing": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$type": "$CC_Completion Date1"},
                                            "date",
                                        ]
                                    },
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
                                                {
                                                    "$subtract": [
                                                        "$mileStoneEndtDate1",
                                                        "$$NOW",
                                                    ]
                                                },
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
                                    "date": {"$toDate": "$CC_Completion Date"},
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
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {
                                    "$project": {
                                        "_id": 0,
                                        "assignerName": "$empName",
                                        "assignerId": {"$toString": "$_id"},
                                    }
                                },
                            ],
                            "as": "assignerResult",
                        }
                    },
                    {
                        "$project": {
                            "assignerId": 0,
                            "mileStoneStartDate1": 0,
                            "mileStoneEndtDate1": 0,
                            "CC_Completion Date1": 0,
                            "siteId": 0,
                            "isLast": 0,
                            "uniqueId": 0,
                        }
                    },
                ],
                "as": "milestoneArray",
            }
        },
        {"$unwind": {"path": "$milestoneArray", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"finalData": {"$mergeObjects": ["$data", "$milestoneArray"]}}},
        {"$replaceRoot": {"newRoot": "$finalData"}},
        {
            "$addFields": {
                "assigners": {
                    "$reduce": {
                        "input": "$assignerResult",
                        "initialValue": "",
                        "in": {
                            "$concat": [
                                "$$value",
                                {
                                    "$cond": [
                                        {"$eq": ["$$value", ""]},
                                        "$$this.assignerName",
                                        {"$concat": [", ", "$$this.assignerName"]},
                                    ]
                                },
                            ]
                        },
                    }
                },
                "Status": "$siteBillingStatus",
            }
        },
        {
            "$project": {
                "_id": 0,
                "SubProjectId": 0,
                "new_u_id": 0,
                "projectuniqueId": 0,
                "uniqueId": 0,
                "assignerResult": 0,
                "CC_MO No": 0,
                "deleated_at": 0,
                "siteStatus": 0,
                "invoiceResult": 0,
            }
        },
        {
            "$addFields": {
                "RFAI Date": {
                    "$dateToString": {
                        "date": {"$toDate": "$RFAI Date"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                # "MS Completition Date": "$mileStoneEndDate",
                "MS Completition Date": "$CC_Completion Date",
                "System ID": "$systemId",
                "Due Date": {
                    "$let": {
                        "vars": {
                            "date": {
                                "$dateFromString": {
                                    "dateString": "$mileStoneStartDate",
                                    "format": "%d-%m-%Y",
                                }
                            },
                            "daysToAdd": "$taskageing",
                        },
                        "in": {
                            "$dateToString": {
                                "date": {
                                    "$add": [
                                        "$$date",
                                        {"$multiply": ["$$daysToAdd", 86400000]},
                                    ]
                                },
                                "format": "%d-%m-%Y",
                            }
                        },
                    }
                },
                "Task Ageing": "$taskageing",
                "Start Date": "$mileStoneStartDate",
                "Task Closure": {
                    "$cond": {
                        "if": {"$eq": ["$Task Closure", None]},
                        "then": "",
                        "else": {
                            "$dateToString": {
                                "date": {"$toDate": "$Task Closure"},
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                    }
                },
                "Custom Status": "$mileStoneStatus",
                "Task Owner": "$PMName",
                "Task Name": "$Name",
                "Unique ID": "$Unique ID",
                "Site ID": "$Site Id",
                "Circle": "$circle",
                "Sub Project": "$subProject",
                "Project Type": "$projectType",
                "Project ID": "$Project Id",
                "Status": "$siteBillingStatus",
                "Site Allocation Date": {
                    "$cond": {
                        "if": {"$eq": ["$Site Allocation Date", None]},
                        "then": "$RFAI Date",
                        "else": "$Site Allocation Date",
                    }
                },
            }
        },
        {
            "$addFields": {
                "Delay": {
                    "$divide": [
                        {
                            "$subtract": [
                                {
                                    "$dateFromString": {
                                        "dateString": "$MS Completition Date",
                                        "format": "%d-%m-%Y",
                                    }
                                },
                                {
                                    "$dateFromString": {
                                        "dateString": "$Due Date",
                                        "format": "%d-%m-%Y",
                                    }
                                },
                            ]
                        },
                        86400000,
                    ]
                }
            }
        },
        {
            "$project": {
                "Project Id": 0,
                "projectType": 0,
                "subProject": 0,
                "circle": 0,
                "Site Id": 0,
                "uniqueid": 0,
                "Name": 0,
                "PMName": 0,
                "rfaidate": 0,
                "rfidate": 0,
                "assignerResult": 0,
                "nominal_aop": 0,
                "mileStoneStatus": 0,
                "systemId": 0,
                "mileStoneEndDate": 0,
                "mileStoneStartDate": 0,
                "taskageing": 0,
                "siteageing": 0,
                "siteEndDate": 0,
                "siteStartDate": 0,
                "CC_Completion Date": 0,
                "Site_Completion Date": 0,
                "Completion Criteria": 0,
                "Estimated Time (Days)": 0,
                "Predecessor": 0,
                "WCC Sign off": 0,
                "estimateDay": 0,
                "indexing": 0,
                "assigners": 0,
                "Unit Rate": 0,
                "customerId": 0,
                "projectGroupid": 0,
            }
        },
    ]
    response = cmo.finding_aggregate("SiteEngineer", arra)["data"]
    dataframe = pd.DataFrame(response)
    for column in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(dataframe[column]):
            if dataframe[column].dt.tz is not None:
                dataframe[column] = dataframe[column].dt.tz_localize(None)
    ll = pd.DataFrame(list_arr)
    sortCol = [
        "Project Group",
        "Project ID",
        "Project Type",
        "Sub Project",
        "Circle",
        "Site ID",
        "Unique ID",
        "System ID",
        "Task Name",
        "Task Owner",
        "Start Date",
        "Due Date",
        "MS Completition Date",
        "Task Closure",
        "Delay",
        "Task Ageing",
        "Custom Status",
    ]
    ll = ll[~ll.isin(sortCol).any(axis=1)]
    kkkk = []
    if 'fieldName' in ll.columns:
        kk = ll["fieldName"].to_list()
        if len(kk):
            for i in kk:
                if i not in kkkk and i not in sortCol:
                    kkkk.append(i)
    jsjsj = sortCol + kkkk
    if "Unit Rate" in jsjsj:
        jsjsj.remove("Unit Rate")
    if "Site Id" in jsjsj:
        jsjsj.remove("Site Id")
    if "Customer" in jsjsj:
        jsjsj.remove("Customer")
    respdf = pd.DataFrame(response)
    if len(respdf)<1:
        return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "No Task Found In this Sub Project !",
                    }
                )
    
    if accessType == "W":
        columnsClear = [
            "WCC No",
            "WCC SignOff Date",
            "PO Number",
            "Item Code",
            "Invoiced Quantity",
            "Invoice Number",
            "Invoice Date",
            "Amount",
        ]
        for col in columnsClear:
            if col in jsjsj and col in columnsClear:
                respdf[col] = np.where(respdf["Task Name"] != "Revenue Recognition", None, respdf[col])  
    for col in respdf.columns:
        respdf[col] = respdf[col].apply(convertToDateBulkExport)
    respdf = respdf.reindex(columns=jsjsj, fill_value="")
    fullPath = excelWriteFunc.excelFileWriter(respdf, "Export_All_With_Task", "All Project")
    return send_file(fullPath)




@export_blueprint.route("/export/siteWithOutTaskUser",methods=["GET"])
@token_required
def ExportSiteWithoutmilestoneAllUser(current_user):
    if request.method == 'GET':
        mappingarra = [
            {
                '$match':{
                    'moduleName':"Financial button under Template",
                    'roleId':current_user['roleId']
                }
            }
        ]
        mapingResponse = cmo.finding_aggregate("uamManagement",mappingarra)['data']
        accessType = "H"
        if len(mapingResponse):
            accessType = mapingResponse[0]['accessType']
        subProjectIdswithoutObjectId=[]
        arr=[
        {
            '$match': {
                'assignerId': {
                    '$in': [
                        ObjectId(current_user['userUniqueId'])
                    ]
                }, 
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'SubProjectId': 1, 
                '_id': 0
            }
        }, {
            '$group': {
                '_id': '$SubProjectId'
            }
        }
    ]
        data=cmo.finding_aggregate("milestone",arr)
        if len(data['data']):
            for i in data['data']:
                subProjectIdswithoutObjectId.append(i['_id'])
        else:
            return respond(
                    {
                        "status": 400,
                        "msg": "This No Task Assigned!",
                        "icon": "error",
                    }
                )
            
                
        subProjectIdswithoutObjectId=subProjectIdswithoutObjectId
        subProjectIds=[]
        for i in subProjectIdswithoutObjectId:
            subProjectIds.append(ObjectId(i))
            if len(subProjectIds):
                arrr=[
    {
        '$match': {
            '_id': {
                '$in': subProjectIds
            },
            
        }
    }
    ]       
                dataff = cmo.finding_aggregate("projectType", arrr)["data"]
                if len(dataff) < 1:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "The Sub Project Type has been deleted",
                        }
                    )
            else:
                return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": "The Sub Project Type has been deleted",
                }
            )
        
        list_arr = []
        for j in dataff:
            if "t_sengg" in j:
                list_arr.extend(j["t_sengg"])
            if "t_tracking" in j:
                list_arr.extend(j["t_tracking"])
            if "t_issues" in j:
                list_arr.extend(j["t_issues"])
            if accessType == "W":
                if "t_sFinancials" in j:
                    list_arr.extend(j["t_sFinancials"])
                    
        siteIds=[]            
        trrr=[
    {
        '$match': {
            'assignerId': {
                '$in': [
                    ObjectId(current_user['userUniqueId'])
                ]
            }, 
            'deleteStatus': {
                '$ne': 1
            }
        }
    }, {
        '$group': {
            '_id': '$siteId'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            }
        }
    }
]
        
        Response=cmo.finding_aggregate("milestone",trrr)
        
        if len(Response['data']):
            data=Response['data']
            for i in data:
                if is_valid_mongodb_objectid(i['_id']):
                    i['_id']=ObjectId(i['_id'])
                    siteIds.append(i['_id'])
                
        pp=[{"$match": {"SubProjectId": {"$in": subProjectIdswithoutObjectId},"_id":{"$in": siteIds}, "deleteStatus": {"$ne": 1}}}] 
        arr = pp+[
            
            {
                "$addFields": {
                    "SubProjectId": {"$toObjectId": "$SubProjectId"},
                    'Site_Completion Date1': {
                    '$cond': {
                        'if': {
                            ' $or': [
                                {
                                    '$eq': [
                                        '$Site_Completion Date', None
                                    ]
                                }, {
                                    '$eq': [
                                        '$Site_Completion Date', ''
                                    ]
                                }
                            ]
                        }, 
                        'then': None, 
                        'else': {
                            ' $toDate': '$Site_Completion Date'
                        }
                    }
                },
                    'siteEndDate1': {
                    '$cond': {
                        'if': {
                            ' $or': [
                                {
                                    '$eq': [
                                        '$siteEndDate', None
                                    ]
                                }, {
                                    '$eq': [
                                        '$siteEndDate', ''
                                    ]
                                }
                            ]
                        }, 
                        'then': None, 
                        'else': {
                            ' $toDate': '$siteEndDate'
                        }
                    }
                },
                }
            },
            {
                "$addFields": {
                    "Site_Completion Date": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                            "then": {
                                "$dateToString": {
                                    "date": "$Site_Completion Date1",
                                    "format": "%d-%m-%Y",
                                    "timezone": "Asia/Kolkata",
                                }
                            },
                            "else": "",
                        }
                    },
                    "siteageing": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                            "then": {
                                "$round": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$siteEndDate1",
                                                "$Site_Completion Date1",
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
                "$lookup": {
                    "from": "projectType",
                    "localField": "SubProjectId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$addFields": {"custId": {"$toObjectId": "$custId"}}},
                        {
                            "$lookup": {
                                "from": "customer",
                                "localField": "custId",
                                "foreignField": "_id",
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                                "as": "customer",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$customer",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {"$addFields": {"custId": {"$toString": "$custId"}}},
                    ],
                    "as": "result",
                }
            },
            {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "projectType": "$result.projectType",
                    "subProject": "$result.subProject",
                    "SubProjectId": {"$toString": "$SubProjectId"},
                    "projectuniqueId": {"$toObjectId": "$projectuniqueId"},
                    "customerName": "$result.customer.customerName",
                }
            },
            {
                "$lookup": {
                    "from": "project",
                    "localField": "projectuniqueId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$addFields": {
                                "PMId": {"$toObjectId": "$PMId"},
                                "circle": {"$toObjectId": "$circle"},
                            }
                        },
                        {
                            "$lookup": {
                                "from": "userRegister",
                                "localField": "PMId",
                                "foreignField": "_id",
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                                "as": "PMarray",
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
                                                {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                                {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                                {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "projectGroupId": {
                                                "$concat": [
                                                    "$customer",
                                                    "-",
                                                    "$zone",
                                                    "-",
                                                    "$costCenter",
                                                ]
                                            },
                                            "zoneId": {"$toString": "$zoneId"},
                                            "customerId": {"$toString": "$customerId"},
                                            "costCenterId": {"$toString": "$costCenterId"},
                                            "uniqueId": {"$toString": "$_id"},
                                        }
                                    },
                                    {
                                        "$project": {
                                            "_id": 0,
                                            "customerId": 1,
                                            "zoneId": 1,
                                            "costCenterId": 1,
                                            "projectGroupId": 1,
                                            "uniqueId": 1,
                                        }
                                    },
                                ],
                                "as": "projectGroup",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$projectGroup",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$lookup": {
                                "from": "circle",
                                "localField": "circle",
                                "foreignField": "_id",
                                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                                "as": "circle",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$circle",
                                "preserveNullAndEmptyArrays": True,
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
                                "projectGroup": "$projectGroup.projectGroupId",
                            }
                        },
                        {
                            "$project": {
                                "PMName": 1,
                                "projectId": 1,
                                "projectGroup": 1,
                                "_id": 0,
                                "circle": "$circle.circleCode",
                            }
                        },
                    ],
                    "as": "projectArray",
                }
            },
            {"$unwind": {"path": "$projectArray", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "PMName": "$projectArray.PMName",
                    "siteId": "$siteid",
                    "Project Id": "$projectArray.projectId",
                    "circle": "$projectArray.circle",
                    "Project Group": "$projectArray.projectGroup",
                    "projectuniqueId": {"$toString": "$projectuniqueId"},
                    "uniqueId": "$_id",
                    "siteStartDate": {
                        "$dateToString": {
                            "date": {"$toDate": "$siteStartDate"},
                            "format": "%d-%m-%Y",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "siteEndDate": {
                        "$dateToString": {
                            "date": {"$toDate": "$siteEndDate"},
                            "format": "%d-%m-%Y",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "rfidate": {
                        "$dateToString": {
                            "date": {"$toDate": "$rfidate"},
                            "format": "%d-%m-%Y",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "siteallocation date": {
                        "$dateToString": {
                            "date": {"$toDate": "$siteallocation date"},
                            "format": "%d-%m-%Y",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                }
            },
            {"$addFields": {"_id": {"$toString": "$_id"}}}
        ]
        
        if accessType == "W":
            arr = arr + [
                {
                    "$lookup": {
                        "from": "invoice",
                        "localField": "_id",
                        "foreignField": "siteId",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {
                                "$addFields": {
                                    "Invoiced Quantity": {
                                        "$cond": {
                                            "if": {"$eq": ["$qty", ""]},
                                            "then": 0,
                                            "else": "$qty",
                                        }
                                    },
                                    "Unit Rate": {
                                        "$cond": {
                                            "if": {"$eq": ["$unitRate", ""]},
                                            "then": 0,
                                            "else": "$unitRate",
                                        }
                                    },
                                    "WCC SignOff Date": {
                                        "$dateToString": {
                                            "date": {"$toDate": "$wccSignOffdate"},
                                            "format": "%d-%m-%Y",
                                            "timezone": "Asia/Kolkata",
                                        }
                                    },
                                    "Invoice Date": {
                                        "$dateToString": {
                                            "date": {"$toDate": "$invoiceDate"},
                                            "format": "%d-%m-%Y",
                                            "timezone": "Asia/Kolkata",
                                        }
                                    },
                                    "Qtyy": {"$toString": "$qty"},
                                    "unitrates": {"$toString": "$unitRate"},
                                    "Amounts": {
                                        "$toString": {
                                            "$cond": [
                                                {
                                                    "$or": [
                                                        {"$eq": ["$qty", ""]},
                                                        {"$eq": ["$unitRate", ""]},
                                                    ]
                                                },
                                                "",
                                                {"$multiply": ["$qty", "$unitRate"]},
                                            ]
                                        }
                                    },
                                    "Amount": {
                                        "$cond": [
                                            {
                                                "$or": [
                                                    {"$eq": ["$qty", ""]},
                                                    {"$eq": ["$unitRate", ""]},
                                                ]
                                            },
                                            "",
                                            {"$multiply": ["$qty", "$unitRate"]},
                                        ]
                                    },
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "customer": 0,
                                    "projectGroup": 0,
                                    "projectId": 0,
                                    "projectType": 0,
                                    "subProject": 0,
                                    "systemId": 0,
                                    "siteId": 0,
                                    "wccSignOffdate": 0,
                                    "invoiceDate": 0,
                                }
                            },
                        ],
                        "as": "invoiceResult",
                    }
                },
                {
                    "$addFields": {
                        "WCC No": {
                            "$reduce": {
                                "input": "$invoiceResult.wccNumber",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                        "Amount": {
                            "$reduce": {
                                "input": "$invoiceResult.Amount",
                                "initialValue": 0,
                                "in": {"$add": ["$$value", "$$this"]},
                            }
                        },
                        "PO Number": {
                            "$reduce": {
                                "input": "$invoiceResult.poNumber",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                        "Item Code": {
                            "$reduce": {
                                "input": "$invoiceResult.itemCode",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                        "Invoiced Quantity": {
                            "$reduce": {
                                "input": "$invoiceResult.Qtyy",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                        "Invoice Number": {
                            "$reduce": {
                                "input": "$invoiceResult.invoiceNumber",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                        "WCC SignOff Date": {
                            "$reduce": {
                                "input": "$invoiceResult.WCC SignOff Date",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                        "Invoice Date": {
                            "$reduce": {
                                "input": "$invoiceResult.Invoice Date",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                        "$$this",
                                    ]
                                },
                            }
                        },
                    }
                }]
        arr = arr + [{
                "$project": {
                    "projectArray": 0,
                    "assignerId": 0,
                    "result": 0,
                    "Site_Completion Date1": 0,
                    "siteEndDate1": 0,
                    "invoiceResult": 0,
                }
            },
            {"$addFields": {"data": "$$ROOT"}},
            {"$project": {"data": 1}},
            {"$addFields": {"finalData": {"$mergeObjects": ["$data"]}}},
            {"$replaceRoot": {"newRoot": "$finalData"}},
            {
                "$project": {
                    "_id": 0,
                    "SubProjectId": 0,
                    "new_u_id": 0,
                    "projectuniqueId": 0,
                    "uniqueId": 0,
                    "assignerResult": 0,
                    "CC_MO No": 0,
                    "deleated_at": 0,
                }
            },
            {
                "$addFields": {
                    "RFAI Date": {
                        "$dateToString": {
                            "date": {"$toDate": "$RFAI Date"},
                            "format": "%d-%m-%Y",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "Status": "$siteBillingStatus",
                    "Customer": "$customerName",
                    "System ID": "$systemId",
                    "Site ID": "$Site Id",
                    # "Nominal AOP": "$nominal_aop",
                    "Circle": "$circle",
                    "Sub Project": "$subProject",
                    "Project Type": "$projectType",
                    "Project ID": "$Project Id",
                }
            },
            {
                "$project": {
                    "Project Id": 0,
                    "projectType": 0,
                    "subProject": 0,
                    "circle": 0,
                    "Site Id": 0,
                    "uniqueid": 0,
                    "Name": 0,
                    "rfaidate": 0,
                    "rfidate": 0,
                    "nominal_aop": 0,
                    "systemId": 0,
                    "Nominal AOP": 0,
                    "customerName": 0,
                    "siteStartDate": 0,
                    "siteEndDate": 0,
                    "siteBillingStatus": 0,
                    "Site_Completion Date": 0,
                    "siteStatus": 0,
                    "siteageing": 0,
                    "siteallocation date": 0,
                    "Unit Rate": 0,
                    "customerId": 0,
                    "projectGroupid": 0,
                }
            },
        ]
        response = cmo.finding_aggregate("SiteEngineer", arr)
        response = response["data"]
        respdf = pd.DataFrame(response)

        for column in respdf.columns:
            if pd.api.types.is_datetime64_any_dtype(respdf[column]):
                if respdf[column].dt.tz is not None:
                    respdf[column] = respdf[column].dt.tz_localize(None)
        ll = pd.DataFrame(list_arr)
        
        sortCol = [
            "Customer",
            "Project Group",
            "Project ID",
            "Project Type",
            "Sub Project",
            "PMName",
            "Circle",
            "Site ID",
            "Unique ID",
            "System ID",
            "RFAI Date",
        ]
        ll = ll[~ll.isin(sortCol).any(axis=1)]
        # print('cvbnmhgfghjk',ll)
        kkkk = []
        if 'fieldName' in ll.columns:
            kk = ll["fieldName"].to_list()
            if len(kk):
                for i in kk:
                    if i not in kkkk and i not in sortCol:
                        kkkk.append(i)

        jsjsj = sortCol + kkkk

        if "Unit Rate" in jsjsj:
            jsjsj.remove("Unit Rate")
        if "Site Id" in jsjsj:
            jsjsj.remove("Site Id")

        respdf = respdf.reindex(columns=jsjsj, fill_value="")
        for col in respdf.columns:
            respdf[col] = respdf[col].apply(convertToDateBulkExport)
        fullPath = excelWriteFunc.excelFileWriter(
            respdf, "Export_All_With_Project", "All Project"
        )
        return send_file(fullPath)





@export_blueprint.route("/export/siteWithTaskUser", methods=["GET","POST"])
def ExportSiteWithwithmilestoneAllUser():
    current_user={}
    current_user['roleId']="65d6471d0ef320c07729d844"
    current_user['userUniqueId']="65d84c4caf0c03c070f4da9d"
    mappingarra = [
        {
            '$match':{
                'moduleName':"Financial button under Template",
                'roleId':current_user['roleId']
            }
        }
    ]
    mapingResponse = cmo.finding_aggregate("uamManagement",mappingarra)['data']
    accessType = "H"
    if len(mapingResponse):
        accessType = mapingResponse[0]['accessType']
        
    subProjectIdswithoutObjectId=[]
    arr=[
        {
            '$match': {
                'assignerId': {
                    '$in': [
                        ObjectId(current_user['userUniqueId'])
                    ]
                }, 
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'SubProjectId': 1, 
                '_id': 0
            }
        }, {
            '$group': {
                '_id': '$SubProjectId'
            }
        }
    ]
    data=cmo.finding_aggregate("milestone",arr)
    if len(data['data']):
        for i in data['data']:
            subProjectIdswithoutObjectId.append(i['_id'])
    else:
        return respond(
                {
                    "status": 400,
                    "msg": "This No Task Assigned!",
                    "icon": "error",
                }
            )
    subProjectIds=[]
    for i in subProjectIdswithoutObjectId:
        subProjectIds.append(ObjectId(i))
        if len(subProjectIds):
            arrr=[
{
    '$match': {
        '_id': {
            '$in': subProjectIds
        },
    }
}
]       
            dataff = cmo.finding_aggregate("projectType", arrr)["data"]
            if len(dataff) < 1:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "The Sub Project Type has been deleted",
                    }
                )
        else:
            return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "The Sub Project Type has been deleted",
                    }
                )
    list_arr = []
    for j in dataff:
        if "t_sengg" in j:
            list_arr.extend(j["t_sengg"])
        if "t_tracking" in j:
            list_arr.extend(j["t_tracking"])
        if "t_issues" in j:
            list_arr.extend(j["t_issues"])
        if accessType == "W":
            if "t_sFinancials" in j:
                list_arr.extend(j["t_sFinancials"])

    data = cmo.finding_aggregate("project", arrr)
    all_ids = [doc["_id"] for doc in data["data"]]
    siteIds=[]            
    trrr=[
{
    '$match': {
        'assignerId': {
            '$in': [
                ObjectId(current_user['userUniqueId'])
            ]
        }, 
        'deleteStatus': {
            '$ne': 1
        }
    }
}, {
    '$group': {
        '_id': '$siteId'
    }
}, {
    '$addFields': {
        '_id': {
            '$toString': '$_id'
        }
    }
}
]
    Response=cmo.finding_aggregate("milestone",trrr)
    if len(Response['data']):
        data=Response['data']
        for i in data:
            if is_valid_mongodb_objectid(i['_id']):
                i['_id']=ObjectId(i['_id'])
                siteIds.append(i['_id'])
            
    pp=[{"$match": {"SubProjectId": {"$in": subProjectIdswithoutObjectId},"_id":{"$in": siteIds}, "deleteStatus": {"$ne": 1}}}] 
    arra = pp+[ 
        {
            "$addFields": {
                "SubProjectId": {"$toObjectId": "$SubProjectId"},
                'Site_Completion Date1': {
                '$cond': {
                    'if': {
                        ' $or': [
                            {
                                '$eq': [
                                    '$Site_Completion Date', None
                                ]
                            }, {
                                '$eq': [
                                    '$Site_Completion Date', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        ' $toDate': '$Site_Completion Date'
                    }
                }
            },
                'siteEndDate1': {
                '$cond': {
                    'if': {
                        ' $or': [
                            {
                                '$eq': [
                                    '$siteEndDate', None
                                ]
                            }, {
                                '$eq': [
                                    '$siteEndDate', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        ' $toDate': '$siteEndDate'
                    }
                }
            },
            }
        },
        {
            "$addFields": {
                "Site_Completion Date": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                        "then": {
                            "$dateToString": {
                                "date": "$Site_Completion Date1",
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "else": "",
                    }
                },
                "siteageing": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$Site_Completion Date1"}, "date"]},
                        "then": {
                            "$round": {
                                "$divide": [
                                    {
                                        "$subtract": [
                                            "$siteEndDate1",
                                            "$Site_Completion Date1",
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
            "$lookup": {
                "from": "projectType",
                "localField": "SubProjectId",
                "foreignField": "_id",
                "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "projectType": "$result.projectType",
                "subProject": "$result.subProject",
                "SubProjectId": {"$toString": "$SubProjectId"},
                "projectuniqueId": {"$toObjectId": "$projectuniqueId"},
            }
        },
        {
            "$lookup": {
                "from": "project",
                "localField": "projectuniqueId",
                "foreignField": "_id",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1}}},
                    {
                        "$addFields": {
                            "PMId": {"$toObjectId": "$PMId"},
                            "circle": {"$toObjectId": "$circle"},
                        }
                    },
                    {
                        "$lookup": {
                            "from": "circle",
                            "localField": "circle",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "circleresult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$circleresult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "PMId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
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
                                    }
                                },
                                {
                                    "$addFields": {
                                        "projectGroupId": {
                                            "$concat": [
                                                "$customer",
                                                "-",
                                                "$zone",
                                                "-",
                                                "$costCenter",
                                            ]
                                        },
                                        "zoneId": {"$toString": "$zoneId"},
                                        "customerId": {"$toString": "$customerId"},
                                        "costCenterId": {"$toString": "$costCenterId"},
                                        "uniqueId": {"$toString": "$_id"},
                                    }
                                },
                                {
                                    "$project": {
                                        "_id": 0,
                                        "customerId": 1,
                                        "zoneId": 1,
                                        "costCenterId": 1,
                                        "projectGroupId": 1,
                                        "uniqueId": 1,
                                    }
                                },
                            ],
                            "as": "projectGroup",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$projectGroup",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "PMName": "$PMarray.empName",
                            "Project Group": "$projectGroup.projectGroupId",
                            "circle": "$circleresult.circleName",
                        }
                    },
                    {
                        "$project": {
                            "PMName": 1,
                            "projectId": 1,
                            "Project Group": 1,
                            "circle": 1,
                            "_id": 0,
                        }
                    },
                ],
                "as": "projectArray",
            }
        },
        {"$unwind": {"path": "$projectArray", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"_id": {"$toString": "$_id"}}}
    ]
    if accessType == "W":
        arra = arra +[{
                "$lookup": {
                    "from": "invoice",
                    "localField": "_id",
                    "foreignField": "siteId",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$addFields": {
                                "Invoiced Quantity": {
                                    "$cond": {
                                        "if": {"$eq": ["$qty", ""]},
                                        "then": 0,
                                        "else": "$qty",
                                    }
                                },
                                "Unit Rate": {
                                    "$cond": {
                                        "if": {"$eq": ["$unitRate", ""]},
                                        "then": 0,
                                        "else": "$unitRate",
                                    }
                                },
                                "WCC SignOff Date": {
                                    "$dateToString": {
                                        "date": {"$toDate": "$wccSignOffdate"},
                                        "format": "%d-%m-%Y",
                                        "timezone": "Asia/Kolkata",
                                    }
                                },
                                "Invoice Date": {
                                    "$dateToString": {
                                        "date": {"$toDate": "$invoiceDate"},
                                        "format": "%d-%m-%Y",
                                        "timezone": "Asia/Kolkata",
                                    }
                                },
                                "Qtyy": {"$toString": "$qty"},
                                "unitrates": {"$toString": "$unitRate"},
                                "Amounts": {
                                    "$toString": {
                                        "$cond": [
                                            {
                                                "$or": [
                                                    {"$eq": ["$qty", ""]},
                                                    {"$eq": ["$unitRate", ""]},
                                                ]
                                            },
                                            "",
                                            {"$multiply": ["$qty", "$unitRate"]},
                                        ]
                                    }
                                },
                                "Amount": {
                                    "$cond": [
                                        {
                                            "$or": [
                                                {"$eq": ["$qty", ""]},
                                                {"$eq": ["$unitRate", ""]},
                                            ]
                                        },
                                        "",
                                        {"$multiply": ["$qty", "$unitRate"]},
                                    ]
                                },
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "customer": 0,
                                "projectGroup": 0,
                                "projectId": 0,
                                "projectType": 0,
                                "subProject": 0,
                                "systemId": 0,
                                "siteId": 0,
                                "wccSignOffdate": 0,
                                "invoiceDate": 0,
                            }
                        },
                    ],
                    "as": "invoiceResult",
                }
            },
            {
                "$addFields": {
                    "WCC No": {
                        "$reduce": {
                            "input": "$invoiceResult.wccNumber",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Amount": {
                        "$reduce": {
                            "input": "$invoiceResult.Amount",
                            "initialValue": 0,
                            "in": {"$add": ["$$value", "$$this"]},
                        }
                    },
                    "PO Number": {
                        "$reduce": {
                            "input": "$invoiceResult.poNumber",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Item Code": {
                        "$reduce": {
                            "input": "$invoiceResult.itemCode",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Invoiced Quantity": {
                        "$reduce": {
                            "input": "$invoiceResult.Qtyy",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Invoice Number": {
                        "$reduce": {
                            "input": "$invoiceResult.invoiceNumber",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "WCC SignOff Date": {
                        "$reduce": {
                            "input": "$invoiceResult.WCC SignOff Date",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                    "Invoice Date": {
                        "$reduce": {
                            "input": "$invoiceResult.Invoice Date",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [{"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    },
                }
            }]
    arra = arra +[{
            "$addFields": {
                "PMName": "$projectArray.PMName",
                "siteId": "$siteid",
                "Project Group": "$projectArray.Project Group",
                "Project Id": "$projectArray.projectId",
                "projectuniqueId": {"$toString": "$projectuniqueId"},
                "circle": "$projectArray.circle",
                "uniqueId": "$_id",
                "siteStartDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$siteStartDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "siteEndDate": {
                    "$dateToString": {
                        "date": {"$toDate": "$siteEndDate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "rfidate": {
                    "$dateToString": {
                        "date": {"$toDate": "$rfidate"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
            }
        },
        {
            "$project": {
                "projectArray": 0,
                "assignerId": 0,
                "result": 0,
                "Site_Completion Date1": 0,
                "siteEndDate1": 0,
            }
        },
        {"$addFields": {"data": "$$ROOT"}},
        {"$project": {"data": 1}},
        {"$addFields": {"_id": {"$toObjectId": "$_id"}}},
        {
            "$lookup": {
                "from": "milestone",
                "localField": "_id",
                "foreignField": "siteId",
                "pipeline": [
                    {"$match": {"deleteStatus": {"$ne": 1},'assignerId': {
                '$in': [
                    ObjectId(current_user['userUniqueId'])
                ]
            }}},
                    {
                        "$addFields": {
                            "_id": {"$toString": "$_id"},
                            "siteId": {"$toString": "$siteId"},
                            "uniqueId": {"$toString": "$_id"},
                            "mileStoneStartDate1": {"$toDate": "$mileStoneStartDate"},
                            "mileStoneEndtDate1": {"$toDate": "$mileStoneEndDate"},
                            "CC_Completion Date1": {"$toDate": "$CC_Completion Date"},
                        }
                    },
                    {
                        "$addFields": {
                            "CC_Completion Date": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$type": "$CC_Completion Date1"},
                                            "date",
                                        ]
                                    },
                                    "then": {
                                        "$dateToString": {
                                            "date": "$CC_Completion Date1",
                                            "format": "%d-%m-%Y",
                                            "timezone": "Asia/Kolkata",
                                        }
                                    },
                                    "else": None,
                                }
                            },
                            "taskageing": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$type": "$CC_Completion Date1"},
                                            "date",
                                        ]
                                    },
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
                                                {
                                                    "$subtract": [
                                                        "$mileStoneEndtDate1",
                                                        "$$NOW",
                                                    ]
                                                },
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
                                    "date": {"$toDate": "$CC_Completion Date"},
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
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {
                                    "$project": {
                                        "_id": 0,
                                        "assignerName": "$empName",
                                        "assignerId": {"$toString": "$_id"},
                                    }
                                },
                            ],
                            "as": "assignerResult",
                        }
                    },
                    {
                        "$project": {
                            "assignerId": 0,
                            "mileStoneStartDate1": 0,
                            "mileStoneEndtDate1": 0,
                            "CC_Completion Date1": 0,
                            "siteId": 0,
                            "isLast": 0,
                            "uniqueId": 0,
                        }
                    },
                ],
                "as": "milestoneArray",
            }
        },
        {"$unwind": {"path": "$milestoneArray", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"finalData": {"$mergeObjects": ["$data", "$milestoneArray"]}}},
        {"$replaceRoot": {"newRoot": "$finalData"}},
        {
            "$addFields": {
                "assigners": {
                    "$reduce": {
                        "input": "$assignerResult",
                        "initialValue": "",
                        "in": {
                            "$concat": [
                                "$$value",
                                {
                                    "$cond": [
                                        {"$eq": ["$$value", ""]},
                                        "$$this.assignerName",
                                        {"$concat": [", ", "$$this.assignerName"]},
                                    ]
                                },
                            ]
                        },
                    }
                },
                "Status": "$siteBillingStatus",
            }
        },
        {
            "$project": {
                "_id": 0,
                "SubProjectId": 0,
                "new_u_id": 0,
                "projectuniqueId": 0,
                "uniqueId": 0,
                "assignerResult": 0,
                "CC_MO No": 0,
                "deleated_at": 0,
                "siteStatus": 0,
                "invoiceResult": 0,
            }
        },
        {
            "$addFields": {
                "RFAI Date": {
                    "$dateToString": {
                        "date": {"$toDate": "$RFAI Date"},
                        "format": "%d-%m-%Y",
                        "timezone": "Asia/Kolkata",
                    }
                },
                # "MS Completition Date": "$mileStoneEndDate",
                "MS Completition Date": "$CC_Completion Date",
                "System ID": "$systemId",
                "Due Date": {
                    "$let": {
                        "vars": {
                            "date": {
                                "$dateFromString": {
                                    "dateString": "$mileStoneStartDate",
                                    "format": "%d-%m-%Y",
                                }
                            },
                            "daysToAdd": "$taskageing",
                        },
                        "in": {
                            "$dateToString": {
                                "date": {
                                    "$add": [
                                        "$$date",
                                        {"$multiply": ["$$daysToAdd", 86400000]},
                                    ]
                                },
                                "format": "%d-%m-%Y",
                            }
                        },
                    }
                },
                "Task Ageing": "$taskageing",
                "Start Date": "$mileStoneStartDate",
                "Task Closure": {
                    "$cond": {
                        "if": {"$eq": ["$Task Closure", None]},
                        "then": "",
                        "else": {
                            "$dateToString": {
                                "date": {"$toDate": "$Task Closure"},
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                    }
                },
                "Custom Status": "$mileStoneStatus",
                "Task Owner": "$PMName",
                "Task Name": "$Name",
                "Unique ID": "$Unique ID",
                "Site ID": "$Site Id",
                "Circle": "$circle",
                "Sub Project": "$subProject",
                "Project Type": "$projectType",
                "Project ID": "$Project Id",
                "Status": "$siteBillingStatus",
                "Site Allocation Date": {
                    "$cond": {
                        "if": {"$eq": ["$Site Allocation Date", None]},
                        "then": "$RFAI Date",
                        "else": "$Site Allocation Date",
                    }
                },
            }
        },
        {
            "$addFields": {
                "Delay": {
                    "$divide": [
                        {
                            "$subtract": [
                                {
                                    "$dateFromString": {
                                        "dateString": "$MS Completition Date",
                                        "format": "%d-%m-%Y",
                                    }
                                },
                                {
                                    "$dateFromString": {
                                        "dateString": "$Due Date",
                                        "format": "%d-%m-%Y",
                                    }
                                },
                            ]
                        },
                        86400000,
                    ]
                }
            }
        },
        {
            "$project": {
                "Project Id": 0,
                "projectType": 0,
                "subProject": 0,
                "circle": 0,
                "Site Id": 0,
                "uniqueid": 0,
                "Name": 0,
                "PMName": 0,
                "rfaidate": 0,
                "rfidate": 0,
                "assignerResult": 0,
                "nominal_aop": 0,
                "mileStoneStatus": 0,
                "systemId": 0,
                "mileStoneEndDate": 0,
                "mileStoneStartDate": 0,
                "taskageing": 0,
                "siteageing": 0,
                "siteEndDate": 0,
                "siteStartDate": 0,
                "CC_Completion Date": 0,
                "Site_Completion Date": 0,
                "Completion Criteria": 0,
                "Estimated Time (Days)": 0,
                "Predecessor": 0,
                "WCC Sign off": 0,
                "estimateDay": 0,
                "indexing": 0,
                "assigners": 0,
                "Unit Rate": 0,
                "customerId": 0,
                "projectGroupid": 0,
            }
        },
    ]
    response = cmo.finding_aggregate("SiteEngineer", arra)["data"]
    dataframe = pd.DataFrame(response)
    for column in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(dataframe[column]):
            if dataframe[column].dt.tz is not None:
                dataframe[column] = dataframe[column].dt.tz_localize(None)
    ll = pd.DataFrame(list_arr)
    sortCol = [
        "Project Group",
        "Project ID",
        "Project Type",
        "Sub Project",
        "Circle",
        "Site ID",
        "Unique ID",
        "System ID",
        "Task Name",
        "Task Owner",
        "Start Date",
        "Due Date",
        "MS Completition Date",
        "Task Closure",
        "Delay",
        "Task Ageing",
        "Custom Status",
    ]
    ll = ll[~ll.isin(sortCol).any(axis=1)]
    kkkk = []
    if 'fieldName' in ll.columns:
        kk = ll["fieldName"].to_list()
        if len(kk):
            for i in kk:
                if i not in kkkk and i not in sortCol:
                    kkkk.append(i)
    jsjsj = sortCol + kkkk
    if "Unit Rate" in jsjsj:
        jsjsj.remove("Unit Rate")
    if "Site Id" in jsjsj:
        jsjsj.remove("Site Id")
    if "Customer" in jsjsj:
        jsjsj.remove("Customer")
    respdf = pd.DataFrame(response)
    if accessType == "W":
        columnsClear = [
            "WCC No",
            "WCC SignOff Date",
            "PO Number",
            "Item Code",
            "Invoiced Quantity",
            "Invoice Number",
            "Invoice Date",
            "Amount",
        ]
        for col in columnsClear:
            if col in jsjsj and col in columnsClear:
                respdf[col] = np.where(respdf["Task Name"] != "Revenue Recognition", None, respdf[col])
                
    for col in respdf.columns:
        respdf[col] = respdf[col].apply(convertToDateBulkExport)
    respdf = respdf.reindex(columns=jsjsj, fill_value="")
    fullPath = excelWriteFunc.excelFileWriter(respdf, "Export_All_With_Task", "All Project")
    return send_file(fullPath)


@export_blueprint.route('/export/MasterUnitRate',methods=['GET','POST'])
@token_required
def MasterUnitRate(current_user):
    
    if request.method == "GET":
        arr=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'project', 
                        'localField': 'project', 
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
                        'as': 'projectResults'
                    }
                }, {
                    '$lookup': {
                        'from': 'projectType', 
                        'localField': 'subProject', 
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
                        'as': 'projectTypeResults'
                    }
                }, {
                    '$unwind': {
                        'path': '$projectTypeResults', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$unwind': {
                        'path': '$projectResults', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'customer', 
                        'localField': 'customer', 
                        'foreignField': '_id', 
                        'as': 'customer'
                    }
                }, {
                    '$addFields': {
                        'activity': '$activity', 
                        'band': '$band', 
                        'itemCode01': '$itemCode01', 
                        'itemCode02': '$itemCode02', 
                        'itemCode03': '$itemCode03', 
                        'itemCode04': '$itemCode04', 
                        'itemCode05': '$itemCode05', 
                        'itemCode06': '$itemCode06', 
                        'itemCode07': '$itemCode07', 
                        'rate': '$rate', 
                        'subProject': {
                            '$toString': '$projectTypeResults._id'
                        }, 
                        'projectId': '$projectResults.projectId', 
                        'project': {
                            '$toString': '$projectResults._id'
                        }, 
                        'projectType': {
                            '$toString': '$projectTypeResults._id'
                        }, 
                        'subProjectName': '$projectTypeResults.subProject', 
                        'projectTypeName': '$projectTypeResults.projectType', 
                        'uniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$sort': {
                        'projectTypeName': 1
                    }
                }, {
                    '$project': {
                        'Customer': {
                            '$arrayElemAt': [
                                '$customer.customerName', 0
                            ]
                        }, 
                        'Project Type': '$projectTypeName', 
                        'Project ID': '$projectId', 
                        'Sub Project': '$subProjectName', 
                        'Band': '$band', 
                        'ACTIVITY': '$activity', 
                        'Rate': '$rate', 
                        'Item Code-01': '$itemCode01', 
                        'Item Code-02': '$itemCode02', 
                        'Item Code-03': '$itemCode03', 
                        'Item Code-04': '$itemCode04', 
                        'Item Code-05': '$itemCode05', 
                        'Item Code-06': '$itemCode06', 
                        'Item Code-07': '$itemCode07', 
                        '_id': 0
                    }
                }
            ]
        # arr=[
        #         {
        #             '$match': {
        #                 'deleteStatus': {
        #                     '$ne': 1
        #                 }
        #             }
        #         }, {
        #             '$lookup': {
        #                 'from': 'project', 
        #                 'localField': 'project', 
        #                 'foreignField': '_id', 
        #                 'pipeline': [
        #                     {
        #                         '$match': {
        #                             'deleteStatus': {
        #                                 '$ne': 1
        #                             }
        #                         }
        #                     }
        #                 ], 
        #                 'as': 'projectResults'
        #             }
        #         }, {
        #             '$lookup': {
        #                 'from': 'projectType', 
        #                 'localField': 'subProject', 
        #                 'foreignField': '_id', 
        #                 'pipeline': [
        #                     {
        #                         '$match': {
        #                             'deleteStatus': {
        #                                 '$ne': 1
        #                             }
        #                         }
        #                     }
        #                 ], 
        #                 'as': 'projectTypeResults'
        #             }
        #         }, {
        #             '$unwind': {
        #                 'path': '$projectTypeResults', 
        #                 'preserveNullAndEmptyArrays': True
        #             }
        #         }, {
        #             '$unwind': {
        #                 'path': '$projectResults', 
        #                 'preserveNullAndEmptyArrays': True
        #             }
        #         }, {
        #             '$addFields': {
        #                 'activity': '$activity', 
        #                 'band': '$band', 
        #                 'itemCode01': '$itemCode01', 
        #                 'itemCode02': '$itemCode02', 
        #                 'itemCode03': '$itemCode03', 
        #                 'itemCode04': '$itemCode04', 
        #                 'itemCode05': '$itemCode05', 
        #                 'itemCode06': '$itemCode06', 
        #                 'itemCode07': '$itemCode07', 
        #                 'rate': '$rate', 
        #                 'subProject': {
        #                     '$toString': '$projectTypeResults._id'
        #                 }, 
        #                 'projectId': '$projectResults.projectId', 
        #                 'project': {
        #                     '$toString': '$projectResults._id'
        #                 }, 
        #                 'projectType': {
        #                     '$toString': '$projectTypeResults._id'
        #                 }, 
        #                 'subProjectName': '$projectTypeResults.subProject', 
        #                 'projectTypeName': '$projectTypeResults.projectType', 
        #                 'uniqueId': {
        #                     '$toString': '$_id'
        #                 }
        #             }
        #         }, {
        #             '$sort': {
        #                 'projectTypeName': 1
        #             }
        #         }, {
        #             '$project': {
        #                 'Project Type': '$projectTypeName', 
        #                 'Project ID': '$projectId', 
        #                 'Sub Project': '$subProjectName', 
        #                 'Band': '$band', 
        #                 'ACTIVITY': '$activity', 
        #                 'Rate': '$rate', 
        #                 'Item Code-01': '$itemCode01', 
        #                 'Item Code-02': '$itemCode02', 
        #                 'Item Code-03': '$itemCode03', 
        #                 'Item Code-04': '$itemCode04', 
        #                 'Item Code-05': '$itemCode05', 
        #                 'Item Code-06': '$itemCode06', 
        #                 'Item Code-07': '$itemCode07',
        #                 'uniqueId':1,
        #                 '_id':0
        #             }
        #         }
        #     ]
        response=cmo.finding_aggregate("AccuralRevenueMaster",arr)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_MasterUnitRate", "MasterUnitRate")
        return send_file(fullPath)
    
    
      

@export_blueprint.route("/export/AdminLogs",methods=['GET','POST'])
@token_required
def ExportAdminLogs(current_user):
    if request.method == "GET":
        arr=[
    {
        '$match': {
            'deleteStatus': {
                '$ne': 1
            }
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'actionBy', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$project': {
                        'empName': 1, 
                        'ustCode': 1, 
                        'email': 1, 
                        '_id': 0
                    }
                }
            ], 
            'as': 'userResults'
        }
    }, {
        '$unwind': {
            'path': '$userResults', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$project': {
            'actionType': '$type', 
            'module': 1, 
            'actionBy': '$userResults.empName', 
            'action': 1, 
            'actionAt': 1, 
            'uniqueId': {
                '$toString': '$_id'
            }, 
            '_id': 0
        }
    }, {
        '$project': {
            'Action By': '$actionBy', 
            'Action AT': '$actionAt', 
            'Module': '$module', 
            'Action Type': '$actionType', 
            'Action': '$action'
        }
    }
]
        response=cmo.finding_aggregate("AdminLogs",arr)
        
        
        response = response["data"]
        dataframe = pd.DataFrame(response)
        dateColumn=['Action AT']
        for col in dateColumn:
            dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
        fullPath = excelWriteFunc.excelFileWriter(
            dataframe, "Export_AdminLogs", "Admin Logs"
        )
        return send_file(fullPath)

def currentuser_circle(empId=None,customerId=None):
    arra = [
        {
            '$match': {
                'empId': empId
            }
        }, {
            '$lookup': {
                'from': 'project', 
                'localField': 'projectIds', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {'$ne': 1},
                            "custId":customerId
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'circle': '$result.circle'
            }
        }, {
            '$group': {
                '_id': '$circle'
            }
        }, {
            '$addFields': {
                'circle': '$_id'
            }
        }, {
            '$project': {
                '_id': 0
            }
        }
    ]
    res = cmo.finding_aggregate("projectAllocation",arra)['data']
    return res

def myFunction(MSType,customerId,matchSubProject,monthArray,Year):
    
    if len(monthArray) == 1 and monthArray[0] == 1 :
        startDate = f'{Year-1}-12-26'
        endDate = f'{Year}-01-25T05:30:00.000+00:00'
    if len(monthArray) == 1 and monthArray[0] != 1 :
        startmonth = monthArray[0]-1
        lastmonth = monthArray[0]
        startDate = f'{Year}-{startmonth:02d}-26'
        endDate = f'{Year}-{lastmonth:02d}-25T05:30:00.000+00:00'
    if len(monthArray) == 3 and monthArray[0] == 1 :
        startDate = f'{Year-1}-12-26'
        endDate = f'{Year}-{monthArray[2]:02d}-25T05:30:00.000+00:00'
    if len(monthArray) == 3 and monthArray[0] != 1 :
        startmonth = monthArray[0]-1
        startDate = f'{Year}-{startmonth:02d}-26'
        endDate = f'{Year}-{monthArray[2]:02d}-25T05:30:00.000+00:00'

    return [
        {
            '$lookup': {
                'from': 'milestone', 
                'let': {
                    'circleId': '$circleId'
                }, 
                'pipeline': [
                    {
                        '$match': {
                            'Name': MSType, 
                            'mileStoneStatus': 'Closed', 
                            'customerId': customerId,
                            "SubProjectId":{
                                '$in': matchSubProject
                            }
                        }
                    }, {
                        '$project': {
                            'CC_Completion Date': 1, 
                            'circleId': 1, 
                            'SubProjectId': 1, 
                            '_id': 0
                        }
                    }, {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$circleId', '$$circleId'
                                ]
                            }
                        }
                    }, {
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
                            'SubProjectId': {
                                '$toObjectId': '$SubProjectId'
                            }
                        }
                    }, {
                        '$addFields': {
                            'startDate': {
                                '$dateFromString': {
                                    'dateString': startDate
                                }
                            }, 
                            'endDate': {
                                '$dateFromString': {
                                    'dateString': endDate
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
                            'from': 'projectType', 
                            'localField': 'SubProjectId', 
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
                                        'subProjectName': {
                                            '$concat': [
                                                '$subProject', '(', '$projectType', ')'
                                            ]
                                        }
                                    }
                                }
                            ], 
                            'as': 'result'
                        }
                    }, {
                        '$addFields': {
                            'subProjectName': {
                                '$arrayElemAt': [
                                    '$result.subProjectName', 0
                                ]
                            }, 
                            'SubProjectId': {
                                '$toString': '$SubProjectId'
                            }
                        }
                    }, {
                        '$group': {
                            '_id': {
                                'SubProjectId': '$SubProjectId', 
                                'SubProjectName': '$subProjectName'
                            }, 
                            'value': {
                                '$sum': 1
                            }
                        }
                    }, {
                        '$addFields': {
                            'subProjectName': '$_id.SubProjectName', 
                            'subProjectId': '$_id.SubProjectId'
                        }
                    }, {
                        '$project': {
                            'subProjectName': 1, 
                            'subProjectId': 1, 
                            'value': 1, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'achievement'
            }
        }
    ]


@export_blueprint.route("/export/forms/EVMActual", methods=["POST","PATCH"])
@export_blueprint.route("/export/forms/EVMActual/<id>", methods=["POST","PATCH"])
@token_required
def EVMActual(current_user, id=None):
    if request.method == "POST":
        allData = request.get_json()
        allocatedCircle = []
        userCircle = currentuser_circle(current_user['userUniqueId'],allData['customerId'])
        if len(userCircle):
            for i in userCircle:
                allocatedCircle.append(ObjectId(i['circle']))
        
        allData['MSType'] = allData['MSType'].split("-")[0]
        matchSubProject = []
        arra = [
            {
                '$match': {
                    'customer': ObjectId(allData['customerId'])
                }
            }, {
                '$unwind': {
                    'path': '$subProjectId', 
                    'preserveNullAndEmptyArrays': True
                }
            }
        ]
        res = cmo.finding_aggregate("masterSubProject",arra)['data']
        if len(res):
            for i in res:
                matchSubProject.append(i['subProjectId'])
        
        arra = [
            {
                '$match': {
                    'customerId': ObjectId(allData['customerId']), 
                    "MSType":allData['MSType'],
                    'month': {
                        '$in': allData['month']
                    }, 
                    'year': allData['year'],
                    "circleId":{
                        '$in':allocatedCircle
                    }
                }
            }
        ] 
        
        if allData['circleId']!=None and allData['circleId']!="":
            arra = arra + [
                {
                    '$match':{
                        'circleId':ObjectId(allData['circleId'])
                    }
                }
            ]
        arra = arra + [
            {
                '$lookup': {
                    'from': 'circle', 
                    'localField': 'circleId', 
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
                    'circle': {
                        '$arrayElemAt': [
                            '$result.circleCode', 0
                        ]
                    }, 
                    'circleId': {
                        '$toString': '$circleId'
                    }, 
                    'target': '$subProjects'
                }
            }
        ]
        arra = arra + myFunction(allData['MSType'],allData['customerId'],matchSubProject,allData['month'],allData['year'])
        arra = arra + [
            {
                '$project': {
                    'circleId': 1, 
                    'circle': 1, 
                    'target': 1, 
                    'achievement': 1,
                    '_id':0,
                    'month':1,
                    'year':1
                }
            },
            {
                '$addFields': {
                    'month': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$month', 1
                                        ]
                                    }, 
                                    'then': 'Jan'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 2
                                        ]
                                    }, 
                                    'then': 'Feb'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 3
                                        ]
                                    }, 
                                    'then': 'Mar'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 4
                                        ]
                                    }, 
                                    'then': 'Apr'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 5
                                        ]
                                    }, 
                                    'then': 'May'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 6
                                        ]
                                    }, 
                                    'then': 'Jun'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 7
                                        ]
                                    }, 
                                    'then': 'Jul'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 8
                                        ]
                                    }, 
                                    'then': 'Aug'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 9
                                        ]
                                    }, 
                                    'then': 'Sep'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 10
                                        ]
                                    }, 
                                    'then': 'Oct'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 11
                                        ]
                                    }, 
                                    'then': 'Nov'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 12
                                        ]
                                    }, 
                                    'then': 'Dec'
                                }
                            ], 
                            'default': "$month"
                        }
                    }
                }
            }
        ]
        
        res = cmo.finding_aggregate("deliveryPVA",arra)['data']
        
        arracustomer = [
        {
            '$match': {
                'customer': ObjectId(allData['customerId'])
            }
        }, {
            '$unwind': {
                'path': '$subProjectIds', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'subProjectIds', 
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
            '$project': {
                'subProjectName': {
                    '$arrayElemAt': [
                        '$result.subProject', 0
                    ]
                }, 
                'subProjectId': {
                    '$toString': '$subProjectIds'
                }, 
                '_id': 0, 
                'projectType': 1
            }
        }, {
            '$addFields': {
                'subProjectName': {
                    '$concat': [
                        '$subProjectName', '(', '$projectType', ')'
                    ]
                }
            }
        }
        ]
        resSubproject = cmo.finding_aggregate("masterSubProject",arracustomer)
        ordered_categories = [
                "UPGRADE(MACRO)", "NEW TOWER(MACRO)", "RELOCATION(RELOCATION)", 
                "NEW(ULS)", "UPGRADE(ULS)", "RELOCATION(MICROWAVE)", 
                "NEW NOMINAL(MICROWAVE)", "ALINGNMENT(MICROWAVE)", "4TR-2TR(DEGROW)", 
                "SECTOR DEGROW(DEGROW)", "LAYER DEGROW(DEGROW)", "TWIN BEAM(DEGROW)"
            ]
        if len(resSubproject['data']):
            ordered_categories=[]
            for i in resSubproject['data']:
                ordered_categories.append(i['subProjectName'])
        print(ordered_categories,'ordered_categoriesordered_categories')
                
        # def generate_excel(data_list, Mstype=None):
        #     output = io.BytesIO()
        #     circles = {}
            

        #     for data in data_list:
        #         circle = data["circle"]

        #         if circle not in circles:
        #             circles[circle] = {"Circle": circle}

        #         for target in data.get("target", []):
        #             target_col = f"{target['subProjectName']} Target"
        #             circles[circle][target_col] = target["value"]

        #         for achievement in data.get("achievement", []):
        #             achievement_col = f"{achievement['subProjectName']} Achievement"
        #             circles[circle][achievement_col] = achievement["value"]

        #     df = pd.DataFrame.from_dict(circles, orient='index').reset_index(drop=True)

        #     # Fill NaN values with 0
        #     df.fillna(0, inplace=True)

        #     # Compute total target and total achievement
        #     target_cols = [col for col in df.columns if "Target" in col and "Total" not in col]
        #     achievement_cols = [col for col in df.columns if "Achievement" in col and "Total" not in col]

        #     df["Total Target"] = df[target_cols].sum(axis=1)
        #     df["Total Achievement"] = df[achievement_cols].sum(axis=1)

        #     # Calculate achievement percentages only if target is greater than 0
        #     for col in achievement_cols:
        #         target_col = col.replace(" Achievement", " Target")
        #         percentage_col = col.replace(" Achievement", " Achievement %")
        #         if target_col in df.columns:
        #             df[percentage_col] = df.apply(lambda row: f"{(row[col] / row[target_col]) * 100:.2f}%" 
        #                                         if row[target_col] > 0 else "0%", axis=1)

        #     # Calculate total achievement percentage
        #     df["Total Achievement %"] = df.apply(lambda row: f"{(row['Total Achievement'] / row['Total Target']) * 100:.2f}%" 
        #                                         if row["Total Target"] > 0 else "0%", axis=1)

        #     # Arrange columns in the required sequence
        #     ordered_target_cols = [f"{category} Target" for category in ordered_categories if f"{category} Target" in df]
        #     ordered_achievement_cols = [f"{category} Achievement" for category in ordered_categories if f"{category} Achievement" in df]
        #     ordered_achievement_perc_cols = [f"{category} Achievement %" for category in ordered_categories if f"{category} Achievement %" in df]

        #     ordered_columns = (
        #         ["Circle"] +
        #         ordered_target_cols + ["Total Target"] +
        #         ordered_achievement_cols + ["Total Achievement"] +
        #         ordered_achievement_perc_cols + ["Total Achievement %"]
        #     )

        #     # Ensure only existing columns are selected
        #     ordered_columns = [col for col in ordered_columns if col in df]
        #     df = df[ordered_columns]

        #     with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        #         df.to_excel(writer, index=False, sheet_name="Summary", startrow=1)
        #         workbook = writer.book
        #         worksheet = writer.sheets["Summary"]

        #         # Write the Mstype message in the first row across all columns
        #         if data_list:
        #             header_text = f"This Data is for {Mstype} {data_list[0]['month']}-{data_list[0]['year']}"
        #             merge_format = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#070101", "font_size": 14, "font_color": "#f9f7f7"})
        #             worksheet.merge_range(0, 0, 0, len(df.columns) - 1, header_text, merge_format)

        #         # Apply header formatting
        #         header_format = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "border": 1})
        #         circle_format = workbook.add_format({"bold": True, "bg_color": "#ADD8E6", "border": 1})
        #         target_format = workbook.add_format({"bold": True, "bg_color": "#90EE90", "border": 1})
        #         achievement_format = workbook.add_format({"bold": True, "bg_color": "#FFFF99", "border": 1})
        #         percentage_format = workbook.add_format({"bold": True, "bg_color": "#FFA07A", "border": 1})

        #         for col_num, value in enumerate(df.columns):
        #             if "Circle" in value:
        #                 worksheet.write(1, col_num, value, circle_format)
        #             elif "Target" in value:
        #                 worksheet.write(1, col_num, value, target_format)
        #             elif "Achievement %" in value:
        #                 worksheet.write(1, col_num, value, percentage_format)
        #             elif "Achievement" in value:
        #                 worksheet.write(1, col_num, value, achievement_format)
        #             else:
        #                 worksheet.write(1, col_num, value, header_format)

        #     output.seek(0)
        #     return output
        def generate_excel(data_list, Mstype=None):
            output = io.BytesIO()
            circles = {}

            # Collect data and organize it by circle
            for data in data_list:
                circle = data["circle"]

                if circle not in circles:
                    circles[circle] = {"Circle": circle}

                for target in data.get("target", []):
                    target_col = f"{target['subProjectName']} Target"
                    circles[circle][target_col] = target["value"]

                for achievement in data.get("achievement", []):
                    achievement_col = f"{achievement['subProjectName']} Achievement"
                    circles[circle][achievement_col] = achievement["value"]

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(circles, orient='index').reset_index(drop=True)

            # Fill NaN values with 0
            df.fillna(0, inplace=True)

            # Ensure all target, achievement, and achievement % columns are present based on ordered_categories
            ordered_target_cols = [f"{category} Target" for category in ordered_categories]
            ordered_achievement_cols = [f"{category} Achievement" for category in ordered_categories]
            ordered_achievement_perc_cols = [f"{category} Achievement %" for category in ordered_categories]

            ordered_columns = (
                ["Circle"] +
                ordered_target_cols + ["Total Target"] +
                ordered_achievement_cols + ["Total Achievement"] +
                ordered_achievement_perc_cols + ["Total Achievement %"]
            )

            # Ensure all required columns exist in DataFrame (initialize missing ones with 0)
            for col in ordered_columns:
                if col not in df.columns:
                    df[col] = 0

            # Reorder the DataFrame
            df = df[ordered_columns]

            # Compute totals after ensuring all columns exist
            df["Total Target"] = df[ordered_target_cols].sum(axis=1, numeric_only=True)
            df["Total Achievement"] = df[ordered_achievement_cols].sum(axis=1, numeric_only=True)

            # Calculate achievement percentages only if target is greater than 0
            for category in ordered_categories:
                target_col = f"{category} Target"
                achievement_col = f"{category} Achievement"
                percentage_col = f"{category} Achievement %"

                if target_col in df.columns and achievement_col in df.columns:
                    df[percentage_col] = df.apply(lambda row: f"{(row[achievement_col] / row[target_col]) * 100:.2f}%" 
                                                if row[target_col] > 0 else "0%", axis=1)

            # Calculate total achievement percentage
            df["Total Achievement %"] = df.apply(lambda row: f"{(row['Total Achievement'] / row['Total Target']) * 100:.2f}%" 
                                                if row["Total Target"] > 0 else "0%", axis=1)

            # Fill NaN values with 0 (for safety)
            df.fillna(0, inplace=True)

            # Create Excel output
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name="Summary", startrow=1)
                workbook = writer.book
                worksheet = writer.sheets["Summary"]

                # Write the Mstype message in the first row across all columns
                if data_list:
                    header_text = f"This Data is for {Mstype} {data_list[0]['month']}-{data_list[0]['year']}"
                    merge_format = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#070101", "font_size": 14, "font_color": "#f9f7f7"})
                    worksheet.merge_range(0, 0, 0, len(df.columns) - 1, header_text, merge_format)

                # Apply header formatting
                header_format = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "border": 1})
                circle_format = workbook.add_format({"bold": True, "bg_color": "#ADD8E6", "border": 1})
                target_format = workbook.add_format({"bold": True, "bg_color": "#90EE90", "border": 1})
                achievement_format = workbook.add_format({"bold": True, "bg_color": "#FFFF99", "border": 1})
                percentage_format = workbook.add_format({"bold": True, "bg_color": "#FFA07A", "border": 1})

                # Write headers with formatting
                for col_num, value in enumerate(df.columns):
                    if "Circle" in value:
                        worksheet.write(1, col_num, value, circle_format)
                    elif "Target" in value:
                        worksheet.write(1, col_num, value, target_format)
                    elif "Achievement %" in value:
                        worksheet.write(1, col_num, value, percentage_format)
                    elif "Achievement" in value:
                        worksheet.write(1, col_num, value, achievement_format)
                    else:
                        worksheet.write(1, col_num, value, header_format)

            output.seek(0)
            return output
        
        df = generate_excel(res,allData['MSType'])
        # fullPath = excelWriteFunc.excelFileWriter(
        # df, "Export_AdminLogs", "Admin Logs"
        # )
        return df
        return send_file(fullPath)
            
        # return respond(res)
    

def pdf_export(document, keys):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=30,
        rightMargin=30,
        topMargin=30,
        bottomMargin=30
    )

    elements = []
    styles = getSampleStyleSheet()
    normal_style = styles["Normal"]
    header_style = styles["Heading2"]

    def build_table(data_dict,section):
        rows = [["Field", "Value"]]  

        for field, val in data_dict.items():

            val_str = str(val).strip()
            image_path = None

            # if section == "SnapData":
            #     print(val_str,"val_str")
            #     image_match = re.search(r"(uploads/uploadSnapImage/[^\s]+?\.(?:jpg|jpeg|png))", val_str, re.IGNORECASE)
            #     if image_match:
            #         rel_path = os.path.relpath(image_match.group(1), "uploads/uploadSnapImage")
            #         local_path = os.path.join("uploads/uploadSnapImage", rel_path)
            #         if os.path.isfile(local_path):
            #             image_path = local_path
            images = []
            if section == "SnapData":
                
                if isinstance(val, list):  # If value is a list of image paths
                    for img_path in val:
                        if isinstance(img_path, str) and img_path.lower().endswith((".jpg", ".jpeg", ".png")):
                            local_path = os.path.join("uploads/uploadSnapImage", os.path.relpath(img_path, "uploads/uploadSnapImage"))
                            if os.path.isfile(local_path):
                                images.append(local_path)
                elif isinstance(val, str):  # Single image string
                    if val.lower().endswith((".jpg", ".jpeg", ".png")):
                        local_path = os.path.join("uploads/uploadSnapImage", os.path.relpath(val, "uploads/uploadSnapImage"))
                        if os.path.isfile(local_path):
                            images.append(local_path)

    
            wrapped_field = Paragraph(field, normal_style)

            # if image_path:
            #     try:
            #         img = PlatypusImage(image_path, width=100, height=100)
            #         rows.append([wrapped_field, img])
            #     except Exception as e:
            #         print(f"[Image Embed Error] {field}: {e}")
            #         rows.append([wrapped_field, Paragraph("Image could not be displayed", normal_style)])
            # else:
            #     wrapped_value = Paragraph(val_str, normal_style)
            #     rows.append([wrapped_field, wrapped_value])

            if images:
                image_elements = []
                for img_path in images:
                    try:
                        img = PlatypusImage(img_path, width=100, height=100)
                        image_elements.append(img)
                    except Exception as e:
                        print(f"[Image Embed Error] {field}: {e}")
                rows.append([wrapped_field, image_elements])
            else:
                wrapped_value = Paragraph(str(val).strip(), normal_style)
                rows.append([wrapped_field, wrapped_value])

        table = Table(rows, colWidths=[160, 320]) 
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        return table

 
    for section in keys:
        section_data = document.get(section)
        if not section_data:
            continue

        elements.append(Paragraph(f"{section.upper()} SECTION", header_style))
        elements.append(Spacer(1, 6))

        if isinstance(section_data, dict):
            elements.append(build_table(section_data,section))
        elif isinstance(section_data, list):
            for item in section_data:
                if isinstance(item, dict):
                    elements.append(build_table(item))
                    elements.append(Spacer(1, 10))
                else:
                    elements.append(build_table({f"{section}": item}))

        elements.append(PageBreak())


    doc.build(elements)
    buffer.seek(0)

    return send_file(
        buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"PAT_Export.pdf"
    )

@export_blueprint.route("/Compliance/templateUpload/<id>/<mName>/<tabType>/<globalSaverId>", methods=["POST"])
@token_required
def compliance_template_upload(current_user,id=None,mName=None,tabType=None,globalSaverId=None):
    if request.method == "POST":
        upload = request.files.get("uploadedFile[]")
        allData = {}
        supportFile = ["xlsx", "xls"]
        pathing = cform.singleFileSaver(upload, "", supportFile)
        if pathing["status"] == 200:
            allData["pathing"] = pathing["msg"]
        elif pathing["status"] == 422:
            return respond(pathing)
        excel_file_path = os.path.join(os.getcwd(), allData["pathing"])
        exceldata = pd.read_excel(excel_file_path)
        if exceldata.empty:
            return respond({
                'status':400,
                "icon":"error",
                "msg":"Your Excel File is Empty"
            })
        exceldata = exceldata.iloc[[0]]
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$project': {
                    'SubProjectId': 1, 
                    'ACTIVITY': 1, 
                    'OEM NAME': 1
                }
            }, {
                '$addFields': {
                    'SubProjectId': {
                        '$toObjectId': '$SubProjectId'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'complianceForm', 
                    'let': {
                        'subProjectId': '$SubProjectId', 
                        'activity':{
                            '$trim':{'input':'$ACTIVITY'}
                        } , 
                        'oem': {
                            "$trim":{"input":'$OEM NAME'}
                        }, 
                        'mName': mName.strip()
                    }, 
                    'pipeline': [
                        {
                            '$match':{
                                'deleteStatus':{'$ne':1}
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$subProject', '$$subProjectId'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$activity', '$$activity'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$oem', '$$oem'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$complianceMilestone', '$$mName'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                tabType:1,
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    '_id': 0, 
                    tabType: {
                        '$ifNull': [
                            f"$result.{tabType}", []
                        ]
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer", arra)['data']
        if response:
            response = response[0]
            columns = []
            required_columns = []
            dropdownField = []
            data_column = []
            for i in response[tabType]:
                if i['fieldName'] not in columns:
                    columns.append(i['fieldName'])
                if i['required'] == "Yes":
                    required_columns.append(i['fieldName'])
                if i['dataType'] == "Dropdown":
                    i['fieldName'] = i['fieldName'].strip()
                    i['dropdownValue'] = [value.strip() for value in i['dropdownValue'].split(',')]
                    dropdownField.append(i)
                if i['dataType'] == "Date":
                    data_column.append(i['fieldName'])

            extra_cols = [col for col in list(exceldata.columns) if col not in columns]
            if extra_cols:
                return respond ({
                    'status':400,
                    "msg": f"Please remove the following column(s) from your file: \n" + ", ".join(extra_cols),
                    "icon":"error"
                })
            require_cols = [col for col in required_columns if col not in list(exceldata.columns)]
            if require_cols:
                return respond({
                    'status':400,
                     "msg": f"The following required column(s) were not found in your file. Please add them:\n" + ", ".join(require_cols),
                     "icon":"error"
                })
            empty_columns = [col for col in required_columns if exceldata[col].isna().any()]
            if empty_columns:
                return respond({
                    "status":400,
                    "msg": f"The following required column(s) contain empty values. Please fill them:\n" + ", ".join(empty_columns),
                    "icon":"error"
                })
            for item in dropdownField:
                field_name = item['fieldName']
                valid_values = item['dropdownValue']
                required = item['required']
                if required == 'No':
                    exceldata[field_name] = exceldata[field_name].apply(lambda x: x if x in valid_values or pd.isna(x) else "")
                else:
                    if field_name in ["BAND"]:
                        exceldata[field_name] = exceldata[field_name].str.split(",")
                        invalid_values = (exceldata[field_name].dropna().apply(lambda lst: [item for item in lst if item not in valid_values]).explode() .dropna().unique())
                        exceldata[field_name] = exceldata[field_name].apply(lambda x: ','.join(map(str, x)) if len(x) > 1 else str(x[0]))
                    else:
                        exceldata[field_name] = exceldata[field_name].astype(str)
                        invalid_values = exceldata[~exceldata[field_name].isin(valid_values)][field_name].dropna().unique()
                    if len(invalid_values) > 0:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":f"Field '{field_name}' has invalid values: {invalid_values}"
                        })
            columns_to_null  = [col for col in data_column if col in exceldata.columns]
            for col in columns_to_null:
                exceldata[col] = None
            dict_data = cfc.dfjson(exceldata)
            allData = {}
            replaceData = {
                'Template':"TemplateData",
                "planDetails":"PlanDetailsData",
                "siteDetails":"SiteDetailsData",
                "ranChecklist":"RanCheckListData",
                "acceptanceLog":"AcceptanceLogData"
            }
            allData[replaceData[tabType]]=dict_data[0]
            print(allData,"allData")
            res = cmo.updating("complianceApproverSaver",{'_id':ObjectId(globalSaverId)},allData,False)
            return respond(res)


        else:
            return respond({
                'status':400,
                "msg":"Please Contact to Admin/PMO Team, Form & Checklist is not found for this Pair",
                "icon":"error"
            })
            

@export_blueprint.route("/compliance/export/<reportType>/<id>", methods=["GET"])
@token_required
def DownloadAttachmentcompliance(current_user,reportType=None,id=None):
    if not reportType or reportType not in ['pdf','Excel']:
        return respond({"status":400,"msg":"please confirm report type","icon":"error"})
    if reportType == "Excel":
        aggr=[
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteuid', 
                    'foreignField': '_id', 
                    'as': 'siteResult'
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'milestoneuid', 
                    'foreignField': '_id', 
                    'as': 'milestoneResult'
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'subprojectId', 
                    'foreignField': '_id', 
                    'as': 'pResult'
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectuniqueId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
                                    {
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
                                            'projectGroup': {
                                                '$concat': [
                                                    '$customer', '-', '$zone', '-', '$costCenter'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0, 
                                            'projectGroup': 1
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$addFields': {
                                'projectGroup': {
                                    '$arrayElemAt': [
                                        '$result.projectGroup', 0
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'projectResult'
                }
            }, {
                '$addFields': {
                    'Site Id': {
                        '$arrayElemAt': [
                            '$siteResult.Site Id', 0
                        ]
                    }, 
                    'Activity': {
                        '$arrayElemAt': [
                            '$siteResult.ACTIVITY', 0
                        ]
                    }, 
                    'SSID': {
                        '$arrayElemAt': [
                            '$siteResult.systemId', 0
                        ]
                    }, 
                    'Unique ID': {
                        '$arrayElemAt': [
                            '$siteResult.Unique ID', 0
                        ]
                    }, 
                    'Milestone': {
                        '$arrayElemAt': [
                            '$milestoneResult.Name', 0
                        ]
                    }, 
                    'Project Type': {
                        '$arrayElemAt': [
                            '$pResult.projectType', 0
                        ]
                    }, 
                    'Sub Project': {
                        '$arrayElemAt': [
                            '$pResult.subProject', 0
                        ]
                    }, 
                    'Project Id': {
                        '$arrayElemAt': [
                            '$projectResult.projectId', 0
                        ]
                    }, 
                    'Project Group': {
                        '$arrayElemAt': [
                            '$projectResult.projectGroup', 0
                        ]
                    }, 
                    'SR Number': '$TemplateData.SR Number', 
                    'L1Age': {
                        '$divide': [
                            {
                                '$subtract': [
                                    {
                                        '$toDate': {
                                            '$ifNull': [
                                                '$L1ActionDate', current_time1()
                                            ]
                                        }
                                    }, {
                                        '$toDate': '$formSubmitDate'
                                    }
                                ]
                            }, 86400000
                        ]
                    }, 
                    'L2Age': {
                        '$divide': [
                            {
                                '$subtract': [
                                    {
                                        '$toDate': {
                                            '$ifNull': [
                                                '$L2ActionDate', current_time1()
                                            ]
                                        }
                                    }, {
                                        '$toDate': '$L1ActionDate'
                                    }
                                ]
                            }, 86400000
                        ]
                    }, 
                    'Form Submission Date': '$formSubmitDate', 
                    'Approval/Rejection Date': '$L1ActionDate', 
                    'Submitted to Airtel Date': '$L2ActionDate', 
                    'Airtel Action Date': '', 
                    'Airtel-Ageing': '', 
                    'Current Status': '$currentStatus'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': [
                            '$$ROOT', '$TemplateData', '$PlanDetailsData', '$SiteDetailsData', '$RanCheckListData', '$AcceptanceLogData'
                        ]
                    }
                }
            }, {
                '$facet': {
                    'outputField1': [
                        {
                            '$project': {
                                'milestoneuid': 0, 
                                'siteuid': 0, 
                                'L1UserId': 0, 
                                'approverType': 0, 
                                'milestoneName': 0, 
                                'projectuniqueId': 0, 
                                'siteIdName': 0, 
                                'subprojectId': 0, 
                                'systemId': 0, 
                                'userId': 0, 
                                'SnapData': 0, 
                                'L2UserId': 0, 
                                'L1ActionDate': 0, 
                                'L2ActionDate': 0, 
                                '_id': 0, 
                                'currentStatus': 0, 
                                'formSubmitDate': 0, 
                                'siteResult': 0, 
                                'milestoneResult': 0, 
                                'pResult': 0, 
                                'projectResult': 0, 
                                'TemplateData': 0, 
                                'PlanDetailsData': 0, 
                                'SiteDetailsData': 0, 
                                'RanCheckListData': 0, 
                                'AcceptanceLogData': 0
                            }
                        }
                    ], 
                    'outputField2': [
                        {
                            '$addFields': {
                                'snapImages': {
                                    '$map': {
                                        'input': {
                                            '$objectToArray': '$SnapData'
                                        }, 
                                        'as': 'snap', 
                                        'in': {
                                            '$cond': {
                                                'if': {
                                                    '$gt': [
                                                        {
                                                            '$size': '$$snap.v.images'
                                                        }, 0
                                                    ]
                                                }, 
                                                'then': {
                                                    '$map': {
                                                        'input': '$$snap.v.images', 
                                                        'as': 'image', 
                                                        'in': {
                                                            '$cond': {
                                                                'if': {
                                                                    '$in': [
                                                                        '$$image.index', '$$snap.v.approvedIndex'
                                                                    ]
                                                                }, 
                                                                'then': {
                                                                    'index': '$$image.index', 
                                                                    'image': '$$image.image', 
                                                                    'fieldName': '$$snap.k'
                                                                }, 
                                                                'else': None
                                                            }
                                                        }
                                                    }
                                                }, 
                                                'else': []
                                            }
                                        }
                                    }
                                }
                            }
                        }, {
                            '$addFields': {
                                'snapImages': {
                                    '$reduce': {
                                        'input': '$snapImages', 
                                        'initialValue': [], 
                                        'in': {
                                            '$concatArrays': [
                                                '$$value', '$$this'
                                            ]
                                        }
                                    }
                                }
                            }
                        }, {
                            '$addFields': {
                                'snapImages': {
                                    '$filter': {
                                        'input': '$snapImages', 
                                        'as': 'img', 
                                        'cond': {
                                            '$ne': [
                                                '$$img', None
                                            ]
                                        }
                                    }
                                }
                            }
                        }, {
                            '$project': {
                                'snapImages': 1, 
                                '_id': 0
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'milestoneuid': 0, 
                    'siteuid': 0, 
                    'L1UserId': 0, 
                    'approverType': 0, 
                    'milestoneName': 0, 
                    'projectuniqueId': 0, 
                    'siteIdName': 0, 
                    'subprojectId': 0, 
                    'systemId': 0, 
                    'userId': 0, 
                    'SnapData': 0, 
                    'L2UserId': 0, 
                    'L1ActionDate': 0, 
                    'L2ActionDate': 0, 
                    '_id': 0, 
                    'currentStatus': 0, 
                    'formSubmitDate': 0, 
                    'siteResult': 0, 
                    'milestoneResult': 0, 
                    'pResult': 0, 
                    'projectResult': 0, 
                    'TemplateData': 0, 
                    'PlanDetailsData': 0, 
                    'SiteDetailsData': 0, 
                    'RanCheckListData': 0, 
                    'AcceptanceLogData': 0
                }
            }
        ]
        fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)
        checklistData = pd.DataFrame(fetchData["data"][0]['outputField1'])
        snapData = fetchData["data"][0]['outputField2'][0]['snapImages']

        # Output Excel path
        fullPath = os.path.join(os.getcwd(), "downloadFile", "Forms_ChecklistL1.xlsx")
        newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")

        # --- Sheet 1: Forms_&_Cheklist ---
        checklistData.to_excel(newExcelWriter, index=False, sheet_name="Forms_&_Cheklist")
        workbook = newExcelWriter.book
        worksheet1 = newExcelWriter.sheets["Forms_&_Cheklist"]
        worksheet1.set_tab_color('#92d050')
        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vtop',
            'bg_color': '#24292d', 
            'border':1,
            'border_color':'black',
            'color':'white'
        })
        cell_format = workbook.add_format({
            'align':'center',
            'valign':'vtop',
        })
        for col_num, value in enumerate(checklistData.columns.values):
            worksheet1.write(0, col_num, value, header_format)
        worksheet1.set_column(0, len(checklistData.columns) - 1, 20, cell_format)

        # --- Sheet 2: Snap Images ---
        worksheet2 = workbook.add_worksheet("Snap Images")
        worksheet2.set_tab_color('#00B0F0')
        if snapData:
            grouped = {}
            for item in snapData:
                field = item['fieldName']
                if field not in grouped:
                    grouped[field] = []
                grouped[field].append(item['image'])

            col_map = {}
            for col_index, field in enumerate(grouped.keys()):
                worksheet2.write(0, col_index, field, header_format)
                col_map[field] = col_index

            for field, images in grouped.items():
                col = col_map[field]
                for row_offset, image_path in enumerate(images, start=1):
                    abs_img_path = os.path.join(os.getcwd(), image_path)  # Adjust relative path
                    if os.path.exists(abs_img_path):
                        try:
                            # Resize large images temporarily if needed
                            img = Image.open(abs_img_path)
                            if img.width > 300 or img.height > 300:
                                img.thumbnail((300, 300))
                                temp_path = os.path.join(os.getcwd(), "temp_thumb.jpg")
                                img.save(temp_path)
                                abs_img_path = temp_path

                            worksheet2.insert_image(row_offset, col, abs_img_path, {
                                'x_offset': 5,
                                'y_offset': 5,
                                'x_scale': 0.5,
                                'y_scale': 0.5
                            })
                        except Exception as e:
                            worksheet2.write(row_offset, col, "Error displaying image")
                    else:
                        worksheet2.write(row_offset, col, "Image not found")

        newExcelWriter.close()
        return send_file(fullPath)




    if reportType == "pdf":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteuid', 
                    'foreignField': '_id', 
                    'as': 'siteResult'
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'milestoneuid', 
                    'foreignField': '_id', 
                    'as': 'milestoneResult'
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'subprojectId', 
                    'foreignField': '_id', 
                    'as': 'pResult'
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectuniqueId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
                                    {
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
                                            'projectGroup': {
                                                '$concat': [
                                                    '$customer', '-', '$zone', '-', '$costCenter'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0, 
                                            'projectGroup': 1
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$addFields': {
                                'projectGroup': {
                                    '$arrayElemAt': [
                                        '$result.projectGroup', 0
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'projectResult'
                }
            }, {
                '$addFields': {
                    'RowData.Site Id': {
                        '$arrayElemAt': [
                            '$siteResult.Site Id', 0
                        ]
                    }, 
                    'RowData.Milestone': {
                        '$arrayElemAt': [
                            '$milestoneResult.Name', 0
                        ]
                    }, 
                    'RowData.SR Number': '$TemplateData.SR Number', 
                    'RowData.Project Group': {
                        '$arrayElemAt': [
                            '$projectResult.projectGroup', 0
                        ]
                    }, 
                    'RowData.Project Type': {
                        '$arrayElemAt': [
                            '$pResult.projectType', 0
                        ]
                    }, 
                    'RowData.Sub Project': {
                        '$arrayElemAt': [
                            '$pResult.subProject', 0
                        ]
                    }, 
                    'RowData.Project ID': {
                        '$arrayElemAt': [
                            '$projectResult.projectId', 0
                        ]
                    }, 
                    'RowData.Activity': {
                        '$arrayElemAt': [
                            '$siteResult.ACTIVITY', 0
                        ]
                    }, 
                    'RowData.SSID': {
                        '$arrayElemAt': [
                            '$siteResult.systemId', 0
                        ]
                    }, 
                    'RowData.TOCO ID': {
                        '$arrayElemAt': [
                            '$siteResult.ACTIVITY', 0
                        ]
                    }, 
                    'RowData.Unique ID': {
                        '$arrayElemAt': [
                            '$siteResult.Unique ID', 0
                        ]
                    }, 
                    'RowData.Form Submission Date': '$formSubmitDate', 
                    'RowData.Approval/Rejection Date': '$L1ActionDate', 
                    'RowData.Submitted to Airtel Date': '$L2ActionDate', 
                    'RowData.Airtel Action Date': '', 
                    'RowData.L1Age': {
                        '$divide': [
                            {
                                '$subtract': [
                                    {
                                        '$toDate': {
                                            '$ifNull': [
                                                '$L1ActionDate', '21-07-2025'
                                            ]
                                        }
                                    }, {
                                        '$toDate': '$formSubmitDate'
                                    }
                                ]
                            }, 86400000
                        ]
                    }, 
                    'RowData.L2Age': {
                        '$divide': [
                            {
                                '$subtract': [
                                    {
                                        '$toDate': {
                                            '$ifNull': [
                                                '$L2ActionDate', '21-07-2025'
                                            ]
                                        }
                                    }, {
                                        '$toDate': '$L1ActionDate'
                                    }
                                ]
                            }, 86400000
                        ]
                    }, 
                    'RowData.Airtel-Ageing': '', 
                    'RowData.Current Status': '$currentStatus'
                }
            }, {
                '$project': {
                    'RowData': 1, 
                    'TemplateData': 1, 
                    'PlanDetailsData': 1, 
                    'SiteDetailsData': 1, 
                    'RanCheckListData': 1, 
                    'AcceptanceLogData': 1, 
                    'SnapData': 1, 
                    '_id': 0
                }
            }
        ]
        fetchData = cmo.finding_aggregate("complianceApproverSaver",arra)['data']
        if fetchData:
            tabData = ["RowData","TemplateData","PlanDetailsData","SiteDetailsData","RanCheckListData","AcceptanceLogData","SnapData"]
            fetchData = fetchData[0]
            if "SnapData" in fetchData:
                snapData = fetchData['SnapData']
                result = {}
                for section, data in snapData.items():
                    result[section] = []
                    approved_indices = data.get("approvedIndex", [])
                    images = data.get("images", [])

                    for img in images:
                        if img["index"] in approved_indices:
                            result[section].append(img["image"])

                fetchData['SnapData'] = result
            return pdf_export(fetchData,tabData)





# @export_blueprint.route("/compliance/export/<reportType>/<id>", methods=["GET"])
# # @token_required
# def DownloadAttachmentcompliance(reportType=None,id=None):
#     if not reportType or reportType not in ['df','Excel']:
#         return respond({"status":400,"msg":"please confirm report type","icon":"error"})
#     aggr=[
#         {
#             '$match': {
#                 '_id': ObjectId(id)
#             }
#         }, {
#             '$lookup': {
#                 'from': 'SiteEngineer', 
#                 'localField': 'siteuid', 
#                 'foreignField': '_id', 
#                 'as': 'siteResult'
#             }
#         }, {
#             '$lookup': {
#                 'from': 'milestone', 
#                 'localField': 'milestoneuid', 
#                 'foreignField': '_id', 
#                 'as': 'milestoneResult'
#             }
#         }, {
#             '$lookup': {
#                 'from': 'projectType', 
#                 'localField': 'subprojectId', 
#                 'foreignField': '_id', 
#                 'as': 'pResult'
#             }
#         }, {
#             '$lookup': {
#                 'from': 'project', 
#                 'localField': 'projectuniqueId', 
#                 'foreignField': '_id', 
#                 'pipeline': [
#                     {
#                         '$lookup': {
#                             'from': 'projectGroup', 
#                             'localField': 'projectGroup', 
#                             'foreignField': '_id', 
#                             'pipeline': [
#                                 {
#                                     '$lookup': {
#                                         'from': 'customer', 
#                                         'localField': 'customerId', 
#                                         'foreignField': '_id', 
#                                         'pipeline': [
#                                             {
#                                                 '$match': {
#                                                     'deleteStatus': {
#                                                         '$ne': 1
#                                                     }
#                                                 }
#                                             }
#                                         ], 
#                                         'as': 'customer'
#                                     }
#                                 }, {
#                                     '$unwind': {
#                                         'path': '$customer', 
#                                         'preserveNullAndEmptyArrays': True
#                                     }
#                                 }, {
#                                     '$lookup': {
#                                         'from': 'zone', 
#                                         'localField': 'zoneId', 
#                                         'foreignField': '_id', 
#                                         'pipeline': [
#                                             {
#                                                 '$match': {
#                                                     'deleteStatus': {
#                                                         '$ne': 1
#                                                     }
#                                                 }
#                                             }
#                                         ], 
#                                         'as': 'zone'
#                                     }
#                                 }, {
#                                     '$unwind': {
#                                         'path': '$zone', 
#                                         'preserveNullAndEmptyArrays': True
#                                     }
#                                 }, {
#                                     '$lookup': {
#                                         'from': 'costCenter', 
#                                         'localField': 'costCenterId', 
#                                         'foreignField': '_id', 
#                                         'pipeline': [
#                                             {
#                                                 '$match': {
#                                                     'deleteStatus': {
#                                                         '$ne': 1
#                                                     }
#                                                 }
#                                             }
#                                         ], 
#                                         'as': 'costCenter'
#                                     }
#                                 }, {
#                                     '$unwind': {
#                                         'path': '$costCenter', 
#                                         'preserveNullAndEmptyArrays': True
#                                     }
#                                 }, {
#                                     '$addFields': {
#                                         'costCenter': '$costCenter.costCenter', 
#                                         'zone': '$zone.shortCode', 
#                                         'customer': '$customer.shortName'
#                                     }
#                                 }, {
#                                     '$addFields': {
#                                         'projectGroup': {
#                                             '$concat': [
#                                                 '$customer', '-', '$zone', '-', '$costCenter'
#                                             ]
#                                         }
#                                     }
#                                 }, {
#                                     '$project': {
#                                         '_id': 0, 
#                                         'projectGroup': 1
#                                     }
#                                 }
#                             ], 
#                             'as': 'result'
#                         }
#                     }, {
#                         '$addFields': {
#                             'projectGroup': {
#                                 '$arrayElemAt': [
#                                     '$result.projectGroup', 0
#                                 ]
#                             }
#                         }
#                     }
#                 ], 
#                 'as': 'projectResult'
#             }
#         }, {
#             '$addFields': {
#                 'Site Id': {
#                     '$arrayElemAt': [
#                         '$siteResult.Site Id', 0
#                     ]
#                 }, 
#                 'Activity': {
#                     '$arrayElemAt': [
#                         '$siteResult.ACTIVITY', 0
#                     ]
#                 }, 
#                 'SSID': {
#                     '$arrayElemAt': [
#                         '$siteResult.systemId', 0
#                     ]
#                 }, 
#                 'Unique ID': {
#                     '$arrayElemAt': [
#                         '$siteResult.Unique ID', 0
#                     ]
#                 }, 
#                 'Milestone': {
#                     '$arrayElemAt': [
#                         '$milestoneResult.Name', 0
#                     ]
#                 }, 
#                 'Project Type': {
#                     '$arrayElemAt': [
#                         '$pResult.projectType', 0
#                     ]
#                 }, 
#                 'Sub Project': {
#                     '$arrayElemAt': [
#                         '$pResult.subProject', 0
#                     ]
#                 }, 
#                 'Project Id': {
#                     '$arrayElemAt': [
#                         '$projectResult.projectId', 0
#                     ]
#                 }, 
#                 'Project Group': {
#                     '$arrayElemAt': [
#                         '$projectResult.projectGroup', 0
#                     ]
#                 }, 
#                 'SR Number': '$TemplateData.SR Number', 
#                 'L1Age': {
#                     '$divide': [
#                         {
#                             '$subtract': [
#                                 {
#                                     '$toDate': {
#                                         '$ifNull': [
#                                             '$L1ActionDate', current_time1()
#                                         ]
#                                     }
#                                 }, {
#                                     '$toDate': '$formSubmitDate'
#                                 }
#                             ]
#                         }, 86400000
#                     ]
#                 }, 
#                 'L2Age': {
#                     '$divide': [
#                         {
#                             '$subtract': [
#                                 {
#                                     '$toDate': {
#                                         '$ifNull': [
#                                             '$L2ActionDate', current_time1()
#                                         ]
#                                     }
#                                 }, {
#                                     '$toDate': '$L1ActionDate'
#                                 }
#                             ]
#                         }, 86400000
#                     ]
#                 }, 
#                 'Form Submission Date': '$formSubmitDate', 
#                 'Approval/Rejection Date': '$L1ActionDate', 
#                 'Submitted to Airtel Date': '$L2ActionDate', 
#                 'Airtel Action Date': '', 
#                 'Airtel-Ageing': '', 
#                 'Current Status': '$currentStatus'
#             }
#         }, {
#             '$replaceRoot': {
#                 'newRoot': {
#                     '$mergeObjects': [
#                         '$$ROOT', '$TemplateData', '$PlanDetailsData', '$SiteDetailsData', '$RanCheckListData', '$AcceptanceLogData'
#                     ]
#                 }
#             }
#         }, {
#             '$facet': {
#                 'outputField1': [
#                     {
#                         '$project': {
#                             'milestoneuid': 0, 
#                             'siteuid': 0, 
#                             'L1UserId': 0, 
#                             'approverType': 0, 
#                             'milestoneName': 0, 
#                             'projectuniqueId': 0, 
#                             'siteIdName': 0, 
#                             'subprojectId': 0, 
#                             'systemId': 0, 
#                             'userId': 0, 
#                             'SnapData': 0, 
#                             'L2UserId': 0, 
#                             'L1ActionDate': 0, 
#                             'L2ActionDate': 0, 
#                             '_id': 0, 
#                             'currentStatus': 0, 
#                             'formSubmitDate': 0, 
#                             'siteResult': 0, 
#                             'milestoneResult': 0, 
#                             'pResult': 0, 
#                             'projectResult': 0, 
#                             'TemplateData': 0, 
#                             'PlanDetailsData': 0, 
#                             'SiteDetailsData': 0, 
#                             'RanCheckListData': 0, 
#                             'AcceptanceLogData': 0
#                         }
#                     }
#                 ], 
#                 'outputField2': [
#                     {
#                         '$addFields': {
#                             'snapImages': {
#                                 '$map': {
#                                     'input': {
#                                         '$objectToArray': '$SnapData'
#                                     }, 
#                                     'as': 'snap', 
#                                     'in': {
#                                         '$cond': {
#                                             'if': {
#                                                 '$gt': [
#                                                     {
#                                                         '$size': '$$snap.v.images'
#                                                     }, 0
#                                                 ]
#                                             }, 
#                                             'then': {
#                                                 '$map': {
#                                                     'input': '$$snap.v.images', 
#                                                     'as': 'image', 
#                                                     'in': {
#                                                         '$cond': {
#                                                             'if': {
#                                                                 '$in': [
#                                                                     '$$image.index', '$$snap.v.approvedIndex'
#                                                                 ]
#                                                             }, 
#                                                             'then': {
#                                                                 'index': '$$image.index', 
#                                                                 'image': '$$image.image', 
#                                                                 'fieldName': '$$snap.k'
#                                                             }, 
#                                                             'else': None
#                                                         }
#                                                     }
#                                                 }
#                                             }, 
#                                             'else': []
#                                         }
#                                     }
#                                 }
#                             }
#                         }
#                     }, {
#                         '$addFields': {
#                             'snapImages': {
#                                 '$reduce': {
#                                     'input': '$snapImages', 
#                                     'initialValue': [], 
#                                     'in': {
#                                         '$concatArrays': [
#                                             '$$value', '$$this'
#                                         ]
#                                     }
#                                 }
#                             }
#                         }
#                     }, {
#                         '$addFields': {
#                             'snapImages': {
#                                 '$filter': {
#                                     'input': '$snapImages', 
#                                     'as': 'img', 
#                                     'cond': {
#                                         '$ne': [
#                                             '$$img', None
#                                         ]
#                                     }
#                                 }
#                             }
#                         }
#                     }, {
#                         '$project': {
#                             'snapImages': 1, 
#                             '_id': 0
#                         }
#                     }
#                 ]
#             }
#         }, {
#             '$project': {
#                 'milestoneuid': 0, 
#                 'siteuid': 0, 
#                 'L1UserId': 0, 
#                 'approverType': 0, 
#                 'milestoneName': 0, 
#                 'projectuniqueId': 0, 
#                 'siteIdName': 0, 
#                 'subprojectId': 0, 
#                 'systemId': 0, 
#                 'userId': 0, 
#                 'SnapData': 0, 
#                 'L2UserId': 0, 
#                 'L1ActionDate': 0, 
#                 'L2ActionDate': 0, 
#                 '_id': 0, 
#                 'currentStatus': 0, 
#                 'formSubmitDate': 0, 
#                 'siteResult': 0, 
#                 'milestoneResult': 0, 
#                 'pResult': 0, 
#                 'projectResult': 0, 
#                 'TemplateData': 0, 
#                 'PlanDetailsData': 0, 
#                 'SiteDetailsData': 0, 
#                 'RanCheckListData': 0, 
#                 'AcceptanceLogData': 0
#             }
#         }
#     ]
#     fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)
#     checklistData = pd.DataFrame(fetchData["data"][0]['outputField1'])
#     snapData = pd.DataFrame(fetchData["data"][0]['outputField2'])
#     dataframes = [checklistData,snapData]
#     sheet_names = ['Forms_&_Cheklist',"Snap"]
#     tab_colors = ['#92d050',"#00B0F0"]
    
#     fullPath = os.path.join(os.getcwd(), "downloadFile", "Forms_Checklist.xlsx")
#     newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
#     checklistData.to_excel(newExcelWriter, index=False, sheet_name="Forms_&_Cheklist")
#     workbook = newExcelWriter.book
#     worksheet = newExcelWriter.sheets["Forms_&_Cheklist"]
#     worksheet.set_tab_color('#92d050')
#     header_format = workbook.add_format({
#         'bold': True,
#         'align': 'center',
#         'valign': 'vtop',
#         'bg_color': '#24292d', 
#         'border':1,
#         'border_color':'black',
#         'color':'white'
#     })
#     cell_format = workbook.add_format({
#         'align':'center',
#         'valign':'vtop',
#     })
#     for col_num, value in enumerate(checklistData.columns.values):
#         worksheet.write(0, col_num, value, header_format)
#     worksheet.set_column(0, len(checklistData.columns) - 1, 20, cell_format)
#     newExcelWriter.close()
#     return send_file(fullPath)
   
#     # if not len(fetchData['data']):
#     #     return respond({"status":204,"msg":"data not found","icon":"error"})
#     # fileName = "Compliance Approver"
#     # if reportType == "Excel":
#     #     sheetName = "Compliance Approved"
#     #     sheetName1 = "Snap"
#     #     dataframe = pd.DataFrame(fetchData['data'])
#     #     fullPath = os.path.join(os.getcwd(), "downloadFile",fileName+".xlsx")
#     #     newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
#     #     dataframe.to_excel(newExcelWriter, index=False, sheet_name=sheetName)
        
        

#     #     workbook = newExcelWriter.book
#     #     worksheet = newExcelWriter.sheets[sheetName]
#     #     worksheet.set_tab_color('#FFFF00')

#     #     header_format = workbook.add_format({
#     #         'bold': True,
#     #         'align': 'center',
#     #         'valign': 'vtop',
#     #         'bg_color': '#24292d', 
#     #         'border':1,
#     #         'border_color':'black',
#     #         'color':'white'
#     #     })
#     #     for col_num, value in enumerate(dataframe.columns.values):
#     #         worksheet.write(0, col_num, value, header_format)
        
#     #     cell_format = workbook.add_format({
#     #         'align':'center',
#     #         'valign':'vtop',
#     #     })

#     #     worksheet.set_column(0, len(dataframe.columns) - 1, 20, cell_format)
#     #     worksheet_snap = newExcelWriter.book.add_worksheet(sheetName1)
    
        
#     #     worksheet_snap.write_row(0, 0, dataframe.columns.values)

        
        
#     #     row = 1  # Start from the second row (A2, B2, etc.)
#     #     col = 0  # Start inserting images from column A
        
#     #     for item in fetchData['data'][0]['snapImages']:
#     #         # Insert each image into consecutive rows
#     #         worksheet_snap.insert_image(row, col, os.path.join(os.getcwd(),item['image']))
#     #         row += 5
#     #     newExcelWriter.close()
#     #     return send_file(fullPath)
#         # return fullPath




#     if reportType == "Pdf":
#         pass


@export_blueprint.route("/export/ptw/<reportType>/<id>", methods=["GET"])
# @token_required
def DownloadAttachmentPtw(reportType = None , id=None):

    aggr=[
    {
        '$match': {
            '_id': ObjectId(id)
        }
    }, {
        '$project': {
            'fileType': 0, 
            'createdAt': 0, 
            '_id': 0, 
            'creatorId': 0
        }
    }
]
    originalData = {
        "checklist" : 'Checklist',
        "photo" : "Photo",
        "riskassessment" : "Risk Assessment",
        "ptwphoto" : "PTW Photo",
        "teamdetails" : "Team Details",
        "roadsafetychecklist2wheeler":"Road Safety Checklist 2-Wheeler",
        "roadsafetychecklist4wheeler": "Road Safety Checklist 4-Wheeler",
        "ptwphoto2wheeler":"Ptw Photo 2-Wheeler",
        "ptwphoto4wheeler":"Ptw Photo 4-Wheeler",
        "rejectionreason":"Rejection Reason"
        }
    response = cmo.finding_aggregate("ptwProjectType", aggr)
    
    response = response['data'][0]

    if not len(response.keys()):
        return jsonify({"message": "No data found"}), 404
    fullPath = os.path.join(os.getcwd(), "downloadFile", "Export_Ptw_Form.xlsx")
    newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
    workbook = newExcelWriter.book
    dataframe = {}
    worksheets={}
    color = [
        "#92d050",
        "#00B0F0",
        "#FFFF00",
        "#FF0000",
       "#e2e2e2",
       "#117C44",
       "#1d0ba2",
        "#ED080F",
       "#700a3b"
    ]
    desired_order = ["fieldName", "dataType", "Status", "required", "dropdownValue"]
    column_rename = {
        "fieldName": "FIELD NAME",
        "dataType": "DATA TYPE",
        "Status": "STATUS",
        "required": "REQUIRED",
        "dropdownValue": "DROPDOWN VALUE"
    }
    
    for i , key in enumerate(response.keys()):
        df = pd.DataFrame(response[key])
        df = df[[col for col in desired_order if col in df.columns]]

        df.rename(columns=column_rename, inplace=True)

        # df.columns = [col.upper() for col in df.columns]
        df.to_excel(newExcelWriter, index=False, sheet_name=originalData[key])
        dataframe[f"dataframe_{i}"]=df
        worksheet = newExcelWriter.sheets[originalData[key]]
        worksheets[f"worksheet_{i}"]=  worksheet
    
    for i , key in enumerate(worksheets.keys()):
        worksheets[key].set_tab_color(color[i])
    
    header_format = workbook.add_format(
                {
                    "bold": True,
                    "align": "center",
                    "valign": "vtop",
                    "bg_color": "#143b64",
                    "border": 1,
                    "border_color": "black",
                    "color": "white",
                }
            )
    
    for i , key in enumerate(dataframe.keys()):
        for col_num, value in enumerate(dataframe[key].columns.values):
            worksheets[f"worksheet_{i}"].write(0, col_num, value, header_format)
    newExcelWriter.close()
 
    return send_file(fullPath, as_attachment=True, download_name="Export_Ptw_Form.xlsx") 
        
        
        
   
    return send_file(fullPath, as_attachment=True)

# ======================================PTW export 
@export_blueprint.route("/ptwTableExport", methods=["GET"])
@token_required
def export_ptw_table(current_user):
    
    exportTableName  = request.args.get("exportTableName")
    startDate = request.args.get("startDate")
    endDate = request.args.get("endDate")

    if(exportTableName=="ptwLogBackup"):
            aggregateData=[
                {
                    '$addFields': {
                        'ssid': '$checklist.SSID', 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'currentStatus': '$status', 
                        'user': {
                            '$toObjectId': '$createdBy'
                        }, 
                        'l1Id': {
                            '$toObjectId': '$L1-Aprover.approverId'
                        }, 
                        'milestoneUniqueId': {
                            '$toObjectId': '$mileStoneId'
                        }, 
                        'projectType': '$checklist.Project type', 
                        'activity': '$checklist.Activity', 
                        'ptwSubmissionDate': {
                            '$dateToString': {
                                'format': '%m/%d/%Y %H:%M', 
                                'date': {
                                    '$toDate': '$ptwRaiseAt'
                                }
                            }
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'ptwMdb', 
                        'let': {
                            'projectType': '$checklist.Project type', 
                            'l1Id': '$l1Id'
                        }, 
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$projectTypeName', '$$projectType'
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$employee', '$$l1Id'
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        ], 
                        'as': 'projectGroupResult'
                    }
                }, {
                    '$addFields': {
                        'projectGroup': {
                            '$arrayElemAt': [
                                '$projectGroupResults.projectGroup', 0
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'projectGroup', 
                        'localField': 'projectGroup', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
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
                                    'customer': '$customer.shortName', 
                                    'Customer': '$customer.customerName'
                                }
                            }, {
                                '$addFields': {
                                    'projectGroupName': {
                                        '$concat': [
                                            '$customer', '-', '$zone', '-', '$costCenter'
                                        ]
                                    }, 
                                    'projectGroup': {
                                        '$toString': '$_id'
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'projectGroupName': 1, 
                                    'projectGroup': 1, 
                                    'Customer': 1, 
                                    'costCenter': 1
                                }
                            }
                        ], 
                        'as': 'projectGroupResults'
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'user', 
                        'foreignField': '_id', 
                        'as': 'userResult'
                    }
                }, {
                    '$lookup': {
                        'from': 'milestone', 
                        'localField': 'milestoneUniqueId', 
                        'foreignField': '_id', 
                        'as': 'milestoneResult'
                    }
                }, {
                    '$addFields': {
                        'ptwRequester': {
                            '$arrayElemAt': [
                                '$userResult.empName', 0
                            ]
                        }, 
                        'milestoneName': {
                            '$arrayElemAt': [
                                '$milestoneResult.Name', 0
                            ]
                        }, 
                        'projectGroupName': {
                            '$arrayElemAt': [
                                '$projectGroupResult.projectTypeName', 0
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'actionAtFinal': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                {
                                                    '$ifNull': [
                                                        '$L2-Aprover.actionAt', None
                                                    ]
                                                }, None
                                            ]
                                        }, {
                                            '$eq': [
                                                {
                                                    '$ifNull': [
                                                        '$L1-Aprover.actionAt', None
                                                    ]
                                                }, None
                                            ]
                                        }
                                    ]
                                }, None, {
                                    '$cond': [
                                        {
                                            '$ne': [
                                                {
                                                    '$ifNull': [
                                                        '$L2-Aprover.actionAt', None
                                                    ]
                                                }, None
                                            ]
                                        }, '$L2-Aprover.actionAt', {
                                            '$cond': [
                                                {
                                                    '$ne': [
                                                        {
                                                            '$ifNull': [
                                                                '$L1-Aprover.actionAt', None
                                                            ]
                                                        }, None
                                                    ]
                                                }, '$L1-Aprover.actionAt', None
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'approvedOrRejectionDate': {
                            '$dateToString': {
                                'format': '%m/%d/%Y %H:%M', 
                                'date': {
                                    '$toDate': '$actionAtFinal'
                                }
                            }
                        }
                    }
                }, {
                    '$project': {
                        'Ptw Number': '$ptwNumber', 
                        'Complition Date': '$complitionDate', 
                        'Milestone': 1, 
                        'Site Id': '$siteId', 
                        'SSID': '$ssId', 
                        'Unique Id': '$uniqueId', 
                        'Sr Number': '$srNumber', 
                        'Project Group': '$projectGroupName', 
                        'Project Id': '$projectID', 
                        'Project Type': '$projectType', 
                        'Sub Project': 'subProject', 
                        'Submission Date': '$submissionDate', 
                        'Action Date': '$actionDate', 
                        'L2 Action Date': '$l2ActionDAte', 
                        'L1 Aging': '$l1Ageing', 
                        'L2 Aging': '$l2Ageing', 
                        'Status': '$status',
                        # 'ptwNumber': 1, 
                        # 'ptwRequester': 1, 
                        # 'milestoneName': 1, 
                        # 'siteId': 1, 
                        # 'ssid': 1, 
                        # 'uniqueId': 1, 
                        # 'projectGroupName': 1, 
                        # 'projectID': 1, 
                        # 'projectType': 1, 
                        # 'subProject': 1, 
                        # 'activity': 1, 
                        # 'ptwSubmissionDate': 1, 
                        # 'approvedOrRejectionDate': 1, 
                        # 'currentStatus': 1, 
                        '_id': 0
                    }
                }
            ]

            response = cmo.finding_aggregate("ptwRaiseTicket",aggregateData)
    
    if exportTableName=="L1ApproverExport":
        arra = [
            {
                '$match': {
                    'status': 'Submitted',
                    "L1-Approver.approverId":current_user['userUniqueId']
                }
            }, {
                '$addFields': {
                    'actionAt': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$isL1Rejected', True
                                ]
                            }, 
                            'then': '$L1-Approver.actionAt', 
                            'else': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$isL2Rejected', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt', 
                                    'else': {
                                        '$cond': {
                                            'if': {
                                                '$eq': [
                                                    '$isL2Approved', True
                                                ]
                                            }, 
                                            'then': '$L2-Approver.actionAt', 
                                            'else': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            '$isL1Approved', True
                                                        ]
                                                    }, 
                                                    'then': '$L1-Approver.actionAt', 
                                                    'else': ''
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }, 
                    
                    'l1ActionDAte': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$isL1Rejected', True
                                        ]
                                    }, 
                                    'then': '$L1-Approver.actionAt'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$isL1Approved', True
                                        ]
                                    }, 
                                    'then': '$L1-Approver.actionAt'
                                }
                            ], 
                            'default': ''
                        }
                    }, 
                    'l2ActionDAte': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$isL2Rejected', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$isL2Approved', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt'
                                }
                            ], 
                            'default': ''
                        }
                    }, 
                    'l1Ageing': {
                        '$dateDiff': {
                            'startDate': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'endDate':  {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$eq': [
                                                    '$isL1Approved', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L1-Approver.actionAt'
                                            }
                                        }, {
                                            'case': {
                                                '$eq': [
                                                    '$isL1Rejected', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L1-Approver.actionAt'
                                            }
                                        }
                                    ], 
                                    'default': {
                                        '$toDate': get_current_date_timestamp()
                                    }
                                }
                            },
                            'unit': 'day'
                        }
                    }, 
                    'l2Ageing': {
                        '$dateDiff': {
                            'startDate': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'endDate': {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$eq': [
                                                    '$isL2Approved', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L2-Approver.actionAt'
                                            }
                                        }, {
                                            'case': {
                                                '$eq': [
                                                    '$isL2Rejected', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L2-Approver.actionAt'
                                            }
                                        }
                                    ], 
                                    'default': {
                                        '$toDate': get_current_date_timestamp()
                                    }
                                }
                            }, 
                            'unit': 'day'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'createdBy': {
                        '$toObjectId': '$createdBy'
                    }, 
                    'ssId': '$checklist.SSID', 
                    'uniqueId': '$checklist.Unique ID', 
                    'srNumber': '', 
                    'projectType': '$checklist.Project type', 
                    'mileStoneId': {
                        '$toObjectId': '$mileStoneId'
                    }, 
                    'activity': '$checklist.Activity', 
                    'submissionDate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y %H:%M', 
                            'date': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'actionDate': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$actionAt', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$actionAt'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'l1ActionDAte': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$l1ActionDAte', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$l1ActionDAte'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'l2ActionDAte': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$l2ActionDAte', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$l2ActionDAte'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectID', 
                    'foreignField': 'projectId', 
                    'pipeline': [
                        {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
                                    {
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
                                            'projectGroupName': {
                                                '$concat': [
                                                    '$customer.customerName', '-', '$zone.shortCode', '-', '$costCenter.costCenter'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'projectGroupName': 1, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'projectGroupData'
                            }
                        }, {
                            '$unwind': '$projectGroupData'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$projectGroupData'
                            }
                        }
                    ], 
                    'as': 'projectGroupName'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'createdBy', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$addFields': {
                                'ptwRequester': {
                                    '$concat': [
                                        '$empName', '(', '$empCode', ')'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'empName': 1, 
                                'ptwRequester': 1
                            }
                        }
                    ], 
                    'as': 'createdByData'
                }
            }, {
                '$addFields': {
                    'projectGroupName': {
                        '$arrayElemAt': [
                            '$projectGroupName.projectGroupName', 0
                        ]
                    }, 
                    'createdBy': {
                        '$arrayElemAt': [
                            '$createdByData.ptwRequester', 0
                        ]
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'mileStoneId': {
                        '$toString': '$mileStoneId'
                    }, 
                    'complitionDate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y %H:%M', 
                            'date': {
                                '$toDate': '$closeAt'
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$project': {
                        '_id': 0, 
                        'Ptw Number': '$ptwNumber', 
                        'Complition Date': '$complitionDate', 
                        'Milestone': 1, 
                        'Site Id': '$siteId', 
                        'SSID': '$ssId', 
                        'Unique Id': '$uniqueId', 
                        'Sr Number': '$srNumber', 
                        'Project Group': '$projectGroupName', 
                        'Project Id': '$projectID', 
                        'Project Type': '$projectType', 
                        'Sub Project': 'subProject', 
                        'Submission Date': '$submissionDate', 
                        'Action Date': '$actionDate', 
                        'L2 Action Date': '$l2ActionDAte', 
                        'L1 Aging': '$l1Ageing', 
                        'L2 Aging': '$l2Ageing', 
                        'Status': '$status',
                    # 'ptwNumber': 1, 
                    # 'complitionDate': 1, 
                    # 'Milestone': 1, 
                    # 'siteId': 1, 
                    # 'ssId': 1, 
                    # 'uniqueId': 1, 
                    # 'srNumber': 1, 
                    # 'projectGroupName': 1, 
                    # 'projectID': 1, 
                    # 'projectType': 1, 
                    # 'subProject': 1, 
                    # 'submissionDate': 1, 
                    # 'actionDate': 1, 
                    # 'l2ActionDate': '$l2ActionDAte', 
                    # 'l1Ageing': 1, 
                    # 'l2Ageing': 1, 
                    # 'status': 1
                }
            }
        ]
        if startDate != None and endDate  != None:
            finalAgg=dateFilter(startDate,endDate,arra)
        finalAgg=arra

        response = cmo.finding_aggregate("ptwRaiseTicket", arra)
    
    if exportTableName=="L2ApproverExport":
        arra = [
            {
                '$match': {
                    'isL1Approved': True, 
                    'status': 'L1-Approved',
                    "L2-Approver.approverId":current_user['userUniqueId']
                }
            }, {
                '$addFields': {
                    'actionAt': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$isL1Rejected', True
                                ]
                            }, 
                            'then': '$L1-Approver.actionAt', 
                            'else': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$isL2Rejected', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt', 
                                    'else': {
                                        '$cond': {
                                            'if': {
                                                '$eq': [
                                                    '$isL2Approved', True
                                                ]
                                            }, 
                                            'then': '$L2-Approver.actionAt', 
                                            'else': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            '$isL1Approved', True
                                                        ]
                                                    }, 
                                                    'then': '$L1-Approver.actionAt', 
                                                    'else': ''
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }, 
                    'l1ActionDAte': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$isL1Rejected', True
                                        ]
                                    }, 
                                    'then': '$L1-Approver.actionAt'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$isL1Approved', True
                                        ]
                                    }, 
                                    'then': '$L1-Approver.actionAt'
                                }
                            ], 
                            'default': ''
                        }
                    }, 
                    'l2ActionDAte': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$isL2Rejected', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$isL2Approved', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt'
                                }
                            ], 
                            'default': ''
                        }
                    }, 
                    'l1Ageing': {
                        '$dateDiff': {
                            'startDate': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'endDate':  {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$eq': [
                                                    '$isL1Approved', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L1-Approver.actionAt'
                                            }
                                        }, {
                                            'case': {
                                                '$eq': [
                                                    '$isL1Rejected', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L1-Approver.actionAt'
                                            }
                                        }
                                    ], 
                                    'default': {
                                        '$toDate': get_current_date_timestamp()
                                    }
                                }
                            },
                            'unit': 'day'
                        }
                    }, 
                    'l2Ageing': {
                        '$dateDiff': {
                            'startDate': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'endDate': {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$eq': [
                                                    '$isL2Approved', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L2-Approver.actionAt'
                                            }
                                        }, {
                                            'case': {
                                                '$eq': [
                                                    '$isL2Rejected', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L2-Approver.actionAt'
                                            }
                                        }
                                    ], 
                                    'default': {
                                        '$toDate': get_current_date_timestamp()
                                    }
                                }
                            }, 
                            'unit': 'day'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'createdBy': {
                        '$toObjectId': '$createdBy'
                    }, 
                    'ssId': '$checklist.SSID', 
                    'uniqueId': '$checklist.Unique ID', 
                    'srNumber': '', 
                    'projectType': '$checklist.Project type', 
                    'mileStoneId': {
                        '$toObjectId': '$mileStoneId'
                    }, 
                    'activity': '$checklist.Activity', 
                    'submissionDate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y %H:%M', 
                            'date': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'actionDate': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$actionAt', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$actionAt'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'l1ActionDAte': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$l1ActionDAte', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$l1ActionDAte'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'l2ActionDAte': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$l2ActionDAte', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$l2ActionDAte'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectID', 
                    'foreignField': 'projectId', 
                    'pipeline': [
                        {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': 'projectGroup', 
                                'foreignField': '_id', 
                                'pipeline': [
                                    {
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
                                            'projectGroupName': {
                                                '$concat': [
                                                    '$customer.customerName', '-', '$zone.shortCode', '-', '$costCenter.costCenter'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'projectGroupName': 1, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'projectGroupData'
                            }
                        }, {
                            '$unwind': '$projectGroupData'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$projectGroupData'
                            }
                        }
                    ], 
                    'as': 'projectGroupName'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'createdBy', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$addFields': {
                                'ptwRequester': {
                                    '$concat': [
                                        '$empName', '(', '$empCode', ')'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'empName': 1, 
                                'ptwRequester': 1
                            }
                        }
                    ], 
                    'as': 'createdByData'
                }
            }, {
                '$addFields': {
                    'projectGroupName': {
                        '$arrayElemAt': [
                            '$projectGroupName.projectGroupName', 0
                        ]
                    }, 
                    'createdBy': {
                        '$arrayElemAt': [
                            '$createdByData.ptwRequester', 0
                        ]
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'mileStoneId': {
                        '$toString': '$mileStoneId'
                    }, 
                    'complitionDate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y %H:%M', 
                            'date': {
                                '$toDate': '$closeAt'
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$project': {
                        '_id': 0, 
                        'Ptw Number': '$ptwNumber', 
                        'Complition Date': '$complitionDate', 
                        'Milestone': 1, 
                        'Site Id': '$siteId', 
                        'SSID': '$ssId', 
                        'Unique Id': '$uniqueId', 
                        'Sr Number': '$srNumber', 
                        'Project Group': '$projectGroupName', 
                        'Project Id': '$projectID', 
                        'Project Type': '$projectType', 
                        'Sub Project': 'subProject', 
                        'Submission Date': '$submissionDate', 
                        'Action Date': '$actionDate', 
                        'L2 Action Date': '$l2ActionDAte', 
                        'L1 Aging': '$l1Ageing', 
                        'L2 Aging': '$l2Ageing', 
                        'Status': '$status',
                    # 'ptwNumber': 1, 
                    # 'complitionDate': 1, 
                    # 'Milestone': 1, 
                    # 'siteId': 1, 
                    # 'ssId': 1, 
                    # 'uniqueId': 1, 
                    # 'srNumber': 1, 
                    # 'projectGroupName': 1, 
                    # 'projectID': 1, 
                    # 'projectType': 1, 
                    # 'subProject': 1, 
                    # 'submissionDate': 1, 
                    # 'actionDate': 1, 
                    # 'l2ActionDate': '$l2ActionDAte', 
                    # 'l1Ageing': 1, 
                    # 'l2Ageing': 1, 
                    # 'status': 1
                }
            }
        ]
        finalAgg=arra
        if startDate != None and endDate  != None:
            finalAgg=dateFilter(startDate,endDate,arra)

        response = cmo.finding_aggregate("ptwRaiseTicket", finalAgg)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    dataframe.columns = [col.upper() for col in dataframe.columns]
    # dataframe.columns = [col for col in dataframe.columns]
    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_PTW_Log_Backup", "PTW_Log_Backup")
    return send_file(fullPath)




# ======================================PTW Logs Exporet
@export_blueprint.route("/ptwLogsExport", methods=["GET","POST"])
def PTW_Logs_Export():
    argsData = request.args.to_dict()
    if argsData.get("mileStoneId") and argsData.get("logsCollection"):
    
            aggregateData=[
                {
                    '$match': {
                        'mileStoneId':argsData.get("mileStoneId")
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'actionBy', 
                        'foreignField': '_id', 
                        'as': 'result'
                    }
                }, {
                    '$addFields': {
                        'timeStamp': {
                            '$toDate': {
                                '$toLong': '$actionAt'
                            }
                        }, 
                        'Time': {
                            '$dateToString': {
                                'format': '%m/%d/%Y %H:%M', 
                                'date': {
                                    '$toDate': {
                                        '$toLong': '$actionAt'
                                    }
                                }, 
                                'timezone': 'Asia/Kolkata'
                            }
                        }, 
                        'Action By': {
                            '$arrayElemAt': [
                                '$result.empName', 0
                            ]
                        }, 
                        'Action Type': '$actionType', 
                        'Action': '$action', 
                        'Module': '$module'
                    }
                }, {
                    '$project': {
                        'Action By': 1, 
                        'Action Type': 1, 
                        'Time': 1, 
                        'Action': 1, 
                        'Module': 1, 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate(argsData["logsCollection"], aggregateData)
            response = response["data"]
            dataframe = pd.DataFrame(response)
            dataframe.columns = [col.upper() for col in dataframe.columns]
            fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_PTW_Log_Backup", "PTW_Log_Backup")
            return send_file(fullPath)
    else:
        return respond({
            'state':4,
            'status':400,
            'data':[],
            'msg':'Milestone ID or Collection is missing'
        })
    
    
    
# new api added by giriraj
@export_blueprint.route("/getOneCompliance/templateExport/<id>/<mName>/<tabType>", methods=["GET"])
@token_required
def compliance_template_export(current_user,id=None,mName=None,tabType=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$project': {
                    'SubProjectId': 1,
                    'ACTIVITY': 1,
                    'OEM NAME': 1
                }
            }, {
                '$addFields': {
                    'SubProjectId': {
                        '$toObjectId': '$SubProjectId'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'complianceForm',
                    'let': {
                        'subProjectId': '$SubProjectId',
                        'activity':{
                            '$trim':{'input':'$ACTIVITY'}
                        } ,
                        'oem': {
                            "$trim":{"input":'$OEM NAME'}
                        },
                        'mName': mName.strip()
                    },
                    'pipeline': [
                        {
                            '$match':{
                                'deleteStatus':{'$ne':1}
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$subProject', '$$subProjectId'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$activity', '$$activity'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$oem', '$$oem'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$complianceMilestone', '$$mName'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                tabType:1,
                                '_id': 0
                            }
                        }
                    ],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    '_id': 0,
                    tabType: {
                        '$ifNull': [
                            f"$result.{tabType}", []
                        ]
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer", arra)['data']
        if response:
            response = response[0]
            headers = []
            for i in response[tabType]:
                if i['fieldName'] not in headers:
                    headers.append(i['fieldName'])
            df = pd.DataFrame({'Field Name': headers})
            fullPath = excelWriteFunc.excelFileWriter(df, "Export_Template", "Template")
            return send_file(fullPath)
        else:
            df = pd.DataFrame()
            fullPath = excelWriteFunc.excelFileWriter(df, "Export_Template", "Template")
            return send_file(fullPath)