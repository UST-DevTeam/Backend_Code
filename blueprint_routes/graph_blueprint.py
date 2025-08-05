from base import *
from collections import defaultdict
from common.config import tempfunction as tempfunction
from common.config import convertToDateBulkExport as convertToDateBulkExport
from common.config import get_financial_year as get_financial_year
from blueprint_routes.currentuser_blueprint import projectId_str,projectGroup_str,projectId_Object,costCenter_str
import pytz
from datetime import datetime
from blueprint_routes.poLifeCycle_blueprint import projectGroup,projectid,subproject,masterunitRate


graph_blueprint = Blueprint('graph_blueprint', __name__)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def start_date():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    start_date_previous_12_months = (current_date - relativedelta(months=11)).replace(day=1)
    last_date_current_month = (current_date + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
    start_date = start_date_previous_12_months.strftime('%d-%m-%Y')
    return start_date

def last_date():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    start_date_previous_12_months = (current_date - relativedelta(months=12)).replace(day=1)
    last_date_current_month = (current_date + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
    last_date = last_date_current_month.strftime('%d-%m-%Y')
    return last_date

def start_date2():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    start_date_previous_12_months = (current_date - relativedelta(months=11)).replace(day=1)
    last_date_current_month = (current_date + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
    start_date = start_date_previous_12_months.strftime('%Y-%m-%d')
    return start_date

def current_week():
    today = datetime.now(pytz.timezone('Asia/Kolkata'))
    start_of_week = today - timedelta(days=today.weekday())  # Monday of the current week
    current_week = today.strftime('%d-%m-%Y')
    return current_week



def last_week():
    today = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_week = today - timedelta(days=today.weekday() + 1)
    return last_week.strftime('%d-%m-%Y')

def Trendlast_date():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    Trendlast_date = current_date.strftime('%d-%m-%Y')
    return Trendlast_date

def Trendlast_date2():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    Trendlast_date = current_date.strftime('%Y-%m-%d')
    return Trendlast_date

def current_year():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    current_year = current_date.year
    return current_year

def current_week():
    today = datetime.now(pytz.timezone('Asia/Kolkata'))
    start_of_week = today - timedelta(days=today.weekday())  # Monday of the current week
    current_week = today.strftime('%d-%m-%Y')
    return current_week

def current_month():
    india_tz = pytz.timezone('Asia/Kolkata')
    return datetime.now(india_tz).month



@graph_blueprint.route("/graph/projectStatus",methods=['GET',"POST"])
@token_required
def graph_projectstatus(current_user):
    if request.method == "GET":
        arra = [
            {
                '$match':{
                    '_id':{
                        '$in':projectId_Object(current_user['userUniqueId'])
                    }
                }
            }, {
                '$group': {
                    '_id': '$status', 
                    'count': {
                        '$sum': 1
                    }, 
                    'status': {
                        '$first': '$status'
                    }
                }
            }, {
                '$sort': {
                    'status': 1
                }
            }
        ]
        response = cmo.finding_aggregate("project",arra)
        return respond(response)
    
    elif request.method == "POST":
        data = request.get_json()
        arra = [
            {
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
                        }
                    ], 
                    'as': 'projectTypeResult'
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
                                'customer': '$customer.shortName'
                            }
                        }, {
                            '$addFields': {
                                'projectGroupId': {
                                    '$concat': [
                                        '$customer', '-', '$zone', '-', '$costCenter'
                                    ]
                                }, 
                                'zoneId': {
                                    '$toString': '$zoneId'
                                }, 
                                'customerId': {
                                    '$toString': '$customerId'
                                }, 
                                'costCenterId': {
                                    '$toString': '$costCenterId'
                                }, 
                                'uniqueId': {
                                    '$toString': '$_id'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'projectGroupId': 1
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'projectTypeName': {
                        '$arrayElemAt': [
                            '$projectTypeResult.projectType', 0
                        ]
                    }, 
                    'projectGroupId': {
                        '$arrayElemAt': [
                            '$result.projectGroupId', 0
                        ]
                    }, 
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
                        }
                    ], 
                    'as': 'PM'
                }
            }, {
                '$unwind': {
                    'path': '$PM', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'PMName': '$PM.empName', 
                    'circle': {
                        '$toObjectId': '$circle'
                    }
                }
            }
        ]
        
        if "selectedProjectGroup" in data:
            arra = arra + [
                {
                    '$match':{
                        'projectGroupId':{
                            '$in':data['selectedProjectGroup']
                        }
                    }
                }
            ]
            
        if "selectedProjectType" in data:
            arra = arra + [
                {
                    '$match':{
                        'projectTypeName':{
                            '$in':data['selectedProjectType']
                        }
                    }
                }
            ]
        
        if "selectedProjectManager" in data:
            arra = arra + [
                {
                    '$match':{
                        'PMName':{
                            '$in':data['selectedProjectManager']
                        }
                    }
                }
            ]
        
        arra = arra + [
            {
                '$group': {
                    '_id': '$status', 
                    'count': {
                        '$sum': 1
                    }, 
                    'status': {
                        '$first': '$status'
                    }
                }
            }, {
                '$sort': {
                    'status': 1
                }
            }
        ]
        response = cmo.finding_aggregate("project",arra)
        return respond(response)
    
    


@graph_blueprint.route("/graph/poStatus",methods=['GET',"POST"])
@token_required
def graph_postatus(current_user):
    
    if request.method == "GET":
        arra = [
            {
                '$match':{
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if current_user['roleName'] not in ['Admin','PMO']:
            arra = arra + [
                {
                    '$match':{
                        'projectGroup':{
                        '$in':projectGroup_str(current_user['userUniqueId'])
                    }
                    }
                }
            ]
        
        arra = [
            {
                '$group': {
                    '_id': '$poNumber', 
                    'poStatus': {
                        '$first': '$poStatus'
                    }
                }
            }, {
                '$group': {
                    '_id': '$poStatus', 
                    'count': {
                        '$sum': 1
                    }, 
                    'status': {
                        '$first': '$poStatus'
                    }
                }
            }, {
                '$addFields': {
                    'sortOrder': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$status', 'Open'
                                ]
                            }, 
                            'then': 1, 
                            'else': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$status', 'Closed'
                                        ]
                                    }, 
                                    'then': 2, 
                                    'else': 3
                                }
                            }
                        }
                    }
                }
            }, {
                '$sort': {
                    'sortOrder': 1
                }
            }, {
                '$project': {
                    'status': 1, 
                    'count': 1, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("PoInvoice",arra)
        return respond(response)
    
    if request.method == "POST":
        allData = request.get_json()
        arra = [
            {
                '$match':{
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if current_user['roleName'] not in ['Admin','PMO']:
            arra = arra + [
                {
                    '$match':{
                        'projectGroup':{
                        '$in':projectGroup_str(current_user['userUniqueId'])
                    }
                    }
                }
            ]
        if "selectedProjectGroup" in allData:
            arra = arra + [
                {
                    '$match':{
                        'projectGroup':{
                            '$in':allData['selectedProjectGroup']
                        }
                    }
                }
            ]
        
        arra = arra + [
            {
                '$group': {
                    '_id': '$poNumber', 
                    'poStatus': {
                        '$first': '$poStatus'
                    }
                }
            }, {
                '$group': {
                    '_id': '$poStatus', 
                    'count': {
                        '$sum': 1
                    }, 
                    'status': {
                        '$first': '$poStatus'
                    }
                }
            }, {
                '$addFields': {
                    'sortOrder': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$status', 'Open'
                                ]
                            }, 
                            'then': 1, 
                            'else': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$status', 'Closed'
                                        ]
                                    }, 
                                    'then': 2, 
                                    'else': 3
                                }
                            }
                        }
                    }
                }
            }, {
                '$sort': {
                    'sortOrder': 1
                }
            }, {
                '$project': {
                    'status': 1, 
                    'count': 1, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("PoInvoice",arra)
        return respond(response)
        
    
        

@graph_blueprint.route("/graph/poTrackingWorkdone",methods=['GET',"POST"])
@token_required
def graph_poTrackingWorkdone(current_user):
    
    if request.method == "GET":
        arra = [
            {
                '$match':{
                    'deleteStatus':{'$ne':1},
                    'poStatus':'Open'
                }
            }, {
                '$project':{
                    'projectGroup':1,
                    'itemCode':{
                        '$toString':'$itemCode'
                    },
                    'poNumber':{
                        '$toString':'$poNumber'
                    },
                    'totalQty':'$initialPoQty',
                    '_id':0,
                }
            }
        ]
        response = cmo.finding_aggregate('PoInvoice',arra)['data']
        totaldf = pd.DataFrame.from_dict(response)
        if not totaldf.empty:
            totaldf = totaldf.groupby(['projectGroup','itemCode','poNumber']).sum().reset_index()
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'projectGroup': 1, 
                        'itemCode':{
                            '$toString':'$itemCode'
                        },
                        'poNumber':{
                            '$toString':'$poNumber'
                        },
                        'invoicedQty':'$qty',
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("invoice",arra)['data']
            invoicedf = pd.DataFrame.from_dict(response)
            if not invoicedf.empty:
                invoicedf = invoicedf.groupby(['projectGroup','itemCode','poNumber']).sum().reset_index()
        arra = [
            {
                '$match':{
                    'deleteStatus': {'$ne': 1},
                    'projectuniqueId':{
                        '$in':projectId_str(current_user['userUniqueId'])
                    },
                    'siteBillingStatus':'Unbilled',
                }
            }, {
                '$project':{
                    'projectuniqueId': 1, 
                    'SubProjectId': 1, 
                    'BAND':{
                        '$ifNull':['$BAND',""]
                    }, 
                    'ACTIVITY':{
                        '$ifNull':['$ACTIVITY',""]
                    }, 
                    'uid':{
                        '$toString':'$_id'
                    },
                    '_id': 0,
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        SiteData = pd.DataFrame(response['data'])
        arra = [
            {
                '$match': {
                    'deleteStatus': {'$ne': 1}, 
                    'mileStoneStatus': 'Closed', 
                    'Name': 'MS2', 
                    'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
                }
            }, {
                '$project': {
                    'uid': {
                        '$toString': '$siteId'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("milestone",arra)
        MilestoneData = pd.DataFrame.from_dict(response['data'])
        pivotedData = SiteData[SiteData['uid'].isin(MilestoneData['uid'])]
        pivotedData = pivotedData.copy()
        pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
        
        projectGroupdf = projectGroup()
        projectIddf = projectid()
        subProjectdf = subproject()
        masterunitRatedf = masterunitRate()
        
        mergedData2 = projectIddf.merge(projectGroupdf,on='projectGroup',how='left')
        mergedData2 = mergedData2[['projectGroup','projectuniqueId','projectId']]
        pivotedData = pivotedData.merge(mergedData2,on='projectuniqueId',how='left')
        
        mergedData3 = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
        pivotedData['subProject'] = mergedData3['subProject']
        pivotedData['projectType'] = mergedData3['projectType']
        
        mergedData4 = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
        masterunitRatedf['subProject'] = mergedData4['subProject']
        masterunitRatedf['projectType'] = mergedData4['projectType']
        
        mergedData5 = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
        masterunitRatedf['projectId'] = mergedData5['projectId']
        
        pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        masterunitRatedf.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        
        pivotedData = pivotedData[['projectGroup','projectType','subProject','projectId','BAND','ACTIVITY']]
        if "rate" in masterunitRatedf.columns:
            del masterunitRatedf['rate']
            
        
        df_macro = pivotedData[pivotedData['projectType'] == 'MACRO']
        if "BAND" in df_macro:
            del df_macro['BAND']
        
        df_dismantle_relocation = pivotedData[(pivotedData['projectType'] == 'RELOCATION') | (pivotedData['subProject'] == 'RELOCATION DISMANTLE')]
        if "ACTIVITY" in df_dismantle_relocation:
            del df_dismantle_relocation['ACTIVITY']
            
        df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO','RELOCATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
        if 'BAND' in df_other.columns:
            del df_other['BAND']
        
        if 'ACTIVITY' in df_other.columns:
            del df_other['ACTIVITY']
        
        df_macro_merged = pd.merge(df_macro, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'ACTIVITY'], how='left')
        df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
        final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
        final_df.drop(['projectType','subProject','projectId','ACTIVITY','BAND'], axis=1, inplace=True)
        
        item_code_columns = ['itemCode01', 'itemCode02', 'itemCode03', 'itemCode04', 'itemCode05', 'itemCode06', 'itemCode07']
        df_list = []
        for item_code in item_code_columns:
            df = final_df[['projectGroup', item_code]].copy()
            df.rename(columns={item_code: 'itemCode'}, inplace=True)
            df['workDoneQty'] = df['itemCode'].apply(lambda x: 1 if pd.notna(x) and x.strip() != '' else 0)
            df_list.append(df)
        workdonedf = pd.concat(df_list, ignore_index=True)
        workdonedf = workdonedf.loc[workdonedf['workDoneQty'] != 0]
        if not workdonedf.empty:
            workdonedf = workdonedf.groupby(['projectGroup','itemCode']).sum().reset_index()
            workdonedf = workdonedf[['workDoneQty']]
        else:
            workdonedf = pd.DataFrame([{'workDoneQty':0}])
    
        if not totaldf.empty:
            if not invoicedf.empty:
                mergedData1 = totaldf.merge(invoicedf,on=['projectGroup','itemCode','poNumber'],how='left')
                mergedData1 = mergedData1[['totalQty','invoicedQty']]
            else:
                mergedData1 = totaldf[['totalQty']]
                mergedData1['invoicedQty'] = 0
        else:
            dataList = [{'totalQty':0,'invoicedQty':0}]
            mergedData1 = pd.DataFrame(dataList)
        
        mergedData1.fillna(0,inplace=True)
        
        
        sum_df1 = pd.DataFrame({
            'totalQty': [mergedData1['totalQty'].sum()],
            'invoicedQty': [mergedData1['invoicedQty'].sum()]
        })
        sum_df2 = pd.DataFrame({
            'workDoneQty': [workdonedf['workDoneQty'].sum()],
        })
        
        sum_df1['workDoneQty'] = sum_df2['workDoneQty']
        final_df = sum_df1
        final_df['openQty'] = final_df['totalQty'] - final_df['invoicedQty'] - final_df['workDoneQty']
        return respond({
            'status':200,
            "msg":"Data Get Successfully",
            "data":json.loads(final_df.to_json(orient="records"))
        })
        
        
    
    if request.method == "POST":
        
        allData = request.get_json()
        arra = [
            {
                '$match':{
                    'deleteStatus':{'$ne':1},
                    'poStatus':'Open'
                }
            }
        ]
        if "selectedProjectGroup" in allData:
            arra = arra + [
                {
                    '$match':{
                        'projectGroup':{
                            '$in':allData['selectedProjectGroup']
                        }
                    }
                }
            ]
        if allData["selectedItemCode"]!="":
            arra = arra + [
                {
                    '$match':{
                        'itemCode':{
                            '$in':[allData['selectedItemCode']]
                        }
                    }
                }
            ]
        arra = arra + [
            {
                '$project':{
                    'projectGroup':1,
                    'itemCode':1,
                    'poNumber':1,
                    'totalQty':'$initialPoQty',
                    '_id':0,
                }
            }
        ]
        response = cmo.finding_aggregate('PoInvoice',arra)['data']
        totaldf = pd.DataFrame.from_dict(response)
        if not totaldf.empty:
            totaldf = totaldf.groupby(['projectGroup','itemCode','poNumber']).sum().reset_index()
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }
            ]
            if "selectedProjectGroup" in allData:
                arra = arra + [
                    {
                        '$match':{
                            'projectGroup':{
                                '$in':allData['selectedProjectGroup']
                            }
                        }
                    }
                ]
            if allData["selectedItemCode"]!="":
                arra = arra + [
                    {
                        '$match':{
                            'itemCode':{
                                '$in':[allData['selectedItemCode']]
                            }
                        }
                    }
                ]
            arra = arra + [
                {
                    '$project': {
                        'projectGroup': 1, 
                        'itemCode': 1, 
                        'poNumber':1,
                        'invoicedQty':'$qty',
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("invoice",arra)['data']
            invoicedf = pd.DataFrame.from_dict(response)
            if not invoicedf.empty:
                invoicedf = invoicedf.groupby(['projectGroup','itemCode','poNumber']).sum().reset_index()
            
        if "selectedProjectGroup" in allData:
            PG = []
            for i in allData['selectedProjectGroup']:
                PG.append(ObjectId(i))
            arra = [
                {
                    '$match': {
                        'projectGroup': {
                            '$in': PG
                        }
                    }
                }, {
                    '$project': {
                        'uid': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("project",arra)['data']
            PID = []
            for i in response:
                PID.append(i['uid'])
        else:
            PID =  projectId_str(current_user['userUniqueId'])   
               
        arra = [
            {
                '$match': {
                    'deleteStatus': {'$ne': 1}, 
                    'mileStoneStatus': 'Closed', 
                    'Name': 'MS2', 
                    'projectuniqueId':{'$in':PID}
                }
            }, {
                '$project': {
                    'uid': {
                        '$toString': '$siteId'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("milestone",arra)
        MilestoneData = pd.DataFrame.from_dict(response['data'])
        if not MilestoneData.empty:
            uid_list = MilestoneData['uid'].tolist()
            uid_list_objectId = []
            for i in uid_list:
                uid_list_objectId.append(ObjectId(i))
            arra = [
                {
                    '$match':{
                        'deleteStatus': {'$ne': 1},
                        'siteBillingStatus':'Unbilled',
                        '_id':{
                            '$in':uid_list_objectId
                        }
                    }
                }, {
                    '$project':{
                        'projectuniqueId': 1, 
                        'SubProjectId': 1, 
                        'BAND':{
                            '$ifNull':['$BAND',""]
                        }, 
                        'ACTIVITY':{
                            '$ifNull':['$ACTIVITY',""]
                        }, 
                        'uid':{
                            '$toString':'$_id'
                        },
                        '_id': 0,
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer",arra)
            SiteData = pd.DataFrame(response['data'])
            pivotedData = SiteData
            pivotedData = pivotedData.copy()
            pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
            
            projectGroupdf = projectGroup()
            projectIddf = projectid()
            subProjectdf = subproject()
            masterunitRatedf = masterunitRate()

            
            mergedData2 = projectIddf.merge(projectGroupdf,on='projectGroup',how='left')
            mergedData2 = mergedData2[['projectGroup','projectuniqueId','projectId']]
            pivotedData = pivotedData.merge(mergedData2,on='projectuniqueId',how='left')
            
            mergedData3 = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
            pivotedData['subProject'] = mergedData3['subProject']
            pivotedData['projectType'] = mergedData3['projectType']
            
            mergedData4 = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
            masterunitRatedf['subProject'] = mergedData4['subProject']
            masterunitRatedf['projectType'] = mergedData4['projectType']
            
            mergedData5 = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
            masterunitRatedf['projectId'] = mergedData5['projectId']
            
            pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
            masterunitRatedf.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
            
            pivotedData = pivotedData[['projectGroup','projectType','subProject','projectId','BAND','ACTIVITY']]
            if "rate" in masterunitRatedf.columns:
                del masterunitRatedf['rate']
                
            
            df_macro = pivotedData[pivotedData['projectType'] == 'MACRO']
            if "BAND" in df_macro:
                del df_macro['BAND']
            
            df_dismantle_relocation = pivotedData[(pivotedData['projectType'] == 'RELOCATION') | (pivotedData['subProject'] == 'RELOCATION DISMANTLE')]
            if "ACTIVITY" in df_dismantle_relocation:
                del df_dismantle_relocation['ACTIVITY']
                
            df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO','RELOCATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
            if 'BAND' in df_other.columns:
                del df_other['BAND']
            
            if 'ACTIVITY' in df_other.columns:
                del df_other['ACTIVITY']
            
            df_macro_merged = pd.merge(df_macro, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'ACTIVITY'], how='left')
            df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
            df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
            final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
            final_df.drop(['projectType','subProject','projectId','ACTIVITY','BAND'], axis=1, inplace=True)
            
            item_code_columns = ['itemCode01', 'itemCode02', 'itemCode03', 'itemCode04', 'itemCode05', 'itemCode06', 'itemCode07']
            df_list = []
            for item_code in item_code_columns:
                df = final_df[['projectGroup', item_code]].copy()
                df.rename(columns={item_code: 'itemCode'}, inplace=True)
                df['workDoneQty'] = df['itemCode'].apply(lambda x: 1 if pd.notna(x) and x.strip() != '' else 0)
                df_list.append(df)
            workdonedf = pd.concat(df_list, ignore_index=True)
            workdonedf = workdonedf.loc[workdonedf['workDoneQty'] != 0]
                        
            if allData["selectedItemCode"]!="":
                workdonedf = workdonedf[workdonedf['itemCode'] == allData["selectedItemCode"]]  
            if not workdonedf.empty:
                workdonedf = workdonedf.groupby(['projectGroup','itemCode']).sum().reset_index()
                workdonedf = workdonedf[['workDoneQty']]
            else:
                workdonedf = pd.DataFrame([{'workDoneQty':0}])
            if not totaldf.empty:
                if not invoicedf.empty:
                    mergedData1 = totaldf.merge(invoicedf,on=['projectGroup','itemCode','poNumber'],how='left')
                    mergedData1 = mergedData1[['totalQty','invoicedQty']]
                else:
                    mergedData1 = totaldf[['totalQty']]
                    mergedData1['invoicedQty'] = 0
            else:
                dataList = [{'totalQty':0,'invoicedQty':0}]
                mergedData1 = pd.DataFrame(dataList)
            
            mergedData1.fillna(0,inplace=True)
            
            
            sum_df1 = pd.DataFrame({
                'totalQty': [mergedData1['totalQty'].sum()],
                'invoicedQty': [mergedData1['invoicedQty'].sum()]
            })
            sum_df2 = pd.DataFrame({
                'workDoneQty': [workdonedf['workDoneQty'].sum()],
            })
            
            sum_df1['workDoneQty'] = sum_df2['workDoneQty']
            final_df = sum_df1
            final_df['openQty'] = final_df['totalQty'] - final_df['invoicedQty'] - final_df['workDoneQty']
            return respond({
                'status':200,
                "msg":"Data Get Successfully",
                "data":json.loads(final_df.to_json(orient="records"))
            })
        else:
            workdonedf = pd.DataFrame([{'workDoneQty':0}])
            if not totaldf.empty:
                if not invoicedf.empty:
                    mergedData1 = totaldf.merge(invoicedf,on=['projectGroup','itemCode','poNumber'],how='left')
                    mergedData1 = mergedData1[['totalQty','invoicedQty']]
                else:
                    mergedData1 = totaldf[['totalQty']]
                    mergedData1['invoicedQty'] = 0
            else:
                dataList = [{'totalQty':0,'invoicedQty':0}]
                mergedData1 = pd.DataFrame(dataList)
            
            mergedData1.fillna(0,inplace=True)
            
            
            sum_df1 = pd.DataFrame({
                'totalQty': [mergedData1['totalQty'].sum()],
                'invoicedQty': [mergedData1['invoicedQty'].sum()]
            })
            sum_df2 = pd.DataFrame({
                'workDoneQty': [workdonedf['workDoneQty'].sum()],
            })
            
            sum_df1['workDoneQty'] = sum_df2['workDoneQty']
            final_df = sum_df1
            final_df['openQty'] = final_df['totalQty'] - final_df['invoicedQty'] - final_df['workDoneQty']
            return respond({
                'status':200,
                "msg":"Data Get Successfully",
                "data":json.loads(final_df.to_json(orient="records"))
            })
    
    
    
    
@graph_blueprint.route("/graph/milestoneStatus",methods=['GET',"POST"])
@token_required
def graph_milestone_status(current_user):
    
    if request.method == "GET":
        arra = [
            
            {
                "$match":{
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if current_user['roleName'] not in ['Admin','PMO']:
            arra = arra + [
                {
                    '$match':{
                        'assignerId': ObjectId(current_user['userUniqueId'])
                    }
                }
            ]
        arra = arra + [
            {
                '$project': {
                   'status':'$mileStoneStatus', 
                    '_id': 0
                }
            },
            {
                '$group': {
                    '_id': '$status', 
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$project': {
                    'status': '$_id', 
                    'count': 1, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("milestone",arra)
        # if len(response['data']):
        #     df = pd.DataFrame.from_dict(response['data'])
        #     count = df['status'].value_counts()
        #     count = count.reset_index()
        #     dict_data = json.loads(count.to_json(orient="records"))
        #     response['data'] = dict_data
        return respond(response)
    
    
    
    if request.method == "POST":
        allData = request.get_json()
        if "selectedProjectType" in allData:
            allData['selectedProjectType'] = [item for sublist in allData['selectedProjectType'] for item in sublist]
        
        arra = [
            {
                '$match':{
                    'deleteStatus':{'$ne':1}
                }
            }
        ]
        if current_user['roleName'] not in ['Admin','PMO']:
            arra = arra + [
                {
                    '$match':{
                        'assignerId': ObjectId(current_user['userUniqueId'])
                    }
                }
            ]
        if "selectedProjectType" in allData:
            arra = arra + [
                {
                    '$match':{
                        'SubProjectId':{
                            '$in':allData['selectedProjectType']
                        }
                    }
                }
            ]
        if "selectedProjectId" in allData: 
            arra = arra + [
                {
                    '$match':{
                        'projectuniqueId':{
                            '$in': allData['selectedProjectId']
                        }
                    }
                }
            ]
        arra = arra + [
            {
                '$project': {
                   'status':'$mileStoneStatus', 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("milestone",arra)
        if len(response['data']):
            df = pd.DataFrame.from_dict(response['data'])
            count = df['status'].value_counts()
            count = count.reset_index()
            dict_data = json.loads(count.to_json(orient="records"))
            response['data'] = dict_data
        return respond(response)
          
    
    
    
@graph_blueprint.route("/graph/accrualRevenueTrend",methods=["POST","GET"])
@token_required
def graph_accrual_trend(current_user):
    
    if request.method == "POST":
        
        allData = request.get_json()
        viewBy = allData["Monthly"].split(",")
        costcenter = ""
        costCenterId = []
        if "costCenter" in allData  and allData['costCenter']!='undefined':
            costcenter = allData['costCenter']
            
        group_stage = {
            '_id': None,
        }
        project_stage = {
            '_id':0
        }
        month_map = {
            'M-1Y': 'Jan',
            'M-2Y': 'Feb',
            'M-3Y': 'Mar',
            'M-4Y': 'Apr',
            'M-5Y': 'May',
            'M-6Y': 'Jun',
            'M-7Y': 'Jul',
            'M-8Y': 'Aug',
            'M-9Y': 'Sep',
            'M-10Y': 'Oct',
            'M-11Y': 'Nov',
            'M-12Y': 'Dec'
        }
        
        for field in viewBy:
            group_stage[field] = {'$sum':'$'+field}
            month_code = field.split('-')[0] + '-' + field.split('-')[1]
            month_name = month_map.get(month_code, 'Unknown')
            project_stage[month_name] = "$"+field
            
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
                    'result.customer':'$customer'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            },
            
        ]    
        response = cmo.finding_aggregate("projectAllocation",arra)
        if len(response['data']):
            response = response['data']
            for i in response:
                if "uniqueId" in i:
                    costCenterId.append(i['uniqueId'])
                        
        arra = []
        if (len(costCenterId)):
            arra = arra + [
                {
                    "$match":{
                        'costCenteruid':{'$in':costCenterId}
                    }
                }
            ]
        if (costcenter!=""):
            arra = arra + [
                {
                    '$match':{
                        'costCenteruid':costcenter
                    }
                }
            ]
        if (len(costCenterId)==0 and costcenter == "" ):
            arra = arra + [
                {
                    '$match':{
                        'costCenteruid':''
                    }
                }
            ]
        arra = arra + [
            {
                '$group': group_stage
            }, {
                '$project':project_stage
            }, {
                '$addFields': {
                    'newField': {
                        '$map': {
                            'input': {
                                '$objectToArray': '$$ROOT'
                            }, 
                            'as': 'pair', 
                            'in': {
                                'description': '$$pair.k', 
                                'y': '$$pair.v'
                            }
                        }
                    }
                }
            }, {
                '$project':{
                    'newField':1
                }
            },
            
        ]
        response = cmo.finding_aggregate("accrualRevenueTrend",arra)
        if (len(response['data'])>0):
            response['data'] = response['data'][0]['newField']
            for i in response['data']:
                i['y'] = round(i['y'] / 100000)
        return respond(response)
    
    
@graph_blueprint.route("/graph/activeCustomer",methods=['GET',"POST"])
def activeCustomer():
    if request.method == "GET":
        arra = [
            {
                '$group': {
                    '_id': '$status', 
                    'count': {
                        '$sum': 1
                    }, 
                    'status': {
                        '$first': '$status'
                    }
                }
            }, {
                '$sort': {
                    'status': 1
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        Response = cmo.finding_aggregate("customer",arra)
        return respond(Response)
    if request.method == "POST":
        data = request.get_json()
        arra = [
            {
                '$group': {
                    '_id': '$status',
                    'count': {
                        '$sum': 1
                    }, 
                    'status': {
                        '$first': '$status'
                    }
                }
            }, {
                '$sort': {
                    'status': 1
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        if 'status' in data:
            arra = arra + [
                {
                   '$match':{
                       'status':{
                           '$in':data['status']
                       }
                   }
                }
            ]
        Response = cmo.finding_aggregate("customer",arra)
        return respond(Response)


@graph_blueprint.route("/graph/activeEmpwithCC",methods=['GET',"POST"])
def graph_active_empwith_CC():
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'status': 'Active', 
                    'costCenter': {
                        '$ne': None
                    }, 
                    'costCenter': {
                        '$ne': ''
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    }
                }
            }, {
                '$addFields': {
                    'costCenter': {
                        '$toObjectId': '$costCenter'
                    }
                }
            }, {
                '$project': {
                    'costCenter': 1, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'costCenter', 
                    'localField': 'costCenter', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'description': {
                                    '$ne': ''
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
                    'description': '$result.description'
                }
            }, {
                '$group': {
                    '_id': '$description', 
                    'description': {
                        '$first': '$description'
                    }, 
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$match': {
                    '_id': {
                        '$ne': None
                    }
                }
            }, {
                '$sort': {
                    "count":-1,
                    'description': 1
                }
            }

        ]   
        response = cmo.finding_aggregate("userRegister",arra)['data']
        if len(response):
            data = pd.DataFrame(response)
            total_count = data['count'].sum()
            data['total'] = total_count
            final_result = json.loads(data.to_json(orient="records"))

            dataResp = {}            
            dataResp['data'] = final_result
            dataResp['status']=200

            dataResp['msg'] = "Data Get Successfully"
  
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                "data":[]
            })

    if request.method == "POST":
        data = request.get_json()
        arra=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'status': 'Active', 
                    'costCenter': {
                        '$ne': None
                    }, 
                    'costCenter': {
                        '$ne': ''
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    }
                }
            }, {
                '$addFields': {
                    'costCenter': {
                        '$toObjectId': '$costCenter'
                    }
                }
            }, {
                '$project': {
                    'costCenter': 1, 
                    'orgLevel': 1, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'costCenter', 
                    'localField': 'costCenter', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'description': {
                                    '$ne': ''
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
                    'description': '$result.description', 
                    'orgLevel': '$orgLevel'
                }
            }, {
                '$group': {
                    '_id': '$description', 
                    'description': {
                        '$first': '$description'
                    }, 
                    'orgLevel': {
                        '$first': '$orgLevel'
                    }, 
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$match': {
                    '_id': {
                        '$ne': None
                    }
                }
            }, {
                '$sort': {
                    'count': -1, 
                    'description': 1
                }
            },
        ]
        if 'orgLevel' in data:
            arra = arra + [
                {
                    '$match':{
                        'orgLevel':{
                            '$in':data['orgLevel']
                        }
                    }
                }
            ]
        response = cmo.finding_aggregate("userRegister",arra)['data']
        if len(response):
            data = pd.DataFrame(response)
            total_count = data['count'].sum()
            data['total'] = total_count
            final_result = json.loads(data.to_json(orient="records"))

            dataResp = {}            
            dataResp['data'] = final_result
            dataResp['status']=200
            dataResp['msg'] = "Data Get Successfully"
  
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                "data":[]
            })

    
@graph_blueprint.route("/graph/getAirtelDescription",methods=["POST","GET"])
@graph_blueprint.route("/graph/getAirtelDescription/<id>",methods=["POST","GET"])
@token_required
def getAirtelDescription(current_user,id=None):
    if request.method == "GET":
        arra=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'status': 'Active', 
                        'costCenter': {
                            '$ne': None
                        }, 
                        'costCenter': {
                            '$ne': ''
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }
                    }
                }, {
                    '$addFields': {
                        'costCenter': {
                            '$toObjectId': '$costCenter'
                        }
                    }
                }, {
                    '$project': {
                        'costCenter': 1, 
                        '_id': 0
                    }
                }, {
                    '$lookup': {
                        'from': 'costCenter', 
                        'localField': 'costCenter', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'description': {
                                        '$ne': ''
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
                        'description': '$result.description'
                    }
                }, {
                    '$group': {
                        '_id': '$description', 
                        'description': {
                            '$first': '$description'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$match': {
                        '_id': {
                            '$ne': None
                        }
                    }
                }, {
                    '$sort': {
                        'count': -1, 
                        'description': 1
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'description': 1
                    }
                }
        ]
        response = cmo.finding_aggregate("userRegister",arra)
        return respond(response)


@graph_blueprint.route("/graph/newjoining",methods=["POST","GET"])
@graph_blueprint.route("/graph/newjoining/<id>",methods=["POST","GET"])
@token_required
def newJoinings(current_user,id=None):
    if request.method == 'GET':
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    }, 
                    'status': 'Active'
                }
            }, {
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        '$month': '$joiningDate'
                    }, 
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'month': '$_id', 
                    'count': 1
                }
            }, {
                '$match': {
                    'month': {
                        '$ne': None
                    }
                }
            }, {
                '$sort': {
                    'month': 1
                }
            }, {
                '$addFields': {
                    'monthName': {
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
                            ]
                        }
                    }
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arr)
        return respond(Response)
    
@graph_blueprint.route("/graph/newjoiningandresign",methods=['POST','GET'])
@graph_blueprint.route("/graph/newjoiningandresign/<id>",methods=['POST','GET'])
@token_required
def newjoiningandresign(current_user,id=None):
    if request.method == 'GET':
        arr=[
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
        '$addFields': {
            'joiningDate': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$eq': [
                                    '$joiningDate', None
                                ]
                            }, {
                                '$eq': [
                                    '$joiningDate', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        '$toDate': '$joiningDate'
                    }
                }
            }, 
            'lastWorkingDay': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$eq': [
                                    '$lastWorkingDay', None
                                ]
                            }, {
                                '$eq': [
                                    '$lastWorkingDay', ''
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        '$toDate': '$lastWorkingDay'
                    }
                }
            }
        }
    }, {
        '$match': {
            'joiningDate': {
                '$exists': True, 
                '$ne': None, 
                '$ne': ''
            }, 
            'lastWorkingDay': {
                '$exists': True, 
                '$ne': None, 
                '$ne': ''
            }
        }
    }, {
        '$facet': {
            'joinings': [
                {
                    '$group': {
                        '_id': {
                            '$month': '$joiningDate'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'month': '$_id', 
                        'joinings': '$count'
                    }
                }, {
                    '$sort': {
                        'month': 1
                    }
                }
            ], 
            'lastWorkingDays': [
                {
                    '$group': {
                        '_id': {
                            '$month': '$lastWorkingDay'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'month': '$_id', 
                        'lastWorkingDays': '$count'
                    }
                }, {
                    '$sort': {
                        'month': 1
                    }
                }
            ]
        }
    }, {
        '$project': {
            'monthlyData': {
                '$map': {
                    'input': {
                        '$range': [
                            1, 13
                        ]
                    }, 
                    'as': 'month', 
                    'in': {
                        'month': '$$month', 
                        'joined': {
                            '$let': {
                                'vars': {
                                    'joining': {
                                        '$arrayElemAt': [
                                            {
                                                '$filter': {
                                                    'input': '$joinings', 
                                                    'as': 'item', 
                                                    'cond': {
                                                        '$eq': [
                                                            '$$item.month', '$$month'
                                                        ]
                                                    }
                                                }
                                            }, 0
                                        ]
                                    }
                                }, 
                                'in': {
                                    '$ifNull': [
                                        '$$joining.joinings', 0
                                    ]
                                }
                            }
                        }, 
                        'exit': {
                            '$let': {
                                'vars': {
                                    'working': {
                                        '$arrayElemAt': [
                                            {
                                                '$filter': {
                                                    'input': '$lastWorkingDays', 
                                                    'as': 'item', 
                                                    'cond': {
                                                        '$eq': [
                                                            '$$item.month', '$$month'
                                                        ]
                                                    }
                                                }
                                            }, 0
                                        ]
                                    }
                                }, 
                                'in': {
                                    '$ifNull': [
                                        '$$working.lastWorkingDays', 0
                                    ]
                                }
                            }
                        }, 
                        'monthName': {
                            '$switch': {
                                'branches': [
                                    {
                                        'case': {
                                            '$eq': [
                                                '$$month', 1
                                            ]
                                        }, 
                                        'then': 'January'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 2
                                            ]
                                        }, 
                                        'then': 'February'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 3
                                            ]
                                        }, 
                                        'then': 'March'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 4
                                            ]
                                        }, 
                                        'then': 'April'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 5
                                            ]
                                        }, 
                                        'then': 'May'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 6
                                            ]
                                        }, 
                                        'then': 'June'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 7
                                            ]
                                        }, 
                                        'then': 'July'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 8
                                            ]
                                        }, 
                                        'then': 'August'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 9
                                            ]
                                        }, 
                                        'then': 'September'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 10
                                            ]
                                        }, 
                                        'then': 'October'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 11
                                            ]
                                        }, 
                                        'then': 'November'
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$$month', 12
                                            ]
                                        }, 
                                        'then': 'December'
                                    }
                                ], 
                                'default': 'Unknown'
                            }
                        }
                    }
                }
            }
        }
    }, {
        '$unwind': '$monthlyData'
    }, {
        '$replaceRoot': {
            'newRoot': '$monthlyData'
        }
    }
]
        Response=cmo.finding_aggregate("userRegister",arr)
        return respond(Response)



@graph_blueprint.route("/graph/newjoiningMonthly",methods=["POST","GET"])
@graph_blueprint.route("/graph/newjoiningMonthly/<id>",methods=["POST","GET"])
@token_required
def newjoiningMonthly(current_user,id=None):
    if request.method == 'GET':
        arra=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    },
                    # 'status': 'Active'
                }
            }, {
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    } 
                }
            }, {
                '$addFields': {
                    'start_date': start_date(), 
                    'last_date': last_date()
                }
            }, {
                '$addFields': {
                    'start_date': {
                        '$dateFromString': {
                            'dateString': '$start_date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'last_date': {
                        '$dateFromString': {
                            'dateString': '$last_date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$match': {
                    '$expr': {
                        '$and': [
                            {
                                '$gte': [
                                    '$joiningDate', '$start_date'
                                ]
                            }, {
                                '$lte': [
                                    '$joiningDate', '$last_date'
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        '$month': '$joiningDate'
                    }, 
                    'count': {
                        '$sum': 1
                    }, 
                    'Year': {
                        '$first': {
                            '$year': '$joiningDate'
                        }
                    }, 
                    'month': {
                        '$first': {
                            '$month': '$joiningDate'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'monthName': {
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
                            ]
                        }
                    }
                }
            }, {
                '$addFields': {
                    'year': {
                        '$toString': '$Year'
                    }
                }
            }, {
                '$addFields': {
                    'yearTwoDigit': {
                        '$substr': [
                            '$year', 2, 2
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'description': {
                        '$concat': [
                            '$monthName', '-', '$yearTwoDigit'
                        ]
                    }, 
                    'year': {
                        '$toInt': '$year'
                    }
                }
            }, {
                '$sort': {
                    'year': 1, 
                    'month': 1
                }
            }, {
                '$project': {
                    'description': 1, 
                    'count': 1, 
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)
    
    if request.method == "POST":
        orgLevel = request.json.get("orgLevel")
        year = request.json.get("year")
        month = request.json.get("month")

        data = request.get_json()

        arra=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    },
                    # 'status': 'Active'
                }
            }
        ]
        if 'orgLevel' in data:
            arra = arra + [
            {
                '$match': {
                    'orgLevel': {'$ne': ''},
                    'orgLevel':{
                        '$in':orgLevel
                    },
                }
            }
        ]
        arra = arra + [
            {
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    }
                }
            }, {
                '$addFields': {
                    'start_date': start_date(), 
                    'last_date': last_date()
                }
            }, {
                '$addFields': {
                    'start_date': {
                        '$dateFromString': {
                            'dateString': '$start_date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'last_date': {
                        '$dateFromString': {
                            'dateString': '$last_date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$match': {
                    '$expr': {
                        '$and': [
                            {
                                '$gte': [
                                    '$joiningDate', '$start_date'
                                ]
                            }, {
                                '$lte': [
                                    '$joiningDate', '$last_date'
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        '$month': '$joiningDate'
                    }, 
                    'count': {
                        '$sum': 1
                    }, 
                    'Year': {
                        '$first': {
                            '$year': '$joiningDate'
                        }
                    }, 
                    'month': {
                        '$first': {
                            '$month': '$joiningDate'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'monthName': {
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
                            ]
                        }
                    }
                }
            }, {
                '$addFields': {
                    'year': {
                        '$toString': '$Year'
                    }
                }
            }, {
                '$addFields': {
                    'yearTwoDigit': {
                        '$substr': [
                            '$year', 2, 2
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'description': {
                        '$concat': [
                            '$monthName', '-', '$yearTwoDigit'
                        ]
                    }, 
                    'year': {
                        '$toInt': '$year'
                    }
                }
            }, {
                '$sort': {
                    'year': 1, 
                    'month': 1
                }
            }
        ]
        if "month" in data:
            arra = arra + [
            {
                '$match': {
                    'month': {
                        '$in':data['month']
                    },
                }
            }
        ]  
        if "year" in data:
            arra = arra + [
            {
                '$match': {
                    'year': data['year']
                }
            }
        ]  
        arra = arra + [
            {
                '$project': {
                    'description': 1, 
                    'count': 1, 
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)





@graph_blueprint.route("/graph/monthlyjoiningVsExit",methods=["POST","GET"])
@graph_blueprint.route("/graph/monthlyjoiningVsExit/<id>",methods=["POST","GET"])
@token_required
def monthlyjoiningVsExit(current_user,id=None):
    if request.method == 'GET':
        arra=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    },
                    # 'status': 'Active'
                }
            }, {
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }, 
                    'lastWorkingDay': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$lastWorkingDay', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$lastWorkingDay', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$lastWorkingDay'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    }, 
                    'lastWorkingDay': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    }
                }
            }, {
                '$addFields': {
                    'start_date': start_date(),
                    'last_date': last_date()
                }
            }, {
                '$addFields': {
                    'start_date': {
                        '$dateFromString': {
                            'dateString': '$start_date', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'last_date': {
                        '$dateFromString': {
                            'dateString': '$last_date', 
                            'format': '%d-%m-%Y'
                        }
                    }
                }
            }, {
                '$facet': {
                    'joinings': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$joiningDate', '$start_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$joiningDate', '$last_date'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': {
                                    '$month': '$joiningDate'
                                }, 
                                'count': {
                                    '$sum': 1
                                }, 
                                'Year': {
                                    '$first': {
                                        '$year': '$joiningDate'
                                    }
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'Year': 1, 
                                'month': '$_id', 
                                'joinings': '$count'
                            }
                        }, {
                            '$sort': {
                                'month': 1
                            }
                        }
                    ], 
                    'lastWorkingDays': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$lastWorkingDay', '$start_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$lastWorkingDay', '$last_date'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': {
                                    '$month': '$lastWorkingDay'
                                }, 
                                'count': {
                                    '$sum': 1
                                }, 
                                'Year': {
                                    '$first': {
                                        '$year': '$lastWorkingDay'
                                    }
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'Year': 1, 
                                'month': '$_id', 
                                'lastWorkingDays': '$count'
                            }
                        }, {
                            '$sort': {
                                'month': 1
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'monthlyData': {
                        '$map': {
                            'input': {
                                '$range': [
                                    1, 13
                                ]
                            }, 
                            'as': 'month', 
                            'in': {
                                'month': '$$month', 
                                'joined': {
                                    '$let': {
                                        'vars': {
                                            'joining': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$joinings', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$joining.joinings', 0
                                            ]
                                        }
                                    }
                                }, 
                                'joiningYear': {
                                    '$let': {
                                        'vars': {
                                            'joining': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$joinings', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$joining.Year', None
                                            ]
                                        }
                                    }
                                }, 
                                'exit': {
                                    '$let': {
                                        'vars': {
                                            'lastWorkingDays': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$lastWorkingDays', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$lastWorkingDays.lastWorkingDays', 0
                                            ]
                                        }
                                    }
                                }, 
                                'exitYear': {
                                    '$let': {
                                        'vars': {
                                            'lastWorkingDays': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$lastWorkingDays', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$lastWorkingDays.Year', None
                                            ]
                                        }
                                    }
                                }, 
                                'monthName': {
                                    '$switch': {
                                        'branches': [
                                            {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 1
                                                    ]
                                                }, 
                                                'then': 'Jan'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 2
                                                    ]
                                                }, 
                                                'then': 'Feb'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 3
                                                    ]
                                                }, 
                                                'then': 'Mar'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 4
                                                    ]
                                                }, 
                                                'then': 'Apr'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 5
                                                    ]
                                                }, 
                                                'then': 'May'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 6
                                                    ]
                                                }, 
                                                'then': 'Jun'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 7
                                                    ]
                                                }, 
                                                'then': 'Jul'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 8
                                                    ]
                                                }, 
                                                'then': 'Aug'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 9
                                                    ]
                                                }, 
                                                'then': 'Sep'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 10
                                                    ]
                                                }, 
                                                'then': 'Oct'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 11
                                                    ]
                                                }, 
                                                'then': 'Nov'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 12
                                                    ]
                                                }, 
                                                'then': 'Dec'
                                            }
                                        ], 
                                        'default': 'Unknown'
                                    }
                                }
                            }
                        }
                    }
                }
            }, {
                '$unwind': '$monthlyData'
            }, {
                '$replaceRoot': {
                    'newRoot': '$monthlyData'
                }
            }, {
                '$addFields': {
                    'Year': {
                        '$ifNull': [
                            '$joiningYear', '$exitYear'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'Year': {
                        '$toString': '$Year'
                    }
                }
            },
            {
                '$addFields': {
                    'yearTwoDigit': {
                        '$substr': [
                            '$Year', 2, 2
                        ]
                    }
                }
            }, 
            {
                '$addFields': {
                    'description': {
                        '$concat': [
                            '$monthName', '-', '$yearTwoDigit'
                        ]
                    },
                    'year': {
                        '$toInt': '$Year'
                    }
                }
            }, {
                '$match': {
                    'year': {
                        '$ne': None
                    }
                }
            }, {
                '$sort': {
                    'year': 1, 
                    'month': 1
                }
            },{
                '$project': {
                    'joined': 1, 
                    'exit': 1, 
                    'description': 1
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)
    
    if request.method == "POST":

        orgLevel = request.json.get("orgLevel")
        year = request.json.get("year")
        month = request.json.get("month")


        data = request.get_json()
        arra=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'type': {
                        '$ne': 'Partner'
                    }, 
                    # 'status': 'Active'
                }
            }
        ]
        if 'orgLevel' in data:
            arra=arra + [
            {
                '$match': {
                    'orgLevel': {'$ne': ''},
                    'orgLevel':{
                        '$in':data['orgLevel']
                    },
                }
            }
        ]
        arra = arra + [    
            {
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }, 
                    'lastWorkingDay': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$lastWorkingDay', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$lastWorkingDay', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$lastWorkingDay'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    }, 
                    'lastWorkingDay': {
                        '$exists': True, 
                        '$ne': None, 
                        '$ne': ''
                    }
                }
            }, {
                '$addFields': {
                    'start_date': start_date(),
                    'last_date': last_date()
                }
            }, {
                '$addFields': {
                    'start_date': {
                        '$dateFromString': {
                            'dateString': '$start_date', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'last_date': {
                        '$dateFromString': {
                            'dateString': '$last_date', 
                            'format': '%d-%m-%Y'
                        }
                    }
                }
            }, {
                '$facet': {
                    'joinings': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$joiningDate', '$start_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$joiningDate', '$last_date'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': {
                                    '$month': '$joiningDate'
                                }, 
                                'count': {
                                    '$sum': 1
                                }, 
                                'Year': {
                                    '$first': {
                                        '$year': '$joiningDate'
                                    }
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'Year': 1, 
                                'month': '$_id', 
                                'joinings': '$count'
                            }
                        }, {
                            '$sort': {
                                'month': 1
                            }
                        }
                    ], 
                    'lastWorkingDays': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$lastWorkingDay', '$start_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$lastWorkingDay', '$last_date'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': {
                                    '$month': '$lastWorkingDay'
                                }, 
                                'count': {
                                    '$sum': 1
                                },
                                'Year': {
                                    '$first': {
                                        '$year': '$lastWorkingDay'
                                    }
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'Year': 1, 
                                'month': '$_id', 
                                'lastWorkingDays': '$count'
                            }
                        }, {
                            '$sort': {
                                'month': 1
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'monthlyData': {
                        '$map': {
                            'input': {
                                '$range': [
                                    1, 13
                                ]
                            }, 
                            'as': 'month', 
                            'in': {
                                'month': '$$month', 
                                'joined': {
                                    '$let': {
                                        'vars': {
                                            'joining': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$joinings', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$joining.joinings', 0
                                            ]
                                        }
                                    }
                                }, 
                                'joiningYear': {
                                    '$let': {
                                        'vars': {
                                            'joining': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$joinings', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$joining.Year', None
                                            ]
                                        }
                                    }
                                }, 
                                'exit': {
                                    '$let': {
                                        'vars': {
                                            'lastWorkingDays': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$lastWorkingDays', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$lastWorkingDays.lastWorkingDays', 0
                                            ]
                                        }
                                    }
                                }, 
                                'exitYear': {
                                    '$let': {
                                        'vars': {
                                            'lastWorkingDays': {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$lastWorkingDays', 
                                                            'as': 'item', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$item.month', '$$month'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        }, 
                                        'in': {
                                            '$ifNull': [
                                                '$$lastWorkingDays.Year', None
                                            ]
                                        }
                                    }
                                }, 
                                'monthName': {
                                    '$switch': {
                                        'branches': [
                                            {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 1
                                                    ]
                                                }, 
                                                'then': 'Jan'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 2
                                                    ]
                                                }, 
                                                'then': 'Feb'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 3
                                                    ]
                                                }, 
                                                'then': 'Mar'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 4
                                                    ]
                                                }, 
                                                'then': 'Apr'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 5
                                                    ]
                                                }, 
                                                'then': 'May'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 6
                                                    ]
                                                }, 
                                                'then': 'Jun'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 7
                                                    ]
                                                }, 
                                                'then': 'Jul'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 8
                                                    ]
                                                }, 
                                                'then': 'Aug'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 9
                                                    ]
                                                }, 
                                                'then': 'Sep'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 10
                                                    ]
                                                }, 
                                                'then': 'Oct'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 11
                                                    ]
                                                }, 
                                                'then': 'Nov'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        '$$month', 12
                                                    ]
                                                }, 
                                                'then': 'Dec'
                                            }
                                        ], 
                                        'default': 'Unknown'
                                    }
                                }
                            }
                        }
                    }
                }
            }, {
                '$unwind': '$monthlyData'
            }, {
                '$replaceRoot': {
                    'newRoot': '$monthlyData'
                }
            }, {
                '$addFields': {
                    'Year': {
                        '$ifNull': [
                            '$joiningYear', '$exitYear'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'Year': {
                        '$toString': '$Year'
                    }
                }
            },
            {
                '$addFields': {
                    'yearTwoDigit': {
                        '$substr': [
                            '$Year', 2, 2
                        ]
                    }
                }
            },
            {
                '$addFields': {
                    'description': {
                        '$concat': [
                            '$monthName', '-', '$yearTwoDigit'
                        ]
                    }, 
                    'year': {
                        '$toInt': '$Year'
                    }
                }
            }, {
                '$match': {
                    'year': {
                        '$ne': None
                    }
                }
            }, {
                '$sort': {
                    'year': 1, 
                    'month': 1
                }
            }
        ]
        if 'month' in data:
            arra = arra + [
            {
                '$match': {
                    'month': {
                        '$in':data["month"]
                    },
                }
            }
        ]
        if 'year' in data:
            arra = arra + [
            {
                '$match': {
                    'year':data['year']
                }
            }
        ]
        arra = arra + [     
            {
                '$project': {
                    'joined': 1, 
                    'exit': 1, 
                    'description': 1
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)


    
@graph_blueprint.route("/graph/monthlyActiveTrend",methods=["POST","GET"])
@graph_blueprint.route("/graph/monthlyActiveTrend/<id>",methods=["POST","GET"])
def monthlyActiveTrend(id=None):
    if request.method == 'GET':
        arra=[
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
                    '$addFields': {
                        'joiningDate': {
                            '$cond': {
                                'if': {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$joiningDate', None
                                            ]
                                        }, {
                                            '$eq': [
                                                '$joiningDate', ''
                                            ]
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$toDate': '$joiningDate'
                                }
                            }
                        }, 
                        'lastWorkingDay': {
                            '$cond': {
                                'if': {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$lastWorkingDay', None
                                            ]
                                        }, {
                                            '$eq': [
                                                '$lastWorkingDay', ''
                                            ]
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$toDate': '$lastWorkingDay'
                                }
                            }
                        }
                    }
                }, {
                    '$match': {
                        'joiningDate': {
                            '$exists': True, 
                            '$ne': ''
                        }, 
                        'lastWorkingDay': {
                            '$exists': True, 
                            '$ne': ''
                        }
                    }
                }, {
                    '$addFields': {
                        'start_date': start_date(), 
                        'last_date': Trendlast_date()
                    }
                }, {
                    '$addFields': {
                        'start_date': {
                            '$dateFromString': {
                                'dateString': '$start_date', 
                                'format': '%d-%m-%Y', 
                                'timezone': 'Asia/Kolkata'
                            }
                        }, 
                        'last_date': {
                            '$dateFromString': {
                                'dateString': '$last_date', 
                                'format': '%d-%m-%Y', 
                                'timezone': 'Asia/Kolkata'
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'start_date': {
                            '$dateAdd': {
                                'startDate': '$start_date', 
                                'unit': 'minute', 
                                'amount': 330
                            }
                        }, 
                        'last_date': {
                            '$dateAdd': {
                                'startDate': '$last_date', 
                                'unit': 'minute', 
                                'amount': 330
                            }
                        }
                    }
                },
                {
            '$project': {
                'status': 1, 
                'joiningDate': 1, 
                'lastWorkingDay': 1, 
                'start_date': 1, 
                'last_date': 1, 
                '_id': 0
            }
             }
            ]
        dara=cmo.finding_aggregate("userRegister",arra)['data']
        df=pd.DataFrame(dara)
        start_dates=start_date() ###01-09-2023
        Trendlast_dates=Trendlast_date() ####29-08-2024
        start_dates = pd.to_datetime(start_dates, dayfirst=True)
        trend_last_dates = pd.to_datetime(Trendlast_dates, dayfirst=True)
        all_months = pd.date_range(start=start_dates, end=trend_last_dates, freq='MS').to_period('M')
        all_months_df = pd.DataFrame(all_months, columns=['year_month'])
        monthly_stats = []
        for period in all_months_df['year_month']:
            if period == trend_last_dates.to_period('M'):
                end_of_month = trend_last_dates
            else:
                end_of_month = period.end_time
            total_active_employees_month = df[(df['joiningDate'] <= end_of_month)].shape[0]
            total_exit_employees_month = df[(df['lastWorkingDay'].notna()) & (df['lastWorkingDay'] <= end_of_month)].shape[0]
            monthly_stats.append({
                'year_month': period,
                'TotalActiveEmployee': total_active_employees_month,
                'totalExitEmployee': total_exit_employees_month
            })
        final_df = pd.DataFrame(monthly_stats)
        final_df['Year'] = final_df['year_month'].dt.year
        final_df['Month'] = final_df['year_month'].dt.month
        final_df['description'] = final_df['year_month'].dt.strftime('%b-%y')
        final_df['count'] = final_df['TotalActiveEmployee'] - final_df['totalExitEmployee']
        final_df['description'] = final_df['year_month'].dt.strftime('%b-%Y')
        columns_to_drop = ['year_month', 'TotalActiveEmployee']
        final_df = final_df.drop(columns=columns_to_drop)

        final_df['description'] = final_df.apply(lambda row: f"{row['description'][:-4]}{str(row['Year'])[-2:]}", axis=1)

        data=cfc.dfjson(final_df)
        Response={}
        Response['data']=data
        Response['status']=200
        Response['msg']="Data Get Successfully!"
        return respond(Response)
    
    if request.method == "POST":
        orgLevel=[]
        filter_criteria = request.get_json()
        if 'orgLevel' in filter_criteria:
            orgLevel=filter_criteria['orgLevel']
            
        arra=[]   
        if len(orgLevel):
            arra=[
                {
                    '$match': {
                        'orgLevel': {
                            '$in': orgLevel
                        }
                    }
                }
            ]
        arra=arra+[
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
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }, 
                    'lastWorkingDay': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$lastWorkingDay', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$lastWorkingDay', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$lastWorkingDay'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': ''
                    }, 
                    'lastWorkingDay': {
                        '$exists': True, 
                        '$ne': ''
                    }
                }
            }, {
                '$addFields': {
                    'start_date': '01-09-2023', 
                    'last_date': '29-08-2024'
                }
            }, {
                '$addFields': {
                    'start_date': {
                        '$dateFromString': {
                            'dateString': '$start_date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'last_date': {
                        '$dateFromString': {
                            'dateString': '$last_date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'start_date': {
                        '$dateAdd': {
                            'startDate': '$start_date', 
                            'unit': 'minute', 
                            'amount': 330
                        }
                    }, 
                    'last_date': {
                        '$dateAdd': {
                            'startDate': '$last_date', 
                            'unit': 'minute', 
                            'amount': 330
                        }
                    }, 
                    'orgLevel': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$orgLevel', ''
                                ]
                            }, 
                            'then': None, 
                            'else': '$orgLevel'
                        }
                    }
                }
            }, {
                '$project': {
                    'status': 1, 
                    'joiningDate': 1, 
                    'lastWorkingDay': 1, 
                    'start_date': 1, 
                    'last_date': 1, 
                    '_id': 0, 
                    'orgLevel': 1
                }
            }
        ]
        
        dara=cmo.finding_aggregate("userRegister",arra)['data']
        df=pd.DataFrame(dara)
        start_dates="01-01-2021" ###01-09-2023
        if 'orgLevel' in filter_criteria:
            if filter_criteria['orgLevel'] not in ['',None]:
                start_dates=start_date()
        Trendlast_dates=Trendlast_date() ####29-08-2024
        start_dates = pd.to_datetime(start_dates, dayfirst=True)
        trend_last_dates = pd.to_datetime(Trendlast_dates, dayfirst=True)
        all_months = pd.date_range(start=start_dates, end=trend_last_dates, freq='MS').to_period('M')
        all_months_df = pd.DataFrame(all_months, columns=['year_month'])
        monthly_stats = []
        for period in all_months_df['year_month']:
            if period == trend_last_dates.to_period('M'):
                end_of_month = trend_last_dates
            else:
                end_of_month = period.end_time
            total_active_employees_month = df[(df['joiningDate'] <= end_of_month)].shape[0]
            total_exit_employees_month = df[(df['lastWorkingDay'].notna()) & (df['lastWorkingDay'] <= end_of_month)].shape[0]
            monthly_stats.append({
                'year_month': period,
                'TotalActiveEmployee': total_active_employees_month,
                'totalExitEmployee': total_exit_employees_month
            })
        final_df = pd.DataFrame(monthly_stats)
        final_df['Year'] = final_df['year_month'].dt.year
        final_df['Month'] = final_df['year_month'].dt.month
        final_df['description'] = final_df['year_month'].dt.strftime('%b-%Y')
        final_df['count'] = final_df['TotalActiveEmployee'] - final_df['totalExitEmployee']
        final_df['description'] = final_df['year_month'].dt.strftime('%b-%Y')
        if 'year' in filter_criteria:
            if filter_criteria['year'] not in ['',None]:
                final_df=final_df[(final_df['Year'] == filter_criteria['year']) ]
        if 'month' in filter_criteria:
            if len(filter_criteria['month']) > 0:  
                final_df = final_df[final_df['Month'].isin(filter_criteria['month'])]
        df_simple = final_df.copy()
        for col in df_simple.columns:
            df_simple[col] = df_simple[col].astype(str)
        columns_to_drop = ['year_month', 'TotalActiveEmployee']
        df_simple = df_simple.drop(columns=columns_to_drop)
        data = json.loads(df_simple.to_json(orient='records'))
        
        # data = final_df.to_json(orient='records')
        # data=cfc.dfjson(final_df)
        Response={}
        Response['data']=data
        Response['status']=200
        Response['msg']="Data Get Successfully!"
        return respond(Response)


        
        
        
        
        
        

    


@graph_blueprint.route("/graph/weeklyActiveEmployee",methods=["POST","GET"])
@graph_blueprint.route("/graph/weeklyActiveEmployee/<id>",methods=["POST","GET"])
# @token_required
def weeklyActiveEmployee(id=None):
    if request.method == 'GET':
        arra=[
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
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }, 
                    'lastWorkingDay': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$lastWorkingDay', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$lastWorkingDay', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$lastWorkingDay'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': ''
                    }, 
                    'lastWorkingDay': {
                        '$exists': True, 
                        '$ne': ''
                    }
                }
            }, {
                '$addFields': {
                    'beginning_date': '01-01-1900', 
                    'last_week': last_week(), 
                    'current_week':current_week()
                }
            }, {
                '$addFields': {
                    'beginning_date': {
                        '$dateFromString': {
                            'dateString': '$beginning_date', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'last_week': {
                        '$dateFromString': {
                            'dateString': '$last_week', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'current_week': {
                        '$dateFromString': {
                            'dateString': '$current_week', 
                            'format': '%d-%m-%Y'
                        }
                    }
                }
            }, {
                '$facet': {
                    'current_week': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$joiningDate', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$joiningDate', '$current_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'joined1': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ], 
                    'last_week': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$joiningDate', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$joiningDate', '$last_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'joined2': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ], 
                    'exit_current_week': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$lastWorkingDay', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$lastWorkingDay', '$current_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'exit1': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ], 
                    'exit_lastweek': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$lastWorkingDay', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$lastWorkingDay', '$last_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'exit2': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ]
                }
            }, {
                '$addFields': {
                    'current_week': {
                        '$map': {
                            'input': '$current_week', 
                            'as': 'cw', 
                            'in': {
                                '$mergeObjects': [
                                    '$$cw', {
                                        'count1': {
                                            '$let': {
                                                'vars': {
                                                    'exitMatch': {
                                                        '$arrayElemAt': [
                                                            {
                                                                '$filter': {
                                                                    'input': '$exit_current_week', 
                                                                    'as': 'ecw', 
                                                                    'cond': {
                                                                        '$eq': [
                                                                            '$$ecw._id', '$$cw._id'
                                                                        ]
                                                                    }
                                                                }
                                                            }, 0
                                                        ]
                                                    }
                                                }, 
                                                'in': {
                                                    '$ifNull': [
                                                        '$$exitMatch.exit1', 0
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }, 
                    'last_week': {
                        '$map': {
                            'input': '$last_week', 
                            'as': 'cw', 
                            'in': {
                                '$mergeObjects': [
                                    '$$cw', {
                                        'count2': {
                                            '$let': {
                                                'vars': {
                                                    'exitMatch': {
                                                        '$arrayElemAt': [
                                                            {
                                                                '$filter': {
                                                                    'input': '$exit_lastweek', 
                                                                    'as': 'ecw', 
                                                                    'cond': {
                                                                        '$eq': [
                                                                            '$$ecw._id', '$$cw._id'
                                                                        ]
                                                                    }
                                                                }
                                                            }, 0
                                                        ]
                                                    }
                                                }, 
                                                'in': {
                                                    '$ifNull': [
                                                        '$$exitMatch.exit2', 0
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'exit_current_week': 0, 
                    'exit_lastweek': 0
                }
            }, {
                '$project': {
                    'mergedData': {
                        '$map': {
                            'input': '$current_week', 
                            'as': 'cw', 
                            'in': {
                                '$mergeObjects': [
                                    '$$cw', {
                                        '$arrayElemAt': [
                                            {
                                                '$filter': {
                                                    'input': '$last_week', 
                                                    'as': 'lw', 
                                                    'cond': {
                                                        '$eq': [
                                                            '$$cw._id', '$$lw._id'
                                                        ]
                                                    }
                                                }
                                            }, 0
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$unwind': {
                    'path': '$mergedData', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    'description': '$mergedData.description', 
                    'orgLevel': '$mergedData.orgLevel', 
                    'joined': {
                        '$subtract': [
                            '$mergedData.joined1', '$mergedData.count1'
                        ]
                    }, 
                    'exit': {
                        '$subtract': [
                            '$mergedData.joined2', '$mergedData.count2'
                        ]
                    }
                }
            }
        ]
        
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)
    
    if request.method == "POST":    
        description = request.json.get("description")
        orgLevel = request.json.get("orgLevel")

        data = request.get_json()

        # arra=[
        #     {
        #         '$match': {
        #             'deleteStatus': {
        #                 '$ne': 1
        #             }, 
        #             'type': {
        #                 '$ne': 'Partner'
        #             }
        #         }
        #     }
        # ]
        arra=[]
        if 'orgLevel' in data:
            arra = arra + [
            {
                '$match':{
                    'orgLevel':{
                        '$in':data['orgLevel']
                    }
                }
            }
        ]  
        arra = arra +[
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
                '$addFields': {
                    'joiningDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$joiningDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$joiningDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$joiningDate'
                            }
                        }
                    }, 
                    'lastWorkingDay': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$lastWorkingDay', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$lastWorkingDay', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$lastWorkingDay'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'joiningDate': {
                        '$exists': True, 
                        '$ne': ''
                    }, 
                    'lastWorkingDay': {
                        '$exists': True, 
                        '$ne': ''
                    }
                }
            }, {
                '$addFields': {
                    'beginning_date': '01-01-1900', 
                    'last_week': last_week(), 
                    'current_week':current_week()
                }
            }, {
                '$addFields': {
                    'beginning_date': {
                        '$dateFromString': {
                            'dateString': '$beginning_date', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'last_week': {
                        '$dateFromString': {
                            'dateString': '$last_week', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'current_week': {
                        '$dateFromString': {
                            'dateString': '$current_week', 
                            'format': '%d-%m-%Y'
                        }
                    }
                }
            }, {
                '$facet': {
                    'current_week': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$joiningDate', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$joiningDate', '$current_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'joined1': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ], 
                    'last_week': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$joiningDate', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$joiningDate', '$last_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'joined2': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ], 
                    'exit_current_week': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$lastWorkingDay', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$lastWorkingDay', '$current_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'exit1': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ], 
                    'exit_lastweek': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$lastWorkingDay', '$beginning_date'
                                            ]
                                        }, {
                                            '$lte': [
                                                '$lastWorkingDay', '$last_week'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'orgLevel': {
                                    '$ne': ''
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$orgLevel', 
                                'description': {
                                    '$first': '$orgLevel'
                                }, 
                                'orgLevel': {
                                    '$first': '$orgLevel'
                                }, 
                                'exit2': {
                                    '$sum': 1
                                }
                            }
                        }, {
                            '$sort': {
                                'description': 1
                            }
                        }
                    ]
                }
            }, {
                '$addFields': {
                    'current_week': {
                        '$map': {
                            'input': '$current_week', 
                            'as': 'cw', 
                            'in': {
                                '$mergeObjects': [
                                    '$$cw', {
                                        'count1': {
                                            '$let': {
                                                'vars': {
                                                    'exitMatch': {
                                                        '$arrayElemAt': [
                                                            {
                                                                '$filter': {
                                                                    'input': '$exit_current_week', 
                                                                    'as': 'ecw', 
                                                                    'cond': {
                                                                        '$eq': [
                                                                            '$$ecw._id', '$$cw._id'
                                                                        ]
                                                                    }
                                                                }
                                                            }, 0
                                                        ]
                                                    }
                                                }, 
                                                'in': {
                                                    '$ifNull': [
                                                        '$$exitMatch.exit1', 0
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }, 
                    'last_week': {
                        '$map': {
                            'input': '$last_week', 
                            'as': 'cw', 
                            'in': {
                                '$mergeObjects': [
                                    '$$cw', {
                                        'count2': {
                                            '$let': {
                                                'vars': {
                                                    'exitMatch': {
                                                        '$arrayElemAt': [
                                                            {
                                                                '$filter': {
                                                                    'input': '$exit_lastweek', 
                                                                    'as': 'ecw', 
                                                                    'cond': {
                                                                        '$eq': [
                                                                            '$$ecw._id', '$$cw._id'
                                                                        ]
                                                                    }
                                                                }
                                                            }, 0
                                                        ]
                                                    }
                                                }, 
                                                'in': {
                                                    '$ifNull': [
                                                        '$$exitMatch.exit2', 0
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'exit_current_week': 0, 
                    'exit_lastweek': 0
                }
            }, {
                '$project': {
                    'mergedData': {
                        '$map': {
                            'input': '$current_week', 
                            'as': 'cw', 
                            'in': {
                                '$mergeObjects': [
                                    '$$cw', {
                                        '$arrayElemAt': [
                                            {
                                                '$filter': {
                                                    'input': '$last_week', 
                                                    'as': 'lw', 
                                                    'cond': {
                                                        '$eq': [
                                                            '$$cw._id', '$$lw._id'
                                                        ]
                                                    }
                                                }
                                            }, 0
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$unwind': {
                    'path': '$mergedData', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    'description': '$mergedData.description', 
                    'orgLevel': '$mergedData.orgLevel', 
                    'joined': {
                        '$subtract': [
                            '$mergedData.joined1', '$mergedData.count1'
                        ]
                    }, 
                    'exit': {
                        '$subtract': [
                            '$mergedData.joined2', '$mergedData.count2'
                        ]
                    }
                }
            }
        ]
        # arra= arra + [
        #     {
        #     '$project': {
        #         'description': '$combined.description', 
        #         'orgLevel': '$combined.orgLevel', 
        #         'joined': '$combined.joined', 
        #         'exit': '$combined.exit'
        #     }
        # }
        # ]
        # if 'description' in data:
        #     arra = arra + [ 
        #     {
        #         '$match':{
        #             'description':{
        #                 '$in':data['description']
        #             }
        #         }
        #     }
        # ]      
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)



@graph_blueprint.route("/graph/getorgLevel",methods=["POST","GET"])
@graph_blueprint.route("/graph/getorgLevel/<id>",methods=["POST","GET"])
@token_required
def getorgLevel(current_user,id=None):
    if request.method == 'GET':
        arra=[
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
                '$match': {
                    'orgLevel': {
                        '$ne': ''
                    }
                }
            }, {
                '$group': {
                    '_id': '$orgLevel', 
                    'description': {
                        '$first': '$orgLevel'
                    }, 
                    'uniqueId': {
                        '$first': '$_id'
                    }
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$uniqueId'
                    }
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)


@graph_blueprint.route("/graph/workdoneRevenueTrend",methods=["POST","GET"])
@graph_blueprint.route("/graph/workdoneRevenueTrend/<id>",methods=["POST","GET"])
@token_required
def workdoneRevenueTrend(current_user, id=None):
    
    subuid = []
    subobjectuid = []
    arra = [
        {
            '$match': {
                'projectType': {
                    '$in': [
                        'ULS', 'MACRO', 'RELOCATION', 'UBR', 'DEGROW', 'MICROWAVE'
                    ]
                }
            }
        }, {
            '$project': {
                'subuid': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)['data']
    if response:
        for i in response:
            subuid.append(i['subuid'])
            subobjectuid.append(ObjectId(i['subuid']))
    
    if request.method == "GET":
        
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        
        projection_stage = {
            '_id': 0,
            'year':1,
        }
        
        for field in viewBy:
                projection_stage[f'M-{field}'] = 1
                
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
                '$match':{
                    'status': 'Active', 
                    'deleteStatus': {'$ne': 1},
                    'projectType':{'$in':subobjectuid}
                }
            }, {
                '$project': {
                    'projectId': 1, 
                    'projectuid': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            },
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
                    'result.projectuid': '$projectuid', 
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len(dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            dataRespdf.rename(columns={
                'M-1': '1', 
                'M-2': '2', 
                'M-3': '3', 
                'M-4': '4', 
                'M-5': '5', 
                'M-6': '6', 
                'M-7': '7', 
                'M-8': '8', 
                'M-9': '9', 
                'M-10': '10', 
                'M-11': '11', 
                'M-12': '12'
            }, inplace=True)
            df_cleaned = dataRespdf.dropna(subset=['year'])
            result = df_cleaned.groupby('year').sum()
            value_vars = result.columns.tolist()
            value_vars = sorted(value_vars, key=int)
            melted_df1 = pd.melt(result,  value_vars=value_vars, var_name='description', value_name='plan')
            year = int(year)
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
                'month': 1, 
                'year': 1, 
                '_id': 0
            }
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1}, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {'$ne': None},
                        "mileStoneStatus":"Closed",
                        "SubProjectId":{'$in':subuid},
                        'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
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
                result = ms2Datadf.groupby(['month','year']).size().reset_index(name='count')
                pivot_df = result.pivot_table(index=['year'],columns='month',values='count',fill_value=0 ).reset_index()
                pivot_df.columns = [f'count-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                pivot_df.rename(columns={
                    'count-1': '1', 
                    'count-2': '2', 
                    'count-3': '3', 
                    'count-4': '4', 
                    'count-5': '5', 
                    'count-6': '6', 
                    'count-7': '7', 
                    'count-8': '8', 
                    'count-9': '9', 
                    'count-10': '10', 
                    'count-11': '11', 
                    'count-12': '12'
                }, inplace=True)
                value_vars = pivot_df.columns.tolist()
                value_vars.remove('year')
                value_vars = sorted(value_vars, key=int)
                
                melted_df2 = pd.melt(pivot_df,  value_vars=value_vars,var_name='description', value_name='achievement')
                mergedDf = melted_df1.merge(melted_df2,on='description',how='left')
                month_map = {
                    "1": 'Jan', "2": 'Feb', "3": 'Mar', "4": 'Apr', "5": 'May',
                    "6": 'Jun', "7": 'Jul', "8": 'Aug', "9": 'Sep', "10": 'Oct',
                    "11": 'Nov', "12": 'Dec'
                }
                mergedDf['description'] = mergedDf['description'].map(month_map)
                mergedDf['ach'] = mergedDf.apply(lambda row: (row['achievement'] / row['plan']) * 100 if row['plan'] != 0 else 0, axis=1)
                mergedDf['ach'].replace([np.inf, -np.inf], np.nan, inplace=True)
                mergedDf['ach'] = mergedDf['ach'].round()
                mergedDf = mergedDf.replace(np.nan,0)
                final_data = json.loads(mergedDf.to_json(orient="records"))
            dataResp['data'] = final_data
            return respond(dataResp)
        
    if request.method == "POST":
        allData = request.get_json()
        
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        
        projection_stage = {
            '_id': 0,
            'year':1,
        }
        
        if "circleName" in allData:
            allData['circleName'] = [item for sublist in allData['circleName'] for item in sublist]
            projectId = []
            for i in allData['circleName']:
                projectId.append(ObjectId(i))
                
        if "projectType" in allData:
            projectTypeinObjectId = []
            allData['projectType'] = [item for sublist in allData['projectType'] for item in sublist]
            for i in allData['projectType']:
                projectTypeinObjectId.append(ObjectId(i))
                
        if "year" in allData:
            year = int(allData['year'])
            
        if "viewBy" in allData:
            viewBy = allData["viewBy"]   
            
        typeSelectional = allData["typeSelectional"]
        
        if typeSelectional == "Weekly":
            viwq = []
            if "viewBy" in allData:
                for i in allData['viewBy']:
                    viwq.append(int(i.replace("WK#", "")))
            else:
                viwq = []
                for i in range(52):
                    field = i + 1
                    viwq.append(field)
        
        
        if typeSelectional == "Monthly":
            for field in viewBy:
                projection_stage[f'M-{field}'] = 1
        else:
            for field in viwq:
                projection_stage[f'WK#{field:02d}'] = 1
                
                
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
                '$match':{
                    'status': 'Active', 
                    'deleteStatus': {'$ne': 1},
                    'projectType':{'$in':subobjectuid}
                }
            }
            ]
        if "circleName" in allData:
            arra = arra + [
                {
                    '$match':{
                        '_id':{
                            '$in':projectId
                        }
                    }
                }
            ]
        if "projectType" in allData:
            arra = arra + [
                {
                    '$match':{
                        'projectType':{
                            '$in':projectTypeinObjectId
                        }
                    }
                }
            ]
        arra = arra +[
            {
                '$project': {
                    'projectId': 1, 
                    'projectuid': {'$toString': '$_id'}, 
                    '_id': 0
                }
            },{
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
                    'result.projectuid': '$projectuid', 
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len(dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            if typeSelectional == "Monthly":
                dataRespdf.rename(columns={
                    'M-1': '1', 
                    'M-2': '2', 
                    'M-3': '3', 
                    'M-4': '4', 
                    'M-5': '5', 
                    'M-6': '6', 
                    'M-7': '7', 
                    'M-8': '8', 
                    'M-9': '9', 
                    'M-10': '10', 
                    'M-11': '11', 
                    'M-12': '12'
                }, inplace=True)
            else:
                dataRespdf.rename(columns={'WK#01': '1', 'WK#02': '2', 'WK#03': '3', 'WK#04': '4', 'WK#05': '5', 'WK#06': '6', 'WK#07': '7', 'WK#08': '8', 'WK#09': '9', 'WK#10': '10', 'WK#11': '11', 'WK#12': '12', 'WK#13': '13', 'WK#14': '14', 'WK#15': '15', 'WK#16': '16', 'WK#17': '17', 'WK#18': '18', 'WK#19': '19', 'WK#20': '20', 'WK#21': '21', 'WK#22': '22', 'WK#23': '23', 'WK#24': '24', 'WK#25': '25', 'WK#26': '26', 'WK#27': '27', 'WK#28': '28', 'WK#29': '29', 'WK#30': '30', 'WK#31': '31', 'WK#32': '32', 'WK#33': '33', 'WK#34': '34', 'WK#35': '35', 'WK#36': '36', 'WK#37': '37', 'WK#38': '38', 'WK#39': '39', 'WK#40': '40', 'WK#41': '41', 'WK#42': '42', 'WK#43': '43', 'WK#44': '44', 'WK#45': '45', 'WK#46': '46', 'WK#47': '47', 'WK#48': '48', 'WK#49': '49', 'WK#50': '50', 'WK#51': '51', 'WK#52': '52'}, inplace=True)
            df_cleaned = dataRespdf.dropna(subset=['year'])
            result = df_cleaned.groupby('year').sum()
            value_vars = result.columns.tolist()
            value_vars = sorted(value_vars, key=int)
            melted_df1 = pd.melt(result,  value_vars=value_vars,var_name='description', value_name='plan')
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
                    'week': 1, 
                    'year': 1, 
                    '_id': 0
                }
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1}, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {'$ne': None},
                        "mileStoneStatus":"Closed",
                        "SubProjectId":{'$in':subuid},
                        'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
                    }
                }
            ]
            if "circleName" in allData:
                arra = arra + [
                    {
                        '$match':{
                            'projectuniqueId':{
                                '$in':allData['circleName']
                            }
                        }
                    }
                ]
            if "projectType" in allData:
                arra = arra + [
                    {
                        '$match':{
                            'SubProjectId':{
                                '$in':allData['projectType']
                            }
                        }
                    }
                ]
            
            arra = arra +[
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
                    result = ms2Datadf.groupby(['month','year']).size().reset_index(name='count')
                    pivot_df = result.pivot_table(index=['year'],columns='month',values='count',fill_value=0 ).reset_index()
                    pivot_df.columns = [f'count-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                    pivot_df.rename(columns={
                        'count-1': '1', 
                        'count-2': '2', 
                        'count-3': '3', 
                        'count-4': '4', 
                        'count-5': '5', 
                        'count-6': '6', 
                        'count-7': '7', 
                        'count-8': '8', 
                        'count-9': '9', 
                        'count-10': '10', 
                        'count-11': '11', 
                        'count-12': '12'
                    }, inplace=True)
                else:
                    result = ms2Datadf.groupby(['week','year']).size().reset_index(name='count')
                    pivot_df = result.pivot_table(index=['year'],columns='week',values='count',fill_value=0 ).reset_index()
                    
                value_vars = pivot_df.columns.tolist()
                value_vars.remove('year')
                value_vars = sorted(value_vars, key=int)
                
                melted_df2 = pd.melt(pivot_df,  value_vars=value_vars,var_name='description', value_name='achievement')
                melted_df2['description'] = melted_df2['description'].astype(str)
                mergedDf = melted_df1.merge(melted_df2,on='description',how='left')
                month_map = {
                    "1": 'Jan', "2": 'Feb', "3": 'Mar', "4": 'Apr', "5": 'May',
                    "6": 'Jun', "7": 'Jul', "8": 'Aug', "9": 'Sep', "10": 'Oct',
                    "11": 'Nov', "12": 'Dec'
                }
                week_map ={'1': 'WK#01', '2': 'WK#02', '3': 'WK#03', '4': 'WK#04', '5': 'WK#05', '6': 'WK#06', '7': 'WK#07', '8': 'WK#08', '9': 'WK#09', '10': 'WK#10', '11': 'WK#11', '12': 'WK#12', '13': 'WK#13', '14': 'WK#14', '15': 'WK#15', '16': 'WK#16', '17': 'WK#17', '18': 'WK#18', '19': 'WK#19', '20': 'WK#20', '21': 'WK#21', '22': 'WK#22', '23': 'WK#23', '24': 'WK#24', '25': 'WK#25', '26': 'WK#26', '27': 'WK#27', '28': 'WK#28', '29': 'WK#29', '30': 'WK#30', '31': 'WK#31', '32': 'WK#32', '33': 'WK#33', '34': 'WK#34', '35': 'WK#35', '36': 'WK#36', '37': 'WK#37', '38': 'WK#38', '39': 'WK#39', '40': 'WK#40', '41': 'WK#41', '42': 'WK#42', '43': 'WK#43', '44': 'WK#44', '45': 'WK#45', '46': 'WK#46', '47': 'WK#47', '48': 'WK#48', '49': 'WK#49', '50': 'WK#50', '51': 'WK#51', '52': 'WK#52'} 
                if typeSelectional == "Monthly":
                    mergedDf['description'] = mergedDf['description'].map(month_map)
                else:
                    mergedDf['description'] = mergedDf['description'].map(week_map)
                mergedDf['ach'] = mergedDf.apply(lambda row: (row['achievement'] / row['plan']) * 100 if row['plan'] != 0 else 0, axis=1)
                mergedDf['ach'].replace([np.inf, -np.inf], np.nan, inplace=True)
                mergedDf['ach'] = mergedDf['ach'].round()
                mergedDf = mergedDf.replace(np.nan,0)
                final_data = json.loads(mergedDf.to_json(orient="records"))
            dataResp['data'] = final_data
            return respond(dataResp)
     

@graph_blueprint.route("/graph/workdoneRevenueCircle",methods=["POST","GET"])
@graph_blueprint.route("/graph/workdoneRevenueCircle/<id>",methods=["POST","GET"])
@token_required
def workdoneRevenueCircle(current_user, id=None):
    
    subuid = []
    subobjectuid = []
    arra = [
        {
            '$match': {
                'projectType': {
                    '$in': [
                        'ULS', 'MACRO', 'RELOCATION', 'UBR', 'DEGROW', 'MICROWAVE'
                    ]
                }
            }
        }, {
            '$project': {
                'subuid': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)['data']
    if response:
        for i in response:
            subuid.append(i['subuid'])
            subobjectuid.append(ObjectId(i['subuid']))
    
    
    if request.method == "GET":

        year = current_year()
        viewBy = [str(current_month())]

        projection_stage = {
            '_id': 0,
            'circleName':1,
        }

        for field in viewBy:
                projection_stage[f'M-{field}'] = 1
                
        
            
        
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
                                'deleteStatus': {'$ne': 1},
                                
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': '$result'
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$match':{
                    'status': 'Active', 
                    'deleteStatus': {'$ne': 1},
                    'projectType':{'$in':subobjectuid}
                }
            }, {
                '$addFields': {
                    'circle': {
                        '$toObjectId': '$circle'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'circle', 
                    'localField': 'circle', 
                    'foreignField': '_id', 
                    'pipeline':[{
                        '$match':{
                            'customer':ObjectId('667d593927f39f1ac03d7863'),
                            'deleteStatus':{'$ne':1}
                        }
                    }],
                    'as': 'circle'
                }
            }, {
                '$project': {
                    'projectId': 1,  
                    'circleName': {
                        '$arrayElemAt': [
                            '$circle.circleCode', 0
                        ]
                    }, 
                    'projectuid': {
                        '$toString': '$_id'
                    },
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'evmActual', 
                    'localField': 'projectuid', 
                    'foreignField': 'projectuid', 
                    'pipeline': [
                        {
                            '$match': {
                                'year': year
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
                    'result.projectuid': '$projectuid', 
                    'result.circleName': '$circleName', 
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len(dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            dataRespdf.rename(columns={
                'M-1': '1', 
                'M-2': '2', 
                'M-3': '3', 
                'M-4': '4', 
                'M-5': '5', 
                'M-6': '6', 
                'M-7': '7', 
                'M-8': '8', 
                'M-9': '9', 
                'M-10': '10', 
                'M-11': '11', 
                'M-12': '12'
            }, inplace=True)
            df_cleaned = dataRespdf.dropna(subset=['circleName'])
            df_cleaned.fillna(0, inplace=True)
            grouped_df = df_cleaned.groupby('circleName').sum().reset_index()
            result = grouped_df
            numeric_columns = [str(i) for i in viewBy]
            result['plan'] = result[numeric_columns].sum(axis=1)
            result1=result[['circleName','plan']]
            year = int(year)
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
                '_id': 0,
                'circleName':1,
                'projectuniqueId':1,
            }
            arra = [
                {
                    '$match':{
                        'custId':'667d593927f39f1ac03d7863'                     
                    }
                }, {
                    '$addFields': {
                        'circle': {
                            '$toObjectId': '$circle'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'circle', 
                        'localField': 'circle', 
                        'foreignField': '_id', 
                        'as': 'result'
                    }
                }, {
                    '$project': {
                        'projectuniqueId': {
                            '$toString': '$_id'
                        }, 
                        'circleName': {
                            '$arrayElemAt': [
                                '$result.circleCode', 0
                            ]
                        }, 
                        '_id': 0
                    }
                }
            ]
            circledf=pd.DataFrame(cmo.finding_aggregate("project",arra)['data'])
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1}, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {'$ne': None},
                        "mileStoneStatus":"Closed",
                        "SubProjectId":{'$in':subuid},
                        'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
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
            final_data = []
            if len(ms2Data):
                ms2Datadf = pd.DataFrame.from_dict(ms2Data)
                ms2Datadf = ms2Datadf.merge(circledf,on="projectuniqueId",how='left')
                del ms2Datadf['projectuniqueId']
                result2 = ms2Datadf.groupby(['circleName']).size().reset_index(name='achievement')
                mergedDf = pd.merge(result1,result2,on='circleName',how='outer')   
                mergedDf = mergedDf.rename(columns={'circleName': 'description'})
                mergedDf = mergedDf.fillna(0)
                mergedDf['ach'] = (mergedDf['achievement'] / mergedDf['plan']) * 100

                mergedDf['ach'].replace([np.inf, -np.inf], np.nan, inplace=True)
                mergedDf['ach'] = mergedDf['ach'].round()

                mergedDf = mergedDf.sort_values(by='ach', ascending=False)

                mergedDf = mergedDf.replace(np.nan,0)
                final_data = json.loads(mergedDf.to_json(orient="records"))

            dataResp['data'] = final_data
            return respond(dataResp)

    if request.method == "POST":
        allData = request.get_json()
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        projection_stage = {
            '_id': 0,
            'circleName':1,
        }
        if "circleName" in allData:
            allData['circleName'] = [item for sublist in allData['circleName'] for item in sublist]
            projectId = []
            for i in allData['circleName']:
                projectId.append(ObjectId(i))
        if "projectType" in allData:
            projectTypeinObjectId = []
            allData['projectType'] = [item for sublist in allData['projectType'] for item in sublist]
            for i in allData['projectType']:
                projectTypeinObjectId.append(ObjectId(i))
        if "year" in allData:
            year = int(allData['year'])
        typeSelectional = allData["typeSelectional"]
        if "viewBy" in allData:
            viewBy = allData["viewBy"]
        if typeSelectional == "Weekly":
            viwq = []
            if "viewBy" in allData:
                for i in allData['viewBy']:
                    viwq.append(int(i.replace("WK#", "")))
            else:
                viwq = []
                for i in range(52):
                    field = i + 1
                    viwq.append(field)
        
        if typeSelectional == "Monthly":
            for field in viewBy:
                projection_stage[f'M-{field}'] = 1
        else:
            for field in viwq:
                projection_stage[f'WK#{field:02d}'] = 1
                
        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            },  {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
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
                '$match':{
                    'status': 'Active', 
                    'deleteStatus': {'$ne': 1},
                    'projectType':{'$in':subobjectuid}
                }
            }
        ]
        if "circleName" in allData:
            arra = arra + [
                {
                    '$match':{
                        '_id':{
                            '$in':projectId
                        }
                    }
                }
            ]
        if "projectType" in allData:
            arra = arra + [
                {
                    '$match':{
                        'projectType':{
                            '$in':projectTypeinObjectId
                        }
                    }
                }
            ]
        arra = arra +[
            {
                '$addFields': {
                    'circle': {
                        '$toObjectId': '$circle'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'circle', 
                    'localField': 'circle', 
                    'foreignField': '_id', 
                    'as': 'circle'
                }
            }, {
                '$project': {
                    'projectId': 1,  
                    'circleName': {
                        '$arrayElemAt': [
                            '$circle.circleCode', 0
                        ]
                    }, 
                    'projectuid': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'evmActual', 
                    'localField': 'projectuid', 
                    'foreignField': 'projectuid', 
                    'pipeline': [
                        {
                            '$match': {
                                'year': year
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
                    'result.projectuid': '$projectuid', 
                    'result.circleName': '$circleName', 
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len(dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            if typeSelectional == "Monthly":
                dataRespdf.rename(columns={
                    'M-1': '1', 
                    'M-2': '2', 
                    'M-3': '3', 
                    'M-4': '4', 
                    'M-5': '5', 
                    'M-6': '6', 
                    'M-7': '7', 
                    'M-8': '8', 
                    'M-9': '9', 
                    'M-10': '10', 
                    'M-11': '11', 
                    'M-12': '12'
                }, inplace=True)
            else:
                dataRespdf.rename(columns={'WK#01': '1', 'WK#02': '2', 'WK#03': '3', 'WK#04': '4', 'WK#05': '5', 'WK#06': '6', 'WK#07': '7', 'WK#08': '8', 'WK#09': '9', 'WK#10': '10', 'WK#11': '11', 'WK#12': '12', 'WK#13': '13', 'WK#14': '14', 'WK#15': '15', 'WK#16': '16', 'WK#17': '17', 'WK#18': '18', 'WK#19': '19', 'WK#20': '20', 'WK#21': '21', 'WK#22': '22', 'WK#23': '23', 'WK#24': '24', 'WK#25': '25', 'WK#26': '26', 'WK#27': '27', 'WK#28': '28', 'WK#29': '29', 'WK#30': '30', 'WK#31': '31', 'WK#32': '32', 'WK#33': '33', 'WK#34': '34', 'WK#35': '35', 'WK#36': '36', 'WK#37': '37', 'WK#38': '38', 'WK#39': '39', 'WK#40': '40', 'WK#41': '41', 'WK#42': '42', 'WK#43': '43', 'WK#44': '44', 'WK#45': '45', 'WK#46': '46', 'WK#47': '47', 'WK#48': '48', 'WK#49': '49', 'WK#50': '50', 'WK#51': '51', 'WK#52': '52'}, inplace=True)
            df_cleaned = dataRespdf.dropna(subset=['circleName'])
            df_cleaned.fillna(0, inplace=True)
            grouped_df = df_cleaned.groupby('circleName').sum().reset_index()
            result = grouped_df
            value_vars = result.columns.tolist()
            if "circleName" in value_vars:
                value_vars.remove('circleName')
            value_vars = sorted(value_vars, key=int)
            if typeSelectional == "Monthly":
                numeric_columns = [str(i) for i in value_vars]
            else:
                numeric_columns = [str(i) for i in value_vars]
            result['plan'] = result[numeric_columns].sum(axis=1)
            result1=result[['circleName','plan']]
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
                    '_id': 0,
                    'circleName':1,
                    'projectuniqueId':1,
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
                    'circleName':1,
                    'projectuniqueId':1, 
                    '_id': 0
                }
              
            arra = [
                {
                    '$addFields': {
                        'circle': {
                            '$toObjectId': '$circle'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'circle', 
                        'localField': 'circle', 
                        'foreignField': '_id', 
                        'as': 'result'
                    }
                }, {
                    '$project': {
                        'projectuniqueId': {
                            '$toString': '$_id'
                        }, 
                        'circleName': {
                            '$arrayElemAt': [
                                '$result.circleCode', 0
                            ]
                        }, 
                        '_id': 0
                    }
                }
            ]
            circledf=pd.DataFrame(cmo.finding_aggregate("project",arra)['data'])
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1}, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {'$ne': None},
                        "mileStoneStatus":"Closed",
                        "SubProjectId":{'$in':subuid},
                        'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
                    }
                }
            ]
            if "circleName" in allData:
                arra = arra + [
                    {
                        '$match':{
                            'projectuniqueId':{
                                '$in':allData['circleName']
                            }
                        }
                    }
                ]
            if "projectType" in allData:
                arra = arra + [
                    {
                        '$match':{
                            'SubProjectId':{
                                '$in':allData['projectType']
                            }
                        }
                    }
                ]
            arra = arra +[
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
            final_data = []
            if len(ms2Data):
                ms2Datadf = pd.DataFrame.from_dict(ms2Data)
                ms2Datadf = ms2Datadf.merge(circledf,on="projectuniqueId",how='left')
                del ms2Datadf['projectuniqueId']
                result2 = ms2Datadf.groupby(['circleName']).size().reset_index(name='achievement')
                mergedDf = pd.merge(result1,result2,on='circleName',how='outer')   
                mergedDf = mergedDf.rename(columns={'circleName': 'description'})
                mergedDf = mergedDf.fillna(0)
                mergedDf['ach'] = (mergedDf['achievement'] / mergedDf['plan']) * 100
                mergedDf['ach'].replace([np.inf, -np.inf], np.nan, inplace=True)
                mergedDf['ach'] = mergedDf['ach'].round()
                mergedDf = mergedDf.sort_values(by='ach', ascending=False)

                mergedDf = mergedDf.replace(np.nan,0)
                final_data = json.loads(mergedDf.to_json(orient="records"))

            dataResp['data'] = final_data
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                "data":[],
                "msg":'Data get successfully'
            })


@graph_blueprint.route("/graph/revenuePlanVsActual",methods=["POST","GET"])
@graph_blueprint.route("/graph/revenuePlanVsActual/<id>",methods=["POST","GET"])
@token_required
def revenuePlanVsActual(current_user,id=None):
    if request.method == "GET":
    
        year = current_year()
        
        projection_stage = {
            '_id': 0, 
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}

        viewBy = [1,2,3,4,5,6,7,8,9,10,11,12]
        
        for field in viewBy:
            field = str(field)
            projection_stage['aop-'+field] = 1
            projection_stage['pv-'+field] = 1
            add_fields_stage['aop-'+field]={'$round':['$'+'aop-'+field]}
            add_fields_stage['pv-'+field]={'$round':['$'+'pv-'+field]}

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
            }, {
               '$addFields': add_fields_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            aop_sums = {}
            pv_sums = {}

            for i in range(1, 13): 
                column_name = f'aop-{i}'
                if column_name in dataRespdf.columns:
                    aop_sums[column_name] = dataRespdf[column_name].sum()

            for i in range(1, 13): 
                column_name = f'pv-{i}'
                if column_name in dataRespdf.columns:
                    pv_sums[column_name] = dataRespdf[column_name].sum()

            df_aop = pd.DataFrame([aop_sums])
            df_pv = pd.DataFrame([pv_sums])

            month = [int(x) for x in viewBy]
            year = int(year)
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1},
                        'projectId':{
                            '$in':projectId_str(current_user['userUniqueId'])
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
                pivot_df = dataRespdf.merge(pivot_df,on='projectgroupuid',how='left')

                amount_columns = [f'amount-{i}' for i in range(1, 13)]

                for col in amount_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0 

                pivot_df = pivot_df[amount_columns]

                amount_sums = {}

                for i in range(1, 13): 
                    column_name = f'amount-{i}'
                    if column_name in pivot_df.columns:
                        amount_sums[column_name] = pivot_df[column_name].sum()

                pivot_df = pd.DataFrame([amount_sums])

                df_amountSum = (pivot_df / 100000).round()
                df_aop_sum = (df_aop / 100000).round()
                df_pv_sum = (df_pv / 100000).round()
                
                df_ach = df_amountSum.div(df_pv_sum.values).mul(100).round(2)
                df_ach = df_ach.fillna(0)
                df_ach = df_ach.replace([np.inf, -np.inf], 0)
                df_ach = df_ach.astype(int)
                df_ach.columns = [f'ach-{i}' for i in range(1, 13)]
                
                df_percentage = df_amountSum.div(df_aop_sum.values).mul(100).round(2)
                df_percentage = df_percentage.fillna(0)
                df_percentage = df_percentage.replace([np.inf, -np.inf], 0)
                df_percentage = df_percentage.astype(int)
                df_percentage.columns = [f'percentage-{i}' for i in range(1, 13)]

                merged_df = pd.concat([df_amountSum, df_aop_sum, df_pv_sum,df_ach,df_percentage], axis=1)
                merged_df = merged_df.fillna(0)

                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                values = []
                for i in range(1 , current_month() + 1):
                    month_name = month_names[i]
                    row = {
                        'description': month_name,
                        'amount': merged_df.get(f'amount-{i}', [0])[0],
                        'pv': merged_df.get(f'pv-{i}', [0])[0],
                        'aop': merged_df.get(f'aop-{i}', [0])[0],
                        'ach': merged_df.get(f'ach-{i}', [0])[0],
                        'percentage': merged_df.get(f'percentage-{i}', [0])[0],
                    }
                    values.append(row)
                    new_df = pd.DataFrame(values)
                dataResp['data'] = json.loads(new_df.to_json(orient="records"))
            return respond(dataResp)   
            
        else:
            return respond({
                'status':200,
                "data":[],
            })

    if request.method == "POST":
        allData = request.get_json()
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        
        if "circleName" in allData:
            allData['circleName'] = [item for sublist in allData['circleName'] for item in sublist]
            projectId = []
            for i in allData['circleName']:
                projectId.append(ObjectId(i))
        if "year" in allData:
            year = allData['year']
        if "viewBy" in allData:
            viewBy = allData['viewBy']

        projection_stage = {
            '_id': 0, 
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}
        for field in viewBy:
            projection_stage['aop-'+field] = 1
            projection_stage['pv-'+field] = 1
            add_fields_stage['aop-'+field]={'$round':['$'+'aop-'+field]}
            add_fields_stage['pv-'+field]={'$round':['$'+'pv-'+field]}
        
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
                                'deleteStatus': {'$ne': 1},
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
            }]
        if "circleName" in allData:
            arra[1]['$lookup']['pipeline'].insert(0,{
                '$match':{
                    '_id':{
                        '$in':projectId
                    }
                }
            })
        arra = arra+[
            {
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
            }, {
               '$addFields': add_fields_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            aop_sums = {}
            pv_sums = {}
            month = [int(x) for x in viewBy]
            for i in month: 
                column_name = f'aop-{i}'
                if column_name in dataRespdf.columns:
                    aop_sums[column_name] = dataRespdf[column_name].sum()

            for i in month: 
                column_name = f'pv-{i}'
                if column_name in dataRespdf.columns:
                    pv_sums[column_name] = dataRespdf[column_name].sum()
                    
            df_aop = pd.DataFrame([aop_sums])
            df_pv = pd.DataFrame([pv_sums])
            year = int(year)
            projectId = projectId_str(current_user['userUniqueId'])
            if "circleName" in allData:
                projectId = allData['circleName']
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1},
                        'projectId':{
                            '$in':projectId
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
                pivot_df = dataRespdf.merge(pivot_df,on='projectgroupuid',how='left')

                amount_columns = [f'amount-{i}' for i in month]

                for col in amount_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0 

                pivot_df = pivot_df[amount_columns]

                amount_sums = {}

                for i in month: 
                    column_name = f'amount-{i}'
                    if column_name in pivot_df.columns:
                        amount_sums[column_name] = pivot_df[column_name].sum()

                pivot_df = pd.DataFrame([amount_sums])

                df_amountSum = (pivot_df / 100000).round()
                df_aop_sum = (df_aop / 100000).round()
                df_pv_sum = (df_pv / 100000).round()

                df_ach = df_amountSum.div(df_pv_sum.values).mul(100).round(2)
                df_ach = df_ach.fillna(0)
                df_ach = df_ach.replace([np.inf, -np.inf], 0)
                df_ach = df_ach.astype(int)
                df_ach.columns = [f'ach-{i}' for i in month]

                df_percentage = df_amountSum.div(df_aop_sum.values).mul(100).round(2)
                df_percentage = df_percentage.fillna(0)
                df_percentage = df_percentage.replace([np.inf, -np.inf], 0)
                df_percentage = df_percentage.astype(int)
                df_percentage.columns = [f'percentage-{i}' for i in month]
                
                
                merged_df = pd.concat([df_amountSum, df_aop_sum, df_pv_sum,df_ach,df_percentage], axis=1)
                merged_df = merged_df.fillna(0)

                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                values = []
                for i in month:
                    month_name = month_names[i]
                    row = {
                        'description': month_name,
                        'amount': merged_df.get(f'amount-{i}', [0])[0],
                        'pv': merged_df.get(f'pv-{i}', [0])[0],
                        'aop': merged_df.get(f'aop-{i}', [0])[0],
                        'ach': merged_df.get(f'ach-{i}', [0])[0],
                        'percentage': merged_df.get(f'percentage-{i}', [0])[0],
                    }
                    values.append(row)
                    new_df = pd.DataFrame(values)
                dataResp['data'] = json.loads(new_df.to_json(orient="records"))
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                "data":[],
            })
            
@graph_blueprint.route("/graph/revenuePlanVsActualCircle",methods=["POST","GET"])
@graph_blueprint.route("/graph/revenuePlanVsActualCircle/<id>",methods=["POST","GET"])
@token_required
def revenuePlanVsActualCircle(current_user, id=None):
    if request.method == "GET":
        year = current_year()
        viewBy = [str(current_month())]
        projection_stage = {
            '_id': 0, 
            'zone':1,
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}
        for field in viewBy:
            projection_stage['aop-'+field] = 1
            projection_stage['pv-'+field] = 1
            add_fields_stage['aop-'+field]={'$round':['$'+'aop-'+field]}
            add_fields_stage['pv-'+field]={'$round':['$'+'pv-'+field]}

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
                                                }, {
                                                    '$lookup': {
                                                        'from': 'zone', 
                                                        'localField': 'zone', 
                                                        'foreignField': '_id', 
                                                        'as': 'zoneresult'
                                                    }
                                                }, {
                                                    '$addFields': {
                                                        'zone': {
                                                            '$arrayElemAt': [
                                                                '$zoneresult.shortCode', 0
                                                            ]
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
                                            'zone': {
                                                '$arrayElemAt': [
                                                    '$costCenter.zone', 0
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
                                'zone': {
                                    '$arrayElemAt': [
                                        '$result.zone', 0
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
            },  {
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
                    'result.projectgroupuid': '$projectgroupuid',
                    'result.zone': '$zone'
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
            {
               '$addFields': add_fields_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            grouped_df = dataRespdf.groupby('zone').sum().reset_index()
            result = grouped_df.fillna(0)
            numeric_columns = [f'aop-{i}' for i in viewBy]
            result['aop'] = result[numeric_columns]
            numeric_columns1 = [f'pv-{i}' for i in viewBy]
            result['pv'] = result[numeric_columns1]
            result1=result[['zone','aop',"pv","projectgroupuid"]]

            month = [int(x) for x in viewBy]
            year = int(year)
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        'projectGroup':{
                            '$in':projectGroup_str(current_user['userUniqueId'])
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
                    '$addFields': {
                        'amount': {
                            '$multiply': [
                                '$qty', '$unitRate'
                            ]
                        }
                    }
                },{
                    '$project':{
                        'projectgroupuid':'$projectGroup',
                        'amount':1,
                    }
                }
            ]
            invoiceData = cmo.finding_aggregate("invoice",arra)['data']
            if len(invoiceData):
                invoicedf = pd.DataFrame.from_dict(invoiceData)
                result2 = invoicedf.groupby(['projectgroupuid'], as_index=False)['amount'].sum()
                mergedDf = pd.merge(result1,result2,on='projectgroupuid',how='outer')
                mergedDf = mergedDf.rename(columns={'zone': 'description'})
                descriptions = ['SRD', 'ASS', 'ORN', 'GEN', 'PRS', 'NRD']
                mergedDf = mergedDf[~mergedDf['description'].isin(descriptions)]
                mergedDf = mergedDf.fillna(0)

                mergedDf['aop'] = (mergedDf['aop'] / 100000).round()
                mergedDf['pv'] = (mergedDf['pv'] / 100000).round()
                mergedDf['amount'] = (mergedDf['amount'] / 100000).round()

                mergedDf['ach'] = mergedDf.apply(lambda row: (row['amount'] / row['pv'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['ach'] = mergedDf['ach'].replace([np.inf, -np.inf], 0)
                mergedDf['ach'] = mergedDf['ach'].astype(int)

                mergedDf = mergedDf.sort_values(by='ach', ascending=False)

                mergedDf['percentage'] = mergedDf.apply(lambda row: (row['amount'] / row['aop'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['percentage'] = mergedDf['percentage'].replace([np.inf, -np.inf], 0)
                mergedDf['percentage'] = mergedDf['percentage'].astype(int)

                final_result = mergedDf[['description', 'aop', 'pv', 'amount','ach','percentage']].round(2)
                final_result = json.loads(final_result.to_json(orient="records"))
            else:
                mergedDf = result1
                mergedDf = mergedDf.rename(columns={'zone': 'description'})
                descriptions = ['SRD', 'ASS', 'ORN', 'GEN', 'PRS', 'NRD']
                mergedDf = mergedDf[~mergedDf['description'].isin(descriptions)]

                mergedDf = mergedDf.fillna(0)

                mergedDf['aop'] = (mergedDf['aop'] / 100000).round()
                mergedDf['pv'] = (mergedDf['pv'] / 100000).round()
                mergedDf['amount'] = 0

                mergedDf['ach'] = mergedDf.apply(lambda row: (row['amount'] / row['pv'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['ach'] = mergedDf['ach'].replace([np.inf, -np.inf], 0)
                mergedDf['ach'] = mergedDf['ach'].astype(int)

                mergedDf['percentage'] = mergedDf.apply(lambda row: (row['amount'] / row['aop'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['percentage'] = mergedDf['percentage'].replace([np.inf, -np.inf], 0)
                mergedDf['percentage'] = mergedDf['percentage'].astype(int)

                final_result = mergedDf[['description', 'aop', 'pv','amount','ach','percentage']].round(2)
                final_result = json.loads(final_result.to_json(orient="records"))

            dataResp['data'] = final_result
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                'data':[]
            })
        
    if request.method == "POST":
        allData = request.get_json()
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        if "year" in allData:
            year = allData['year']

        if "viewBy" in allData:
            viewBy = allData['viewBy']
        else:
            viewBy = [str(current_month())]

                 
        projection_stage = {
            '_id': 0, 
            'zone':1,
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}

        add_fields_stage = {}
        for field in viewBy:
            projection_stage['aop-'+field] = 1
            projection_stage['pv-'+field] = 1
            add_fields_stage['aop-'+field]={'$round':['$'+'aop-'+field]}
            add_fields_stage['pv-'+field]={'$round':['$'+'pv-'+field]}

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
                                                }, {
                                                    '$lookup': {
                                                        'from': 'zone', 
                                                        'localField': 'zone', 
                                                        'foreignField': '_id', 
                                                        'as': 'zoneresult'
                                                    }
                                                }, {
                                                    '$addFields': {
                                                        'zone': {
                                                            '$arrayElemAt': [
                                                                '$zoneresult.shortCode', 0
                                                            ]
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
                                            'zone': {
                                                '$arrayElemAt': [
                                                    '$costCenter.zone', 0
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
                                'zone': {
                                    '$arrayElemAt': [
                                        '$result.zone', 0
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
            }
        ] 
        if 'projectgroupuid' in allData:
            arra = arra + [
                {
                    '$match':{
                        'projectgroupuid':{
                            '$in':allData['projectgroupuid']
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
                    'result.projectgroupuid': '$projectgroupuid',
                    'result.zone': '$zone'
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
            {
               '$addFields': add_fields_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            grouped_df = dataRespdf.groupby('zone').sum().reset_index()
            result = grouped_df.fillna(0)

            month = [int(x) for x in viewBy]

            if current_month() in month:
                numeric_columns_aop = [f'aop-{i}' for i in month]
                result['aop'] = result[numeric_columns_aop].sum(axis=1)

                numeric_columns_pv = [f'pv-{i}' for i in month]
                result['pv'] = result[numeric_columns_pv].sum(axis=1)
            else:
                aop_columns = [f'aop-{i}' for i in month]
                result['aop'] = result[aop_columns].sum(axis=1)

                pv_columns = [f'pv-{i}' for i in month]
                result['pv'] = result[pv_columns].sum(axis=1)

            result1=result[['zone','aop',"pv","projectgroupuid"]]

            month = [int(x) for x in viewBy]
            year = int(year)
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        'projectGroup':{
                            '$in':projectGroup_str(current_user['userUniqueId'])
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
                    '$addFields': {
                        'amount': {
                            '$multiply': [
                                '$qty', '$unitRate'
                            ]
                        }
                    }
                },{
                    '$project':{
                        'projectgroupuid':'$projectGroup',
                        'amount':1,
                    }
                }
            ]
            if 'projectgroupuid' in allData:
                arra = arra + [
                    {
                        '$match':{
                            'projectgroupuid':{
                                '$in':allData['projectgroupuid']
                            }
                        }
                    }
            ]
            invoiceData = cmo.finding_aggregate("invoice",arra)['data']
            if len(invoiceData):
                invoicedf = pd.DataFrame.from_dict(invoiceData)
                result2 = invoicedf.groupby(['projectgroupuid'], as_index=False)['amount'].sum()
                mergedDf = pd.merge(result1,result2,on='projectgroupuid',how='outer')
                mergedDf = mergedDf.rename(columns={'zone': 'description'})
                descriptions = ['SRD', 'ASS', 'ORN', 'GEN', 'PRS', 'NRD']
                mergedDf = mergedDf[~mergedDf['description'].isin(descriptions)]
                mergedDf = mergedDf.fillna(0)

                mergedDf['aop'] = (mergedDf['aop'] / 100000).round()
                mergedDf['pv'] = (mergedDf['pv'] / 100000).round()
                mergedDf['amount'] = (mergedDf['amount'] / 100000).round()

                mergedDf['ach'] = mergedDf.apply(lambda row: (row['amount'] / row['pv'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['ach'] = mergedDf['ach'].replace([np.inf, -np.inf], 0)
                mergedDf['ach'] = mergedDf['ach'].astype(int)

                mergedDf = mergedDf.sort_values(by='ach', ascending=False)

                mergedDf['percentage'] = mergedDf.apply(lambda row: (row['amount'] / row['aop'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['percentage'] = mergedDf['percentage'].replace([np.inf, -np.inf], 0)
                mergedDf['percentage'] = mergedDf['percentage'].astype(int)

                final_result = mergedDf[['description', 'aop', 'pv', 'amount','ach','percentage']].round(2)
                final_result = json.loads(final_result.to_json(orient="records"))
            else:
                mergedDf = result1
                mergedDf = mergedDf.rename(columns={'zone': 'description'})
                descriptions = ['SRD', 'ASS', 'ORN', 'GEN', 'PRS', 'NRD']
                mergedDf = mergedDf[~mergedDf['description'].isin(descriptions)]

                mergedDf = mergedDf.fillna(0)

                mergedDf['aop'] = (mergedDf['aop'] / 100000).round()
                mergedDf['pv'] = (mergedDf['pv'] / 100000).round()
                mergedDf['amount'] = 0

                mergedDf['ach'] = mergedDf.apply(lambda row: (row['amount'] / row['pv'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['ach'] = mergedDf['ach'].replace([np.inf, -np.inf], 0)
                mergedDf['ach'] = mergedDf['ach'].astype(int)

                mergedDf['percentage'] = mergedDf.apply(lambda row: (row['amount'] / row['aop'] * 100) if row['pv'] != 0 else 0, axis=1)
                mergedDf['percentage'] = mergedDf['percentage'].replace([np.inf, -np.inf], 0)
                mergedDf['percentage'] = mergedDf['percentage'].astype(int)

                final_result = mergedDf[['description', 'aop', 'pv','amount','ach','percentage']].round(2)
                final_result = json.loads(final_result.to_json(orient="records"))

            dataResp['data'] = final_result
            return respond(dataResp)


@graph_blueprint.route("/graph/trendPlanVsActualCumulative",methods=["POST","GET"])
@graph_blueprint.route("/graph/trendPlanVsActualCumulative/<id>",methods=["POST","GET"])
@token_required
def trendPlanVsActualCumulative(current_user,id=None):
    if request.method == "GET":
        
        year = current_year()
        
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        
        projection_stage = {
            '_id': 0, 
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}
        
        for field in viewBy:
            projection_stage['aop-' + (field)] = 1
            projection_stage['pv-'+ (field)] = 1
            add_fields_stage['aop-'+ (field)]={'$round':['$'+'aop-'+(field)]}
            add_fields_stage['pv-'+ (field)]={'$round':['$'+'pv-'+(field)]}

        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, 
            {
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
            }, {
               '$addFields': add_fields_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])

            aop_sums = {}
            pv_sums = {}

            for i in range(1, 13): 
                column_name = f'aop-{i}'
                if column_name in dataRespdf.columns:
                    aop_sums[column_name] = dataRespdf[column_name].sum()

            for i in range(1, 13): 
                column_name = f'pv-{i}'
                if column_name in dataRespdf.columns:
                    pv_sums[column_name] = dataRespdf[column_name].sum()

            df_aop = pd.DataFrame([aop_sums])
            df_pv = pd.DataFrame([pv_sums])

            df_aop_cumsum = df_aop.cumsum(axis=1)
            df_pv_cumsum = df_pv.cumsum(axis=1)

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
                result = invoicedf.groupby(['month', 'year','projectgroupuid'], as_index=False)['amount'].sum()
                pivot_df = result.pivot_table(index=['year','projectgroupuid'],columns='month',values='amount',fill_value=0  ).reset_index()
                pivot_df.columns = [f'amount-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                pivot_df.drop(columns=['year'], inplace=True)
                pivot_df = dataRespdf.merge(pivot_df,on='projectgroupuid',how='left')

                amount_columns = [f'amount-{i}' for i in range(1, 13)]

                for col in amount_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0 

                pivot_df = pivot_df[amount_columns]

                amount_sums = {}

                for i in range(1, 13): 
                    column_name = f'amount-{i}'
                    if column_name in pivot_df.columns:
                        amount_sums[column_name] = pivot_df[column_name].sum()

                pivot_df = pd.DataFrame([amount_sums])

                df_cumsum = pivot_df.cumsum(axis=1)
                df_cumsum = (df_cumsum / 100000).round()
                df_aop_cumsum = (df_aop_cumsum / 100000).round()
                df_pv_cumsum = (df_pv_cumsum / 100000).round()

                df_ach = df_cumsum.div(df_pv_cumsum.values).mul(100).round(2)
                df_ach = df_ach.fillna(0)
                df_ach = df_ach.replace([np.inf, -np.inf], 0)
                df_ach = df_ach.astype(int)
                df_ach.columns = [f'ach-{i}' for i in range(1, 13)]

                df_percentage = df_cumsum.div(df_aop_cumsum.values).mul(100).round(2)
                df_percentage = df_percentage.fillna(0)
                df_percentage = df_percentage.replace([np.inf, -np.inf], 0)
                df_percentage = df_percentage.astype(int)
                df_percentage.columns = [f'percentage-{i}' for i in range(1, 13)]

                merged_df = pd.concat([df_cumsum, df_aop_cumsum, df_pv_cumsum,df_ach,df_percentage], axis=1)
                merged_df = merged_df.fillna(0)
                
                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                values = []
                for i in range(1,current_month() + 1):
                    month_name = month_names[i]
                    row = {
                        'description': month_name,
                        'amount': merged_df.get(f'amount-{i}', [0])[0],
                        'pv': merged_df.get(f'pv-{i}', [0])[0],
                        'aop': merged_df.get(f'aop-{i}', [0])[0],
                        'ach': merged_df.get(f'ach-{i}', [0])[0],
                        'percentage': merged_df.get(f'percentage-{i}', [0])[0],

                    }
                    values.append(row)

                new_df = pd.DataFrame(values)
                dataResp['data'] = json.loads(new_df.to_json(orient="records"))
            return respond(dataResp)
            
        else:
            return respond({
                'status':200,
                "data":[],
            })

    if request.method == "POST":
        allData = request.get_json()
        year = current_year()

        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']

        if "year" in allData:
            year = allData['year']

        if "viewBy" in allData:
            viewBy = allData['viewBy']

        projection_stage = {
            '_id': 0, 
            "customer":1,
            "costCenter":1,
            "uniqueId":1,
            "projectgroupuid":1,
            "year":{'$toInt':'$year'} 
        }
        add_fields_stage = {}
        for field in viewBy:
            projection_stage['aop-' + (field)] = 1
            projection_stage['pv-'+ (field)] = 1
            add_fields_stage['aop-'+ (field)]={'$round':['$'+'aop-'+(field)]}
            add_fields_stage['pv-'+ (field)]={'$round':['$'+'pv-'+(field)]}

        arra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, 
            {
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
            }, {
               '$addFields': add_fields_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])

            aop_sums = {}
            pv_sums = {}

            month = [int(x) for x in viewBy]

            for i in month: 
                column_name = f'aop-{i}'
                if column_name in dataRespdf.columns:
                    aop_sums[column_name] = dataRespdf[column_name].sum()

            for i in month: 
                column_name = f'pv-{i}'
                if column_name in dataRespdf.columns:
                    pv_sums[column_name] = dataRespdf[column_name].sum()

            df_aop = pd.DataFrame([aop_sums])
            df_pv = pd.DataFrame([pv_sums])

            df_aop_cumsum = df_aop.cumsum(axis=1)
            df_pv_cumsum = df_pv.cumsum(axis=1)

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
                result = invoicedf.groupby(['month', 'year','projectgroupuid'], as_index=False)['amount'].sum()
                pivot_df = result.pivot_table(index=['year','projectgroupuid'],columns='month',values='amount',fill_value=0  ).reset_index()
                pivot_df.columns = [f'amount-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                pivot_df.drop(columns=['year'], inplace=True)
                pivot_df = dataRespdf.merge(pivot_df,on='projectgroupuid',how='left')

                amount_columns = [f'amount-{i}' for i in month]

                for col in amount_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0 

                pivot_df = pivot_df[amount_columns]

                amount_sums = {}

                for i in month: 
                    column_name = f'amount-{i}'
                    if column_name in pivot_df.columns:
                        amount_sums[column_name] = pivot_df[column_name].sum()

                pivot_df = pd.DataFrame([amount_sums])

                df_cumsum = pivot_df.cumsum(axis=1)
                df_cumsum = (df_cumsum / 100000).round()
                df_aop_cumsum = (df_aop_cumsum / 100000).round()
                df_pv_cumsum = (df_pv_cumsum / 100000).round()

                df_ach = df_cumsum.div(df_pv_cumsum.values).mul(100).round(2)
                df_ach = df_ach.fillna(0)
                df_ach = df_ach.replace([np.inf, -np.inf], 0)
                df_ach = df_ach.astype(int)
                df_ach.columns = [f'ach-{i}' for i in month]

                df_percentage = df_cumsum.div(df_aop_cumsum.values).mul(100).round(2)
                df_percentage = df_percentage.fillna(0)
                df_percentage = df_percentage.replace([np.inf, -np.inf], 0)
                df_percentage = df_percentage.astype(int)
                df_percentage.columns = [f'percentage-{i}' for i in month]

                merged_df = pd.concat([df_cumsum, df_aop_cumsum, df_pv_cumsum,df_ach,df_percentage], axis=1)
                merged_df = merged_df.fillna(0)
                
                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                values = []
                for i in month:
                    month_name = month_names[i]
                    row = {
                        'description': month_name,
                        'amount': merged_df.get(f'amount-{i}', [0])[0],
                        'pv': merged_df.get(f'pv-{i}', [0])[0],
                        'aop': merged_df.get(f'aop-{i}', [0])[0],
                        'ach': merged_df.get(f'ach-{i}', [0])[0],
                        'percentage': merged_df.get(f'percentage-{i}', [0])[0],

                    }
                    values.append(row)

                new_df = pd.DataFrame(values)
                dataResp['data'] = json.loads(new_df.to_json(orient="records"))
            return respond(dataResp)
            
        else:
            return respond({
                'status':200,
                "data":[],
            })


@graph_blueprint.route("/graph/getorganationLevel",methods=["POST","GET"])
@graph_blueprint.route("/graph/getorganationLevel/<id>",methods=["POST","GET"])
@token_required
def getorganationLevel(current_user,id=None):
    if request.method == 'GET':
        arra=[
            {
                '$match': {
                    '$and': [
                        {
                            'orgLevel': {
                                '$ne': ''
                            }
                        }, {
                            'orgLevel': {
                                '$ne': None
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$orgLevel', 
                    'orgLevel': {
                        '$first': '$orgLevel'
                    }
                }
            }, {
                '$sort': {
                    'orgLevel': 1
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)

@graph_blueprint.route("/graph/partnerStatus",methods=["POST","GET"])
@graph_blueprint.route("/graph/partnerStatus/<id>",methods=["POST","GET"])
@token_required
def partnerStatus(current_user,id=None):
    if request.method == 'GET':
        arra=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'type': {
                        '$eq': 'Partner'
                    }, 
                    'status': {
                        '$ne': ''
                    }
                }
            }, {
                '$group': {
                    '_id': '$status', 
                    'status': {
                        '$first': '$status'
                    }, 
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("userRegister",arra)
        return respond(Response)
    if request.method == "POST":
        data=request.get_json()
        circleCode=[]
        if 'circleCode' in data:
            circleCode=data['circleCode']
            arr=[
                    {
                        '$match': {
                            'type': 'Partner', 
                            'deleteStatus': {
                                '$ne': 1
                            }
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
                                    '$addFields': {
                                        'circle': {
                                            '$toObjectId': '$circle'
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
                                            }, {
                                                '$project': {
                                                    'circleName': 1, 
                                                    'circleCode': 1, 
                                                    '_id': 0
                                                }
                                            }
                                        ], 
                                        'as': 'circleresult'
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$circleresult', 
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$project': {
                                        'circle': {
                                            '$toString': '$circle'
                                        }, 
                                        'circleName': '$circleresult.circleName', 
                                        'circleCode': '$circleresult.circleCode'
                                    }
                                }, {
                                    '$group': {
                                        '_id': '$circleCode'
                                    }
                                }, {
                                    '$addFields': {
                                        'circleCode': '$_id'
                                    }
                                }, {
                                    '$project': {
                                        '_id': 0
                                    }
                                }
                            ], 
                            'as': 'result'
                        }
                    }, {
                        '$unwind': {
                            'path': '$result', 
                            'preserveNullAndEmptyArrays': False
                        }
                    }, {
                        '$addFields': {
                            'empId': {
                                '$toObjectId': '$empId'
                            }
                        }
                    }, {
                        '$lookup': {
                            'from': 'userRegister', 
                            'localField': 'empId', 
                            'foreignField': '_id', 
                            'pipeline': [
                                {
                                    '$match': {
                                        'type': 'Partner', 
                                        'deleteStatus': {
                                            '$ne': 1
                                        }
                                    }
                                }, {
                                    '$project': {
                                        'email': 1, 
                                        'vendorName': 1, 
                                        'vendorCode': 1
                                    }
                                }
                            ], 
                            'as': 'PartnerResults'
                        }
                    }, {
                        '$unwind': {
                            'path': '$PartnerResults', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$addFields': {
                            'circleCode': '$result.circleCode', 
                            'email': '$PartnerResults.email'
                        }
                    }, {
                        '$match': {
                            'circleCode': {
                                '$in': circleCode
                            }
                        }
                    }, {
                        '$group': {
                            '_id': '$circleCode', 
                            'count': {
                                '$sum': 1
                            }
                        }
                    }
                ]
            Response=cmo.finding_aggregate('projectAllocation',arr)['data']
            if len(Response):
                count=0
                for i in Response:
                    count=count+i['count']
                data={}
                data['count']=count
                data['status']='Active'
                Response={}
                Response['data']=[]
                Response['data'].append(data)
                Response['status']=200
                Response['msg']='Data get Successfully'
                return (Response)
                
                
        


@graph_blueprint.route('/graph/getAllprojectType',methods=["GET","POST"])
def getAllprojectType():
    if request.method == "GET":
        arra = [
            {
                '$match':{
                    'custId':'667d593927f39f1ac03d7863',
                    'deleteStatus':{"$ne":1},
                }
            },{
                '$group': {
                    '_id': '$projectType', 
                    'projectType': {
                        '$first': '$projectType'
                    }
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }, {
                '$sort': {
                    'projectType': 1
                }
            }
        ]
        Response = cmo.finding_aggregate("projectType", arra)
        return respond(Response)


@graph_blueprint.route('/graph/trendExpenseAdvance',methods=["GET","POST"])
@token_required
def trendExpenseAdvance(current_user):
    if request.method == "GET":
        arry=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }, 
                        '_id': ObjectId(current_user['userUniqueId'])
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
                            }, {
                                '$project': {
                                    'roleName': 1,
                                    '_id':0
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
                        'userRole': '$result.roleName'
                    }
                }
            ]
        userRole=cmo.finding_aggregate('userRegister',arry)['data']
        if len(userRole):
            current_user['userRoleName']=userRole[0]['userRole']
        Response=None
        if current_user['userRoleName'] in ['Admin','Finance','PMO']:
            arra=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            'customisedStatus': 6, 
                            'customStatus': 'L3-Approved'
                        }
                    }, {
                    '$addFields': {
                        'actionAt': {
                            '$cond': {
                                'if': {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$actionAt', None
                                            ]
                                        }, {
                                            '$eq': [
                                                '$actionAt', ''
                                            ]
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$toDate': '$actionAt'
                                }
                            }
                        }
                    }
                }, {
                    '$match': {
                        'actionAt': {
                            '$exists': True, 
                            '$ne': ''
                        }
                    }
                }, {
                    '$addFields': {
                        'start_date': {
                            '$dateFromString': {
                                'dateString': start_date(), 
                                'format': '%d-%m-%Y'
                            }
                        }, 
                        'last_date': {
                            '$dateFromString': {
                                'dateString': last_date(), 
                                'format': '%d-%m-%Y'
                            }
                        }
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$gte': [
                                        '$actionAt', '$start_date'
                                    ]
                                }, {
                                    '$lte': [
                                        '$actionAt', '$last_date'
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'month': {
                            '$month': '$actionAt'
                        }
                    }
                }, {
                    '$facet': {
                        'ExpAmount': [
                            {
                                '$match': {
                                    'type': 'Expense'
                                }
                            }, {
                                '$addFields': {
                                    'month': {
                                        '$cond': {
                                            'if': {
                                                '$gt': [
                                                    {
                                                        '$dayOfMonth': '$actionAt'
                                                    }, 25
                                                ]
                                            }, 
                                            'then': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, 
                                                    'then': 1, 
                                                    'else': {
                                                        '$add': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 1
                                                        ]
                                                    }
                                                }
                                            }, 
                                            'else': {
                                                '$month': '$actionAt'
                                            }
                                        }
                                    }, 
                                    'Year': {
                                        '$cond': {
                                            'if': {
                                                '$and': [
                                                    {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, {
                                                        '$gt': [
                                                            {
                                                                '$dayOfMonth': '$actionAt'
                                                            }, 25
                                                        ]
                                                    }
                                                ]
                                            }, 
                                            'then': {
                                                '$add': [
                                                    {
                                                        '$year': '$actionAt'
                                                    }, 1
                                                ]
                                            }, 
                                            'else': {
                                                '$year': '$actionAt'
                                            }
                                        }
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': '$month', 
                                    'ExpApprovedAmount': {
                                        '$sum': '$ApprovedAmount'
                                    }, 
                                    'Year': {
                                        '$first': '$Year'
                                    }, 
                                    'month': {
                                        '$first': '$month'
                                    }
                                }
                            }
                        ], 
                        'AdvAmount': [
                            {
                                '$match': {
                                    'type': 'Advance'
                                }
                            }, {
                                '$addFields': {
                                    'month': {
                                        '$cond': {
                                            'if': {
                                                '$gt': [
                                                    {
                                                        '$dayOfMonth': '$actionAt'
                                                    }, 25
                                                ]
                                            }, 
                                            'then': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, 
                                                    'then': 1, 
                                                    'else': {
                                                        '$add': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 1
                                                        ]
                                                    }
                                                }
                                            }, 
                                            'else': {
                                                '$month': '$actionAt'
                                            }
                                        }
                                    }, 
                                    'Year': {
                                        '$cond': {
                                            'if': {
                                                '$and': [
                                                    {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, {
                                                        '$gt': [
                                                            {
                                                                '$dayOfMonth': '$actionAt'
                                                            }, 25
                                                        ]
                                                    }
                                                ]
                                            }, 
                                            'then': {
                                                '$add': [
                                                    {
                                                        '$year': '$actionAt'
                                                    }, 1
                                                ]
                                            }, 
                                            'else': {
                                                '$year': '$actionAt'
                                            }
                                        }
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': '$month', 
                                    'AdvApprovedAmount': {
                                        '$sum': '$ApprovedAmount'
                                    }, 
                                    'Year': {
                                        '$first': '$Year'
                                    }, 
                                    'month': {
                                        '$first': '$month'
                                    }
                                }
                            }
                        ]
                    }
                }, {
                    '$project': {
                        'combinedData': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                {
                                                    '$size': '$ExpAmount'
                                                }, 0
                                            ]
                                        }, {
                                            '$eq': [
                                                {
                                                    '$size': '$AdvAmount'
                                                }, 0
                                            ]
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                {
                                                    '$size': '$ExpAmount'
                                                }, 0
                                            ]
                                        }, 
                                        'then': '$AdvAmount', 
                                        'else': {
                                            '$cond': {
                                                'if': {
                                                    '$eq': [
                                                        {
                                                            '$size': '$AdvAmount'
                                                        }, 0
                                                    ]
                                                }, 
                                                'then': '$ExpAmount', 
                                                'else': {
                                                    '$map': {
                                                        'input': '$ExpAmount', 
                                                        'as': 'exp', 
                                                        'in': {
                                                            '$mergeObjects': [
                                                                '$$exp', {
                                                                    '$arrayElemAt': [
                                                                        {
                                                                            '$filter': {
                                                                                'input': '$AdvAmount', 
                                                                                'as': 'adv', 
                                                                                'cond': {
                                                                                    '$and': [
                                                                                        {
                                                                                            '$eq': [
                                                                                                '$$exp.Year', '$$adv.Year'
                                                                                            ]
                                                                                        }, {
                                                                                            '$eq': [
                                                                                                '$$exp.month', '$$adv.month'
                                                                                            ]
                                                                                        }
                                                                                    ]
                                                                                }
                                                                            }
                                                                        }, 0
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, {
                    '$project': {
                        'combinedData': {
                            '$map': {
                                'input': '$combinedData', 
                                'as': 'item', 
                                'in': {
                                    'Year': '$$item.Year', 
                                    'month': '$$item.month', 
                                    'ExpApprovedAmount': {
                                        '$ifNull': [
                                            {
                                                '$round': [
                                                    {
                                                        '$divide': [
                                                            '$$item.ExpApprovedAmount', 100000
                                                        ]
                                                    }, 2
                                                ]
                                            }, 0
                                        ]
                                    }, 
                                    'AdvApprovedAmount': {
                                        '$ifNull': [
                                            {
                                                '$round': [
                                                    {
                                                        '$divide': [
                                                            '$$item.AdvApprovedAmount', 100000
                                                        ]
                                                    }, 2
                                                ]
                                            }, 0
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }, {
                    '$unwind': '$combinedData'
                }, {
                    '$project': {
                        'Year': '$combinedData.Year', 
                        'month': '$combinedData.month', 
                        'ExpApprovedAmount': '$combinedData.ExpApprovedAmount', 
                        'AdvApprovedAmount': '$combinedData.AdvApprovedAmount'
                    }
                }, {
                    '$addFields': {
                        'monthName': {
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
                                'default': 'Unknown'
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'Year': {
                            '$toString': '$Year'
                        }
                    }
                }, {
                    '$addFields': {
                        'yearTwoDigit': {
                            '$substr': [
                                '$Year', 2, 2
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'description': {
                            '$concat': [
                                '$monthName', '-', '$yearTwoDigit'
                            ]
                        }, 
                        'year': {
                            '$toInt': '$Year'
                        }
                    }
                }, {
                    '$sort': {
                        'year': 1, 
                        'month': 1
                    }
                }, {
                    '$project': {
                        'ExpApprovedAmount': 1, 
                        'AdvApprovedAmount': 1, 
                        'description': 1, 
                        'year': 1, 
                        'month': 1
                    }
                }
                ]
            
            Response = cmo.finding_aggregate("Approval", arra)
            return respond(Response)
        else:
            arra=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'customisedStatus': 6,
                        'addedFor':ObjectId(current_user['userUniqueId'])
                    }
                    },{
                        '$addFields': {
                            'actionAt': {
                                '$cond': {
                                    'if': {
                                        '$or': [
                                            {
                                                '$eq': [
                                                    '$actionAt', None
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$actionAt', ''
                                                ]
                                            }
                                        ]
                                    }, 
                                    'then': None, 
                                    'else': {
                                        '$toDate': '$actionAt'
                                    }
                                }
                            }
                        }
                    }, {
                        '$match': {
                            'actionAt': {
                                '$exists': True, 
                                '$ne': ''
                            }
                        }
                    }, {
                        '$addFields': {
                            'start_date': {
                                '$dateFromString': {
                                    'dateString': start_date(), 
                                    'format': '%d-%m-%Y'
                                }
                            }, 
                            'last_date': {
                                '$dateFromString': {
                                    'dateString': last_date(), 
                                    'format': '%d-%m-%Y'
                                }
                            }
                        }
                    }, {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$gte': [
                                            '$actionAt', '$start_date'
                                        ]
                                    }, {
                                        '$lte': [
                                            '$actionAt', '$last_date'
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'month': {
                                '$month': '$actionAt'
                            }
                        }
                    }, {
                        '$facet': {
                            'ExpAmount': [
                                {
                                    '$match': {
                                        'type': 'Expense'
                                    }
                                }, {
                                    '$addFields': {
                                        'month': {
                                            '$cond': {
                                                'if': {
                                                    '$gt': [
                                                        {
                                                            '$dayOfMonth': '$actionAt'
                                                        }, 25
                                                    ]
                                                }, 
                                                'then': {
                                                    '$cond': {
                                                        'if': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAt'
                                                                }, 12
                                                            ]
                                                        }, 
                                                        'then': 1, 
                                                        'else': {
                                                            '$add': [
                                                                {
                                                                    '$month': '$actionAt'
                                                                }, 1
                                                            ]
                                                        }
                                                    }
                                                }, 
                                                'else': {
                                                    '$month': '$actionAt'
                                                }
                                            }
                                        }, 
                                        'Year': {
                                            '$cond': {
                                                'if': {
                                                    '$and': [
                                                        {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAt'
                                                                }, 12
                                                            ]
                                                        }, {
                                                            '$gt': [
                                                                {
                                                                    '$dayOfMonth': '$actionAt'
                                                                }, 25
                                                            ]
                                                        }
                                                    ]
                                                }, 
                                                'then': {
                                                    '$add': [
                                                        {
                                                            '$year': '$actionAt'
                                                        }, 1
                                                    ]
                                                }, 
                                                'else': {
                                                    '$year': '$actionAt'
                                                }
                                            }
                                        }
                                    }
                                }, {
                                    '$group': {
                                        '_id': '$month', 
                                        'ExpApprovedAmount': {
                                            '$sum': '$ApprovedAmount'
                                        }, 
                                        'Year': {
                                            '$first': '$Year'
                                        }, 
                                        'month': {
                                            '$first': '$month'
                                        }
                                    }
                                }
                            ], 
                            'AdvAmount': [
                                {
                                    '$match': {
                                        'type': 'Advance'
                                    }
                                }, {
                                    '$addFields': {
                                        'month': {
                                            '$cond': {
                                                'if': {
                                                    '$gt': [
                                                        {
                                                            '$dayOfMonth': '$actionAt'
                                                        }, 25
                                                    ]
                                                }, 
                                                'then': {
                                                    '$cond': {
                                                        'if': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAt'
                                                                }, 12
                                                            ]
                                                        }, 
                                                        'then': 1, 
                                                        'else': {
                                                            '$add': [
                                                                {
                                                                    '$month': '$actionAt'
                                                                }, 1
                                                            ]
                                                        }
                                                    }
                                                }, 
                                                'else': {
                                                    '$month': '$actionAt'
                                                }
                                            }
                                        }, 
                                        'Year': {
                                            '$cond': {
                                                'if': {
                                                    '$and': [
                                                        {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAt'
                                                                }, 12
                                                            ]
                                                        }, {
                                                            '$gt': [
                                                                {
                                                                    '$dayOfMonth': '$actionAt'
                                                                }, 25
                                                            ]
                                                        }
                                                    ]
                                                }, 
                                                'then': {
                                                    '$add': [
                                                        {
                                                            '$year': '$actionAt'
                                                        }, 1
                                                    ]
                                                }, 
                                                'else': {
                                                    '$year': '$actionAt'
                                                }
                                            }
                                        }
                                    }
                                }, {
                                    '$group': {
                                        '_id': '$month', 
                                        'AdvApprovedAmount': {
                                            '$sum': '$ApprovedAmount'
                                        }, 
                                        'Year': {
                                            '$first': '$Year'
                                        }, 
                                        'month': {
                                            '$first': '$month'
                                        }
                                    }
                                }
                            ]
                        }
                    }, {
                        '$project': {
                            'combinedData': {
                                '$cond': {
                                    'if': {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    {
                                                        '$size': '$ExpAmount'
                                                    }, 0
                                                ]
                                            }, {
                                                '$eq': [
                                                    {
                                                        '$size': '$AdvAmount'
                                                    }, 0
                                                ]
                                            }
                                        ]
                                    }, 
                                    'then': None, 
                                    'else': {
                                        '$cond': {
                                            'if': {
                                                '$eq': [
                                                    {
                                                        '$size': '$ExpAmount'
                                                    }, 0
                                                ]
                                            }, 
                                            'then': '$AdvAmount', 
                                            'else': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$size': '$AdvAmount'
                                                            }, 0
                                                        ]
                                                    }, 
                                                    'then': '$ExpAmount', 
                                                    'else': {
                                                        '$map': {
                                                            'input': '$ExpAmount', 
                                                            'as': 'exp', 
                                                            'in': {
                                                                '$mergeObjects': [
                                                                    '$$exp', {
                                                                        '$arrayElemAt': [
                                                                            {
                                                                                '$filter': {
                                                                                    'input': '$AdvAmount', 
                                                                                    'as': 'adv', 
                                                                                    'cond': {
                                                                                        '$and': [
                                                                                            {
                                                                                                '$eq': [
                                                                                                    '$$exp.Year', '$$adv.Year'
                                                                                                ]
                                                                                            }, {
                                                                                                '$eq': [
                                                                                                    '$$exp.month', '$$adv.month'
                                                                                                ]
                                                                                            }
                                                                                        ]
                                                                                    }
                                                                                }
                                                                            }, 0
                                                                        ]
                                                                    }
                                                                ]
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }, {
                        '$project': {
                            'combinedData': {
                                '$map': {
                                    'input': '$combinedData', 
                                    'as': 'item', 
                                    'in': {
                                        'Year': '$$item.Year', 
                                        'month': '$$item.month', 
                                        'ExpApprovedAmount': {
                                            '$ifNull': [
                                                {
                                                    '$round': [
                                                        {
                                                            '$divide': [
                                                                '$$item.ExpApprovedAmount', 100000
                                                            ]
                                                        }, 2
                                                    ]
                                                }, 0
                                            ]
                                        }, 
                                        'AdvApprovedAmount': {
                                            '$ifNull': [
                                                {
                                                    '$round': [
                                                        {
                                                            '$divide': [
                                                                '$$item.AdvApprovedAmount', 100000
                                                            ]
                                                        }, 2
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }, {
                        '$unwind': '$combinedData'
                    }, {
                        '$project': {
                            'Year': '$combinedData.Year', 
                            'month': '$combinedData.month', 
                            'ExpApprovedAmount': '$combinedData.ExpApprovedAmount', 
                            'AdvApprovedAmount': '$combinedData.AdvApprovedAmount'
                        }
                    }, {
                        '$addFields': {
                            'monthName': {
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
                                    'default': 'Unknown'
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'Year': {
                                '$toString': '$Year'
                            }
                        }
                    }, {
                        '$addFields': {
                            'yearTwoDigit': {
                                '$substr': [
                                    '$Year', 2, 2
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'description': {
                                '$concat': [
                                    '$monthName', '-', '$yearTwoDigit'
                                ]
                            }, 
                            'year': {
                                '$toInt': '$Year'
                            }
                        }
                    }, {
                        '$sort': {
                            'year': 1, 
                            'month': 1
                        }
                    }, {
                        '$project': {
                            'ExpApprovedAmount': 1, 
                            'AdvApprovedAmount': 1, 
                            'description': 1, 
                            'year': 1, 
                            'month': 1
                        }
                    }
                ]
            Response = cmo.finding_aggregate("Approval", arra)
            return respond(Response)
    if request.method == "POST":
        data = request.get_json()
        arry=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }, 
                        '_id': ObjectId(current_user['userUniqueId'])
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
                            }, {
                                '$project': {
                                    'roleName': 1,
                                    '_id':0
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
                        'userRole': '$result.roleName'
                    }
                }
            ]
        userRole=cmo.finding_aggregate('userRegister',arry)['data']
        if len(userRole):
            current_user['userRoleName']=userRole[0]['userRole']
        Response=None
        arra=[]
        if current_user['userRoleName'] not in ['Admin','Finance','PMO']:
            arra=[{
                    '$match': {
                         'addedFor':ObjectId(current_user['userUniqueId'])
                        
                    }
                },]
        arra=arra+[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'customisedStatus': 6
                    }
                            }, {
                    '$addFields': {
                        'actionAt': {
                            '$cond': {
                                'if': {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$actionAt', None
                                            ]
                                        }, {
                                            '$eq': [
                                                '$actionAt', ''
                                            ]
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$toDate': '$actionAt'
                                }
                            }
                        }
                    }
                }, {
                    '$match': {
                        'actionAt': {
                            '$exists': True, 
                            '$ne': ''
                        }
                    }
                }, {
                    '$addFields': {
                        'start_date': {
                            '$dateFromString': {
                                'dateString': start_date(), 
                                'format': '%d-%m-%Y'
                            }
                        }, 
                        'last_date': {
                            '$dateFromString': {
                                'dateString': last_date(), 
                                'format': '%d-%m-%Y'
                            }
                        }
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$gte': [
                                        '$actionAt', '$start_date'
                                    ]
                                }, {
                                    '$lte': [
                                        '$actionAt', '$last_date'
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'month': {
                            '$month': '$actionAt'
                        }
                    }
                }, {
                    '$facet': {
                        'ExpAmount': [
                            {
                                '$match': {
                                    'type': 'Expense'
                                }
                            }, {
                                '$addFields': {
                                    'month': {
                                        '$cond': {
                                            'if': {
                                                '$gt': [
                                                    {
                                                        '$dayOfMonth': '$actionAt'
                                                    }, 25
                                                ]
                                            }, 
                                            'then': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, 
                                                    'then': 1, 
                                                    'else': {
                                                        '$add': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 1
                                                        ]
                                                    }
                                                }
                                            }, 
                                            'else': {
                                                '$month': '$actionAt'
                                            }
                                        }
                                    }, 
                                    'Year': {
                                        '$cond': {
                                            'if': {
                                                '$and': [
                                                    {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, {
                                                        '$gt': [
                                                            {
                                                                '$dayOfMonth': '$actionAt'
                                                            }, 25
                                                        ]
                                                    }
                                                ]
                                            }, 
                                            'then': {
                                                '$add': [
                                                    {
                                                        '$year': '$actionAt'
                                                    }, 1
                                                ]
                                            }, 
                                            'else': {
                                                '$year': '$actionAt'
                                            }
                                        }
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': '$month', 
                                    'ExpApprovedAmount': {
                                        '$sum': '$ApprovedAmount'
                                    }, 
                                    'Year': {
                                        '$first': '$Year'
                                    }, 
                                    'month': {
                                        '$first': '$month'
                                    }
                                }
                            }
                        ], 
                        'AdvAmount': [
                            {
                                '$match': {
                                    'type': 'Advance'
                                }
                            }, {
                                '$addFields': {
                                    'month': {
                                        '$cond': {
                                            'if': {
                                                '$gt': [
                                                    {
                                                        '$dayOfMonth': '$actionAt'
                                                    }, 25
                                                ]
                                            }, 
                                            'then': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, 
                                                    'then': 1, 
                                                    'else': {
                                                        '$add': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 1
                                                        ]
                                                    }
                                                }
                                            }, 
                                            'else': {
                                                '$month': '$actionAt'
                                            }
                                        }
                                    }, 
                                    'Year': {
                                        '$cond': {
                                            'if': {
                                                '$and': [
                                                    {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAt'
                                                            }, 12
                                                        ]
                                                    }, {
                                                        '$gt': [
                                                            {
                                                                '$dayOfMonth': '$actionAt'
                                                            }, 25
                                                        ]
                                                    }
                                                ]
                                            }, 
                                            'then': {
                                                '$add': [
                                                    {
                                                        '$year': '$actionAt'
                                                    }, 1
                                                ]
                                            }, 
                                            'else': {
                                                '$year': '$actionAt'
                                            }
                                        }
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': '$month', 
                                    'AdvApprovedAmount': {
                                        '$sum': '$ApprovedAmount'
                                    }, 
                                    'Year': {
                                        '$first': '$Year'
                                    }, 
                                    'month': {
                                        '$first': '$month'
                                    }
                                }
                            }
                        ]
                    }
                }, {
                    '$project': {
                        'combinedData': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                {
                                                    '$size': '$ExpAmount'
                                                }, 0
                                            ]
                                        }, {
                                            '$eq': [
                                                {
                                                    '$size': '$AdvAmount'
                                                }, 0
                                            ]
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                {
                                                    '$size': '$ExpAmount'
                                                }, 0
                                            ]
                                        }, 
                                        'then': '$AdvAmount', 
                                        'else': {
                                            '$cond': {
                                                'if': {
                                                    '$eq': [
                                                        {
                                                            '$size': '$AdvAmount'
                                                        }, 0
                                                    ]
                                                }, 
                                                'then': '$ExpAmount', 
                                                'else': {
                                                    '$map': {
                                                        'input': '$ExpAmount', 
                                                        'as': 'exp', 
                                                        'in': {
                                                            '$mergeObjects': [
                                                                '$$exp', {
                                                                    '$arrayElemAt': [
                                                                        {
                                                                            '$filter': {
                                                                                'input': '$AdvAmount', 
                                                                                'as': 'adv', 
                                                                                'cond': {
                                                                                    '$and': [
                                                                                        {
                                                                                            '$eq': [
                                                                                                '$$exp.Year', '$$adv.Year'
                                                                                            ]
                                                                                        }, {
                                                                                            '$eq': [
                                                                                                '$$exp.month', '$$adv.month'
                                                                                            ]
                                                                                        }
                                                                                    ]
                                                                                }
                                                                            }
                                                                        }, 0
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, {
                    '$project': {
                        'combinedData': {
                            '$map': {
                                'input': '$combinedData', 
                                'as': 'item', 
                                'in': {
                                    'Year': '$$item.Year', 
                                    'month': '$$item.month', 
                                    'ExpApprovedAmount': {
                                        '$ifNull': [
                                            {
                                                '$round': [
                                                    {
                                                        '$divide': [
                                                            '$$item.ExpApprovedAmount', 100000
                                                        ]
                                                    }, 2
                                                ]
                                            }, 0
                                        ]
                                    }, 
                                    'AdvApprovedAmount': {
                                        '$ifNull': [
                                            {
                                                '$round': [
                                                    {
                                                        '$divide': [
                                                            '$$item.AdvApprovedAmount', 100000
                                                        ]
                                                    }, 2
                                                ]
                                            }, 0
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }, {
                    '$unwind': '$combinedData'
                }, {
                    '$project': {
                        'Year': '$combinedData.Year', 
                        'month': '$combinedData.month', 
                        'ExpApprovedAmount': '$combinedData.ExpApprovedAmount', 
                        'AdvApprovedAmount': '$combinedData.AdvApprovedAmount'
                    }
                }, {
                    '$addFields': {
                        'monthName': {
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
                                'default': 'Unknown'
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'Year': {
                            '$toString': '$Year'
                        }
                    }
                }, {
                    '$addFields': {
                        'yearTwoDigit': {
                            '$substr': [
                                '$Year', 2, 2
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'description': {
                            '$concat': [
                                '$monthName', '-', '$yearTwoDigit'
                            ]
                        }, 
                        'year': {
                            '$toInt': '$Year'
                        }
                    }
                }, {
                    '$sort': {
                        'year': 1, 
                        'month': 1
                    }
                }, {
                    '$project': {
                        'ExpApprovedAmount': 1, 
                        'AdvApprovedAmount': 1, 
                        'description': 1, 
                        'year': 1, 
                        'month': 1
                    }
                }
                        ]
        Response = cmo.finding_aggregate("Approval", arra)
        df=pd.DataFrame(Response['data'])
        if 'month' in data and 'year' in data:
            df = df[(df['year'] == data['year']) & (df['month'].isin(data['month']))]
        if 'year' in data:
            df = df[df['year'] == data['year']]
        if 'month' in data and 'year' not in data:
            data['year'] = current_year()
            # print('gggggddggd',data)
            df = df[(df['year'] == data['year']) & (df['month'].isin(data['month']))]
            # df=df['month'].isin(data['month'])
            # print('ggdggdgggdueueuuuurururuur',df)
        
        Response['data'] = json.loads(df.to_json(orient="records"))
        return respond(Response)


@graph_blueprint.route('/graph/ExpenseApprovalStatus',methods=["GET","POST"])
@token_required
def ExpenseApprovalStatus(current_user):
    if request.method == "GET":
        arry=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }, 
                        '_id': ObjectId(current_user['userUniqueId'])
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
                            }, {
                                '$project': {
                                    'roleName': 1,
                                    '_id':0
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
                        'userRole': '$result.roleName'
                    }
                }
            ]
        userRole=cmo.finding_aggregate('userRegister',arry)['data']
        if len(userRole):
            current_user['userRoleName']=userRole[0]['userRole']
        Response=None
        if current_user['userRoleName'] in ['Admin','Finance','PMO']:
            arra=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$facet': {
                        'Submitted': [
                            {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L1Pending': [
                            {
                                '$match': {
                                    'status': 'Submitted'
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L3Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 6
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L1Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 2
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L2Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 4
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'Rejected': [
                            {
                                '$match': {
                                    'customisedStatus': {
                                        '$in': [
                                            3, 5, 7
                                        ]
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ]
                    }
                }, {
                    '$project': {
                        'Claim Submitted': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$Submitted.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1-Pending': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$sum': [
                                                {
                                                    '$ifNull': [
                                                        {
                                                            '$arrayElemAt': [
                                                                '$L1Pending.count', 0
                                                            ]
                                                        }, 0
                                                    ]
                                                }
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L2-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L2Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Expense Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L3Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Rejected': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$sum': [
                                                {
                                                    '$ifNull': [
                                                        {
                                                            '$arrayElemAt': [
                                                                '$Rejected.count', 0
                                                            ]
                                                        }, 0
                                                    ]
                                                }
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'fields': {
                            '$objectToArray': '$$ROOT'
                        }
                    }
                }, {
                    '$unwind': '$fields'
                }, {
                    '$replaceRoot': {
                        'newRoot': {
                            'count': '$fields.v', 
                            'description': '$fields.k'
                        }
                    }
                }
            ]
            Response = cmo.finding_aggregate("Expenses", arra)
            return respond(Response)
        else:
            arra=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        'addedFor':ObjectId(current_user['userUniqueId'])
                    }
                }, {
                '$facet': {
                    'Submitted': [
                        {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$Amount'
                                }
                            }
                        }
                    ], 
                    'L1Pending': [
                        {
                            '$match': {
                                'status': 'Submitted'
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$Amount'
                                }
                            }
                        }
                    ], 
                    'L3Approved': [
                        {
                            '$match': {
                                'customisedStatus': 6
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$ApprovedAmount'
                                }
                            }
                        }
                    ], 
                    'L1Approved': [
                        {
                            '$match': {
                                'customisedStatus': 2
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$ApprovedAmount'
                                }
                            }
                        }
                    ], 
                    'L2Approved': [
                        {
                            '$match': {
                                'customisedStatus': 4
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$ApprovedAmount'
                                }
                            }
                        }
                    ], 
                    'Rejected': [
                        {
                            '$match': {
                                'customisedStatus': {
                                    '$in': [
                                        3, 5, 7
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$Amount'
                                }
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'Claim Submitted': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$Submitted.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'L1-Pending': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$sum': [
                                            {
                                                '$ifNull': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$L1Pending.count', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'L1-Approved': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$L1Approved.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'L2-Approved': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$L2Approved.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'Claim Approved': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$L3Approved.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'Rejected': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$sum': [
                                            {
                                                '$ifNull': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$Rejected.count', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }
                }
            }, {
                '$project': {
                    'fields': {
                        '$objectToArray': '$$ROOT'
                    }
                }
            }, {
                '$unwind': '$fields'
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        'count': '$fields.v', 
                        'description': '$fields.k'
                    }
                }
            }
            ]
            Response = cmo.finding_aggregate("Expenses", arra)
            return respond(Response)
    if request.method == "POST":
        filterdata=request.get_json()
        arry=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }, 
                        '_id': ObjectId(current_user['userUniqueId'])
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
                            }, {
                                '$project': {
                                    'roleName': 1,
                                    '_id':0
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
                        'userRole': '$result.roleName'
                    }
                }
            ]
        userRole=cmo.finding_aggregate('userRegister',arry)['data']
        if len(userRole):
            current_user['userRoleName']=userRole[0]['userRole']
        Response=None
        arra=[]
        arrp=[
            {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'addedFor', 
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
                                'orgLevel': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'orgResult'
                }
            }, {
                '$unwind': {
                    'path': '$orgResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'submissionDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$submissionDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$submissionDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': '$CreatedAt', 
                            'else': '$submissionDate'
                        }
                    }, 
                    'organizationLevel': '$orgResult.orgLevel'
                }
            }, {
                '$addFields': {
                    'submissionDate': {
                        '$toDate': {
                            '$ifNull': [
                                {
                                    '$toDate': '$submissionDate'
                                }, {
                                    '$toDate': '$CreatedAt'
                                }
                            ]
                        }
                    }
                }
            }, {
                '$addFields': {
                    'submissionDate': {
                        '$dateAdd': {
                            'startDate': '$submissionDate', 
                            'unit': 'minute', 
                            'amount': 330
                        }
                    }
                }
            }, {
                '$addFields': {
                    'submissionMonth': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    {
                                        '$dayOfMonth': '$submissionDate'
                                    }, 25
                                ]
                            }, 
                            'then': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            {
                                                '$month': '$submissionDate'
                                            }, 12
                                        ]
                                    }, 
                                    'then': 1, 
                                    'else': {
                                        '$add': [
                                            {
                                                '$month': '$submissionDate'
                                            }, 1
                                        ]
                                    }
                                }
                            }, 
                            'else': {
                                '$month': '$submissionDate'
                            }
                        }
                    }, 
                    'submissionYear': {
                        '$cond': {
                            'if': {
                                '$and': [
                                    {
                                        '$eq': [
                                            {
                                                '$month': '$submissionDate'
                                            }, 12
                                        ]
                                    }, {
                                        '$gt': [
                                            {
                                                '$dayOfMonth': '$submissionDate'
                                            }, 25
                                        ]
                                    }
                                ]
                            }, 
                            'then': {
                                '$add': [
                                    {
                                        '$year': '$submissionDate'
                                    }, 1
                                ]
                            }, 
                            'else': {
                                '$year': '$submissionDate'
                            }
                        }
                    }
                }
            },
        ]
        if 'description' in filterdata:
            if len(filterdata['description']):
                arrp=arrp+[
                    {
                        '$match':{
                            'organizationLevel':{
                                '$in':filterdata['description']
                            }
                        }
                    }
                ]
        if 'year' in filterdata:
            if filterdata['year'] not in ['',None,'undefined']:
                arrp=arrp+[
                    {
                        '$match':{
                            'submissionYear':filterdata['year']
                        }
                    }
                ]
        if 'month' in filterdata:
            if len(filterdata['month']):
                arrp=arrp+[
                    {
                        '$match':{
                            'submissionMonth':{
                                '$in':filterdata['month']
                            }
                        }
                    }
                ] 
        if 'month' not in filterdata and 'year' not in filterdata and 'description' not in filterdata:
            arrp = []
        if current_user['userRoleName'] not in ['Admin','Finance','PMO']:
            arra=[
                {
                    '$match':{
                         'addedFor':ObjectId(current_user['userUniqueId'])
                    }
                }
            ]
        arra=arra+[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }]          
        arra=arrp+[
            {
                '$facet': {
                    'Submitted': [
                        {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$Amount'
                                }
                            }
                        }
                    ], 
                    'L1Pending': [
                        {
                            '$match': {
                                'status': 'Submitted'
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$Amount'
                                }
                            }
                        }
                    ], 
                    'L3Approved': [
                        {
                            '$match': {
                                'customisedStatus': 6
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$ApprovedAmount'
                                }
                            }
                        }
                    ], 
                    'L1Approved': [
                        {
                            '$match': {
                                'customisedStatus': 2
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$ApprovedAmount'
                                }
                            }
                        }
                    ], 
                    'L2Approved': [
                        {
                            '$match': {
                                'customisedStatus': 4
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$ApprovedAmount'
                                }
                            }
                        }
                    ], 
                    'Rejected': [
                        {
                            '$match': {
                                'customisedStatus': {
                                    '$in': [
                                        3, 5, 7
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None, 
                                'count': {
                                    '$sum': '$Amount'
                                }
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'Claim Submitted': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$Submitted.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'L1-Pending': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$sum': [
                                            {
                                                '$ifNull': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$L1Pending.count', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'L1-Approved': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$L1Approved.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'L2-Approved': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$L2Approved.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'Claim Approved': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$ifNull': [
                                            {
                                                '$arrayElemAt': [
                                                    '$L3Approved.count', 0
                                                ]
                                            }, 0
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }, 
                    'Rejected': {
                        '$round': [
                            {
                                '$divide': [
                                    {
                                        '$sum': [
                                            {
                                                '$ifNull': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$Rejected.count', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        ]
                                    }, 100000
                                ]
                            }, 2
                        ]
                    }
                }
            }, {
                '$project': {
                    'fields': {
                        '$objectToArray': '$$ROOT'
                    }
                }
            }, {
                '$unwind': '$fields'
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        'count': '$fields.v', 
                        'description': '$fields.k'
                    }
                }
            }
        ]
        Response = cmo.finding_aggregate("Expenses", arra)
        return respond(Response)
        
@graph_blueprint.route('/graph/advanceApprovalStatus',methods=["GET","POST"])
@token_required
def advanceApprovalStatus(current_user):
    if request.method == "GET":
        
        arry=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }, 
                        '_id': ObjectId(current_user['userUniqueId'])
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
                            }, {
                                '$project': {
                                    'roleName': 1,
                                    '_id':0
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
                        'userRole': '$result.roleName'
                    }
                }
            ]
        userRole=cmo.finding_aggregate('userRegister',arry)['data']
        if len(userRole):
            current_user['userRoleName']=userRole[0]['userRole']
        Response=None
        if current_user['userRoleName'] in ['Admin','Finance','PMO']:
            arra=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                            }, {
                    '$facet': {
                        'Submitted': [
                            {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L3Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 6
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L1Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 2
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L2Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 4
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'Rejected': [
                            {
                                '$match': {
                                    'type': 'Advance', 
                                    'customisedStatus': {
                                        '$in': [
                                            3, 5, 7
                                        ]
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L1Pending': [
                            {
                                '$match': {
                                    'status': 'Submitted'
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ]
                    }
                }, {
                    '$project': {
                        'Claim Submitted': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$Submitted.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1Pending': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Pending.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L2-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L2Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Advance Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L3Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Rejected': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$sum': [
                                                {
                                                    '$ifNull': [
                                                        {
                                                            '$arrayElemAt': [
                                                                '$Rejected.count', 0
                                                            ]
                                                        }, 0
                                                    ]
                                                }
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'fields': {
                            '$objectToArray': '$$ROOT'
                        }
                    }
                }, {
                    '$unwind': '$fields'
                }, {
                    '$replaceRoot': {
                        'newRoot': {
                            'count': '$fields.v', 
                            'description': '$fields.k'
                        }
                    }
                }
            ]
            Response = cmo.finding_aggregate("Advance", arra)
            return respond(Response)
        else:
            arra=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            'addedFor':ObjectId(current_user['userUniqueId'])
                        }
                    },
                     {
                    '$facet': {
                        'Submitted': [
                            {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L3Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 6
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L1Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 2
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L2Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 4
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'Rejected': [
                            {
                                '$match': {
                                    'type': 'Advance', 
                                    'customisedStatus': {
                                        '$in': [
                                            3, 5, 7
                                        ]
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L1Pending': [
                            {
                                '$match': {
                                    'status': 'Submitted'
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ]
                    }
                }, {
                    '$project': {
                        'Claim Submitted': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$Submitted.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1Pending': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Pending.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L2-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L2Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Claim Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L3Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Rejected': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$sum': [
                                                {
                                                    '$ifNull': [
                                                        {
                                                            '$arrayElemAt': [
                                                                '$Rejected.count', 0
                                                            ]
                                                        }, 0
                                                    ]
                                                }
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'fields': {
                            '$objectToArray': '$$ROOT'
                        }
                    }
                }, {
                    '$unwind': '$fields'
                }, {
                    '$replaceRoot': {
                        'newRoot': {
                            'count': '$fields.v', 
                            'description': '$fields.k'
                        }
                    }
                }
                ]
            Response = cmo.finding_aggregate("Advance", arra)
            return respond(Response)
    if request.method == "POST":
        filterdata=request.get_json()
        arry=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'type': {
                            '$ne': 'Partner'
                        }, 
                        '_id': ObjectId(current_user['userUniqueId'])
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
                            }, {
                                '$project': {
                                    'roleName': 1,
                                    '_id':0
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
                        'userRole': '$result.roleName'
                    }
                }
            ]
        userRole=cmo.finding_aggregate('userRegister',arry)['data']
        if len(userRole):
            current_user['userRoleName']=userRole[0]['userRole']
        Response=None
        arra=[]
        arrp=[
            {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'addedFor', 
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
                                'orgLevel': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'orgResult'
                }
            }, {
                '$unwind': {
                    'path': '$orgResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'submissionDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$submissionDate', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$submissionDate', ''
                                        ]
                                    }
                                ]
                            }, 
                            'then': '$CreatedAt', 
                            'else': '$submissionDate'
                        }
                    }, 
                    'organizationLevel': '$orgResult.orgLevel'
                }
            }, {
                '$addFields': {
                    'submissionDate': {
                        '$toDate': {
                            '$ifNull': [
                                {
                                    '$toDate': '$submissionDate'
                                }, {
                                    '$toDate': '$CreatedAt'
                                }
                            ]
                        }
                    }
                }
            }, {
                '$addFields': {
                    'submissionDate': {
                        '$dateAdd': {
                            'startDate': '$submissionDate', 
                            'unit': 'minute', 
                            'amount': 330
                        }
                    }
                }
            }, {
                '$addFields': {
                    'submissionMonth': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    {
                                        '$dayOfMonth': '$submissionDate'
                                    }, 25
                                ]
                            }, 
                            'then': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            {
                                                '$month': '$submissionDate'
                                            }, 12
                                        ]
                                    }, 
                                    'then': 1, 
                                    'else': {
                                        '$add': [
                                            {
                                                '$month': '$submissionDate'
                                            }, 1
                                        ]
                                    }
                                }
                            }, 
                            'else': {
                                '$month': '$submissionDate'
                            }
                        }
                    }, 
                    'submissionYear': {
                        '$cond': {
                            'if': {
                                '$and': [
                                    {
                                        '$eq': [
                                            {
                                                '$month': '$submissionDate'
                                            }, 12
                                        ]
                                    }, {
                                        '$gt': [
                                            {
                                                '$dayOfMonth': '$submissionDate'
                                            }, 25
                                        ]
                                    }
                                ]
                            }, 
                            'then': {
                                '$add': [
                                    {
                                        '$year': '$submissionDate'
                                    }, 1
                                ]
                            }, 
                            'else': {
                                '$year': '$submissionDate'
                            }
                        }
                    }
                }
            },
        ]
        if 'description' in filterdata:
            if len(filterdata['description']):
                arrp=arrp+[
                    {
                        '$match':{
                            'organizationLevel':{
                                '$in':filterdata['description']
                            }
                        }
                    }
                ]
        if 'year' in filterdata:
            if filterdata['year'] not in ['',None,'undefined']:
                arrp=arrp+[
                    {
                        '$match':{
                            'submissionYear':filterdata['year']
                        }
                    }
                ]
        if 'month' in filterdata:
            if len(filterdata['month']):
                arrp=arrp+[
                    {
                        '$match':{
                            'submissionMonth':{
                                '$in':filterdata['month']
                            }
                        }
                    }
                ]  
        if 'month' not in filterdata and 'year' not in filterdata and 'description' not in filterdata:
            arrp = []
        if current_user['userRoleName'] not in ['Admin','Finance','PMO']:
            arra=[
                {
                    '$match':{
                         'addedFor':ObjectId(current_user['userUniqueId'])
                    }
                }
            ]
        arra=arra+[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }]
        arra=arrp+[ {
                    '$facet': {
                        'Submitted': [
                            {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L3Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 6
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L1Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 2
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'L2Approved': [
                            {
                                '$match': {
                                    'customisedStatus': 4
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$ApprovedAmount'
                                    }
                                }
                            }
                        ], 
                        'Rejected': [
                            {
                                '$match': {
                                    'type': 'Advance', 
                                    'customisedStatus': {
                                        '$in': [
                                            3, 5, 7
                                        ]
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ], 
                        'L1Pending': [
                            {
                                '$match': {
                                    'status': 'Submitted'
                                }
                            }, {
                                '$group': {
                                    '_id': None, 
                                    'count': {
                                        '$sum': '$Amount'
                                    }
                                }
                            }
                        ]
                    }
                }, {
                    '$project': {
                        'Claim Submitted': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$Submitted.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1Pending': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Pending.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L1-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L1Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'L2-Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L2Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Advance Approved': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$L3Approved.count', 0
                                                    ]
                                                }, 0
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }, 
                        'Rejected': {
                            '$round': [
                                {
                                    '$divide': [
                                        {
                                            '$sum': [
                                                {
                                                    '$ifNull': [
                                                        {
                                                            '$arrayElemAt': [
                                                                '$Rejected.count', 0
                                                            ]
                                                        }, 0
                                                    ]
                                                }
                                            ]
                                        }, 100000
                                    ]
                                }, 2
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'fields': {
                            '$objectToArray': '$$ROOT'
                        }
                    }
                }, {
                    '$unwind': '$fields'
                }, {
                    '$replaceRoot': {
                        'newRoot': {
                            'count': '$fields.v', 
                            'description': '$fields.k'
                        }
                    }
                }]
        Response = cmo.finding_aggregate("Advance", arra)
        return respond(Response)
 
        
  
        
@graph_blueprint.route("/graph/circlewiseMS1AndMs2",methods=['GET','POST'])
@graph_blueprint.route("/graph/circlewiseMS1AndMs2/<id>",methods=['GET','POST'])
@token_required
def circlewiseMS1AndMs2(current_user,id=None):
    if request.method == "GET":
        arrci=[
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
                        'circleCode': 1, 
                        '_id': 0
                    }
                }
            ]
        circledf=cmo.finding_aggregate("circle",arrci)['data']
        circledf=pd.DataFrame(circledf)
        arrrp=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'projectuniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0, 
                    'Circle': "$circle"
                }
            }
        ]
        projectdf=cmo.finding_aggregate("project",arrrp)['data']
        projectdf=pd.DataFrame(projectdf)
        dfpro = projectdf.merge(circledf, on=["Circle"], how="left")
        dfpro['Circle']=dfpro['circleCode']
        dfpro = dfpro.drop(columns=['circleCode'])
        arra = [
            {
                '$match': {
                    'deleteStatus': {'$ne': 1}, 
                    'projectType': {
                        '$in': ['ULS', 'UBR', 'MACRO', 'DEGROW', 'RELOCATION']
                    }
                }
            }, {
                '$project': {
                    'id': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }
        ]
        projectTypeUid = cmo.finding_aggregate("projectType",arra)['data']
        projectTypeArray = []
        for i in projectTypeUid:
            projectTypeArray.append(i['id'])
        
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'projectuniqueId': {
                        '$in': projectId_str(current_user['userUniqueId'])
                    },
                    'SubProjectId':{
                        '$in':projectTypeArray
                    }
                }
            },
            # {
            #     '$addFields': {
            #         'SubProjectId': {
            #             '$toObjectId': '$SubProjectId'
            #         }
            #     }
            # },
            # {
            #     '$lookup': {
            #         'from': 'projectType', 
            #         'localField': 'SubProjectId', 
            #         'foreignField': '_id', 
            #         # 'pipeline': [
            #         #     {
            #         #         '$match': {
            #         #             'deleteStatus': {
            #         #                 '$ne': 1
            #         #             },
            #         #     #         'projectType': {
            #         #     #     '$in': [
            #         #     #         'ULS', 'UBR', 'MACRO', 'DEGROW', 'RELOCATION'
            #         #     #     ]
            #         #     # }
            #         #         }
            #         #     }
            #         # ], 
            #         'as': 'projectTypeResults'
            #     }
            # }, {
            #     '$unwind': {
            #         'path': '$projectTypeResults', 
            #         'preserveNullAndEmptyArrays': True
            #     }
            # },
            {
                '$project': {
                    'Circle': 1, 
                    'RFAI Date': 1, 
                    'Site Id': 1, 
                    'siteId': '$_id', 
                    '_id': 0, 
                    'projectuniqueId': 1, 
                    # 'projectType': '$projectTypeResults.projectType'
                }
            }
        ]
        sitedf=cmo.finding_aggregate("SiteEngineer",arr)['data']
        sitedf=pd.DataFrame(sitedf)
        sitedf = sitedf.merge(dfpro, on=["projectuniqueId"], how="left")
        
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    },
                    'projectuniqueId':{
                         '$in':projectId_str(current_user['userUniqueId'])
                    }, 
                    'Name': {
                        '$in': [
                            'MS1', 'MS2'
                        ]
                    }
                }
            }, {
                '$project': {
                    'Name': 1, 
                    'mileStoneStatus': 1, 
                    'siteId': 1, 
                    'projectuniqueId': 1, 
                    '_id': 0
                }
            }
        ]
      
        Response=cmo.finding_aggregate("milestone",arr)['data']
        milestonedf=pd.DataFrame(Response)
        # print(milestonedf,"milestonedfmilestonedf")
        df = milestonedf.merge(sitedf, on=["siteId"], how="left")
        # df = df[df['projectType'].isin(['ULS','UBR','MACRO','DEGROW','RELOCATION'])]
        condition_ms1 = (df['Name'] == 'MS1') & (df['mileStoneStatus'] == 'Closed') & (df['RFAI Date'].notnull())
        condition_ms2 = (df['Name'] == 'MS2') & (df['mileStoneStatus'] == 'Closed') & (df['RFAI Date'].notnull())
        # print('tempfunction08',tempfunction())
        total_ms1_done = df[condition_ms1].groupby('Circle').size().rename('TotalMS1Done')
        total_ms2_done = df[condition_ms2].groupby('Circle').size().rename('TotalMS2Done')
        # print('tempfunction09',tempfunction())
        siteidCount = df.groupby('Circle')['siteId'].nunique().rename('SiteIdCount')
        # print('tempfunction10',tempfunction())
        result = pd.concat([total_ms1_done, total_ms2_done,siteidCount], axis=1).fillna(0).astype(int)

        result['ach1'] = (result['TotalMS1Done'] / result['SiteIdCount'] * 100).round(0).astype(int)
        result['ach2'] = (result['TotalMS2Done'] / result['TotalMS1Done'] * 100).round(0).astype(int)
        result = result.sort_values(by='ach2', ascending=False)

        result = result.reset_index()
        result['description']=result['Circle']
        result = result.drop(columns=['Circle'])
        data=cfc.dfjson(result)
        Response={}
        Response['data']=data
        Response['status']=200
        Response['msg']='Data get Successfully'
        return (Response)
    
    
    if request.method == "POST":
        data=request.get_json()
        financialyearArry=[]
        projectType=[]
        circleArray=[]
        if 'projectType' in data:
            projectType=data['projectType']
        if 'year' in data:
            financialyearArry.append(data['year'])
        if 'circleCode' in data:
            circleArray=data['circleCode'] 
        checkinggarry=['',None,'undefined']
        arrci=[]
        if  len(circleArray):
            arrci=arrci+[{
                '$match': {
                    'circleCode': {
                        '$in': circleArray
                    }
                }
            }]
        arrci=arrci+[
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
                        'circleCode': 1, 
                        '_id': 0
                    }
                }
            ]
        circledf=cmo.finding_aggregate("circle",arrci)['data']
        circledf=pd.DataFrame(circledf)
        
        arrrp=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
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
                            '$project': {
                                'projectType': 1, 
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
                    'projectuniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0, 
                    'Circle': '$circle', 
                    'projectType': '$result.projectType'
                }
            }
        ]
        projectdf=cmo.finding_aggregate("project",arrrp)['data']
        projectdf=pd.DataFrame(projectdf)
        dfpro = projectdf.merge(circledf, on=["Circle"], how="left")
        dfpro['Circle']=dfpro['circleCode']
        dfpro = dfpro.drop(columns=['circleCode'])
        
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    },
                    'projectuniqueId':{
                         '$in':projectId_str(current_user['userUniqueId'])
                    },
                }
            }, {
                '$project': {
                    'Circle': 1, 
                    'RFAI Date': 1, 
                    'Site Id': 1, 
                    'siteId': '$_id', 
                    '_id': 0, 
                    'projectuniqueId': 1
                }
            }
        ]
        sitedf=cmo.finding_aggregate("SiteEngineer",arr)['data']
        sitedf=pd.DataFrame(sitedf)
        sitedf = sitedf.merge(dfpro, on=["projectuniqueId"], how="left")
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    },
                    'projectuniqueId':{
                         '$in':projectId_str(current_user['userUniqueId'])
                    }, 
                    'Name': {
                        '$in': [
                            'MS1', 'MS2'
                        ]
                    }
                }
            }, {
                '$project': {
                    'Name': 1, 
                    'mileStoneStatus': 1, 
                    'siteId': 1, 
                    'projectuniqueId': 1, 
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("milestone",arr)['data']
        milestonedf=pd.DataFrame(Response)
        
        df = milestonedf.merge(sitedf, on=["siteId"], how="left")
        dateColumns=['RFAI Date']
        for col in dateColumns:
            df[col] = df[col].apply(convertToDateBulkExport)
        
        df['RFAI Date'] = pd.to_datetime(df['RFAI Date'])
        
        df['Year'] = df['RFAI Date'].dt.year
        if len(circleArray):
            df = df[df['Circle'].isin(circleArray)]
        if len(projectType):
            df = df[df['projectType'].isin(projectType)]
        if len(financialyearArry):
            df = df[df['Year'].isin(financialyearArry)]
        
        condition_ms1 = (df['Name'] == 'MS1') & (df['mileStoneStatus'] == 'Closed') & (df['RFAI Date'].notnull())
        condition_ms2 = (df['Name'] == 'MS2') & (df['mileStoneStatus'] == 'Closed') & (df['RFAI Date'].notnull())
        total_ms1_done = df[condition_ms1].groupby('Circle').size().rename('TotalMS1Done')
        total_ms2_done = df[condition_ms2].groupby('Circle').size().rename('TotalMS2Done')
        siteidCount = df.groupby('Circle')['siteId'].nunique().rename('SiteIdCount')
        result = pd.concat([total_ms1_done, total_ms2_done,siteidCount], axis=1).fillna(0).astype(int)

        result['ach1'] = (result['TotalMS1Done'] / result['SiteIdCount'] * 100).round(0).astype(int)
        result['ach2'] = (result['TotalMS2Done'] / result['TotalMS1Done'] * 100).round(0).astype(int)
        result = result.sort_values(by='ach2', ascending=False)
        
        result = result.reset_index()
        result['description']=result['Circle']
        result = result.drop(columns=['Circle'])
        data=cfc.dfjson(result)
        Response={}
        Response['data']=data
        Response['status']=200
        Response['msg']='Data get Successfully'
        return (Response)
  

  
@graph_blueprint.route("/graph/MS1&MS2Circle",methods=['GET','POST']) 
@graph_blueprint.route("/graph/MS1&MS2Circle/<id>",methods=['GET','POST'])    
def MS1_MS2Circle():
    if request.method== "GET":
        arr=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'circle': '$circleCode', 
                        'circleId': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
        Response=cmo.finding_aggregate("circle",arr)
        return respond(Response)


@graph_blueprint.route("/graph/cumulativeWorkdone",methods=["POST","GET"])
@graph_blueprint.route("/graph/cumulativeWorkdone/<id>",methods=["POST","GET"])
@token_required
def cumulativeWorkdone(current_user,id=None):
    
    subuid = []
    subobjectuid = []
    arra = [
        {
            '$match': {
                'projectType': {
                    '$in': [
                        'ULS', 'MACRO', 'RELOCATION', 'UBR', 'DEGROW', 'MICROWAVE'
                    ]
                }
            }
        }, {
            '$project': {
                'subuid': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)['data']
    if response:
        for i in response:
            subuid.append(i['subuid'])
            subobjectuid.append(ObjectId(i['subuid']))
    
    if request.method == "GET":
        
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']
        
        projection_stage = {
            '_id': 0,
            'projectId':1,
            "projectuid":1,
            'year':1,
        }
        for field in viewBy:
                projection_stage[f'M-{field}'] = 1
                
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
                '$match':{
                    'status': 'Active', 
                    'deleteStatus': {'$ne': 1},
                    'projectType':{'$in':subobjectuid}
                }
            }, {
                '$project': {
                    'projectId': 1,  
                    'projectuid': {'$toString': '$_id'}, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'evmActual', 
                    'localField': 'projectuid', 
                    'foreignField': 'projectuid', 
                    'pipeline': [
                        {
                            '$match': {
                                'year': year
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
                    'result.projectuid': '$projectuid',
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len(dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            plan_sum = {}
            for i in range(1, 13):
                column_name = f'M-{i}'
                if column_name in dataRespdf.columns:
                    plan_sum[column_name] = dataRespdf[column_name].sum()

            df_pv = pd.DataFrame([plan_sum])
            df_pv_cumsum = df_pv.cumsum(axis=1)

            year = int(year)
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
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1}, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {'$ne': None},
                        "mileStoneStatus":"Closed",
                        "SubProjectId":{'$in':subuid},
                        'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
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
                result = ms2Datadf.groupby(['month','year','projectuid']).size().reset_index(name='count')
                pivot_df = result.pivot_table(index=['year','projectuid'],columns='month',values='count',fill_value=0 ).reset_index()
                pivot_df.columns = [f'count-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                pivot_df.drop(columns=['year'], inplace=True)
                pivot_df = dataRespdf.merge(pivot_df,on='projectuid',how='left')

                count_columns = [f'count-{i}' for i in range(1, 13)]

                for col in count_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0 

                pivot_df = pivot_df[count_columns]

                count_columns = {}

                for i in range(1, 13): 
                    column_name = f'count-{i}'
                    if column_name in pivot_df.columns:
                        count_columns[column_name] = pivot_df[column_name].sum()

                pivot_df = pd.DataFrame([count_columns])

                df_cumsum = pivot_df.cumsum(axis=1)

                
                df_ach = ((df_cumsum.values / df_pv_cumsum.values) * 100).round().astype(int)
                df_ach = pd.DataFrame(df_ach, columns=[f"ach-{i+1}" for i in range(df_ach.shape[1])])

                merged_df = pd.concat([df_cumsum, df_pv_cumsum,df_ach], axis=1)
                merged_df = merged_df.fillna(0)

                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                values = []
                for i in range(1, current_month() + 1):
                    month_name = month_names[i]
                    row = {
                        'description': month_name,
                        'month':i,
                        'count': merged_df.get(f'count-{i}', [0])[0],
                        'plan': merged_df.get(f'M-{i}', [0])[0],
                        'ach':merged_df.get(f'ach-{i}',[0])[0],
                    }
                    values.append(row)

                new_df = pd.DataFrame(values)
                dataResp['data'] = json.loads(new_df.to_json(orient="records"))
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                "msg":'Data not found',
                "data":[]
            })

    if request.method == "POST":
        allData = request.get_json()
        year = current_year()
        viewBy = ['1','2','3','4','5','6','7','8','9','10','11','12']

        if "year" in allData:
            year = allData['year']

        if "viewBy" in allData:
            viewBy = allData['viewBy']

        if "projectType" in allData:
            projectTypeinObjectId = []
            allData['projectType'] = [item for sublist in allData['projectType'] for item in sublist] 
            for i in allData['projectType']:
                projectTypeinObjectId.append(ObjectId(i))

        projection_stage = {
            '_id': 0,
            'projectType':1,
            'projectId':1,
            "projecttypeuid":1,
            "projectuid":1,
            "year":{'$toInt':'$year'}
        }
        for field in viewBy:
                projection_stage[f'M-{field}'] = 1

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
                '$match':{
                    'status': 'Active', 
                    'deleteStatus': {'$ne': 1},
                    'projectType':{'$in':subobjectuid}
                }
            }
        ]
        if "projectType" in allData:
            arra = arra + [
                {
                    '$match':{
                        'projectType':{
                            '$in':projectTypeinObjectId
                        }
                    }
                }
            ]
        arra = arra + [
            {
                '$project': {
                    'projectId': 1,  
                    'projectuid': {'$toString': '$_id'},
                    '_id': 0
                }
            }, {
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
                    'result.projectuid': '$projectuid', 
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': projection_stage
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra)
        if len(dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
            plan_sum = {}
            month = [int(x) for x in viewBy]

            for i in month: 
                column_name = f'M-{i}'
                if column_name in dataRespdf.columns:
                    plan_sum[column_name] = dataRespdf[column_name].sum()

            df_pv = pd.DataFrame([plan_sum])
            df_pv_cumsum = df_pv.cumsum(axis=1)

            year = int(year)
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
                'projectType':1,
                'month': 1, 
                'year': 1, 
                '_id': 0
            }
            
            arra = [
                {
                    '$match': {
                        'deleteStatus': {'$ne': 1}, 
                        'Name': 'MS2', 
                        'CC_Completion Date': {'$ne': None},
                        "mileStoneStatus":"Closed",
                        "SubProjectId":{'$in':subuid},
                        'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])}
                    }
                }
            ]
            if "projectType" in allData:
                arra = arra + [
                    {
                        '$match':{
                            'SubProjectId':{
                                '$in':allData['projectType']
                            }
                        }
                    }
                ]
            arra = arra + [
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
                result = ms2Datadf.groupby(['month','year','projectuid']).size().reset_index(name='count')
                pivot_df = result.pivot_table(index=['year','projectuid'],columns='month',values='count',fill_value=0 ).reset_index()
                pivot_df.columns = [f'count-{col}' if isinstance(col, int) else col for col in pivot_df.columns]
                pivot_df.drop(columns=['year'], inplace=True)
                pivot_df = dataRespdf.merge(pivot_df,on='projectuid',how='left')

                count_columns = [f'count-{i}' for i in month]

                for col in count_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = 0 

                pivot_df = pivot_df[count_columns]

                count_columns = {}

                for i in month: 
                    column_name = f'count-{i}'
                    if column_name in pivot_df.columns:
                        count_columns[column_name] = pivot_df[column_name].sum()

                pivot_df = pd.DataFrame([count_columns])

                df_cumsum = pivot_df.cumsum(axis=1)

                
                df_ach = ((df_cumsum.values / df_pv_cumsum.values) * 100).round().astype(int)
                df_ach = pd.DataFrame(df_ach, columns=[f"ach-{i+1}" for i in range(df_ach.shape[1])])

                merged_df = pd.concat([df_cumsum, df_pv_cumsum,df_ach], axis=1)
                merged_df = merged_df.fillna(0)

                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                values = []
                for i in month:
                    month_name = month_names[i]
                    row = {
                        'description': month_name,
                        'month':i,
                        'count': merged_df.get(f'count-{i}', [0])[0],
                        'plan': merged_df.get(f'M-{i}', [0])[0],
                        'ach':merged_df.get(f'ach-{i}',[0])[0],
                    }
                    values.append(row)

                new_df = pd.DataFrame(values)
                dataResp['data'] = json.loads(new_df.to_json(orient="records"))
            return respond(dataResp)
        else:
            return respond({
                'status':200,
                "msg":'Data not found',
                "data":[]
            })
        


@graph_blueprint.route("/graph/getZone",methods=["POST","GET"])
@graph_blueprint.route("/graph/getZone/<id>",methods=["POST","GET"])
@token_required
def getZone(current_user, id=None):
    if request.method == "GET":
        arra=[
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
                                                }, {
                                                    '$lookup': {
                                                        'from': 'zone', 
                                                        'localField': 'zone', 
                                                        'foreignField': '_id', 
                                                        'as': 'zoneresult'
                                                    }
                                                }, {
                                                    '$addFields': {
                                                        'zone': {
                                                            '$arrayElemAt': [
                                                                '$zoneresult.shortCode', 0
                                                            ]
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
                                            'zone': {
                                                '$arrayElemAt': [
                                                    '$costCenter.zone', 0
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
                                'zone': {
                                    '$arrayElemAt': [
                                        '$result.zone', 0
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
                '$project': {
                    'projectgroupuid': 1, 
                    'zone': 1
                }
            },
            {
                '$sort': {
                    'zone': 1
                }
            }
        ]
        response = cmo.finding_aggregate("projectAllocation",arra)
        return respond(response)
    
    
@graph_blueprint.route('/graph/MS2VsWCCPendingReason',methods=["GET","POST"])
@token_required
def MS2VsWCCPendingReason(current_user):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'Name': 'MS2', 
                    'mileStoneStatus': 'Closed', 
                    'deleteStatus': {'$ne': 1}
                }   
            }, {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
            
            arra = [
                {
                    '$match':{
                        '_id':{
                            '$in':siteuid
                        },
                        'siteBillingStatus': 'Unbilled',
                        'deleteStatus':{'$ne':1}
                    }
                }, {
                    '$project':{
                        "reason":'$WCC SIGNOFF PENDING REASON',
                        "_id":0
                    }
                }, {
                    '$match': {
                        '$and': [
                            {
                                'reason': {
                                    '$ne': None
                                }
                            }, {
                                'reason': {
                                    '$ne': ''
                                }
                            }
                        ]
                    }
                }, {
                    '$group': {
                        '_id': '$reason', 
                        'count': {
                            '$sum': 1
                        }, 
                        'description': {
                            '$first': '$reason'
                        }
                    }
                }, {
                    '$sort': {
                        'count': 1
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer",arra)
            return respond(response)

    if request.method == "POST":
        data = request.get_json()
        arra = [
            {
                '$match': {
                        'Name': 'MS2', 
                        'mileStoneStatus': 'Closed', 
                        'deleteStatus': {'$ne': 1}
                }   
            }
        ]
        if "circleName" in data:
            arra = arra + [
                {
                    '$match':{
                        'projectuniqueId':{
                            '$in':[item for sublist in data['circleName'] for item in sublist]
                        }
                    }
                }
            ]
        if "projectType" in data:
            arra = arra + [
                {
                    '$match':{
                        'SubProjectId':{
                            '$in':[item for sublist in data['projectType'] for item in sublist]
                        }
                    }
                }
            ]
        arra = arra + [
            {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
            
            arra = [
                {
                    '$match':{
                        '_id':{
                            '$in':siteuid
                        },
                        'siteBillingStatus': 'Unbilled',
                        'deleteStatus':{'$ne':1}
                    }
                }, {
                    '$project':{
                        "reason":'$WCC SIGNOFF PENDING REASON',
                        "_id":0
                    }
                }, {
                    '$match': {
                        '$and': [
                            {
                                'reason': {
                                    '$ne': None
                                }
                            }, {
                                'reason': {
                                    '$ne': ''
                                }
                            }
                        ]
                    }
                }, {
                    '$group': {
                        '_id': '$reason', 
                        'count': {
                            '$sum': 1
                        }, 
                        'description': {
                            '$first': '$reason'
                        }
                    }
                }, {
                    '$sort': {
                        'count': 1
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer",arra)
            return respond(response)
        else:
            return respond({
                'status':200,
                "msg":"Data Get Successfully",
                'data':[]
            })
            
            
            
#=========================================================================

@graph_blueprint.route('/graph/softMS1Reason',methods=["GET","POST"])
def softMS1Reason():
    if request.method == "GET":
        subuid = []
        arra = [
            {
                '$match': {
                    'projectType': {
                        '$in': [
                            'ULS', 'MACRO', 'RELOCATION'
                        ]
                    }, 
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'subuid': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("projectType",arra)['data']
        for i in response:
            subuid.append(i['subuid'])
        arra = [
            {
                '$match': {
                    '$and': [
                        {
                            'SubProjectId':{'$in':subuid}
                        }, {
                            'Name': {
                                '$in': [
                                    'MS1', 'MS2'
                                ]
                            }
                        }, {
                            '$or': [
                                {
                                    '$and': [
                                        {
                                            'Name': 'MS1'
                                        }, {
                                            'mileStoneStatus': 'Closed'
                                        }
                                    ]
                                }, {
                                    '$and': [
                                        {
                                            'Name': 'MS2'
                                        }, {
                                            'mileStoneStatus': 'Open'
                                        }
                                    ]
                                }
                            ]
                        }, {
                            'deleteStatus': {'$ne': 1}
                        }
                    ]
                }
            }, {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
                
        arra = [
            {
                '$match':{
                    '_id':{
                        '$in':siteuid
                    },
                    'deleteStatus':{'$ne':1},
                }
            }, {
                '$addFields': {
                    'reasonSoftAt':'$REASON-SOFT AT REJECTION/NOT OFFERED BEYOND 5 DAYS'
                }
            }, {
                '$match': {
                    '$and': [
                        {
                            'reasonSoftAt': {
                                '$ne': None
                            }
                        }, {
                            'reasonSoftAt': {
                                '$ne': ''
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$reasonSoftAt', 
                    'count': {
                        '$sum': 1
                    }, 
                    'description': {
                        '$first': '$reasonSoftAt'
                    }
                }
            }, {
                '$sort': {
                    'count': -1
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        return respond(response)
    
    if request.method == "POST":
        data = request.get_json()
        subuid = []
        if "projectType" in data:
            for i in [item for sublist in data['projectType'] for item in sublist]:
                subuid.append(i)
        else:
            arra = [
                {
                    '$match': {
                        'projectType': {
                            '$in': [
                                'ULS', 'MACRO', 'RELOCATION'
                            ]
                        }, 
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'subuid': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType",arra)['data']
            for i in response:
                subuid.append(i['subuid'])
        
        arra = [
            {
                '$match': {
                    '$and': [
                        {
                            'Name': {
                                '$in': [
                                    'MS1', 'MS2'
                                ]
                            }
                        }, {
                            '$or': [
                                {
                                    '$and': [
                                        {
                                            'Name': 'MS1'
                                        }, {
                                            'mileStoneStatus': 'Closed'
                                        }
                                    ]
                                }, {
                                    '$and': [
                                        {
                                            'Name': 'MS2'
                                        }, {
                                            'mileStoneStatus': 'Open'
                                        }
                                    ]
                                }
                            ]
                        }, {
                            'SubProjectId':{'$in':subuid}
                        }
                    ]
                }
            }
        ]
        if "circleName" in data:
            arra = arra + [
                {
                    '$match':{
                        'projectuniqueId':{
                            '$in':[item for sublist in data['circleName'] for item in sublist]
                        }
                    }
                }
            ]
        arra = arra +[
            {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
            arra = [
                {
                    '$match':{
                        '_id':{
                            '$in':siteuid
                        },
                    }
                }, {
                    '$addFields': {
                        'reasonSoftAt':'$REASON-SOFT AT REJECTION/NOT OFFERED BEYOND 5 DAYS'
                            
                        
                    }
                }, {
                    '$match': {
                        '$and': [
                            {
                                'reasonSoftAt': {
                                    '$ne': None
                                }
                            }, {
                                'reasonSoftAt': {
                                    '$ne': ''
                                }
                            }
                        ]
                    }
                }, {
                    '$group': {
                        '_id': '$reasonSoftAt', 
                        'count': {
                            '$sum': 1
                        }, 
                        'description': {
                            '$first': '$reasonSoftAt'
                        }
                    }
                }, {
                    '$sort': {
                        'count': -1
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer",arra)
            return respond(response) 
        else:
            return respond({
                'status':200,
                "msg":"Data get Successfully",
                "data":[]
            })

@graph_blueprint.route('/graph/phyMS1Reason',methods=["GET","POST"])
def phyMS1Reason():
    if request.method == "GET":
        subuid = []
        arra = [
            {
                '$match': {
                    'projectType': {
                        '$in': [
                            'ULS', 'MACRO', 'RELOCATION'
                        ]
                    }, 
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'subuid': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("projectType",arra)['data']
        for i in response:
            subuid.append(i['subuid'])
        arra = [
            {
                '$match': {
                    '$and': [
                        {
                            'Name': {
                                '$in': [
                                    'MS1', 'MS2'
                                ]
                            }
                        }, {
                            '$or': [
                                {
                                    '$and': [
                                        {
                                            'Name': 'MS1'
                                        }, {
                                            'mileStoneStatus': 'Closed'
                                        }
                                    ]
                                }, {
                                    '$and': [
                                        {
                                            'Name': 'MS2'
                                        }, {
                                            'mileStoneStatus': 'Open'
                                        }
                                    ]
                                }
                            ]
                        }, {
                            'deleteStatus': {'$ne': 1}
                        }, {
                            'SubProjectId':{'$in':subuid}
                        }
                    ]
                }
            }, {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
        arra = [
            {
                '$match':{
                    '_id':{
                        '$in':siteuid
                    },
                    'deleteStatus':{'$ne':1},
                }
            }, {
                '$addFields': {
                    'reasonSoftAt':'$REASON- PHY AT REJECTION/NOT OFFERED BEYOND 5 DAYS'
                }
            }, {
                '$match': {
                    '$and': [
                        {
                            'reasonSoftAt': {
                                '$ne': None
                            }
                        }, {
                            'reasonSoftAt': {
                                '$ne': ''
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$reasonSoftAt', 
                    'count': {
                        '$sum': 1
                    }, 
                    'description': {
                        '$first': '$reasonSoftAt'
                    }
                }
            }, {
                '$sort': {
                    'count': -1
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        return respond(response)

    if request.method == "POST":
        data = request.get_json()
        subuid = []
        if "projectType" in data:
            for i in [item for sublist in data['projectType'] for item in sublist]:
                subuid.append(i)
        else:
            arra = [
                {
                    '$match': {
                        'projectType': {
                            '$in': [
                                'ULS', 'MACRO', 'RELOCATION'
                            ]
                        }, 
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'subuid': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType",arra)['data']
            for i in response:
                subuid.append(i['subuid'])
        arra = [
            {
                '$match': {
                    '$and': [
                        {
                            'Name': {
                                '$in': [
                                    'MS1', 'MS2'
                                ]
                            }
                        }, {
                            '$or': [
                                {
                                    '$and': [
                                        {
                                            'Name': 'MS1'
                                        }, {
                                            'mileStoneStatus': 'Closed'
                                        }
                                    ]
                                }, {
                                    '$and': [
                                        {
                                            'Name': 'MS2'
                                        }, {
                                            'mileStoneStatus': 'Open'
                                        }
                                    ]
                                }
                            ]
                        }, {
                            'deleteStatus': {'$ne': 1}
                        }, {
                            'SubProjectId':{'$in':subuid}
                        }
                    ]
                }
            }
        ]
        if "circleName" in data:
            arra = arra + [
                {
                    '$match':{
                        'projectuniqueId':{
                            '$in':[item for sublist in data['circleName'] for item in sublist]
                        }
                    }
                }
            ]
        arra = arra +[
            {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
            arra = [
                {
                    '$match':{
                        '_id':{
                            '$in':siteuid
                        },
                        'deleteStatus':{'$ne':1},
                    }
                }, {
                    '$addFields': {
                        'reasonSoftAt':'$REASON- PHY AT REJECTION/NOT OFFERED BEYOND 5 DAYS'
                    }
                }, {
                    '$match': {
                        '$and': [
                            {
                                'reasonSoftAt': {
                                    '$ne': None
                                }
                            }, {
                                'reasonSoftAt': {
                                    '$ne': ''
                                }
                            }
                        ]
                    }
                }, {
                    '$group': {
                        '_id': '$reasonSoftAt', 
                        'count': {
                            '$sum': 1
                        }, 
                        'description': {
                            '$first': '$reasonSoftAt'
                        }
                    }
                }, {
                    '$sort': {
                        'count': -1
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer",arra)
            return respond(response)
        else:
                return respond({
                    'status':200,
                    "msg":"Data get Successfully",
                    "data":[]
                })
       


@graph_blueprint.route('/graph/kpiMS1Reason',methods=["GET","POST"])
def kpiMS1Reason():
    if request.method == "GET":
        subuid = []
        arra = [
            {
                '$match': {
                    'projectType': {
                        '$in': [
                            'ULS', 'MACRO', 'RELOCATION'
                        ]
                    }, 
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'subuid': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("projectType",arra)['data']
        for i in response:
            subuid.append(i['subuid'])
        arra = [
            {
                '$match': {
                    '$and': [
                        {
                            'Name': {
                                '$in': [
                                    'MS1', 'MS2'
                                ]
                            }
                        }, {
                            '$or': [
                                {
                                    '$and': [
                                        {
                                            'Name': 'MS1'
                                        }, {
                                            'mileStoneStatus': 'Closed'
                                        }
                                    ]
                                }, {
                                    '$and': [
                                        {
                                            'Name': 'MS2'
                                        }, {
                                            'mileStoneStatus': 'Open'
                                        }
                                    ]
                                }
                            ]
                        }, {
                            'deleteStatus': {'$ne': 1}
                        }, {
                            'SubProjectId':{'$in':subuid}
                        }
                    ]
                }
            }, {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
                
        arra = [
            {
                '$match':{
                    '_id':{
                        '$in':siteuid
                    },
                    'deleteStatus':{'$ne':1},
                }
            }, {
                '$addFields': {
                    'reasonSoftAt':'$REASON- KPI REJECTION/ NOT OFFERED BEYOND 7 DAYS'
                }
            }, {
                '$match': {
                    '$and': [
                        {
                            'reasonSoftAt': {
                                '$ne': None
                            }
                        }, {
                            'reasonSoftAt': {
                                '$ne': ''
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$reasonSoftAt', 
                    'count': {
                        '$sum': 1
                    }, 
                    'description': {
                        '$first': '$reasonSoftAt'
                    }
                }
            }, {
                '$sort': {
                    'count': -1
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        return respond(response)
    
    if request.method == "POST":
        data = request.get_json()
        subuid = []
        if "projectType" in data:
            for i in [item for sublist in data['projectType'] for item in sublist]:
                subuid.append(i)
        else:
            arra = [
                {
                    '$match': {
                        'projectType': {
                            '$in': [
                                'ULS', 'MACRO', 'RELOCATION'
                            ]
                        }, 
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'subuid': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType",arra)['data']
            for i in response:
                subuid.append(i['subuid'])
        
        arra = [
            {
                '$match': {
                    '$and': [
                        {
                            'Name': {
                                '$in': [
                                    'MS1', 'MS2'
                                ]
                            }
                        }, {
                            '$or': [
                                {
                                    '$and': [
                                        {
                                            'Name': 'MS1'
                                        }, {
                                            'mileStoneStatus': 'Closed'
                                        }
                                    ]
                                }, {
                                    '$and': [
                                        {
                                            'Name': 'MS2'
                                        }, {
                                            'mileStoneStatus': 'Open'
                                        }
                                    ]
                                }
                            ]
                        }, {
                            'SubProjectId':{'$in':subuid}
                        }
                    ]
                }
            }
        ]
        if "circleName" in data:
            arra = arra + [
                {
                    '$match':{
                        'projectuniqueId':{
                            '$in':[item for sublist in data['circleName'] for item in sublist]
                        }
                    }
                }
            ]
        arra = arra +[
            {
                '$project':{
                    'siteId':1,
                    "_id":0
                }
            }
        ]
        milestoneData = cmo.finding_aggregate("milestone",arra)['data']
        if milestoneData:
            siteuid = []
            for i in milestoneData:
                siteuid.append(ObjectId(i['siteId']))
            arra = [
                {
                    '$match':{
                        '_id':{
                            '$in':siteuid
                        },
                    }
                }, {
                    '$addFields': {
                        'reasonSoftAt':'$REASON- KPI REJECTION/ NOT OFFERED BEYOND 7 DAYS'
                            
                        
                    }
                }, {
                    '$match': {
                        '$and': [
                            {
                                'reasonSoftAt': {
                                    '$ne': None
                                }
                            }, {
                                'reasonSoftAt': {
                                    '$ne': ''
                                }
                            }
                        ]
                    }
                }, {
                    '$group': {
                        '_id': '$reasonSoftAt', 
                        'count': {
                            '$sum': 1
                        }, 
                        'description': {
                            '$first': '$reasonSoftAt'
                        }
                    }
                }, {
                    '$sort': {
                        'count': -1
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer",arra)
            return respond(response) 
        else:
            return respond({
                'status':200,
                "msg":"Data get Successfully",
                "data":[]
            })
    
@graph_blueprint.route('/graph/RFAIVsMS1Reason',methods=["GET","POST"])
def RFAIVsMS1Reason():
    if request.method == "GET":
        arra=[
            {
                '$match': {
                    'Name': 'MS1', 
                    'mileStoneStatus': 'Open'
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'siteId': 1
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'RFAI Date': {
                                    '$ne': None, 
                                    '$ne': ''
                                }
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'reason': {
                        '$arrayElemAt': [
                            '$result.RFAI VS MS1 REMARKS', 0
                        ]
                    }
                }
            }, {
                '$match': {
                    '$and': [
                        {
                            'reason': {
                                '$ne': None
                            }
                        }, {
                            'reason': {
                                '$ne': ''
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$reason', 
                    'count': {
                        '$sum': 1
                    }, 
                    'description': {
                        '$first': '$reason'
                    }
                }
            }, {
                '$sort': {
                    'count': -1
                }
            }
        ]
        response = cmo.finding_aggregate("milestone",arra)
        return respond(response)

@graph_blueprint.route('/graph/profitloss',methods=["GET","POST"])
@token_required
def graph_profit_loss(current_user):
    if request.method == "GET":
        costCenterId = []
        costCenter = costCenter_str(current_user['userUniqueId'])
        for i in costCenter:
            costCenterId.append(ObjectId(i['uniqueId']))
        year = current_year()
        month = [current_month(),current_month()-1]
        arra = [
            {
                '$match':{
                    'costCenter':{'$in':costCenterId},
                    'year':year,
                    'month':{
                        '$in':month
                    }
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
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields':{
                    'description':{
                        '$arrayElemAt': [
                            '$result.costCenter', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'costCenter': 0, 
                    '_id': 0,
                    'month':0,
                    'result':0
                }
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)
        if response['data']:
            df = pd.DataFrame.from_dict(response['data'])
            df = df.groupby(['description','year']).sum().reset_index()
            df['projectedGrossMargin'] = df.apply(lambda row: round(((row['projectedRevenue'] - row['projectedCost']) / row['projectedRevenue']) * 100, 0) if row['projectedRevenue'] != 0 else 0, axis=1)
            
            df['actualGrossMargin'] = df.apply(
                lambda row: round(((row['actualRevenue'] - row['actualCost']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            
            df['netMargin'] = df.apply(
                lambda row: round((((row['actualRevenue'] - row['actualCost']) - row['sgna']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            dict_data = df.to_dict(orient='records')
            response['data'] = dict_data
        return respond(response)
    
    if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        if "costCenter" in allData:
            for i in allData['costCenter']:
                costCenterId.append(ObjectId(i))
        else:
            costCenter = costCenter_str(current_user['userUniqueId'])
            for i in costCenter:
                costCenterId.append(ObjectId(i['uniqueId']))
                
        if "year" in allData:
            year = allData['year']
        else:
            year = current_year()
        if "viewBy" in allData:
            month = allData['viewBy']
        else:
            month = [current_month(),current_month()-1]
        
        arra = [
            {
                '$match':{
                    'costCenter':{'$in':costCenterId},
                    'year':year,
                    'month':{
                        '$in':month
                    }
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
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields':{
                    'description':{
                        '$arrayElemAt': [
                            '$result.costCenter', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'costCenter': 0, 
                    '_id': 0,
                    'month':0,
                    'result':0
                }
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)
        if response['data']:
            df = pd.DataFrame.from_dict(response['data'])
            df = df.groupby(['description','year']).sum().reset_index()
            df['projectedGrossMargin'] = df.apply(
                lambda row: round(((row['projectedRevenue'] - row['projectedCost']) / row['projectedRevenue']) * 100, 0)
                if row['projectedRevenue'] != 0 else 0, axis=1)
            
            df['actualGrossMargin'] = df.apply(
                lambda row: round(((row['actualRevenue'] - row['actualCost']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            
            df['netMargin'] = df.apply(
                lambda row: round((((row['actualRevenue'] - row['actualCost']) - row['sgna']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            dict_data = df.to_dict(orient='records')
            response['data'] = dict_data
        return respond(response)
    
@graph_blueprint.route('/graph/profitlossTrend',methods=["GET","POST"])
@token_required
def graph_profit_loss_margin(current_user):
    if request.method == "GET":
        costCenterId = []
        costCenter = costCenter_str(current_user['userUniqueId'])
        for i in costCenter:
            costCenterId.append(ObjectId(i['uniqueId']))
        year = current_year()
        arra = [
            {
                '$match':{
                    'costCenter':{'$in':costCenterId},
                    'year':year,
                }
            }, {
                '$addFields':{
                    'description':"$month"
                }
            }, {
                '$project': {
                    'costCenter': 0, 
                    '_id': 0,
                    'year':0,
                    'month':0
                }
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)
        if response['data']:
            df = pd.DataFrame.from_dict(response['data'])
            df = df.groupby(['description']).sum().reset_index()
            
            df['projectedGrossMargin'] = df.apply(
                lambda row: round(((row['projectedRevenue'] - row['projectedCost']) / row['projectedRevenue']) * 100, 0)
                if row['projectedRevenue'] != 0 else 0, axis=1)
            
            df['actualGrossMargin'] = df.apply(
                lambda row: round(((row['actualRevenue'] - row['actualCost']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            
            df['netMargin'] = df.apply(
                lambda row: round((((row['actualRevenue'] - row['actualCost']) - row['sgna']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            
            
            
            month_mapping = {1: 'Jan',2: 'Feb',3: 'Mar',4: 'Apr',5: 'May',6: 'Jun',7: 'Jul',8: 'Aug',9: 'Sep',10: 'Oct',11: 'Nov',12: 'Dec'}
            df['description'] = df['description'].map(month_mapping)
            delColumn = ['projectedCost','projectedRevenue','actualCost','actualRevenue','sgna']
            for i in delColumn:
                if i in df.columns:
                    del df[i]
            dict_data = df.to_dict(orient='records')
            response['data'] = dict_data
        return respond(response)
    
    if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        if "costCenter" in allData:
            for i in allData['costCenter']:
                costCenterId.append(ObjectId(i))
        else:
            costCenter = costCenter_str(current_user['userUniqueId'])
            for i in costCenter:
                costCenterId.append(ObjectId(i['uniqueId']))
                
        if "year" in allData:
            year = allData['year']
        else:
            year = current_year()
        if "viewBy" in allData:
            month = allData['viewBy']
        else:
            month = [1,2,3,4,5,6,7,8,9,10,11,12]
        
        arra = [
            {
                '$match':{
                    'costCenter':{'$in':costCenterId},
                    'year':year,
                    'month':{'$in':month},
                }
            }, {
                '$addFields':{
                    'description':"$month"
                }
            }, {
                '$project': {
                    'costCenter': 0, 
                    '_id': 0,
                    'year':0,
                    'month':0
                }
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)
        if response['data']:
            df = pd.DataFrame.from_dict(response['data'])
            df = df.groupby(['description']).sum().reset_index()
            
            df['projectedGrossMargin'] = df.apply(
                lambda row: round(((row['projectedRevenue'] - row['projectedCost']) / row['projectedRevenue']) * 100, 0)
                if row['projectedRevenue'] != 0 else 0, axis=1)
            
            df['actualGrossMargin'] = df.apply(
                lambda row: round(((row['actualRevenue'] - row['actualCost']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            
            df['netMargin'] = df.apply(
                lambda row: round((((row['actualRevenue'] - row['actualCost']) - row['sgna']) / row['actualRevenue']) * 100, 0)
                if row['actualRevenue'] != 0 else 0, axis=1)
            
            month_mapping = {1: 'Jan',2: 'Feb',3: 'Mar',4: 'Apr',5: 'May',6: 'Jun',7: 'Jul',8: 'Aug',9: 'Sep',10: 'Oct',11: 'Nov',12: 'Dec'}
            df['description'] = df['description'].map(month_mapping)
            delColumn = ['projectedCost','projectedRevenue','actualCost','actualRevenue','sgna']
            for i in delColumn:
                if i in df.columns:
                    del df[i]
            dict_data = df.to_dict(orient='records')
            response['data'] = dict_data
        return respond(response)
    
    
@graph_blueprint.route('/graph/unbilled/projecttypewise',methods=["GET","POST"])
@token_required
def graph_unbilled_projectType(current_user):
    
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'siteBillingStatus':"Unbilled", 
                    'deleteStatus': {'$ne': 1}, 
                    'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])},
                }
            }, {
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
                    'BAND':{
                        '$ifNull':['$BAND',""]
                    }, 
                    'ACTIVITY':{
                        '$ifNull':['$ACTIVITY',""]
                    },
                    "MS1":1,
                    "MS2":1,
                    '_id':0,
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        if len(response['data']):
            pivotedData = pd.DataFrame(response['data'])
            pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
            
            projectIddf = projectid()
            subProjectdf = subproject()
            masterunitRatedf = masterunitRate()
            
            
            mergedData = pivotedData.merge(projectIddf,on="projectuniqueId",how="left")
            pivotedData['projectId'] = mergedData['projectId']
            
            
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
            # final_df['amount1'] = (final_df['amount1']).fillna(0).round()
            # final_df['amount2'] = ( final_df['amount2']).fillna(0).round()
            # # final_df['final_amount'] = (final_df['amount1'] + final_df['amount2']).fillna(0).round()
            # final_df['final_amount'] = (final_df['amount1'] + final_df['amount2']).fillna(0)
            
            # final_df['final_amount'] = final_df['final_amount'] / 100000
            # final_df['final_amount'] = final_df['final_amount'].round(2)
            final_df[['amount1', 'amount2']] = final_df[['amount1', 'amount2']].fillna(0).round()
            final_df['final_amount'] = ((final_df['amount1'] + final_df['amount2']) / 100000).round(2)
            
            final_df = final_df.groupby('projectType', as_index=False)['final_amount'].sum()
            final_df = final_df.sort_values(by='final_amount',ascending=False) 
            final_df.rename(columns={'projectType': 'description', 'final_amount': 'count'}, inplace=True)
            dict_data = json.loads(final_df.to_json(orient='records'))
            return respond({
               'status':200,
                "msg":"Data get Successfully",
                "data":dict_data
            })
        else:
            return respond({
                'status':200,
                "msg":"Data get Successfully",
                "data":[]
            })
        
    if request.method == "POST":
        allData = request.get_json()
        # arra = [
        #     {
        #         '$match': {
        #             'siteBillingStatus':"Unbilled", 
        #             'deleteStatus': {'$ne': 1},
        #         }
        #     }
        # ]
        # if "projectType" in allData:
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'SubProjectId':{
        #                     '$in':allData['projectType']
        #                 }
        #             }
        #         }
        #     ]
        # arra = arra + [
        #     {
        #         '$project':{
        #             'projectuniqueId': 1, 
        #             'SubProjectId': 1, 
        #             'BAND':{
        #                 '$ifNull':['$BAND',""]
        #             }, 
        #             'ACTIVITY':{
        #                 '$ifNull':['$ACTIVITY',""]
        #             },
        #             'siteId':{'$toString':'$_id'},
        #             '_id':0,
        #         }
        #     }
        # ]
        # SiteData = cmo.finding_aggregate("SiteEngineer",arra)['data']
        # SiteDatadf = pd.DataFrame(SiteData)
        
        # arra = [
        #     {
        #         '$match':{
        #             'deleteStatus': {'$ne': 1}, 
        #             'Name': {'$in': ['MS1', 'MS2']}, 
        #             'mileStoneStatus': 'Closed',
        #         }
        #     }
        # ]
        # if "projectType" in allData:
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'SubProjectId':{
        #                     '$in':allData['projectType']
        #                 }
        #             }
        #         }
        #     ]
        # arra = arra + [
        #     {
        #         '$project':{
        #             'siteId':{'$toString':'$siteId'},
        #             "Name":1,
        #             "CC_Completion Date":1,
        #             '_id':0
        #         }
        #     }
        # ]
        # MilestoneData = cmo.finding_aggregate("milestone",arra)['data']
        # MilestoneDatadf = pd.DataFrame(MilestoneData)
        # MilestoneDatadf = MilestoneDatadf.pivot(index='siteId', columns='Name', values='CC_Completion Date').reset_index()
        
        # mergedData = MilestoneDatadf.merge(SiteDatadf,on='siteId',how="left")
        
        # if "siteId" in mergedData:
        #     del mergedData['siteId']
    
        # if not mergedData.empty:
        #     pivotedData = mergedData
        #     pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
            
        arra = [
            {
                '$match': {
                    'siteBillingStatus':"Unbilled", 
                    'deleteStatus': {'$ne': 1}, 
                    'projectuniqueId':{'$in':projectId_str(current_user['userUniqueId'])},
                }
            }
        ]
        if "projectType" in allData:
            arra = arra + [
                {
                    '$match':{
                        'SubProjectId':{
                            '$in':allData['projectType']
                        }
                    }
                }
            ]
        arra = arra +[
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
                    'BAND':{
                        '$ifNull':['$BAND',""]
                    }, 
                    'ACTIVITY':{
                        '$ifNull':['$ACTIVITY',""]
                    },
                    "MS1":1,
                    "MS2":1,
                    '_id':0,
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer",arra)
        if len(response['data']):
            pivotedData = pd.DataFrame(response['data'])
            pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
            
            projectIddf = projectid()
            subProjectdf = subproject()
            masterunitRatedf = masterunitRate()
            
            
            
            mergedData = pivotedData.merge(projectIddf,on="projectuniqueId",how="left")
            pivotedData['projectId'] = mergedData['projectId']
            
            
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
            # final_df['amount1'] = (final_df['amount1']).fillna(0).round()
            # final_df['amount2'] = ( final_df['amount2']).fillna(0).round()
            # final_df['final_amount'] = (final_df['amount1'] + final_df['amount2']).fillna(0).round()
            
            final_df[['amount1', 'amount2']] = (final_df[['amount1', 'amount2']].fillna(0) / 100000).round(2)
            final_df['final_amount'] = (final_df['amount1'] + final_df['amount2']).round(2)
            # final_df['final_amount'] = ((final_df['amount1'] + final_df['amount2']) / 100000).round(2)
            
            Amount = "final_amount"
            if "amount" in allData:
                Amount = allData['amount']
            
            if "projectType" in allData:
                final_df = final_df.groupby('subProject', as_index=False)[Amount].sum()
                final_df = final_df.sort_values(by=Amount,ascending=False) 
                final_df.rename(columns={'subProject': 'description', Amount: 'count'}, inplace=True)
            else:
                final_df = final_df.groupby('projectType', as_index=False)[Amount].sum()
                final_df = final_df.sort_values(by=Amount,ascending=False) 
                final_df.rename(columns={'projectType': 'description', Amount: 'count'}, inplace=True)
              
            dict_data = json.loads(final_df.to_json(orient='records'))
            return respond({
               'status':200,
                "msg":"Data get Successfully",
                "data":dict_data
            })
        else:
            return respond({
                'status':200,
                "msg":"Data get Successfully",
                "data":[]
            })