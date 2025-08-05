from base import *
from common.config import *
from blueprint_routes.currentuser_blueprint import projectId_str,projectGroup_str,sub_project
from blueprint_routes.sample_blueprint import *
import datetime
from datetime import datetime as dtt
import pytz
import calendar
from calendar import monthrange

from blueprint_routes.poLifeCycle_blueprint import projectGroup,projectid,subproject,masterunitRate

gpTrackingScheduler_blueprint = Blueprint("gpTrackingScheduler_blueprint", __name__)

def getcccdc():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_time = dtt.now(kolkata_tz)
    return current_time.strftime('%Y-%m-%d %H:%M:%S')

def getcccdc2():
    india_tz = pytz.timezone("Asia/Kolkata")
    current_time = dtt.now(india_tz).strftime("%Y-%m-%d %H:%M:%S")
    return current_time

def getCustomMonthYear():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    today = dtt.now(kolkata_tz)
    if today.day >= 26:
        if today.month == 12:
            custom_month = 1  # January
            custom_year = today.year + 1  # Next year
        else:
            custom_month = today.month + 1  # Next month
            custom_year = today.year  # Same year
    else:
        if today.month == 1:
            custom_month = 12  # December
            custom_year = today.year - 1  # Previous year
        else:
            custom_month = today.month - 1  # Previous month
            custom_year = today.year  # Same year

    # Calculate days_spent in the custom month
    if today.day >= 26:
        days_spent = today.day - 25  # Days from 26th to today
    else:
        # Days from 26th of previous month to today
        prev_month_days = monthrange(today.year, today.month - 1 if today.month > 1 else 12)[1]
        days_spent = (prev_month_days - 25) + today.day
    if custom_month == 1 and today.month == 12 and today.day >= 26:
        # Special case: December 26 to January 25
        total_days_in_custom_month = (31 - 25) + 25  # Days from Dec 26 to Jan 25
    else:
        # Normal case: 26th of current month to 25th of next month
        total_days_in_custom_month = (monthrange(custom_year, custom_month)[1] - 25) + 25

    return custom_month+1, custom_year, days_spent, total_days_in_custom_month
  
def get_custom_month_year(date):
    # If date is 26 or later, move to next month
    if date.day >= 26:
        custom_date = date + pd.DateOffset(months=1)
    else:
        custom_date = date
    # Custom logic for year and month
    custom_month = custom_date.month
    custom_year = custom_date.year
    return pd.Series([custom_month, custom_year])
  
def custom_month_year(date):
    if date.day > 25:
        if date.month == 12:
            custom_month = 1
            custom_year = date.year + 1
        else:
            custom_month = date.month + 1
            custom_year = date.year
    else:
        custom_month = date.month
        custom_year = date.year
    
    return custom_month, custom_year 


  
  
def get_current_month_days():
    today = dtt.today()
    year, month = today.year, today.month
    total_days = calendar.monthrange(year, month)[1]
    spent_days = today.day
    return {"month": month, "total_days": total_days, "spent_days": spent_days}
   
def sub_project2():
    
    
    art=[
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
                                }, 
                                
                            }
                        }
                    ], 
                    'as': 'customerResults'
                }
            }
        ]
    
    arra = art+[
        {
            '$match': {
                'deleteStatus': {'$ne': 1},
                'status':'Active' 
                # '_id': {
                #     '$in': projectId_Object(empId)
                # }
            }
        },
        {
            '$project': {
                'projectType': 1, 
                '_id': 0
            }
        },
        {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'projectType', 
                'foreignField': '_id', 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'projectType': {
                    '$arrayElemAt': [
                        '$result.projectType', 0
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$projectType', 
                'projectType': {
                    '$first': '$projectType'
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
            '$addFields': {
                'uid': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$group': {
                '_id': '$projectType', 
                'projectType': {
                    '$first': '$projectType'
                }, 
                'uid': {
                    '$push': '$uid'
                }
            }
        }, {
            '$sort': {
                'projectType': 1
            }
        }, {
            '$project': {
                'projectType': 1, 
                'uid': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)['data']
    if len(response):
        yydyd=[]
        # print(response,'dduhjdyjhdhhdhhjjhd')
        for i in response:
            if 'uid' in i:
                if len(i['uid']):
                    yydyd=yydyd+i['uid']
                     
                    
        # print(yydyd,'8794794747yiebdbdebm')    
        return yydyd
    
    else:
        return[{'projectType':'','uid':[]}]


def projectId_str2():
    arra = [
        # {
        #     '$match': {
        #         'empId': '65d84c4caf0c03c070f4da9d'
        #     }
        # },
        {
            '$unwind': {
                'path': '$projectIds', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$project': {
                'projectIds':{
                    '$toString':'$projectIds'
                },
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectAllocation",arra)['data']
    projectId = []
    if len(response):
        for i in response:
            projectId.append(i['projectIds'])
    
    return projectId


# def toGetWorkDoneData():
#     arra = [
#         {
#             '$match':{
#                 'deleteStatus': {'$ne': 1},
#                 # 'projectuniqueId':'67c2b762f4b5d73f9bf7db62'
                
#             }
#         }
#     ]
#     arra = arra +[
#          {
#             '$lookup': {
#                 'from': 'milestone', 
#                 'localField': '_id', 
#                 'foreignField': 'siteId', 
#                 'pipeline': [
#                     {
#                         '$match': {
#                             'deleteStatus': {'$ne': 1}, 
#                             'Name': {'$in': ['MS1', 'MS2']}, 
#                             'mileStoneStatus': 'Closed'
#                         }
#                     }, {
#                         '$addFields': {
#                             'CC_Completion Date': {
#                                 '$toDate': '$CC_Completion Date'
#                             }
#                         }
#                     }, {
#                         '$addFields': {
#                             'CC_Completion Date': {
#                                 '$dateAdd': {
#                                     'startDate': '$CC_Completion Date', 
#                                     'unit': 'minute', 
#                                     'amount': 330
#                                 }
#                             }
#                         }
#                     }, {
#                         '$sort': {
#                             'Name': 1
#                         }
#                     }
#                 ], 
#                 'as': 'Milestone'
#             }
#         }, {
#             '$match': {
#                 'Milestone': {
#                     '$ne': []
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'MS1': {
#                     '$arrayElemAt': [
#                         {
#                             '$filter': {
#                                 'input': '$Milestone', 
#                                 'as': 'milestone', 
#                                 'cond': {
#                                     '$eq': [
#                                         '$$milestone.Name', 'MS1'
#                                     ]
#                                 }
#                             }
#                         }, 0
#                     ]
#                 }, 
#                 'MS2': {
#                     '$arrayElemAt': [
#                         {
#                             '$filter': {
#                                 'input': '$Milestone', 
#                                 'as': 'milestone', 
#                                 'cond': {
#                                     '$eq': [
#                                         '$$milestone.Name', 'MS2'
#                                     ]
#                                 }
#                             }
#                         }, 0
#                     ]
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'MS1': '$MS1.CC_Completion Date',
#                 'MS2': '$MS2.CC_Completion Date',
#             }
#         }, 
        
#         {
#             '$project':{
#                 'projectuniqueId': 1, 
#                 'SubProjectId': 1, 
#                 'siteBillingStatus': 1, 
#                 'BAND':{
#                     '$ifNull':['$BAND',""]
#                 }, 
#                 'ACTIVITY':{
#                     '$ifNull':['$ACTIVITY',""]
#                 },
#                 'siteId': {
#                     '$toString': '$_id'
#                 }, 
#                 'siteuid':{
#                     '$toString':'$_id'
#                 },
#                 'Site Id':1,
#                 "MS1":1,
#                 "MS2":1,
#                 "systemId":1,
#                 '_id':0,
#                 'projectIdNew':"$projectuniqueId"
#             }
#                 },
               
#     ]
#     response = cmo.finding_aggregate("SiteEngineer",arra)
#     if len(response['data']):
#         pivotedData = pd.DataFrame(response['data'])
#         pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
#         projectGroupdf = projectGroup()
#         projectIddf = projectid()
#         subProjectdf = subproject()
#         masterunitRatedf = masterunitRate()
#         mergedData = projectIddf.merge(projectGroupdf,on='projectGroup',how='left')
#         projectIddf['projectGroup'] = mergedData['projectGroupName']
#         projectIddf['customer'] = mergedData['Customer']
#         # print(mergedData.columns,'mergedData1')
#         projectIddf['costCenter'] = mergedData['costCenter']
#         # print(projectIddf.columns,'mergedData2')
#         mergedData = pivotedData.merge(projectIddf,on="projectuniqueId",how="left")
        
#         pivotedData['projectId'] = mergedData['projectId']
#         pivotedData['projectGroup'] = mergedData['projectGroup']
#         pivotedData['customer'] = mergedData['customer']
#         pivotedData['costCenter'] = mergedData['costCenter']
#         mergedData = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
#         pivotedData['subProject'] = mergedData['subProject']
#         pivotedData['projectType'] = mergedData['projectType']
#         mergedData = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
#         masterunitRatedf['projectId'] = mergedData['projectId']
#         mergedData = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
#         masterunitRatedf['subProject'] = mergedData['subProject']
#         masterunitRatedf['projectType'] = mergedData['projectType']
#         pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
#         if 'customer' in masterunitRatedf.columns:
#             masterunitRatedf.drop(['projectuniqueId', 'SubProjectId','customer'], axis=1, inplace=True)
#         else:
#             masterunitRatedf.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
#         df_macro = pivotedData[pivotedData['projectType'] == 'MACRO']
#         if "BAND" in df_macro:
#             del df_macro['BAND']
#         # df_ibs_operation = pivotedData[pivotedData['projectType'].isin(['IBS', 'OPERATION'])]
#         df_dismantle_relocation = pivotedData[(pivotedData['projectType'] == 'RELOCATION') | (pivotedData['subProject'] == 'RELOCATION DISMANTLE')]
#         if "ACTIVITY" in df_dismantle_relocation:
#             del df_dismantle_relocation['ACTIVITY']
#         # df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO', 'IBS', 'OPERATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
#         df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO','RELOCATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
#         if 'BAND' in df_other.columns:
#             del df_other['BAND']
#         if 'ACTIVITY' in df_other.columns:
#             del df_other['ACTIVITY']
#         df_macro_merged = pd.merge(df_macro, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'ACTIVITY'], how='left')
#         # print('###########1',df_dismantle_relocation['customer'],df_dismantle_relocation.columns)
#         df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
#         # print(df_dismantle_relocation_merged.columns,'mergedData8')
#         # df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
#         df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
#         final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
#         def calculate_amounts(row):
#             amount1 = row['rate'] * 0.65 if pd.notna(row['MS1']) else 0
#             amount2 = row['rate'] * 0.35 if pd.notna(row['MS2']) else 0
#             return pd.Series([amount1, amount2])
#         final_df[['amount1', 'amount2']] = final_df.apply(calculate_amounts, axis=1)
#         final_df['final_amount'] = final_df['amount1'] + final_df['amount2']
#         final_df['uniqueId'] = "1"
        
#         columnsToDelete = [
#             'subProject', 'projectType', 'itemCode01', 'itemCode02', 'itemCode03','Site Id', 'systemId', 'siteBillingStatus', 'siteId', 'siteuid',
#             'itemCode04', 'itemCode05', 'itemCode06', 'itemCode07', 'rate', 'BAND',
#             'Item Code-08', 'Item Code-09','uniqueId', 'ACTIVITY','projectGroup','projectId']
        
#         final_df = final_df.drop(columns=[col for col in columnsToDelete if col in final_df.columns])
#         final_df['MS1'] = pd.to_datetime(final_df['MS1'])
#         final_df['MS2'] = pd.to_datetime(final_df['MS2'])
#         final_df[['MS1CustomMonth', 'MS1CustomYear']] = final_df['MS1'].apply(lambda x: pd.Series(custom_month_year(x)))
#         final_df[['MS2CustomMonth', 'MS2CustomYear']] = final_df['MS2'].apply(lambda x: pd.Series(custom_month_year(x)))
#         final_df['amount1'] = final_df['amount1'].fillna(0)
#         final_df['amount2'] = final_df['amount2'].fillna(0)
#         dfMS1 = final_df[["MS1CustomMonth", "MS1CustomYear", "projectIdNew",'costCenter','customer','amount1']].copy()
#         dfMS2 = final_df[["MS2CustomMonth", "MS2CustomYear", "projectIdNew",'costCenter','customer','amount2']].copy()
#         dfMS1['amount_1'] = dfMS1.groupby(['MS1CustomMonth', 'MS1CustomYear', 'projectIdNew','costCenter','customer'])['amount1'].transform('sum')
#         dfMS2['amount_2'] = dfMS2.groupby(['MS2CustomMonth', 'MS2CustomYear', 'projectIdNew','costCenter','customer'])['amount2'].transform('sum')
#         dfMS1 = dfMS1.drop_duplicates(subset=['projectIdNew', 'MS1CustomMonth', 'MS1CustomYear','costCenter','customer'], keep='first')
#         dfMS2 = dfMS2.drop_duplicates(subset=['projectIdNew', 'MS2CustomMonth', 'MS2CustomYear','costCenter','customer'], keep='first')
#         # Assigning Month and Year
#         dfMS1.loc[:, "Month"] = dfMS1["MS1CustomMonth"]
#         dfMS1.loc[:, "Year"] = dfMS1["MS1CustomYear"]
#         dfMS2.loc[:, "Month"] = dfMS2["MS2CustomMonth"]
#         dfMS2.loc[:, "Year"] = dfMS2["MS2CustomYear"]
#         columnsToDelete2 = ['projectIdNew', 'MS1CustomMonth', 'MS2CustomMonth', 'MS1CustomYear','MS2CustomYear','amount1', 'amount2','final_amount','amount_2']
#         columnsToDelete3 =  ['projectIdNew', 'MS1CustomMonth', 'MS2CustomMonth', 'MS1CustomYear','MS2CustomYear','amount1', 'amount2','final_amount','amount_1']
#         dfMS1 = dfMS1.drop(columns=[col for col in columnsToDelete2 if col in dfMS1.columns])
#         dfMS2 = dfMS2.drop(columns=[col for col in columnsToDelete3 if col in dfMS2.columns])
#         dfMS1 = dfMS1.groupby(["Month", "Year", "customer", "costCenter"], as_index=False)["amount_1"].sum()
#         dfMS2 = dfMS2.groupby(["Month", "Year", "customer", "costCenter"], as_index=False)["amount_2"].sum()
#         revenue_merged_df = pd.merge(dfMS1, dfMS2, on=['Month','Year','customer','costCenter'], how='outer')
#         revenue_merged_df['total_Amount'] = revenue_merged_df[['amount_1', 'amount_2']].fillna(0).sum(axis=1)
#         return revenue_merged_df
#         # json_Data = json.loads(final_df.to_json(orient='records'))
#         # print('json_Datajson_Datajson_Datajson_Data',json_Data)
#         # return respond({
#         #     'status':200,
#         #     "msg":'Data Get Successfully',
#         #     "data":json_Data
#         # }) 
#     else:
#         pivotedData = pd.DataFrame()
#         return pivotedData




def toGetExpensesAccordingtoproject():
    arr2=[
        {
            '$match': {
                'customisedStatus': 4
            }
        }, {
            '$project': {
                'projectId': 1, 
                'actionAt': 1, 
                'customisedStatus': 1, 
                'ApprovedAmount': 1
            }
        }, {
            '$addFields': {
                'actionAt': {
                    '$toDate': '$actionAt'
                }
            }
        }, 
        {
            '$addFields': {
                'actionAt2': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$customisedStatus', 6
                            ]
                        }, 
                        'then': '$actionAt', 
                        'else': None
                    }
                }
            }
        },
        {
            '$addFields': {
                'actionAtKolkata': {
                    '$dateAdd': {
                        'startDate': '$actionAt2', 
                        'unit': 'minute', 
                        'amount': 330
                    }
                }, 
                'projectId': {
                    '$toString': '$projectId'
                }, 
                'dayOfMonth': {
                    '$dayOfMonth': {
                        'date': {
                            '$dateAdd': {
                                'startDate': '$actionAt2', 
                                'unit': 'minute', 
                                'amount': 330
                            }
                        }
                    }
                }
            }
        },
        {
            '$addFields': {
                'month': {
                    '$switch': {
                        'branches': [
                            {
                                'case': {
                                    '$lt': [
                                        '$dayOfMonth', 26
                                    ]
                                }, 
                                'then': {
                                    '$switch': {
                                        'branches': [
                                            {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 1
                                                    ]
                                                }, 
                                                'then': 1
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 2
                                                    ]
                                                }, 
                                                'then': 2
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 3
                                                    ]
                                                }, 
                                                'then': 3
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 4
                                                    ]
                                                }, 
                                                'then': 4
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 5
                                                    ]
                                                }, 
                                                'then': 5
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 6
                                                    ]
                                                }, 
                                                'then': 6
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 7
                                                    ]
                                                }, 
                                                'then': 7
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 8
                                                    ]
                                                }, 
                                                'then': 8
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 9
                                                    ]
                                                }, 
                                                'then': 9
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 10
                                                    ]
                                                }, 
                                                'then': 10
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 11
                                                    ]
                                                }, 
                                                'then': 11
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 12
                                                    ]
                                                }, 
                                                'then': 12
                                            }
                                        ], 
                                        'default': None
                                    }
                                }
                            }, {
                                'case': {
                                    '$gte': [
                                        '$dayOfMonth', 25
                                    ]
                                }, 
                                'then': {
                                    '$switch': {
                                        'branches': [
                                            {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 1
                                                    ]
                                                }, 
                                                'then': 2
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 2
                                                    ]
                                                }, 
                                                'then': 3
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 3
                                                    ]
                                                }, 
                                                'then': 4
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 4
                                                    ]
                                                }, 
                                                'then': 5
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 5
                                                    ]
                                                }, 
                                                'then': 6
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 6
                                                    ]
                                                }, 
                                                'then': 7
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 7
                                                    ]
                                                }, 
                                                'then': 8
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 8
                                                    ]
                                                }, 
                                                'then': 9
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 9
                                                    ]
                                                }, 
                                                'then': 10
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 10
                                                    ]
                                                }, 
                                                'then': 11
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 11
                                                    ]
                                                }, 
                                                'then': 12
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$month': '$actionAtKolkata'
                                                        }, 12
                                                    ]
                                                }, 
                                                'then': 1
                                            }
                                        ], 
                                        'default': None
                                    }
                                }
                            }
                        ], 
                        'default': None
                    }
                }
            }
        },
        {
            '$addFields': {
                'year': {
                    '$cond': {
                        'if': {
                            '$gt': [
                                {
                                    '$dateToString': {
                                        'format': '%m-%d', 
                                        'date': '$actionAt'
                                    }
                                }, '12-25'
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
            '$project': {
                'Year': '$year', 
                'Month': '$month', 
                'projectId': 1, 
                'ApprovedAmount': 1, 
                '_id': 0
            }
        }
    ]
    
    
    Response2=cmo.finding_aggregate("Expenses",arr2)
    if len(Response2['data']):
        expenseDf = pd.DataFrame(Response2['data'])
        expenseDf = expenseDf.groupby(['Year', 'Month', 'projectId'], as_index=False)['ApprovedAmount'].sum()
        # expenseDf=expenseDf[expenseDf['projectId'].isin(projectDf['projectId'])]
        arr34=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        'status':'Active'
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
                                    'from': 'costCenter', 
                                    'localField': 'costCenterId', 
                                    'foreignField': '_id', 
                                    'as': 'costCenterResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerId', 
                                    'foreignField': '_id', 
                                    'as': 'customerResults'
                                }
                            }, {
                                '$project': {
                                    'costCenter': {
                                        '$arrayElemAt': [
                                            '$costCenterResults.costCenter', 0
                                        ]
                                    }, 
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$customerResults.customerName', 0
                                        ]
                                    }, 
                                    '_id': 0
                                }
                            }
                        ], 
                        'as': 'projectGroupResults'
                    }
                }, {
                    '$project': {
                        'projectId': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0, 
                        'costCenter': {
                            '$arrayElemAt': [
                                '$projectGroupResults.costCenter', 0
                            ]
                        }, 
                        'customer': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$projectGroupResults.customer', 0
                                ]
                            }
                        }
                    }
                }
            ]
        ResponseProject=cmo.finding_aggregate("project",arr34)
        projectGroup=pd.DataFrame(ResponseProject['data'])
        expenseDf = pd.merge(expenseDf, projectGroup, on='projectId', how='inner')
        expenseDf = expenseDf.drop('projectId', axis=1)
        expenseDf = expenseDf.groupby(['Year', 'Month', 'costCenter', 'customer'], as_index=False).agg({'ApprovedAmount': 'sum'})
        expensedf = expenseDf.groupby(['Year', 'Month', 'costCenter', 'customer'], as_index=False).agg({'ApprovedAmount': 'sum'})
        return expenseDf
    else:
        expenseDf=pd.DataFrame([])
        return expenseDf

def TestingFunc():
    count=0
    arr=[
        "67ee7cfdf9b0a1e1d6b9b136",
        "67ee7cfdf9b0a1e1d6b9b149",
        "67ee7cfdf9b0a1e1d6b9b15c",
        "67ee7cfdf9b0a1e1d6b9b16f",
        "67ee7cfdf9b0a1e1d6b9b182",
        "67ee7cfdf9b0a1e1d6b9b195",
        "67ee7cfdf9b0a1e1d6b9b1a8",
        "67ee7cfdf9b0a1e1d6b9b1bb",
        "67ee7cfdf9b0a1e1d6b9b1ce",
        "67ee7cfdf9b0a1e1d6b9b1e1",
        "67ee7cfef9b0a1e1d6b9b1f4",
        "67ee7cfef9b0a1e1d6b9b207",
        "67ee7cfef9b0a1e1d6b9b21a",
        "67ee7cfef9b0a1e1d6b9b22d",
        "67ee7cfef9b0a1e1d6b9b240",
        "67ee7cfef9b0a1e1d6b9b253",
        "67ee7cfef9b0a1e1d6b9b266",
        "67ee7cfef9b0a1e1d6b9b279",
        "67ee7cfef9b0a1e1d6b9b28c",
        "67ee7cfef9b0a1e1d6b9b29f",
        "67ee7cfef9b0a1e1d6b9b2b2",
        "67ee7cfef9b0a1e1d6b9b2c5",
        "67ee7cfef9b0a1e1d6b9b2d8",
        "67ee7cfef9b0a1e1d6b9b2eb",
        "67ee7cfef9b0a1e1d6b9b2fe",
        "67ee7cfef9b0a1e1d6b9b311",
        "67ee7cfef9b0a1e1d6b9b324",
        "67ee7cfef9b0a1e1d6b9b337",
        "67ee7cfef9b0a1e1d6b9b34a",
        "67ee7cfef9b0a1e1d6b9b35d",
        "67ee7cfef9b0a1e1d6b9b370",
        "67ee7cfef9b0a1e1d6b9b383",
        "67ee7cfef9b0a1e1d6b9b396",
        "67ee7cfef9b0a1e1d6b9b3a9",
        "67ee7cfef9b0a1e1d6b9b3bc",
        "67ee7cfef9b0a1e1d6b9b3cf",
        "67ee7cfef9b0a1e1d6b9b3e2",
        "67ee7cfef9b0a1e1d6b9b3f5",
        "67ee7cfef9b0a1e1d6b9b408",
        "67ee7cfef9b0a1e1d6b9b41b",
        "67ee7cfef9b0a1e1d6b9b42e",
        "67ee7cfef9b0a1e1d6b9b441",
        "67ee7cfef9b0a1e1d6b9b454",
        "67ee7cfef9b0a1e1d6b9b467",
        "67ee7cfef9b0a1e1d6b9b47a",
        "67ee7cfef9b0a1e1d6b9b48d",
        "67ee7cfef9b0a1e1d6b9b4a0",
        "67ee7cfef9b0a1e1d6b9b4b3",
        "67ee7cfef9b0a1e1d6b9b4c6",
        "67ee7cfef9b0a1e1d6b9b4d9",
        "67ee7cfef9b0a1e1d6b9b4ec",
        "67ee7cfef9b0a1e1d6b9b4ff",
        "67ee7cfef9b0a1e1d6b9b512",
        "67ee7cfef9b0a1e1d6b9b525",
        "67ee7cfef9b0a1e1d6b9b538",
        "67ee7cfef9b0a1e1d6b9b54b",
        "67ee7cfef9b0a1e1d6b9b55e",
        "67ee7cfef9b0a1e1d6b9b571",
        "67ee7cfef9b0a1e1d6b9b584",
        "67ee7cfef9b0a1e1d6b9b597",
        "67ee7cfef9b0a1e1d6b9b5aa",
        "67ee7cfef9b0a1e1d6b9b5bd",
        "67ee7cfef9b0a1e1d6b9b5d0",
        "67ee7cfef9b0a1e1d6b9b5e3",
        "67ee7cfef9b0a1e1d6b9b5f6",
        "67ee7cfef9b0a1e1d6b9b609",
        "67ee7cfef9b0a1e1d6b9b61c",
        "67ee7cfef9b0a1e1d6b9b62f",
        "67ee7cfef9b0a1e1d6b9b642",
        "67ee7cfef9b0a1e1d6b9b655",
        "67ee7cfef9b0a1e1d6b9b668",
        "67ee7cfef9b0a1e1d6b9b67b",
        "67ee7cfef9b0a1e1d6b9b68e",
        "67ee7cfef9b0a1e1d6b9b6a1",
        "67ee7cfef9b0a1e1d6b9b6b4",
        "67ee7cfef9b0a1e1d6b9b6c7",
        "67ee7cfef9b0a1e1d6b9b6da",
        "67ee7cfef9b0a1e1d6b9b6ed",
        "67ee7cfef9b0a1e1d6b9b700",
        "67ee7cfef9b0a1e1d6b9b713",
        "67ee7cfef9b0a1e1d6b9b726",
        "67ee7cfef9b0a1e1d6b9b739",
        "67ee7cfef9b0a1e1d6b9b74c",
        "67ee7cfef9b0a1e1d6b9b75f",
        "67ee7cfef9b0a1e1d6b9b772",
        "67ee7cfef9b0a1e1d6b9b785",
        "67ee7cfef9b0a1e1d6b9b798",
        "67ee7cfef9b0a1e1d6b9b7ab",
        "67ee7cfef9b0a1e1d6b9b7be",
        "67ee7cfef9b0a1e1d6b9b7d1",
        "67ee7cfef9b0a1e1d6b9b7e4",
        "67ee7cfef9b0a1e1d6b9b7f7",
        "67ee7cfef9b0a1e1d6b9b80a",
        "67ee7cfef9b0a1e1d6b9b81d",
        "67ee7cfef9b0a1e1d6b9b830",
        "67ee7cfef9b0a1e1d6b9b843",
        "67ee7cfef9b0a1e1d6b9b856",
        "67ee7cfef9b0a1e1d6b9b869",
        "67ee7cfef9b0a1e1d6b9b87c",
        "67ee7cfef9b0a1e1d6b9b88f",
        "67ee7cfef9b0a1e1d6b9b8a2",
        "67ee7cfef9b0a1e1d6b9b8b5",
        "67ee7cfef9b0a1e1d6b9b8c8",
        "67ee7cfef9b0a1e1d6b9b8db",
        "67ee7cfef9b0a1e1d6b9b8ee",
        "67ee7cfef9b0a1e1d6b9b901",
        "67ee7cfff9b0a1e1d6b9b914",
        "67ee7cfff9b0a1e1d6b9b927",
        "67ee7cfff9b0a1e1d6b9b93a",
        "67ee7cfff9b0a1e1d6b9b94d",
        "67ee7cfff9b0a1e1d6b9b960",
        "67ee7cfff9b0a1e1d6b9b973",
        "67ee7cfff9b0a1e1d6b9b986",
        "67ee7cfff9b0a1e1d6b9b999",
        "67ee7cfff9b0a1e1d6b9b9ac",
        "67ee7cfff9b0a1e1d6b9b9bf",
        "67ee7cfff9b0a1e1d6b9b9d2",
        "67ee7cfff9b0a1e1d6b9b9e5",
        "67ee7cfff9b0a1e1d6b9b9f8",
        "67ee7cfff9b0a1e1d6b9ba0b",
        "67ee7cfff9b0a1e1d6b9ba1e",
        "67ee7cfff9b0a1e1d6b9ba31",
        "67ee7cfff9b0a1e1d6b9ba44",
        "67ee7cfff9b0a1e1d6b9ba57",
        "67ee7cfff9b0a1e1d6b9ba6a",
        "67ee7cfff9b0a1e1d6b9ba7d",
        "67ee7cfff9b0a1e1d6b9ba90",
        "67ee7cfff9b0a1e1d6b9baa3",
        "67ee7cfff9b0a1e1d6b9bab6",
        "67ee7cfff9b0a1e1d6b9bac9",
        "67ee7cfff9b0a1e1d6b9badc",
        "67ee7cfff9b0a1e1d6b9baef",
        "67ee7cfff9b0a1e1d6b9bb02",
        "67ee7cfff9b0a1e1d6b9bb15",
        "67ee7cfff9b0a1e1d6b9bb28",
        "67ee7cfff9b0a1e1d6b9bb3b",
        "67ee7cfff9b0a1e1d6b9bb4e",
        "67ee7cfff9b0a1e1d6b9bb61",
        "67ee7cfff9b0a1e1d6b9bb74",
        "67ee7cfff9b0a1e1d6b9bb87",
        "67ee7cfff9b0a1e1d6b9bb9a",
        "67ee7cfff9b0a1e1d6b9bbad",
        "67ee7cfff9b0a1e1d6b9bbc0",
        "67ee7cfff9b0a1e1d6b9bbd3",
        "67ee7cfff9b0a1e1d6b9bbe6",
        "67ee7cfff9b0a1e1d6b9bbf9",
        "67ee7cfff9b0a1e1d6b9bc0c",
        "67ee7cfff9b0a1e1d6b9bc1f",
        "67ee7cfff9b0a1e1d6b9bc32",
        "67ee7cfff9b0a1e1d6b9bc45",
        "67ee7cfff9b0a1e1d6b9bc58",
        "67ee7cfff9b0a1e1d6b9bc6b"
    ]
    for i in arr:
        cmo.updating('SiteEngineer',{'_id':ObjectId(i)},{'deleteStatus':1,'deletedBy':'SandeepWrongdata03-04-2025'},False)
        cmo.updating_m('milestone',{'siteId':ObjectId(i)},{'deleteStatus':1,'deletedBy':'SandeepWrongdata03-04-2025'},False)
        count=count+1
    print('deletedSite',count)

# TestingFunc()
# 
# toGetWorkDoneData()


def toGetWorkDoneData():
    arra = [
        {
            '$match':{
                'deleteStatus': {'$ne': 1},
                # 'projectuniqueId':'67c2b762f4b5d73f9bf7db62'
                
            }
        }
    ]
    arra = arra +[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': '_id', 
                    'foreignField': 'siteId', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'Name': {
                                    '$in': [
                                        'MS1', 'MS2'
                                    ]
                                }, 
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
                    'MS2': '$MS2.CC_Completion Date'
                }
            }, {
                '$project': {
                    'projectuniqueId': 1, 
                    'SubProjectId': 1, 
                    'siteBillingStatus': 1, 
                    'BAND': {
                        '$ifNull': [
                            '$BAND', ''
                        ]
                    }, 
                    'ACTIVITY': {
                        '$ifNull': [
                            '$ACTIVITY', ''
                        ]
                    }, 
                    'siteId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0, 
                    'Site Id': 1, 
                    'MS1': 1, 
                    'MS2': 1, 
                    'systemId': 1
                }
            }
        ]
    response = cmo.finding_aggregate("SiteEngineer",arra)
    if len(response['data']):
        pivotedData = pd.DataFrame(response['data'])
        pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
        projectGroupdf = projectGroup()
        projectIddf = projectid()
        subProjectdf = subproject()
        masterunitRatedf = masterunitRate()
        mergedData = projectIddf.merge(projectGroupdf,on='projectGroup',how='left')
        projectIddf['projectGroup'] = mergedData['projectGroupName']
        projectIddf['customer'] = mergedData['Customer']
        # print(mergedData.columns,'mergedData1')
        projectIddf['costCenter'] = mergedData['costCenter']
        # print(projectIddf.columns,'mergedData2')
        mergedData = pivotedData.merge(projectIddf,on="projectuniqueId",how="left")
        
        pivotedData['projectId'] = mergedData['projectId']
        pivotedData['projectGroup'] = mergedData['projectGroup']
        pivotedData['customer'] = mergedData['customer']
        pivotedData['costCenter'] = mergedData['costCenter']
        mergedData = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
        pivotedData['subProject'] = mergedData['subProject']
        pivotedData['projectType'] = mergedData['projectType']
        mergedData = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
        masterunitRatedf['projectId'] = mergedData['projectId']
        mergedData = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
        masterunitRatedf['subProject'] = mergedData['subProject']
        masterunitRatedf['projectType'] = mergedData['projectType']
        pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        if 'customer' in masterunitRatedf.columns:
            masterunitRatedf.drop(['projectuniqueId', 'SubProjectId','customer'], axis=1, inplace=True)
        else:
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
        # print('###########1',df_dismantle_relocation['customer'],df_dismantle_relocation.columns)
        df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        # print(df_dismantle_relocation_merged.columns,'mergedData8')
        # df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
        final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
        def calculate_amounts(row):
            amount1 = row['rate'] * 0.65 if pd.notna(row['MS1']) else 0
            amount2 = row['rate'] * 0.35 if pd.notna(row['MS2']) else 0
            return pd.Series([amount1, amount2])
        final_df[['amount1', 'amount2']] = final_df.apply(calculate_amounts, axis=1)
        final_df['final_amount'] = final_df['amount1'] + final_df['amount2']
        final_df['uniqueId'] = "1"
        
        columnsToDelete = [
            'subProject', 'projectType', 'itemCode01', 'itemCode02', 'itemCode03','Site Id', 'systemId', 'siteBillingStatus', 'siteId', 'siteuid',
            'itemCode04', 'itemCode05', 'itemCode06', 'itemCode07', 'rate', 'BAND',
            'Item Code-08', 'Item Code-09','uniqueId', 'ACTIVITY','projectGroup','projectId']
        
        final_df = final_df.drop(columns=[col for col in columnsToDelete if col in final_df.columns])
        for col in ["MS1","MS2"]:
            final_df[col] = final_df[col].apply(convertToDateBulkExport)
        final_df['MS1'] = pd.to_datetime(final_df['MS1'])
        final_df['MS2'] = pd.to_datetime(final_df['MS2'])
        final_df[['MS1CustomMonth', 'MS1CustomYear']] = final_df['MS1'].apply(lambda x: pd.Series(custom_month_year(x)))
        final_df[['MS2CustomMonth', 'MS2CustomYear']] = final_df['MS2'].apply(lambda x: pd.Series(custom_month_year(x)))
        # fullPath = excelWriteFunc.excelFileWriter(final_df, "Export_PO_WorkDone", "PO_WorkDone")
        # return send_file(fullPath)
        final_df['amount1'] = final_df['amount1'].fillna(0)
        final_df['amount2'] = final_df['amount2'].fillna(0)
        dfMS1 = final_df[["MS1CustomMonth", "MS1CustomYear",'costCenter','customer','amount1']].copy()
        dfMS2 = final_df[["MS2CustomMonth", "MS2CustomYear",'costCenter','customer','amount2']].copy()
        dfMS1['amount_1'] = dfMS1.groupby(['MS1CustomMonth', 'MS1CustomYear', 'costCenter','customer'])['amount1'].transform('sum')
        dfMS2['amount_2'] = dfMS2.groupby(['MS2CustomMonth', 'MS2CustomYear', 'costCenter','customer'])['amount2'].transform('sum')
        dfMS1 = dfMS1.drop_duplicates(subset=[ 'MS1CustomMonth', 'MS1CustomYear','costCenter','customer'], keep='first')
        dfMS2 = dfMS2.drop_duplicates(subset=[ 'MS2CustomMonth', 'MS2CustomYear','costCenter','customer'], keep='first')
        # Assigning Month and Year
        dfMS1.loc[:, "Month"] = dfMS1["MS1CustomMonth"]
        dfMS1.loc[:, "Year"] = dfMS1["MS1CustomYear"]
        dfMS2.loc[:, "Month"] = dfMS2["MS2CustomMonth"]
        dfMS2.loc[:, "Year"] = dfMS2["MS2CustomYear"]
        columnsToDelete2 = ['projectIdNew', 'MS1CustomMonth', 'MS2CustomMonth', 'MS1CustomYear','MS2CustomYear','amount1', 'amount2','final_amount','amount_2']
        columnsToDelete3 =  ['projectIdNew', 'MS1CustomMonth', 'MS2CustomMonth', 'MS1CustomYear','MS2CustomYear','amount1', 'amount2','final_amount','amount_1']
        dfMS1 = dfMS1.drop(columns=[col for col in columnsToDelete2 if col in dfMS1.columns])
        dfMS2 = dfMS2.drop(columns=[col for col in columnsToDelete3 if col in dfMS2.columns])
        dfMS1 = dfMS1.groupby(["Month", "Year", "customer", "costCenter"], as_index=False)["amount_1"].sum()
        dfMS2 = dfMS2.groupby(["Month", "Year", "customer", "costCenter"], as_index=False)["amount_2"].sum()
        revenue_merged_df = pd.merge(dfMS1, dfMS2, on=['Month','Year','customer','costCenter'], how='outer')
        revenue_merged_df['total_Amount'] = revenue_merged_df[['amount_1', 'amount_2']].fillna(0).sum(axis=1)
        return revenue_merged_df
        # json_Data = json.loads(final_df.to_json(orient='records'))
        # print('json_Datajson_Datajson_Datajson_Data',json_Data)
        # return respond({
        #     'status':200,
        #     "msg":'Data Get Successfully',
        #     "data":json_Data
        # }) 
    else:
        pivotedData = pd.DataFrame()
        return pivotedData


# testinfFunc2()

# def get_gpTracking():
#     arty=[
#         {
#             '$match': {
#                 'typeAssigned': 'Partner', 
#                 'mileStoneStatus': 'Closed'
#             }
#         }, {
#             '$addFields': {
#                 '_id': {
#                     '$toString': '$_id'
#                 }, 
#                 'uniqueId': {
#                     '$toString': '$_id'
#                 }, 
#                 'mileStoneStartDate1': {
#                     '$toDate': '$mileStoneStartDate'
#                 }, 
#                 'mileStoneEndtDate1': {
#                     '$toDate': '$mileStoneEndDate'
#                 }, 
#                 'CC_Completion Date1': {
#                     '$toDate': '$CC_Completion Date'
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'CC_Completion Date': {
#                     '$cond': {
#                         'if': {
#                             '$eq': [
#                                 {
#                                     '$type': '$CC_Completion Date1'
#                                 }, 'date'
#                             ]
#                         }, 
#                         'then': {
#                             '$dateToString': {
#                                 'date': '$CC_Completion Date1', 
#                                 'format': '%d-%m-%Y', 
#                                 'timezone': 'Asia/Kolkata'
#                             }
#                         }, 
#                         'else': ''
#                     }
#                 }
#             }
#         }, {
#             '$lookup': {
#                 'from': 'userRegister', 
#                 'localField': 'assignerId', 
#                 'foreignField': '_id', 
#                 'pipeline': [
#                     {
#                         '$match': {
#                             'deleteStatus': {
#                                 '$ne': 1
#                             }, 
#                             'type': 'Partner'
#                         }
#                     }, {
#                         '$project': {
#                             '_id': 0, 
#                             'assignerName': '$vendorName', 
#                             'vendorCode': {
#                                 '$toString': '$vendorCode'
#                             }, 
#                             'assignerId': {
#                                 '$toString': '$_id'
#                             }
#                         }
#                     }
#                 ], 
#                 'as': 'assignerResult'
#             }
#         }, {
#             '$project': {
#                 'assignerId': 0
#             }
#         }, {
#             '$group': {
#                 '_id': '$siteId', 
#                 'milestoneArray': {
#                     '$addToSet': '$$ROOT'
#                 }, 
#                 'siteId': {
#                     '$first': '$siteId'
#                 }
#             }
#         }, {
#             '$lookup': {
#                 'from': 'SiteEngineer', 
#                 'localField': 'siteId', 
#                 'foreignField': '_id', 
#                 'pipeline': [
#                     {
#                         '$match': {
#                             'deleteStatus': {
#                                 '$ne': 1
#                             }
#                         }
#                     }, {
#                         '$addFields': {
#                             'SubProjectId': {
#                                 '$toObjectId': '$SubProjectId'
#                             }
#                         }
#                     }, {
#                         '$lookup': {
#                             'from': 'projectType', 
#                             'localField': 'SubProjectId', 
#                             'foreignField': '_id', 
#                             'pipeline': [
#                                 {
#                                     '$match': {
#                                         'deleteStatus': {
#                                             '$ne': 1
#                                         }
#                                     }
#                                 }
#                             ], 
#                             'as': 'SubTypeResult'
#                         }
#                     }, {
#                         '$unwind': {
#                             'path': '$SubTypeResult', 
#                             'preserveNullAndEmptyArrays': True
#                         }
#                     }, {
#                         '$addFields': {
#                             'SubProject': '$SubTypeResult.subProject', 
#                             'projectuniqueId': {
#                                 '$toObjectId': '$projectuniqueId'
#                             }, 
#                             'SubProjectId': {
#                                 '$toString': '$SubProjectId'
#                             }, 
#                             'projectType': '$SubTypeResult.projectType'
#                         }
#                     }, {
#                         '$lookup': {
#                             'from': 'project', 
#                             'localField': 'projectuniqueId', 
#                             'foreignField': '_id', 
#                             'pipeline': [
#                                 {
#                                     '$match': {
#                                         'deleteStatus': {
#                                             '$ne': 1
#                                         }
#                                     }
#                                 }, {
#                                     '$addFields': {
#                                         'PMId': {
#                                             '$toObjectId': '$PMId'
#                                         }
#                                     }
#                                 }, {
#                                     '$addFields': {
#                                         'customerId': {
#                                             '$toObjectId': '$custId'
#                                         }
#                                     }
#                                 }, {
#                                     '$lookup': {
#                                         'from': 'projectGroup', 
#                                         'localField': 'projectGroup', 
#                                         'foreignField': '_id', 
#                                         'pipeline': [
#                                             {
#                                                 '$match': {
#                                                     'deleteStatus': {
#                                                         '$ne': 1
#                                                     }
#                                                 }
#                                             }, {
#                                                 '$lookup': {
#                                                     'from': 'customer', 
#                                                     'localField': 'customerId', 
#                                                     'foreignField': '_id', 
#                                                     'pipeline': [
#                                                         {
#                                                             '$match': {
#                                                                 'deleteStatus': {
#                                                                     '$ne': 1
#                                                                 }
#                                                             }
#                                                         }
#                                                     ], 
#                                                     'as': 'customer'
#                                                 }
#                                             }, {
#                                                 '$unwind': {
#                                                     'path': '$customer', 
#                                                     'preserveNullAndEmptyArrays': True
#                                                 }
#                                             }, {
#                                                 '$lookup': {
#                                                     'from': 'zone', 
#                                                     'localField': 'zoneId', 
#                                                     'foreignField': '_id', 
#                                                     'pipeline': [
#                                                         {
#                                                             '$match': {
#                                                                 'deleteStatus': {
#                                                                     '$ne': 1
#                                                                 }
#                                                             }
#                                                         }
#                                                     ], 
#                                                     'as': 'zone'
#                                                 }
#                                             }, {
#                                                 '$unwind': {
#                                                     'path': '$zone', 
#                                                     'preserveNullAndEmptyArrays': True
#                                                 }
#                                             }, {
#                                                 '$lookup': {
#                                                     'from': 'costCenter', 
#                                                     'localField': 'costCenterId', 
#                                                     'foreignField': '_id', 
#                                                     'pipeline': [
#                                                         {
#                                                             '$match': {
#                                                                 'deleteStatus': {
#                                                                     '$ne': 1
#                                                                 }
#                                                             }
#                                                         }
#                                                     ], 
#                                                     'as': 'costCenter'
#                                                 }
#                                             }, {
#                                                 '$unwind': {
#                                                     'path': '$costCenter', 
#                                                     'preserveNullAndEmptyArrays': True
#                                                 }
#                                             }, {
#                                                 '$addFields': {
#                                                     'costCenter': '$costCenter.costCenter', 
#                                                     'zone': '$zone.shortCode', 
#                                                     'customer': '$customer.shortName', 
#                                                     'Customer': '$customer.customerName'
#                                                 }
#                                             }, {
#                                                 '$addFields': {
#                                                     'projectGroupName': {
#                                                         '$concat': [
#                                                             '$customer', '-', '$zone', '-', '$costCenter'
#                                                         ]
#                                                     }, 
#                                                     'projectGroup': {
#                                                         '$toString': '$_id'
#                                                     }
#                                                 }
#                                             }, {
#                                                 '$project': {
#                                                     '_id': 0, 
#                                                     'projectGroupName': 1, 
#                                                     'projectGroup': 1, 
#                                                     'Customer': 1, 
#                                                     'costCenter': 1
#                                                 }
#                                             }
#                                         ], 
#                                         'as': 'projectGroupResult'
#                                     }
#                                 }, {
#                                     '$lookup': {
#                                         'from': 'userRegister', 
#                                         'localField': 'PMId', 
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
#                                         'as': 'PMarray'
#                                     }
#                                 }, {
#                                     '$unwind': {
#                                         'path': '$PMarray', 
#                                         'preserveNullAndEmptyArrays': True
#                                     }
#                                 }, {
#                                     '$addFields': {
#                                         'PMName': '$PMarray.empName', 
#                                         'projectGroupName': {
#                                             '$arrayElemAt': [
#                                                 '$projectGroupResult.projectGroupName', 0
#                                             ]
#                                         }, 
#                                         'Customer': {
#                                             '$arrayElemAt': [
#                                                 '$projectGroupResult.Customer', 0
#                                             ]
#                                         }
#                                     }
#                                 }, {
#                                     '$project': {
#                                         'PMName': 1, 
#                                         'projectId': 1, 
#                                         '_id': 0, 
#                                         'Customer': 1, 
#                                         'projectGroupName': 1, 
#                                         'projectGroupId': '$projectGroup', 
#                                         'customerId': 1
#                                     }
#                                 }
#                             ], 
#                             'as': 'projectArray'
#                         }
#                     }, {
#                         '$unwind': {
#                             'path': '$projectArray', 
#                             'preserveNullAndEmptyArrays': True
#                         }
#                     }
#                 ], 
#                 'as': 'siteResult'
#             }
#         }, {
#             '$unwind': '$siteResult'
#         }, {
#             '$addFields': {
#                 'siteResult.milestoneArray': '$milestoneArray'
#             }
#         }, {
#             '$replaceRoot': {
#                 'newRoot': '$siteResult'
#             }
#         }, {
#             '$addFields': {
#                 'siteStartDate': {
#                     '$toDate': '$siteStartDate'
#                 }, 
#                 'siteEndDate': {
#                     '$toDate': '$siteStartDate'
#                 }, 
#                 'Site_Completion Date': {
#                     '$toDate': '$siteStartDate'
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'uniqueId': {
#                     '$toString': '$_id'
#                 }, 
#                 'projectuniqueId': {
#                     '$toString': '$projectuniqueId'
#                 }, 
#                 'siteStartDate': {
#                     '$dateToString': {
#                         'date': '$siteStartDate', 
#                         'format': '%d-%m-%Y', 
#                         'timezone': 'Asia/Kolkata'
#                     }
#                 }, 
#                 'siteEndDate': {
#                     '$dateToString': {
#                         'date': '$siteEndDate', 
#                         'format': '%d-%m-%Y', 
#                         'timezone': 'Asia/Kolkata'
#                     }
#                 }, 
#                 'Site_Completion Date': {
#                     '$dateToString': {
#                         'date': '$Site_Completion Date', 
#                         'format': '%d-%m-%Y', 
#                         'timezone': 'Asia/Kolkata'
#                     }
#                 }, 
#                 'PMName': '$projectArray.PMName', 
#                 'projectId': '$projectArray.projectId', 
#                 'subProject': '$SubProject', 
#                 'siteageing': {
#                     '$cond': {
#                         'if': {
#                             '$eq': [
#                                 {
#                                     '$type': '$Site_Completion Date'
#                                 }, 'date'
#                             ]
#                         }, 
#                         'then': {
#                             '$round': {
#                                 '$divide': [
#                                     {
#                                         '$subtract': [
#                                             '$siteEndDate', '$Site_Completion Date'
#                                         ]
#                                     }, 86400000
#                                 ]
#                             }
#                         }, 
#                         'else': ''
#                     }
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'milestoneArray': {
#                     '$sortArray': {
#                         'input': '$milestoneArray', 
#                         'sortBy': {
#                             '_id': 1
#                         }
#                     }
#                 }, 
#                 '_id': {
#                     '$toString': '$_id'
#                 }, 
#                 'projectGroupName': '$projectArray.projectGroupName', 
#                 'Customer': '$projectArray.Customer'
#             }
#         }, {
#             '$unwind': {
#                 'path': '$milestoneArray'
#             }
#         }, {
#             '$group': {
#                 '_id': {
#                     'workDescription': '$milestoneArray.workDescription', 
#                     'siteId': '$milestoneArray.siteId'
#                 }, 
#                 'items': {
#                     '$addToSet': '$$ROOT'
#                 }, 
#                 'milestones': {
#                     '$addToSet': '$$ROOT.milestoneArray.Name'
#                 }, 
#                 'data': {
#                     '$addToSet': {
#                         'assignerIdResult': {
#                             '$first': '$$ROOT.milestoneArray.assignerResult.assignerId'
#                         }, 
#                         'SubProjectId': '$$ROOT.milestoneArray.SubProjectId', 
#                         'siteId': '$$ROOT.milestoneArray.siteId', 
#                         'customerId': '$$ROOT.milestoneArray.customerId', 
#                         'projectGropupId': '$$ROOT.milestoneArray.projectGropupId'
#                     }
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'subProjectId': {
#                     '$toObjectId': {
#                         '$arrayElemAt': [
#                             '$data.SubProjectId', 0
#                         ]
#                     }
#                 }, 
#                 'POEligibility': {
#                     '$cond': {
#                         'if': {
#                             '$eq': [
#                                 {
#                                     '$size': {
#                                         '$filter': {
#                                             'input': '$items', 
#                                             'as': 'item', 
#                                             'cond': {
#                                                 '$and': [
#                                                     {
#                                                         '$eq': [
#                                                             '$$item.milestoneArray.mileStoneStatus', 'Closed'
#                                                         ]
#                                                     }, {
#                                                         '$ne': [
#                                                             '$$item.milestoneArray.workDescription', None
#                                                         ]
#                                                     }, {
#                                                         '$ne': [
#                                                             '$$item.milestoneArray.workDescription', ''
#                                                         ]
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                     }
#                                 }, {
#                                     '$size': '$items'
#                                 }
#                             ]
#                         }, 
#                         'then': 'Yes', 
#                         'else': 'No'
#                     }
#                 }, 
#                 'assignerIdResult': {
#                     '$toObjectId': {
#                         '$arrayElemAt': [
#                             '$data.assignerIdResult', 0
#                         ]
#                     }
#                 }, 
#                 'siteId': {
#                     '$toObjectId': {
#                         '$arrayElemAt': [
#                             '$data.siteId', 0
#                         ]
#                     }
#                 }, 
#                 'projectGroupId': {
#                     '$toObjectId': {
#                         '$arrayElemAt': [
#                             '$data.projectGropupId', 0
#                         ]
#                     }
#                 }, 
#                 'customerId': {
#                     '$toObjectId': {
#                         '$arrayElemAt': [
#                             '$data.customerId', 0
#                         ]
#                     }
#                 }
#             }
#         }, {
#             '$lookup': {
#                 'let': {
#                     'milestoneNames': '$milestones', 
#                     'subProjectId': '$subProjectId', 
#                     'projectGroupId': '$projectGroupId', 
#                     'customerId': '$customerId'
#                 }, 
#                 'from': 'vendorCostMilestone', 
#                 'localField': 'assignerIdResult', 
#                 'foreignField': 'vendorId', 
#                 'pipeline': [
#                     {
#                         '$match': {
#                             '$expr': {
#                                 '$and': [
#                                     {
#                                         '$setIsSubset': [
#                                             '$milestoneList', '$$milestoneNames'
#                                         ]
#                                     }, {
#                                         '$setIsSubset': [
#                                             '$$milestoneNames', '$milestoneList'
#                                         ]
#                                     }, {
#                                         '$eq': [
#                                             '$subProjectId', '$$subProjectId'
#                                         ]
#                                     }, {
#                                         '$eq': [
#                                             '$projectGroup', '$$projectGroupId'
#                                         ]
#                                     }, {
#                                         '$eq': [
#                                             '$customerId', '$$customerId'
#                                         ]
#                                     }
#                                 ]
#                             }
#                         }
#                     }
#                 ], 
#                 'as': 'itemCodeResults'
#             }
#         }, {
#             '$addFields': {
#                 'subProjectId': {
#                     '$toString': '$subProjectId'
#                 }, 
#                 'vendorItemCode': {
#                     '$arrayElemAt': [
#                         '$itemCodeResults.itemCode', {
#                             '$indexOfArray': [
#                                 '$itemCodeResults.rate', {
#                                     '$min': '$itemCodeResults.rate'
#                                 }
#                             ]
#                         }
#                     ]
#                 }, 
#                 'vendorRate': {
#                     '$min': '$itemCodeResults.rate'
#                 }, 
#                 'MS1Date': {
#                     '$let': {
#                         'vars': {
#                             'ms1': {
#                                 '$arrayElemAt': [
#                                     {
#                                         '$filter': {
#                                             'input': '$milestoneArray', 
#                                             'as': 'item', 
#                                             'cond': {
#                                                 '$eq': [
#                                                     '$$item.Name', 'MS1'
#                                                 ]
#                                             }
#                                         }
#                                     }, 0
#                                 ]
#                             }
#                         }, 
#                         'in': {
#                             '$ifNull': [
#                                 '$$ms1.CC_Completion Date', ''
#                             ]
#                         }
#                     }
#                 }, 
#                 'MS2Date': {
#                     '$let': {
#                         'vars': {
#                             'ms1': {
#                                 '$arrayElemAt': [
#                                     {
#                                         '$filter': {
#                                             'input': '$milestoneArray', 
#                                             'as': 'item', 
#                                             'cond': {
#                                                 '$eq': [
#                                                     '$$item.Name', 'MS2'
#                                                 ]
#                                             }
#                                         }
#                                     }, 0
#                                 ]
#                             }
#                         }, 
#                         'in': {
#                             '$ifNull': [
#                                 '$$ms1.CC_Completion Date', ''
#                             ]
#                         }
#                     }
#                 }
#             }
#         }, {
#             '$unwind': {
#                 'path': '$items'
#             }
#         }, {
#             '$addFields': {
#                 'items.rate': {
#                     '$cond': {
#                         'if': {
#                             '$gt': [
#                                 {
#                                     '$size': '$itemCodeResults'
#                                 }, 0
#                             ]
#                         }, 
#                         'then': {
#                             '$arrayElemAt': [
#                                 '$itemCodeResults.rate', {
#                                     '$indexOfArray': [
#                                         '$itemCodeResults.rate', {
#                                             '$min': '$itemCodeResults.rate'
#                                         }
#                                     ]
#                                 }
#                             ]
#                         }, 
#                         'else': None
#                     }
#                 }, 
#                 'items.itemCode': {
#                     '$cond': {
#                         'if': {
#                             '$gt': [
#                                 {
#                                     '$size': '$itemCodeResults'
#                                 }, 0
#                             ]
#                         }, 
#                         'then': {
#                             '$arrayElemAt': [
#                                 '$itemCodeResults.itemCode', {
#                                     '$indexOfArray': [
#                                         '$itemCodeResults.rate', {
#                                             '$min': '$itemCodeResults.rate'
#                                         }
#                                     ]
#                                 }
#                             ]
#                         }, 
#                         'else': None
#                     }
#                 }, 
#                 'items.POEligibility': '$POEligibility'
#             }
#         }, {
#             '$replaceRoot': {
#                 'newRoot': '$items'
#             }
#         }, {
#             '$addFields': {
#                 'milestoneArray.siteId': {
#                     '$toString': '$milestoneArray.siteId'
#                 }
#             }
#         }, {
#             '$group': {
#                 '_id': '$_id', 
#                 'items': {
#                     '$addToSet': {
#                         '$mergeObjects': [
#                             '$$ROOT.milestoneArray', {
#                                 'itemCode': '$$ROOT.itemCode', 
#                                 'rate': '$$ROOT.rate', 
#                                 'POEligibility': '$$ROOT.POEligibility'
#                             }
#                         ]
#                     }
#                 }, 
#                 'realData': {
#                     '$first': '$$ROOT'
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'realData.milestoneArray': '$items'
#             }
#         }, {
#             '$replaceRoot': {
#                 'newRoot': '$realData'
#             }
#         }, {
#             '$addFields': {
#                 'milestoneArray': {
#                     '$sortArray': {
#                         'input': '$milestoneArray', 
#                         'sortBy': {
#                             '_id': 1
#                         }
#                     }
#                 }
#             }
#         }, {
#             '$project': {
#                 'projectArray': 0, 
#                 'SubTypeResult': 0
#             }
#         }, {
#             '$sort': {
#                 '_id': 1
#             }
#         }, {
#             '$match': {
#                 'POEligibility': 'Yes'
#             }
#         }, {
#             '$addFields': {
#                 'rate': {
#                     '$arrayElemAt': [
#                         '$milestoneArray.rate', -1
#                     ]
#                 }, 
#                 'CC_Completion Date': {
#                     '$arrayElemAt': [
#                         '$milestoneArray.CC_Completion Date', -1
#                     ]
#                 }
#             }
#         }, {
#             '$project': {
#                 '_id': 0, 
#                 'Customer': 1, 
#                 'Project ID': '$projectuniqueId', 
#                 'Completion Date': '$CC_Completion Date', 
#                 'TotalAmountvendorCost': {
#                     '$toInt': {
#                         '$ifNull': [
#                             '$rate', 0
#                         ]
#                     }
#                 }
#             }
#         }
#     ]
#     arty2=[
#         {
#             '$match': {
#                 'deleteStatus': {
#                     '$ne': 1
#                 }
#             }
#         }, {
#             '$addFields': {
#                 'custId': {
#                     '$toObjectId': '$custId'
#                 }
#             }
#         }, {
#             '$lookup': {
#                 'from': 'projectGroup', 
#                 'localField': 'projectGroup', 
#                 'foreignField': '_id', 
#                 'pipeline': [
#                     {
#                         '$match': {
#                             'deleteStatus': {
#                                 '$ne': 1
#                             }
#                         }
#                     }, {
#                         '$lookup': {
#                             'from': 'costCenter', 
#                             'localField': 'costCenterId', 
#                             'foreignField': '_id', 
#                             'pipeline': [
#                                 {
#                                     '$match': {
#                                         'deleteStatus': {
#                                             '$ne': 1
#                                         }
#                                     }
#                                 }
#                             ], 
#                             'as': 'costCenterResults'
#                         }
#                     }, {
#                         '$project': {
#                             'costCenter': {
#                                 '$arrayElemAt': [
#                                     '$costCenterResults.costCenter', 0
#                                 ]
#                             }, 
#                             '_id': 0
#                         }
#                     }
#                 ], 
#                 'as': 'projectGroupResults'
#             }
#         }, {
#             '$lookup': {
#                 'from': 'customer', 
#                 'localField': 'custId', 
#                 'foreignField': '_id', 
#                 'as': 'customerResults'
#             }
#         }, {
#             '$project': {
#                 '_id': 0, 
#                 'costCenter': {
#                     '$arrayElemAt': [
#                         '$projectGroupResults.costCenter', 0
#                     ]
#                 }, 
#                 'Project ID': {
#                     '$toString': '$_id'
#                 }, 
#                 'Customer': {
#                     '$arrayElemAt': [
#                         '$customerResults.customerName', 0
#                     ]
#                 }
#             }
#         }
#     ]
#     Responser5=cmo.finding_aggregate("project",arty2)['data']
#     Responser4=cmo.finding_aggregate("milestone",arty)['data']
#     vendorCost=pd.DataFrame(Responser4)
#     dateList=["Completion Date"]
#     for col in dateList:
#         if col in vendorCost.columns:
#             vendorCost[col] = vendorCost[col].apply(convertToDateBulkExport)
#     vendorCost['Completion Date'] = pd.to_datetime(vendorCost['Completion Date'])
#     vendorCost[['Month', 'Year']] = vendorCost['Completion Date'].apply(get_custom_month_year)
#     print(vendorCost,'vendorCostvendorCost',vendorCost.columns)
#     vendorCost = vendorCost.groupby(['Year', 'Month', 'Project ID', 'Customer'], as_index=False).agg({'TotalAmountvendorCost': 'sum'})
#     print(vendorCost,'vendorCostvendorCost2',vendorCost.columns)
#     vendorCost2=pd.DataFrame(Responser5)
#     print(vendorCost2,'vendorCostvendorCost22',vendorCost2.columns)
#     vendorCost = vendorCost.merge(vendorCost2, on=['Project ID', 'Customer'], how='left')
#     vendorCost = vendorCost.drop(columns='Project ID')
#     #####
#     vendorCost['Month'] = vendorCost['Month'].astype(str)
#     vendorCost['Year'] = vendorCost['Year'].astype(str)
#     vendorCost['TotalAmountvendorCost'] = vendorCost['TotalAmountvendorCost'].apply(
#     lambda x: int(x) if pd.notnull(x) and str(x).isdigit() else 0
#         )   
#     vendorCost = vendorCost.groupby(['Year', 'Month', 'costCenter', 'Customer'], as_index=False).agg({'TotalAmountvendorCost': 'sum'})
#     vendorCost['Month'] = vendorCost['Month'].astype(int)
#     vendorCost['Year'] = vendorCost['Year'].astype(int)
#     print('78e78e7g7bx738',vendorCost)
    
#     # arty=[
#     #         {
#     #             '$match': {
#     #                 'deleteStatus': {
#     #                     '$ne': 1
#     #                 }, 
#     #                 'workDescription': {
#     #                     '$ne': None
#     #                 }, 
#     #                 'mileStoneStatus': 'Closed'
#     #             }
#     #         }, {
#     #             '$addFields': {
#     #                 'milestoneClosingDate': {
#     #                     '$toDate': '$CC_Completion Date'
#     #                 }, 
#     #                 'customerId': {
#     #                     '$toObjectId': '$customerId'
#     #                 }, 
#     #                 'SubProjectId': {
#     #                     '$toObjectId': '$SubProjectId'
#     #                 }, 
#     #                 'projectGropupId': {
#     #                     '$toObjectId': '$projectGropupId'
#     #                 }, 
#     #                 'assignerId': {
#     #                     '$arrayElemAt': [
#     #                         '$assignerId', 0
#     #                     ]
#     #                 }
#     #             }
#     #         }, {
#     #             '$addFields': {
#     #                 'actionAtKolkata': {
#     #                     '$dateAdd': {
#     #                         'startDate': '$milestoneClosingDate', 
#     #                         'unit': 'minute', 
#     #                         'amount': 330
#     #                     }
#     #                 }, 
#     #                 'dayOfMonth': {
#     #                     '$dayOfMonth': {
#     #                         'date': {
#     #                             '$dateAdd': {
#     #                                 'startDate': '$milestoneClosingDate', 
#     #                                 'unit': 'minute', 
#     #                                 'amount': 330
#     #                             }
#     #                         }
#     #                     }
#     #                 }
#     #             }
#     #         }, {
#     #             '$addFields': {
#     #                 'month': {
#     #                     '$switch': {
#     #                         'branches': [
#     #                             {
#     #                                 'case': {
#     #                                     '$lt': [
#     #                                         '$dayOfMonth', 26
#     #                                     ]
#     #                                 }, 
#     #                                 'then': {
#     #                                     '$switch': {
#     #                                         'branches': [
#     #                                             {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 1
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 1
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 2
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 2
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 3
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 3
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 4
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 4
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 5
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 5
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 6
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 6
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 7
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 7
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 8
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 8
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 9
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 9
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 10
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 10
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 11
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 11
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 12
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 12
#     #                                             }
#     #                                         ], 
#     #                                         'default': None
#     #                                     }
#     #                                 }
#     #                             }, {
#     #                                 'case': {
#     #                                     '$gte': [
#     #                                         '$dayOfMonth', 25
#     #                                     ]
#     #                                 }, 
#     #                                 'then': {
#     #                                     '$switch': {
#     #                                         'branches': [
#     #                                             {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 1
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 2
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 2
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 3
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 3
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 4
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 4
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 5
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 5
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 6
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 6
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 7
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 7
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 8
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 8
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 9
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 9
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 10
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 10
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 11
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 11
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 12
#     #                                             }, {
#     #                                                 'case': {
#     #                                                     '$eq': [
#     #                                                         {
#     #                                                             '$month': '$actionAtKolkata'
#     #                                                         }, 12
#     #                                                     ]
#     #                                                 }, 
#     #                                                 'then': 1
#     #                                             }
#     #                                         ], 
#     #                                         'default': None
#     #                                     }
#     #                                 }
#     #                             }
#     #                         ], 
#     #                         'default': None
#     #                     }
#     #                 }, 
#     #                 'year': {
#     #                     '$cond': {
#     #                         'if': {
#     #                             '$gt': [
#     #                                 {
#     #                                     '$dateToString': {
#     #                                         'format': '%m-%d', 
#     #                                         'date': '$milestoneClosingDate'
#     #                                     }
#     #                                 }, '12-25'
#     #                             ]
#     #                         }, 
#     #                         'then': {
#     #                             '$add': [
#     #                                 {
#     #                                     '$year': '$milestoneClosingDate'
#     #                                 }, 1
#     #                             ]
#     #                         }, 
#     #                         'else': {
#     #                             '$year': '$milestoneClosingDate'
#     #                         }
#     #                     }
#     #                 }
#     #             }
#     #         }, {
#     #             '$group': {
#     #                 '_id': {
#     #                     'assignerId': '$assignerId', 
#     #                     'siteId': '$siteId', 
#     #                     'workDescription': '$workDescription'
#     #                 }, 
#     #                 'data': {
#     #                     '$addToSet': '$$ROOT'
#     #                 }, 
#     #                 'data2': {
#     #                     '$addToSet': '$Name'
#     #                 }, 
#     #                 'assignerId': {
#     #                     '$first': '$assignerId'
#     #                 }
#     #             }
#     #         }, {
#     #             '$lookup': {
#     #                 'from': 'vendorCostMilestone', 
#     #                 'let': {
#     #                     'subProjectId': {
#     #                         '$arrayElemAt': [
#     #                             '$data.SubProjectId', 0
#     #                         ]
#     #                     }, 
#     #                     'projectGroup': {
#     #                         '$arrayElemAt': [
#     #                             '$data.projectGropupId', 0
#     #                         ]
#     #                     }, 
#     #                     'customerId': {
#     #                         '$arrayElemAt': [
#     #                             '$data.customerId', 0
#     #                         ]
#     #                     }, 
#     #                     'pair': '$data2', 
#     #                     'vendorId': '$assignerId'
#     #                 }, 
#     #                 'pipeline': [
#     #                     {
#     #                         '$match': {
#     #                             '$expr': {
#     #                                 '$and': [
#     #                                     {
#     #                                         '$ne': [
#     #                                             '$deleteStatus', 1
#     #                                         ]
#     #                                     }, {
#     #                                         '$eq': [
#     #                                             '$subProjectId', '$$subProjectId'
#     #                                         ]
#     #                                     }, {
#     #                                         '$eq': [
#     #                                             '$vendorId', '$$vendorId'
#     #                                         ]
#     #                                     }, {
#     #                                         '$eq': [
#     #                                             '$projectGroup', '$$projectGroup'
#     #                                         ]
#     #                                     }, {
#     #                                         '$eq': [
#     #                                             '$customerId', '$$customerId'
#     #                                         ]
#     #                                     }
#     #                                 ]
#     #                             }
#     #                         }
#     #                     }, {
#     #                         '$project': {
#     #                             'milestoneList': 1, 
#     #                             'rate': 1, 
#     #                             'itemCode': 1, 
#     #                             '_id': 0
#     #                         }
#     #                     }, {
#     #                         '$match': {
#     #                             '$expr': {
#     #                                 '$and': [
#     #                                     {
#     #                                         '$setIsSubset': [
#     #                                             '$milestoneList', '$$pair'
#     #                                         ]
#     #                                     }, {
#     #                                         '$setIsSubset': [
#     #                                             '$$pair', '$milestoneList'
#     #                                         ]
#     #                                     }
#     #                                 ]
#     #                             }
#     #                         }
#     #                     }
#     #                 ], 
#     #                 'as': 'milestoneResults'
#     #             }
#     #         }, {
#     #             '$match': {
#     #                 'milestoneResults': {
#     #                     '$ne': []
#     #                 }
#     #             }
#     #         }, {
#     #             '$addFields': {
#     #                 'minRate': {
#     #                     '$min': '$milestoneResults.rate'
#     #                 }, 
#     #                 'SubProjectId': {
#     #                     '$arrayElemAt': [
#     #                         '$data.SubProjectId', -1
#     #                     ]
#     #                 }, 
#     #                 'projectGropupId': {
#     #                     '$arrayElemAt': [
#     #                         '$data.projectGropupId', -1
#     #                     ]
#     #                 }, 
#     #                 'customerId': {
#     #                     '$arrayElemAt': [
#     #                         '$data.customerId', -1
#     #                     ]
#     #                 }, 
#     #                 'year': {
#     #                     '$arrayElemAt': [
#     #                         '$data.year', -1
#     #                     ]
#     #                 }, 
#     #                 'month': {
#     #                     '$arrayElemAt': [
#     #                         '$data.month', -1
#     #                     ]
#     #                 }
#     #             }
#     #         }, {
#     #             '$lookup': {
#     #                 'from': 'projectGroup', 
#     #                 'localField': 'projectGropupId', 
#     #                 'foreignField': '_id', 
#     #                 'pipeline': [
#     #                     {
#     #                         '$match': {
#     #                             'deleteStatus': {
#     #                                 '$ne': 1
#     #                             }
#     #                         }
#     #                     }, {
#     #                         '$lookup': {
#     #                             'from': 'costCenter', 
#     #                             'localField': 'costCenterId', 
#     #                             'foreignField': '_id', 
#     #                             'pipeline': [
#     #                                 {
#     #                                     '$match': {
#     #                                         'deleteStatus': {
#     #                                             '$ne': 1
#     #                                         }
#     #                                     }
#     #                                 }, {
#     #                                     '$project': {
#     #                                         'costCenter': 1
#     #                                     }
#     #                                 }
#     #                             ], 
#     #                             'as': 'costCenterResults'
#     #                         }
#     #                     }, {
#     #                         '$lookup': {
#     #                             'from': 'customer', 
#     #                             'localField': 'customerId', 
#     #                             'foreignField': '_id', 
#     #                             'pipeline': [
#     #                                 {
#     #                                     '$match': {
#     #                                         'deleteStatus': {
#     #                                             '$ne': 1
#     #                                         }
#     #                                     }
#     #                                 }, {
#     #                                     '$project': {
#     #                                         'customerName': 1
#     #                                     }
#     #                                 }
#     #                             ], 
#     #                             'as': 'customerResults'
#     #                         }
#     #                     }, {
#     #                         '$project': {
#     #                             'costCenter': {
#     #                                 '$arrayElemAt': [
#     #                                     '$costCenterResults.costCenter', 0
#     #                                 ]
#     #                             }, 
#     #                             'customerResults': {
#     #                                 '$arrayElemAt': [
#     #                                     '$customerResults.customerName', 0
#     #                                 ]
#     #                             }, 
#     #                             '_id': 0
#     #                         }
#     #                     }
#     #                 ], 
#     #                 'as': 'projectGroupResults'
#     #             }
#     #         }, {
#     #             '$addFields': {
#     #                 'costCenter': {
#     #                     '$arrayElemAt': [
#     #                         '$projectGroupResults.costCenter', 0
#     #                     ]
#     #                 }, 
#     #                 'customer': {
#     #                     '$arrayElemAt': [
#     #                         '$projectGroupResults.customerResults', 0
#     #                     ]
#     #                 }
#     #             }
#     #         }, {
#     #             '$project': {
#     #                 '_id': 0, 
#     #                 'TotalAmountvendorCost': {
#     #                     '$cond': {
#     #                         'if': {
#     #                             '$or': [
#     #                                 {
#     #                                     '$eq': [
#     #                                         '$minRate', None
#     #                                     ]
#     #                                 }, {
#     #                                     '$eq': [
#     #                                         '$minRate', ''
#     #                                     ]
#     #                                 }, {
#     #                                     '$not': [
#     #                                         '$minRate'
#     #                                     ]
#     #                                 }
#     #                             ]
#     #                         }, 
#     #                         'then': 0, 
#     #                         'else': '$minRate'
#     #                     }
#     #                 }, 
#     #                 'Month': '$month', 
#     #                 'Year': '$year', 
#     #                 'Customer': '$customer', 
#     #                 'costCenter': '$costCenter'
#     #             }
#     #         }, {
#     #             '$match': {
#     #                 '$and': [
#     #                     {
#     #                         'Month': {
#     #                             '$exists': True, 
#     #                             '$ne': None, 
#     #                             '$ne': ''
#     #                         }
#     #                     }, {
#     #                         'Year': {
#     #                             '$exists': True, 
#     #                             '$ne': None, 
#     #                             '$ne': ''
#     #                         }
#     #                     }, {
#     #                         'Customer': {
#     #                             '$exists': True, 
#     #                             '$ne': None, 
#     #                             '$ne': ''
#     #                         }
#     #                     }, {
#     #                         'costCenter': {
#     #                             '$exists': True, 
#     #                             '$ne': None, 
#     #                             '$ne': ''
#     #                         }
#     #                     }, {
#     #                         'TotalAmountvendorCost': {
#     #                             '$exists': True, 
#     #                             '$ne': None, 
#     #                             '$ne': ''
#     #                         }
#     #                     }
#     #                 ]
#     #             }
#     #         }
#     #     ]
#     # # print(arty,'rtyuiuytrtyuiopoiuytrtyuiop')
#     # Responser4=cmo.finding_aggregate("milestone",arty)['data']
#     # vendorCost=pd.DataFrame(Responser4)
#     # vendorCost['Month'] = vendorCost['Month'].astype(str)
#     # vendorCost['Year'] = vendorCost['Year'].astype(str)
#     # vendorCost['TotalAmountvendorCost'] = vendorCost['TotalAmountvendorCost'].apply(
#     # lambda x: int(x) if pd.notnull(x) and str(x).isdigit() else 0
#     #     )   
#     # vendorCost = vendorCost.groupby(['Year', 'Month', 'costCenter', 'Customer'], as_index=False).agg({'TotalAmountvendorCost': 'sum'})
#     # vendorCost['Month'] = vendorCost['Month'].astype(int)
#     # vendorCost['Year'] = vendorCost['Year'].astype(int)
#     # print('78e78e7g7bx738',vendorCost)
#     workDoneDatadf=toGetWorkDoneData()
#     # print(getcccdc(),'djkidjkjdkjdjj782')
#     # print('workDoneDatadfworkDoneDatadfworkDoneDatadf',workDoneDatadf)
#     expenseDf=toGetExpensesAccordingtoproject()
#     # print(getcccdc(),'djkidjkjdkjdjj783')
#     # print('expenseDfexpenseDfexpenseDf',expenseDf)
#     mainDf=pd.DataFrame([])
#     mainDf=workDoneDatadf
#     # print('tttt5',getcccdc())
#     if workDoneDatadf.empty or workDoneDatadf.shape[0] == 0:
#         if expenseDf.empty or expenseDf.shape[0] == 0: 
#             pass
#         else:
#             mainDf=expenseDf
#     else:
#         if expenseDf.empty or expenseDf.shape[0] == 0: 
#             pass
#         else:
#             mainDf=pd.merge(expenseDf, workDoneDatadf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
#     # print(mainDf,'djkjkdjkdjkjkdjkdjk')
#     # print(vendorCost,'vendorCostvendorCostvendorCost')
#     # print(vendorCost.columns,'vendorCostvendorCostvendorCost')
#     # print(mainDf,'mainDfmainDfmainDfmainDf')
#     if vendorCost.empty or vendorCost.shape[0] == 0:
#         pass
#     else:
#         vendorCost['customer']=vendorCost['Customer']
#         vendorCost = vendorCost.drop('Customer', axis=1)
#         if mainDf.empty or mainDf.shape[0] == 0:
#             mainDf=vendorCost
#         else:
#             mainDf=pd.merge(mainDf, vendorCost, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
#     stttysty=[
#             {
#                 '$match': {
#                     'deleteStatus': {
#                         '$ne': 1
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'customer', 
#                     'localField': 'customer', 
#                     'foreignField': '_id', 
#                     'as': 'customerResult'
#                 }
#             }, {
#                 '$project': {
#                     'costCenter': 1, 
#                     'costCenterId': {
#                         '$toString': '$_id'
#                     }, 
#                     'customer': {
#                         '$arrayElemAt': [
#                             '$customerResult.customerName', 0
#                         ]
#                     }, 
#                     'customerId': {
#                         '$toString': '$customer'
#                     }, 
#                     '_id': 0
#                 }
#             }
#         ]
#     Resty=cmo.finding_aggregate("costCenter",stttysty)['data']
#     if len(Resty):
#         DataDf = pd.DataFrame(Resty)
#         mainDf = mainDf.merge(DataDf, on=['customer', 'costCenter'], how='left')
#     lastUpdatedAt=getcccdc()
#     json_Data = json.loads(mainDf.to_json(orient='records'))
    
#     Response=None
#     for i in json_Data:
#         jsonsDta={
#             'Year':i['Year'],
#             'Month':i['Month'],
#             'costCenterId':ObjectId(i['costCenterId']),
#             'customerId':ObjectId(i['customerId']),
#             'ApprovedAmount':i['ApprovedAmount'],
#             'total_Amount':i['total_Amount'],
#             'TotalAmountvendorCost':i['TotalAmountvendorCost'],
#             'lastUpdatedAt':lastUpdatedAt
#         }
        
#         Response=cmo.updating("schedulerData",{'Year':i['Year'],'Month':i['Month'],'costCenterId':ObjectId(i['costCenterId']),'customerId':ObjectId(i['customerId'])},jsonsDta,True)
    
     
#     # print(workDoneDatadf,'revenue_merged_dfrevenue_merged_dfrevenue_merged_df2')
#     # print(expenseDf,'expenseDfexpenseDf')
#     # print(salaryDBDf,'salaryDBDfsalaryDBDf')
#     # fullPath = excelWriteFunc.excelFileWriter(
#     #     mainDf, "Export_Expenses", "Expenses And Advance"
#     # )
#     # # print("fullPathfullPathfullPath", fullPath)
#     # return send_file(fullPath)
#     # print(OtherFixedCostDf,'OtherFixedCostDfOtherFixedCostDfOtherFixedCostDf')
#     # print(vendorCost,'vendorCostvendorCost')
#     # print(workDoneDatadf.columns,'revenue_merged_dfrevenue_merged_dfrevenue_merged_df2')
#     # # print(expenseDf.columns,'expenseDfexpenseDf')
#     # print(salaryDBDf.columns,'salaryDBDfsalaryDBDf')
#     # print(OtherFixedCostDf.columns,'OtherFixedCostDfOtherFixedCostDfOtherFixedCostDf')
#     # print(vendorCost.columns,'vendorCostvendorCost')



def get_gpTracking():
    
    arty=[
            {
                '$addFields': {
                    'newAssigndate': {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': {
                                '$dateFromString': {
                                    'dateString': '$assignDate', 
                                    'format': '%d-%m-%Y'
                                }
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'typeAssigned': 'Partner'
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'mileStoneStartDate1': {
                        '$toDate': '$mileStoneStartDate'
                    }, 
                    'mileStoneEndtDate1': {
                        '$toDate': '$mileStoneEndDate'
                    }, 
                    'CC_Completion Date1': {
                        '$toDate': '$CC_Completion Date'
                    }, 
                    'Task Closure': {
                        '$toDate': '$Task Closure'
                    }
                }
            }, {
                '$addFields': {
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
                    'CC_Completion Date': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    {
                                        '$type': '$CC_Completion Date1'
                                    }, 'date'
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'date': '$CC_Completion Date1', 
                                    'format': '%d-%m-%Y', 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'taskageing': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    {
                                        '$type': '$CC_Completion Date1'
                                    }, 'date'
                                ]
                            }, 
                            'then': {
                                '$round': {
                                    '$divide': [
                                        {
                                            '$subtract': [
                                                '$mileStoneEndtDate1', '$CC_Completion Date1'
                                            ]
                                        }, 86400000
                                    ]
                                }
                            }, 
                            'else': {
                                '$round': {
                                    '$divide': [
                                        {
                                            '$subtract': [
                                                '$mileStoneEndtDate1', '$$NOW'
                                            ]
                                        }, 86400000
                                    ]
                                }
                            }
                        }
                    }, 
                    'mileStoneStartDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$mileStoneStartDate'
                            }, 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'mileStoneEndDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$mileStoneEndDate'
                            }, 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
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
                                }, 
                                'type': 'Partner'
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'assignerName': '$vendorName', 
                                'vendorCode': {
                                    '$toString': '$vendorCode'
                                }, 
                                'assignerId': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ], 
                    'as': 'assignerResult'
                }
            }, {
                '$project': {
                    'assignerId': 0
                }
            }, {
                '$group': {
                    '_id': '$siteId', 
                    'milestoneArray': {
                        '$addToSet': '$$ROOT'
                    }, 
                    'siteId': {
                        '$first': '$siteId'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteId', 
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
                                'SubProjectId': {
                                    '$toObjectId': '$SubProjectId'
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
                                    }
                                ], 
                                'as': 'SubTypeResult'
                            }
                        }, {
                            '$unwind': {
                                'path': '$SubTypeResult', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$addFields': {
                                'SubProject': '$SubTypeResult.subProject', 
                                'projectuniqueId': {
                                    '$toObjectId': '$projectuniqueId'
                                }, 
                                'SubProjectId': {
                                    '$toString': '$SubProjectId'
                                }, 
                                'projectType': '$SubTypeResult.projectType'
                            }
                        }, {
                            '$lookup': {
                                'from': 'project', 
                                'localField': 'projectuniqueId', 
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
                                            'PMId': {
                                                '$toObjectId': '$PMId'
                                            }
                                        }
                                    }, {
                                        '$addFields': {
                                            'customerId': {
                                                '$toObjectId': '$custId'
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
                                            'as': 'PMarray'
                                        }
                                    }, {
                                        '$unwind': {
                                            'path': '$PMarray', 
                                            'preserveNullAndEmptyArrays': True
                                        }
                                    }, {
                                        '$addFields': {
                                            'PMName': '$PMarray.empName', 
                                            'projectGroupName': {
                                                '$arrayElemAt': [
                                                    '$projectGroupResult.projectGroupName', 0
                                                ]
                                            }, 
                                            'Customer': {
                                                '$arrayElemAt': [
                                                    '$projectGroupResult.Customer', 0
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'PMName': 1, 
                                            'projectId': 1, 
                                            '_id': 0, 
                                            'Customer': 1, 
                                            'projectGroupName': 1, 
                                            'projectGroupId': '$projectGroup', 
                                            'customerId': 1
                                        }
                                    }
                                ], 
                                'as': 'projectArray'
                            }
                        }, {
                            '$unwind': {
                                'path': '$projectArray', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }
                    ], 
                    'as': 'siteResult'
                }
            }, {
                '$unwind': '$siteResult'
            }, {
                '$addFields': {
                    'siteResult.milestoneArray': '$milestoneArray'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$siteResult'
                }
            }, {
                '$addFields': {
                    'siteStartDate': {
                        '$toDate': '$siteStartDate'
                    }, 
                    'siteEndDate': {
                        '$toDate': '$siteStartDate'
                    }, 
                    'Site_Completion Date': {
                        '$toDate': '$siteStartDate'
                    }
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'projectuniqueId': {
                        '$toString': '$projectuniqueId'
                    }, 
                    'siteStartDate': {
                        '$dateToString': {
                            'date': '$siteStartDate', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'siteEndDate': {
                        '$dateToString': {
                            'date': '$siteEndDate', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'Site_Completion Date': {
                        '$dateToString': {
                            'date': '$Site_Completion Date', 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'PMName': '$projectArray.PMName', 
                    'projectId': '$projectArray.projectId', 
                    'subProject': '$SubProject', 
                    'siteageing': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    {
                                        '$type': '$Site_Completion Date'
                                    }, 'date'
                                ]
                            }, 
                            'then': {
                                '$round': {
                                    '$divide': [
                                        {
                                            '$subtract': [
                                                '$siteEndDate', '$Site_Completion Date'
                                            ]
                                        }, 86400000
                                    ]
                                }
                            }, 
                            'else': ''
                        }
                    }
                }
            }, {
                '$addFields': {
                    'milestoneArray': {
                        '$sortArray': {
                            'input': '$milestoneArray', 
                            'sortBy': {
                                '_id': 1
                            }
                        }
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'projectGroupName': '$projectArray.projectGroupName', 
                    'Customer': '$projectArray.Customer'
                }
            }, {
                '$unwind': {
                    'path': '$milestoneArray'
                }
            }, {
                '$group': {
                    '_id': {
                        'workDescription': '$milestoneArray.workDescription', 
                        'siteId': '$milestoneArray.siteId'
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
                        '$toObjectId': {
                            '$arrayElemAt': [
                                '$data.SubProjectId', 0
                            ]
                        }
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
            }, {
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
                    'newRoot': '$realData'
                }
            }, {
                '$addFields': {
                    'milestoneArray': {
                        '$sortArray': {
                            'input': '$milestoneArray', 
                            'sortBy': {
                                '_id': 1
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
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]
    arty2=[
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
                            'as': 'costCenterResults'
                        }
                    }, {
                        '$project': {
                            'costCenter': {
                                '$arrayElemAt': [
                                    '$costCenterResults.costCenter', 0
                                ]
                            }, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'projectGroupResults'
            }
        }, {
            '$lookup': {
                'from': 'customer', 
                'localField': 'custId', 
                'foreignField': '_id', 
                'as': 'customerResults'
            }
        }, {
            '$project': {
                '_id': 0, 
                'costCenter': {
                    '$arrayElemAt': [
                        '$projectGroupResults.costCenter', 0
                    ]
                }, 
                'Project ID': {
                    '$toString': '$_id'
                }, 
                'Customer': {
                    '$arrayElemAt': [
                        '$customerResults.customerName', 0
                    ]
                }
            }
        }
    ]
    Responser5=cmo.finding_aggregate("project",arty2)['data']
    
    response = cmo.finding_aggregate("milestone", arty)["data"]
    sites_data = []
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
                "Month":milestone["completionMonth"],
                "Year":milestone["completionYear"],
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
    df=sites_df    
    df = pd.DataFrame(sites_data)
    for col in dateList:
        if col in df.columns:
            df[col] = df[col].apply(convertToDateBulkExport)
    # df['MS Completition Date'] = pd.to_datetime(df['MS Completition Date'], errors='coerce')
    # df[['Month', 'Year']] = df['MS Completition Date'].apply(get_custom_month_year)
    
    # print(df,'hjhjdhjhjdjhd',df['Month'],df['Year'])
    # df['MS Completition Date'] = pd.to_datetime(df['MS Completition Date'], errors='coerce')
    df = df[~df['Month'].isnull() & (df['Month'] != '')]

    df = df[ (df['PO eligibility (Yes/No)'] == 'Yes')]
    df['NewColumn'] =df['System Id'] + df['Vendor Item Code']
    df = df.drop_duplicates(subset=['NewColumn', 'Month', 'Year'], keep='last')
    df['Vendor Rate'] = df['Vendor Rate'].apply(lambda x: int(x) if pd.notnull(x) and str(x).isdigit() else 0) 
    df = df.groupby(['Project Group', 'Customer', 'Month', 'Year'])['Vendor Rate'].sum().reset_index()
    df = df.sort_values(by=['Year', 'Month', 'Project Group', 'Customer'])
    df['costCenter'] = df['Project Group'].str.extract(r'-([^-\s]+)$')   
    vendorCost=df
    vendorCost = vendorCost.drop('Project Group', axis=1)
    
    # df = df[ (df['PO eligibility (Yes/No)'] == 'Yes')]
    # df['NewColumn']=df['Vendor Item Code']+df['System Id']
    # # df = df.drop_duplicates(subset='NewColumn', keep='last')
    # df = df.drop_duplicates(subset=['NewColumn'], keep='last')
    # df['Vendor Rate'] = df['Vendor Rate'].apply(lambda x: int(x) if pd.notnull(x) and str(x).isdigit() else 0) 
    # df = df.groupby(['Project Group', 'Customer', 'Month', 'Year'])['Vendor Rate'].sum().reset_index()
    # df = df.sort_values(by=['Year', 'Month', 'Project Group', 'Customer'])
    # df['costCenter'] = df['Project Group'].str.extract(r'-([^-\s]+)$')   
    # vendorCost=df
    # vendorCost = vendorCost.drop('Project Group', axis=1)
    
    workDoneDatadf=toGetWorkDoneData()
    expenseDf=toGetExpensesAccordingtoproject()
    mainDf=pd.DataFrame([])
    mainDf=workDoneDatadf
    print(mainDf,'mainDfmainDfmainDfmainDf1')
    if workDoneDatadf.empty or workDoneDatadf.shape[0] == 0:
        if expenseDf.empty or expenseDf.shape[0] == 0: 
            pass
        else:
            mainDf=expenseDf
    else:
        if expenseDf.empty or expenseDf.shape[0] == 0: 
            pass
        else:
            mainDf=pd.merge(expenseDf, workDoneDatadf, on=['Month', 'Year', 'costCenter', 'customer'], how='right')
    # print(mainDf,'djkjkdjkdjkjkdjkdjk')
    # print(vendorCost,'vendorCostvendorCostvendorCost')
    # print(vendorCost.columns,'vendorCostvendorCostvendorCost')
    print(mainDf,'mainDfmainDfmainDfmainDf11')
    if vendorCost.empty or vendorCost.shape[0] == 0:
        pass
    else:
        vendorCost['customer']=vendorCost['Customer']
        vendorCost = vendorCost.drop('Customer', axis=1)
        if mainDf.empty or mainDf.shape[0] == 0:
            mainDf=vendorCost
        else:
            mainDf=pd.merge(mainDf, vendorCost, on=['Month', 'Year', 'costCenter', 'customer'], how='left')
    print(mainDf,'mainDfmainDfmainDfmainDf12')
    stttysty=[
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
                    'as': 'customerResult'
                }
            }, {
                '$project': {
                    'costCenter': 1, 
                    'costCenterId': {
                        '$toString': '$_id'
                    }, 
                    'customer': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'customerId': {
                        '$toString': '$customer'
                    }, 
                    '_id': 0
                }
            }
        ]
    Resty=cmo.finding_aggregate("costCenter",stttysty)['data']
    if len(Resty):
        DataDf = pd.DataFrame(Resty)
        mainDf = mainDf.merge(DataDf, on=['customer', 'costCenter'], how='inner')
    json_Data = json.loads(mainDf.to_json(orient='records'))
    
    Response=None
    jkjkdjkjkd=[]
    for i in json_Data:
        jsonsDta={
            'Year':i['Year'],
            'Month':i['Month'],
            'costCenterId':ObjectId(i['costCenterId']),
            'customerId':ObjectId(i['customerId']),
            # 'ApprovedAmount':i['ApprovedAmount'],
            'total_Amount':i['total_Amount'],
            'TotalAmountvendorCost':i['Vendor Rate'],
            'lastUpdatedAt':getcccdc2()
        }
        jkjkdjkjkd.append(jsonsDta)
        
        Response=cmo.updating("schedulerData",{'Year':i['Year'],'Month':i['Month'],'costCenterId':ObjectId(i['costCenterId']),'customerId':ObjectId(i['customerId'])},jsonsDta,True)
    
    print(len(jkjkdjkjkd),'dhhdhdjh')
   