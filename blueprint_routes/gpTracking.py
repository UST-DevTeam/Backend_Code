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

gpTracking_blueprint = Blueprint("gpTracking_blueprint", __name__)
def getcccdc():
    current_time333 = dtt.now()
    return current_time333


#####DOn't Touch until it works

def get_days_in_month(month_number, year):
    _, num_days = calendar.monthrange(year, month_number)
    return num_days

def get_month_for_range_in_kolkata():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_date = dtt.now(kolkata_tz)
    day = current_date.day
    if day >= 26:
        if current_date.month == 12: 
            month_number = 1
            year = current_date.year + 1
        else:
            month_number = current_date.month + 1
            year = current_date.year
    else:
        month_number = current_date.month
        year = current_date.year

    return month_number, year



def getCustomMonthYear():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    today = dtt.now(kolkata_tz)
    if today.day >= 26:
        if today.month == 12:
            custom_month = 1 
            custom_year = today.year + 1  
        else:
            custom_month = today.month + 1 
            custom_year = today.year
    else:
        if today.month == 1:
            custom_month = 12
            custom_year = today.year - 1 
        else:
            custom_month = today.month - 1 
            custom_year = today.year 

    if today.day >= 26:
        days_spent = today.day - 25 
    else:
        
        prev_month_days = monthrange(today.year, today.month - 1 if today.month > 1 else 12)[1]
        days_spent = (prev_month_days - 25) + today.day

    if today.month == 12 and today.day >= 26:
        total_days_in_custom_month = (31 - 25) + 25 
    else:
        total_days_in_custom_month = monthrange(custom_year, custom_month)[1] - 25 + 25
    jjj=get_month_for_range_in_kolkata()
    return jjj[0]-1, jjj[1], days_spent,get_days_in_month(jjj[0]-1, jjj[1]) 

### dont touch until it works
def get_days_in_month(month_number, year):
    _, num_days = calendar.monthrange(year, month_number)
    return num_days

def get_month_for_range_in_kolkata():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_date = dtt.now(kolkata_tz)
    day = current_date.day
    if day >= 26:
        if current_date.month == 12: 
            month_number = 1
            year = current_date.year + 1
        else:
            month_number = current_date.month + 1
            year = current_date.year
    else:
        month_number = current_date.month
        year = current_date.year

    return month_number, year



def getCustomMonthYear():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    today = dtt.now(kolkata_tz)
    if today.day >= 26:
        if today.month == 12:
            custom_month = 1 
            custom_year = today.year + 1  
        else:
            custom_month = today.month + 1 
            custom_year = today.year
    else:
        if today.month == 1:
            custom_month = 12
            custom_year = today.year - 1 
        else:
            custom_month = today.month - 1 
            custom_year = today.year 

    if today.day >= 26:
        days_spent = today.day - 25 
    else:
        
        prev_month_days = monthrange(today.year, today.month - 1 if today.month > 1 else 12)[1]
        days_spent = (prev_month_days - 25) + today.day

    if today.month == 12 and today.day >= 26:
        total_days_in_custom_month = (31 - 25) + 25 
    else:
        total_days_in_custom_month = monthrange(custom_year, custom_month)[1] - 25 + 25
    jjj=get_month_for_range_in_kolkata()
    return jjj[0], jjj[1], days_spent,get_days_in_month(jjj[0]-1, jjj[1]) 
  
  
def get_current_month_days():
    today = dtt.today()
    year, month = today.year, today.month
    total_days = calendar.monthrange(year, month)[1]
    spent_days = today.day
    return {"month": month, "total_days": total_days, "spent_days": spent_days}
   
def sub_project2(customMonthList):
    art=[]
    if len(customMonthList):
        art=art+[
                {
                    '$addFields': {
                        'custId': {
                            '$toObjectId': '$custId'
                        },
                        'status':'Active'
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
                                    'customerName': {
                                        '$in': customMonthList
                                    }
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



def toGetExpensesAccordingtoproject(empId,customMonthList,customYearList):
    # arr1=[
    #         # {
    #         #     '$match': {
    #         #         'empId': empId
    #         #     }
    #         # },
    #         {
    #             '$unwind': {
    #                 'path': '$projectIds'
    #             }
    #         }, {
    #             '$project': {
    #                 'empId': 1, 
    #                 '_id': 0, 
    #                 'projectId': {
    #                     '$toString': '$projectIds'
    #                 }
    #             }
    #         }
    #     ]
    # projectAll=cmo.finding_aggregate("projectAllocation",arr1)
    # projectDf = pd.DataFrame(projectAll['data'])
    arr2=[
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
                        '$toDate': '$actionAt'
                    }
                }
            }, {
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
            }, {
                '$addFields': {
                    'actionAtKolkata': {
                        '$dateAdd': {
                            'startDate': '$actionAt2', 
                            'unit': 'minute', 
                            'amount': 330
                        }
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
            }, {
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
            }, {
                '$addFields': {
                    'year': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    {
                                        '$dateToString': {
                                            'format': '%m-%d', 
                                            'date': '$actionAt2'
                                        }
                                    }, '12-25'
                                ]
                            }, 
                            'then': {
                                '$add': [
                                    {
                                        '$year': '$actionAt2'
                                    }, 1
                                ]
                            }, 
                            'else': {
                                '$year': '$actionAt2'
                            }
                        }
                    }
                }
            }, {
                '$match': {
                    'year': {
                        '$in': customYearList
                    }, 
                    'month': {
                        '$in': customMonthList
                    }
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectId', 
                    'foreignField': '_id', 
                    'as': 'projectResult'
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'Year': '$year', 
                    'Month': '$month', 
                    'projectId': {
                        '$toString': '$projectId'
                    }, 
                    'ApprovedAmount': 1
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
        expenseDf = pd.merge(expenseDf, projectGroup, on='projectId', how='left')
        expenseDf = expenseDf.drop('projectId', axis=1)
        expenseDf = expenseDf.groupby(['Year', 'Month', 'costCenter', 'customer'], as_index=False).agg({'ApprovedAmount': 'sum'})
        expensedf = expenseDf.groupby(['Year', 'Month', 'costCenter', 'customer'], as_index=False).agg({'ApprovedAmount': 'sum'})
        return expenseDf
    else:
        expenseDf=pd.DataFrame([])
        return expenseDf
  
  
  
        
def toGetWorkDoneData(customersList,customMonthList,customYearList):
    # print(customersList,customYearList,'tey6ue663e663eugfugjgyuj')
    montharay=[
        {
        '$match': {
            'MS1CustomMonth': {
                '$in': customMonthList
            }, 
            'MS2CustomMonth': {
                '$in': customMonthList
            }, 
            'MS1CustomYear': {
                '$in': customYearList
            }, 
            'MS2CustomYear': {
                '$in': customYearList
            }
        }
    }
    ]
    subProjectArray = sub_project2(customersList)
    # print(subProjectArray,'djkudkkuduuduuidui')
    if request.args.get("projectType")!=None and request.args.get("projectType")!="undefined":
        subProjectArray = request.args.get("projectType").split(',')
    arra = [
        {
            '$match':{
                'deleteStatus': {'$ne': 1},
                'projectuniqueId':{
                    '$in':projectId_str2()
                }
            }
        }
    ]
    if request.args.get("siteId")!=None and request.args.get("siteId")!='undefined':
        arra = arra + [
            {
                '$match': {
                    'Site Id':{
                        '$regex':request.args.get("siteId").strip(),
                        '$options': 'i'
                    }
                }
            }
        ]
        subProjectArray = []
        for i in (sub_project2(customersList)):
            for k in i['uid']:
                subProjectArray.append(k)
    if request.args.get("siteBillingStatus")!=None and request.args.get("siteBillingStatus")!="undefined":
        siteStatus = request.args.get("siteBillingStatus")
        arra = arra + [
            {
                '$match':{
                    'siteBillingStatus':siteStatus,
                    
                }
            }
        ]
    
    arra = arra +[
        {
            '$match':{
                'SubProjectId':{'$in':subProjectArray}
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
            '$addFields':{
                'MS1':{
                    '$dateToString': {
                        'date': '$MS1', 
                        'timezone': 'Asia/Kolkata', 
                        'format': '%d/%m/%Y'
                    }
                },
                'MS2':{
                    '$dateToString': {
                        'date': '$MS2', 
                        'timezone': 'Asia/Kolkata', 
                        'format': '%d/%m/%Y'
                    }
                }
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
                'siteuid':{
                    '$toString':'$_id'
                },
                'Site Id':1,
                "MS1":1,
                "MS2":1,
                "systemId":1,
                '_id':0,
                'projectIdNew':"$projectuniqueId"
            }
                },
                {
                '$addFields': {
                    'MS1DateNew': {
                        '$dateFromString': {
                            'dateString': '$MS1', 
                            'format': '%d/%m/%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'MS2DateNew': {
                        '$dateFromString': {
                            'dateString': '$MS2', 
                            'format': '%d/%m/%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'MS1dateToProcess': {
                        '$cond': [
                            {
                                '$ifNull': [
                                    '$MS1DateNew', False
                                ]
                            }, '$MS1DateNew', None
                        ]
                    }, 
                    'MS2dateToProcess': {
                        '$cond': [
                            {
                                '$ifNull': [
                                    '$MS2DateNew', False
                                ]
                            }, '$MS2DateNew', None
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'ms2dateParts': {
                        '$dateToParts': {
                            'date': '$MS2dateToProcess', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'ms1dateParts': {
                        '$dateToParts': {
                            'date': '$MS1dateToProcess', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'MS1CustomMonth': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$gte': [
                                            '$ms1dateParts.day', 26
                                        ]
                                    }, 
                                    'then': '$ms1dateParts.month'
                                }, {
                                    'case': {
                                        '$lt': [
                                            '$ms1dateParts.day', 26
                                        ]
                                    }, 
                                    'then': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$ms1dateParts.month', 1
                                                ]
                                            }, 12, {
                                                '$subtract': [
                                                    '$ms1dateParts.month', 1
                                                ]
                                            }
                                        ]
                                    }
                                }
                            ], 
                            'default': None
                        }
                    }, 
                    'MS2CustomMonth': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$gte': [
                                            '$ms2dateParts.day', 26
                                        ]
                                    }, 
                                    'then': '$ms2dateParts.month'
                                }, {
                                    'case': {
                                        '$lt': [
                                            '$ms2dateParts.day', 26
                                        ]
                                    }, 
                                    'then': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$ms2dateParts.month', 1
                                                ]
                                            }, 12, {
                                                '$subtract': [
                                                    '$ms2dateParts.month', 1
                                                ]
                                            }
                                        ]
                                    }
                                }
                            ], 
                            'default': None
                        }
                    }, 
                    'MS1CustomYear': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$gte': [
                                            '$ms1dateParts.day', 26
                                        ]
                                    }, 
                                    'then': '$ms1dateParts.year'
                                }, {
                                    'case': {
                                        '$lt': [
                                            '$ms1dateParts.day', 26
                                        ]
                                    }, 
                                    'then': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$ms1dateParts.month', 1
                                                ]
                                            }, {
                                                '$subtract': [
                                                    '$ms1dateParts.year', 1
                                                ]
                                            }, '$ms1dateParts.year'
                                        ]
                                    }
                                }
                            ], 
                            'default': None
                        }
                    }, 
                    'MS2CustomYear': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$gte': [
                                            '$ms2dateParts.day', 26
                                        ]
                                    }, 
                                    'then': '$ms2dateParts.year'
                                }, {
                                    'case': {
                                        '$lt': [
                                            '$ms2dateParts.day', 26
                                        ]
                                    }, 
                                    'then': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$ms2dateParts.month', 1
                                                ]
                                            }, {
                                                '$subtract': [
                                                    '$ms2dateParts.year', 1
                                                ]
                                            }, '$ms2dateParts.year'
                                        ]
                                    }
                                }
                            ], 
                            'default': None
                        }
                    }
                }
            }, {
                '$addFields': {
                    'MS1CustomMonth': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$MS1CustomMonth', 12
                                ]
                            }, 1, {
                                '$add': [
                                    '$MS1CustomMonth', 1
                                ]
                            }
                        ]
                    }, 
                    'MS2CustomMonth': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$MS2CustomMonth', 12
                                ]
                            }, 1, {
                                '$add': [
                                    '$MS2CustomMonth', 1
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$project': {
                    'MS1DateNew': 0, 
                    'MS2DateNew': 0, 
                    'MS1dateToProcess': 0, 
                    'MS2dateToProcess': 0, 
                    'ms1dateParts': 0, 
                    'ms2dateParts': 0
                }
            }
    ]
    response = cmo.finding_aggregate("SiteEngineer",arra+montharay)
    if len(response['data']):
        pivotedData = pd.DataFrame(response['data'])
        pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
        # print(pivotedData)
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
        # print(mergedData.columns,'mergedData3')
        pivotedData['projectId'] = mergedData['projectId']
        pivotedData['projectGroup'] = mergedData['projectGroup']
        pivotedData['customer'] = mergedData['customer']
        pivotedData['costCenter'] = mergedData['costCenter']
        # print(pivotedData.columns,'mergedData4')
        mergedData = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
        # print(mergedData.columns,'mergedData5')
        pivotedData['subProject'] = mergedData['subProject']
        pivotedData['projectType'] = mergedData['projectType']
        mergedData = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
        # print(mergedData.columns,'mergedData6')
        masterunitRatedf['projectId'] = mergedData['projectId']
        mergedData = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
        # print(mergedData.columns,'mergedData7')
        masterunitRatedf['subProject'] = mergedData['subProject']
        masterunitRatedf['projectType'] = mergedData['projectType']
        pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        # print(pivotedData,'skjuiui3eiuui4iuruifiuf')
        if 'customer' in masterunitRatedf.columns:
            masterunitRatedf.drop(['projectuniqueId', 'SubProjectId','customer'], axis=1, inplace=True)
        else:
            masterunitRatedf.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
        df_macro = pivotedData[pivotedData['projectType'] == 'MACRO']
        # print(df_macro)
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
        # print(df_macro_merged.columns,'mergedData7')
        # print('###########1',df_dismantle_relocation['customer'],df_dismantle_relocation.columns)
        df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        # print(df_dismantle_relocation_merged.columns,'mergedData8')
        # df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
        df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
        # print(df_other_merged.columns,'mergedData9')
        final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
        # print(final_df.columns,'mergedData10')
        def calculate_amounts(row):
            amount1 = row['rate'] * 0.65 if pd.notna(row['MS1']) else 0
            amount2 = row['rate'] * 0.35 if pd.notna(row['MS2']) else 0
            return pd.Series([amount1, amount2])
        final_df[['amount1', 'amount2']] = final_df.apply(calculate_amounts, axis=1)
        # print(final_df.columns,'mergedData11')
        final_df['final_amount'] = final_df['amount1'] + final_df['amount2']
        # print(final_df.columns,'mergedData12')
        final_df['uniqueId'] = "1"
        # print(final_df.columns,'ghjkjhgfghjk1')
        columnsToDelete = [
            'subProject', 'projectType', 'itemCode01', 'itemCode02', 'itemCode03','Site Id', 'systemId', 'siteBillingStatus', 'siteId', 'siteuid',
            'itemCode04', 'itemCode05', 'itemCode06', 'itemCode07', 'rate', 'BAND',
            'Item Code-08', 'Item Code-09','uniqueId', 'ACTIVITY','projectGroup','MS1', 'MS2','projectId']
        # workDoneDataworkDoneDataworkDoneDataworkDoneDataNew111print(workDoneDatadf.columns,'workDoneDataworkDoneDataworkDoneDataworkDoneDataNew110')
        final_df = final_df.drop(columns=[col for col in columnsToDelete if col in final_df.columns])
        final_df['amount1'] = final_df['amount1'].fillna(0)
        final_df['amount2'] = final_df['amount2'].fillna(0)
        # print(final_df['customer'],'final_dffinal_dffinal_dffinal_df')
        final_df['amount_1'] = final_df.groupby(['MS1CustomMonth', 'MS1CustomYear', 'projectIdNew','costCenter','customer'])['amount1'].transform('sum')
        final_df['amount_2'] = final_df.groupby(['MS2CustomMonth', 'MS2CustomYear', 'projectIdNew','costCenter','customer'])['amount2'].transform('sum')
        # workDoneDatadf=workDoneDatadf.drop_duplicates()
        final_df = final_df.drop_duplicates(subset=['projectIdNew', 'MS1CustomMonth', 'MS1CustomYear','costCenter','customer'], keep='first')
        final_df = final_df.drop_duplicates(subset=['projectIdNew', 'MS2CustomMonth', 'MS2CustomYear','costCenter','customer'], keep='first')
        # print(workDoneDatadf,'workDoneDatadfworkDoneDatadf1')
        final_df['projectId']=final_df['projectIdNew']
        # print(final_df,'final_dffinal_dffinal_dffinal_df')
        filtered_df_MS1 = final_df[(final_df['MS1CustomMonth'].isin(customMonthList)) & (final_df['MS1CustomYear'].isin(customYearList))]
        filtered_df_MS2 = final_df[(final_df['MS2CustomMonth'].isin(customMonthList)) & (final_df['MS2CustomYear'].isin(customYearList))]
        filtered_df_MS1['Month']=filtered_df_MS1['MS1CustomMonth']
        filtered_df_MS1['Year']=filtered_df_MS1['MS1CustomYear']
        filtered_df_MS2['Month']=filtered_df_MS2['MS2CustomMonth']
        filtered_df_MS2['Year']=filtered_df_MS2['MS2CustomYear']
        columnsToDelete2 = ['projectIdNew', 'MS1CustomMonth', 'MS2CustomMonth', 'MS1CustomYear','MS2CustomYear','amount1', 'amount2','final_amount','amount_2']
        columnsToDelete3 =  ['projectIdNew', 'MS1CustomMonth', 'MS2CustomMonth', 'MS1CustomYear','MS2CustomYear','amount1', 'amount2','final_amount','amount_1']
        filtered_df_MS1 = filtered_df_MS1.drop(columns=[col for col in columnsToDelete2 if col in filtered_df_MS1.columns])
        filtered_df_MS2 = filtered_df_MS2.drop(columns=[col for col in columnsToDelete3 if col in filtered_df_MS2.columns])
        revenue_merged_df = pd.merge(filtered_df_MS1, filtered_df_MS2, on=['Month','Year','projectId','customer','costCenter'], how='outer')
        revenue_merged_df['total_Amount'] = revenue_merged_df[['amount_1', 'amount_2']].fillna(0).sum(axis=1)
        revenue_merged_df = revenue_merged_df.drop('projectId', axis=1)
        revenue_merged_df = revenue_merged_df.groupby(['Year', 'Month', 'customer','costCenter'], as_index=False).agg({'total_Amount': 'sum'})
        # print(revenue_merged_df,'revenue_merged_dfrevenue_merged_df1236767367376')
        # revenue_merged_df = revenue_merged_df.groupby(['Year', 'Month', 'projectId'], as_index=False).agg({'total_Amount': 'sum'})
        # print(revenue_merged_df,'revenue_merged_dfrevenue_merged_df1234')
        # revenue_merged_df = revenue_merged_df.drop('projectId', axis=1)
        # print(revenue_merged_df,'revenue_merged_dfrevenue_merged_df12345')
        # revenue_merged_df = revenue_merged_df.groupby(['Year', 'Month', 'customer','costCenter'], as_index=False).agg({'total_Amount': 'sum'})
        # print(revenue_merged_df,'revenue_merged_dfrevenue_merged_df123456')
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





@gpTracking_blueprint.route("/gp/customer",methods=["GET","POST"])
@token_required

def getCustomer(current_user):
    if request.method == "GET":
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
                '$project': {
                    'customer': '$shortName', 
                    'customerName':'$customerName',
                    'customerId': {
                        '$toString': '$_id'
                    }, 
                    'uniqueId':{
                       '$toString': '$_id' 
                    },
                    '_id': 0
                }
            }
        ]
        Response=cmo.finding_aggregate("customer",arr)
        return respond(Response)



            


@gpTracking_blueprint.route("/gp/costCenter",methods=["GET","POST"])
@gpTracking_blueprint.route("/gp/costCenter/<id>",methods=["GET","POST"])
@token_required
def getprojectGroup(current_user,id=None):
    if request.method == "GET":
        if id != None:
            
            if id.lower() ==  "airtel" :
                arrt=[
                        {
                            '$addFields': {
                                'customerName': {
                                    '$toLower': '$customerName'
                                }
                            }
                        }, {
                            '$match': {
                                'customerName': 'airtel'
                            }
                        }, {
                            '$project': {
                                '_id': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ]
                rett=cmo.finding_aggregate("customer",arrt)['data']
                if len(rett):
                    id = rett[0]['_id']
            
            
            arr=[
                    {
                    '$match': {
                        'customer': ObjectId(id)
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'costCenter': 1, 
                        'costCenterId': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$sort': {
                        'costCenter': 1
                    }
                }
                ]
            Response=cmo.finding_aggregate("costCenter",arr)
            return respond(Response)
        else:
            arr=[
                    # {
                    #     '$match': {
                    #         'empId': current_user['userUniqueId']
                    #     }
                    # }, 
                     {
                    '$project': {
                        '_id': 0, 
                        'costCenter': 1, 
                        'costCenterId': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$sort': {
                        'costCenter': 1
                    }
                }
                ]
            Response=cmo.finding_aggregate("costCenter",arr)
            return respond(Response)
            


@gpTracking_blueprint.route("/gp/zone2",methods=["GET","POST"])
@gpTracking_blueprint.route("/gp/zone2/<id>",methods=["GET","POST"])
@token_required
def getgpZone2(current_user,id=None):
    if request.method == "GET":
        fdfdf=[]
        if id != None:
            fdfdf=[{
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                       'customer': ObjectId(id)
                    }
                }]
            
        arr=fdfdf+[
                 {
                    '$project': {
                        'zone': '$shortCode', 
                        'zoneId': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
        Response=cmo.finding_aggregate("zone",arr)
        return respond(Response)



@gpTracking_blueprint.route("/gp/zone",methods=["GET","POST"])
@gpTracking_blueprint.route("/gp/zone/<id>",methods=["GET","POST"])
@token_required
def getgpZone(current_user,id=None):
    if request.method == "GET":
        fdfdf=[]
        if id != None:
            fdfdf=[{
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                       '_id': ObjectId(id)
                    }
                }]
            
        arr=fdfdf+[
                 {
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
                                '$zoneResult.shortCode', 0
                            ]
                        }, 
                        'zoneId': {
                            '$toString': '$zone'
                        }, 
                        '_id': 0
                    }
                }
            ]
        Response=cmo.finding_aggregate("costCenter",arr)
        return respond(Response)


@gpTracking_blueprint.route("/gp/salaryDB",methods=["GET","POST","PUT","DELETE"])
@gpTracking_blueprint.route("/gp/salaryDB/<id>",methods=["GET","POST","PUT","DELETE"])
@token_required
def salaryDB(current_user,id=None):
    if request.method == "GET":
        arrr=[
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
                    '_id': 0
                }
            }
        ]
        arrr = arrr + apireq.commonarra + apireq.args_pagination(request.args)
        Response=cmo.finding_aggregate("SalaryDB",arrr)
        return respond(Response)
    if request.method == "POST":
        if id == None:
            data=request.get_json()
            if data['customerId']:
                try:
                    data['customer']=ObjectId(data['customerId'])
                except Exception as e:
                    print(e)  
            if data['costCenterId']:
                try:
                    data['costCenter']=ObjectId(data['costCenterId'])
                except Exception as e:
                    print(e)   
            if 'cost' in data:
                if data['cost'] not in ['',None,'undefined']:
                    try:
                        print('wjhujwhujhujhujuhyjuyh',data['cost'])
                        data['cost']=float(data['cost'])
                        print('wjhujwhujhujhujuhyjuyh2',data['cost'])
                        if data['cost'] < 0:
                            return respond(
                                {
                                    "status": 400,
                                    "msg": "Please Enter a Valid Cost Grater Than or Equal to Zero",
                                    "icon": "error",
                                }
                            )
                            
                    except Exception as error:
                        return respond(
                    {
                        "status": 400,
                        "msg": "Please Enter a Valid Cost",
                        "icon": "error",
                    }
                )
                        
                                    
            arr=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            'month': data['month'], 
                            'year': data['year'], 
                            'customer': data['customer'], 
                            'costCenter': data['costCenter']
                        }
                    }
                ]   
            Response2=cmo.finding_aggregate("SalaryDB",arr)
            print(Response2,type(Response2),'huyhkueyge3u3kyg')
            if len(Response2['data']):
                return respond(
                    {
                        "status": 400,
                        "msg": "Cost Already Added for this Month,Year and Cost Center",
                        "icon": "error",
                    }
                )
            Response=cmo.insertion("SalaryDB",data)
            return respond(Response)
        if id != None:
            data=request.get_json()
            if 'cost' in data:
                if data['cost'] not in ['',None,'undefined']:
                    try:
                        data['cost'] = float(data['cost'])
                        if data['cost'] < 0:
                            return respond(
                                {
                                    "status": 400,
                                    "msg": "Please Enter a Valid Cost Grater Than or Equal to Zero",
                                    "icon": "error",
                                }
                            )
                            
                    except Exception as error:
                        return respond(
                    {
                        "status": 400,
                        "msg": "Please Enter a Valid Cost",
                        "icon": "error",
                    }
                )
            Response=cmo.updating("SalaryDB",{"_id":ObjectId(id)},{'cost':data['cost']},False)
            return respond(Response)
    if request.method == "DELETE":
        response = cmo.deleting("SalaryDB", id)
        return respond(response)  
   
        
@gpTracking_blueprint.route("/gp/OtherCostTypes",methods=["GET","POST","PUT","DELETE"]) 
@gpTracking_blueprint.route("/gp/OtherCostTypes/<id>",methods=["GET","POST","PUT","DELETE"])   
@token_required
def  OtherCostTypes(current_user,id=None):
    if request.method == "GET":
        arr=[
            {
                '$project': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0, 
                    'costType': 1,
                    'costTypeId':{
                        '$toString': '$_id'
                    },
                }
            }
        ]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        Response=cmo.finding_aggregate("OtherCostTypes",arr)
        return respond(Response)
    if request.method == "POST":
        if id == None:
            data=request.get_json()
            Response=cmo.insertion("OtherCostTypes",data)
            return respond(Response)
        if id != None:
            data=request.get_json()
            Response=cmo.updating("OtherCostTypes",{'_id':ObjectId(id)},{'costType':data['costType']},False)
            return respond(Response)
    if request.method == "DELETE":
        response = cmo.deleting("OtherCostTypes", id)
        return respond(response)


@gpTracking_blueprint.route("/gp/OtherFixedCost",methods=["GET","POST","PUT","DELETE"])
@gpTracking_blueprint.route("/gp/OtherFixedCost/<id>",methods=["GET","POST","PUT","DELETE"])
@token_required
def OtherFixedCost(current_user,id=None):
    
    if request.method == "GET":
        arrr=[
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
            }
        ]
        arrr = arrr + apireq.commonarra + apireq.args_pagination(request.args)
        Response=cmo.finding_aggregate("OtherFixedCost",arrr)
        return respond(Response)
    if request.method == "POST":
        if id == None:
            data=request.get_json()
            if data['customerId']:
                try:
                    data['customer']=ObjectId(data['customerId'])
                except Exception as e:
                    print(e)  
            if data['costCenterId']:
                try:
                    data['costCenter']=ObjectId(data['costCenterId'])
                except Exception as e:
                    print(e)   
            if data['costTypeId']:
                try:
                    data['costType']=ObjectId(data['costTypeId'])
                except Exception as e:
                    print(e)  
            if 'cost' in data:
                if data['cost'] not in ['',None,'undefined']:
                    try:
                        data['cost']=float(data['cost'])
                        if data['cost'] < 0:
                            return respond(
                                {"status": 400,"msg": "Please Enter a Valid Cost Grater Than or Equal to Zero","icon": "error"})  
                    except Exception as error:
                        return respond(
                                {
                                    "status": 400,
                                    "msg": "Please Enter a Valid Cost",
                                    "icon": "error",
                                }
                            ) 
            arr=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            'month': data['month'], 
                            'year': data['year'], 
                            'customer': data['customer'], 
                            'costCenter': data['costCenter'],
                            'costTypeId':data['costTypeId']
                        }
                    }
                ]   
            Response2=cmo.finding_aggregate("OtherFixedCost",arr)
            
            if len(Response2['data']):
                return respond(
                    {
                        "status": 400,
                        "msg": "Cost Already Added for this Month,Year,Cost Type and Cost Center",
                        "icon": "error",
                    }
                )
            Response=cmo.insertion("OtherFixedCost",data)
            return respond(Response)
        if id != None:
            data=request.get_json()
            if 'cost' in data:
                if data['cost'] not in ['',None,'undefined']:
                    try:
                        data['cost']=float(data['cost'])
                        if data['cost'] < 0:
                            return respond(
                                {"status": 400,"msg": "Please Enter a Valid Cost Grater Than or Equal to Zero","icon": "error"})  
                    except Exception as error:
                        return respond(
                                {
                                    "status": 400,
                                    "msg": "Please Enter a Valid Cost",
                                    "icon": "error",
                                }
                            ) 
            Response=cmo.updating("OtherFixedCost",{"_id":ObjectId(id)},{'cost':data['cost']},False)
            return respond(Response)
    if request.method == "DELETE":
        response = cmo.deleting("OtherFixedCost", id)
        return respond(response) 

   
@gpTracking_blueprint.route("/gpTracking",methods=['GET','POST'])
@token_required
def get_gpTracking(current_user,id=None):
    if request.method == "GET":
        monthss=request.args.get('Month')
        years=request.args.get('year')
        costCenters=request.args.get('costCenter')
        zones=request.args.get('zoneName')
        zoneList=[]
        if zones not in ['',None,'undefined']:
            zoneList=zones.split(",")
        costCentersList=[]
        if costCenters not in ['',None,'undefined']:
            costCentersList=costCenters.split(",")
        customersList =[]
        customers=request.args.get('customer')
        if customers not in ['',None,'undefined']:
            customersList=customers.split(",")
        month_map = {
                1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
            }
        customMonthList=[]
        customYearList=[]
        gggg=getCustomMonthYear()
        daysSpent=gggg[2]
        totalMonthDays=gggg[3]
        if monthss not in ['', 'undefined', None]:
            customMonthList = [int(i) for i in monthss.split(",")]  
        else:
            customMonthList.append(gggg[0]) 
        if years not in ['',None,'undefined']:
            customYearList.append(int(years))
            # customYearList=years.split(",")
        else:
            customYearList.append(gggg[1])
        print(getcccdc(),'888888')
        # workDoneDatadf=toGetWorkDoneData(customersList,customMonthList,customYearList)
        artu=[]
       
        if len(customMonthList):
            artu=artu+[
                {
                '$match': {
                    'Month': {
                        '$in': customMonthList
                    },  
                }
            }
            ]
        if len(customYearList):
            artu=artu+[
                {
                '$match': {
                    
                    'Year': {
                        '$in': customYearList
                    }, 
                    
                }
            }
            ]
        artu=artu+[
                 {
                    '$lookup': {
                        'from': 'costCenter', 
                        'localField': 'costCenterId', 
                        'foreignField': '_id', 
                        'as': 'costCenterResult'
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
                        'Month': 1, 
                        'Year': 1, 
                        '_id': 0, 
                        'customer': {
                            '$arrayElemAt': [
                                '$customerResults.customerName', 0
                            ]
                        }, 
                        'costCenter': {
                            '$arrayElemAt': [
                                '$costCenterResult.costCenter', 0
                            ]
                        }, 
                        'total_Amount': 1,
                        'ApprovedAmount':1,
                        'TotalAmountvendorCost':1
                    }
                }
            ]
        workDoneDatadfResponse=cmo.finding_aggregate("schedulerData",artu)['data'] 
        workDoneDatadf=pd.DataFrame([])
        if len(workDoneDatadfResponse):
            workDoneDatadf = pd.DataFrame(workDoneDatadfResponse)
        arrt1=[]
        if daysSpent<31 and len(customMonthList)== 1 and customMonthList[0]==gggg[0]:
            arrt1=[{
                '$addFields': {
                    'totalSalary': {
                        '$divide': [
                            {
                                '$multiply': [
                                    '$totalSalary', daysSpent
                                ]
                            }, totalMonthDays
                        ]
                    }
                }
            }]   
        arrt=[
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
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'year': '$year', 
                            'month': '$month', 
                            'costCenter': '$costCenter', 
                            'customer': '$customer'
                        }, 
                        'totalcost': {
                            '$sum': '$cost'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        'totalSalary': "$totalcost", 
                        'costCenter': '$_id.costCenter', 
                        'Year': '$_id.year', 
                        'Month': '$_id.month', 
                        'customer': '$_id.customer', 
                        'zone': '$_id.zone', 
                        '_id': 0
                    }
                },
                
            ]
        Response3=cmo.finding_aggregate("SalaryDB",arrt+arrt1)
        salaryDBDf = pd.DataFrame(Response3['data'])
        artty1=[]
        if daysSpent<31 and len(customMonthList)== 1 and customMonthList[0]==gggg[0]:
            artty1=[{
                '$addFields': {
                    'totalOtherFixedCost': {
                        '$divide': [
                            {
                                '$multiply': [
                                    '$totalOtherFixedCost', daysSpent
                                ]
                            }, totalMonthDays
                        ]
                    }
                }
            }]
        artty=[
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
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'year': '$year', 
                            'month': '$month', 
                            'costCenter': '$costCenter', 
                            'customer': '$customer'
                        }, 
                        'totalcost': {
                            '$sum': '$cost'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        'totalOtherFixedCost': "$totalcost", 
                        'costCenter': '$_id.costCenter', 
                        'Year': '$_id.year', 
                        'Month': '$_id.month', 
                        'customer': '$_id.customer', 
                        'zone': '$_id.zone', 
                        '_id': 0
                    }
                }
            ]
        Response4=cmo.finding_aggregate("OtherFixedCost",artty+artty1)
        OtherFixedCostDf = pd.DataFrame(Response4['data'])
        ####logic for Vendor Cost
        
        mainDf=pd.DataFrame([])
        if workDoneDatadf.empty or workDoneDatadf.shape[0] == 0:
            mainDf=mainDf
        else:
            mainDf=workDoneDatadf
        
        if salaryDBDf.empty or salaryDBDf.shape[0] == 0:
            pass
        else:
            if mainDf.empty or mainDf.shape[0] == 0:
                mainDf=salaryDBDf
            else:
                mainDf=pd.merge(mainDf, salaryDBDf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
        
        
        
        # print(mainDf,'mainDfmainDfmainDfmainDfmainDf12345')
        print(getcccdc(),'88888811')
        if OtherFixedCostDf.empty or OtherFixedCostDf.shape[0] == 0:
            pass
        else:
            if mainDf.empty or mainDf.shape[0] == 0:
                mainDf=OtherFixedCostDf
            else:
                mainDf=pd.merge(mainDf, OtherFixedCostDf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer') 
        # print(mainDf,'mainDfmainDfmainDf')
        print(getcccdc(),'88888812')
        if 'costCenter' in mainDf.columns:
            mainDf['uniqueId']=mainDf['costCenter']
        if 'Month' in mainDf:
            mainDf['month']=mainDf['Month']
        if 'Year' in mainDf:
            mainDf['year']=mainDf['Year']
        ## zone Logic
        print(getcccdc(),'88888813')
        artty=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                       
                    }
                }, {
                    '$lookup': {
                        'from': 'customer', 
                        'localField': 'customer', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'status': 'Active'
                                }
                            }
                        ], 
                        'as': 'customerResults'
                    }
                }, {
                    '$lookup': {
                        'from': 'zone', 
                        'localField': 'zone', 
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
                                    'zoneName': '$shortCode'
                                }
                            }
                        ], 
                        'as': 'zoneResult'
                    }
                }, {
                    '$project': {
                        'costCenter': 1, 
                        '_id': 0, 
                        'zone': {
                            '$arrayElemAt': [
                                '$zoneResult.zoneName', 0
                            ]
                        }, 
                        'customer': {
                            '$arrayElemAt': [
                                '$customerResults.customerName', 0
                            ]
                        }
                    }
                }
            ]
        Resup=cmo.finding_aggregate("costCenter",artty)['data']
        zonedf=pd.DataFrame(Resup)
        
        mainDf = pd.merge(mainDf, zonedf, on=['customer','costCenter'], how='left')
        
        # expenseDf=expenseDf[expenseDf['projectId'].isin(projectDf['projectId'])]
        if 'ApprovedAmount' not in mainDf.columns:
            mainDf["ApprovedAmount"] = None

        mainDf['COGS']=mainDf['ApprovedAmount'].fillna(0)
        if 'totalSalary' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['totalSalary'].fillna(0)
        if 'TotalAmountvendorCost' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['TotalAmountvendorCost'].fillna(0)
        if 'totalOtherFixedCost' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['totalOtherFixedCost'].fillna(0)
              
        # mainDf['COGS']=mainDf['ApprovedAmount'].fillna(0)+mainDf['totalSalary'].fillna(0)+mainDf['TotalAmountvendorCost'].fillna(0)+mainDf['totalOtherFixedCost'].fillna(0)
        if 'total_Amount' not in mainDf:
            mainDf['total_Amount'] = None   
        mainDf['GROSSPROFITINR'] = mainDf['total_Amount'].fillna(0) - (mainDf['COGS'].fillna(0))
        mainDf['GPRevenuePercentage'] = (mainDf['GROSSPROFITINR'].fillna(0) / mainDf['total_Amount'].fillna(0)) * 100
        # print(mainDf.columns,'whyhywhhjhjehjyyegyyu')
        ###Filters
        # print('mainDfmainDfmainDfmainDfmainDf123456',mainDf)
        if len(customMonthList):
            mainDf = mainDf[mainDf['Month'].isin(customMonthList)]
        if len(customYearList):
            mainDf = mainDf[mainDf['Year'].isin(customYearList)]
        # print('mainDfmainDfmainDfmainDfmainDf12345678',mainDf.columns)
        if len(zoneList):
            mainDf = mainDf[mainDf['zone'].isin(zoneList)]
        if len(costCentersList):
            mainDf = mainDf[mainDf['costCenter'].isin(costCentersList)]
        if len(customersList):
            mainDf = mainDf[mainDf['customer'].isin(customersList)]
        artyi=[
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
                                'as': 'customerResults'
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
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$project': {
                                'costCenter': {
                                    '$arrayElemAt': [
                                        '$result.costCenter', 0
                                    ]
                                }, 
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customerResults.customerName', 0
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
                                '_id': {
                                    'customer': '$customer', 
                                    'costCenter': '$costCenter'
                                }
                            }
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$_id'
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
                '$sort': {
                    'costCenter': 1
                }
            }
        ]
        Resty=cmo.finding_aggregate("projectAllocation",artyi)['data']
        if len(Resty):
            projectAlloctionDf=pd.DataFrame(Resty)
            mainDf=pd.merge(projectAlloctionDf,mainDf,on=['costCenter','customer'],how='inner')
        mainDf['Month'] = mainDf['Month'].map(month_map)
          
        columnsLPA = ['totalOtherFixedCost', 'ApprovedAmount', 'COGS', 'TotalAmount','TotalAmountvendorCost', 'totalSalary', 'total_Amount','GROSSPROFITINR']
        for col in columnsLPA:
            if col in mainDf.columns:
                mainDf[col] = mainDf[col].apply(
                    lambda x: round(float(x) / 100000, 2) if pd.notna(x) and x != '' else 0
                )
        mainDf['uniqueId']=1
        json_Data = json.loads(mainDf.to_json(orient='records'))
        return respond({
            'status':200,
            "msg":'Data Get Successfully',
            "data":json_Data
        }) 
        
@gpTracking_blueprint.route("/export/gpTracking",methods=['GET','POST'])
@token_required
def get_gpTracking_export(current_user,id=None):
    if request.method == "POST":
        data=request.get_json()
        
        monthss=None
        years=None
        costCenters=None
        customers=None
        costCentersList=[]
        if 'Month' in data:
            monthss=data['Month'].split(",")
        if 'year' in data:
            years=data['year']
        if 'costCenter' in data:
            if len(data['costCenter']):
                for i in data['costCenter']:
                    if i != None:
                        costCentersList.append(i)
        if 'customer' in data:
            customers=data['customer']
        
        
        customersList =[]
        zones=[]
        
        zoneList=[]
        if 'zoneName' in data:
            if len(data['zoneName']):
                for i in data['zoneName']:
                    if i != None:
                        zoneList.append(i)
        
          
        if customers not in ['',None,'undefined']:
            customersList=customers.split(",")
        month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        customMonthList=[]
        customYearList=[]
        gggg=getCustomMonthYear()
        
        daysSpent=gggg[2]
        totalMonthDays=gggg[3]
        if monthss not in ['', 'undefined', None]:
            customMonthList = [int(i) for i in monthss]
        else:
            customMonthList.append(gggg[0]) 
        
        if years not in ['',None,'undefined']:
            customYearList.append(int(years))
            # customYearList=years.split(",")
        else:
            customYearList.append(gggg[1])
        
        artu=[]
        
        if len(customMonthList):
            artu=artu+[
                {
                '$match': {
                    'Month': {
                        '$in': customMonthList
                    },  
                }
            }
            ]
        if len(customYearList):
            artu=artu+[
                {
                '$match': {
                    
                    'Year': {
                        '$in': customYearList
                    }, 
                    
                }
            }
            ]
        artu=artu+[
                 {
                    '$lookup': {
                        'from': 'costCenter', 
                        'localField': 'costCenterId', 
                        'foreignField': '_id', 
                        'as': 'costCenterResult'
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
                        'Month': 1, 
                        'Year': 1, 
                        '_id': 0, 
                        'customer': {
                            '$arrayElemAt': [
                                '$customerResults.customerName', 0
                            ]
                        }, 
                        'costCenter': {
                            '$arrayElemAt': [
                                '$costCenterResult.costCenter', 0
                            ]
                        }, 
                        'total_Amount': 1,
                        'ApprovedAmount':1,
                        'TotalAmountvendorCost':1
                    }
                }
            ]
        
        workDoneDatadfResponse=cmo.finding_aggregate("schedulerData",artu)['data'] 
        workDoneDatadf=pd.DataFrame([])
        if len(workDoneDatadfResponse):
            workDoneDatadf = pd.DataFrame(workDoneDatadfResponse)
            workDoneDatadf = workDoneDatadf.groupby(['Month', 'Year', 'customer', 'costCenter'])[['total_Amount', 'ApprovedAmount', 'TotalAmountvendorCost']].sum().reset_index()
            print(workDoneDatadf['total_Amount'],workDoneDatadf['ApprovedAmount'],workDoneDatadf['TotalAmountvendorCost'],'workDoneDatadfworkDoneDatadfworkDoneDatadfworkDoneDatadfworkDoneDatadf')
        
        arrt1=[]
        if daysSpent<31 and len(customMonthList)== 1 and customMonthList[0]==gggg[0]:
            arrt1=[{
                '$addFields': {
                    'totalSalary': {
                        '$divide': [
                            {
                                '$multiply': [
                                    '$totalSalary', daysSpent
                                ]
                            }, totalMonthDays
                        ]
                    }
                }
            }]   
        arrt=[
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
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'year': '$year', 
                            'month': '$month', 
                            'costCenter': '$costCenter', 
                            'customer': '$customer'
                        }, 
                        'totalcost': {
                            '$sum': '$cost'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        'totalSalary': "$totalcost", 
                        'costCenter': '$_id.costCenter', 
                        'Year': '$_id.year', 
                        'Month': '$_id.month', 
                        'customer': '$_id.customer', 
                        'zone': '$_id.zone', 
                        '_id': 0
                    }
                },
                
            ]
        Response3=cmo.finding_aggregate("SalaryDB",arrt+arrt1)
        salaryDBDf = pd.DataFrame(Response3['data'])
        artty1=[]
        if daysSpent<31 and len(customMonthList)== 1 and customMonthList[0]==gggg[0]:
            artty1=[{
                '$addFields': {
                    'totalOtherFixedCost': {
                        '$divide': [
                            {
                                '$multiply': [
                                    '$totalOtherFixedCost', daysSpent
                                ]
                            }, totalMonthDays
                        ]
                    }
                }
            }]
        artty=[
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
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'year': '$year', 
                            'month': '$month', 
                            'costCenter': '$costCenter', 
                            'customer': '$customer'
                        }, 
                        'totalcost': {
                            '$sum': '$cost'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        'totalOtherFixedCost': "$totalcost", 
                        'costCenter': '$_id.costCenter', 
                        'Year': '$_id.year', 
                        'Month': '$_id.month', 
                        'customer': '$_id.customer', 
                        'zone': '$_id.zone', 
                        '_id': 0
                    }
                }
            ]
        Response4=cmo.finding_aggregate("OtherFixedCost",artty+artty1)
        OtherFixedCostDf = pd.DataFrame(Response4['data'])
        
        ####logic for Vendor Cost
        
        mainDf=pd.DataFrame([])
        if workDoneDatadf.empty or workDoneDatadf.shape[0] == 0:
            mainDf=mainDf
        else:
            mainDf=workDoneDatadf
        if salaryDBDf.empty or salaryDBDf.shape[0] == 0:
            pass
        else:
            if mainDf.empty or mainDf.shape[0] == 0:
                mainDf=salaryDBDf
            else:
                mainDf=pd.merge(mainDf, salaryDBDf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
        
        
        print(mainDf,'mainDfmainDfmainDfmainDfmainDf12345')
        
        if OtherFixedCostDf.empty or OtherFixedCostDf.shape[0] == 0:
            pass
        else:
            if mainDf.empty or mainDf.shape[0] == 0:
                mainDf=OtherFixedCostDf
            else:
                mainDf=pd.merge(mainDf, OtherFixedCostDf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer') 
        
        if 'costCenter' in mainDf.columns:
            mainDf['uniqueId']=mainDf['costCenter']
        if 'Month' in mainDf:
            mainDf['month']=mainDf['Month']
        if 'Year' in mainDf:
            mainDf['year']=mainDf['Year']
        ## zone Logic
        
        artty=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        
                    }
                }, {
                    '$lookup': {
                        'from': 'customer', 
                        'localField': 'customer', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'status': 'Active'
                                }
                            }
                        ], 
                        'as': 'customerResults'
                    }
                }, {
                    '$lookup': {
                        'from': 'zone', 
                        'localField': 'zone', 
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
                                    'zoneName': '$shortCode'
                                }
                            }
                        ], 
                        'as': 'zoneResult'
                    }
                }, {
                    '$project': {
                        'costCenter': 1, 
                        '_id': 0, 
                        'zone': {
                            '$arrayElemAt': [
                                '$zoneResult.zoneName', 0
                            ]
                        }, 
                        'customer': {
                            '$arrayElemAt': [
                                '$customerResults.customerName', 0
                            ]
                        }
                    }
                }
            ]
        Resup=cmo.finding_aggregate("costCenter",artty)['data']
        zonedf=pd.DataFrame(Resup)
        
        mainDf = pd.merge(mainDf, zonedf, on=['customer','costCenter'], how='left')
        
        # expenseDf=expenseDf[expenseDf['projectId'].isin(projectDf['projectId'])]
        if 'ApprovedAmount' not in mainDf.columns:
            mainDf["ApprovedAmount"] = None
        print(getcccdc(),'djkidjkjdkjdjj12311')    
        mainDf['COGS']=mainDf['ApprovedAmount'].fillna(0)
        if 'totalSalary' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['totalSalary'].fillna(0)
        if 'TotalAmountvendorCost' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['TotalAmountvendorCost'].fillna(0)
        if 'totalOtherFixedCost' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['totalOtherFixedCost'].fillna(0)
        print(getcccdc(),'djkidjkjdkjdjj12312')         
        # mainDf['COGS']=mainDf['ApprovedAmount'].fillna(0)+mainDf['totalSalary'].fillna(0)+mainDf['TotalAmountvendorCost'].fillna(0)+mainDf['totalOtherFixedCost'].fillna(0)
        if 'total_Amount' not in mainDf:
            mainDf['total_Amount'] = None
            
        mainDf['GROSSPROFITINR'] = mainDf['total_Amount'].fillna(0) - (mainDf['COGS'].fillna(0))
        mainDf['GPRevenuePercentage'] = (mainDf['GROSSPROFITINR'].fillna(0) / mainDf['total_Amount'].fillna(0)) * 100
        
        if len(customMonthList):
            mainDf = mainDf[mainDf['Month'].isin(customMonthList)]
        if len(customYearList):
            mainDf = mainDf[mainDf['Year'].isin(customYearList)]
        if len(zoneList):
            mainDf = mainDf[mainDf['zone'].isin(zoneList)]
        if len(costCentersList):
            mainDf = mainDf[mainDf['costCenter'].isin(costCentersList)]
        if len(customersList):
            mainDf = mainDf[mainDf['customer'].isin(customersList)]
        
        # mainDf = mainDf[mainDf['customer'].isin(customYearList)]
        artyi=[
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
                                'as': 'customerResults'
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
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'result'
                            }
                        }, {
                            '$project': {
                                'costCenter': {
                                    '$arrayElemAt': [
                                        '$result.costCenter', 0
                                    ]
                                }, 
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customerResults.customerName', 0
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
                                '_id': {
                                    'customer': '$customer', 
                                    'costCenter': '$costCenter'
                                }
                            }
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$_id'
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
                '$sort': {
                    'costCenter': 1
                }
            }
        ]
        Resty=cmo.finding_aggregate("projectAllocation",artyi)['data']
        if len(Resty):
            projectAlloctionDf=pd.DataFrame(Resty)
            mainDf=pd.merge(projectAlloctionDf,mainDf,on=['costCenter','customer'],how='inner')
        
        mainDf['Month'] = mainDf['Month'].map(month_map)
        columnsLPA = ['totalOtherFixedCost', 'ApprovedAmount', 'COGS', 'TotalAmount','TotalAmountvendorCost', 'totalSalary', 'total_Amount','GROSSPROFITINR']
        for col in columnsLPA:
            if col in mainDf.columns:
                mainDf[col] = mainDf[col].apply(
                    lambda x: round(float(x) / 100000, 2) if pd.notna(x) and x != '' else 0
                )
        
        
        # workDoneDatadf=toGetWorkDoneData(customersList,customMonthList,customYearList)
        
        NewDf = pd.DataFrame()
        NewDf['Year']=mainDf['Year']
        NewDf['Month']=mainDf['Month']
        NewDf['Customer']=mainDf['customer']
        NewDf['Cost Center']=mainDf['costCenter']
        
        NewDf['Revenue']=mainDf['total_Amount']
        NewDf['Salary']=mainDf['totalSalary']
        NewDf['Vendor Cost']=mainDf['TotalAmountvendorCost']
        if 'totalOtherFixedCost' in mainDf: 
            NewDf['Other Fixed Cost']=mainDf['totalOtherFixedCost']
        NewDf['Employee Expanse']=mainDf['ApprovedAmount']
        NewDf['COGS']=mainDf['COGS']
        NewDf['Gross Profit(INR)']=mainDf['GROSSPROFITINR']
        NewDf['GROSS MARGIN (%)']=mainDf['GPRevenuePercentage']
        NewDf['GROSS MARGIN (%)'] = NewDf['GROSS MARGIN (%)'].apply(
            lambda x: f"{round(x, 2)} %" if pd.notna(x) and x != '' and x != 0 else str(x)
        )
        fullPath = excelWriteFunc.excelFileWriter(
            NewDf, "Export_GP_Tracking", "GP Tracking"
        )
        # print("fullPathfullPathfullPath", fullPath)
        return send_file(fullPath)
    
        
   


@gpTracking_blueprint.route("/export/vendorTracking",methods=['GET','POST'])
@token_required
def get_vendorTracking_export(current_user,id=None):
    if request.method == "POST":
        customMonthList=[1,2,3,4,5,6,7,8,9,10,11,12]
        customYearList=[2022,2023,2024,2025]
        month_map = {
                1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
            }
        gggg=getCustomMonthYear()
        daysSpent=gggg[2]
        totalMonthDays=gggg[3]
        customMonthList.append(gggg[0])
        customYearList.append(gggg[1])
        customersList=[]
        workDoneDatadf=toGetWorkDoneData(customersList,customMonthList,customYearList)
        expenseDf=toGetExpensesAccordingtoproject(current_user['userUniqueId'],customMonthList,customYearList)
        arrt1=[]
        # if daysSpent<30:
        arrt1=[{
            '$addFields': {
                'totalSalary': {
                    '$divide': [
                        {
                            '$multiply': [
                                '$totalSalary', daysSpent
                            ]
                        }, totalMonthDays
                    ]
                }
            }
        }]
            
        arrt=[
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
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'year': '$year', 
                            'month': '$month', 
                            'costCenter': '$costCenter', 
                            'customer': '$customer'
                        }, 
                        'totalcost': {
                            '$sum': '$cost'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        'totalSalary': "$totalcost", 
                        'costCenter': '$_id.costCenter', 
                        'Year': '$_id.year', 
                        'Month': '$_id.month', 
                        'customer': '$_id.customer', 
                        'zone': '$_id.zone', 
                        '_id': 0
                    }
                },
                
            ]
        Response3=cmo.finding_aggregate("SalaryDB",arrt+arrt1)
        salaryDBDf = pd.DataFrame(Response3['data'])
        artty1=[]
        # if daysSpent<30:
        artty1=[{
            '$addFields': {
                'totalOtherFixedCost': {
                    '$divide': [
                        {
                            '$multiply': [
                                '$totalOtherFixedCost', daysSpent
                            ]
                        }, totalMonthDays
                    ]
                }
            }
        }]
        artty=[
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
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'year': '$year', 
                            'month': '$month', 
                            'costCenter': '$costCenter', 
                            'customer': '$customer'
                        }, 
                        'totalcost': {
                            '$sum': '$cost'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                }, {
                    '$project': {
                        'totalOtherFixedCost': "$totalcost", 
                        'costCenter': '$_id.costCenter', 
                        'Year': '$_id.year', 
                        'Month': '$_id.month', 
                        'customer': '$_id.customer', 
                        'zone': '$_id.zone', 
                        '_id': 0
                    }
                }
            ]
        Response4=cmo.finding_aggregate("OtherFixedCost",artty+artty1)
        # print('tttt4',getcccdc())
        OtherFixedCostDf = pd.DataFrame(Response4['data'])
        
        ####logic for Vendor Cost
        arty=[
            {
                '$lookup': {
                    'let': {
                        'subProjectId': {
                            '$toString': '$subProjectId'
                        }, 
                        'projectGroupId': '$projectGroup', 
                        'customerId': '$customerId', 
                        'milestoneList': '$milestoneList'
                    }, 
                    'from': 'milestone', 
                    'localField': 'vendorId', 
                    'foreignField': 'assignerId', 
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$typeAssigned', 'Partner'
                                            ]
                                        }, {
                                            '$ne': [
                                                '$deleteStatus', 1
                                            ]
                                        }, {
                                            '$eq': [
                                                '$mileStoneStatus', 'Closed'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$SubProjectId', '$$subProjectId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'Name': 1, 
                                'siteId': 1, 
                                'projectuniqueId': 1, 
                                'SubProjectId': 1, 
                                'projectuniqueId': {
                                    '$toObjectId': '$projectuniqueId'
                                }, 
                                'assignerId': 1, 
                                'milestoneClosingDate': {
                                    '$toDate': '$CC_Completion Date'
                                }
                            }
                        }, {
                            '$addFields': {
                                'actionAtKolkata': {
                                    '$dateAdd': {
                                        'startDate': '$milestoneClosingDate', 
                                        'unit': 'minute', 
                                        'amount': 330
                                    }
                                }, 
                                'dayOfMonth': {
                                    '$dayOfMonth': {
                                        'date': {
                                            '$dateAdd': {
                                                'startDate': '$milestoneClosingDate', 
                                                'unit': 'minute', 
                                                'amount': 330
                                            }
                                        }
                                    }
                                }
                            }
                        }, {
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
                                }, 
                                'year': {
                                    '$cond': {
                                        'if': {
                                            '$gt': [
                                                {
                                                    '$dateToString': {
                                                        'format': '%m-%d', 
                                                        'date': '$milestoneClosingDate'
                                                    }
                                                }, '12-25'
                                            ]
                                        }, 
                                        'then': {
                                            '$add': [
                                                {
                                                    '$year': '$milestoneClosingDate'
                                                }, 1
                                            ]
                                        }, 
                                        'else': {
                                            '$year': '$milestoneClosingDate'
                                        }
                                    }
                                }
                            }
                        }, {
                            '$group': {
                                '_id': {
                                    'assignerId': '$assignerId', 
                                    'siteId': '$siteId'
                                }, 
                                'data': {
                                    '$addToSet': '$$ROOT'
                                }, 
                                'data2': {
                                    '$addToSet': '$Name'
                                }
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$setIsSubset': [
                                                '$$milestoneList', '$data2'
                                            ]
                                        }, {
                                            '$setIsSubset': [
                                                '$data2', '$$milestoneList'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$addFields': {
                                'Year': {
                                    '$arrayElemAt': [
                                        '$data.year', 0
                                    ]
                                }, 
                                'Month': {
                                    '$arrayElemAt': [
                                        '$data.month', 0
                                    ]
                                }, 
                                'SubProjectId': {
                                    '$arrayElemAt': [
                                        '$data.SubProjectId', 0
                                    ]
                                }, 
                                'projectuniqueId': {
                                    '$arrayElemAt': [
                                        '$data.projectuniqueId', 0
                                    ]
                                }, 
                                'siteId': {
                                    '$arrayElemAt': [
                                        '$data.siteId', 0
                                    ]
                                }
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
                                                        'costCenter': 1, 
                                                        'costCenterId': 1
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
                                            }, 
                                            'costCenter': {
                                                '$arrayElemAt': [
                                                    '$projectGroupResult.costCenter', 0
                                                ]
                                            }, 
                                            'costCenterId': {
                                                '$arrayElemAt': [
                                                    '$projectGroupResult.costCenterId', 0
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
                                            'customerId': 1, 
                                            'costCenter': 1, 
                                            'costCenterId': 1
                                        }
                                    }
                                ], 
                                'as': 'projectResults'
                            }
                        }, {
                            '$addFields': {
                                'projectGroupId': {
                                    '$arrayElemAt': [
                                        '$projectResults.projectGroupId', 0
                                    ]
                                }, 
                                'projectId': {
                                    '$arrayElemAt': [
                                        '$projectResults.projectId', 0
                                    ]
                                }, 
                                'customerId': {
                                    '$arrayElemAt': [
                                        '$projectResults.customerId', 0
                                    ]
                                }, 
                                'projectGroupName': {
                                    '$arrayElemAt': [
                                        '$projectResults.projectGroupName', 0
                                    ]
                                }, 
                                'projectGroupId': {
                                    '$arrayElemAt': [
                                        '$projectResults.projectGroupId', 0
                                    ]
                                }, 
                                'Customer': {
                                    '$arrayElemAt': [
                                        '$projectResults.Customer', 0
                                    ]
                                }, 
                                'costCenter': {
                                    '$arrayElemAt': [
                                        '$projectResults.costCenter', 0
                                    ]
                                }, 
                                'costCenterId': {
                                    '$arrayElemAt': [
                                        '$projectResults.costCenterId', 0
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$projectGroupId', '$$projectGroupId'
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
                    'as': 'milestoneResults'
                }
            }, {
                '$match': {
                    'milestoneResults': {
                        '$ne': []
                    }
                }
            }, {
                '$unwind': {
                    'path': '$milestoneResults'
                }
            }, {
                '$addFields': {
                    'Year': '$milestoneResults.Year', 
                    'Month': '$milestoneResults.Month', 
                    'Customer': '$milestoneResults.Customer', 
                    'costCenter': '$milestoneResults.costCenter', 
                    'projectGroupName': '$milestoneResults.projectGroupName', 
                    # 'rateInt': {
                    #     '$cond': {
                    #         'if': {
                    #             '$regexMatch': {
                    #                 'input': '$rate', 
                    #                 'regex': re.compile(r"^\\d+$")
                    #             }
                    #         }, 
                    #         'then': {
                    #             '$toInt': '$rate'
                    #         }, 
                    #         'else': 0
                    #     }
                    # }
                    "rate":{
                    "$toInt":"$rate"
                }
                }
            }, {
                '$group': {
                    '_id': {
                        'Customer': '$Customer', 
                        'costCenter': '$costCenter', 
                        'Year': '$Year', 
                        'Month': '$Month'
                    }, 
                    'TotalAmount': {
                        '$sum': '$rate'
                    }
                }
            }, {
                '$addFields': {
                    'Month': '$_id.Month', 
                    'Customer': '$_id.Customer', 
                    'costCenter': '$_id.costCenter', 
                    'Year': '$_id.Year', 
                    'TotalAmountvendorCost': "$TotalAmount"
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        Responser4=cmo.finding_aggregate("vendorCostMilestone",arty)['data']
        vendorCost=pd.DataFrame(Responser4)
        # print('tttt5',getcccdc())
        if workDoneDatadf.empty or workDoneDatadf.shape[0] == 0:
            pass
        else:
            mainDf=pd.merge(expenseDf, workDoneDatadf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
        # print(mainDf,'mainDfmainDfmainDfmainDfmainDf123')
        if salaryDBDf.empty or salaryDBDf.shape[0] == 0:
            pass
        else:
            mainDf=pd.merge(mainDf, salaryDBDf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
        # print(mainDf,'mainDfmainDfmainDfmainDfmainDf1234')
        if vendorCost.empty or vendorCost.shape[0] == 0:
            pass
        else:
            vendorCost['customer']=vendorCost['Customer']
            vendorCost = vendorCost.drop('Customer', axis=1)
            mainDf=pd.merge(mainDf, vendorCost, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
        
        
        if OtherFixedCostDf.empty or OtherFixedCostDf.shape[0] == 0:
            pass
        else:
            mainDf=pd.merge(mainDf, OtherFixedCostDf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
        # print(mainDf,'mainDfmainDfmainDfmainDfmainDf123456')
        # mainDf['uniqueId']=mainDf['costCenter']
        mainDf['month']=mainDf['Month']
        mainDf['year']=mainDf['Year']
        ## zone Logi
        
        artty=[
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
                                'zoneName': "$shortCode"
                            }
                        }
                    ], 
                    'as': 'zoneResult'
                }
            }, {
                '$project': {
                    'costCenter': 1, 
                    '_id': 0, 
                    'zone': {
                        '$arrayElemAt': [
                            '$zoneResult.zoneName', 0
                        ]
                    }
                }
            }
        ]
        Resup=cmo.finding_aggregate("costCenter",artty)['data']
        zonedf=pd.DataFrame(Resup)
        # print(zonedf,'dhhjdhjdhhyjhyjzonedf')
        # print(mainDf,'mainDfmainDfmainDfmainDfJKHDYDHUUYH')
        mainDf = pd.merge(mainDf, zonedf, on='costCenter', how='left')
        # expenseDf=expenseDf[expenseDf['projectId'].isin(projectDf['projectId'])]
        mainDf['COGS']=mainDf['ApprovedAmount'].fillna(0)
        if 'totalSalary' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['totalSalary'].fillna(0)
        if 'TotalAmountvendorCost' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['TotalAmountvendorCost'].fillna(0)
        if 'totalOtherFixedCost' in mainDf.columns:
            mainDf['COGS']=mainDf['COGS']+mainDf['totalOtherFixedCost'].fillna(0)
                
        # mainDf['COGS']=mainDf['ApprovedAmount'].fillna(0)+mainDf['totalSalary'].fillna(0)+mainDf['TotalAmountvendorCost'].fillna(0)+mainDf['totalOtherFixedCost'].fillna(0)
        mainDf['GROSSPROFITINR'] = mainDf['total_Amount'].fillna(0) - (mainDf['COGS'].fillna(0))
        mainDf['GPRevenuePercentage'] = (mainDf['GROSSPROFITINR'].fillna(0) / mainDf['total_Amount'].fillna(0)) * 100
        
        if 'total_Amount' in mainDf.columns:
            mainDf['total_Amount'] = mainDf['total_Amount'].apply(lambda x: round(x, 2) if x is not None else 0)

        if 'totalSalary' in mainDf.columns:
            mainDf['totalSalary'] = mainDf['totalSalary'].apply(lambda x: round(x, 2) if x is not None else 0)

        if 'TotalAmountvendorCost' in mainDf.columns:
            mainDf['TotalAmountvendorCost'] = mainDf['TotalAmountvendorCost'].apply(lambda x: round(x, 2) if x is not None else 0)

        if 'totalOtherFixedCost' in mainDf.columns:
            mainDf['totalOtherFixedCost'] = mainDf['totalOtherFixedCost'].apply(lambda x: round(x, 2) if x is not None else 0)

        if 'ApprovedAmount' in mainDf.columns:
            mainDf['ApprovedAmount'] = mainDf['ApprovedAmount'].apply(lambda x: round(x, 2) if x is not None else 0)

        if 'COGS' in mainDf.columns:
            mainDf['COGS'] = mainDf['COGS'].apply(lambda x: round(x, 2) if x is not None else 0)

        if 'GROSSPROFITINR' in mainDf.columns:
            mainDf['GROSSPROFITINR'] = mainDf['GROSSPROFITINR'].apply(lambda x: f"₹ {round(x, 2)}" if x is not None else "₹ 0")

        if 'GPRevenuePercentage' in mainDf.columns:
            mainDf['GPRevenuePercentage'] = mainDf['GPRevenuePercentage'].apply(
                lambda x: f"{round(x, 2)} %" if pd.notnull(x) and x != float('-inf') else "0 %"
            )
        mainDf['Month'] = mainDf['Month'].map(month_map)
        
        NewDf = pd.DataFrame()
        NewDf['Year']=mainDf['Year']
        NewDf['Month']=mainDf['Month']
        NewDf['Customer']=mainDf['customer']
        NewDf['Cost Center']=mainDf['costCenter']
        
        NewDf['Revenue']=mainDf['total_Amount']
        NewDf['Salary']=mainDf['totalSalary']
        NewDf['Vendor Cost']=mainDf['TotalAmountvendorCost']
        NewDf['Other Fixed Cost']=mainDf['totalOtherFixedCost']
        NewDf['Employee Expanse']=mainDf['ApprovedAmount']
        NewDf['COGS']=mainDf['COGS']
        NewDf['Gross Profit(INR)']=mainDf['GROSSPROFITINR']
        NewDf['GROSS MARGIN (%)']=mainDf['GPRevenuePercentage']
        fullPath = excelWriteFunc.excelFileWriter(
            NewDf, "Export_GP_Tracking", "GP Tracking"
        )
        # print("fullPathfullPathfullPath", fullPath)
        return send_file(fullPath)

        
        
        
        

def getAOPExpenseANDRevenue(current_user,id=None,customMonthList=[1,2,3,4,5,6,7,8,9,10,11,12],customYearList=[2022,2023,2024,2025]):
    month_map = {
            1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
            7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
        }
    gggg=getCustomMonthYear()
    daysSpent=gggg[2]
    totalMonthDays=gggg[3]
    customMonthList.append(gggg[0])
    customYearList.append(gggg[1])
    customersList=[]
    workDoneDatadf=toGetWorkDoneData(customersList,customMonthList,customYearList)
    expenseDf=toGetExpensesAccordingtoproject(current_user['userUniqueId'],customMonthList,customYearList)
    if workDoneDatadf.empty or workDoneDatadf.shape[0] == 0:
        pass
    else:
        mainDf=pd.merge(expenseDf, workDoneDatadf, on=['Month', 'Year', 'costCenter', 'customer'], how='outer')
    
    mainDf['month']=mainDf['Month']
    mainDf['year']=mainDf['Year']
    ## zone Logi
    
    artty=[
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
                            'zoneName': "$shortCode"
                        }
                    }
                ], 
                'as': 'zoneResult'
            }
        }, {
            '$project': {
                'costCenter': 1, 
                '_id': 0, 
                'zone': {
                    '$arrayElemAt': [
                        '$zoneResult.zoneName', 0
                    ]
                }
            }
        }
    ]
    Resup=cmo.finding_aggregate("costCenter",artty)['data']
    zonedf=pd.DataFrame(Resup)
    mainDf = pd.merge(mainDf, zonedf, on='costCenter', how='left')
    # expenseDf=expenseDf[expenseDf['projectId'].isin(projectDf['projectId'])]
    mainDf['ExpensesApproved']=mainDf['ApprovedAmount'].fillna(0)
    mainDf['Month'] = mainDf['Month'].map(month_map)
    
    NewDf = pd.DataFrame()
    NewDf['Year']=mainDf['Year']
    NewDf['Month']=mainDf['Month']
    NewDf['Customer']=mainDf['customer']
    NewDf['Cost Center']=mainDf['costCenter']
    
    NewDf['Revenue']=mainDf['total_Amount']
    # NewDf['Salary']=mainDf['totalSalary']
    # NewDf['Vendor Cost']=mainDf['TotalAmountvendorCost']
    # NewDf['Other Fixed Cost']=mainDf['totalOtherFixedCost']
    NewDf['Employee Expanse']=mainDf['ExpensesApproved']
    return NewDf



          