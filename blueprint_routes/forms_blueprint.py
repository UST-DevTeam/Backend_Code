from datetime import datetime
from base import *
import calendar
import common.excel_write as excelWriteFunc
from blueprint_routes.currentuser_blueprint import projectId_str,projectGroup_str,projectId_Object,costCenter_str
from blueprint_routes.gpTracking import getAOPExpenseANDRevenue as getAOPExpenseANDRevenue
forms_blueprint = Blueprint("forms_blueprint", __name__)


def get_next_six_months():
    today = datetime.today()
    months = []
    for i in range(6): 
        month_year = today.replace(month=((today.month + i - 1) % 12) + 1, year=today.year + ((today.month + i - 1) // 12))
        months.append(f"{calendar.month_abbr[month_year.month]}-{month_year.year}")
    return months

def current_year():
    india_tz = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(india_tz)
    current_year = current_date.year
    return current_year

def current_month():
    india_tz = pytz.timezone('Asia/Kolkata')
    return datetime.now(india_tz).month

def get_progressive_data(data, months):
    
    accumulated_data = []
    cumulative_totals = {}
    for month in months:
       
        filtered_data = [item for item in data if item['month'] == month]
        
        for item in filtered_data:
            key = (item.get('customerName',""), item.get('costCenter',""), item.get('year',""),item.get('customerId',""),item.get('customerId',""))
            if key not in cumulative_totals:
                cumulative_totals[key] = {
                    'customerName': item.get('customerName',""),
                    'costCenter':  item.get('costCenter',""),
                    'actualRevenue': 0,
                    'COGS': 0,
                    'SGNA':0,
                    'planRevenue':0,
                    'actualSalary':0,
                    'actualVendorCost':0,
                    'otherFixedCost':0,
                    'employeeExpanse':0,
                    'miscellaneousExpenses':0,
                    'miscellaneousExpensesSecond':0,
                    'planGp':0,
                    'actualCOGS':0,
                    'actualGp':0,
                    'year': item['year'],
                    'actualSGNA':0
                }
            cumulative_totals[key]['actualRevenue'] += item.get('actualRevenue', 0) if item.get('actualRevenue') is not None else 0
            cumulative_totals[key]['COGS'] += item.get('COGS', 0) if item.get('COGS') is not None else 0
            cumulative_totals[key]['SGNA'] += item.get('SGNA', 0) if item.get('SGNA') is not None else 0
            cumulative_totals[key]['planRevenue'] += item.get('planRevenue', 0) if item.get('planRevenue') is not None else 0
            cumulative_totals[key]['actualSalary'] += item.get('actualSalary', 0) if item.get('actualSalary') is not None else 0
            cumulative_totals[key]['actualVendorCost'] += item.get('actualVendorCost', 0) if item.get('actualVendorCost') is not None else 0
            cumulative_totals[key]['otherFixedCost'] += item.get('otherFixedCost', 0) if item.get('otherFixedCost') is not None else 0
            cumulative_totals[key]['employeeExpanse'] += item.get('employeeExpanse', 0) if item.get('employeeExpanse') is not None else 0
            cumulative_totals[key]['miscellaneousExpenses'] += item.get('miscellaneousExpenses', 0) if item.get('miscellaneousExpenses') is not None else 0
            cumulative_totals[key]['miscellaneousExpensesSecond'] += item.get('miscellaneousExpensesSecond', 0) if item.get('miscellaneousExpensesSecond') is not None else 0
            cumulative_totals[key]['planGp'] += item.get('planGp', 0) if item.get('planGp') is not None else 0
            cumulative_totals[key]['actualCOGS'] += item.get('actualCOGS', 0) if item.get('actualCOGS') is not None else 0
            cumulative_totals[key]['actualGp'] += item.get('actualGp', 0) if item.get('actualGp') is not None else 0
            cumulative_totals[key]['actualSGNA'] += item.get('actualSGNA', 0) if item.get('actualSGNA') is not None else 0
            # cumulative_totals[key]['miscellaneousExpensesSecond'] += item['COGS']
            accumulated_data.append({
                'customerName': item.get('customerName',""),
                'costCenter': item.get('costCenter',""),
                'actualRevenue': round(cumulative_totals[key]['actualRevenue'], 2) if isinstance(cumulative_totals[key]['actualRevenue'], (int, float)) else cumulative_totals[key]['actualRevenue'],
                'COGS': round(cumulative_totals[key]['COGS'], 2) if isinstance(cumulative_totals[key]['COGS'], (int, float)) else cumulative_totals[key]['COGS'],
                'SGNA': round(cumulative_totals[key]['SGNA'], 2) if isinstance(cumulative_totals[key]['SGNA'], (int, float)) else cumulative_totals[key]['SGNA'],
                'planRevenue': round(cumulative_totals[key]['planRevenue'], 2) if isinstance(cumulative_totals[key]['planRevenue'], (int, float)) else cumulative_totals[key]['planRevenue'],
                'actualSalary': round(cumulative_totals[key]['actualSalary'], 2) if isinstance(cumulative_totals[key]['actualSalary'], (int, float)) else cumulative_totals[key]['actualSalary'],
                'actualVendorCost': round(cumulative_totals[key]['actualVendorCost'], 2) if isinstance(cumulative_totals[key]['actualVendorCost'], (int, float)) else cumulative_totals[key]['actualVendorCost'],
                'otherFixedCost': round(cumulative_totals[key]['otherFixedCost'], 2) if isinstance(cumulative_totals[key]['otherFixedCost'], (int, float)) else cumulative_totals[key]['otherFixedCost'],
                'employeeExpanse': round(cumulative_totals[key]['employeeExpanse'], 2) if isinstance(cumulative_totals[key]['employeeExpanse'], (int, float)) else cumulative_totals[key]['employeeExpanse'],
                'miscellaneousExpenses': round(cumulative_totals[key]['miscellaneousExpenses'], 2) if isinstance(cumulative_totals[key]['miscellaneousExpenses'], (int, float)) else cumulative_totals[key]['miscellaneousExpenses'],
                'miscellaneousExpensesSecond': round(cumulative_totals[key]['miscellaneousExpensesSecond'], 2) if isinstance(cumulative_totals[key]['miscellaneousExpensesSecond'], (int, float)) else cumulative_totals[key]['miscellaneousExpensesSecond'],
                'planGp': round(cumulative_totals[key]['planGp'], 2) if isinstance(cumulative_totals[key]['planGp'], (int, float)) else cumulative_totals[key]['planGp'],
                'actualCOGS': round(cumulative_totals[key]['actualCOGS'], 2) if isinstance(cumulative_totals[key]['actualCOGS'], (int, float)) else cumulative_totals[key]['actualCOGS'],
                'actualGp': round(cumulative_totals[key]['actualGp'], 2) if isinstance(cumulative_totals[key]['actualGp'], (int, float)) else cumulative_totals[key]['actualGp'],
                'actualSGNA': round(cumulative_totals[key]['actualSGNA'], 2) if isinstance(cumulative_totals[key]['actualSGNA'], (int, float)) else cumulative_totals[key]['actualSGNA'],
                # 'actualRevenue': cumulative_totals[key]['actualRevenue'],
                # 'COGS': cumulative_totals[key]['COGS'],
                # 'SGNA': cumulative_totals[key]['SGNA'],
                # 'planRevenue': cumulative_totals[key]['planRevenue'],
                # 'actualSalary': cumulative_totals[key]['actualSalary'],
                # 'actualVendorCost': cumulative_totals[key]['actualVendorCost'],
                # 'otherFixedCost': cumulative_totals[key]['otherFixedCost'],
                # 'employeeExpanse': cumulative_totals[key]['employeeExpanse'],
                # 'miscellaneousExpenses': cumulative_totals[key]['miscellaneousExpenses'],
                # 'miscellaneousExpensesSecond': cumulative_totals[key]['miscellaneousExpensesSecond'],
                # 'planGp': cumulative_totals[key]['planGp'],
                # 'actualCOGS': cumulative_totals[key]['actualCOGS'],
                # 'actualGp': cumulative_totals[key]['actualGp'],
                # 'actualSGNA': cumulative_totals[key]['actualSGNA'],
                'gm': round(cumulative_totals[key]['planGp'] / cumulative_totals[key]['planRevenue'], 2) if cumulative_totals[key]['planRevenue'] != 0 else 0,
                'np': round(cumulative_totals[key]['planGp'] - (cumulative_totals[key]['SGNA'] / cumulative_totals[key]['planRevenue'] if cumulative_totals[key]['planRevenue'] != 0 else 0), 2),
                'actualGm': round(cumulative_totals[key]['actualGp'] / cumulative_totals[key]['actualRevenue'], 4) if cumulative_totals[key]['actualRevenue'] != 0 else 0,
                'actualNp': round(cumulative_totals[key]['actualGp'] - (cumulative_totals[key]['SGNA'] / cumulative_totals[key]['actualRevenue'] if cumulative_totals[key]['actualRevenue'] != 0 else 0), 2),
                'month': month,
                'year': item['year'],
                'zone':item['zone'],
                'ustProjectID':item['ustProjectID'],
                'uniqueId':item['uniqueId'],
                "businessUnit":item["businessUnit"]
            })

    return accumulated_data



def get_progressive_data_export(data, months):
    
    accumulated_data = []
    cumulative_totals = {}
    for month in months:
        
        filtered_data = [item for item in data if item['month'] == month]
        
        for item in filtered_data:
            key = (item['Customer'], item['Cost Center'], item['Year'])
            if key not in cumulative_totals:
                cumulative_totals[key] = {
                    'Customer': item['Customer'],
                    'Cost Center': item['Cost Center'],
                    'Actual Revenue': 0,
                    'Planned Gross Profit':0,
                    'Planned COGS': 0,
                    'Planned SGNA':0,
                    'Planned Revenue':0,
                    'Actual Salary':0,
                    'Actual Vendor Cost':0,
                    'Other Fixed Cost':0,
                    'Employee Expanse':0,
                    'Miscellaneous Expenses':0,
                    'Miscellaneous Expenses Second':0,
                    'Planned Gross Profit':0,
                    'Actual COGS':0,
                    'Actual Gross Profit':0,
                    'Year': item['Year']
                }
            cumulative_totals[key]['Actual Revenue'] += item.get('Actual Revenue', 0) if item.get('Actual Revenue') is not None else 0
            cumulative_totals[key]['Planned COGS'] += item.get('Planned COGS', 0) if item.get('Planned COGS') is not None else 0
            cumulative_totals[key]['Planned SGNA'] += item.get('Planned SGNA', 0) if item.get('Planned SGNA') is not None else 0
            cumulative_totals[key]['Planned Revenue'] += item.get('Planned Revenue', 0) if item.get('Planned Revenue') is not None else 0
            cumulative_totals[key]['Planned Gross Profit'] += item.get('Planned Gross Profit', 0) if item.get('Planned Gross Profit') is not None else 0
            cumulative_totals[key]['Actual Salary'] += item.get('Actual Salary', 0) if item.get('Actual Salary') is not None else 0
            cumulative_totals[key]['Actual Vendor Cost'] += item.get('Actual Vendor Cost', 0) if item.get('Actual Vendor Cost') is not None else 0
            cumulative_totals[key]['Other Fixed Cost'] += item.get('Other Fixed Cost', 0) if item.get('Other Fixed Cost') is not None else 0
            cumulative_totals[key]['Employee Expanse'] += item.get('Employee Expanse', 0) if item.get('Employee Expanse') is not None else 0
            cumulative_totals[key]['Miscellaneous Expenses'] += item.get('Miscellaneous Expenses', 0) if item.get('Miscellaneous Expenses') is not None else 0
            cumulative_totals[key]['Miscellaneous Expenses Second'] += item.get('Miscellaneous Expenses Second', 0) if item.get('Miscellaneous Expenses Second') is not None else 0
            cumulative_totals[key]['Planned Gross Profit'] += item.get('Planned Gross Profit', 0) if item.get('Planned Gross Profit') is not None else 0
            cumulative_totals[key]['Actual COGS'] += item.get('Actual COGS', 0) if item.get('Actual COGS') is not None else 0
            cumulative_totals[key]['Actual Gross Profit'] += item.get('Actual Gross Profit', 0) if item.get('Actual Gross Profit') is not None else 0
            # cumulative_totals[key]['Miscellaneous ExpensesSecond'] += item['COGS']
            accumulated_data.append({
                'Customer': item['Customer'],
                'Cost Center': item['Cost Center'],
                'Actual Revenue': cumulative_totals[key]['Actual Revenue'],
                'Planned COGS': cumulative_totals[key]['Planned COGS'],
                'Planned SGNA': cumulative_totals[key]['Planned SGNA'],
                'Planned Revenue': cumulative_totals[key]['Planned Revenue'],
                'Planned Gross Profit': cumulative_totals[key]['Planned Gross Profit'],
                'Actual Salary': cumulative_totals[key]['Actual Salary'],
                'Actual Vendor Cost': cumulative_totals[key]['Actual Vendor Cost'],
                'Other Fixed Cost': cumulative_totals[key]['Other Fixed Cost'],
                'Employee Expanse': cumulative_totals[key]['Employee Expanse'],
                'Miscellaneous Expenses': cumulative_totals[key]['Miscellaneous Expenses'],
                'Miscellaneous Expenses Second': cumulative_totals[key]['Miscellaneous Expenses Second'],
                'Planned Gross Profit': cumulative_totals[key]['Planned Gross Profit'],
                'Actual COGS': cumulative_totals[key]['Actual COGS'],
                'Actual Gross Profit': cumulative_totals[key]['Actual Gross Profit'],
                'Planned Gross Margin':cumulative_totals[key]['Planned Gross Profit'] / cumulative_totals[key]['Planned Revenue'] if cumulative_totals[key]['Planned Revenue'] != 0 else 0,
                'Planned Net Profit': cumulative_totals[key]['Planned Gross Profit'] - (cumulative_totals[key]['Planned SGNA'] / cumulative_totals[key]['Planned Revenue'] if cumulative_totals[key]['Planned Revenue'] != 0 else 0) ,
                'Actual Gross Margin' : cumulative_totals[key]['Actual Gross Profit'] / cumulative_totals[key]['Actual Revenue'] if cumulative_totals[key]['Actual Revenue'] != 0 else 0 ,
                'Actual Net Profit' : cumulative_totals[key]['Actual Gross Profit']  - (cumulative_totals[key]['Planned SGNA'] / cumulative_totals[key]['Actual Revenue'] if cumulative_totals[key]['Actual Revenue'] != 0 else 0) ,
                'month': month,
                'Year': item['Year'],
                'Month': item['Month'],
                'UST Project ID':item['UST Project ID'],
                "Business Unit":item["Business Unit"]
            })

    return accumulated_data


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

@forms_blueprint.route("/forms/earnValue", methods=["POST", "PUT"])
@forms_blueprint.route("/forms/earnValue/<id>", methods=["POST", "PUT"])
@token_required
def forms(current_user, id=None): 
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
                    'preserveNullAndEmptyArrays': True
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
                '$addFields': {
                    'year': {
                        '$ifNull': [
                            '$year', Year[0]
                        ]
                    }
                }
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
        zoneArray=[
            {
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
            }
        ]
        dataResp = cmo.finding_aggregate("projectAllocation", arra+zoneArray)
        if len (dataResp['data']):
            dataRespdf = pd.DataFrame.from_dict(dataResp['data'])
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
                    df_pivot.iloc[:, 5 + i] += df_pivot.iloc[:, 5 + i - 1]
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
                dataResp['data'] = json.loads(mergedDf.to_json(orient="records"))
            return respond(dataResp)   
            
        else:
            return respond({
                'status':200,
                "data":[],
            })

    elif request.method == "PUT":
        
        allData = request.get_json()
        allData['year'] = int(allData['year'])
        
        updateBy = {
           'uniqueId': allData['uniqueId'],
           'year': allData['year']
        }
        if 'roleName' in allData:
            del allData['roleName']
            
        response = cmo.updating("earnValue",updateBy,allData,True)
        return respond(response)


    
@forms_blueprint.route("/forms/EVMActual", methods=["POST","PATCH"])
@forms_blueprint.route("/forms/EVMActual/<id>", methods=["POST","PATCH"])
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
                    '_id':0
                }
            }
        ]
        print(arra,'arraarraarra')
        res = cmo.finding_aggregate("deliveryPVA",arra)
        return respond(res)
        
        


    if request.method == "PATCH":
        allData = request.get_json()
        allData['customerId'] = ObjectId(allData['customerId'])
        allData['circleId'] = ObjectId(allData['circleId'])
        updateBy = {
            'year':allData['year'],
            "month":allData['month'],
            "customerId":allData['customerId'],
            "circleId":allData['circleId'],
            "MSType":allData['MSType']
        }
        res = cmo.updating("deliveryPVA",updateBy,allData,True)
        return respond(res)


@forms_blueprint.route("/forms/profilt&loss", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@forms_blueprint.route("/forms/profilt&loss/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def forms_profit_loss(current_user,id=None):
    
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
                    'month':{'$in':month},
                    'year':year
                }
            }
        ]
        arra = arra + apireq.countarra("profitloss",arra) + apireq.args_pagination(request.args)
        arra = arra + [
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
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
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
                    'costCenter': {
                        '$toString': '$costCenter'
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
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)
        return respond(response)
        
    if request.method == "POST":
        allData = request.get_json()
        costCenterId = []
        costCenter = costCenter_str(current_user['userUniqueId'])
        for i in costCenter:
            costCenterId.append(ObjectId(i['uniqueId']))
        year = int(allData["year"])
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
            }
        ]
        arra = arra + apireq.countarra("profitloss",arra) + apireq.args_pagination(request.args)
        arra = arra + [
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
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
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
                    'costCenter': {
                        '$toString': '$costCenter'
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
            }
        ]
        response = cmo.finding_aggregate("profitloss",arra)
        return respond(response)
    
    elif request.method == "PUT":
        if id == None:
            allData = request.get_json()
            allData['costCenter'] = ObjectId(allData['costCenter'])
            arra = [
                {
                    '$match':{
                        'costCenter':allData['costCenter'],
                        'year':allData['year'],
                        'month':allData['month'],
                        'deleteStatus':{'$ne':1}
                    }
                }
            ]
            check = cmo.finding_aggregate("profitloss",arra)['data']
            if len(check)>0:
                return respond({
                    'status':400,
                    "icon":'error',
                    "msg":'The Cost Center for this Month and Year is already exist in Database'
                })
            else:
                response = cmo.insertion("profitloss",allData)
                return respond(response)
        else:
            allData = request.get_json()
            if "month" in allData:
                del allData['month']
            if "costCenterName" in allData:
                del allData['costCenterName']
            updateBy = {
                '_id':ObjectId(id)
            }
            response = cmo.updating("profitloss",updateBy,allData,False)
            return respond(response)
        
    elif request.method == "DELETE":
        if id!=None:
            response = cmo.deleting("profitloss",id,current_user['userUniqueId'])
            return respond(response)

@forms_blueprint.route("/forms/accrualRevenueTrend", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def forms_accrual_revenue_trend(current_user):
    
    if request.method == "POST":
        
        allData = request.get_json()
        viewBy = allData["Monthly"].split(",")
        
        projection_stage = {
            '_id': 0, 
            "customer":1,
            "costCenter":1,
            "ustProjectid":1,
            "uniqueId":1,
        }
        for field in viewBy:
            projection_stage[field] = 1
        
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
        arra = arra + apireq.countarra("projectAllocation",arra)
        response = cmo.finding_aggregate("projectAllocation",arra)
        return respond(response)

    if request.method == "PUT":
        allData = request.get_json()
        updateBy = {
            'costCenteruid':allData['costCenteruid']
        }
        response = cmo.updating("accrualRevenueTrend",updateBy,allData,True)
        return respond(response)
    
    

@forms_blueprint.route("/forms/dynamicHeaderSOB", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def dynamicHeaderSOB(current_user):
    if request.method == "GET":
        arra =[
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }, {
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
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'status': 'Active'
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNoneAndEmptyArrays': True
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
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
                    'as': 'circleresult'
                }
            }, {
                '$unwind': {
                    'path': '$circleresult', 
                    'preserveNoneAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'projectType', 
                    'foreignField': '_id', 
                    'as': 'projectTyperesult'
                }
            }, {
                '$unwind': {
                    'path': '$projectTyperesult', 
                    'preserveNoneAndEmptyArrays': True
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
                                'as': 'customer'
                            }
                        }, {
                            '$unwind': {
                                'path': '$customer', 
                                'preserveNoneAndEmptyArrays': True
                            }
                        }, {
                            '$lookup': {
                                'from': 'zone', 
                                'localField': 'zoneId', 
                                'foreignField': '_id', 
                                'as': 'zone'
                            }
                        }, {
                            '$unwind': {
                                'path': '$zone', 
                                'preserveNoneAndEmptyArrays': True
                            }
                        }, {
                            '$lookup': {
                                'from': 'costCenter', 
                                'localField': 'costCenterId', 
                                'foreignField': '_id', 
                                'as': 'costCenter'
                            }
                        }, {
                            '$unwind': {
                                'path': '$costCenter', 
                                'preserveNoneAndEmptyArrays': True
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
                                'projectGroupUid': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0, 
                                'projectGroupId': 1, 
                                'costCenter': 1
                            }
                        }
                    ], 
                    'as': 'projectGroupresult'
                }
            }, {
                '$unwind': {
                    'path': '$projectGroupresult', 
                    'preserveNoneAndEmptyArrays': True
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'circleName': '$circleresult.circleName', 
                    'projectTypeName': '$projectTyperesult.projectType', 
                    'projectGroupName': '$projectGroupresult.projectGroupId', 
                    'circleuid': {
                        '$toString': '$circle'
                    }, 
                    'projectTypeuid': {
                        '$toString': '$projectType'
                    }, 
                    'projectGroupuid': {
                        '$toString': '$projectGroup'
                    }
                }
            }, {
                '$facet': {
                    'circle': [
                        {
                            '$group': {
                                '_id': '$circleName', 
                                'circleuid': {
                                    '$first': '$circleuid'
                                }
                            }
                        }
                    ], 
                    'projectType': [
                        {
                            '$group': {
                                '_id': '$projectTypeName', 
                                'projectTypeuid': {
                                    '$first': '$projectTypeuid'
                                }
                            }
                        }
                    ], 
                    'projectGroup': [
                        {
                            '$group': {
                                '_id': '$projectGroupName', 
                                'projectGroupuid': {
                                    '$first': '$projectGroupuid'
                                }
                            }
                        }
                    ]
                }
            }
        ]
        response = cmo.finding_aggregate("projectAllocation",arra)
        return respond(response)


@forms_blueprint.route("/forms/SOB", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def sob(current_user):
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
                    'preserveNoneAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'result.competitor': '$competitor',
                    'result.uniqueId': '$uniqueId',
                    'result.circleName': {
                '$toObjectId': '$result.circle'
            }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            },
            {
            '$lookup': {
                'from': 'circle', 
                'localField': 'circleName', 
                'foreignField': '_id', 
                'as': 'result1'
            }
        }, {
            '$addFields': {
                'circleName': {
                    '$arrayElemAt': [
                        '$result1.circleName', 0
                    ]
                },
            }
        }, {
            '$project': {
                '_id': 0, 
                'result1': 0
            }
        }
        ]
        response = cmo.finding_aggregate("sob",arra)
        return respond(response)
    
    if request.method == "PUT":
        allData = request.get_json()
        if 'undefined' in allData:
            del allData['undefined']
        updateBy = {
            'uniqueId':allData['uniqueId'],
            "circle":allData['circle'],
            'viewBy':allData['viewBy'],
            'year':allData['year']
        }
        response = cmo.updating("sobAction",updateBy,allData,True)
        return respond(response)
    
    
@forms_blueprint.route("/AOP", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@forms_blueprint.route("/AOP/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def AOP(current_user,id=None):
    airtelId=None
    filter1=[
        {
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
                        '$project': {
                            '_id': 0, 
                            'businessUnit': 1, 
                            'ustProjectId': 1
                        }
                    }
                ], 
                'as': 'costCenterResults2'
            }
        }, {
            '$addFields': {
                'businessUnit': {
                    '$arrayElemAt': [
                        '$costCenterResults2.businessUnit', 0
                    ]
                }, 
                'ustProjectID': {
                    '$arrayElemAt': [
                        '$costCenterResults2.ustProjectId', 0
                    ]
                }
            }
        }, {
            '$project': {
                'costCenterResults2': 0
            }
        }
    ]
    if request.args.get("forAirtel"):
        query=[
                    {
                        "$addFields":{
                            "customerName":{
                                "$toLower":"$customerName"
                            }
                        }
                    },
                    {
                      "$match":{
                          "customerName":"airtel"
                      }  
                    }
                ]
        data=cmo.finding_aggregate("customer",query)
        if len(data["data"]):
            airtelId=data["data"][0]["_id"]
    rqMethod=request.method
    query =[
        {
        "$match":get_previous_months_mongodb_query()
                                }, {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
                },
                {
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
                            '$lookup': {
                                'from': 'customer', 
                                'localField': 'customer', 
                                'foreignField': '_id', 
                                'as': 'customer'
                            }
                        }, {
                            '$unwind': {
                                'path': '$customer'
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
                                'customerName': '$customer.customerName', 
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResult.shortCode', 0
                                    ]
                                }, 
                                'zoneId': {
                                    '$toString': '$zone'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'costCenter': 1, 
                                'customerName': 1, 
                                'zone': 1, 
                               
                            }
                        }
                    ], 
                    'as': 'costCenterResults'
                }
                }, {
                    '$addFields': {
                        'costCenterId': {
                            '$toString': '$costCenterId'
                        }, 
                        'costCenter': {
                            '$arrayElemAt': [
                                '$costCenterResults.costCenter', 0
                            ]
                        }, 
                        
                        'zone': {
                            '$arrayElemAt': [
                                '$costCenterResults.zone', 0
                            ]
                        }, 
                        'zoneId': {
                            '$arrayElemAt': [
                                '$costCenterResults.zoneId', 0
                            ]
                        }, 
                        'customerName': {
                            '$arrayElemAt': [
                                '$costCenterResults.customerName', 0
                            ]
                        }, 
                        'customerId': {
                            '$toString': '$customerId'
                        }, 
                        'costCenterResults': 1, 
                        'uniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }, {
            '$project': {
                '_id': 0
            }
        }, {
        '$addFields': {
            'planGp': {
                '$subtract': [
                    '$planRevenue', '$COGS'
                ]
            }, 
            'actualCOGS': {
                '$sum': [
                    '$actualSalary', '$actualVendorCost', '$otherFixedCost', '$employeeExpanse', '$miscellaneousExpenses', '$miscellaneousExpensesSecond'
                ]
            }
            }
        }, {
            '$addFields': {
                'actualGp': {
                '$subtract': [
                    '$actualRevenue', '$actualCOGS'
                ]
                }
            }
        }, {
            '$addFields': {
                'COGS': {
                '$cond': [
                    {
                        '$ne': [
                            '$COGS', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$COGS', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'SGNA': {
                '$cond': [
                    {
                        '$ne': [
                            '$SGNA', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$SGNA', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'planRevenue': {
                '$cond': [
                    {
                        '$ne': [
                            '$planRevenue', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$planRevenue', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'actualSalary': {
                '$cond': [
                    {
                        '$ne': [
                            '$actualSalary', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$actualSalary', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'actualSGNA': {
                '$cond': [
                    {
                        '$ne': [
                            '$actualSGNA', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$actualSGNA', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'actualVendorCost': {
                '$cond': [
                    {
                        '$ne': [
                            '$actualVendorCost', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$actualVendorCost', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'actualRevenue': {
                '$cond': [
                    {
                        '$ne': [
                            '$actualRevenue', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$actualRevenue', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'otherFixedCost': {
                '$cond': [
                    {
                        '$ne': [
                            '$otherFixedCost', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$otherFixedCost', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'employeeExpanse': {
                '$cond': [
                    {
                        '$ne': [
                            '$employeeExpanse', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$employeeExpanse', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'miscellaneousExpenses': {
                '$cond': [
                    {
                        '$ne': [
                            '$miscellaneousExpenses', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$miscellaneousExpenses', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'miscellaneousExpensesSecond': {
                '$cond': [
                    {
                        '$ne': [
                            '$miscellaneousExpensesSecond', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$miscellaneousExpensesSecond', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'planGp': {
                '$cond': [
                    {
                        '$ne': [
                            '$planGp', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$planGp', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'actualCOGS': {
                '$cond': [
                    {
                        '$ne': [
                            '$actualCOGS', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$actualCOGS', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }, 
            'actualGp': {
                '$cond': [
                    {
                        '$ne': [
                            '$actualGp', None
                        ]
                    }, {
                        '$round': [
                            {
                                '$divide': [
                                    '$actualGp', 1000000
                                ]
                            }, 2
                        ]
                    }, None
                ]
            }
        }
        },{
            '$addFields': {
                'gm': {
                '$cond': {
                    'if': {
                        '$eq': [
                            '$planRevenue', 0
                        ]
                    }, 
                    'then': 0, 
                    'else': {
                        '$divide': [
                            '$planGp', '$planRevenue'
                        ]
                    }
                }
            }, 
            'actualGm': {
                '$cond': {
                    'if': {
                        '$eq': [
                            '$actualRevenue', 0
                        ]
                    }, 
                    'then': 0, 
                    'else': {
                        '$divide': [
                            '$actualGp', '$actualRevenue'
                        ]
                    }
                }
            }
        }
        }, {
            '$addFields': {
                'actualGm': {
                    '$cond': {
                        'if': {
                            '$isNumber': '$actualGm'
                        }, 
                        'then': {
                            '$round': [
                                '$actualGm', 4
                            ]
                        }, 
                        'else': '$actualGm'
                    }
                }, 
                'gm': {
                    '$cond': {
                        'if': {
                            '$isNumber': '$gm'
                        }, 
                        'then': {
                            '$round': [
                                '$gm', 4
                            ]
                        }, 
                        'else': '$gm'
                    }
                }
            }
            }, {
                '$addFields': {
                'np': {
                    '$subtract': [
                        '$planGp', '$SGNA'
                    ]
                }, 
                'actualNp': {
                    '$subtract': [
                        '$actualGp', '$actualSGNA'
                    ]
                }
                }
            }, {
                '$addFields': {
                    'np': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$planRevenue', 0
                            ]
                        }, 
                        'then': 0, 
                        'else': {
                            '$divide': [
                                '$np', '$planRevenue'
                            ]
                        }
                    }
                }, 
                'actualNp': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$actualRevenue', 0
                            ]
                        }, 
                        'then': 0, 
                        'else': {
                            '$divide': [
                                '$actualNp', '$actualRevenue'
                            ]
                        }
                    }
                }
            }
        },
        ]
    
    if rqMethod == 'GET':
        if airtelId:
            query=[
                {
                    "$match":{
                        "customerId":ObjectId(airtelId)
                    }
                }
            ]+query

        if request.args.get("dollorView")=="true":
            query=query+[
                {
                '$lookup': {
                    'from': 'exChangeRate', 
                    'localField': 'year', 
                    'foreignField': 'year', 
                    'as': 'year'
                }
                }, {
                    '$unwind': {
                        'path': '$year'
                    }
                }, {
                    '$addFields': {
                        'year': '$year.year', 
                        'rate': '$year.rate'
                    }
                },
                {
                    '$addFields': {
                        'COGS': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$COGS', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'SGNA': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$SGNA', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'planRevenue': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planRevenue', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualSalary': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualSalary', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualSGNA': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualSGNA', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualVendorCost': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualVendorCost', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualRevenue': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualRevenue', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'otherFixedCost': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$otherFixedCost', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'employeeExpanse': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$employeeExpanse', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'miscellaneousExpenses': {
                            '$cond': {
                                'if': {
                                    '$ne': [
                                        '$miscellaneousExpenses', None
                                    ]
                                }, 
                                'then': {
                                    '$round': [
                                        {
                                            '$divide': [
                                                '$miscellaneousExpenses', '$rate'
                                            ]
                                        }, 4
                                    ]
                                }, 
                                'else': None
                            }
                        }, 
                        'miscellaneousExpensesSecond': {
                            '$cond': {
                                'if': {
                                    '$ne': [
                                        '$miscellaneousExpensesSecond', None
                                    ]
                                }, 
                                'then': {
                                    '$round': [
                                        {
                                            '$divide': [
                                                '$miscellaneousExpensesSecond', '$rate'
                                            ]
                                        }, 4
                                    ]
                                }, 
                                'else': None
                            }
                        }, 
                        'costCenterResults': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$costCenterResults', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'planGp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planGp', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualCOGS': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualCOGS', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualGp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualGp', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'gm': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$gm', '$rate'
                                    ]
                                }, 6
                            ]
                        }, 
                        'np': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$np', '$rate'
                                    ]
                                }, 6
                            ]
                        }, 
                        'actualGm': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualGm', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualNp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualNp', '$rate'
                                    ]
                                }, 6
                            ]
                        }
                    }
                },
                
            
            ]
        
        resp=cmo.finding_aggregate("AOP",filter1+query)
        print()
        if(request.args.get("Cumulative")=="true"):
                data=get_progressive_data(resp['data'], get_previous_months_Arrey())
                resp['data']=data
                
                return respond(resp)
        
        return respond(resp)
    if (rqMethod=="POST"):
        if request.args.get("filter"):
            payload=request.get_json()
            cost_center = payload.get("Cost Center", "")
            month = payload.get("month", "")
            year = payload.get("year", "")
            customerId = payload.get("Customer", "")
            businessUnit= payload.get("Business unit", "")
            cost_center_ids = [ObjectId(center) for center in cost_center.split(",") if center]
            customerId_ids = [ObjectId(customer) for customer in customerId.split(",") if customer]
            months = [int(m) for m in month.split(",") if m.isdigit()]
            years = [int(y) for y in year.split(",") if y.isdigit()]
            filter ={}
            businessUnit= [ item for item in businessUnit.split(",") if businessUnit]
            applyCummlativefilter=True
            keyToAdd=""
            groupKey=""
            if customerId:
                customerId=customerId
                applyCummlativefilter=False
                keyToAdd="customerName"
                groupKey="$customerName"
                filter["customerId"]={
                        "$in":customerId_ids
                    }
            if months:
                filter["month"]={
                        "$in":months
                    }
                if len(months)==1 and request.args.get("Cumulative")!="true" and len(cost_center_ids)==0 and  len(businessUnit)==0 and len(customerId_ids)==0:
                    
                    applyCummlativefilter=False
            if years:
                filter["year"]={
                        "$in":years
                    }
            if cost_center_ids:
                filter["costCenterId"]={
                        "$in":cost_center_ids
                    }
                applyCummlativefilter=False
                keyToAdd="costCenter"
                groupKey="$costCenter"
            if businessUnit:
                filter["businessUnit"]={
                        "$in":businessUnit
                    }
                applyCummlativefilter=False
                keyToAdd="businessUnit"
                groupKey="$businessUnit"
                
            if filter:
                query=[
                    {
                        "$match":filter
                    }
                ]+query[1:]
                # group =""
                # if customerId:
                #     group:"$customerId"
                # if businessUnit :
                #     group="$businessUnit"
                if applyCummlativefilter or  (len(months)==1 and len(cost_center_ids)==0 and  len(businessUnit)==0 and len(customerId_ids)==0):
                    query.append({
                                    '$sort': {
                                        'year': 1, 
                                        'month': 1
                                    }
                                })
                else:
                    print("k;jncldjcndwejfnlvejriogjfiorjf=",applyCummlativefilter)
                    query=query[0:4]+[{
                                '$sort': {
                                    'year': 1, 
                                    'month': 1
                                }
                            },{
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
                                'default': 'Unknown'
                            }
                        }
                    }
                    }, {
                        '$group': {
                            '_id': groupKey , 
                            'COGS': {
                                '$sum': '$COGS'
                            }, 
                            'SGNA': {
                                '$sum': '$SGNA'
                            }, 
                            'actualRevenue': {
                                '$sum': '$actualRevenue'
                            }, 
                            'actualSGNA': {
                                '$sum': '$actualSGNA'
                            }, 
                            'actualSalary': {
                                '$sum': '$actualSalary'
                            }, 
                            'actualVendorCost': {
                                '$sum': '$actualVendorCost'
                            }, 
                            'employeeExpanse': {
                                '$sum': '$employeeExpanse'
                            }, 
                            'miscellaneousExpenses': {
                                '$sum': '$miscellaneousExpenses'
                            }, 
                            'miscellaneousExpensesSecond': {
                                '$sum': '$miscellaneousExpensesSecond'
                            }, 
                            'otherFixedCost': {
                                '$sum': '$otherFixedCost'
                            }, 
                            'planRevenue': {
                                '$sum': '$planRevenue'
                            }, 
                            'costCenterResults': {
                                '$sum': '$costCenterResults'
                            }, 
                            'planGp': {
                                '$sum': '$planGp'
                            }, 
                            'actualCOGS': {
                                '$sum': '$actualCOGS'
                            }, 
                            'actualGp': {
                                '$sum': '$actualGp'
                            }, 
                            'gm': {
                                '$sum': '$gm'
                            }, 
                            'np': {
                                '$sum': '$np'
                            }, 
                            'actualGm': {
                                '$sum': '$actualGm'
                            }, 
                            'actualNp': {
                                '$sum': '$actualNp'
                            }, 
                            'year': {
                                '$first': '$year'
                            }, 
                            'months': {
                                '$push': '$month'
                            },
                             "customerName":{
                                "$first":"$customerName"
                            },
                                                        # keyToAdd:"$_id"
                                                    }
                                                }  if  cost_center_ids else {
                        '$group': {
                            '_id': groupKey , 
                            'COGS': {
                                '$sum': '$COGS'
                            }, 
                            'SGNA': {
                                '$sum': '$SGNA'
                            }, 
                            'actualRevenue': {
                                '$sum': '$actualRevenue'
                            }, 
                            'actualSGNA': {
                                '$sum': '$actualSGNA'
                            }, 
                            'actualSalary': {
                                '$sum': '$actualSalary'
                            }, 
                            'actualVendorCost': {
                                '$sum': '$actualVendorCost'
                            }, 
                            'employeeExpanse': {
                                '$sum': '$employeeExpanse'
                            }, 
                            'miscellaneousExpenses': {
                                '$sum': '$miscellaneousExpenses'
                            }, 
                            'miscellaneousExpensesSecond': {
                                '$sum': '$miscellaneousExpensesSecond'
                            }, 
                            'otherFixedCost': {
                                '$sum': '$otherFixedCost'
                            }, 
                            'planRevenue': {
                                '$sum': '$planRevenue'
                            }, 
                            'costCenterResults': {
                                '$sum': '$costCenterResults'
                            }, 
                            'planGp': {
                                '$sum': '$planGp'
                            }, 
                            'actualCOGS': {
                                '$sum': '$actualCOGS'
                            }, 
                            'actualGp': {
                                '$sum': '$actualGp'
                            }, 
                            'gm': {
                                '$sum': '$gm'
                            }, 
                            'np': {
                                '$sum': '$np'
                            }, 
                            'actualGm': {
                                '$sum': '$actualGm'
                            }, 
                            'actualNp': {
                                '$sum': '$actualNp'
                            }, 
                            'year': {
                                '$first': '$year'
                            }, 
                            'months': {
                                '$addToSet': '$month'
                            },
                            # keyToAdd:"$_id"
                        }
                    }, {
                    "$addFields": {
                        "months": {
                            "$reduce": {
                                "input": "$months",
                    "initialValue": [],
                    "in": {
                        "$cond": {
                            "if": { "$in": ["$$this", "$$value"] },
                            "then": "$$value",
                            "else": { "$concatArrays": ["$$value", ["$$this"]] }
                        }
                    }
                                }
                            }
                        }
                    }, {
                        '$project': {
                            "customerName":1 if keyToAdd!="customerName" else 0,
                            "uniqueId":"$_id" ,
                            '_id': 1,
                            keyToAdd:"$_id" ,
                            'COGS': 1, 
                            'SGNA': 1, 
                            'actualRevenue': 1, 
                            'actualSGNA': 1, 
                            'actualSalary': 1, 
                            'actualVendorCost': 1, 
                            'employeeExpanse': 1, 
                            'miscellaneousExpenses': 1, 
                            'miscellaneousExpensesSecond': 1, 
                            'otherFixedCost': 1, 
                            'planRevenue': 1, 
                            'costCenterResults': 1, 
                            'planGp': 1, 
                            'actualCOGS': 1, 
                            'actualGp': 1, 
                            'gm': 1, 
                            'np': 1, 
                            'actualGm': 1, 
                            'actualNp': 1, 
                            'year': 1, 
                            'months': 1, 
                            'allMonthsString': {
                                '$reduce': {
                                    'input': '$months', 
                                    'initialValue': '', 
                                    'in': {
                                        '$concat': [
                                            {
                                                '$ifNull': [
                                                    '$$value', ''
                                                ]
                                            }, {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$indexOfBytes': [
                                                                    '$$value', ''
                                                                ]
                                                            }, -1
                                                        ]
                                                    }, 
                                                    'then': '', 
                                                    'else': '-'
                                                }
                                            }, {
                                                '$toString': '$$this'
                                            }
                                        ]
                                    }
                                }
                            }, 
                            'allMonthsStringFormatted': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            {
                                                '$substrBytes': [
                                                    '$allMonthsString', 0, 1
                                                ]
                                            }, '-'
                                        ]
                                    }, 
                                    'then': {
                                        '$substrBytes': [
                                            '$allMonthsString', 1, {
                                                '$strLenBytes': '$allMonthsString'
                                            }
                                        ]
                                    }, 
                                    'else': '$allMonthsString'
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'month': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            {
                                                '$substrBytes': [
                                                    '$allMonthsString', 0, 1
                                                ]
                                            }, '-'
                                        ]
                                    }, 
                                    'then': {
                                        '$substrBytes': [
                                            '$allMonthsString', 1, {
                                                '$strLenBytes': '$allMonthsString'
                                            }
                                        ]
                                    }, 
                                    'else': '$allMonthsString'
                                }
                            }
                        }
                    }, {
                        '$project': {
                            'allMonthsString': 0
                        }
                    },
       
                        ]+query[4:]+[        
                                    {
                            "$addFields":{
                                "uniqueId":1
                            }
                        }]
                    
                if airtelId:
            
                    query=[
                        {
                            "$match":{
                                "customerId":ObjectId(airtelId)
                            }
                        }
                    ]+query
                
                resp=cmo.finding_aggregate("AOP",filter1+query)
                if applyCummlativefilter==True:
                    data=get_progressive_data(resp['data'], months)
                    resp['data']=data
                return respond(resp)
        payload=request.get_json()
        validPayload={
                "COGS":"",
                "SGNA":"",
                "costCenterId":"",
                "customerId":"",
                "month":"",
                "planRevenue":"",
                
                "year":"",
                "SGNA":"",
                "actualSalary":"",
                "actualSGNA":"",
                "actualVendorCost":"",
                "actualRevenue":"",
                "otherFixedCost":"",
                "employeeExpanse":"",
                "miscellaneousExpenses":"",
                
                "miscellaneousExpensesSecond":"",
        }
        for key,value in validPayload.items():
            if (key not in ["miscellaneousExpenses","miscellaneousExpensesSecond"] and key not in payload) and ("customerId" not in payload and (airtelId==None or id!=None)) and  ("costCenterId" not in payload and id==None):
                # reponse=
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":f"please add {key}."
                })
            else:
                
                validPayload[key]=payload.get(key)
        if id ==None:
            validPayload["costCenterId"]=ObjectId(validPayload["costCenterId"])        
            validPayload["customerId"]=ObjectId(airtelId) if airtelId else ObjectId(validPayload["customerId"])        
        # response=cmo.insertion("AOP",validPayload)
        response={}
        if id!=None:
            del validPayload["year"]
            del validPayload["month"]
            if 'costCenter' in validPayload:
                del validPayload["costCenter"]
            
            if 'customerName' in validPayload:
                del validPayload["customerName"]
            # del validPayload["miscellaneousExpenses"]
            if 'customerId' in validPayload:
                del validPayload["customerId"]
            if 'costCenterId' in validPayload:
                del validPayload["costCenterId"]
            
            # del validPayload["miscellaneousExpenses"]
            # del validPayload["miscellaneousExpenses"]
            # del validPayload["year"]
            # del validPayload["year"]
            # del validPayload["year"]
            # del validPayload["year"]
            response=cmo.updating("AOP",{'_id':ObjectId(id),'deleteStatus':{'$ne':1}},validPayload,True)
        else:
            response=cmo.updating("AOP",{'customerId':ObjectId(validPayload['customerId']),'costCenterId':ObjectId(validPayload['costCenterId']),'year':validPayload['year'],'month':validPayload['month'],'deleteStatus':{'$ne':1}},validPayload,True)
        return respond(response)
 
    if request.method == "DELETE":
        
        if id != None and id != "undefined":
            Response = cmo.deleting("AOP", id)
            return respond(Response)
 
 
 
# data =[
#     {
#             "COGS": 100,
#             "SGNA": 10,
#             "actualCOGS": 620,
#             "actualGm": 0.38,
#             "actualGp": 380,
#             "actualNp": None,
#             "actualRevenue": 1000,
#             "actualSalary": 10,
#             "actualVendorCost": 10,
#             "costCenter": "MCT0356",
#             "costCenterId": "66a873b7cacd794390f67d38",
#             "costCenterResults": 1,
#             "customerId": "667d593927f39f1ac03d7863",
#             "customerName": "AIRTEL",
#             "employeeExpanse": 500,
#             "gm": 0.9,
#             "miscellaneousExpenses": None,
#             "miscellaneousExpensesSecond": None,
#             "month": 1,
#             "np": 899.99,
#             "otherFixedCost": 100,
#             "planGp": 900,
#             "planRevenue": 1000,
#             "uniqueId": "679b8471ce2a6854919f44ed",
#             "ustProjectID": "UST Project ID",
#             "year": 2023,
#             "zone": "DEL"
#         },
#     {
#             "COGS": 100,
#             "SGNA": 10,
#             "actualCOGS": 620,
#             "actualGm": 0.38,
#             "actualGp": 380,
#             "actualNp": None,
#             "actualRevenue": 1000,
#             "actualSalary": 10,
#             "actualVendorCost": 10,
#             "costCenter": "MCT0356",
#             "costCenterId": "66a873b7cacd794390f67d38",
#             "costCenterResults": 1,
#             "customerId": "667d593927f39f1ac03d7863",
#             "customerName": "AIRTEL",
#             "employeeExpanse": 500,
#             "gm": 0.9,
#             "miscellaneousExpenses": None,
#             "miscellaneousExpensesSecond": None,
#             "month": 2,
#             "np": 899.99,
#             "otherFixedCost": 100,
#             "planGp": 900,
#             "planRevenue": 1000,
#             "uniqueId": "679b8471ce2a6854919f44ed",
#             "ustProjectID": "UST Project ID",
#             "year": 2023,
#             "zone": "DEL"
#         },
#     {
#             "COGS": 100,
#             "SGNA": 10,
#             "actualCOGS": 620,
#             "actualGm": 0.38,
#             "actualGp": 380,
#             "actualNp": None,
#             "actualRevenue": 1000,
#             "actualSalary": 10,
#             "actualVendorCost": 10,
#             "costCenter": "MCT0356",
#             "costCenterId": "66a873b7cacd794390f67d38",
#             "costCenterResults": 1,
#             "customerId": "667d593927f39f1ac03d7863",
#             "customerName": "AIRTEL",
#             "employeeExpanse": 500,
#             "gm": 0.9,
#             "miscellaneousExpenses": None,
#             "miscellaneousExpensesSecond": None,
#             "month": 3,
#             "np": 899.99,
#             "otherFixedCost": 100,
#             "planGp": 900,
#             "planRevenue": 1000,
#             "uniqueId": "679b8471ce2a6854919f44ed",
#             "ustProjectID": "UST Project ID",
#             "year": 2023,
#             "zone": "DEL"
#         },
   
#     ]
    
    

# months = [1, 2, 3]
# result = get_progressive_data(data, months)
# print(result,'resultresultresult')
# # Print the result
# for record in result:
#     print(record)

@forms_blueprint.route("/exchange", methods=["GET", "POST", "DELETE"])
@forms_blueprint.route("/exchange/<id>", methods=["GET", "POST", "DELETE"])
@token_required
def exchange(current_user,id=None):
    rqType=request.method
    if(rqType=="GET"):
        query =[
            {
                "$addFields":{
                    "uniqueId":{
                        "$toString":"$_id"
                    }
                },
               
            }, {
                "$project":{
                    "_id":0
                }
            },{
                '$sort':{
                    'year':1
                }
            }
        ]
        query  = query+ apireq.commonarra + apireq.args_pagination(request.args)
        exhanges=cmo.finding_aggregate("exChangeRate",query)
        return respond(exhanges)
    
    elif rqType=="POST":
        payload=request.get_json()
        if "year" not in payload or "rate" not in payload:
            return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"Please provide year and exchange rate."
                })
        resp=cmo.updating("exChangeRate",{"year":payload["year"]},{"year":payload["year"],"rate":payload["rate"]},True)
        return respond(resp)
    
    elif rqType=="DELETE":
        res = cmo.deleting("exChangeRate",id,current_user['userUniqueId'])
        return respond(res)
    
    
@forms_blueprint.route("/businessUnit", methods=["GET"])
@token_required
def businessUnit(current_user):
    rqType=request.method
    if rqType=="GET":
        query=[{
        '$match': {
            'businessUnit': {
                '$ne': None
            }
        }
    }, {
        '$project': {
            'businessUnit': 1
        }
    }, {
        '$group': {
            '_id': None, 
            'businessUnit': {
                '$addToSet': '$businessUnit'
            }
        }
    }]
        res=cmo.finding_aggregate("AOP",query)
        return respond(res)
    
    
@forms_blueprint.route("/export/AOP", methods=["GET", "POST"])
@token_required
def ExportAOP(current_user):
    airtelId=None
    airtelFilter=None
    Cumulative=None
    monthsFilter=None 
    
    filter1=[
        {
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
                        '$project': {
                            '_id': 0, 
                            'businessUnit': 1, 
                            'ustProjectId': 1
                        }
                    }
                ], 
                'as': 'costCenterResults2'
            }
        }, {
            '$addFields': {
                'businessUnit': {
                    '$arrayElemAt': [
                        '$costCenterResults2.businessUnit', 0
                    ]
                }, 
                'ustProjectID': {
                    '$arrayElemAt': [
                        '$costCenterResults2.ustProjectId', 0
                    ]
                }
            }
        }, {
            '$project': {
                'costCenterResults2': 0
            }
        }
    ]
    
    
    if request.args.get('forAirtel'):
        if request.args.get('forAirtel') not in ['',None,'undefined']:
            airtelFilter=request.args.get('forAirtel')
    if request.args.get('Cumulative'):
        if request.args.get('Cumulative') not in ['',None,'undefined']:
            Cumulative=request.args.get('Cumulative')  
    if airtelFilter:
        query=[
                    {
                        "$addFields":{
                            "customerName":{
                                "$toLower":"$customerName"
                            }
                        }
                    },
                    {
                      "$match":{
                          "customerName":"airtel"
                      }  
                    }
                ]
        data=cmo.finding_aggregate("customer",query)
        if len(data["data"]):
            airtelId=data["data"][0]["_id"]
    rqMethod=request.method
    
    query =[
        {
        "$match":get_previous_months_mongodb_query()
                                }, {
        '$match': {
            'deleteStatus': {
                '$ne': 1
            }
        }
        },{
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
                    '$lookup': {
                        'from': 'customer', 
                        'localField': 'customer', 
                        'foreignField': '_id', 
                        'as': 'customer'
                    }
                }, {
                    '$unwind': {
                        'path': '$customer'
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
                        'customerName': '$customer.customerName', 
                        'zone': {
                            '$arrayElemAt': [
                                '$zoneResult.shortCode', 0
                            ]
                        }, 
                        'zoneId': {
                            '$toString': '$zone'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'costCenter': 1, 
                        'customerName': 1, 
                        'zone': 1, 
                       
                    }
                }
            ], 
            'as': 'costCenterResults'
        }
     }, {
        '$addFields': {
            'costCenterId': {
                '$toString': '$costCenterId'
            }, 
            'costCenter': {
                '$arrayElemAt': [
                    '$costCenterResults.costCenter', 0
                ]
            }, 
             
            'zone': {
                '$arrayElemAt': [
                    '$costCenterResults.zone', 0
                ]
            }, 
            'zoneId': {
                '$arrayElemAt': [
                    '$costCenterResults.zoneId', 0
                ]
            }, 
            'customerName': {
                '$arrayElemAt': [
                    '$costCenterResults.customerName', 0
                ]
            }, 
            'customerId': {
                '$toString': '$customerId'
            }, 
            'costCenterResults': 1, 
            'uniqueId': {
                '$toString': '$_id'
            }
        }
        }, {
                '$project': {
                    '_id': 0
                }
            }, {
            '$addFields': {
                'planGp': {
                    '$subtract': [
                        '$planRevenue', '$COGS'
                    ]
                }, 
                'actualCOGS': {
                    '$sum': [
                        '$actualSalary', '$actualVendorCost', '$otherFixedCost', '$employeeExpanse', '$miscellaneousExpenses', '$miscellaneousExpensesSecond'
                    ]
                }
                }
            }, {
                '$addFields': {
                    'actualGp': {
                    '$subtract': [
                        '$actualRevenue', '$actualCOGS'
                    ]
                    }
                }
            }, {
                '$addFields': {
                    'COGS': {
                    '$cond': [
                        {
                            '$ne': [
                                '$COGS', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$COGS', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'SGNA': {
                    '$cond': [
                        {
                            '$ne': [
                                '$SGNA', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$SGNA', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'planRevenue': {
                    '$cond': [
                        {
                            '$ne': [
                                '$planRevenue', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planRevenue', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'actualSalary': {
                    '$cond': [
                        {
                            '$ne': [
                                '$actualSalary', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualSalary', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'actualSGNA': {
                    '$cond': [
                        {
                            '$ne': [
                                '$actualSGNA', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualSGNA', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'actualVendorCost': {
                    '$cond': [
                        {
                            '$ne': [
                                '$actualVendorCost', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualVendorCost', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'actualRevenue': {
                    '$cond': [
                        {
                            '$ne': [
                                '$actualRevenue', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualRevenue', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'otherFixedCost': {
                    '$cond': [
                        {
                            '$ne': [
                                '$otherFixedCost', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$otherFixedCost', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'employeeExpanse': {
                    '$cond': [
                        {
                            '$ne': [
                                '$employeeExpanse', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$employeeExpanse', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'miscellaneousExpenses': {
                    '$cond': [
                        {
                            '$ne': [
                                '$miscellaneousExpenses', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$miscellaneousExpenses', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'miscellaneousExpensesSecond': {
                    '$cond': [
                        {
                            '$ne': [
                                '$miscellaneousExpensesSecond', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$miscellaneousExpensesSecond', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'planGp': {
                    '$cond': [
                        {
                            '$ne': [
                                '$planGp', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planGp', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'actualCOGS': {
                    '$cond': [
                        {
                            '$ne': [
                                '$actualCOGS', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualCOGS', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }, 
                'actualGp': {
                    '$cond': [
                        {
                            '$ne': [
                                '$actualGp', None
                            ]
                        }, {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualGp', 1000000
                                    ]
                                }, 2
                            ]
                        }, None
                    ]
                }
            }
            },{
                '$addFields': {
                    'gm': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$planRevenue', 0
                            ]
                        }, 
                        'then': 0, 
                        'else': {
                            '$divide': [
                                '$planGp', '$planRevenue'
                            ]
                        }
                    }
                }, 
                'actualGm': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$actualRevenue', 0
                            ]
                        }, 
                        'then': 0, 
                        'else': {
                            '$divide': [
                                '$actualGp', '$actualRevenue'
                            ]
                        }
                    }
                }
            }
            }, {
                '$addFields': {
                    'actualGm': {
                        '$cond': {
                            'if': {
                                '$isNumber': '$actualGm'
                            }, 
                            'then': {
                                '$round': [
                                    '$actualGm', 4
                                ]
                            }, 
                            'else': '$actualGm'
                        }
                    }, 
                    'gm': {
                        '$cond': {
                            'if': {
                                '$isNumber': '$gm'
                            }, 
                            'then': {
                                '$round': [
                                    '$gm', 4
                                ]
                            }, 
                            'else': '$gm'
                        }
                    }
                }
                }, {
                    '$addFields': {
                    'np': {
                        '$subtract': [
                            '$planGp', '$SGNA'
                        ]
                    }, 
                    'actualNp': {
                        '$subtract': [
                            '$actualGp', '$actualSGNA'
                        ]
                    }
                    }
                }, {
                    '$addFields': {
                        'np': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$planRevenue', 0
                                ]
                            }, 
                            'then': 0, 
                            'else': {
                                '$divide': [
                                    '$np', '$planRevenue'
                                ]
                            }
                        }
                    }, 
                    'actualNp': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$actualRevenue', 0
                                ]
                            }, 
                            'then': 0, 
                            'else': {
                                '$divide': [
                                    '$actualNp', '$actualRevenue'
                                ]
                            }
                        }
                    }
                }
            },
            ]
        
    if rqMethod == 'POST':
        if airtelId:
            query=[
                {
                    "$match":{
                        "customerId":ObjectId(airtelId)
                    }
                }
            ]+query
        
        if request.args.get("dollorView")=="true":
            query=query+[
                {
                '$lookup': {
                    'from': 'exChangeRate', 
                    'localField': 'year', 
                    'foreignField': 'year', 
                    'as': 'year'
                }
                }, {
                    '$unwind': {
                        'path': '$year'
                    }
                }, {
                    '$addFields': {
                        'year': '$year.year', 
                        'rate': '$year.rate'
                    }
                },
                {
                    '$addFields': {
                        'COGS': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$COGS', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'SGNA': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$SGNA', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'planRevenue': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planRevenue', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualSalary': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualSalary', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualSGNA': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualSGNA', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualVendorCost': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualVendorCost', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualRevenue': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualRevenue', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'otherFixedCost': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$otherFixedCost', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'employeeExpanse': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$employeeExpanse', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'miscellaneousExpenses': {
                            '$cond': {
                                'if': {
                                    '$ne': [
                                        '$miscellaneousExpenses', None
                                    ]
                                }, 
                                'then': {
                                    '$round': [
                                        {
                                            '$divide': [
                                                '$miscellaneousExpenses', '$rate'
                                            ]
                                        }, 4
                                    ]
                                }, 
                                'else': None
                            }
                        }, 
                        'miscellaneousExpensesSecond': {
                            '$cond': {
                                'if': {
                                    '$ne': [
                                        '$miscellaneousExpensesSecond', None
                                    ]
                                }, 
                                'then': {
                                    '$round': [
                                        {
                                            '$divide': [
                                                '$miscellaneousExpensesSecond', '$rate'
                                            ]
                                        }, 4
                                    ]
                                }, 
                                'else': None
                            }
                        }, 
                        'costCenterResults': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$costCenterResults', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'planGp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planGp', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualCOGS': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualCOGS', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualGp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualGp', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'gm': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$gm', '$rate'
                                    ]
                                }, 6
                            ]
                        }, 
                        'np': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$np', '$rate'
                                    ]
                                }, 6
                            ]
                        }, 
                        'actualGm': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualGm', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'actualNp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$actualNp', '$rate'
                                    ]
                                }, 6
                            ]
                        }
                    }
                },
                
            
            ]
        
        if request.args.get("filter"):
            payload=request.get_json()
            cost_center = payload.get("Cost Center", "")
            month = payload.get("month", "")
            year = payload.get("year", "")
            customerId = payload.get("Customer", "")
            businessUnit= payload.get("Business unit", "")
            cost_center_ids = [ObjectId(center) for center in cost_center.split(",") if center]
            customerId_ids = [ObjectId(customer) for customer in customerId.split(",") if customer]
            months = [int(m) for m in month.split(",") if m.isdigit()]
            years = [int(y) for y in year.split(",") if y.isdigit()]
            filter ={}
            businessUnit= [ item for item in businessUnit.split(",") if businessUnit]
            applyCummlativefilter=True
            keyToAdd=""
            groupKey=""
            if customerId:
                customerId=customerId
                applyCummlativefilter=False
                keyToAdd="customerName"
                groupKey="$customerName"
                filter["customerId"]={
                        "$in":customerId_ids
                    }
            if months:
                filter["month"]={
                        "$in":months
                    }
                if len(months)==1 and request.args.get("Cumulative")=="true" and len(cost_center_ids)==0 and  len(businessUnit)==0 and len(customerId_ids)==0:                    
                    applyCummlativefilter=False
            if years:
                filter["year"]={
                        "$in":years
                    }
            if cost_center_ids:
                filter["costCenterId"]={
                        "$in":cost_center_ids
                    }
                applyCummlativefilter=False
                keyToAdd="costCenter"
                groupKey="$costCenter"
            if businessUnit:
                filter["businessUnit"]={
                        "$in":businessUnit
                    }
                applyCummlativefilter=False
                keyToAdd="businessUnit"
                groupKey="$businessUnit"
                
            if filter:
                # print(
                #     "l;wmcklemspfomerkofjiveroifmvjkdbnvrg",query[1:]
                # )
                query=[
                    {
                        "$match":filter
                    }
                ]+(query[1:] if not airtelId else query[2:])
                # group =""
                # if customerId:
                #     group:"$customerId"
                # if businessUnit :
                #     group="$businessUnit"
                if applyCummlativefilter or  (len(months)==1 and len(cost_center_ids)==0 and  len(businessUnit)==0 and len(customerId_ids)==0):
                    query.append({
                                    '$sort': {
                                        'year': 1, 
                                        'month': 1
                                    }
                                })
                else:
                    # print("k;jncldjcndwejfnlvejriogjfiorjf=",applyCummlativefilter)
                    query=query[0:4]+[{
                                '$sort': {
                                    'year': 1, 
                                    'month': 1
                                }
                            },{
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
                                'default': 'Unknown'
                            }
                        }
                    }
                    }, {
                        '$group': {
                            '_id': groupKey , 
                            'COGS': {
                                '$sum': '$COGS'
                            }, 
                            'SGNA': {
                                '$sum': '$SGNA'
                            }, 
                            'actualRevenue': {
                                '$sum': '$actualRevenue'
                            }, 
                            'actualSGNA': {
                                '$sum': '$actualSGNA'
                            }, 
                            'actualSalary': {
                                '$sum': '$actualSalary'
                            }, 
                            'actualVendorCost': {
                                '$sum': '$actualVendorCost'
                            }, 
                            'employeeExpanse': {
                                '$sum': '$employeeExpanse'
                            }, 
                            'miscellaneousExpenses': {
                                '$sum': '$miscellaneousExpenses'
                            }, 
                            'miscellaneousExpensesSecond': {
                                '$sum': '$miscellaneousExpensesSecond'
                            }, 
                            'otherFixedCost': {
                                '$sum': '$otherFixedCost'
                            }, 
                            'planRevenue': {
                                '$sum': '$planRevenue'
                            }, 
                            'costCenterResults': {
                                '$sum': '$costCenterResults'
                            }, 
                            'planGp': {
                                '$sum': '$planGp'
                            }, 
                            'actualCOGS': {
                                '$sum': '$actualCOGS'
                            }, 
                            'actualGp': {
                                '$sum': '$actualGp'
                            }, 
                            'gm': {
                                '$sum': '$gm'
                            }, 
                            'np': {
                                '$sum': '$np'
                            }, 
                            'actualGm': {
                                '$sum': '$actualGm'
                            }, 
                            'actualNp': {
                                '$sum': '$actualNp'
                            }, 
                            'year': {
                                '$first': '$year'
                            }, 
                            'months': {
                                '$push': '$month'
                            },
                             "customerName":{
                                "$first":"$customerName"
                            },
                                                        # keyToAdd:"$_id"
                                                    }
                                                }  if  cost_center_ids else {
                        '$group': {
                            '_id': groupKey , 
                            'COGS': {
                                '$sum': '$COGS'
                            }, 
                            'SGNA': {
                                '$sum': '$SGNA'
                            }, 
                            'actualRevenue': {
                                '$sum': '$actualRevenue'
                            }, 
                            'actualSGNA': {
                                '$sum': '$actualSGNA'
                            }, 
                            'actualSalary': {
                                '$sum': '$actualSalary'
                            }, 
                            'actualVendorCost': {
                                '$sum': '$actualVendorCost'
                            }, 
                            'employeeExpanse': {
                                '$sum': '$employeeExpanse'
                            }, 
                            'miscellaneousExpenses': {
                                '$sum': '$miscellaneousExpenses'
                            }, 
                            'miscellaneousExpensesSecond': {
                                '$sum': '$miscellaneousExpensesSecond'
                            }, 
                            'otherFixedCost': {
                                '$sum': '$otherFixedCost'
                            }, 
                            'planRevenue': {
                                '$sum': '$planRevenue'
                            }, 
                            'costCenterResults': {
                                '$sum': '$costCenterResults'
                            }, 
                            'planGp': {
                                '$sum': '$planGp'
                            }, 
                            'actualCOGS': {
                                '$sum': '$actualCOGS'
                            }, 
                            'actualGp': {
                                '$sum': '$actualGp'
                            }, 
                            'gm': {
                                '$sum': '$gm'
                            }, 
                            'np': {
                                '$sum': '$np'
                            }, 
                            'actualGm': {
                                '$sum': '$actualGm'
                            }, 
                            'actualNp': {
                                '$sum': '$actualNp'
                            }, 
                            'year': {
                                '$first': '$year'
                            }, 
                            'months': {
                                '$addToSet': '$month'
                            },
                            # keyToAdd:"$_id"
                        }
                    }, {
                    "$addFields": {
                        "months": {
                            "$reduce": {
                                "input": "$months",
                    "initialValue": [],
                    "in": {
                        "$cond": {
                            "if": { "$in": ["$$this", "$$value"] },
                            "then": "$$value",
                            "else": { "$concatArrays": ["$$value", ["$$this"]] }
                        }
                    }
                                }
                            }
                        }
                    }, {
                        '$project': {
                            "customerName":1 if keyToAdd!="customerName" else 0,
                            "uniqueId":"$_id" ,
                            '_id': 1,
                            keyToAdd:"$_id" ,
                            'COGS': 1, 
                            'SGNA': 1, 
                            'actualRevenue': 1, 
                            'actualSGNA': 1, 
                            'actualSalary': 1, 
                            'actualVendorCost': 1, 
                            'employeeExpanse': 1, 
                            'miscellaneousExpenses': 1, 
                            'miscellaneousExpensesSecond': 1, 
                            'otherFixedCost': 1, 
                            'planRevenue': 1, 
                            'costCenterResults': 1, 
                            'planGp': 1, 
                            'actualCOGS': 1, 
                            'actualGp': 1, 
                            'gm': 1, 
                            'np': 1, 
                            'actualGm': 1, 
                            'actualNp': 1, 
                            'year': 1, 
                            'months': 1, 
                            'allMonthsString': {
                                '$reduce': {
                                    'input': '$months', 
                                    'initialValue': '', 
                                    'in': {
                                        '$concat': [
                                            {
                                                '$ifNull': [
                                                    '$$value', ''
                                                ]
                                            }, {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            {
                                                                '$indexOfBytes': [
                                                                    '$$value', ''
                                                                ]
                                                            }, -1
                                                        ]
                                                    }, 
                                                    'then': '', 
                                                    'else': '-'
                                                }
                                            }, {
                                                '$toString': '$$this'
                                            }
                                        ]
                                    }
                                }
                            }, 
                            'allMonthsStringFormatted': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            {
                                                '$substrBytes': [
                                                    '$allMonthsString', 0, 1
                                                ]
                                            }, '-'
                                        ]
                                    }, 
                                    'then': {
                                        '$substrBytes': [
                                            '$allMonthsString', 1, {
                                                '$strLenBytes': '$allMonthsString'
                                            }
                                        ]
                                    }, 
                                    'else': '$allMonthsString'
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'month': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            {
                                                '$substrBytes': [
                                                    '$allMonthsString', 0, 1
                                                ]
                                            }, '-'
                                        ]
                                    }, 
                                    'then': {
                                        '$substrBytes': [
                                            '$allMonthsString', 1, {
                                                '$strLenBytes': '$allMonthsString'
                                            }
                                        ]
                                    }, 
                                    'else': '$allMonthsString'
                                }
                            }
                        }
                    }, {
                        '$project': {
                            'allMonthsString': 0
                        }
                    },
       
                        ]+query[4:]+[        
                                    {
                            "$addFields":{
                                "uniqueId":1
                            }
                        }]
                    
                if airtelId:
            
                    query=[
                        {
                            "$match":{
                                "customerId":ObjectId(airtelId)
                            }
                        }
                    ]+query
                # print("klcjewshfocijeswdfkl;ml;kl;k;lkl;kiocjerfg",query)
                query.append(
                            {
                                '$project': {
                                'Year': '$year',
                                'month':1, 
                                'Month': {
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
                                        'default': '$month'
                                    }
                                    },  
                                'UST Project ID': '$ustProjectID', 
                                "Customer":"$customerName",
                                'Cost Center': '$costCenter', 
                                'Zone':"$zone",
                                'Business Unit': '$businessUnit', 
                                'Planned Revenue': '$planRevenue', 
                                'Planned COGS': '$COGS', 
                                "Planned Gross Profit":"$planGp",
                                # "Planned Gross Margin":"$gm",
                                "Planned Gross Margin(%)": {
                                    "$round": [
                                        { "$multiply": ["$gm", 100] }, 
                                        2
                                    ]
                                    },
                                'Planned SGNA': '$SGNA', 
                                'Planned Net Profit(%)':{
                                    "$round": [
                                        { "$multiply": ["$np", 100] }, 
                                        2
                                    ]
                                    },
                                'Actual Revenue': '$actualRevenue', 
                                'Actual Salary': '$actualSalary', 
                                'Actual Vendor Cost': '$actualVendorCost', 
                                "Actual COGS": "$actualCOGS",
                                "Actual Gross Profit": "$actualGp",
                                "Actual Gross Margin(%)":{
                                    "$round": [
                                        { "$multiply": ["$actualGm", 100] }, 
                                        2
                                    ]
                                    },
                                'Actual SGNA': '$actualSGNA', 
                                'Actual Net Profit(%)':{
                                    "$round": [
                                        { "$multiply": ["$actualNp", 100] },
                                        2
                                    ]
                                    },
                                'Employee Expanse': '$employeeExpanse', 
                                'Other Fixed Cost': '$otherFixedCost', 
                                
                                'Miscellaneous Expenses': '$miscellaneousExpenses', 
                                'Miscellaneous Expenses Second': '$miscellaneousExpensesSecond',
                                'month':1
                        }
                    }
                )
                print("kljhgvchvhbjklhgvcvh",query)
                resp=cmo.finding_aggregate("AOP",filter1+query)
                # if applyCummlativefilter==True:
                #     # data=get_progressive_data(resp['data'], months)
                #     resp['data']=data
                print('djkjhebdgyugehyjrbgryhjurgh',resp,type(resp))    
                dataframe = pd.DataFrame(resp['data'])
                if 'month' in dataframe:
                    dataframe.drop(columns=['month'], inplace=True)
                # dataframe=dataframe[sequenceList]
                fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_AOP", "AOP")
                return send_file(fullPath)
                # return respond(resp)
        
        
        
        query.append(
                            {
                                '$project': {
                                'Year': '$year',
                                'month':1, 
                                'Month': {
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
                                        'default': ''
                                    }
                                    },  
                                'UST Project ID': '$ustProjectID', 
                                "Customer":"$customerName",
                                'Cost Center': '$costCenter', 
                                'Zone':"$zone",
                                'Business Unit': '$businessUnit', 
                                'Planned Revenue': '$planRevenue', 
                                'Planned COGS': '$COGS', 
                                "Planned Gross Profit":"$planGp",
                                # "Planned Gross Margin":"$gm",
                                "Planned Gross Margin(%)": {
                                    "$round": [
                                        { "$multiply": ["$gm", 100] }, 
                                        2
                                    ]
                                    },
                                'Planned SGNA': '$SGNA', 
                                'Planned Net Profit(%)':{
                                    "$round": [
                                        { "$multiply": ["$np", 100] }, 
                                        2
                                    ]
                                    },
                                'Actual Revenue': '$actualRevenue', 
                                'Actual Salary': '$actualSalary', 
                                'Actual Vendor Cost': '$actualVendorCost', 
                                "Actual COGS": "$actualCOGS",
                                "Actual Gross Profit": "$actualGp",
                                "Actual Gross Margin(%)":{
                                    "$round": [
                                        { "$multiply": ["$actualGm", 100] }, 
                                        2
                                    ]
                                    },
                                'Actual SGNA': '$actualSGNA', 
                                'Actual Net Profit(%)':{
                                    "$round": [
                                        { "$multiply": ["$actualNp", 100] },
                                        2
                                    ]
                                    },
                                'Employee Expanse': '$employeeExpanse', 
                                'Other Fixed Cost': '$otherFixedCost', 
                                
                                'Miscellaneous Expenses': '$miscellaneousExpenses', 
                                'Miscellaneous Expenses Second': '$miscellaneousExpensesSecond',
                                'month':1
                        }
                    }
                )
        
            
        
        resp=cmo.finding_aggregate("AOP",filter1+query)
        if Cumulative:
            if(Cumulative.lower()=="true"):
                sequenceList= ["Year", "Month", "UST Project ID", "Customer", "Cost Center", "Business Unit", "Planned Revenue", "Planned COGS", "Planned Gross Profit","Planned SGNA","Actual Revenue","Actual Salary","Actual Vendor Cost","Actual COGS","Actual SGNA","Actual Gross Profit","Employee Expanse","Other Fixed Cost","Miscellaneous Expenses","Miscellaneous Expenses Second"]
                if monthsFilter:
                    data=get_progressive_data_export(resp['data'],monthsFilter)
                    dataframe = pd.DataFrame(data)
                    if 'month' in dataframe:
                        dataframe.drop(columns=['month'], inplace=True)
                    # dataframe=dataframe[sequenceList]
                    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_AOP", "AOP")
                    return send_file(fullPath)
                else:
                    data=get_progressive_data_export(resp['data'], get_previous_months_Arrey())
                    dataframe = pd.DataFrame(data)
                    if 'month' in dataframe:
                        dataframe.drop(columns=['month'], inplace=True)
                    # dataframe=dataframe[sequenceList]
                    fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_AOP", "AOP")
                    return send_file(fullPath)
            
        dataframe = pd.DataFrame(resp['data'])
        if 'month' in dataframe:
            dataframe.drop(columns=['month'], inplace=True) 
        
        fullPath = excelWriteFunc.excelFileWriter(dataframe, "Export_AOP", "AOP")
        return send_file(fullPath) 
  


@forms_blueprint.route("/ForecastCOGS",methods=['GET','POST'])  
@forms_blueprint.route("/ForecastCOGS/<id>",methods=['GET','POST'])  
# @token_required
def ForecastCOGS(id=None):
    airtelId=None
    monthList=get_next_six_months()
    print(monthList,'jkjkdjkjkdjdkjkd')
    rqMethod=request.method
    query =[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            },
            {
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
                            '$lookup': {
                                'from': 'customer', 
                                'localField': 'customer', 
                                'foreignField': '_id', 
                                'as': 'customer'
                            }
                        }, {
                            '$unwind': {
                                'path': '$customer'
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
                                'customerName': '$customer.customerName', 
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResult.shortCode', 0
                                    ]
                                }, 
                                'zoneId': {
                                    '$toString': '$zone'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'costCenter': 1, 
                                'customerName': 1, 
                                'zone': 1
                            }
                        }
                    ], 
                    'as': 'costCenterResults'
                }
            },
            {
                '$addFields': {
                    'costCenterId': {
                        '$toString': '$costCenterId'
                    }, 
                    'costCenter': {
                        '$arrayElemAt': [
                            '$costCenterResults.costCenter', 0
                        ]
                    }, 
                    'zone': {
                        '$arrayElemAt': [
                            '$costCenterResults.zone', 0
                        ]
                    }, 
                    'zoneId': {
                        '$arrayElemAt': [
                            '$costCenterResults.zoneId', 0
                        ]
                    }, 
                    'customerName': {
                        '$arrayElemAt': [
                            '$costCenterResults.customerName', 0
                        ]
                    }, 
                    'customerId': {
                        '$toString': '$customerId'
                    }, 
                    'costCenterResults': 1, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }, 
            {
                '$project': {
                    '_id': 0
                }
            },
            {
                '$addFields': {
                    'planGp': {
                        '$subtract': [
                            '$planRevenue', '$COGS'
                        ]
                    }, 
                    'customMonth': {
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
                            'default': ''
                        }
                    }, 
                    'customYear': {
                        '$toString': '$year'
                    }
                }
            },
            {
                '$addFields': {
                    'plannedgm': {
                        '$divide': [
                            '$planGp', '$planRevenue'
                        ]
                    }, 
                    'customMonth': {
                        '$concat': [
                            '$customMonth', '-', '$customYear'
                        ]
                    }
                }
            },
            {
                '$addFields': {
                    'plannedgm': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$plannedgm', None
                                ]
                            }, 
                            'then': {
                                '$round': [
                                    '$plannedgm', 2
                                ]
                            }, 
                            'else': ''
                        }
                    }
                }
            }, 
            {
                '$project': {
                    'SGNA': 0, 
                    'actualSalary': 0, 
                    'actualSGNA': 0, 
                    'actualVendorCost': 0, 
                    'actualRevenue': 0, 
                    'miscellaneousExpenses': 0, 
                    'miscellaneousExpensesSecond': 0, 
                    'otherFixedCost': 0, 
                    'employeeExpanse': 0, 
                    'customYear': 0
                }
            }, 
            
        ]
    if rqMethod == 'GET':
        
        if request.args.get("dollorView")=="true":
            query=query+[
                {
                    '$lookup': {
                        'from': 'exChangeRate', 
                        'localField': 'year', 
                        'foreignField': 'year', 
                        'as': 'exChange'
                    }
                }, {
                    '$unwind': {
                        'path': '$exChange'
                    }
                }, {
                    '$addFields': {
                        'year': '$exChange.year', 
                        'rate': '$exChange.rate'
                    }
                }, {
                    '$addFields': {
                        'COGS': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$COGS', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'planRevenue': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planRevenue', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'costCenterResults': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$costCenterResults', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'planGp': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$planGp', '$rate'
                                    ]
                                }, 4
                            ]
                        }, 
                        'plannedgm': {
                            '$round': [
                                {
                                    '$divide': [
                                        '$plannedgm', '$rate'
                                    ]
                                }, 4
                            ]
                        }
                    }
                },
                ]
        # arrrr=getAOPExpenseANDRevenue(current_user)
        query=query+[{
                '$group': {
                    '_id': '$customMonth', 
                    'data': {
                        '$push': {
                            'COGS': '$COGS', 
                            'costCenterId': '$costCenterId', 
                            'customerId': '$customerId', 
                            'month': '$month', 
                            'planRevenue': '$planRevenue', 
                            'ustProjectID': '$ustProjectID', 
                            'year': '$year', 
                            'businessUnit': '$businessUnit', 
                            'costCenterResults': '$costCenterResults', 
                            'costCenter': '$costCenter', 
                            'zone': '$zone', 
                            'customerName': '$customerName', 
                            'uniqueId': '$uniqueId', 
                            'planGp': '$planGp', 
                            'customMonth': '$customMonth', 
                            'plannedgm': '$plannedgm'
                        }
                    }
                }
            }, {
                '$project': {
                    'customMonth': '$_id', 
                    'data': 1, 
                    '_id': 0
                }
            }
            ]
        resp=cmo.finding_aggregate("AOP",query)
        return respond(resp)
    
@forms_blueprint.route("/form/circle/<id>",methods=["GET"])
@token_required
def forms_circle(current_user):
    arra = [
        {
            '$match': {
                'customer': ObjectId(id)
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
    res = cmo.finding_aggregate("circle",arra)
    return respond(res)