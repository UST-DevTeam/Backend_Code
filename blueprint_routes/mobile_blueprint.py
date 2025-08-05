from base import *
from datetime import datetime as dtt
from datetime import timedelta
import pytz
from common.config import generate_new_ExpenseNo as generate_new_ExpenseNo
from common.config import *
import common.excel_write as excelWriteFunc
from blueprint_routes.project_blueprint import current_time
from blueprint_routes.project_blueprint import current_time as current_date1
from common.mongo_operations import db as database

tz = pytz.timezone("Asia/Kolkata")
current_date = datetime.now(tz)
three_days_ago = current_date - timedelta(days=3)
current_date = dtt.now()

def currentFinancialMonth():
    tz = pytz.timezone("Asia/Kolkata")
    current_date = datetime.now(tz)
    three_days_ago = current_date - timedelta(days=3)
    current_date = dtt.now()
    current_year = current_date.year
    current_month = current_date.strftime("%b").upper()
    if current_date.month >= 4:
        fiscal_year = f"{current_year % 100}-{(current_year + 1) % 100}"
    else:
        fiscal_year = f"{(current_year - 1) % 100}-{current_year % 100}"
    currentFinancialMonth = f"{fiscal_year}"
    return currentFinancialMonth


mobile_blueprint = Blueprint("mobile_blueprint", __name__)

def form_changer(text):
    return str(text)


def ExpenseNonewLogic():
    ExpenseNo2 = "EXP"
    ExpenseNo2 = f"{ExpenseNo2}/{currentFinancialMonth()}/"
    counter = database.fileDetail.find_one_and_update({"id": "expenseIdCounter"},{"$inc": {"sequence_value": 1}},return_document=True,upsert=True)
    sequence_value = counter["sequence_value"]
    sequence_value=str(sequence_value).zfill(6)
    ExpenseNo2=ExpenseNo2+str(sequence_value)
    return ExpenseNo2




def AdvanceNonewLogic():
    AdvanceNo2 = "ADV"
    AdvanceNo2 = f"{AdvanceNo2}/{currentFinancialMonth()}/"
    counter = database.fileDetail.find_one_and_update({"id": "AdvanceIdCounter"},{"$inc": {"sequence_value": 1}},return_document=True,upsert=True)
    sequence_value = counter["sequence_value"]
    sequence_value=str(sequence_value).zfill(6)
    AdvanceNo2=AdvanceNo2+str(sequence_value)
    return AdvanceNo2
  

# @mobile_blueprint.route('/mobile/project/myTask',methods=['GET'])
# @token_required
# def project_mytask(current_user):
#     if request.method == "GET":
#         taskStatus = ["Open","In Process","Reject"]
#         if request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined":
#             taskStatus = ["Open","Closed"]
#         if request.args.get("mileStoneStatus")!=None and request.args.get("mileStoneStatus")!='undefined':
#             taskStatus = [request.args.get("mileStoneStatus")]
#         if taskStatus == ["both"]:
#             taskStatus = ["Open","Closed"]
            
#         arra = [
#             {
#                 '$match':{
#                     'deleteStatus':{'$ne':1},
#                     "mileStoneStatus":{
#                         '$in':taskStatus
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'matchingIndex': {
#                         '$indexOfArray': [
#                             '$assignerId', ObjectId(current_user['userUniqueId'])
#                         ]
#                     }
#                 }
#             }, {
#                 '$match': {
#                     '$and': [
#                         {
#                             'matchingIndex': {
#                                 '$ne': -1
#                             }
#                         }, {
#                             'matchingIndex': {
#                                 '$ne': None
#                             }
#                         }
#                     ]
#                 }
#             }, {
#                 '$addFields': {
#                     'projectuniqueId': {
#                         '$toObjectId': '$projectuniqueId'
#                     }, 
#                     'empId': {
#                         '$arrayElemAt': [
#                             '$assignerId', '$matchingIndex'
#                         ]
#                     }, 
#                     'uniqueId': {
#                         '$toString': '$siteId'
#                     }
#                 }
#             }
#         ]
#         arra = arra + apireq.countarra("milestone",arra) + apireq.args_pagination(request.args)
#         arra = arra +[
#             {
#                 '$lookup': {
#                     'from': 'project', 
#                     'localField': 'projectuniqueId', 
#                     'foreignField': '_id', 
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 'deleteStatus': {
#                                     '$ne': 1
#                                 }
#                             }
#                         }, {
#                             '$addFields': {
#                                 'circle': {
#                                     '$toObjectId': '$circle'
#                                 }
#                             }
#                         }, {
#                             '$lookup': {
#                                 'from': 'circle', 
#                                 'localField': 'circle', 
#                                 'foreignField': '_id', 
#                                 'as': 'circleResult'
#                             }
#                         }, {
#                             '$addFields': {
#                                 'circle': {
#                                     '$arrayElemAt': [
#                                         '$circleResult.circleName', 0
#                                     ]
#                                 }
#                             }
#                         }
#                     ], 
#                     'as': 'projectResult'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'userRegister', 
#                     'localField': 'empId', 
#                     'foreignField': '_id', 
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 'deleteStatus': {
#                                     '$ne': 1
#                                 }
#                             }
#                         }
#                     ], 
#                     'as': 'empResult'
#                 }
#             }, {
#                 '$addFields': {
#                     'circle': {
#                         '$arrayElemAt': [
#                             '$projectResult.circle', 0
#                         ]
#                     }, 
#                     'projectId': {
#                         '$arrayElemAt': [
#                             '$projectResult.projectId', 0
#                         ]
#                     }, 
#                     'empName': {
#                         '$arrayElemAt': [
#                             '$empResult.empName', 0
#                         ]
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'planStartDate': {
#                         '$dateToString': {
#                             'date': {
#                                 '$toDate': '$mileStoneStartDate'
#                             }, 
#                             'format': '%d-%m-%Y', 
#                             'timezone': 'Asia/Kolkata'
#                         }
#                     }, 
#                     'planEndDate': {
#                         '$dateToString': {
#                             'date': {
#                                 '$toDate': '$mileStoneStartDate'
#                             }, 
#                             'format': '%d-%m-%Y', 
#                             'timezone': 'Asia/Kolkata'
#                         }
#                     }, 
#                     'completionDate': {
#                         '$dateToString': {
#                             'date': {
#                                 '$toDate': '$CC_Completion Date'
#                             }, 
#                             'format': '%d-%m-%Y', 
#                             'timezone': 'Asia/Kolkata'
#                         }
#                     }, 
#                     'status': '$mileStoneStatus', 
#                     'milestoneId': {
#                         '$toString': '$_id'
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'newField': '$$ROOT'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'SiteEngineer', 
#                     'localField': 'siteId', 
#                     'foreignField': '_id', 
#                     'as': 'siteResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$siteResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$addFields': {
#                     'newObject': {
#                         '$mergeObjects': [
#                             '$newField', '$siteResult'
#                         ]
#                     }
#                 }
#             }, {
#                 '$replaceRoot': {
#                     'newRoot': '$newObject'
#                 }
#             }, {
#                 '$addFields': {
#                     'uniqueId': {
#                         '$toString': '$siteId'
#                     }, 
#                     'siteId': '$Site Id', 
#                     'projectuniqueId': {
#                         '$toString': '$projectuniqueId'
#                     },
#                     'SubProjectId': {
#                         '$toObjectId': '$SubProjectId'
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'projectType', 
#                     'localField': 'SubProjectId', 
#                     'foreignField': '_id', 
#                     'as': 'result'
#                 }
#             }, {
#                 '$addFields': {
#                     'projectType': {
#                         '$arrayElemAt': [
#                             '$result.projectType', 0
#                         ]
#                     }, 
#                     'subProject': {
#                         '$arrayElemAt': [
#                             '$result.subProject', 0
#                         ]
#                     },
#                     'SubProjectId': {
#                         '$toString': '$SubProjectId'
#                     }
#                 }
#             }, {
#                 '$project': {
#                     '_id': 0, 
#                     'assignerId': 0, 
#                     'empId': 0, 
#                     'mileStoneStatus': 0, 
#                     'isLast': 0, 
#                     'projectResult': 0, 
#                     'empResult': 0,
#                     'result':0,
#                 }
#             }
#         ]
#         response = cmo.finding_aggregate("milestone",arra)
#         return respond(response)
  
@mobile_blueprint.route('/mobile/project/myTask',methods=['GET'])
@token_required
def project_mytask(current_user):
    if request.method == "GET":
        taskStatus = ["Open","In Process","Reject"]
        if request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined":
            taskStatus = ["Open","Closed"]
        if request.args.get("mileStoneStatus")!=None and request.args.get("mileStoneStatus")!='undefined':
            taskStatus = [request.args.get("mileStoneStatus")]
        if taskStatus == ["both"]:
            taskStatus = ["Open","In Process","Submit","Approve","Reject","Submit to Airtel","Closed"]  
        arra = [
    {
        '$match': {
            'deleteStatus': {
                '$ne': 1
            }, 
            'mileStoneStatus': {
                '$in': taskStatus
            }
        }
    }, {
        '$addFields': {
            'matchingIndex': {
                '$indexOfArray': [
                    '$assignerId',  ObjectId(current_user['userUniqueId'])
                ]
            }
        }
    }, {
        '$match': {
            '$and': [
                {
                    'matchingIndex': {
                        '$ne': -1
                    }
                }, {
                    'matchingIndex': {
                        '$ne': None
                    }
                }
            ]
        }
    }, {
        '$addFields': {
            'projectuniqueId': {
                '$toObjectId': '$projectuniqueId'
            }, 
            'empId': {
                '$arrayElemAt': [
                    '$assignerId', '$matchingIndex'
                ]
            }, 
            'uniqueId': {
                '$toString': '$siteId'
            }
        }
    },
    {
        '$addFields': {
            'newID': {
                '$toString': '$_id'
            }
        }
    },
         {
        '$lookup': {
            'from': 'ptwRaiseTicket', 
            'localField': 'newID', 
            'foreignField': 'mileStoneId', 
             'pipeline': [
                {
                    '$match': {
                        'ptwNumber': {
                            '$exists': True
                        },
                        'ptwDeleteStatus':{'$ne':1}
                    }
                }
            ],
            'as': 'ptwData'
        }
    },{
        '$addFields': {
            'isPtwRaise':  {
                '$cond': {
                    'if': {
                        '$gt': [
                            {
                                '$size': '$ptwData'
                            }, 0
                        ]
                    }, 
                    'then': True, 
                    'else': False
                }
            },
            'isL2Approve': {
                '$cond': {
                    'if': {
                        '$gt': [
                            {
                                '$size': '$ptwData'
                            }, 0
                        ]
                    }, 
                    'then': {
                        '$arrayElemAt': [
                            '$ptwData.isL2Approved', 0
                        ]
                    }, 
                    'else': False
                }
            }, 
            
            'ptwNumber': {
                '$cond': {
                    'if': {
                    '$and': [
                        { '$gt': [ { '$size': '$ptwData' }, 0 ] },
                        {
                        '$ne': [
                            {
                            '$type': {
                                '$getField': {
                                'field': 'ptwNumber',
                                'input': { '$arrayElemAt': [ '$ptwData', 0 ] }
                                }
                            }
                            }, 'missing'
                        ]
                        }
                    ]
                    },
                    'then': {
                    '$getField': {
                        'field': 'ptwNumber',
                        'input': { '$arrayElemAt': [ '$ptwData', 0 ] }
                    }
                    },
                    'else': None
                }
                }

        }
        
    }
        ]
        arra = arra + apireq.countarra("milestone",arra) + apireq.args_pagination(request.args)
        arra = arra +[
            {
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
                        'circle': {
                            '$toObjectId': '$circle'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'circle', 
                        'localField': 'circle', 
                        'foreignField': '_id', 
                        'as': 'circleResult'
                    }
                }, {
                    '$addFields': {
                        'circle': {
                            '$arrayElemAt': [
                                '$circleResult.circleName', 0
                            ]
                        }
                    }
                }
            ], 
            'as': 'projectResult'
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'empId', 
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
            'as': 'empResult'
        }
    }, {
        '$addFields': {
            'circle': {
                '$arrayElemAt': [
                    '$projectResult.circle', 0
                ]
            }, 
            'projectId': {
                '$arrayElemAt': [
                    '$projectResult.projectId', 0
                ]
            }, 
            'empName': {
                '$arrayElemAt': [
                    '$empResult.empName', 0
                ]
            }
        }
    }, {
        '$addFields': {
            'planStartDate': {
                '$dateToString': {
                    'date': {
                        '$toDate': '$mileStoneStartDate'
                    }, 
                    'format': '%d-%m-%Y', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'planEndDate': {
                '$dateToString': {
                    'date': {
                        '$toDate': '$mileStoneStartDate'
                    }, 
                    'format': '%d-%m-%Y', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'completionDate': {
                '$dateToString': {
                    'date': {
                        '$toDate': '$CC_Completion Date'
                    }, 
                    'format': '%d-%m-%Y', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'status': '$mileStoneStatus', 
            'milestoneId': {
                '$toString': '$_id'
            }
        }
    }, {
        '$addFields': {
            'newField': '$$ROOT'
        }
    }, {
        '$lookup': {
            'from': 'SiteEngineer', 
            'localField': 'siteId', 
            'foreignField': '_id', 
            'as': 'siteResult'
        }
    }, {
        '$unwind': {
            'path': '$siteResult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'newObject': {
                '$mergeObjects': [
                    '$newField', '$siteResult'
                ]
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$newObject'
        }
    }, {
        '$addFields': {
            'uniqueId': {
                '$toString': '$siteId'
            }, 
            'siteId': '$Site Id', 
            'projectuniqueId': {
                '$toString': '$projectuniqueId'
            }, 
            'SubProjectId': {
                '$toObjectId': '$SubProjectId'
            }
        }
    }, {
        '$lookup': {
            'from': 'projectType', 
            'localField': 'SubProjectId', 
            'foreignField': '_id', 
            'as': 'result'
        }
    }, {
        '$addFields': {
            'projectType': {
                '$arrayElemAt': [
                    '$result.projectType', 0
                ]
            }, 
            'subProject': {
                '$arrayElemAt': [
                    '$result.subProject', 0
                ]
            }, 
            'SubProjectId': {
                '$toString': '$SubProjectId'
            }
        }
    }, {
        '$addFields': {
            'milestone': '$Name'
        }
    }, {
        '$addFields': {
            'customerId': {
                '$toObjectId': '$customerId'
            }
        }
    }, {
        '$lookup': {
            'from': 'customer', 
            'localField': 'customerId', 
            'foreignField': '_id', 
            'as': 'customerResult'
        }
    }, {
        '$unwind': {
            'path': '$customerResult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'customer': '$customerResult.customerName', 
            'ACTIVITY': {
                '$ifNull': [
                    '$ACTIVITY', None
                ]
            },
            'customerId': {
                '$toString': '$customerId'
            }, 'closeAt': {
                '$arrayElemAt': [
                    '$ptwData.closeAt', 0
                ]
            }
        }
    },{
        '$addFields': {
            'ptwStatus':{
                '$cond': {
                    'if': {
                        '$gt': [
                            {
                                '$size': '$ptwData'
                            }, 0
                        ]
                    }, 
                    'then': {
                        '$arrayElemAt': [
                            '$ptwData.status', 0
                        ]
                    }, 
                    'else': None
                }
            },
        }
    },
    {
        '$project': {
            '_id': 0, 
            'assignerId': 0, 
            'empId': 0, 
            'mileStoneStatus': 0, 
            'ptwData': 0,
            'isLast': 0, 
            'projectResult': 0, 
            'empResult': 0, 
            'result': 0, 
            'customerResult': 0
        }
    }
        ]
        
        print(arra,"qwerttyuiopqwertyuiop")
        
        response = cmo.finding_aggregate("milestone",arra)
        
        # print(response,"thiskajsjdfkjasdlkfjs")
        return respond(response)
  
  
@mobile_blueprint.route('/mobile/dropdownField',methods=['GET','POST'])  
@mobile_blueprint.route('/mobile/dropdownField/<id>',methods=['GET','POST']) 
@token_required 
def dropdownField(current_user,id=None):
    if request.method == "GET":
        if id not in ['',None,'undefined']:
            arr=[
                {
                    '$match': {
                        '_id': ObjectId(id), 
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
                            }, {
                                '$match': {
                                    't_sengg.dataType': 'Auto Created'
                                }
                            }, {
                                '$project': {
                                    't_sign': {
                                        '$filter': {
                                            'input': '$t_sengg', 
                                            'as': 'item', 
                                            'cond': {
                                                '$eq': [
                                                    '$$item.dataType', 'Auto Created'
                                                ]
                                            }
                                        }
                                    }
                                }
                            }, {
                                '$unwind': {
                                    'path': '$t_sign', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'dropdownValue': '$t_sign.dropdownValue'
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
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0, 
                        'dropdownValue': '$result.dropdownValue'
                    }
                }
            ]
            Response=cmo.finding_aggregate("milestone",arr)
            return respond(Response)
        else:
            return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "Please Provide a valid ID",
                    }
                )
                 

@mobile_blueprint.route("/mobile/getOneSiteEngg/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def mobileonesiteengg(id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id), 
                    'deleteStatus': {
                        '$ne': 1
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
                        }
                    ], 
                    'as': 'siteArray'
                }
            }, {
                '$unwind': {
                    'path': '$siteArray', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'siteArray.mileStoneStatus': '$mileStoneStatus', 
                    'siteArray.Completion Criteria': '$Completion Criteria'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$siteArray'
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
                        }, {
                            '$addFields': {
                                '_id': {
                                    '$toString': '$_id'
                                }
                            }
                        }, {
                            '$project': {
                                't_sengg': 1, 
                                't_tracking': 1, 
                                't_issues': 1, 
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
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'SubProjectId': 0
                }
            }
        ]
        response = cmo.finding_aggregate("milestone", arra)
        return respond(response)
    
    
    
@mobile_blueprint.route("/mobile/claimAndAdvance", methods=["GET", "POST", "PUT", "DELETE"])
@mobile_blueprint.route("/mobile/claimAndAdvance/<id>", methods=["GET", "POST", "PUT", "DELETE"])
@token_required
def claimsAndAdvance(current_user, id=None):
    Number = request.args.get("Number")
    
    if request.method == "GET":
        
        if Number != None and Number != "undefined":
            type = Number[:3]
            if type == "EXP":
                arr = [
                    {"$match": {"ExpenseNo": Number}},
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "addedFor",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {
                                    "$addFields": {
                                        "designation": {"$toObjectId": "$designation"}
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "designation",
                                        "localField": "designation",
                                        "foreignField": "_id",
                                        "pipeline": [
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
                                        ],
                                        "as": "designationResult",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$designationResult",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "designation": "$designationResult.designation"
                                    }
                                },
                            ],
                            "as": "l1approver",
                        }
                    },
                    {"$addFields": {"length": {"$size": "$l1approver"}}},
                    {"$match": {"length": {"$ne": 0}}},
                    {
                        "$unwind": {
                            "path": "$l1approver",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "project",
                            "localField": "projectId",
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
                                                        },
                                                        {
                                                            "$project": {
                                                                "_id": 0,
                                                                "costCenter": 1,
                                                            }
                                                        },
                                                    ],
                                                    "as": "costcenterresult",
                                                }
                                            },
                                            {
                                                "$addFields": {
                                                    "costcenter": "$costcenterresult.costCenter"
                                                }
                                            },
                                            {
                                                "$unwind": {
                                                    "path": "$costcenter",
                                                    "preserveNullAndEmptyArrays": True,
                                                }
                                            },
                                        ],
                                        "as": "projectGroupResult",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$projectGroupResult",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "costcenter": "$projectGroupResult.costcenter",
                                        "circle": {"$toObjectId": "$circle"},
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "circle",
                                        "localField": "circle",
                                        "foreignField": "_id",
                                        "pipeline": [
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
                                        ],
                                        "as": "circle",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$circle",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {"$addFields": {"circle": "$circle.circleName"}},
                            ],
                            "as": "projectResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$projectResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "claimType",
                            "localField": "claimType",
                            "foreignField": "_id",
                            "as": "claimResults",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$claimResults",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "empName": "$l1approver.empName",
                            "empCode": "$l1approver.empCode",
                            "designation": "$l1approver.designation",
                            "circle": "$projectResult.circle",
                            "costcenter": "$projectResult.costcenter",
                            "projectIdName": "$projectResult.projectId",
                            "CreatedAt": {"$toDate": "$CreatedAt"},
                            "claimType": "$claimResults.claimType",
                        }
                    },
                    {
                        "$addFields": {
                            "expenseDate": {
                                "$dateToString": {
                                    "format": "%d-%m-%Y",
                                    "date": "$CreatedAt",
                                }
                            },
                            "expensemonth": {
                                "$dateToString": {
                                    "format": "%m-%Y",
                                    "date": "$CreatedAt",
                                }
                            },
                            "projectId": {"$toString": "$projectId"},
                            "uniqueId": {"$toString": "$_id"},
                            "addedFor": {"$toString": "$addedFor"},
                            "name": "$ExpenseNo",
                            "types": "$claimType",
                            "addedBy": {"$toString": "$addedBy"},
                            "expenseuniqueId": {"$toString": "$_id"},
                        }
                    },
                    {
                        "$lookup": {
                            "from": "milestone",
                            "localField": "taskName",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$project": {"_id": 1, "Name": 1}},
                            ],
                            "as": "taskResults",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "SiteEngineer",
                            "localField": "Site Id",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$project": {"_id": 1, "Site Id": 1}},
                            ],
                            "as": "SiteResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$SiteResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$taskResults",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "Site_Id": "$SiteResult.Site Id",
                            "Task": "$taskResults.Name",
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {
                        "$project": {
                            "_id": 0,
                            "taskName": 0,
                            "Site Id": 0,
                            "CreatedAt": 0,
                            "l1approver": 0,
                            "claimResults": 0,
                            "projectResult": 0,
                            "actionBy": 0,
                            "addedby": 0,
                            "addedBy": 0,
                            "ExpenseUniqueId": 0,
                            "L1Approver": 0,
                            "L2Approver": 0,
                            "L3Approver": 0,
                            "SiteResult": 0,
                            "taskResults": 0,
                        }
                    },
                ]
                arr = arr + apireq.commonarra + apireq.args_pagination(request.args)

                Response = cmo.finding_aggregate("Expenses", arr)
                return respond(Response)
            else:
                arr = [
                    {
                        "$match": {
                            "deleteStatus": {"$ne": 1},
                            "addedFor": ObjectId(current_user["userUniqueId"]),
                            "AdvanceNo": Number,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "addedFor",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "approverDetails",
                        }
                    },
                    {"$addFields": {"length": {"$size": "$approverDetails"}}},
                    {"$match": {"length": {"$ne": 0}}},
                    {
                        "$unwind": {
                            "path": "$approverDetails",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "designation",
                            "localField": "designation",
                            "foreignField": "_id",
                            "as": "designationResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$designationResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "project",
                            "localField": "projectId",
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
                                                        },
                                                        {
                                                            "$project": {
                                                                "_id": 0,
                                                                "costCenter": 1,
                                                            }
                                                        },
                                                    ],
                                                    "as": "costcenterresult",
                                                }
                                            },
                                            {
                                                "$addFields": {
                                                    "costcenter": "$costcenterresult.costCenter"
                                                }
                                            },
                                            {
                                                "$unwind": {
                                                    "path": "$costcenter",
                                                    "preserveNullAndEmptyArrays": True,
                                                }
                                            },
                                        ],
                                        "as": "projectGroupResult",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$projectGroupResult",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "costcenter": "$projectGroupResult.costcenter",
                                        "circle": {"$toObjectId": "$circle"},
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "circle",
                                        "localField": "circle",
                                        "foreignField": "_id",
                                        "pipeline": [
                                            {"$match": {"deleteStatus": {"$ne": 1}}}
                                        ],
                                        "as": "circle",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$circle",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {"$addFields": {"circle": "$circle.circleName"}},
                            ],
                            "as": "projectResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$projectResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "CreatedAt": {"$toDate": "$CreatedAt"},
                            "projectId": {"$toString": "$projectId"},
                            "circle": "$projectResult.circle",
                            "costcenter": "$projectResult.costcenter",
                            "empName": "$approverDetails.empName",
                            "empCode": "$approverDetails.empCode",
                            "designation": "$designationResult.designation",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "claimType",
                            "localField": "advanceTypeId",
                            "foreignField": "_id",
                            "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                            "as": "claimTypeAdvance",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$claimTypeAdvance",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "projectIdName": "$projectResult.projectId",
                            "costcenter": "$projectResult.costcenter",
                            "uniqueId": {"$toString": "$_id"},
                            "CreatedAt": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$CreatedAt",
                                }
                            },
                            "expenseDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%Y",
                                    "date": "$CreatedAt",
                                }
                            },
                            "expensemonth": {
                                "$dateToString": {
                                    "format": "%m-%Y",
                                    "date": "$CreatedAt",
                                }
                            },
                            "addedFor": {"$toString": "$addedFor"},
                            "name": "$AdvanceNo",
                            "types": "$claimTypeAdvance.claimType",
                            "advanceType": "$claimTypeAdvance.claimType",
                            "advanceTypeId": {"$toString": "$_id"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "projectResult": 0,
                            "addedAt": 0,
                            "approverDetails": 0,
                            "designationResult": 0,
                            "length": 0,
                            "actionBy": 0,
                            "AdvanceUniqueId": 0,
                            "L1Approver": 0,
                            "L2Approver": 0,
                            "L3Approver": 0,
                            "claimTypeAdvance": 0,
                        }
                    },
                ]
                arr = arr + apireq.commonarra + apireq.args_pagination(request.args)

                Response = cmo.finding_aggregate("Advance", arr)
                return respond(Response)
        else:
            arr = [
            {"$match": {"_id": ObjectId(current_user["userUniqueId"])}}, {
            '$lookup': {
            'from': 'Expenses', 
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
                    '$lookup': {
                        'from': 'project', 
                        'localField': 'projectId', 
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
                                                        '$project': {
                                                            '_id': 0, 
                                                            'costCenter': 1
                                                        }
                                                    }
                                                ], 
                                                'as': 'costcenterresult'
                                            }
                                        }, {
                                            '$addFields': {
                                                'costcenter': '$costcenterresult.costCenter'
                                            }
                                        }, {
                                            '$unwind': {
                                                'path': '$costcenter', 
                                                'preserveNullAndEmptyArrays': True
                                            }
                                        }
                                    ], 
                                    'as': 'projectGroupResult'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$projectGroupResult', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'costcenter': '$projectGroupResult.costcenter', 
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
                                        }
                                    ], 
                                    'as': 'circle'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$circle', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName'
                                }
                            }
                        ], 
                        'as': 'projectResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$projectResult', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'projectIdName': '$projectResult.projectId', 
                        'name': '$ExpenseNo', 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'expenseDate': {
                            '$cond': {
                                'if': {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$expenseDate', ''
                                            ]
                                        }, {
                                            '$regexMatch': {
                                                'input': '$expenseDate', 
                                                'regex': '^\\d{2}:\\d{2}:\\d{4}$'
                                            }
                                        }, {
                                            '$regexMatch': {
                                                'input': '$expenseDate', 
                                                'regex': '^\\d{4}:\\d{2}:\\d{2}$'
                                            }
                                        }, {
                                            '$regexMatch': {
                                                'input': '$expenseDate', 
                                                'regex': '^\\d{1}:\\d{2}:\\d{4}$'
                                            }
                                        }
                                    ]
                                }, 
                                'then': None, 
                                'else': {
                                    '$toDate': '$expenseDate'
                                }
                            }
                        }
                    }
                }
            ], 
            'as': 'ExpenseDetails'
        }
            
    }, {
        '$lookup': {
            'from': 'Advance', 
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
                    '$lookup': {
                        'from': 'project', 
                        'localField': 'projectId', 
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
                                                        '$project': {
                                                            '_id': 0, 
                                                            'costCenter': 1
                                                        }
                                                    }
                                                ], 
                                                'as': 'costcenterresult'
                                            }
                                        }, {
                                            '$addFields': {
                                                'costcenter': '$costcenterresult.costCenter'
                                            }
                                        }, {
                                            '$unwind': {
                                                'path': '$costcenter', 
                                                'preserveNullAndEmptyArrays': True
                                            }
                                        }
                                    ], 
                                    'as': 'projectGroupResult'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$projectGroupResult', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'costcenter': '$projectGroupResult.costcenter', 
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
                                        }
                                    ], 
                                    'as': 'circle'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$circle', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName'
                                }
                            }
                        ], 
                        'as': 'projectResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$projectResult', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'projectIdName': '$projectResult.projectId', 
                        'name': '$AdvanceNo', 
                        'uniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }
            ], 
            'as': 'AdvanceResults'
        }
    }, {
        '$addFields': {
            'newData': {
                '$concatArrays': [
                    '$AdvanceResults', '$ExpenseDetails'
                ]
            }
        }
    }, {
        '$unwind': {
            'path': '$newData', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'AddedAt': {
                '$toDate': '$newData.addedAt'
            }, 
            'uniqueId': '$newData.uniqueId', 
            'expenseMonth': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': '$newData.expenseDate'
                }
            }, 
            'submissionDate': {
                '$dateToString': {
                    'format': '%d-%m-%Y', 
                    'date': '$newData.expenseDate', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'actionAt': {
                '$toDate': '$newData.actionAt'
            }
        }
    }, {
        '$project': {
            'AddedAt': {
                '$dateToString': {
                    'format': '%d-%m-%Y', 
                    'date': '$AddedAt', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'addedMonth': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': '$AddedAt'
                }
            }, 
            'actionAt': {
                '$dateToString': {
                    'format': '%d-%m-%Y', 
                    'date': '$actionAt'
                }
            }, 
            'expenseMonth': '$expenseMonth', 
            'submissionDate': '$submissionDate', 
            'name': '$newData.name', 
            'costcenter': '$newData.projectResult.costcenter', 
            'remark': '$newData.remark', 
            'ApprovedAmount': '$newData.ApprovedAmount', 
            'customStatus': '$newData.customStatus', 
            'uniqueId': 1, 
            'customisedStatus': '$newData.customisedStatus', 
            'type': '$newData.type'
        }
    }, {
        '$addFields': {
            'submissionDate': {
                '$cond': {
                    'if': {
                        '$eq': [
                            '$type', 'Advance'
                        ]
                    }, 
                    'then': '$AddedAt', 
                    'else': '$submissionDate'
                }
            }
        }
    }, {
        '$group': {
            '_id': '$name', 
            'totalApprovedAmount': {
                '$sum': '$ApprovedAmount'
            }, 
            'totalApprovedAmountRow': {
                '$sum': {
                    '$cond': [
                        {
                            '$and': [
                                {
                                    '$eq': [
                                        '$customisedStatus', 6
                                    ]
                                }
                            ]
                        }, '$ApprovedAmount', 0
                    ]
                }
            }, 
            'docs': {
                '$push': '$$ROOT'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }, {
        '$unwind': {
            'path': '$docs'
        }
    }, {
        '$addFields': {
            'docs.totalApprovedAmount': '$totalApprovedAmount', 
            'docs.totalApprovedAmountRow': '$totalApprovedAmountRow'
        }
    }, {
        '$group': {
            '_id': '$_id', 
            'docs': {
                '$first': '$docs'
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$docs'
        }
    }, {
        '$match': {
            'name': {
                '$nin': [
                    '', True
                ]
            }
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            }, 
            # 'last6Digits': {
            #     '$toInt': {
            #         '$substrBytes': [
            #             '$name', {
            #                 '$subtract': [
            #                     {
            #                         '$strLenBytes': '$name'
            #                     }, 6
            #                 ]
            #             }, 6
            #         ]
            #     }
            # }
            'last6Digits': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$eq': [
                                    {
                                        '$ifNull': [
                                            '$name', ''
                                        ]
                                    }, ''
                                ]
                            }, {
                                '$lt': [
                                    {
                                        '$strLenBytes': '$name'
                                    }, 6
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': {
                        '$toInt': {
                            '$substrBytes': [
                                '$name', {
                                    '$subtract': [
                                        {
                                            '$strLenBytes': '$name'
                                        }, 6
                                    ]
                                }, 6
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$sort': {
            'last6Digits': -1
        }
    }, {
        '$lookup': {
            'from': 'CurrentBalance', 
            'localField': '_id', 
            'foreignField': 'employee', 
            'as': 'Openingbalance'
        }
    }, {
        '$unwind': {
            'path': '$Openingbalance', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'Openingbalance': {
                '$ifNull': [
                    '$Openingbalance.Amount', 0
                ]
            }
        }
    }, {
        '$group': {
            '_id': True, 
            'data': {
                '$addToSet': '$$ROOT'
            }, 
            'ExpenseAmountTotal': {
                '$sum': {
                    '$cond': [
                        {
                            '$and': [
                                {
                                    '$ne': [
                                        '$type', 'Advance'
                                    ]
                                }, {
                                    '$eq': [
                                        '$customisedStatus', 6
                                    ]
                                }
                            ]
                        }, '$totalApprovedAmount', 0
                    ]
                }
            }, 
            'AdvanceAmountTotal': {
                '$sum': {
                    '$cond': [
                        {
                            '$and': [
                                {
                                    '$eq': [
                                        '$type', 'Advance'
                                    ]
                                }, {
                                    '$eq': [
                                        '$customisedStatus', 6
                                    ]
                                }
                            ]
                        }, '$totalApprovedAmount', 0
                    ]
                }
            }, 
            'Openingbalance': {
                '$first': '$Openingbalance'
            }
        }
    }, {
        '$addFields': {
            'data': {
                '$sortArray': {
                    'input': '$data', 
                    'sortBy': {
                        'last6Digits': -1
                    }
                }
            }, 
            'AdvanceAmountTotal1': {
                '$cond': {
                    'if': {
                        '$gt': [
                            '$Openingbalance', 0
                        ]
                    }, 
                    'then': {
                        '$add': [
                            '$AdvanceAmountTotal', '$Openingbalance'
                        ]
                    }, 
                    'else': '$AdvanceAmountTotal'
                }
            }, 
            'ExpenseAmountTotal1': {
                '$cond': {
                    'if': {
                        '$lt': [
                            '$Openingbalance', 0
                        ]
                    }, 
                    'then': {
                        '$subtract': [
                            '$ExpenseAmountTotal', '$Openingbalance'
                        ]
                    }, 
                    'else': '$ExpenseAmountTotal'
                }
            }
        }
    }, {
        '$addFields': {
            'finalAmount': {
                '$subtract': [
                    '$AdvanceAmountTotal1', '$ExpenseAmountTotal1'
                ]
            }
        }
    }, {
        '$project': {
            'AdvanceAmountTotal1': 0, 
            'ExpenseAmountTotal1': 0
        }
    }
]
            # print('ghjkkjhghjkl',arr)
            arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
            Response = cmo.finding_aggregate("userRegister", arr)
            return respond(Response)
    
    
####to get claimtype and categories in expense and advance
@mobile_blueprint.route("/mobile/claimTypeRole", methods=["GET", "POST"])
@token_required
def claimTypeRole(current_user):
    claimTypeId = request.args.get("categories")
    claimtypeDa = request.args.get("claimtypeDa")
    if request.method == "GET":
        if claimTypeId != None and claimTypeId != 'undefined':
            arr = [
                {
                    "$match": {
                        "claimTypeId": ObjectId(claimTypeId),
                        "designation": current_user["designation"],
                        "value": {"$ne": False},
                    }
                },
                {
                    "$lookup": {
                        "from": "claimType",
                        "localField": "claimTypeId",
                        "foreignField": "_id",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "result",
                    }
                },
                {"$match": {"$expr": {"$gt": [{"$size": "$result"}, 0]}}},
                {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
                {
                    "$addFields": {
                        "categories": "$result.categories",
                        "DesignationClaimid": {"$toString": "$DesignationClaimid"},
                        "claimTypeId": {"$toString": "$claimTypeId"},
                        "mergerDesignationClaimid": {"$toString": "$_id"},
                    }
                },
                {"$project": {"result": 0, "_id": 0}},
            ]
            Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
            return respond(Response)
        elif claimtypeDa != None and claimtypeDa != 'undefined':
            arr = [
    {
        '$match': {
            'deleteStatus': {
                '$ne': 1
            }, 
            'designation': current_user["designation"], 
            'value': {
                '$ne': False
            }
        }
    }, {
        '$lookup': {
            'from': 'claimType', 
            'localField': 'claimTypeId', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'categoriesType': 'Daily Allowance'
                    }
                }
            ], 
            'as': 'claimResult'
        }
    }, {
        '$unwind': {
            'path': '$claimResult', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$addFields': {
            'DesignationClaimid': {
                '$toString': '$DesignationClaimid'
            }, 
            'claimTypeId': {
                '$toString': '$claimTypeId'
            }, 
            'mergerDesignationClaimId': {
                '$toString': '$_id'
            }
        }
    }, {
        '$group': {
            '_id': '$claimTypeId', 
            'doc': {
                '$first': '$$ROOT'
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$doc'
        }
    }, {
        '$project': {
            '_id': 0, 
            'claimResult': 0
        }
    }
]
            
            Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
            return respond(Response)
        else:
            arr = [
    {
        '$match': {
            'deleteStatus': {
                '$ne': 1
            }, 
            'designation': current_user["designation"], 
            'value': {
                '$ne': False
            }
        }
    }, {
        '$lookup': {
            'from': 'claimType', 
            'localField': 'claimTypeId', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'categoriesType': 'Expense'
                    }
                }
            ], 
            'as': 'claimTypeResult'
        }
    }, {
        '$unwind': {
            'path': '$claimTypeResult', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$addFields': {
            'DesignationClaimid': {
                '$toString': '$DesignationClaimid'
            }, 
            'claimTypeId': {
                '$toString': '$claimTypeId'
            }, 
            'mergerDesignationClaimId': {
                '$toString': '$_id'
            }, 
            'categories': '$claimTypeResult.categories'
        }
    }, {
        '$group': {
            '_id': '$claimTypeId', 
            'doc': {
                '$first': '$$ROOT'
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$doc'
        }
    }, {
        '$project': {
            '_id': 0, 
            'claimTypeResult': 0
        }
    }
]
            
            Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
            # print("ResponseResponseResponsepp", Response)
            return respond(Response)    
    
    
    
####to get project for expenses module
@mobile_blueprint.route("/mobile/projectDetails", methods=["GET"])
@token_required
def expensesProjectDetails(current_user):
    if request.method == "GET":
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "empId": current_user["userUniqueId"],
                }
            },
            {"$unwind": {"path": "$projectIds", "preserveNullAndEmptyArrays": True}},
            {
                "$lookup": {
                    "from": "project",
                    "localField": "projectIds",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1},'status': 'Active'}},
                        {
                            "$project": {
                                "_id": 0,
                                "projectUniqueId": {"$toString": "$_id"},
                                "projectId": 1,
                            }
                        },
                    ],
                    "as": "project",
                }
            },
            {"$unwind": {"path": "$project", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "_id": 0,
                    "projectUniqueId": "$project.projectUniqueId",
                    "projectId": "$project.projectId",
                }
            },
        ]
        # print("dfghjklurrtyuio", arr)
        Response = cmo.finding_aggregate("projectAllocation", arr)
        return respond(Response)
    else:
        return jsonify({"msg": "No Data Found"})
    
####to get Sites 
@mobile_blueprint.route("/mobile/projectSite", methods=["GET"])
@mobile_blueprint.route("/mobile/projectSite/<projectUniqueId>", methods=["GET"])
@token_required
def expensesSiteDetails(current_user, projectUniqueId=None):
    projectUniqueId = request.args.get("projectId")
    if projectUniqueId != None or projectUniqueId != "undefined":
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "assignerId": {"$in": [ObjectId(current_user["userUniqueId"])]},
                    "projectuniqueId": projectUniqueId,
                }
            },
            {
                "$lookup": {
                    "from": "SiteEngineer",
                    "localField": "siteId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$addFields": {
                                "Site Id": {
                                    "$concat": ["$Site Id", "(", "$systemId", ")"]
                                }
                            }
                        },
                        {"$project": {"_id": 0, "Site Id": 1}},
                    ],
                    "as": "siteResult",
                }
            },
            {"$match": {"siteResult": {"$exists": True, "$gt": {"$size": 0}}}},
            {"$unwind": {"path": "$siteResult", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "Site Id": "$siteResult.Site Id",
                    "siteUniqueId": {"$toString": "$siteId"},
                    "_id": 0,
                }
            },
            {"$group": {"_id": "$Site Id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
        ]

        Response = cmo.finding_aggregate("milestone", arr)
        return respond(Response)
    else:
        return jsonify({"msg": "Please Provide a valid Project Id"})
    
#toget Task according to site
@mobile_blueprint.route("/mobile/projectSiteTask", methods=["GET"])
@mobile_blueprint.route("/mobile/projectSiteTask/<siteUniqueId>", methods=["GET"])
@token_required
def expensesprojectSiteTask(current_user, siteUniqueId=None):
    # print("current_user", current_user)
    siteUniqueId = request.args.get("siteId")
    if siteUniqueId != None or siteUniqueId != "undefined":
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "siteId": ObjectId(siteUniqueId),
                    "assignerId": {"$in": [ObjectId(current_user["userUniqueId"])]},
                    "CC_Completion Date": {"$gte": str(three_days_ago)},
                }
            },
            {"$project": {"taskUniqueId": {"$toString": "$_id"}, "Name": 1, "_id": 0}},
        ]
        # print("ddfdf", arr)
        Response = cmo.finding_aggregate("milestone", arr)
        return respond(Response)
    else:
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "assignerId": {"$in": [ObjectId(current_user["userUniqueId"])]},
                    "CC_Completion Date": {"$gte": str(three_days_ago)},
                }
            },
            {"$project": {"taskUniqueId": {"$toString": "$_id"}, "Name": 1, "_id": 0}},
        ]
        Response = cmo.finding_aggregate("milestone", arr)
        return respond(Response)


###Add,edit delete fill Expense
@mobile_blueprint.route("/mobile/fillExpense", methods=["GET", "POST","DELETE"])
@mobile_blueprint.route("/mobile/fillExpense/<id>", methods=["GET", "POST","DELETE"])
@token_required
def expensesFillExpense(current_user, id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    if request.method == "GET":
        arr = []
        if ExpenseNo != None and ExpenseNo != "undefined":
            arr = [
                {
                    "$match": {
                        "addedFor": ObjectId(current_user["userUniqueId"]),
                        "deleteStatus": {"$ne": 1},
                        "ExpenseNo": ExpenseNo,
                    }
                }
            ]
        else:
            arr = [
                {
                    "$match": {
                        "addedFor": ObjectId(current_user["userUniqueId"]),
                        "deleteStatus": {"$ne": 1},
                    }
                }
            ]

        arr = arr + [
            {
                "$lookup": {
                    "from": "milestone",
                    "localField": "taskName",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": 0, "Name": 1}},
                    ],
                    "as": "milestoneresult",
                }
            },
            {
                "$lookup": {
                    "from": "userRegister",
                    "localField": "addedFor",
                    "foreignField": "_id",
                    "as": "userInfo",
                }
            },
            {
                "$lookup": {
                    "from": "SiteEngineer",
                    "localField": "Site Id",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": 0, "Site Id": 1}},
                    ],
                    "as": "siteResults",
                }
            },
            {
                "$lookup": {
                    "from": "project",
                    "localField": "projectId",
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
                                            "from": "costCenter",
                                            "localField": "costCenterId",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {
                                                    "$match": {
                                                        "deleteStatus": {"$ne": 1}
                                                    }
                                                },
                                                {
                                                    "$project": {
                                                        "_id": 0,
                                                        "costCenter": 1,
                                                    }
                                                },
                                            ],
                                            "as": "costcenterresult",
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "costcenter": "$costcenterresult.costCenter"
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$costcenter",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                ],
                                "as": "projectGroupResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$projectGroupResult",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$addFields": {
                                "costcenter": "$projectGroupResult.costcenter"
                            }
                        },
                    ],
                    "as": "projectResult",
                }
            },
            {"$unwind": {"path": "$projectResult", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$siteResults", "preserveNullAndEmptyArrays": True}},
            {
                "$unwind": {
                    "path": "$milestoneresult",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$lookup": {
                    "from": "claimType",
                    "localField": "claimType",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "claimTyperesult",
                }
            },
            {
                "$unwind": {
                    "path": "$claimTyperesult",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$addFields": {
                    "projectIdName": "$projectResult.projectId",
                    "costcenter": "$projectResult.costcenter",
                    "Claim_Date": {"$toDate": "$addedAt"},
                    "empCode": "$userInfo.empCode",
                    "empName": "$userInfo.empName",
                    "mobile": "$userInfo.mobile",
                    "Site_Id": "$siteResults.Site Id",
                    "Task": "$milestoneresult.Name",
                    "claimType": "$claimTyperesult.claimType",
                    "projectId": {"$toString": "$projectId"},
                    # "expenseDate": {"$toDate": "$expenseDate"},
                    'expenseDate': {
                                '$cond': {
                                    'if': {
                                        '$or': [
                                            {
                                                '$eq': [
                                                    '$expenseDate', ''
                                                ]
                                            }, {
                                                '$regexMatch': {
                                                    'input': '$expenseDate', 
                                                    'regex': re.compile(r"^\\d{2}:\d{2}:\d{4}$")
                                                }
                                            }, {
                                                '$regexMatch': {
                                                    'input': '$expenseDate', 
                                                    'regex': re.compile(r"^\\d{4}:\d{2}:\d{2}$")
                                                }
                                            }, {
                                                '$regexMatch': {
                                                    'input': '$expenseDate', 
                                                    'regex': re.compile(r"^\\d{1}:\d{2}:\d{4}$")
                                                }
                                            }
                                        ]
                                    }, 
                                    'then': None, 
                                    'else': {
                                        '$toDate': '$expenseDate'
                                    }
                                }
                                },
                    "claimTypeId": {"$toString": "$claimTyperesult._id"},
                }
            },
            {
                "$addFields": {
                    "SubmissionDate": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$Claim_Date","timezone":"Asia/Kolkata"}
                    },
                    "expenseDate": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$expenseDate","timezone":"Asia/Kolkata"}
                    },
                    "expenseuniqueId": {"$toString": "$_id"},
                    "addedFor": {"$toString": "$addedFor"},
                    "addedBy": {"$toString": "$addedBy"},
                }
            },
            {
                "$project": {
                    "Site Id": 0,
                    "taskName": 0,
                    "milestoneresult": 0,
                    "siteResults": 0,
                    "userInfo": 0,
                    "projectResult": 0,
                    "_id": 0,
                    "claimTyperesult": 0,
                    "ExpenseUniqueId": 0,
                    "actionBy": 0,
                    "L1Approver": 0,
                    "L2Approver": 0,
                    "L3Approver": 0,
                    "addedBy": 0,
                }
            },
            {
                "$group": {
                    "_id": "$ExpenseNo",
                    "data": {"$first": "$$ROOT"},
                    "Amount": {"$sum": "$Amount"},
                }
            },
            {"$addFields": {"data.Amount": "$Amount"}},
            {"$replaceRoot": {"newRoot": "$data"}},
            {"$sort": {"expenseuniqueId": 1}},
        ]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        Response = cmo.finding_aggregate("Expenses", arr)

        return respond(Response)
    if request.method == "POST":
        if id is None:
            ExpenseNo = request.form.get("expenseId")
            Siteid = request.form.get("Site Id")
            taskName = request.form.get("Name")
            claimType = request.form.get("claimType")
            categories = request.form.get("categories")
            Amount = request.form.get("Amount")
            Total_distance = request.form.get("totalKm")
            if Total_distance not in ["", "undefined", None, 0, "0"]:
                if int(Total_distance) < 0:
                    Total_distance = int(Total_distance) * -1
            imagetype = ["jpg", "jpeg", "pdf", "png"]
            attachment = request.files.get("attachment")
            billNumber = request.form.get("billNumber")
            projectId = request.form.get("projectId")
            startKm = request.form.get("startKm")
            endKm = request.form.get("endKm")
            checkInDate = request.form.get("checkInDate")
            checkOutDate = request.form.get("checkOutDate")
            totaldays = request.form.get("totaldays")
            expenseDate = request.form.get("ExpenseDate")
            startLocation = request.form.get("startLocation")
            endLocation = request.form.get("endLocation")
            additionalInfo = request.form.get("additionalInfo")
            
            if categories not in ['','undefined',None]:
                if Total_distance in ['', 'undefined', None]:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "Please Enter Valid Start and End KM",
                        }
                    )
                else:
                    try:
                        Total_distance = int(Total_distance)
                        if Total_distance < 1:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "Please Enter Valid Start and End KM",
                                }
                            )
                        
                    except ValueError:
                        return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "Please Enter Valid Start and End KM",
                                }
                            )
            try:
                Amount = int(Amount)
            except Exception as e:
                if categories in ["", "undefined", None]:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "Please Enter Amount or select Category and Total distance",
                        }
                    )
                Amount = 0
            if isinstance(expenseDate, str) and expenseDate.endswith("Z"):
                try:
                    date_obj = dtt.fromisoformat(expenseDate.replace("Z", "+00:00"))
                    kolkata_tz = pytz.timezone("Asia/Kolkata")
                    date_obj = date_obj.astimezone(kolkata_tz)
                    expenseDate = date_obj.isoformat()
                except ValueError as e:
                    pass
                    # print(f"Error converting date: {e}")
                    
            if ExpenseNo in ['',None,'undefined']:
                if expenseDate in ['',None,'undefined']:
                    return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": f"Please Fill  Expense Date",
                                    }
                                )
                    
            
            
            if ExpenseNo != None and ExpenseNo != "":
                arr = [
                    {"$match": {"ExpenseNo": ExpenseNo, "deleteStatus": {"$ne": 1}}},
                    {
                        "$addFields": {
                            "expenseDate2": {
                                "$cond": {
                                    "if": {
                                        "$or": [
                                            {"$eq": ["$expenseDate", None]},
                                            {"$eq": ["$expenseDate", ""]},
                                        ]
                                    },
                                    "then": None,
                                    "else": {"$toDate": "$expenseDate"},
                                }
                            }
                        }
                    },
                    {
                        "$project": {
                            "expenseDate2": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$expenseDate2",
                                    "timezone": "Asia/Kolkata",
                                }
                            },
                            "expenseDate": 1,
                            "_id": 0,
                        }
                    },
                ]
                Response = cmo.finding_aggregate("Expenses", arr)
                if len(Response["data"]):
                    if not is_valid_date(expenseDate):
                        if expenseDate != None and expenseDate != "":
                            if expenseDate != Response["data"][0]["expenseDate"]:
                                # print("Response['data'][0]", Response["data"][0])
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": f"Please Fill Same Expense Date under {ExpenseNo} and the date is {Response['data'][0]['expenseDate2']}",
                                    }
                                )
                    else:
                        expenseDate = Response["data"][0]["expenseDate"]

            siteandTaskmatchingData = {
                "Siteid": Siteid,
                "taskName": taskName,
                "attachment": attachment,
                "categories": categories,
                "Total_distance": Total_distance,
                "Amount": Amount,
                "startKm": startKm,
                "endKm": endKm,
                "ExpenseNo": ExpenseNo,
                "expenseDate": expenseDate,
                "startLocation": startLocation,
                "endLocation": endLocation,
                "totaldays": totaldays,
                "checkInDate": checkInDate,
                "checkOutDate": checkOutDate,
            }
            if (
                siteandTaskmatchingData["Amount"] is None
                and siteandTaskmatchingData["Total_distance"] is None
            ):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "Please Enter Amount or select Category and Total distance",
                    }
                )

            arr = [
                {
                    "$match": {
                        "claimTypeId": ObjectId(claimType),
                        "designation": current_user["designation"],
                    }
                },
                {
                    "$lookup": {
                        "from": "DesignationclaimType",
                        "localField": "DesignationClaimid",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {"$project": {"taskName": 1, "siteId": 1, "_id": 0}},
                        ],
                        "as": "DesignationclaimTypeResult",
                    }
                },
                {
                    "$unwind": {
                        "path": "$DesignationclaimTypeResult",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "siteId": "$DesignationclaimTypeResult.siteId",
                        "taskName": "$DesignationclaimTypeResult.taskName",
                        "claimTypeId": {"$toString": "$claimTypeId"},
                    }
                },
                {
                    "$project": {
                        "DesignationclaimTypeResult": 0,
                        "_id": 0,
                        "DesignationClaimid": 0,
                    }
                },
            ]
            dataTomatch = cmo.finding_aggregate("mergerDesignationClaim", arr)["data"]
            if len(dataTomatch):
                dataTomatch = dataTomatch[0]
                matchingclaimType = ["Project Advance", "Daily Allowance"]
                # if dataTomatch["name"] in matchingclaimType:
                #     return respond(
                #         {
                #             "status": 400,
                #             "icon": "error",
                #             "msg": f"You can not add this {dataTomatch['name']} claim Type",
                #         }
                #     )
            if len(dataTomatch) < 1:
                return respond(
                    {"status": 400, "icon": "error", "msg": "Claim Type Not Found"}
                )

            if dataTomatch.get("siteId") and dataTomatch.get("taskName"):
                if (
                    not siteandTaskmatchingData.get("Siteid")
                    or siteandTaskmatchingData["Siteid"] == "undefined"
                ):
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "You have to select the site",
                        }
                    )
                if (
                    not siteandTaskmatchingData.get("taskName")
                    or siteandTaskmatchingData["taskName"] == "undefined"
                ):
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "You have to select the task mandatory",
                        }
                    )

            if dataTomatch.get("taskName") and (not siteandTaskmatchingData.get("taskName") or siteandTaskmatchingData["taskName"] == "undefined"):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "You have to select the task mandatory",
                    }
                )

            if dataTomatch.get("siteId") and (
                not siteandTaskmatchingData.get("Siteid")
                or siteandTaskmatchingData["Siteid"] == "undefined"
            ):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "You have to select the site",
                    }
                )

            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(claimType)}},
                {"$addFields": {"claimTypeId": {"$toString": "$_id"}}},
                {"$project": {"_id": 0}},
            ]
            shortcodeData = cmo.finding_aggregate("claimType", arr)["data"]

            if len(shortcodeData) and shortcodeData[0].get("attachment"):
                if (
                    not siteandTaskmatchingData.get("attachment")
                    or siteandTaskmatchingData["attachment"] == "undefined"
                ):
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "You have to attach file Compulsory",
                        }
                    )
                else:
                    pathing = cform.singleFileSaver(attachment, "", imagetype)
                    if pathing["status"] == 422:
                        return respond(pathing)
                    attachment = pathing["msg"]
                    result = attachment[attachment.find("uploads") :]
                    attachment = result
                    # path = os.path.join(os.getcwd(), pathing["msg"])
                    # resized_file_path = process_file(path)
                    # if resized_file_path != None:
                    #     attachment = resized_file_path
                    #     result = attachment[attachment.find("uploads") :]
                    #     attachment = result
                    # else:
                    #     attachment = pathing["msg"]
                    #     result = attachment[attachment.find("uploads") :]
                    #     attachment = result
            elif attachment:
                pathing = cform.singleFileSaver(attachment, "", imagetype)
                if pathing["status"] == 422:
                    return respond(pathing)
                attachment = pathing["msg"]
                result = attachment[attachment.find("uploads") :]
                attachment = result

            if siteandTaskmatchingData.get("categories"):

                checkingCondition = ["", "undefined", None]
                if siteandTaskmatchingData["categories"] not in checkingCondition:
                    arru = [
                        {
                            "$match": {
                                "categories": siteandTaskmatchingData["categories"]
                            }
                        },
                        {"$project": {"unitRate": 1, "_id": 0}},
                    ]
                    datas = cmo.finding_aggregate("unitRate", arru)["data"]
                    # print('datasdatas',datas)
                    if not datas:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "This Category not matched for declaring unit rate",
                            }
                        )
                    else:
                        unitRate = datas[0]["unitRate"]
                        # print('Amouynnn',unitRate,Total_distance)
                        Amount = float(Total_distance) * float(unitRate)

            if dataTomatch.get("value") and Amount:
                if not isinstance(dataTomatch["value"], bool):
                    if Amount != None and Amount != "undefined":
                        if isinstance(int(dataTomatch["value"]), int):
                            if (
                                siteandTaskmatchingData["totaldays"] != None
                                and siteandTaskmatchingData["totaldays"] != "undefined"
                            ):
                                if isinstance(
                                    int(siteandTaskmatchingData["totaldays"]), int
                                ):
                                    hotelTotalValue = int(dataTomatch["value"]) * (
                                        int(siteandTaskmatchingData["totaldays"])
                                    )
                                    if int(Amount) > int(hotelTotalValue):
                                        return respond(
                                            {
                                                "status": 400,
                                                "icon": "error",
                                                "msg": f"You can fill amount only equal to or less than {hotelTotalValue} not for this {Amount}",
                                            }
                                        )
                            else:

                                checkingCondition = ["", "undefined", None, "NaN"]
                                if (
                                    Amount
                                    and dataTomatch["value"] not in checkingCondition
                                ):
                                    if int(Amount) > int(dataTomatch["value"]):
                                        return respond(
                                            {
                                                "status": 400,
                                                "icon": "error",
                                                "msg": f"You can fill amount only equal to or less than {dataTomatch['value']} not for this {Amount}",
                                            }
                                        )
                        else:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"Amount Not Found",
                                }
                            )
            if ExpenseNo and ExpenseNo != "undefined":
                if siteandTaskmatchingData["expenseDate"] in ["", None, "undefined"]:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "Expense Date not found",
                        }
                    )

                date_object = dtt.fromisoformat(siteandTaskmatchingData["expenseDate"])
                arryy = [
                    {
                        "$addFields": {
                            "expenseDate": {
                                "$cond": {
                                    "if": {
                                        "$or": [
                                            {"$eq": ["$expenseDate", None]},
                                            {"$eq": ["$expenseDate", ""]},
                                        ]
                                    },
                                    "then": None,
                                    "else": {"$toDate": "$expenseDate"},
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "expenseDate": {"$ne": date_object},
                            "ExpenseNo": ExpenseNo,
                        }
                    },
                    {"$project": {"_id": {"$toString": "$_id"}}},
                ]
                Responser = cmo.finding_aggregate("Expenses", arryy)["data"]
                # if len(Responser):
                #     return respond(
                #                     {
                #                         "status": 400,
                #                         "icon": "error",
                #                         "msg": "You should fill  expense  date in all Row",
                #                     }
                #                 )
                arr = [
                    {
                        "$match": {
                            "ExpenseNo": ExpenseNo,
                            "customisedStatus": {"$nin": [1, 3, 5]},
                        }
                    },
                    {"$project": {"_id": 0, "ExpenseNo": 1}},
                ]

                Response = cmo.finding_aggregate("Expenses", arr)["data"]
                if len(Response):
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": f"You can not add row in this {Response[0]['ExpenseNo']} No is already in Process",
                        }
                    )

            # ExpenseNo generation logic
            if not ExpenseNo or ExpenseNo == "undefined":
                if claimType:
                    
                    if len(shortcodeData) > 0:
                        
                        ExpenseNo = ExpenseNonewLogic()

                    else:
                        ExpenseNo = ExpenseNo + "000001"
                else:
                    return respond(
                        {"icon": "error", "status": 400, "msg": "Claim Type not Found!"}
                    )
            financialyear = None
            varible = None
            varibleInt = None
            ggg = ExpenseNo.split("/")
            financialyear = ggg[1]
            varible = ggg[-1]
            varibleInt = int(varible)
            if is_valid_mongodb_objectid(siteandTaskmatchingData.get("Siteid")):
                siteandTaskmatchingData["Siteid"] = ObjectId(
                    siteandTaskmatchingData["Siteid"]
                )
            if is_valid_mongodb_objectid(siteandTaskmatchingData.get("taskName")):
                siteandTaskmatchingData["taskName"] = ObjectId(
                    siteandTaskmatchingData["taskName"]
                )
            if is_valid_mongodb_objectid(projectId):
                projectId = ObjectId(projectId)

            
            datatoinsert = {
                "attachment": attachment,
                "ExpenseNo": ExpenseNo,
                "Site Id": siteandTaskmatchingData["Siteid"],
                "taskName": siteandTaskmatchingData["taskName"],
                "claimType": ObjectId(claimType),
                "categories": categories,
                "Total_distance": Total_distance,
                "Amount": int(Amount),
                "addedAt": get_current_date_timestamp(),
                "CreatedAt": int(unique_timestampexpense()),
                "addedFor": ObjectId(current_user["userUniqueId"]),
                "designation": current_user["designation"],
                "billNumber": billNumber,
                "projectId": projectId,
                "status": "Submitted",
                "customStatus": "Submitted",
                "customisedStatus": 1,
                "endKm": endKm,
                "startKm": startKm,
                "type": "Expense",
                "expenseDate": siteandTaskmatchingData["expenseDate"],
                "addedBy": ObjectId(current_user["userUniqueId"]),
                "startLocation": siteandTaskmatchingData["startLocation"],
                "endLocation": siteandTaskmatchingData["endLocation"],
                "checkInDate": siteandTaskmatchingData["checkInDate"],
                "checkOutDate": siteandTaskmatchingData["checkOutDate"],
                "totaldays": siteandTaskmatchingData["totaldays"],
                "actionAt": get_current_date_timestamp(),
                "additionalInfo": additionalInfo,
                "FinancialYear": financialyear,
                "expenseNumberInt": varibleInt,
                "expenseNumberStr": varible,
                'email':current_user["email"],
            }
            
            arr4=[
                {
                    '$match': {
                        'ExpenseNo': ExpenseNo, 
                        
                    }
                },
                            {
                    '$project': {
                        'addedFor': 1, 
                        '_id': 0
                    }
                }
            ]
            Response4=cmo.finding_aggregate_with_deleteStatus("Expenses",arr4)['data']
            if len(Response4):
                userId=Response4[0]['addedFor']
                print(userId,'userId')
                if userId!=datatoinsert['addedFor']:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": f"Please Try Again,System is in Another Process",
                        }
                    )
            cmo.insertion("TestingExpenses",{'userId':datatoinsert['addedFor'],'ExpenseNo':ExpenseNo,'email':current_user["email"]})

            Response = cmo.insertion("Expenses", datatoinsert)
            
            return respond(Response)
        if id != None and id != "undefined":
            arr = [
                {
                    "$match": {
                        "_id": ObjectId(id),
                        "customisedStatus": {"$nin": [1, 3, 5, 7]},
                    }
                },
                {"$project": {"_id": 0, "customisedStatus": 1}},
            ]
            Response = cmo.finding_aggregate("Expenses", arr)
            if len(Response["data"]):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"This can not be editable Because its Status is not submitted",
                    }
                )
            else:
                expenseDate = request.form.get("ExpenseDate")
                claimType = request.form.get("claimType")
                categories = request.form.get("categories")
                Amount = request.form.get("Amount")
                startKm = request.form.get("startKm")
                endKm = request.form.get("endKm")
                Total_distance = request.form.get("totalKm")
                if Total_distance not in ["", "undefined", None, 0, "0"]:
                    if int(Total_distance) < 0:
                        Total_distance = int(Total_distance) * -1
                billNumber = request.form.get("billNumber")
                attachment = request.files.get("attachment")
                startLocation = request.form.get("startLocation")
                endLocation = request.form.get("endLocation")
                checkOutDate = request.form.get("checkOutDate")
                checkInDate = request.form.get("checkInDate")
                totaldays = request.form.get("totaldays")
                additionalInfo = request.form.get("additionalInfo")
                
                
                
                if categories not in ['','undefined',None]:
                    if Total_distance in ['', 'undefined', None]:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "Please Enter Valid Start and End KM",
                            }
                        )
                    else:
                        try:
                            Total_distance = int(Total_distance)
                            if Total_distance < 1:
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": "Please Enter Valid Start and End KM",
                                    }
                                )
                            
                        except ValueError:
                            return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": "Please Enter Valid Start and End KM",
                                    }
                                )
                
                
                datatocheck = {
                    "expenseDate": expenseDate,
                    "claimType": claimType,
                    "categories": categories,
                    "Amount": Amount,
                    "startKm": startKm,
                    "endKm": endKm,
                    "billNumber": billNumber,
                    "attachment": attachment,
                    "Total_distance": Total_distance,
                    "startLocation": startLocation,
                    "endLocation": endLocation,
                    "checkInDate": checkInDate,
                    "checkOutDate": checkOutDate,
                    "totaldays": totaldays,
                }
                # print("datatattatocheck", datatocheck, claimType)
                ##checking conditions
                if is_valid_mongodb_objectid(datatocheck["claimType"]):
                    claimType = ObjectId(claimType)
                    arr = [
                        {
                            "$match": {
                                "claimTypeId": claimType,
                                "designation": current_user["designation"],
                            }
                        },
                        {
                            "$lookup": {
                                "from": "DesignationclaimType",
                                "localField": "DesignationClaimid",
                                "foreignField": "_id",
                                "pipeline": [
                                    {"$match": {"deleteStatus": {"$ne": 1}}},
                                    {
                                        "$project": {
                                            "taskName": 1,
                                            "siteId": 1,
                                            "_id": 0,
                                        }
                                    },
                                ],
                                "as": "DesignationclaimTypeResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$DesignationclaimTypeResult",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$addFields": {
                                "siteId": "$DesignationclaimTypeResult.siteId",
                                "taskName": "$DesignationclaimTypeResult.taskName",
                                "claimTypeId": {"$toString": "$claimTypeId"},
                            }
                        },
                        {
                            "$project": {
                                "DesignationclaimTypeResult": 0,
                                "_id": 0,
                                "DesignationClaimid": 0,
                            }
                        },
                    ]
                    dataTomatch = cmo.finding_aggregate("mergerDesignationClaim", arr)[
                        "data"
                    ]

                    if len(dataTomatch):
                        dataTomatch = dataTomatch[0]
                    if len(dataTomatch) < 1:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "Claim Type Not Found",
                            }
                        )

                    if dataTomatch.get("value") and Amount:
                        if not isinstance(dataTomatch["value"], bool):
                            if Amount != None and Amount != "undefined":
                                if isinstance(int(dataTomatch["value"]), int):
                                    if (
                                        datatocheck["totaldays"] != None
                                        and datatocheck["totaldays"] != "undefined"
                                    ):
                                        if isinstance(
                                            int(datatocheck["totaldays"]), int
                                        ):
                                            hotelTotalValue = int(
                                                dataTomatch["value"]
                                            ) * (int(datatocheck["totaldays"]))
                                            if int(Amount) > int(hotelTotalValue):
                                                return respond(
                                                    {
                                                        "status": 400,
                                                        "icon": "error",
                                                        "msg": f"You can fill amount only equal to or less than {hotelTotalValue} not for this {Amount}",
                                                    }
                                                )
                                    else:
                                        if int(Amount) > int(dataTomatch["value"]):
                                            return respond(
                                                {
                                                    "status": 400,
                                                    "icon": "error",
                                                    "msg": f"You can fill amount only equal to or less than {dataTomatch['value']} not for this {Amount}",
                                                }
                                            )
                                else:
                                    return respond(
                                        {
                                            "status": 400,
                                            "icon": "error",
                                            "msg": f"Amount Not Found",
                                        }
                                    )

                    if (datatocheck["categories"] != None and datatocheck["categories"] != ""):
                        arru = [
                            {"$match": {"categories": datatocheck["categories"]}},
                            {"$project": {"unitRate": 1, "_id": 0}},
                        ]
                        datas = cmo.finding_aggregate("unitRate", arru)["data"]
                        if not datas:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "This Category not matched for declaring unit rate",
                                }
                            )
                        else:
                            unitRate = datas[0]["unitRate"]
                            # print(
                            #     "hjlkjhghjkl", datatocheck["Total_distance"], unitRate
                            # )
                            Amount = float(datatocheck["Total_distance"]) * float(
                                unitRate
                            )
                    if datatocheck["attachment"] != None:
                        imagetype = ["jpg", "jpeg", "png", "pdf"]
                        pathing = cform.singleFileSaver(
                            datatocheck["attachment"], "", imagetype
                        )
                        if pathing["status"] == 422:
                            return respond(pathing)
                        attachment = pathing["msg"]
                        result = attachment[attachment.find("uploads") :]
                        attachment = result
                    datatoupdate = {
                        "attachment": attachment,
                        "claimType": claimType,
                        "categories": categories,
                        "Total_distance": Total_distance,
                        "Amount": int(Amount),
                        "updatedAt": get_current_date_timestamp(),
                        "billNumber": billNumber,
                        "status": "Submitted",
                        "customStatus": "Submitted",
                        "customisedStatus": 1,
                        "endKm": endKm,
                        "startKm": startKm,
                        "type": "Expense",
                        "expenseDate": datatocheck["expenseDate"],
                        "startLocation": datatocheck["startLocation"],
                        "endLocation": datatocheck["endLocation"],
                        "additionalInfo": additionalInfo,
                    }
                    datatoupdte2 = {
                        "status": "Submitted",
                        "customStatus": "Submitted",
                        "customisedStatus": 1,
                        "type": "Expense",
                        "actionAt": get_current_date_timestamp(),
                    }

                    cmo.updating(
                        "Approval",
                        {"ExpenseUniqueId": ObjectId(id)},
                        datatoupdte2,
                        False,
                    )
                    Response = cmo.updating("Expenses", {"_id": ObjectId(id)}, datatoupdate, False)
                    ######bulk status updating condition
                    artt = [
                        {"$match": {"_id": ObjectId(id)}},
                        {"$project": {"ExpenseNo": 1, "_id": 0}},
                    ]
                    dataff = cmo.finding_aggregate("Expenses", artt)["data"]
                    if len(dataff):
                        ExpenseNo = dataff[0]["ExpenseNo"]
                        artty = [
                            {
                                "$match": {
                                    "ExpenseNo": ExpenseNo,
                                }
                            },
                            {"$project": {"_id": {"$toString": "$_id"}}},
                        ]
                        # print("arttyarttyjjj", artty)
                        datafg = cmo.finding_aggregate("Expenses", artty)["data"]
                        # print("arttyarttyhhhh", datafg)
                        for i in datafg:
                            cmo.updating(
                                "Approval",
                                {"ExpenseUniqueId": ObjectId(i["_id"])},
                                {
                                    "status": "Submitted",
                                    "customStatus": "Submitted",
                                    "customisedStatus": 1,
                                    "type": "Expense",
                                    "actionAt": get_current_date_timestamp(),
                                },
                                False,
                            )
                            cmo.updating(
                                "Expenses",
                                {"_id": ObjectId(i["_id"])},
                                {
                                    "status": "Submitted",
                                    "customStatus": "Submitted",
                                    "customisedStatus": 1,
                                    "type": "Expense",
                                    "actionAt": get_current_date_timestamp(),
                                },
                                False,
                            )

                        # print(Response, "ResponseResponse")
                    return respond(Response)

                else:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "This Claim Type not found",
                        }
                    )
    
    if request.method == "DELETE":
        if id != None and id != "undefined":
            arr = [
                {
                    "$match": {
                        "_id": ObjectId(id),
                        "customisedStatus": {"$nin": [1, 3, 5, 7]},
                    }
                },
                {"$project": {"_id": 0, "customisedStatus": 1}},
            ]
            Response = cmo.finding_aggregate("Expenses", arr)
            if len(Response["data"]):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"This can not be deleted Because its Status is not submitted",
                    }
                )
            else:
                Response = cmo.deleting("Expenses", id)
                cmo.real_deleting("Approval", {"ExpenseUniqueId": ObjectId(id)})
                return respond(Response)
        else:
            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": f"Id not found",
                }
            )



#####Add,edit delete fill Advance
@mobile_blueprint.route("/mobile/fillAdvance", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@mobile_blueprint.route("/mobile/fillAdvance/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def expensesAdvance(current_user, id=None):
    # print("fghjkkhgfghjk", current_user)
    AdvanceNo = request.args.get("AdvanceNo")
    if request.method == "GET":
        arrr = []
        if AdvanceNo != None and AdvanceNo != "undefined":
            arrr = [
                {
                    "$match": {
                        "addedFor": ObjectId(current_user["userUniqueId"]),
                        "deleteStatus": {"$ne": 1},
                        "AdvanceNo": AdvanceNo,
                    }
                }
            ]
        else:
            arrr = [
                {
                    "$match": {
                        "addedFor": ObjectId(current_user["userUniqueId"]),
                        "deleteStatus": {"$ne": 1},
                    }
                }
            ]

        arrr = arrr + [
            {
                "$lookup": {
                    "from": "project",
                    "localField": "projectId",
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
                                            "from": "costCenter",
                                            "localField": "costCenterId",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {
                                                    "$match": {
                                                        "deleteStatus": {"$ne": 1}
                                                    }
                                                },
                                                {
                                                    "$project": {
                                                        "_id": 0,
                                                        "costCenter": 1,
                                                    }
                                                },
                                            ],
                                            "as": "costcenterresult",
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "costcenter": "$costcenterresult.costCenter"
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$costcenter",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                ],
                                "as": "projectGroupResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$projectGroupResult",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$addFields": {
                                "costcenter": "$projectGroupResult.costcenter"
                            }
                        },
                    ],
                    "as": "projectResult",
                }
            },
            {"$unwind": {"path": "$projectResult", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "CreatedAt": {"$toDate": "$CreatedAt"},
                    "projectId": {"$toString": "$projectId"},
                }
            },
            {
                "$lookup": {
                    "from": "claimType",
                    "localField": "advanceTypeId",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "claimTypeAdvance",
                }
            },
            {
                "$unwind": {
                    "path": "$claimTypeAdvance",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$addFields": {
                    "projectIdName": "$projectResult.projectId",
                    "costcenter": "$projectResult.costcenter",
                    "uniqueId": {"$toString": "$_id"},
                    "CreatedAt": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$CreatedAt",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "addedFor": {"$toString": "$addedFor"},
                    "advanceTypeId": {"$toString": "$advanceTypeId"},
                    "claimTypeId": {"$toString": "$claimTypeId"},
                    "advanceType": "$claimTypeAdvance.claimType",
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "designation": 0,
                    "projectResult": 0,
                    "addedAt": 0,
                    "AdvanceUniqueId": 0,
                    "actionBy": 0,
                    "L1Approver": 0,
                    "L2Approver": 0,
                    "L3Approver": 0,
                    "claimTypeAdvance": 0,
                }
            },
        ]
        arrr = arrr + apireq.commonarra + apireq.args_pagination(request.args)
        Response = cmo.finding_aggregate("Advance", arrr)
        return respond(Response)
    
    if request.method == "POST":
        if id == None:
            projectId = request.form.get("projectId")
            advanceTypeId = request.form.get("name")
            amount = request.form.get("Amount")
            additionalInfo = request.form.get("additionalInfo")
            advanceType = "Project Advance"
            remark = request.form.get("remark")
            claimTypeId = request.form.get("claimTypeId")
            # print("advanceTypeIdadvanceTypeId", advanceTypeId)
            chekingarray = ["", "undefined", None]
            if amount not in chekingarray:
                amount = int(amount)
                if amount < 0:
                    return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"Amount Should be Positive",
                    }
                )
            if is_valid_mongodb_objectid(advanceTypeId):
                arr = [
                    {
                        "$match": {
                            "designation": current_user["designation"],
                            "claimTypeId": ObjectId(advanceTypeId),
                        }
                    },
                    {"$project": {"_id": 0, "value": 1}},
                ]
                Responsess = cmo.finding_aggregate("mergerDesignationClaim", arr)["data"]
                # print("ResponsessResponsessResponsess", Responsess)
                if len(Responsess):
                    if int(amount) > int(Responsess[0]["value"]):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"Advance Limit for this Advance type is {Responsess[0]['value']}",
                            }
                        )

            else:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"Advance Type Not Found value",
                    }
                )

            arrppp = [
                {"$match": {"_id": ObjectId(current_user["userUniqueId"])}}, {
                    '$lookup': {
                        'from': 'Expenses', 
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
                                '$addFields': {
                                    'name': '$ExpenseNo', 
                                    'uniqueId': {
                                        '$toString': '$_id'
                                    }
                                }
                            }
                        ], 
                        'as': 'ExpenseDetails'
                    }
                }, {
                    '$lookup': {
                        'from': 'Advance', 
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
                                '$addFields': {
                                    'name': '$AdvanceNo', 
                                    'uniqueId': {
                                        '$toString': '$_id'
                                    }
                                }
                            }
                        ], 
                        'as': 'AdvanceResults'
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
                        'as': 'OpeningBalance'
                    }
                }, {
                    '$unwind': {
                        'path': '$OpeningBalance', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'newData': {
                            '$concatArrays': [
                                '$AdvanceResults', '$ExpenseDetails'
                            ]
                        }, 
                        'OpeningBalance': '$OpeningBalance.Amount'
                    }
                }, {
                    '$unwind': {
                        'path': '$newData', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$project': {
                        'name': '$newData.name', 
                        'ApprovedAmount': '$newData.ApprovedAmount', 
                        'customStatus': '$newData.customStatus', 
                        'uniqueId': 1, 
                        'customisedStatus': '$newData.customisedStatus', 
                        'type': '$newData.type', 
                        'Amount': '$newData.Amount', 
                        'OpeningBalance': 1
                    }
                }, {
                    '$group': {
                        '_id': '$name', 
                        'totalApprovedAmountRow': {
                            '$sum': {
                                '$cond': [
                                    {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$customisedStatus', 6
                                                ]
                                            }
                                        ]
                                    }, '$ApprovedAmount', 0
                                ]
                            }
                        }, 
                        'docs': {
                            '$push': '$$ROOT'
                        }
                    }
                }, {
                    '$sort': {
                        '_id': 1
                    }
                }, {
                    '$unwind': {
                        'path': '$docs'
                    }
                }, {
                    '$addFields': {
                        'docs.totalApprovedAmountRow': '$totalApprovedAmountRow'
                    }
                }, {
                    '$group': {
                        '_id': '$_id', 
                        'docs': {
                            '$first': '$docs'
                        }
                    }
                }, {
                    '$replaceRoot': {
                        'newRoot': '$docs'
                    }
                }, {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$group': {
                        '_id': None, 
                        'ExpenseAmountTotalApproved': {
                            '$sum': {
                                '$cond': [
                                    {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$type', 'Expense'
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$customisedStatus', 6
                                                ]
                                            }
                                        ]
                                    }, '$totalApprovedAmountRow', 0
                                ]
                            }
                        }, 
                        'AdvanceAmountTotalApproved': {
                            '$sum': {
                                '$cond': [
                                    {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$type', 'Advance'
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$customisedStatus', 6
                                                ]
                                            }
                                        ]
                                    }, '$ApprovedAmount', 0
                                ]
                            }
                        }, 
                        'AdvanceAmountTotal': {
                            '$sum': {
                                '$cond': [
                                    {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$type', 'Advance'
                                                ]
                                            }, {
                                                '$in': [
                                                    '$customisedStatus', [
                                                        1, 2, 4
                                                    ]
                                                ]
                                            }
                                        ]
                                    }, '$Amount', 0
                                ]
                            }
                        }, 
                        'data': {
                            '$addToSet': '$$ROOT'
                        }
                    }
                }, {
                    '$addFields': {
                        'OpeningBalance': {
                            '$arrayElemAt': [
                                '$data.OpeningBalance', 0
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'ExpenseAmountTotalApproved': {
                            '$cond': {
                                'if': {
                                    '$lt': [
                                        '$OpeningBalance', 0
                                    ]
                                }, 
                                'then': {
                                    '$subtract': [
                                        '$ExpenseAmountTotalApproved', '$OpeningBalance'
                                    ]
                                }, 
                                'else': '$ExpenseAmountTotalApproved'
                            }
                        }, 
                        'AdvanceAmountTotalApproved': {
                            '$cond': {
                                'if': {
                                    '$gt': [
                                        '$OpeningBalance', 0
                                    ]
                                }, 
                                'then': {
                                    '$add': [
                                        '$AdvanceAmountTotalApproved', '$OpeningBalance'
                                    ]
                                }, 
                                'else': '$AdvanceAmountTotalApproved'
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'finalAmount': {
                            '$subtract': [
                                '$AdvanceAmountTotalApproved', '$ExpenseAmountTotalApproved'
                            ]
                        }, 
                        'checkingAmount2': {
                            '$sum': [
                                '$AdvanceAmountTotalApproved', '$AdvanceAmountTotal'
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'checkingAmount': {
                            '$subtract': [
                                '$checkingAmount2', '$ExpenseAmountTotalApproved'
                            ]
                        }
                    }
                }
            ]
            # print('arrppparrppparrppp',arrppp)
            datatta = cmo.finding_aggregate("userRegister", arrppp)["data"]

            dynamicCheckingAmount = 0
            if is_valid_mongodb_objectid(advanceTypeId):
                arr = [
                    {"$match": {"designation": current_user["designation"]}},
                    {
                        "$lookup": {
                            "from": "claimType",
                            "localField": "claimTypeId",
                            "foreignField": "_id",
                            "pipeline": [
                                {
                                    "$match": {
                                        "deleteStatus": {"$ne": 1},
                                        "categoriesType": "Advance",
                                    }
                                }
                            ],
                            "as": "ClaimResults",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$ClaimResults",
                            "preserveNullAndEmptyArrays": False,
                        }
                    },
                    {
                        "$addFields": {
                            "value": {
                                "$cond": {
                                    "if": {
                                        "$and": [
                                            {"$ne": ["$value", True]},
                                            {"$ne": ["$value", False]},
                                            {"$ne": ["$value", ""]},
                                            {"$ne": ["$value", "Yes"]},
                                            {"$ne": ["$value", "No"]},
                                            {"$ne": ["$value", "yes"]},
                                            {"$ne": ["$value", "no"]},
                                        ]
                                    },
                                    "then": {"$toInt": "$value"},
                                    "else": "$value",
                                }
                            }
                        }
                    },
                    {"$group": {"_id": "$designation", "value": {"$sum": "$value"}}},
                    {"$project": {"_id": 0, "value": 1}},
                ]
                # print("arrarrarr3", arr)
                Responsess = cmo.finding_aggregate("mergerDesignationClaim", arr)["data"]
                if len(Responsess):
                    # print("ResponsessResponsess", Responsess, dynamicCheckingAmount)
                    dynamicCheckingAmount = dynamicCheckingAmount + int(
                        Responsess[0]["value"]
                    )
                if is_valid_mongodb_objectid(projectId):
                    projectId = ObjectId(projectId)
                data = {
                    "projectId": projectId,
                    # "advanceType": advanceType,
                    "advanceTypeId": ObjectId(advanceTypeId),
                    "Amount": int(amount),
                    "remark": remark,
                    "type": "Advance",
                    "status": "Submitted",
                    "customStatus": "Submitted",
                    "customisedStatus": 1,
                    "additionalInfo": additionalInfo,
                    "actionAt": get_current_date_timestamp(),
                }
                data["addedFor"] = ObjectId(current_user["userUniqueId"])
                data['email']=current_user["email"],
                data["designation"] = ObjectId(current_user["designation"])
                data["addedAt"] = get_current_date_timestamp()
                data["CreatedAt"] = int(unique_timestampexpense())
                ##### Logic for Advance No
                AdvanceNo = "ADV"
                # AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
                newArra = [{"$sort": {"_id": -1}}]
                responseData = cmo.finding_aggregate_with_deleteStatus(
                    "Advance", newArra
                )
                responseData = responseData["data"]
                if len(responseData) > 0:
                    if "AdvanceNo" in responseData[0]:
                        AdvanceNo = AdvanceNonewLogic()
                        
                        # isWrongExpense = cmo.checkAdvanceNo(AdvanceNo)
                        # while isWrongExpense:
                        #     AdvanceNo = AdvanceNonewLogic()

                        #     isWrongExpense = cmo.checkAdvanceNo(AdvanceNo)

                        # oldadvanceNo = responseData[0]["AdvanceNo"]
                        # AdvanceNo = generate_new_AdvanceNo(oldadvanceNo)
                        # if cmo.checkAdvanceNo(AdvanceNo):
                        #     AdvanceNo = "ADV"
                        #     arra = [{'$match': {'deleteStatus': {'$ne': 1},'fileType':'Advance'}}]
                        #     AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
                        #     newArra = [{"$sort": {"_id": -1}}]
                        #     responseData = cmo.finding_aggregate_with_deleteStatus("Advance", newArra)
                        #     responseData = responseData["data"]
                        #     if len(responseData) > 0:
                        #         if "AdvanceNo" in responseData[0]:
                        #             oldadvanceNo = responseData[0]["AdvanceNo"]
                        #             AdvanceNo = generate_new_AdvanceNo(oldadvanceNo)

                if len(responseData) < 1:
                    AdvanceNo = AdvanceNo + "000001"

                data["AdvanceNo"] = AdvanceNo
                financialyear = None
                varible = None
                varibleInt = None
                ggg = AdvanceNo.split("/")
                financialyear = ggg[1]
                varible = ggg[-1]
                varibleInt = int(varible)
                data["advanceNumberInt"] = varibleInt
                data["advanceNumberStr"] = varible
                data["FinancialYear"] = financialyear
                if len(datatta):

                    approvedAdvances = 0
                    claimedAdvances = int(datatta[0]["AdvanceAmountTotal"])
                    approvedAdvances = int(datatta[0]["AdvanceAmountTotalApproved"])
                    ApprovedExpenses = int(datatta[0]["ExpenseAmountTotalApproved"])
                    totallimit = int(dynamicCheckingAmount)
                    x = totallimit - (
                        (claimedAdvances + approvedAdvances) - ApprovedExpenses
                    )
                    # print('hhjjjj',claimedAdvances,ApprovedExpenses,totallimit)
                    if (
                        int(datatta[0]["checkingAmount"]) + int(data["Amount"])
                        > dynamicCheckingAmount
                    ):
                        # print('jjkkkk',int(datatta[0]["checkingAmount"]) + int(data["Amount"]),dynamicCheckingAmount)
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"Your approved Expenses is {datatta[0]['ExpenseAmountTotalApproved']} and your claimed Advance other than approved is {datatta[0]['AdvanceAmountTotal']} and your Approved Advance  is  already {datatta[0]['AdvanceAmountTotalApproved']} Advance and you can take advance upto {dynamicCheckingAmount} only! while you are filling {data['Amount']} then your amount Advance amount will be {data['Amount']+(claimedAdvances+approvedAdvances)} ,Now you can fill only upto{x}",
                            }
                        )
                # print('dddsdsdsdsd',data)

                Response = cmo.insertion("Advance", data)
                
                return respond(Response)

        if id != None and id != "undefined":
            arr = [
                {
                    "$match": {
                        "_id": ObjectId(id),
                        "customisedStatus": {"$nin": [1, 3, 5, 7]},
                    }
                },
                {"$project": {"_id": 0, "customisedStatus": 1}},
            ]
            Response = cmo.finding_aggregate("Advance", arr)
            if len(Response["data"]):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"This can not be editable Because its Status is not submitted",
                    }
                )
            else:
                chekingarray = ["", "undefined", None]
                Amount = request.form.get("Amount")
                additionalInfo = request.form.get("additionalInfo")
                remark = request.form.get("remark")
                if Amount not in chekingarray:
                    Amount = int(Amount)
                    if amount < 0:
                        return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": f"Amount Should be Positive",
                        }
                    )
                setData = {
                    "Amount": Amount,
                    "remark": remark,
                    "customisedStatus": 1,
                    "customStatus": "Submitted",
                    "status": "Submitted",
                    "actionAt": get_current_date_timestamp(),
                    "additionalInfo": additionalInfo,
                }
                setData["updatedAt"] = get_current_date_timestamp()
                lookData = {"_id": ObjectId(id)}
                arrp = [
                    {"$match": {"_id": ObjectId(id), "deleteStatus": {"$ne": 1}}},
                    {"$addFields": {"designation": {"$toString": "$designation"}}},
                    {
                        "$lookup": {
                            "from": "mergerDesignationClaim",
                            "let": {
                                "designation": "$designation",
                                "advanceTypeId": "$advanceTypeId",
                            },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                {
                                                    "$eq": [
                                                        "$$designation",
                                                        "$designation",
                                                    ]
                                                },
                                                {
                                                    "$eq": [
                                                        "$$advanceTypeId",
                                                        "$claimTypeId",
                                                    ]
                                                },
                                                {"$ne": ["$deleteStatus", 1]},
                                            ]
                                        }
                                    }
                                },
                                {"$project": {"value": 1, "_id": 0}},
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
                        "$lookup": {
                            "from": "mergerDesignationClaim",
                            "localField": "designation",
                            "foreignField": "designation",
                            "pipeline": [
                                {
                                    "$match": {
                                        "value": {"$ne": False},
                                        "deleteStatus": {"$ne": 1},
                                    }
                                },
                                {
                                    "$lookup": {
                                        "from": "claimType",
                                        "localField": "claimTypeId",
                                        "foreignField": "_id",
                                        "pipeline": [
                                            {"$match": {"categoriesType": "Advance"}}
                                        ],
                                        "as": "resultf",
                                    }
                                },
                                {
                                    "$addFields": {
                                        "length": {"$size": "$resultf"},
                                        "value": {"$toInt": "$value"},
                                    }
                                },
                                {"$match": {"length": {"$ne": 0}}},
                                {
                                    "$group": {
                                        "_id": None,
                                        "totalAmount": {"$sum": "$value"},
                                    }
                                },
                            ],
                            "as": "totalAdvancelimit",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$totalAdvancelimit",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$project": {
                            "value": {"$toInt": "$result.value"},
                            "_id": {"$toString": "$_id"},
                            "totalAdvancelimit": "$totalAdvancelimit.totalAmount",
                        }
                    },
                ]
                valuetoCheck = cmo.finding_aggregate("Advance", arrp)["data"]
                totalAdvancelimit = 0
                if len(valuetoCheck):
                    # print("rtuioirtyui", valuetoCheck[0])
                    totalAdvancelimit = totalAdvancelimit + int(valuetoCheck[0]["totalAdvancelimit"])
                    valuetoCheck = int(valuetoCheck[0]["value"])
                    if Amount > valuetoCheck:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"Your Advance Limit for this advance Type  is only {valuetoCheck}",
                            }
                        )
                arrppp = [
                    {"$match": {"_id": ObjectId(current_user["userUniqueId"])}}, {
                        '$lookup': {
                            'from': 'Expenses', 
                            'localField': '_id', 
                            'foreignField': 'addedFor', 
                            'pipeline': [
                                {
                                    '$match': {
                                        'deleteStatus': {
                                            '$ne': 1
                                        }, 
                                    }
                                }, {
                                    '$addFields': {
                                        'name': '$ExpenseNo', 
                                        'uniqueId': {
                                            '$toString': '$_id'
                                        }
                                    }
                                }
                            ], 
                            'as': 'ExpenseDetails'
                        }
                    }, {
                        '$lookup': {
                            'from': 'Advance', 
                            'localField': '_id', 
                            'foreignField': 'addedFor', 
                            'pipeline': [
                                {
                                    '$match': {
                                        'deleteStatus': {
                                            '$ne': 1
                                        },
                                        '_id': {
                                        '$ne': ObjectId(id)}
                                    }
                                }, {
                                    '$addFields': {
                                        'name': '$AdvanceNo', 
                                        'uniqueId': {
                                            '$toString': '$_id'
                                        }
                                    }
                                }
                            ], 
                            'as': 'AdvanceResults'
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
                            'as': 'OpeningBalance'
                        }
                    }, 
                    {
        '$addFields': {
            'OpeningBalance': {
                '$ifNull': [
                    {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$OpeningBalance', []
                                ]
                            }, 
                            'then': 0, 
                            'else': '$OpeningBalance'
                        }
                    }, 0
                ]
            }
        }
    },
                    
                    {
                        '$unwind': {
                            'path': '$OpeningBalance', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$addFields': {
                            'newData': {
                                '$concatArrays': [
                                    '$AdvanceResults', '$ExpenseDetails'
                                ]
                            }, 
                           'OpeningBalance': {
                '$ifNull': [
                    {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$OpeningBalance', 0
                                ]
                            }, 
                            'then': 0, 
                            'else': '$OpeningBalance.Amount'
                        }
                    }, 0
                ]
            }
        
                        }
                    }, {
                        '$unwind': {
                            'path': '$newData', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$project': {
                            'name': '$newData.name', 
                            'ApprovedAmount': '$newData.ApprovedAmount', 
                            'customStatus': '$newData.customStatus', 
                            'uniqueId': 1, 
                            'customisedStatus': '$newData.customisedStatus', 
                            'type': '$newData.type', 
                            'Amount': '$newData.Amount', 
                            'OpeningBalance': 1
                        }
                    }, {
                        '$group': {
                            '_id': '$name', 
                            'totalApprovedAmountRow': {
                                '$sum': {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$customisedStatus', 6
                                                    ]
                                                }
                                            ]
                                        }, '$ApprovedAmount', 0
                                    ]
                                }
                            }, 
                            'docs': {
                                '$push': '$$ROOT'
                            }
                        }
                    }, {
                        '$sort': {
                            '_id': 1
                        }
                    }, {
                        '$unwind': {
                            'path': '$docs'
                        }
                    }, {
                        '$addFields': {
                            'docs.totalApprovedAmountRow': '$totalApprovedAmountRow'
                        }
                    }, {
                        '$group': {
                            '_id': '$_id', 
                            'docs': {
                                '$first': '$docs'
                            }
                        }
                    }, {
                        '$replaceRoot': {
                            'newRoot': '$docs'
                        }
                    }, {
                        '$addFields': {
                            '_id': {
                                '$toString': '$_id'
                            }
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'ExpenseAmountTotalApproved': {
                                '$sum': {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$type', 'Expense'
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$customisedStatus', 6
                                                    ]
                                                }
                                            ]
                                        }, '$totalApprovedAmountRow', 0
                                    ]
                                }
                            }, 
                            'AdvanceAmountTotalApproved': {
                                '$sum': {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$type', 'Advance'
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$customisedStatus', 6
                                                    ]
                                                }
                                            ]
                                        }, '$ApprovedAmount', 0
                                    ]
                                }
                            }, 
                            'AdvanceAmountTotal': {
                                '$sum': {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$type', 'Advance'
                                                    ]
                                                }, {
                                                    '$in': [
                                                        '$customisedStatus', [
                                                            1, 2, 4
                                                        ]
                                                    ]
                                                }
                                            ]
                                        }, '$Amount', 0
                                    ]
                                }
                            }, 
                            'data': {
                                '$addToSet': '$$ROOT'
                            }
                        }
                    }, {
                        '$addFields': {
                            'OpeningBalance': {
                                '$arrayElemAt': [
                                    '$data.OpeningBalance', 0
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'ExpenseAmountTotalApproved': {
                                '$cond': {
                                    'if': {
                                        '$lt': [
                                            '$OpeningBalance', 0
                                        ]
                                    }, 
                                    'then': {
                                        '$subtract': [
                                            '$ExpenseAmountTotalApproved', '$OpeningBalance'
                                        ]
                                    }, 
                                    'else': '$ExpenseAmountTotalApproved'
                                }
                            }, 
                            'AdvanceAmountTotalApproved': {
                                '$cond': {
                                    'if': {
                                        '$gt': [
                                            '$OpeningBalance', 0
                                        ]
                                    }, 
                                    'then': {
                                        '$add': [
                                            '$AdvanceAmountTotalApproved', '$OpeningBalance'
                                        ]
                                    }, 
                                    'else': '$AdvanceAmountTotalApproved'
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'finalAmount': {
                                '$subtract': [
                                    '$AdvanceAmountTotalApproved', '$ExpenseAmountTotalApproved'
                                ]
                            }, 
                            'checkingAmount2': {
                                '$sum': [
                                    '$AdvanceAmountTotalApproved', '$AdvanceAmountTotal'
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'checkingAmount': {
                                '$subtract': [
                                    '$checkingAmount2', '$ExpenseAmountTotalApproved'
                                ]
                            }
                        }
                    }
                ]
                datatta = cmo.finding_aggregate("userRegister", arrppp)["data"]
                if len(datatta):
                    if int(datatta[0]["checkingAmount"]) + int(setData["Amount"]) > int(
                        totalAdvancelimit
                    ):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"Your Advance Limit is over",
                            }
                        )
                cmo.updating(
                    "Approval", {"AdvanceUniqueId": ObjectId(id)}, setData, False
                )
                Response = cmo.updating("Advance", lookData, setData, False)
                return respond(Response)
    if request.method == "DELETE":
        if id != None and id != "undefined":
            arr = [
                {
                    "$match": {
                        "_id": ObjectId(id),
                        "customisedStatus": {"$nin": [1, 3, 5, 7]},
                    }
                },
                {"$project": {"_id": 0, "customisedStatus": 1}},
            ]
            Response = cmo.finding_aggregate("Advance", arr)
            if len(Response["data"]):
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"This can not be deleted Because its Status is not submitted",
                    }
                )
            else:
                Response = cmo.deleting("Advance", id)
                cmo.real_deleting("Approval", {"AdvanceUniqueId": ObjectId(id)})
                return respond(Response)
        else:
            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": f"id not found",
                }
            )



###to get advace claim Type according to user Designation
@mobile_blueprint.route("/mobile/claimTypeAdvance", methods=["GET", "POST", "DELETE"])
@mobile_blueprint.route("/mobile/claimTypeAdvance/<id>", methods=["GET", "POST", "DELETE"])
@token_required
def claimTypeAdvance(current_user, id=None):
   
    if request.method == "GET":
        arr = [
    {
        '$match': {
            'value': {
                '$ne': False
            }, 
            'designation': current_user["designation"]
        }
    }, {
        '$lookup': {
            'from': 'claimType', 
            'localField': 'claimTypeId', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'categoriesType': 'Advance'
                    }
                }
            ], 
            'as': 'claimResult'
        }
    }, {
        '$unwind': {
            'path': '$claimResult', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$addFields': {
            'DesignationClaimid': {
                '$toString': '$DesignationClaimid'
            }, 
            'claimTypeId': {
                '$toString': '$claimTypeId'
            }, 
            'mergerDesignationClaimId': {
                '$toString': '$_id'
            }
        }
    }, {
        '$group': {
            '_id': '$claimTypeId', 
            'doc': {
                '$first': '$$ROOT'
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$doc'
        }
    }, {
        '$project': {
            '_id': 0, 
            'claimResult': 0
        }
    }
]
        

        Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
        return respond(Response)

@mobile_blueprint.route("/mobile/export/ExpensesAndAdvance", methods=["GET", "POST"])
@mobile_blueprint.route("/mobile/export/ExpensesAndAdvance/<id>", methods=["GET", "POST"])
@token_required
def exportExpensesAndAdvance(current_user, id=None):
    if request.method == "GET":
        arr = [
            {"$match": {"_id": ObjectId(current_user["userUniqueId"])}},
            {
                "$lookup": {
                    "from": "Expenses",
                    "localField": "_id",
                    "foreignField": "addedFor",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$lookup": {
                                "from": "project",
                                "localField": "projectId",
                                "foreignField": "_id",
                                "pipeline": [
                                    {"$match": {"deleteStatus": {"$ne": 1}}},
                                    {
                                        "$lookup": {
                                            "from": "projectGroup",
                                            "localField": "projectGroup",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {
                                                    "$match": {
                                                        "deleteStatus": {"$ne": 1}
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
                                                            },
                                                            {
                                                                "$project": {
                                                                    "_id": 0,
                                                                    "costCenter": 1,
                                                                }
                                                            },
                                                        ],
                                                        "as": "costcenterresult",
                                                    }
                                                },
                                                {
                                                    "$addFields": {
                                                        "costcenter": "$costcenterresult.costCenter"
                                                    }
                                                },
                                                {
                                                    "$unwind": {
                                                        "path": "$costcenter",
                                                        "preserveNullAndEmptyArrays": True,
                                                    }
                                                },
                                            ],
                                            "as": "projectGroupResult",
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$projectGroupResult",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "costcenter": "$projectGroupResult.costcenter",
                                            "circle": {"$toObjectId": "$circle"},
                                        }
                                    },
                                    {
                                        "$lookup": {
                                            "from": "circle",
                                            "localField": "circle",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {"$match": {"deleteStatus": {"$ne": 1}}}
                                            ],
                                            "as": "circle",
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$circle",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                    {"$addFields": {"circle": "$circle.circleName"}},
                                ],
                                "as": "projectResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$projectResult",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$addFields": {
                                "projectIdName": "$projectResult.projectId",
                                "name": "$ExpenseNo",
                                "uniqueId": {"$toString": "$_id"},
                            }
                        },
                    ],
                    "as": "ExpenseDetails",
                }
            },
            {
                "$lookup": {
                    "from": "Advance",
                    "localField": "_id",
                    "foreignField": "addedFor",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$lookup": {
                                "from": "project",
                                "localField": "projectId",
                                "foreignField": "_id",
                                "pipeline": [
                                    {"$match": {"deleteStatus": {"$ne": 1}}},
                                    {
                                        "$lookup": {
                                            "from": "projectGroup",
                                            "localField": "projectGroup",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {
                                                    "$match": {
                                                        "deleteStatus": {"$ne": 1}
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
                                                            },
                                                            {
                                                                "$project": {
                                                                    "_id": 0,
                                                                    "costCenter": 1,
                                                                }
                                                            },
                                                        ],
                                                        "as": "costcenterresult",
                                                    }
                                                },
                                                {
                                                    "$addFields": {
                                                        "costcenter": "$costcenterresult.costCenter"
                                                    }
                                                },
                                                {
                                                    "$unwind": {
                                                        "path": "$costcenter",
                                                        "preserveNullAndEmptyArrays": True,
                                                    }
                                                },
                                            ],
                                            "as": "projectGroupResult",
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$projectGroupResult",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "costcenter": "$projectGroupResult.costcenter",
                                            "circle": {"$toObjectId": "$circle"},
                                        }
                                    },
                                    {
                                        "$lookup": {
                                            "from": "circle",
                                            "localField": "circle",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {"$match": {"deleteStatus": {"$ne": 1}}}
                                            ],
                                            "as": "circle",
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$circle",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                    {"$addFields": {"circle": "$circle.circleName"}},
                                ],
                                "as": "projectResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$projectResult",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$addFields": {
                                "projectIdName": "$projectResult.projectId",
                                "name": "$AdvanceNo",
                                "uniqueId": {"$toString": "$_id"},
                            }
                        },
                    ],
                    "as": "AdvanceResults",
                }
            },
            {
                "$addFields": {
                    "newData": {"$concatArrays": ["$AdvanceResults", "$ExpenseDetails"]}
                }
            },
            {"$unwind": {"path": "$newData", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "AddedAt": {"$toDate": "$newData.addedAt"},
                    "uniqueId": "$newData.uniqueId",
                    "expenseDate": {"$toDate": "$newData.expenseDate"},
                }
            },
            {
                "$addFields": {
                    "expenseDate": {
                        "$dateToString": {"format": "%d-%m-%Y", "date": "$expenseDate","timezone":"Asia/Kolkata"}
                    }
                }
            },
            {
                "$addFields": {
                    "month": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 1]},
                                    "then": "Jan",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 2]},
                                    "then": "Feb",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 3]},
                                    "then": "Mar",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 4]},
                                    "then": "Apr",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 5]},
                                    "then": "May",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 6]},
                                    "then": "Jun",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 7]},
                                    "then": "Jul",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 8]},
                                    "then": "Aug",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 9]},
                                    "then": "Sep",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 10]},
                                    "then": "Oct",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 11]},
                                    "then": "Nov",
                                },
                                {
                                    "case": {"$eq": [{"$month": "$AddedAt"}, 12]},
                                    "then": "Dec",
                                },
                            ],
                            "default": None,
                        }
                    },
                    "year": {"$year": "$AddedAt"},
                }
            },
            {"$addFields": {"year": {"$toString": "$year"}}},
            {
                "$project": {
                    "AddedAt": {
                        "$dateToString": {"format": "%d-%m-%Y", "date": "$AddedAt","timezone":"Asia/Kolkata"}
                    },
                    "addedMonth": {"$concat": ["$month", "/", "$year"]},
                    "expenseDate": "$expenseDate",
                    "name": "$newData.name",
                    "costcenter": "$newData.projectResult.costcenter",
                    "remark": "$newData.remark",
                    "ApprovedAmount": "$newData.ApprovedAmount",
                    "customStatus": "$newData.customStatus",
                    "uniqueId": 1,
                    "customisedStatus": "$newData.customisedStatus",
                    "type": "$newData.type",
                }
            },
            {
                "$group": {
                    "_id": "$name",
                    "totalApprovedAmount": {"$sum": "$ApprovedAmount"},
                    "totalApprovedAmountRow": {
                        "$sum": {
                            "$cond": [
                                {"$and": [{"$eq": ["$customisedStatus", 6]}]},
                                "$ApprovedAmount",
                                0,
                            ]
                        }
                    },
                    "docs": {"$push": "$$ROOT"},
                }
            },
            {"$sort": {"_id": 1}},
            {"$unwind": {"path": "$docs"}},
            {
                "$addFields": {
                    "docs.totalApprovedAmount": "$totalApprovedAmount",
                    "docs.totalApprovedAmountRow": "$totalApprovedAmountRow",
                }
            },
            {"$group": {"_id": "$_id", "docs": {"$first": "$docs"}}},
            {"$replaceRoot": {"newRoot": "$docs"}},
            {"$addFields": {"_id": {"$toString": "$_id"}}},
            {
                "$addFields": {
                    "finalAmount": {
                        "$subtract": ["$AdvanceAmountTotal", "$ExpenseAmountTotal"]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Month-Year": "$addedMonth",
                    "Expense/Advance": "$name",
                    "Cost Center": "$costCenter",
                    "Claim Date": {
                        "$cond": {
                            "if": {"$eq": ["$type", "Advance"]},
                            "then": "$AddedAt",
                            "else": "$expenseDate",
                        }
                    },
                    "Submission Date": "$AddedAt",
                    "Advance (Credit)": {
                        "$cond": {
                            "if": {"$eq": ["$type", "Advance"]},
                            "then": "$totalApprovedAmountRow",
                            "else": 0,
                        }
                    },
                    "Expense (Debit)": {
                        "$cond": {
                            "if": {"$eq": ["$type", "Expense"]},
                            "then": "$totalApprovedAmountRow",
                            "else": 0,
                        }
                    },
                    "Status": "$customStatus",
                }
            },
        ]
        # print("arrggggr", arr)
        response = cmo.finding_aggregate("userRegister", arr)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        for col in dataframe.columns:
            dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
        fullPath = excelWriteFunc.excelFileWriter(
            dataframe, "Export_Expenses", "Expenses And Advance"
        )
        # print("fullPathfullPathfullPath", fullPath)
        return send_file(fullPath)



@mobile_blueprint.route("/mobile/DownloadAttachment", methods=["GET", "POST"])
@mobile_blueprint.route("/mobile/DownloadAttachment/<id>", methods=["GET", "POST"])
@token_required
def DownloadAttachment(id=None):
    expenseId = request.args.get("expenseId")
    if request.method == "GET":
        if expenseId != None:
            if expenseId.startswith("EXP"):
                arr = [
    {
        '$match': {
            'ExpenseNo': expenseId
        }
    }, {
        '$lookup': {
            'from': 'milestone', 
            'localField': 'taskName', 
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
                        'Name': 1
                    }
                }
            ], 
            'as': 'milestoneresult'
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'addedFor', 
            'foreignField': '_id', 
            'as': 'userInfo'
        }
    }, {
        '$lookup': {
            'from': 'SiteEngineer', 
            'localField': 'Site Id', 
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
                        'Site Id': 1
                    }
                }
            ], 
            'as': 'siteResults'
        }
    }, {
        '$lookup': {
            'from': 'project', 
            'localField': 'projectId', 
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
                                            '$project': {
                                                '_id': 0, 
                                                'costCenter': 1
                                            }
                                        }
                                    ], 
                                    'as': 'costcenterresult'
                                }
                            }, {
                                '$addFields': {
                                    'costcenter': '$costcenterresult.costCenter'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$costcenter', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }
                        ], 
                        'as': 'projectGroupResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$projectGroupResult', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'costcenter': '$projectGroupResult.costcenter'
                    }
                }
            ], 
            'as': 'projectResult'
        }
    }, {
        '$unwind': {
            'path': '$projectResult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$unwind': {
            'path': '$userInfo', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$unwind': {
            'path': '$siteResults', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$unwind': {
            'path': '$milestoneresult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'claimType', 
            'localField': 'claimType', 
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
            'as': 'claimTyperesult'
        }
    }, {
        '$unwind': {
            'path': '$claimTyperesult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'actionBy', 
            'foreignField': '_id', 
            'as': 'approverResults'
        }
    }, {
        '$unwind': {
            'path': '$approverResults', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'projectId': '$projectResult.projectId', 
            'costcenter': '$projectResult.costcenter', 
            'Claim_Date': {
                '$toDate': '$addedAt'
            }, 
            'Signature': '$approverResults.empName', 
            'empCode': '$userInfo.empCode', 
            'empName': '$userInfo.empName', 
            'mobile': '$userInfo.mobile', 
            'Site_Id': '$siteResults.Site Id', 
            'Task': '$milestoneresult.Name', 
            'ClaimType': '$claimTyperesult.claimType', 
            'expenseDate': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$eq': [
                                    '$newData.expenseDate', ''
                                ]
                            }, {
                                '$regexMatch': {
                                    'input': '$newData.expenseDate', 
                                    'regex': re.compile(r"^\\d{2}:\d{2}:\d{4}$")
                                }
                            }, {
                                '$regexMatch': {
                                    'input': '$newData.expenseDate', 
                                    'regex': re.compile(r"^\\d{4}:\d{2}:\d{2}$")
                                }
                            }, {
                                '$regexMatch': {
                                    'input': '$newData.expenseDate', 
                                    'regex': re.compile(r"^\\d{1}:\d{2}:\d{4}$")
                                }
                            }
                        ]
                    }, 
                    'then': '$newData.expenseDate', 
                    'else': {
                        '$toDate': '$newData.expenseDate'
                    }
                }
            },
            'actionAt': {
                '$toDate': '$actionAt'
            }
        }
    }, {
        '$addFields': {
            'Claim_Date': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$Claim_Date'
                }
            }, 
            'expenseuniqueId': {
                '$toString': '$_id'
            }, 
            'addedFor': {
                '$toString': '$addedFor'
            }, 
            'addedBy': {
                '$toString': '$addedBy'
            }, 
            'expenseDate': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$expenseDate'
                }
            }, 
            'actionAt': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$actionAt'
                }
            }, 
            'L1ApproverStatus': {
                '$arrayElemAt': [
                    '$L1Approver.status', 0
                ]
            }, 
            'L1ApproverId': {
                '$arrayElemAt': [
                    '$L1Approver.actionBy', 0
                ]
            }, 
            'L2ApproverStatus': {
                '$arrayElemAt': [
                    '$L2Approver.status', 0
                ]
            }, 
            'L2ApproverId': {
                '$arrayElemAt': [
                    '$L2Approver.actionBy', 0
                ]
            }, 
            'L3ApproverStatus': {
                '$arrayElemAt': [
                    '$L3Approver.status', 0
                ]
            }, 
            'L3ApproverId': {
                '$arrayElemAt': [
                    '$L3Approver.actionBy', 0
                ]
            }, 
            'L1ApproverDate': {
                '$arrayElemAt': [
                    '$L1Approver.actionAt', 0
                ]
            }, 
            'L2ApproverDate': {
                '$arrayElemAt': [
                    '$L2Approver.actionAt', 0
                ]
            }, 
            'L3ApproverDate': {
                '$arrayElemAt': [
                    '$L3Approver.actionAt', 0
                ]
            }
        }
    }, {
        '$addFields': {
            'L1ApproverDate': {
                '$toDate': '$L1ApproverDate'
            }, 
            'L2ApproverDate': {
                '$toDate': '$L2ApproverDate'
            }, 
            'L3ApproverDate': {
                '$toDate': '$L3ApproverDate'
            }
        }
    }, {
        '$addFields': {
            'L1ApproverDate': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$L1ApproverDate'
                }
            }, 
            'L2ApproverDate': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$L2ApproverDate'
                }
            }, 
            'L3ApproverDate': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$L3ApproverDate'
                }
            }
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'L1ApproverId', 
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
                        'empName': 1
                    }
                }
            ], 
            'as': 'L1ApproverResult'
        }
    }, {
        '$unwind': {
            'path': '$L1ApproverResult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'L2ApproverId', 
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
                        'empName': 1
                    }
                }
            ], 
            'as': 'L2ApproverResult'
        }
    }, {
        '$unwind': {
            'path': '$L2ApproverResult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'L3ApproverId', 
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
                        'empName': 1
                    }
                }
            ], 
            'as': 'L3ApproverResult'
        }
    }, {
        '$unwind': {
            'path': '$L3ApproverResult', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$project': {
            'Site Id': 0, 
            'taskName': 0, 
            'claimType': 0, 
            'milestoneresult': 0, 
            'siteResults': 0, 
            'userInfo': 0, 
            'projectResult': 0, 
            '_id': 0, 
            'claimTyperesult': 0, 
            'ExpenseUniqueId': 0, 
            'actionBy': 0
        }
    }, {
        '$project': {
            'Expense No': '$ExpenseNo', 
            'Expense Date': '$expenseDate', 
            'Claim Type': '$ClaimType', 
            'Category': '$categories', 
            'Start KM': '$startKm', 
            'End KM': '$endKm', 
            'Total Distance': '$Total_distance', 
            'Cost Center': '$costcenter', 
            'Project ID': '$projectId', 
            'Site ID': '$Site_Id', 
            'Task Name': '$Task', 
            'Employee Name': '$empName', 
            'Employee Code': '$empCode', 
            'Claimed Amount': '$Amount', 
            'Approved Amount': '$ApprovedAmount', 
            'Bill Number': '$billNumber', 
            'Check-In Date': '$checkInDate', 
            'Check-Out Date': '$checkOutDate', 
            'Total Days': '$totaldays', 
            'Remark': '$remark', 
            'Status': '$customStatus', 
            'Attachment': '$attachment', 
            'Signature': 1, 
            'Approved At': '$actionAt', 
            'User Signature': '$empName', 
            'L1-Approver': '$L1ApproverResult.empName', 
            'L2-Approver': '$L2ApproverResult.empName', 
            'L3-Approver': '$L3ApproverResult.empName', 
            'L1-Approver Date': '$L1ApproverDate', 
            'L2-Approver Date': '$L2ApproverDate', 
            'L3-Approver Date': '$L3ApproverDate', 
            'L1ApproverStatus': '$L1ApproverStatus', 
            'L2ApproverStatus': '$L2ApproverStatus', 
            'L3ApproverStatus': '$L3ApproverStatus'
        }
    }
]
                Response = cmo.finding_aggregate("Expenses", arr)

            elif expenseId.startswith("ADV"):
                arr = [
                    {"$match": {"AdvanceNo": expenseId}},
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "addedFor",
                            "foreignField": "_id",
                            "as": "userInfo",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "project",
                            "localField": "projectId",
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
                                                        },
                                                        {
                                                            "$project": {
                                                                "_id": 0,
                                                                "costCenter": 1,
                                                            }
                                                        },
                                                    ],
                                                    "as": "costcenterresult",
                                                }
                                            },
                                            {
                                                "$addFields": {
                                                    "costcenter": "$costcenterresult.costCenter"
                                                }
                                            },
                                            {
                                                "$unwind": {
                                                    "path": "$costcenter",
                                                    "preserveNullAndEmptyArrays": True,
                                                }
                                            },
                                        ],
                                        "as": "projectGroupResult",
                                    }
                                },
                                {
                                    "$unwind": {
                                        "path": "$projectGroupResult",
                                        "preserveNullAndEmptyArrays": True,
                                    }
                                },
                                {
                                    "$addFields": {
                                        "costcenter": "$projectGroupResult.costcenter"
                                    }
                                },
                            ],
                            "as": "projectResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$projectResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$userInfo",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "actionBy",
                            "foreignField": "_id",
                            "as": "approverResults",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$approverResults",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$addFields": {
                            "projectId": "$projectResult.projectId",
                            "costcenter": "$projectResult.costcenter",
                            "Claim_Date": {"$toDate": "$addedAt"},
                            "Signature": "$approverResults.empName",
                            "empCode": "$userInfo.empCode",
                            "empName": "$userInfo.empName",
                            "mobile": "$userInfo.mobile",
                            "advanceType": "$advanceType",
                            "actionAt": {"$toDate": "$actionAt"},
                        }
                    },
                    {
                        "$addFields": {
                            "Claim_Date": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$Claim_Date",
                                }
                            },
                            "expenseuniqueId": {"$toString": "$_id"},
                            "addedFor": {"$toString": "$addedFor"},
                            "addedBy": {"$toString": "$addedBy"},
                            "expenseDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$expenseDate",
                                }
                            },
                            "actionAt": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$actionAt",
                                }
                            },
                            "L1ApproverStatus": {
                                "$arrayElemAt": ["$L1Approver.status", 0]
                            },
                            "L1ApproverId": {
                                "$arrayElemAt": ["$L1Approver.actionBy", 0]
                            },
                            "L2ApproverStatus": {
                                "$arrayElemAt": ["$L2Approver.status", 0]
                            },
                            "L2ApproverId": {
                                "$arrayElemAt": ["$L2Approver.actionBy", 0]
                            },
                            "L3ApproverStatus": {
                                "$arrayElemAt": ["$L3Approver.status", 0]
                            },
                            "L3ApproverId": {
                                "$arrayElemAt": ["$L3Approver.actionBy", 0]
                            },
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "L1ApproverId",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$project": {"_id": 0, "empName": 1}},
                            ],
                            "as": "L1ApproverResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$L1ApproverResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "L2ApproverId",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$project": {"_id": 0, "empName": 1}},
                            ],
                            "as": "L2ApproverResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$L2ApproverResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "userRegister",
                            "localField": "L3ApproverId",
                            "foreignField": "_id",
                            "pipeline": [
                                {"$match": {"deleteStatus": {"$ne": 1}}},
                                {"$project": {"_id": 0, "empName": 1}},
                            ],
                            "as": "L3ApproverResult",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$L3ApproverResult",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$project": {
                            "Site Id": 0,
                            "taskName": 0,
                            "claimType": 0,
                            "milestoneresult": 0,
                            "siteResults": 0,
                            "userInfo": 0,
                            "projectResult": 0,
                            "_id": 0,
                            "claimTyperesult": 0,
                            "ExpenseUniqueId": 0,
                            "actionBy": 0,
                        }
                    },
                    {
                        "$project": {
                            "Cost Center": "$costcenter",
                            "Project ID": "$projectId",
                            "Employee Name": "$empName",
                            "Employee Code": "$empCode",
                            "Claimed Amount": "$Amount",
                            "Approved Amount": "$ApprovedAmount",
                            "Remark": "$remark",
                            "Status": "$customStatus",
                            "Advance Date": "$Claim_Date",
                            "Signature": 1,
                            "Approved At": "$actionAt",
                            "L1Approver": "$L1ApproverResult.empName",
                            "L2Approver": "$L2ApproverResult.empName",
                            "L3Approver": "$L3ApproverResult.empName",
                        }
                    },
                ]
                # print("vjklkjhgghjkl", arr)
                Response = cmo.finding_aggregate("Advance", arr)
                # print("ResponseRespggggonseResponse", Response)
            if len(Response["data"]):
                Response = Response["data"]
                # print("ggghhhh", Response)

                try:
                    title = "Expense/Advance Report"
                    output_path = os.path.join(
                        os.getcwd(), "uploads", "Expense_Report.pdf"
                    )
                    pdf_buffer = generate_pdf_from_dict(Response, output_path, title)
                    # print("pdfffpathhhhhhh", output_path)
                    return send_file(output_path)
                    # return send_file(output_path, as_attachment=True, download_name='Expense_Report.pdf', mimetype='application/pdf')
                except Exception as e:
                    # print("uuuuu", e)
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "Some thing went Wrong",
                        }
                    )

            else:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": "No Data Found",
                    }
                )
        else:
            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": "Id not found",
                }
            )

@mobile_blueprint.route("/mobile/fillDAEmpData", methods=["GET", "POST"])
@mobile_blueprint.route("/mobile/fillDAEmpData/<id>", methods=["GET", "POST"])
@token_required
def expensesfillDAEmpData(current_user, id=None):
    if request.method == "GET":
        arr = [
            {"$match": {"L1Approver": current_user["userUniqueId"],'status':'Active'}},
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "userRoleId": {"$toString": "$userRole"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "email": 1,
                    "uniqueId": 1,
                    "empName": 1,
                    "empCode": 1,
                    "designation": 1,
                    "userRoleId": 1,
                }
            },
        ]
        Response = cmo.finding_aggregate("userRegister", arr)
        return respond(Response)


@mobile_blueprint.route("/mobile/fillDA", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@mobile_blueprint.route("/mobile/fillDA/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def expensesFillDA(current_user, id=None):
    if request.method == "GET":
        arr = []
        if id != None and id != "undefined":
            arr = [{"$match": {"_id": ObjectId(id)}}]
        arr = arr + [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "fillBy": "L1-Approver",
                    "addedBy": ObjectId(current_user["userUniqueId"]),
                }
            },
            {
                "$lookup": {
                    "from": "milestone",
                    "localField": "taskName",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": 0, "Name": 1}},
                    ],
                    "as": "milestoneresult",
                }
            },
            {
                "$lookup": {
                    "from": "userRegister",
                    "localField": "addedFor",
                    "foreignField": "_id",
                    "as": "userInfo",
                }
            },
            {
                "$lookup": {
                    "from": "SiteEngineer",
                    "localField": "Site Id",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": 0, "Site Id": 1}},
                    ],
                    "as": "siteResults",
                }
            },
            {
                "$lookup": {
                    "from": "project",
                    "localField": "projectId",
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
                                            "from": "costCenter",
                                            "localField": "costCenterId",
                                            "foreignField": "_id",
                                            "pipeline": [
                                                {
                                                    "$match": {
                                                        "deleteStatus": {"$ne": 1}
                                                    }
                                                },
                                                {
                                                    "$project": {
                                                        "_id": 0,
                                                        "costCenter": 1,
                                                    }
                                                },
                                            ],
                                            "as": "costcenterresult",
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "costcenter": "$costcenterresult.costCenter"
                                        }
                                    },
                                    {
                                        "$unwind": {
                                            "path": "$costcenter",
                                            "preserveNullAndEmptyArrays": True,
                                        }
                                    },
                                ],
                                "as": "projectGroupResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$projectGroupResult",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {
                            "$addFields": {
                                "costcenter": "$projectGroupResult.costCenterId",
                                "costcenterName": "$projectGroupResult.costcenter",
                            }
                        },
                    ],
                    "as": "projectResult",
                }
            },
            {"$unwind": {"path": "$projectResult", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$siteResults", "preserveNullAndEmptyArrays": True}},
            {
                "$unwind": {
                    "path": "$milestoneresult",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$lookup": {
                    "from": "claimType",
                    "localField": "claimType",
                    "foreignField": "_id",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "claimTyperesult",
                }
            },
            {
                "$unwind": {
                    "path": "$claimTyperesult",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                '$addFields': {
                    'expenseDate': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$expenseDate', ''
                                        ]
                                    }, {
                                        '$regexMatch': {
                                            'input': '$expenseDate', 
                                            'regex': '^\\d{2}:\\d{2}:\\d{4}$'
                                        }
                                    }, {
                                        '$regexMatch': {
                                            'input': '$expenseDate', 
                                            'regex': '^\\d{4}:\\d{2}:\\d{2}$'
                                        }
                                    }, {
                                        '$regexMatch': {
                                            'input': '$expenseDate', 
                                            'regex': '^\\d{1}:\\d{2}:\\d{4}$'
                                        }
                                    }
                                ]
                            }, 
                            'then': None, 
                            'else': {
                                '$toDate': '$expenseDate'
                            }
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "projectIdName": "$projectResult.projectId",
                    "costcenter": "$projectResult.costcenter",
                    "costcenterName": "$projectResult.costcenterName",
                    "Claim_Date": {"$toDate": "$expenseDate"},
                    "empCode": "$userInfo._id",
                    "empName": "$userInfo._id",
                    "employeeCode": "$userInfo.empCode",
                    "employeeName": "$userInfo.empName",
                    "mobile": "$userInfo.mobile",
                    "Site_Id": "$siteResults.Site Id",
                    "Task": "$milestoneresult.Name",
                    "ClaimType": "$claimTyperesult._id",
                    "ClaimTypeName": "$claimTyperesult.claimType",
                }
            },
            {
                "$addFields": {
                    "Claim_Date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$Claim_Date",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "expenseuniqueId": {"$toString": "$_id"},
                    "addedFor": {"$toString": "$addedFor"},
                    "addedBy": {"$toString": "$addedBy"},
                    "uniqueId": {"$toString": "$_id"},
                    "projectId": {"$toString": "$projectId"},
                }
            },
            {
                "$project": {
                    "Site Id": 0,
                    "taskName": 0,
                    "milestoneresult": 0,
                    "siteResults": 0,
                    "userInfo": 0,
                    "projectResult": 0,
                    "_id": 0,
                    "claimTyperesult": 0,
                    "ExpenseUniqueId": 0,
                    "actionBy": 0,
                    "L1Approver": 0,
                    "L2Approver": 0,
                    "L3Approver": 0,
                    "L1-Approver": 0,
                    "L2-Approver": 0,
                    "L3-Approver": 0,
                }
            },
            {
                "$addFields": {
                    "claimType": {"$toString": "$claimType"},
                    "costcenter": {"$toString": "$costcenter"},
                    "empCode": {"$toString": "$empCode"},
                    "empName": {"$toString": "$empName"},
                    "ClaimType": {"$toString": "$ClaimType"},
                }
            },
            {"$group": {"_id": "$ExpenseNo", "data": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$data"}},
            {"$sort": {"expenseuniqueId": 1}},
        ]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        Response = cmo.finding_aggregate("Expenses", arr)
        return respond(Response)
    
    if request.method == "POST":
        if id == None:
            data = request.get_json()
            claimType = data["claimType"]
            Amount = data["Amount"]
            projectId = data["projectId"]
            
            
            siteandTaskmatchingData = {
                "Amount": Amount,
            }
            if is_valid_mongodb_objectid(claimType):
                claimType = ObjectId(claimType)
            else:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"Invalid Claim Type Not Found",
                    }
                )
            arro=[
                {
                    '$match': {
                        '_id': ObjectId(data['EmpCode'])
                    }
                }, {
                    '$project': {
                        'designation': 1, 
                        '_id': 0
                    }
                }
            ]
            currentUserDesignation=current_user["designation"]
            Responses=cmo.finding_aggregate("userRegister",arro)['data']
            if len(Responses):
                currentUserDesignation=Responses[0]['designation']
            arr = [
                {
                    "$match": {
                        "claimTypeId": (claimType),
                        "designation": currentUserDesignation,
                    }
                },
                {
                    "$lookup": {
                        "from": "DesignationclaimType",
                        "localField": "DesignationClaimid",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {"$project": {"taskName": 1, "siteId": 1, "_id": 0}},
                        ],
                        "as": "DesignationclaimTypeResult",
                    }
                },
                {
                    "$unwind": {
                        "path": "$DesignationclaimTypeResult",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "siteId": "$DesignationclaimTypeResult.siteId",
                        "taskName": "$DesignationclaimTypeResult.taskName",
                        "claimTypeId": {"$toString": "$claimTypeId"},
                    }
                },
                {
                    "$project": {
                        "DesignationclaimTypeResult": 0,
                        "_id": 0,
                        "DesignationClaimid": 0,
                    }
                },
            ]
            # print("ghklgfdfghjkl;", arr)
            dataTomatch = cmo.finding_aggregate("mergerDesignationClaim", arr)["data"]
            if len(dataTomatch):
                dataTomatch = dataTomatch[0]
            if len(dataTomatch) < 1:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"Claim Type Not Found",
                    }
                )

            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": claimType}},
                {"$addFields": {"claimTypeId": {"$toString": "$_id"}}},
                {"$project": {"_id": 0}},
            ]
            shortcodeData = cmo.finding_aggregate("claimType", arr)["data"]
            if dataTomatch["value"] != True:
                # print("Amounggggt", dataTomatch["value"], Amount)
                if Amount != None or Amount != "undefined":
                    if float(Amount) > int(dataTomatch["value"]):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"You can fill amount only equal to or less than {dataTomatch['value']} not for this {Amount}",
                            }
                        )
            # expenseNo=
            ####logic for expenseNo

            if data["EmpCode"] != None:
                arrry = [
                    {"$match": {"_id": ObjectId(data["EmpCode"])}},
                    {"$project": {"_id": 0, "designation": 1}},
                ]
                rtyuuytrt = cmo.finding_aggregate("userRegister", arrry)["data"]
                if len(rtyuuytrt):
                    data["designation"] = rtyuuytrt[0]["designation"]
            if claimType is not None and claimType != "undefined":
                ExpenseNo = "EXP"

                

                if len(shortcodeData) > 0:
                    ExpenseNo = ExpenseNonewLogic()
                        
            if is_valid_mongodb_objectid(projectId):
                projectId = ObjectId(projectId)

            financialyear = None
            varible = None
            varibleInt = None

            if ExpenseNo not in ["", "undefined", None]:
                ggg = ExpenseNo.split("/")
                financialyear = ggg[1]
                varible = ggg[-1]
                varibleInt = int(varible)

            datatoinsert = {
                "ExpenseNo": ExpenseNo,
                "claimType": claimType,
                "Amount": float(Amount),
                "addedAt": get_current_date_timestamp(),
                "actionAt": get_current_date_timestamp(),
                "actionBy": ObjectId(current_user["userUniqueId"]),
                "CreatedAt": int(unique_timestampexpense()),
                "expenseDate": data["Claim_Date"],
                "addedFor": ObjectId(data["EmpCode"]),
                "designation": data["designation"],
                "addedBy": ObjectId(current_user["userUniqueId"]),
                "projectId": projectId,
                "status": "L1-Approved",
                "customStatus": "L1-Approved",
                "customisedStatus": 2,
                "type": "Expense",
                "fillBy": "L1-Approver",
                "remark": data["remark"],
                "action": "Approved",
                "additionalInfo": data["additionalInfo"],
                "FinancialYear": financialyear,
                "expenseNumberInt": varibleInt,
                "expenseNumberStr": varible,
                'ApprovedAmount':float(Amount)
            }
            datatoinsert["L1Approver"] = []
            datatoinsert["L1Approver"].insert(
                0,
                {
                    "actionBy": ObjectId(current_user["userUniqueId"]),
                    "actionAt": get_current_date_timestamp(),
                    "status": "L1-Approved",
                    "customisedStatus": 2,
                    'ApprovedAmount':float(Amount)
                },
            )

            arr4=[
                {
                    '$match': {
                        'ExpenseNo': ExpenseNo, 
                        
                    }
                },
                            {
                    '$project': {
                        'addedFor': 1, 
                        '_id': 0
                    }
                }
            ]
            Response4=cmo.finding_aggregate_with_deleteStatus("Expenses",arr4)['data']
            if len(Response4):
                userId=Response4[0]['addedFor']
                print(userId,'userId')
                if userId != datatoinsert['addedFor']:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": f"Please Try Again,System is in Another Process",
                        }
                    )
            cmo.insertion("TestingExpenses",{'userId':datatoinsert['addedFor'],'ExpenseNo':ExpenseNo})

            Response = cmo.insertion("Expenses", datatoinsert)

            datatoinsert2 = {
                "ExpenseNo": ExpenseNo,
                "action": "Approved",
                "actionAt": get_current_date_timestamp(),
                "actionBy": ObjectId(current_user["userUniqueId"]),
                "addedFor": ObjectId(data["EmpCode"]),
                "status": "L1-Approved",
                "customStatus": "L1-Approved",
                "customisedStatus": 2,
                "remark": data["remark"],
                "type": "Expense",
                "fillBy": "L1-Approver",
                "additionalInfo": data["additionalInfo"],
                'ApprovedAmount':float(Amount),
                'Amount':float(Amount)
            }
            datatoinsert2["ExpenseUniqueId"] = ObjectId(Response["operation_id"])
            datatoinsert2["L1Approver"] = []
            datatoinsert2["L1Approver"].insert(
                0,
                {
                    "actionBy": ObjectId(current_user["userUniqueId"]),
                    "actionAt": get_current_date_timestamp(),
                    "status": "L1-Approved",
                    "customisedStatus": 2,
                    'ApprovedAmount':float(Amount),
                },
            )
            # print("fghjklghl;", datatoinsert2)
            datatoAproveris = cmo.insertion("Approval", datatoinsert2)
            
            # print(datatoAproveris["operation_id"])
            return respond(Response)
        
        if id != None:
            data = request.get_json()
            claimType = data["claimType"]
            Amount = data["Amount"]
            projectId = data["projectId"]
            siteandTaskmatchingData = {
                "Amount": Amount,
            }
            print('datadata',data)
            arro=[
                {
                    '$match': {
                        '_id': ObjectId(data['addedFor'])
                    }
                }, {
                    '$project': {
                        'designation': 1, 
                        '_id': 0
                    }
                }
            ]
            currentUserDesignation=current_user["designation"]
            
            Responses=cmo.finding_aggregate("userRegister",arro)['data']
            if len(Responses):
                currentUserDesignation=Responses[0]['designation']
            
            
            arr = [
                {
                    "$match": {
                        "claimTypeId": ObjectId(claimType),
                        "designation": currentUserDesignation,
                    }
                },
                {
                    "$lookup": {
                        "from": "DesignationclaimType",
                        "localField": "DesignationClaimid",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {"$project": {"taskName": 1, "siteId": 1, "_id": 0}},
                        ],
                        "as": "DesignationclaimTypeResult",
                    }
                },
                {
                    "$unwind": {
                        "path": "$DesignationclaimTypeResult",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "siteId": "$DesignationclaimTypeResult.siteId",
                        "taskName": "$DesignationclaimTypeResult.taskName",
                        "claimTypeId": {"$toString": "$claimTypeId"},
                    }
                },
                {
                    "$project": {
                        "DesignationclaimTypeResult": 0,
                        "_id": 0,
                        "DesignationClaimid": 0,
                    }
                },
            ]
            # print("ghklgfdfghjkl;", arr)
            dataTomatch = cmo.finding_aggregate("mergerDesignationClaim", arr)["data"]
            if len(dataTomatch):
                dataTomatch = dataTomatch[0]
            if len(dataTomatch) < 1:
                return respond(
                    {
                        "status": 400,
                        "icon": "error",
                        "msg": f"Claim Type Not Found",
                    }
                )
            # if dataTomatch["name"] != "Daily Allowance":
            #     return respond(
            #         {
            #             "status": 400,
            #             "icon": "error",
            #             "msg": f"You can fill only Daily Allowance",
            #         }
            #     )

            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(claimType)}},
                {"$addFields": {"claimTypeId": {"$toString": "$_id"}}},
                {"$project": {"_id": 0}},
            ]
            shortcodeData = cmo.finding_aggregate("claimType", arr)["data"]
            if dataTomatch["value"] != True:
                # print("Amounggggt", dataTomatch["value"], Amount)
                if Amount != None or Amount != "undefined":
                    if float(Amount) > int(dataTomatch["value"]):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"You can fill DA only equal to or less than {dataTomatch['value']} not for this {Amount}",
                            }
                        )

            if data["EmpCode"] != None:
                arrry = [
                    {"$match": {"_id": ObjectId(data["EmpCode"])}},
                    {"$project": {"_id": 0, "designation": 1}},
                ]
                rtyuuytrt = cmo.finding_aggregate("userRegister", arrry)["data"]
                if len(rtyuuytrt):
                    data["designation"] = rtyuuytrt[0]["designation"]

            datatoinsert = {
                "claimType": ObjectId(claimType),
                "Amount": float(Amount),
                "actionAt": get_current_date_timestamp(),
                "actionBy": ObjectId(current_user["userUniqueId"]),
                "expenseDate": data["Claim_Date"],
                "addedFor": ObjectId(data["EmpCode"]),
                "designation": data["designation"],
                "addedBy": ObjectId(current_user["userUniqueId"]),
                "projectId": ObjectId(projectId),
                "status": "L1-Approved",
                "customStatus": "L1-Approved",
                "customisedStatus": 2,
                "type": "Expense",
                "fillBy": "L1-Approver",
                "remark": data["remark"],
                "action": "Approved",
                "additionalInfo": data["additionalInfo"],
            }
            datatoinsert["L1Approver"] = []
            datatoinsert["L1Approver"].insert(
                0,
                {
                    "actionBy": ObjectId(current_user["userUniqueId"]),
                    "actionAt": get_current_date_timestamp(),
                    "status": "L1-Approved",
                    "customisedStatus": 2,
                    'ApprovedAmount':float(Amount),
                },
            )
            # print("fghgyuio", datatoinsert)
            Response = cmo.updating("Expenses", {"_id": ObjectId(id)}, datatoinsert)

            datatoinsert2 = {
                "action": "Approved",
                "actionAt": get_current_date_timestamp(),
                "actionBy": ObjectId(current_user["userUniqueId"]),
                "addedFor": ObjectId(data["EmpCode"]),
                "status": "L1-Approved",
                "customStatus": "L1-Approved",
                "customisedStatus": 2,
                "remark": data["remark"],
                "type": "Expense",
                "fillBy": "L1-Approver",
                "additionalInfo": data["additionalInfo"],
            }

            datatoinsert2["L1Approver"] = []
            datatoinsert2["L1Approver"].insert(
                0,
                {
                    "actionBy": ObjectId(current_user["userUniqueId"]),
                    "actionAt": get_current_date_timestamp(),
                    "status": "L1-Approved",
                    "customisedStatus": 2,
                    'ApprovedAmount':float(Amount),
                },
            )
            datatoAproveris = cmo.updating("Approval", {"ExpenseUniqueId": ObjectId(id)}, datatoinsert2)
            return respond(Response)

    if request.method == "DELETE":
        if id != None and id != "undefined":
            cmo.deleting_m2("Approval", {"ExpenseUniqueId": ObjectId(id)}, current_user["userUniqueId"])
            Response = cmo.deleting("Expenses", id,current_user["userUniqueId"])
            return respond(Response)



@mobile_blueprint.route("/mobile/DAFillcostCenter", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@mobile_blueprint.route("/mobile/DAFillcostCenter/<id>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def ExpensesDAFillcostCenter(current_user, id=None):
    projectId = request.args.get("projectId")
    # print("projectIDprojectIDprojectIDprojectID", projectId)
    if request.method == "GET":
        if projectId != None:
            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(projectId)}},
                {
                    "$lookup": {
                        "from": "projectGroup",
                        "localField": "projectGroup",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {
                                "$lookup": {
                                    "from": "costCenter",
                                    "localField": "costCenterId",
                                    "foreignField": "_id",
                                    "pipeline": [
                                        {"$match": {"deleteStatus": {"$ne": 1}}},
                                        {"$project": {"_id": 0, "costCenter": 1}},
                                    ],
                                    "as": "costcenterresult",
                                }
                            },
                            {
                                "$addFields": {
                                    "costcenter": "$costcenterresult.costCenter"
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$costcenter",
                                    "preserveNullAndEmptyArrays": True,
                                }
                            },
                        ],
                        "as": "projectGroupResult",
                    }
                },
                {
                    "$unwind": {
                        "path": "$projectGroupResult",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "costcenter": "$projectGroupResult.costcenter",
                        "costcenterId": {
                            "$toString": "$projectGroupResult.costCenterId"
                        },
                    }
                },
                {"$project": {"costcenter": 1, "_id": 0, "costcenterId": 1}},
            ]
            # print("guiuyuioiuyuio", arr)
            Response = cmo.finding_aggregate("project", arr)
            return respond(Response)

        else:
            return jsonify({"msg": "id not found"})

@mobile_blueprint.route("/mobile/DAFillProjectId", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@mobile_blueprint.route("/mobile/DAFillProjectId/<id>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def ExpensesDAFillProjectId(current_user, id=None):
    userId = request.args.get("empCode")
    if request.method == "GET":
        if userId != None:
            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "empId": userId}},
                {
                    "$unwind": {
                        "path": "$projectIds",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$lookup": {
                        "from": "project",
                        "localField": "projectIds",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "projectUniqueId": {"$toString": "$_id"},
                                    "projectId": 1,
                                }
                            },
                        ],
                        "as": "project",
                    }
                },
                {"$addFields": {"length": {"$size": "$project"}}},
                {"$match": {"length": {"$ne": 0}}},
                {"$unwind": {"path": "$project", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "projectId": {"$toString": "$projectIds"},
                        "projectIdName": "$project.projectId",
                    }
                },
            ]
            # print("eruiiuytrertyui", arr)
            Response = cmo.finding_aggregate("projectAllocation", arr)
            return respond(Response)
        else:
            return jsonify({"msg": "id not found"})



@mobile_blueprint.route("/mobile/fillDAEmpName", methods=["GET", "POST"])
@mobile_blueprint.route("/mobile/fillDAEmpName/<id>", methods=["GET", "POST"])
@token_required
def expensesfillDAEmpName(current_user, id=None):
    EmpCode=request.args.get('empCode')
    # print('EmpCodeEmpCode',EmpCode)
    errors=[None,'undefined','']
    if request.method == "GET":
        if EmpCode not in errors:
            arr=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            '_id': ObjectId(EmpCode)
                        }
                    }, {
                        '$addFields': {
                            'uniqueId': {
                                '$toString': '$_id'
                            }, 
                            'userRoleId': {
                                '$toString': '$userRole'
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0, 
                            'email': 1, 
                            'uniqueId': 1, 
                            'empName': 1, 
                            'empCode': 1, 
                            'designation': 1, 
                            'userRoleId': 1
                        }
                    }
                ]
            # print('fghjhgfjkl',arr)
            Response = cmo.finding_aggregate("userRegister", arr)
            return respond(Response)
        else:
            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": f"Employee Code not found",
                }
            )


@mobile_blueprint.route("/mobile/globalSaver", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@mobile_blueprint.route("/mobile/globalSaver/<uniqueId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def mobileglobalSaver(current_user,uniqueId=None):
    if request.method == "PATCH":

        allData = request.get_json()
        
        
        uiid = allData["from"]["uid"]
        useruniqueId = current_user["userUniqueId"]
        name = allData["name"]
        uoData = allData["data"]
        uid = allData["from"]["uid"]
        # plannedStartDate = allData['data']['plannedStartDate']
        if name == "updateSiteEngg":
            if uiid != None:
                evl.siteEngineerLogs(uiid, allData, useruniqueId, "SiteEngineer")
            updatingData = allData["data"]
            uid = allData["from"]["uid"]
            
            if (uid!=None and uid!= "undefined"):
                arra = [
                    {"$match": {"_id": ObjectId(uid)}},
                    {"$project": {"_id": 0, "SubProjectId": 1}},
                ]
                subprojectid = cmo.finding_aggregate("SiteEngineer", arra)['data']
                if len(subprojectid):
                    subprojectid=subprojectid[0]['SubProjectId']
                    arry = [
                        {"$match": {"_id": ObjectId(subprojectid)}},
                        {
                            "$project": {
                                "Commercial": 0,
                                "_id": 0,
                                "t_tracking": 0,
                                "t_sFinancials": 0,
                                "t_issues": 0,
                                "MileStone": 0,
                            }
                        },
                    ]
                    datas=cmo.finding_aggregate("projectType",arry)['data']
                    if len(datas):
                        for i in datas[0]['t_sengg']:
                            if i['fieldName']=="Unique ID" and i['dataType']=="Auto Created":
                                uniqueIdDropDownvalues=i['dropdownValue']
                                keys = uniqueIdDropDownvalues.split(',')
                                extracted_values = [updatingData[key] for key in keys if key in updatingData]
                                extracted_values = [str(element) for element in extracted_values]
                                changedUniqueId = '-'.join(extracted_values)
                                updatingData['Unique ID'] = changedUniqueId
                                arra = [
                                    {
                                        '$match':{
                                            '_id':ObjectId(uid)
                                        }
                                    }
                                ]
                                currentUniqueId = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['Unique ID']
                                if currentUniqueId!=changedUniqueId:
                                    duplicate = cmo.finding("SiteEngineer",{"Unique ID":changedUniqueId})['data']
                                    if duplicate:
                                        duplicateId = duplicate[0]['Unique ID']
                                        return respond({
                                            'status':400,
                                            "msg":f"This Unique Id '{duplicateId}' is already exist in Database",
                                            "icon":"error"
                                        })
                                            
            updateBy = {"_id": ObjectId(uid)}
            if "RFAI Date" in updatingData:
                mileArra = [
                    {"$match": {"siteId": ObjectId(uid)}},
                    {"$sort": {"indexing": 1}},
                ]
                mileList = cmo.finding_aggregate("milestone", mileArra)["data"]
                finaler = ctm.strtodate(updatingData["RFAI Date"])
                finalerStart = finaler
                for milei in mileList:
                    finalerEnd = ctm.add_hour_in_udate(
                        finalerStart, int(milei["estimateDay"])
                    )
                    dataMileList = {
                        "mileStoneStartDate": finalerStart.strftime("%Y-%m-%d")+ "T00:00:00",
                        "mileStoneEndDate": finalerEnd.strftime("%Y-%m-%d")+ "T00:00:00",
                    }

                    upby = {"_id": ObjectId(milei["_id"])}

                    cmo.updating("milestone", upby, dataMileList, False)

                    finalerStart = finalerEnd

                    updatingData["siteStartDate"] = finaler.strftime("%Y-%m-%d") + "T00:00:00"
                    updatingData["siteEndDate"] = finalerEnd.strftime("%Y-%m-%d") + "T00:00:00"
            response = cmo.updating("SiteEngineer", updateBy, updatingData, False)
            return respond(response)

        if name == "bulktask":
            assigningTo = allData['data']['assigningTo']
            dayta = "bulktask"
            finalData = []
            for oneas in uoData["assignerId"].split(","):
                finalData.append(ObjectId(oneas))

            dtwq = []
            response = {}
            for oneas in uid:

                updateBy = {"_id": ObjectId(oneas)}

                resping = cmo.finding("milestone", updateBy)["data"][0]

                if resping["mileStoneStatus"] == "Closed":
                    dtwq.append(resping["Name"])

                else:
                    data = {"assignerId": finalData}
                    response = cmo.updating("milestone", updateBy, data, False)
                    userArra = [{"$match": {"_id": {"$in": finalData}}}]
                    dtewformail = cmo.finding_aggregate("userRegister", userArra)["data"]
                    listingew = []
                    for smail in dtewformail:
                        if assigningTo == "vendor":
                            smail['empName'] = smail['vendorName']
                        listingew.append("Allocate a new task to "+ smail["empName"] + " using task allocation.")

                    if oneas != None:
                        evl.siteEngineerLogsDirect(
                            oneas, listingew, useruniqueId, "milestone"
                        )

                # for smail in dtewformail:
                #     # cmailer.formatted_sendmail([smail["email"].replace("@","#")],["shubham.singh@fourbrick.com","vishal.yadav@fourbrick.com"],"New Task Allocate","Hello User you have allocate new task")
                #     cmailer.formatted_sendmail(["shubham.singh@fourbrick.com"],["vishal.yadav@fourbrick.com"],"New Task Allocate","Hello User you have allocate new task")

            if len(dtwq) == 0:
                return respond(response)
            else:
                return respond(
                    {
                        "status": 200,
                        "icon": "warning",
                        "msg": ", ".join(dtwq)
                        + " not reallocate because its already Closed",
                        "data": [],
                    }
                )
        if name == "bulksite":
            dayta = "bulksite"
            finalData = []
            for oneas in uoData["assignerId"].split(","):
                finalData.append(ObjectId(oneas))

            response = {}

            for oneas in uid:
                updateBy = {"siteId": ObjectId(oneas)}
                data = {"assignerId": finalData}
                # data['assignDate']=current_time()
                response = cmo.updating_m("milestone", updateBy, data, False)

                userArra = [{"$match": {"_id": {"$in": finalData}}}]
                dtewformail = cmo.finding_aggregate("userRegister", userArra)

                listingew = []
                # print("smialbbbbd", dtewformail)
                # for smail in dtewformail:
                #     print('smailsmail',smail)
                #     listingew.append(
                #         "Allocate a new site to "
                #         + smail["empName"]
                #         + " using site allocation."
                #     )
                listingew.append(
                    "Allocate a new site to "
                    + dtewformail["data"][0]["empName"]
                    + " using site allocation."
                )

                # print(oneas, "oneas")

                # print(listingew, "listingew")

                if oneas != None:
                    evl.siteEngineerLogsDirect(
                        oneas, listingew, useruniqueId, "milestone"
                    )

                # for smail in dtewformail:
                #     # cmailer.formatted_sendmail([smail["email"].replace("@","#")],["shubham.singh@fourbrick.com","vishal.yadav@fourbrick.com"],"New Task Allocate","Hello User you have allocate new task")
                #     cmailer.formatted_sendmail(["shubham.singh@fourbrick.com"],["vishal.yadav@fourbrick.com"],"New Task Allocate","Hello User you have allocate new task")

            return respond(response)

        updateBy = {"_id": ObjectId(uid)}

        if name == "mileStone":
            if "assignerId" in uoData:
                # print("uoDatauoData", uoData, "jdjdjjdjd")
                finalData = []
                for oneas in uoData["assignerId"].split(","):
                    finalData.append(ObjectId(oneas))
                data = {"assignerId": finalData}
                evl.siteEngineerLogs(
                    uid, allData, useruniqueId, "milestone", type="Mile"
                )
                # data['assignDate']=current_time()
                # evl.siteEngineerLogs(
                #     uid, allData, useruniqueId, "milestone", type="Mile"
                # )
                response = cmo.updating("milestone", updateBy, data, False)
                # userArra = [{"$match": {"_id": {"$in": finalData}}}]
                # dtewformail = cmo.finding_aggregate("userRegister", userArra)["data"]
                # listingew=[]
                # for smail in dtewformail:
                #     listingew.append("Allocate a new site to "+ smail["empName"]+ " using site allocation.")
                return respond(response)

            if "plannedStartDate" in uoData:

                oldDta = cmo.finding_aggregate(
                    "milestone", {"_id": ObjectId(uniqueId)}
                )["data"][0]
                getdata = {"siteId": oldDta["siteId"]}

                # print(getdata, "getdatagetdatagetdata")

                oldDting = cmo.finding_aggregate("milestone", getdata)["data"]

                for i in oldDting:
                    print(i)
                data = {"assignerId": uoData["assignerId"]}
                # data['assignDate']=current_time()
                response = cmo.updating("milestone", updateBy, data, False)
                return respond(response)    
            
            
@mobile_blueprint.route("/mobile/circlewithPG/<projectuid>", methods=["GET"])
def circle_with_Pg(projectuid=None):
    arra = [
        {
            '$match': {
                '_id': ObjectId(projectuid)
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
                            'as': 'result'
                        }
                    }, {
                        '$addFields': {
                            'circle': {
                                '$arrayElemAt': [
                                    '$result.circle', 0
                                ]
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
                            'as': 'result'
                        }
                    }, {
                        '$unwind': {
                            'path': '$result', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$addFields': {
                            'circle': '$result.circleCode',
                            'band':'$result.band',
                            'uniqueId': {
                                '$toString': '$result._id'
                            }
                        }
                    }, {
                        '$project': {
                            'circle': 1, 
                            'band':1,
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
            '$addFields': {
                'Circle': '$result.circle',
                'BAND':'$result.band',
            }
        }, {
            '$project': {
                'Circle': 1, 
                'BAND':1,
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project", arra)
    return respond(response) 
 
@mobile_blueprint.route("/mobile/admin/complectionCriteria",methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def compectioncriteria(current_user,id=None):
    if request.method == "GET":
        arra = [
            {
                '$addFields':{
                    'uniqueId':{
                        '$toString':'$_id'
                    },
                    '_id':{
                        '$toString':'$_id'
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("complectionCriteria",arra)
        return respond(response)



@mobile_blueprint.route("/mobile/closeMilestone/<id>", methods=["POST"])
@token_required
def mobilecloseMilestone(current_user, id=None):
    
    data = request.form
    closedData = dict(data)
    resp = {}
    tkn = False
    
    
    globalSaverId=None
    if "Checklist" in closedData and closedData['Checklist'] == "Yes" :
        for i in closedData:
            closedData[i] = closedData[i].strip()
            
        if closedData['projectTypeName'] == "DEGROW" and closedData['subProjectTypeName'] in ['TWIN BEAM','LAYER DEGROW','SECTOR DEGROW','4TR-2TR']:
            checkingArray= {
                "TWIN BEAM":{
                    "Survey":["tbAnteena","existingAntenna","radio","bbuCard","miscMaterial","SnapData","subProjectName"],
                    "SRQ Raise":["Template"],
                    "Dismantle":['SnapData'],
                },
                "LAYER DEGROW":{
                    "Survey":["existingAntenna","radio","bbuCard","miscMaterial","SnapData","subProjectName"],
                    "SRQ Raise":["Template"],
                    "Dismantle":['SnapData'],
                },
                "SECTOR DEGROW":{
                    "Survey":["existingAntenna","radio","bbuCard","miscMaterial","SnapData","subProjectName"],
                    "SRQ Raise":["Template"],
                    "Dismantle":['SnapData'],
                },
                "4TR-2TR":{
                    "Survey":["radio","bbuCard","miscMaterial"],
                    "SRQ Raise":["Template"],
                    "Dismantle":['SnapData'],
                },
            }
            
            arra = [
                {
                    '$match':{
                        'siteuid':ObjectId(closedData['siteuid']),
                        "milestoneuid":ObjectId(id),
                    }
                }, {
                    "$addFields":{
                        "_id":{"$toString":"$_id"},
                        'mergedObjects': {
                            'TB Antenna Specifications': '$tbAnteena.TB Antenna Quantity', 
                            'Existing Other Antenna Specifications': '$existingAntenna.Existing Other Antenna Quantity', 
                            'Radio Specifications In Sector': '$radio.Radio Quantity', 
                            'BBU/card Specifications': '$bbuCard.BBU/Card Quantity'
                        }
                    }
                }
            ]
            fetchCASData=cmo.finding_aggregate("complianceApproverSaver",arra)
            if not len(fetchCASData['data']):
                return respond({"status":400,"msg":"Please fill Complete--> 'Forms & Checklist'","icon":"error"}) 
            globalSaverId = fetchCASData['data'][0]['_id']
            notKeyExist= []
            notMatchLength = []
            for key in checkingArray[closedData['subProjectTypeName']][closedData['mName']]:
                if key not in list(fetchCASData['data'][0].keys()):
                    notKeyExist.append(key)
            
            if len(notKeyExist):
                replacefieldName = {
                    "tbAnteena":"TB Antenna Specifications in Sector",
                    "existingAntenna":"Existing Antenna Specifications In Sector",
                    "radio":"Radio Specifications In Sector",
                    "bbuCard":"BBU/card Specifications",
                    "miscMaterial":"Misc Material Specifications",
                    "SnapData":"Snap",
                    "Template":closedData['subProjectTypeName'],
                    "subProjectName":closedData['subProjectTypeName']
                }
                notKeyExist = [replacefieldName[key] if key in replacefieldName else key for key in notKeyExist]
                return respond({"status":400,"msg":f"Please fill Tab -- {','.join(notKeyExist)} ","icon":"error"})
            if "SnapData" in checkingArray[closedData['subProjectTypeName']][closedData['mName']]:
                if closedData['mName'] == "Survey":
                    for key,value in fetchCASData['data'][0]['mergedObjects'].items():
                        if key not in list(fetchCASData['data'][0]['SnapData'].keys()):
                            notKeyExist.append(key)   
                    if len(notKeyExist):
                        return respond({"status":400,"msg":f"Please upload snap on Field Name -- {','.join(notKeyExist)} ","icon":"error"})
                    
                    for key,value in fetchCASData['data'][0]['mergedObjects'].items():
                        expected_length = int(value)
                        actual_length = len(fetchCASData['data'][0]['SnapData'][key]['images'])
                        if actual_length != expected_length:
                            notMatchLength.append(key)
                    if len(notMatchLength):
                        return respond({"status":400,"msg":f"Please upload All snap on Field Name -- {','.join(notMatchLength)} ","icon":"error"})
                
                elif closedData['mName'] == "Dismantle":
                    arra = [
                        {
                            "$match":{
                                "siteuid":ObjectId(closedData['siteuid']),
                                'milestoneName': {
                                    '$in': [
                                        'Survey', 'SRQ Raise'
                                    ]
                                }
                            }
                        }, {
                            '$addFields': {
                                'sortkey': {
                                    '$cond': [
                                        {
                                            '$ne': [
                                                {
                                                    '$type': '$Template'
                                                }, 'missing'
                                            ]
                                        }, 1, 2
                                    ]
                                },
                                'allObj': {
                                    'TB Antenna': '$tbAnteena.TB Antenna Quantity', 
                                    'Existing Other Antenna': '$existingAntenna.Existing Other Antenna Quantity', 
                                    'Existing Antenna': '$existingAntenna.Existing Antenna Quantity', 
                                    'Radio': '$radio.Radio Quantity', 
                                    'BBU/Card': '$bbuCard.BBU/Card Quantity', 
                                    'Jumper': '$miscMaterial.Jumper Quantity', 
                                    'Cipri': '$miscMaterial.Cipri Quantity', 
                                    'Power cable': '$miscMaterial.Power cable Quantity', 
                                    'SFP': '$miscMaterial.SFP Quantity', 
                                    'Dipexure': '$miscMaterial.Dipexure Quantity', 
                                    'others': '$miscMaterial.others Quantity'
                                }
                            }
                        }, {
                            '$sort': {
                                'sortkey': 1
                            }
                        }
                    ]
                    fetchData = cmo.finding_aggregate("complianceApproverSaver",arra)
                    setObj={}
                    if len(fetchData['data'])==2:
                        for key,value in fetchData['data'][0]['Template'].items():
                            if value=="YES" and key in fetchData['data'][1]['allObj'] and fetchData['data'][1]['allObj'][key] != "":
                                setObj[key] = fetchData['data'][1]['allObj'][key]
                        arra = [
                            {
                                '$match':{
                                    'siteuid':ObjectId(closedData['siteuid']),
                                    "milestoneuid":ObjectId(id),
                                }
                            }, {
                                '$replaceRoot': {
                                    'newRoot': '$SnapData'
                                }
                            }
                        ]
                        resData = cmo.finding_aggregate("complianceApproverSaver",arra)['data'][0]
                        notKeyExist= []
                        notMatchLength = []
                        for key,value in setObj.items():
                            if key not in list(resData.keys()):
                                notKeyExist.append(key)   
                        if len(notKeyExist):
                            return respond({"status":400,"msg":f"Please upload snap on Field Name -- {','.join(notKeyExist)} ","icon":"error"})
                    
                        for key,value in setObj.items():
                            expected_length = int(value)
                            actual_length = len(resData[key]['images'])
                            if actual_length != expected_length:
                                notMatchLength.append(key)
                        if len(notMatchLength):
                            return respond({"status":400,"msg":f"Please upload All snap on Field Name -- {','.join(notMatchLength)} ","icon":"error"})
                    else:
                        return respond({
                        'status':400,
                        "icon":"error",
                        "msg":"To close a Dismantle Task, you must first complete and close the Survey Task and the SRQ Raise Task"
                    })
            
        else:
            checkingArray= {
                    "TemplateData": "Template data is not available.",
                    "PlanDetailsData": "Plan details are missing at the moment.",
                    "SiteDetailsData": "Site details are not available right now.",
                    "RanCheckListData": "Ran checklist data is not present.",
                    "SnapData": "Snap data is currently missing.",
                    "AcceptanceLogData": "Acceptance log data is not available."
            }
            arra = [
                {
                    '$match': {
                        '_id': ObjectId(closedData['siteuid'])
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
                            'activity': {
                                '$trim':{
                                    'input':'$ACTIVITY'
                                }
                            }, 
                            'oem': {
                                '$trim':{
                                    'input':'$OEM NAME'
                                }
                            },
                            'mName': closedData['mName']
                        }, 
                        'pipeline': [
                            {
                                '$match':{
                                    'deleteStatus':{'$ne':1}
                                }
                            }, {
                                '$addFields':{
                                    'activity':{
                                        '$trim':{
                                            'input':'$activity'
                                        }
                                    },
                                    'oem':{
                                        '$trim':{
                                            'input':'$oem'
                                        }
                                    },
                                    'complianceMilestone':{
                                        '$trim':{
                                            'input':'$complianceMilestone'
                                        }
                                    },
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
                    '$project': {
                        '_id': 0, 
                        'snap':'$result.snap',
                        "Template":'$result.Template',
                        "planDetails":'$result.planDetails',
                        "siteDetails":'$result.siteDetails',
                        "ranChecklist":'$result.ranChecklist',
                        "acceptanceLog":'$result.acceptanceLog',
                    }
                }
            ]
            response = cmo.finding_aggregate("SiteEngineer", arra)
            if not len(response['data']):
                return respond({"status":400,"msg":"please provide valid id ","icon":"error"})
        
            checkingArrayKey = list(response['data'][0].keys())
            replacement = {
                'snap': 'SnapData',
                'Template': 'TemplateData',
                'planDetails': 'PlanDetailsData',
                'siteDetails': 'SiteDetailsData',
                'ranChecklist': 'RanCheckListData',
                'acceptanceLog': 'AcceptanceLogData',
            }
        
            checkingArrayKey = [replacement[item] if item in replacement else item for item in checkingArrayKey]
        
        
            arra = [
                {
                    '$match':{
                        'siteuid':ObjectId(closedData['siteuid']),
                        "milestoneuid":ObjectId(id),
                    }
                }, {
                    "$addFields":{
                        "_id":{"$toString":"$_id"}
                    }
                }
            ]
            fetchCASData=cmo.finding_aggregate("complianceApproverSaver",arra)
        
            if not len(fetchCASData['data']):
                return respond({"status":400,"msg":"Please fill Complete--> 'Forms & Checklist'","icon":"error"}) 
            globalSaverId = fetchCASData['data'][0]['_id']
            notKeyExist= []
            for key in checkingArrayKey:
                if key not in list(fetchCASData['data'][0].keys()):
                    notKeyExist.append(checkingArray[key])
             
            if len(notKeyExist):
                return respond({"status":400,"msg":f"Please fill This Tab -- {','.join(notKeyExist)} Data ","icon":"error"})
            if "SnapData" in checkingArrayKey:
                for item in response['data'][0]['snap']:
                    if item['fieldName'] not in list(fetchCASData['data'][0]['SnapData'].keys()):
                        notKeyExist.append(item['fieldName'])
                if len(notKeyExist):
                    return respond({"status":400,"msg":f"Please upload at least one snap on fieldName -- {','.join(notKeyExist)} ","icon":"error"})
    
    arra = [
        {
            '$match': {
                '_id': ObjectId(id)
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
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'PprojectType': '$result.projectType'
            }
        }
    ]
    respw = cmo.finding_aggregate("milestone",arra)["data"][0]
    mappedDta = cmo.finding("mappedData",{"projectTypeName": respw["PprojectType"], "milestoneName": respw["Name"]})["data"]
    

    respwin = {}
    itsWork = False

    
    if respw['Name'] == "Revenue Recognition":
        return respond(
                {
                    "status": 400,
                    "msg": f"Revenue Recognition Can't be closed manually!",
                    "icon": "error",
                    "data": [],
                }
            )
                    
                 
    if respw["Name"] == "MS2":
        
        respwin = cmo.finding("milestone",{"siteId": respw["siteId"], "Name": "MS1"})["data"]
        if (len(respwin)):
            respwin = respwin[0]
            completion_date = respwin.get('CC_Completion Date')
            if isinstance(completion_date, dict) and '$date' in completion_date:
                completion_date = completion_date['$date']
            elif isinstance(completion_date, str):
                completion_date = completion_date
            if (respwin["mileStoneStatus"] == "Closed"):
                data = dict(data)
                data["mileStoneEndDate"] = parser.isoparse(data["CC_Completion Date"])
                respwin["CC_Completion Date"] = parser.isoparse(completion_date)
                
                if data["mileStoneEndDate"] < respwin["CC_Completion Date"]:
                    return respond(
                        {
                            "status": 400,
                            "msg": f"MS2 can't be close before MS1 Completion Date...",
                            "icon": "error",
                            "data": [],
                        }
                    )
                else:

                    itsWork = True
            else:
                return respond(
                    {
                        "status": 400,
                        "msg": f"Predecessor ``{respwin['Name']}`` is open",
                        "icon": "error",
                        "data": [],
                    }
                )
        else:
            return respond({
                'status':400,
                "icon":"error",
                "msg":"The MS1 is not found for this site ID, so you can't close MS2."
            })        
        
    if respw["Name"] != "Survey" or respw["Name"] == "SACFA Check":
        if respw["Predecessor"] != "" and respw["Name"] != "MS2":
            respwin = cmo.finding("milestone",{"siteId": respw["siteId"], "Name": respw["Predecessor"]})["data"]
            if respwin:
                respwin = respwin[0]
                if respwin["mileStoneStatus"] == "Closed":
                    itsWork = True
                else:
                    return respond(
                        {
                            "status": 400,
                            "msg": f"Predecessor ``{respwin['Name']}`` is open",
                            "icon": "error",
                            "data": [],
                        }
                    )
            else:
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":f'This Predecessor {respw["Predecessor"]} is not match Any MileStone.'
                })
        else:
            itsWork = True
    if respw["Name"] == "Survey" or respw["Name"] == "SACFA Check":
        itsWork = True
    if itsWork:
        for i in data:
            if i == "CC_Completion Date":
                tkn = True

            resp[i] = data.get(i)
            
            

        if tkn == False:
            resp["CC_Completion Date"] = ctm.updatedatetimeObj()
        

        if respw['Name'] == "MS2":
            if "CC_WCC Signoff Pending Reason" in closedData:
                updateBy = {"_id": respw["siteId"]}
                updateData = {"WCC SIGNOFF PENDING REASON":closedData['CC_WCC Signoff Pending Reason']}
                cmo.updating("SiteEngineer", updateBy, updateData, False)
            
        if "Checklist" not in closedData:
            if len(mappedDta) > 0:

                updationbyS = {"_id": respw["siteId"]}
                updationDataS = {mappedDta[0]["headerName"]: resp["CC_Completion Date"]}
                cmo.updating("SiteEngineer", updationbyS, updationDataS, False)
            arra = [
                {
                    '$match':{
                        'siteId':ObjectId(respw["siteId"]),
                        'mileStoneStatus':"Open"
                    }
                }
            ]
            response = cmo.finding_aggregate("milestone",arra)['data']
            if len(response) == 1:
                updationData = {
                    "Site_Completion Date": resp["CC_Completion Date"],
                    "siteStatus": "Close",
                }
                updationby = {"_id": respw["siteId"]}
                cmo.updating("SiteEngineer", updationby, updationData, False)
                
            resp["mileStoneStatus"] = "Closed"
            resp["Task Closure"] = current_time()
            if "mileStoneEndDate" in resp:
                del resp["mileStoneEndDate"]
            response = cmo.updating("milestone", {"_id": ObjectId(id)}, resp, False)
            evl.newMileStone(id, current_user["userUniqueId"],respw['projectuniqueId'],respw['Name'],str(respw['siteId']),msg="MileStone Closed")
            if "formType" in closedData and closedData['formType'] == "Static":
                setObjForLog={
                    "userId":ObjectId(current_user['userUniqueId']),
                    "globalSaverId":ObjectId(globalSaverId),
                    "type":"",
                    "Event": "This compliance has been closed by the L2 Approver",
                    "currentTime":current_time().replace("T"," ")
                }
                cmo.insertion("complianceLog",setObjForLog)
                cmo.updating("complianceApproverSaver",{"milestoneuid":ObjectId(id)},{"currentStatus":"Closed","closedBy":"L2"},False)
            return respond(response)
        else:
            if closedData['projectTypeName'] == "DEGROW" and closedData['subProjectTypeName'] in ['TWIN BEAM','LAYER DEGROW','SECTOR DEGROW','4TR-2TR']:
                response = cmo.updating("milestone", {"_id": ObjectId(id)}, {'mileStoneStatus':"Closed","Task Closure":current_time()}, False)
                cmo.updating("complianceApproverSaver",{"siteuid":ObjectId(closedData['siteuid']),"milestoneuid":ObjectId(id)},{'currentStatus':"Closed","formSubmitDate":current_time1()},False)
                evl.newMileStone(id, current_user["userUniqueId"],respw['projectuniqueId'],respw['Name'],str(respw['siteId']),msg="MileStone Closed")
                return respond(response)
            else:
                response = cmo.updating("milestone", {"_id": ObjectId(id)}, {'mileStoneStatus':"Submit"}, False)
                cmo.updating("complianceApproverSaver",{"siteuid":ObjectId(closedData['siteuid']),"milestoneuid":ObjectId(id)},{'currentStatus':"Submit","formSubmitDate":current_time1()},False)
                setObjForLog={
                    "userId":ObjectId(current_user['userUniqueId']),
                    "globalSaverId":ObjectId(globalSaverId),
                    "type":"",
                    "Event": "This compliance has been submitted by the user and forwarded to L1.",
                    "currentTime":current_time().replace("T"," ")
                }
                cmo.insertion("complianceLog",setObjForLog)
                evl.newMileStone(id, current_user["userUniqueId"],respw['projectuniqueId'],respw['Name'],str(respw['siteId']),msg="MileStone Submit to L1")
                return respond(response)
    
    
############################ Phase 2 #################################


@mobile_blueprint.route("/mobile/getOneCompliance/<id>/<mName>", methods=["GET"])
def mobile_onecompliance(id=None,mName=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
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
                                'planDetails': 1, 
                                'siteDetails': 1, 
                                'ranChecklist': 1, 
                                'snap': 1, 
                                'acceptanceLog': 1, 
                                'Template': 1, 
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
                    'SubProjectId': 0
                }
            }
        ]
        response = cmo.finding_aggregate("SiteEngineer", arra)
        return respond(response)
    
    
    
@mobile_blueprint.route("/mobile/getOneComplianceL1List/<id>/<mName>", methods=["GET"])
@token_required
def mobile_onecompliancel1list(current_user,id=None,mName=None):
    if request.method == "GET":
        arra = [
            {
                '$match':{
                    '_id':ObjectId(id)
                }
            }, {
                '$addFields': {
                    'projectuniqueId': {
                        '$toObjectId': '$projectuniqueId'
                    },
                    'SubProjectId': {
                        '$toObjectId': '$SubProjectId'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectuniqueId', 
                    'foreignField': '_id', 
                    'as': 'result'
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'localField': 'SubProjectId', 
                    'foreignField': '_id', 
                    'as': 'presult'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$presult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'projectGroup': {
                        '$toString': '$result.projectGroup'
                    },
                    'customer':'$result.custId',
                    'projectType': '$presult.projectType'
                }
            }
        ]
        resData = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]
        
        arra = [
            {
                '$match': {
                    'approverType': 'L1Approver', 
                    'customer': ObjectId(resData['customer']),
                    'projectGroup':ObjectId(resData['projectGroup']),
                    'projectType':resData['projectType'],
                    'complianceMilestone': mName,
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
                                'deleteStatus': {'$ne': 1},
                                'status':"Active"
                            }
                        }, {
                            '$project': {
                                'empName': 1, 
                                'empCode': 1
                            }
                        }, {
                            '$addFields': {
                                'empName': {
                                    '$toString': '$empName'
                                }, 
                                'empCode': {
                                    '$toString': '$empCode'
                                }, 
                                'uniqueId': {
                                    '$toString': '$_id'
                                }
                            }
                        }, {
                            '$addFields': {
                                'empApprover': {
                                    '$concat': [
                                        '$empName', '(', '$empCode', ')'
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'empResult'
                }
            }, {
                '$unwind': {
                    'path': '$empResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'approverName': '$empResult.empApprover', 
                    'approverId': '$empResult.uniqueId'
                }
            }, {
                '$project': {
                    'approverName': 1, 
                    'approverId': 1, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("complianceApprover", arra)
        return respond(response)

    
@mobile_blueprint.route("/mobile/compliance/globalSaver", methods=["GET", "PATCH"])
@mobile_blueprint.route("/mobile/compliance/globalSaver/<siteId>/<milestoneId>", methods=["GET", "PATCH"])
@token_required
def compliance_global_saver(current_user,siteId=None,milestoneId=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'siteuid': ObjectId(siteId), 
                    'milestoneuid': ObjectId(milestoneId)
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L1UserId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$addFields': {
                                'empName': {
                                    '$toString': '$empName'
                                }, 
                                'empCode': {
                                    '$toString': '$empCode'
                                }
                            }
                        }, {
                            '$addFields': {
                                'approverName': {
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
                    'L1UserName': {
                        '$arrayElemAt': [
                            '$result.approverName', 0
                        ]
                    },
                    "L1UserId":{
                        '$toString':"$L1UserId"
                    }
                }
            }, {
                '$project': {
                    'TemplateData': 1, 
                    'PlanDetailsData': 1, 
                    'SiteDetailsData': 1, 
                    'RanCheckListData': 1, 
                    'AcceptanceLogData': 1, 
                    'L1UserId': 1, 
                    "SnapData":1,
                    "currentStatus":1,
                    "L1UserName":1,
                    "subProjectName":1,
                    "tbAnteena":1,
                    "existingAntenna":1,
                    "radio":1,
                    "bbuCard":1,
                    "miscMaterial":1,
                    'Template': 1,
                    '_id': 0
                }
            }
        ]
        res = cmo.finding_aggregate("complianceApproverSaver",arra)
        return respond(res)
    
    if request.method == "PATCH":
        forFile=request.args.get("forFile") == "true"
        allData={}
        imagePath=""

        if forFile:
            allData= request.form.to_dict()
            imagePath=cform.singleFileSaver(request.files.get("image"),"uploads/uploadSnapImage",['png','jpg','jpeg',"PNG"])
            if imagePath['status'] != 200:
                return respond({
                    'status':imagePath['status'],
                    "icon":"error",
                    "msg":imagePath['msg']
                })
            imagePath = imagePath['msg']
            aggr=[
                {
                '$match': {
                    'siteuid':ObjectId(allData['siteuid']),
                    'milestoneuid':ObjectId(allData['milestoneuid'])
                }
            }
            ]
            fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)
            allImage=[]
            isExist=False
            approved=[]
            disApproved = []
            remarks = []

            if len(fetchData['data']):
                
                if "SnapData" in fetchData['data'][0]:
                    
                    if allData['fieldName'] in fetchData['data'][0]['SnapData']:
                        
                        if "approvedIndex" in fetchData['data'][0]['SnapData'][allData['fieldName']]:
                            approved=fetchData['data'][0]['SnapData'][allData['fieldName']]['approvedIndex']

                        if "disApprovedIndex" in fetchData['data'][0]['SnapData'][allData['fieldName']]:
                            disApproved=fetchData['data'][0]['SnapData'][allData['fieldName']]['disApprovedIndex']

                        if "remarks" in fetchData['data'][0]['SnapData'][allData['fieldName']]:
                            remarks=fetchData['data'][0]['SnapData'][allData['fieldName']]['remarks']


                        for item in fetchData['data'][0]['SnapData'][allData['fieldName']]['images']:
                            
                            if item["index"] == allData['index']:
                                if fetchData['data'][0]['SnapData'][allData['fieldName']].get('approvedIndex',[]) and item['index'] in fetchData['data'][0]['SnapData'][allData['fieldName']].get('approvedIndex',[]):
                                        return respond({"status": 400, "msg": f"The Image at Index-{item['index']} can't be updated because it has already been approved.", "icon": "error"})
                                
                                # fullPathOfImage = os.path.join(os.getcwd(),item["image"])
                                # if os.path.exists(fullPathOfImage):
                                #     os.remove(fullPathOfImage)
                                    

                                isExist=True
                                item.update({
                                    "index": allData['index'],
                                    "image": imagePath
                                })
                            allImage.append(item)
                        if isExist == False:
                            allImage.append({
                                        "index": allData['index'],
                                        "image": imagePath
                                        })
                        
                        if allData['index'] in disApproved:
                            disApproved.remove(allData['index'])

                        if remarks:
                            for i, item in enumerate(remarks):
                                if item['index'] == allData['index']:
                                    del remarks[i]
                                    break



                    else:
                        allImage.append({
                                    "index": allData['index'],
                                    "image": imagePath
                                    })
                else:
                    allImage.append({
                                    "index": allData['index'],
                                    "image": imagePath
                                    })
            else:
                allImage.append({
                                    "index": allData['index'],
                                    "image": imagePath
                                    })
            allData[f"SnapData.{allData['fieldName']}"]={"images":allImage,"approvedIndex":approved,"disApprovedIndex":disApproved,"remarks":remarks}
        
            del allData['index']
            del allData['fieldName']
            
        
        else:
            allData = request.get_json()
        if "formType" in allData and allData['formType'] == "Static":
            convertObjectIdKeys=["milestoneuid","siteuid","projectuniqueId","subprojectId","userId"]
        else:
            convertObjectIdKeys=["milestoneuid","siteuid","projectuniqueId","subprojectId","userId","L1UserId"] 
        for key in convertObjectIdKeys:
            if key in allData:
                try:
                    allData[key]=ObjectId(allData[key])
                except :
                    return respond({"status":400,"msg":"Provide Valid Id","icon":"error"})
            elif key not in allData and key !="userId":
                return respond({"status":400,"msg":"Please Fill All Required Fields","icon":"error"})

        updateBy = {
            'siteuid':ObjectId(allData['siteuid']),
            'milestoneuid':ObjectId(allData['milestoneuid'])
        }
        res = cmo.updating("complianceApproverSaver",updateBy,allData,True,unset={"L1ActionDate":1,"L2ActionDate":1,"formSubmitDate":1,"AirtelActionDate":1})
        res['imagePath'] =imagePath
        cmo.updating("milestone",{"_id":ObjectId(allData['milestoneuid'])},{'mileStoneStatus':allData['currentStatus']},False)
        return respond(res)
    