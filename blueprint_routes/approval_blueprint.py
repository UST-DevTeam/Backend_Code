from base import *
from common.config import *
import common.excel_write as excelWriteFunc

approval_blueprint = Blueprint("approval_blueprint", __name__)

def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time

@approval_blueprint.route("/approval/l1Approval", methods=["GET", "POST", "DELETE", "PUT"])
@approval_blueprint.route("/approval/l1Approval/<id>", methods=["GET", "POST", "DELETE", "PUT"])
@token_required
def l1approval(current_user, id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    filterstatus = request.args.get('status')
    empName = request.args.get('empName')
    empCode = request.args.get('empCode')
    
    arpp2=[]
    arpp3=[]
    
    status='Submitted'
    if filterstatus not in ['',None,'undefined']:
        status=filterstatus  
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    costCenter = request.args.get('costCenter')
    arpp4=[]
    if costCenter not in ['',None,'undefined']:
        arpp4=[{
            '$match':{
                "costcenter":{
                    '$regex':costCenter.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        
        
        if ExpenseNo != None and ExpenseNo != "undefined":
            arr = [
                {"$match": {"ExpenseNo": ExpenseNo,'deleteStatus':{'$ne':1}}},
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "L1Approver": current_user["userUniqueId"],
                                }
                            },
                            {
                                "$addFields": {
                                    "designation": {"$toObjectId": "$designation"}
                                }
                            },
                            # {
                            #     "$lookup": {
                            #         "from": "designation",
                            #         "localField": "designation",
                            #         "foreignField": "_id",
                            #         "pipeline": [
                            #             {"$match": {"deleteStatus": {"$ne": 1}}}
                            #         ],
                            #         "as": "designationResult",
                            #     }
                            # },
                            # {
                            #     "$unwind": {
                            #         "path": "$designationResult",
                            #         "preserveNullAndEmptyArrays": True,
                            #     }
                            # },
                            {
                                "$addFields": {
                                    "designation": "$band"
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
                                    "costcenter": "$projectGroupResult.costcenter",
                                    "circle": {"$toObjectId": "$circle"},
                                    'customerName': {
                      '$toObjectId': "$custId"
                    }
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
                            {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName', 
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
                                }
                            }
                            
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
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'taskName', 
                    'foreignField': '_id', 
                    'as': 'TaskResults'
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'Site Id', 
                    'foreignField': '_id', 
                    'as': 'siteReSult'
                }
                 }, {
                '$addFields': {
                    'siteReSult': {
                        '$arrayElemAt': [
                            '$siteReSult', 0
                        ]
                    }, 
                    'TaskResults': {
                        '$arrayElemAt': [
                            '$TaskResults', 0
                        ]
                    }
                }
            },
                {
                    "$addFields": {
                        'Site_Id': '$siteReSult.Site Id', 
                        'Task': '$TaskResults.Name',
                        "empName": "$l1approver.empName",
                        "empCode": "$l1approver.empCode",
                        "ustCode": "$l1approver.ustCode",
                        "designation": "$l1approver.designation",
                        "circle": "$projectResult.circle",
                        "costcenter": "$projectResult.costcenter",
                        "projectIdName": "$projectResult.projectId",
                        "customerName": "$projectResult.customerName",
                        "CreatedAt": {"$toDate": "$CreatedAt"},
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
                        }, 
                        "claimType": "$claimResults.claimType",
                    }
                },
                {
                    "$addFields": {
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%d-%m-%Y",
                                "date": "$expenseDate",
                                'timezone': 'Asia/Kolkata'
                            }
                        },
                        "expensemonth": {
                            "$dateToString": {"format": "%m", "date": "$CreatedAt"}
                        },
                        "projectId": {"$toString": "$projectId"},
                        "uniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                        "actionBy": {"$toString": "$actionBy"},
                        "ExpenseUniqueId": {"$toString": "$ExpenseUniqueId"},
                    }
                },
                {"$sort": {"_id": 1}},
                {
                '$addFields': {
                    'Site Id': {
                        '$toString': '$Site Id'
                    }, 
                    'claimType': {
                        '$toString': '$claimType'
                    }, 
                    'taskName': {
                        '$toString': '$taskName'
                    },
                    'projectId': {
                        '$toString': '$projectId'
                    }
                }
             },
                {
                    "$project": {
                        "_id": 0,
                        "taskName": 0,
                        "Site Id": 0,
                        "CreatedAt": 0,
                        "l1approver": 0,
                        "claimResults": 0,
                        "projectResult": 0,
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        'TaskResults': 0, 
                        'siteReSult': 0
                    }
                },
                
            ]
            # print("89899", arr)
            Response = cmo.finding_aggregate("Expenses", arr)
            return respond(Response)
        else:
            arr=[
                {"$match": {"status": status,'deleteStatus':{'$ne':1}}},
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "L1Approver": current_user["userUniqueId"],
                                }
                            },
                            {
                                "$addFields": {
                                    "designation": {"$toObjectId": "$designation"}
                                }
                            },
                            # {
                            #     "$lookup": {
                            #         "from": "designation",
                            #         "localField": "designation",
                            #         "foreignField": "_id",
                            #         "pipeline": [
                            #             {"$match": {"deleteStatus": {"$ne": 1}}}
                            #         ],
                            #         "as": "designationResult",
                            #     }
                            # },
                            # {
                            #     "$unwind": {
                            #         "path": "$designationResult",
                            #         "preserveNullAndEmptyArrays": True,
                            #     }
                            # },
                            # {
                            #     "$addFields": {
                            #         "designation": "$designationResult.designation"
                            #     }
                            # },
                            {
                                "$addFields": {
                                    "designation": "$band"
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
                                    "costcenter": "$projectGroupResult.costcenter",
                                    "circle": {"$toObjectId": "$circle"},
                                    'customerName': {
                      '$toObjectId': "$custId"
                    }
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
                            {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName', 
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
                                }
                            }
                           
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
                        "ustCode": "$l1approver.ustCode",
                        "designation": "$l1approver.designation",
                        "circle": "$projectResult.circle",
                        "customerName": "$projectResult.customerName",
                        "costcenter": "$projectResult.costcenter",
                        "projectIdName": "$projectResult.projectId",
                        "CreatedAt": {"$toDate": "$CreatedAt"},
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
                            }, 
                        "claimType": "$claimResults.claimType",
                    }
                }]
            # print(type(arr),type(arpp2),'ghjklkjhghjkl')
            arr=arr+arpp2+arpp3+arpp4+[
                {
                    "$addFields": {
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%d-%m-%Y",
                                "date": "$expenseDate",
                                'timezone': 'Asia/Kolkata'
                            }
                        },
                        "expensemonth": {
                            "$dateToString": {"format": "%m", "date": "$CreatedAt"}
                        },
                        "projectId": {"$toString": "$projectId"},
                        "uniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "taskName": 0,
                        "Site Id": 0,
                        "CreatedAt": 0,
                        "l1approver": 0,
                        "claimResults": 0,
                        "projectResult": 0,
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        "L1-Approver": 0,
                        "L2-Approver": 0,
                        "L3-Approver": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {
                    "$group": {
                        "_id": "$ExpenseNo",
                        "data": {"$first": "$$ROOT"},
                        "uniqueCustomisedStatus": {"$addToSet": "$customisedStatus"},
                        "Amount": {"$sum": "$Amount"},
                    }
                },
                {"$addFields": {"data.Amount": "$Amount"}},
                {
                    "$match": {
                        "$expr": {"$eq": [{"$size": "$uniqueCustomisedStatus"}, 1]}
                    }
                },
                {"$replaceRoot": {"newRoot": "$data"}},
                {
                '$addFields': {
                    'Site Id': {
                        '$toString': '$Site Id'
                    }, 
                    'claimType': {
                        '$toString': '$claimType'
                    }, 
                    'taskName': {
                        '$toString': '$taskName'
                    },
                    'projectId': {
                        '$toString': '$projectId'
                    }
                }
            },
                {"$sort": {"ExpenseNo": 1}},
            ]
            Response = cmo.finding_aggregate("Expenses", arr)
            return respond(Response)


@approval_blueprint.route("/approval/l2Approval", methods=["GET", "POST", "DELETE", "PUT"])
@approval_blueprint.route("/approval/l2Approval/<id>", methods=["GET", "POST", "DELETE", "PUT"])
@token_required
def l2approval(current_user, id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    filterstatus = request.args.get('status')
    empName = request.args.get('empName')
    empCode = request.args.get('empCode')
    arpp2=[]
    arpp3=[]
    status="L1-Approved"
    if filterstatus not in ['',None,'undefined']:
        status=filterstatus  
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    costCenter = request.args.get('costCenter')
    arpp4=[]
    if costCenter not in ['',None,'undefined']:
        arpp4=[{
            '$match':{
                "costcenter":{
                    '$regex':costCenter.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        if ExpenseNo != None and ExpenseNo != "undefined":
            # print('ExpenseNoExpenseNoExpenseNoExpenseNo',ExpenseNo)
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        
                        
                        "ExpenseNo": ExpenseNo,
                    }
                },
                {
                    '$lookup': {
                        'from': 'Expenses', 
                        'localField': 'ExpenseUniqueId', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }
                                }
                            }, 
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
                                                }, 
                                                'L2Approver':current_user["userUniqueId"],
                                            }
                                        }, {
                                            '$addFields': {
                                                'designation': {
                                                    '$toObjectId': '$designation'
                                                }
                                            }
                                        }, 
                                        # {
                                        #     '$lookup': {
                                        #         'from': 'designation', 
                                        #         'localField': 'designation', 
                                        #         'foreignField': '_id', 
                                        #         'pipeline': [
                                        #             {
                                        #                 '$match': {
                                        #                     'deleteStatus': {
                                        #                         '$ne': 1
                                        #                     }
                                        #                 }
                                        #             }
                                        #         ], 
                                        #         'as': 'designationResult'
                                        #     }
                                        # }, {
                                        #     '$unwind': {
                                        #         'path': '$designationResult', 
                                        #         'preserveNullAndEmptyArrays': True
                                        #     }
                                        # }, {
                                        #     '$addFields': {
                                        #         'designation': '$designationResult.designation'
                                        #     }
                                        # }
                                        {
                                "$addFields": {
                                    "designation": "$band"
                                }
                            },
                                    ], 
                                    'as': 'l1approver'
                                }
                            }, {
                                '$addFields': {
                                    'length': {
                                        '$size': '$l1approver'
                                    }
                                }
                            }, {
                                '$match': {
                                    'length': {
                                        '$ne': 0
                                    }
                                }
                            }, {
                                '$unwind': {
                                    'path': '$l1approver', 
                                    'preserveNullAndEmptyArrays': True
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
                                        }, 
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
                                                'customerName': {
                                                    '$toObjectId': "$custId"
                                                    },
                                                'circle': {
                                                    '$toObjectId': '$circle'
                                                }
                                            }
                                        }, 
                                        {
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
                                        }, 
                                        {
                                            '$unwind': {
                                                'path': '$circle', 
                                                'preserveNullAndEmptyArrays': True
                                            }
                                        }, 
                                         {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName', 
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
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
                                '$lookup': {
                                    'from': 'claimType', 
                                    'localField': 'claimType', 
                                    'foreignField': '_id', 
                                    'as': 'claimResults'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$claimResults', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$lookup': {
                                    'from': 'milestone', 
                                    'localField': 'taskName', 
                                    'foreignField': '_id', 
                                    'as': 'TaskResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'SiteEngineer', 
                                    'localField': 'Site Id', 
                                    'foreignField': '_id', 
                                    'as': 'siteReSult'
                                }
                            }, {
                                '$addFields': {
                                    'siteReSult': {
                                        '$arrayElemAt': [
                                            '$siteReSult', 0
                                        ]
                                    }, 
                                    'TaskResults': {
                                        '$arrayElemAt': [
                                            '$TaskResults', 0
                                        ]
                                    }
                                }
                            }, {
                                '$addFields': {
                                    'Site_Id': '$siteReSult.Site Id', 
                                    'Task': '$TaskResults.Name', 
                                    'empName': '$l1approver.empName', 
                                    'empCode': '$l1approver.empCode',
                                    'ustCode': '$l1approver.ustCode', 
                                    'designation': '$l1approver.designation', 
                                    'circle': '$projectResult.circle', 
                                    "customerName": "$projectResult.customerName",
                                    'costcenter': '$projectResult.costcenter', 
                                    'projectIdName': '$projectResult.projectId', 
                                    'CreatedAt': {
                                        '$toDate': '$CreatedAt'
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
                            }, 
                                    'claimType': '$claimResults.claimType'
                                }
                            }, {
                                '$addFields': {
                                    'expenseDate': {
                                        '$dateToString': {
                                            'format': '%d-%m-%Y', 
                                            'date': '$expenseDate',
                                            'timezone': 'Asia/Kolkata'
                                        }
                                    }, 
                                    'expensemonth': {
                                        '$dateToString': {
                                            'format': '%m', 
                                            'date': '$CreatedAt'
                                        }
                                    }, 
                                    'projectId': {
                                        '$toString': '$projectId'
                                    }, 
                                    'uniqueId': {
                                        '$toString': '$_id'
                                    }, 
                                    'addedFor': {
                                        '$toString': '$addedFor'
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'taskName': 0, 
                                    'Site Id': 0, 
                                    'CreatedAt': 0, 
                                    'l1approver': 0, 
                                    'claimResults': 0, 
                                    'projectResult': 0, 
                                    'siteReSult': 0, 
                                    'TaskResults': 0
                                }
                            }
                        ], 
                        'as': 'ExpenseDetails'
                    }
                }, {
                    '$addFields': {
                        'length': {
                            '$size': '$ExpenseDetails'
                        }
                    }
                }, {
                    '$match': {
                        'length': {
                            '$ne': 0
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$ExpenseDetails', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'actionAt': {
                            '$toDate': '$actionAt'
                        }, 
                        'Site_Id': '$ExpenseDetails.Site_Id', 
                        'Task': '$ExpenseDetails.Task', 
                        'expenseDate': '$ExpenseDetails.expenseDate', 
                        'expensemonth': '$ExpenseDetails.expensemonth', 
                        'empName': '$ExpenseDetails.empName', 
                        'circle': '$ExpenseDetails.circle', 
                        'approvedAmount': '$ExpenseDetails.approvedAmount', 
                        'costcenter': '$ExpenseDetails.costcenter', 
                        'projectIdName': '$ExpenseDetails.projectIdName', 
                        'empCode': '$ExpenseDetails.empCode', 
                        "ustCode": "$ExpenseDetails.ustCode",
                        'designation': '$ExpenseDetails.designation', 
                        'categories': '$ExpenseDetails.categories', 
                        'billNumber': '$ExpenseDetails.billNumber', 
                        'Amount': '$ExpenseDetails.Amount', 
                        'attachment': '$ExpenseDetails.attachment', 
                        'uniqueId': {
                            '$toString': '$ExpenseUniqueId'
                        }, 
                        'totaldays': '$ExpenseDetails.totaldays', 
                        'checkOutDate': '$ExpenseDetails.checkOutDate', 
                        'checkInDate': '$ExpenseDetails.checkInDate', 
                        'customerName': '$ExpenseDetails.customerName', 
                        'startKm': '$ExpenseDetails.startKm', 
                        'endKm': '$ExpenseDetails.endKm', 
                        'Total_distance': '$ExpenseDetails.Total_distance', 
                        'startLocation': '$ExpenseDetails.startLocation', 
                        'endLocation': '$ExpenseDetails.endLocation', 
                        'claimType': '$ExpenseDetails.claimType',
                        'additionalInfo':'$ExpenseDetails.additionalInfo'
                    }
                }, {
                    '$addFields': {
                        'actionAt': {
                            '$dateToString': {
                                'format': '%d-%m-%Y', 
                                'date': '$actionAt'
                            }
                        }, 
                        'addedFor': {
                            '$toString': '$addedFor'
                        }, 
                        'addedBy': {
                            '$toString': '$addedBy'
                        }
                    }
                }, {
                    '$project': {
                        'ExpenseDetails': 0, 
                        '_id': 0, 
                        'actionBy': 0, 
                        'length': 0, 
                        'ExpenseUniqueId': 0, 
                        'L1Approver': 0, 
                        'L2Approver': 0, 
                        'L3Approver': 0
                    }
                }, {
                    '$addFields': {
                        'Site Id': {
                            '$toString': '$Site Id'
                        }, 
                        'claimType': {
                            '$toString': '$claimType'
                        }, 
                        'taskName': {
                            '$toString': '$taskName'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }
                    }
                }, {
                    '$sort': {
                        'ExpenseNo': 1
                    }
                }
            ]
            
            Response = cmo.finding_aggregate("Approval", arr)
            return respond(Response)

        else:
            
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        "customStatus":status,
                       
                    }
                },
                {
                    "$lookup": {
                        "from": "Expenses",
                        "localField": "ExpenseUniqueId",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {
                                "$lookup": {
                                    "from": "userRegister",
                                    "localField": "addedFor",
                                    "foreignField": "_id",
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "deleteStatus": {"$ne": 1},
                                                "L2Approver": current_user["userUniqueId"],
                                            }
                                        },
                                        {
                                            "$addFields": {
                                                "designation": {
                                                    "$toObjectId": "$designation"
                                                }
                                            }
                                        },
                                        # {
                                        #     "$lookup": {
                                        #         "from": "designation",
                                        #         "localField": "designation",
                                        #         "foreignField": "_id",
                                        #         "pipeline": [
                                        #             {
                                        #                 "$match": {
                                        #                     "deleteStatus": {"$ne": 1}
                                        #                 }
                                        #             }
                                        #         ],
                                        #         "as": "designationResult",
                                        #     }
                                        # },
                                        # {
                                        #     "$unwind": {
                                        #         "path": "$designationResult",
                                        #         "preserveNullAndEmptyArrays": True,
                                        #     }
                                        # },
                                        # {
                                        #     "$addFields": {
                                        #         "designation": "$designationResult.designation"
                                        #     }
                                        # },
                                        {
                                "$addFields": {
                                    "designation": "$band"
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
                                                'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
                                            }
                                        },
                                        {
                                            "$lookup": {
                                                "from": "circle",
                                                "localField": "circle",
                                                "foreignField": "_id",
                                                "pipeline": [
                                                    {
                                                        "$match": {
                                                            "deleteStatus": {"$ne": 1}
                                                        }
                                                    }
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
                                         {
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
                                            }
                                        }
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
                                    "ustCode": "$l1approver.ustCode",
                                    "designation": "$l1approver.designation",
                                    "circle": "$projectResult.circle",
                                    "costcenter": "$projectResult.costcenter",
                                    "projectIdName": "$projectResult.projectId",
                                    "customerName": "$projectResult.customerName",
                                    "CreatedAt": {"$toDate": "$CreatedAt"},
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
                            }, 
                                    "claimType": "$claimResults.claimType",
                                }
                            },
                            {
                                "$addFields": {
                                    "expenseDate": {
                                        "$dateToString": {
                                            "format": "%d-%m-%Y",
                                            "date": "$expenseDate",
                                            'timezone': 'Asia/Kolkata'
                                        }
                                    },
                                    "expensemonth": {
                                        "$dateToString": {
                                            "format": "%m",
                                            "date": "$CreatedAt",
                                        }
                                    },
                                    "projectId": {"$toString": "$projectId"},
                                    "uniqueId": {"$toString": "$_id"},
                                    "addedFor": {"$toString": "$addedFor"},
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "taskName": 0,
                                    "Site Id": 0,
                                    "CreatedAt": 0,
                                    "l1approver": 0,
                                    "claimResults": 0,
                                    "projectResult": 0,
                                }
                            },
                        ],
                        "as": "ExpenseDetails",
                    }
                },
                {"$addFields": {"length": {"$size": "$ExpenseDetails"}}},
                {"$match": {"length": {"$ne": 0}}},
                {
                    "$unwind": {
                        "path": "$ExpenseDetails",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "actionAt": {"$toDate": "$actionAt"},
                        "expenseDate": "$ExpenseDetails.expenseDate",
                        "expensemonth": "$ExpenseDetails.expensemonth",
                        "empName": "$ExpenseDetails.empName",
                        "circle": "$ExpenseDetails.circle",
                        "approvedAmount": "$ExpenseDetails.approvedAmount",
                        "costcenter": "$ExpenseDetails.costcenter",
                        "projectIdName": "$ExpenseDetails.projectIdName",
                        "customerName": "$ExpenseDetails.customerName",
                        "empCode": "$ExpenseDetails.empCode",
                        "ustCode": "$ExpenseDetails.ustCode",
                        "designation": "$ExpenseDetails.designation",
                        "categories": "$ExpenseDetails.categories",
                        "billNumber": "$ExpenseDetails.billNumber",
                        "Amount": "$ExpenseDetails.Amount",
                        "attachment": "$ExpenseDetails.attachment",
                        "uniqueId": {"$toString": "$ExpenseUniqueId"},
                    }
                }]
            arr=arr+arpp2+arpp3+arpp4+[{
                    "$addFields": {
                        "actionAt": {
                            "$dateToString": {"format": "%d-%m-%Y", "date": "$actionAt"}
                        },
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                    }
                },
                {
                    "$project": {
                        "ExpenseDetails": 0,
                        "_id": 0,
                        "actionBy": 0,
                        "length": 0,
                        "ExpenseUniqueId": 0,
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
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
                {
                    '$addFields': {
                        'Site Id': {
                            '$toString': '$Site Id'
                        }, 
                        'claimType': {
                            '$toString': '$claimType'
                        }, 
                        'taskName': {
                            '$toString': '$taskName'
                        },
                        'projectId': {
                            '$toString': '$projectId'
                        }
                        
                    }
                },
                {"$sort": {"ExpenseNo": 1}},
            ]
            
            
            print()
            Response = cmo.finding_aggregate("Approval", arr)
            return respond(Response)


@approval_blueprint.route("/approval/financeApprover", methods=["GET", "POST", "DELETE", "PUT"])
@approval_blueprint.route("/approval/financeApprover/<id>", methods=["GET", "POST", "DELETE", "PUT"])
@token_required
def financeApprover(current_user, id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    filterstatus = request.args.get('status')
    empName = request.args.get('empName')
    empCode = request.args.get('empCode')
    arpp2=[]
    arpp3=[]
    costCenter = request.args.get('costCenter')
    arpp4=[]
    if costCenter not in ['',None,'undefined']:
        arpp4=[{
            '$match':{
                "costcenter":{
                    '$regex':costCenter.strip(),
                    '$options':'i'
                }
            }
        }]
    status="L2-Approved"
    if filterstatus not in ['',None,'undefined']:
        status=filterstatus  
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        # print('empCodeempCodeempCodeempCodesss',empCode)
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        if ExpenseNo != None and ExpenseNo != "undefined":
            # print('ExpenseNoExpenseNoExpenseNoExpenseNo',ExpenseNo)
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        
                        
                        "ExpenseNo": ExpenseNo,
                    }
                },
                {
                    '$lookup': {
                        'from': 'Expenses', 
                        'localField': 'ExpenseUniqueId', 
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
                                    'from': 'userRegister', 
                                    'localField': 'addedFor', 
                                    'foreignField': '_id', 
                                    'pipeline': [
                                        {
                                            '$match': {
                                                'deleteStatus': {
                                                    '$ne': 1
                                                }, 
                                                'financeApprover':current_user["userUniqueId"]
                                            }
                                        }, {
                                            '$addFields': {
                                                'designation': {
                                                    '$toObjectId': '$designation'
                                                }
                                            }
                                        },
                                        # {
                                        #     '$lookup': {
                                        #         'from': 'designation', 
                                        #         'localField': 'designation', 
                                        #         'foreignField': '_id', 
                                        #         'pipeline': [
                                        #             {
                                        #                 '$match': {
                                        #                     'deleteStatus': {
                                        #                         '$ne': 1
                                        #                     }
                                        #                 }
                                        #             }
                                        #         ], 
                                        #         'as': 'designationResult'
                                        #     }
                                        # }, {
                                        #     '$unwind': {
                                        #         'path': '$designationResult', 
                                        #         'preserveNullAndEmptyArrays': True
                                        #     }
                                        # }, {
                                        #     '$addFields': {
                                        #         'designation': '$designationResult.designation'
                                        #     }
                                        # }
                                        {
                                "$addFields": {
                                    "designation": "$band"
                                }
                            },
                                    ], 
                                    'as': 'l1approver'
                                }
                            }, {
                                '$addFields': {
                                    'length': {
                                        '$size': '$l1approver'
                                    }
                                }
                            }, {
                                '$unwind': {
                                    'path': '$l1approver', 
                                    'preserveNullAndEmptyArrays': True
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
                                        },
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
                                        },
                                        {
                                            '$unwind': {
                                                'path': '$projectGroupResult', 
                                                'preserveNullAndEmptyArrays': True
                                            }
                                        }, {
                                            '$addFields': {
                                                'costcenter': '$projectGroupResult.costcenter', 
                                                'circle': {
                                                    '$toObjectId': '$circle'
                                                },
                                                'customerName': {
                                            '$toObjectId': "$custId"
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
                                        },  {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName', 
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
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
                                '$lookup': {
                                    'from': 'claimType', 
                                    'localField': 'claimType', 
                                    'foreignField': '_id', 
                                    'as': 'claimResults'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$claimResults', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$lookup': {
                                    'from': 'milestone', 
                                    'localField': 'taskName', 
                                    'foreignField': '_id', 
                                    'as': 'TaskResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'SiteEngineer', 
                                    'localField': 'Site Id', 
                                    'foreignField': '_id', 
                                    'as': 'siteReSult'
                                }
                            }, {
                                '$addFields': {
                                    'siteReSult': {
                                        '$arrayElemAt': [
                                            '$siteReSult', 0
                                        ]
                                    }, 
                                    'TaskResults': {
                                        '$arrayElemAt': [
                                            '$TaskResults', 0
                                        ]
                                    }
                                }
                            }, {
                                '$addFields': {
                                    'Site_Id': '$siteReSult.Site Id', 
                                    'Task': '$TaskResults.Name', 
                                    'empName': '$l1approver.empName', 
                                    'empCode': '$l1approver.empCode',
                                    'ustCode': '$l1approver.ustCode', 
                                    'designation': '$l1approver.designation', 
                                    'circle': '$projectResult.circle', 
                                    'costcenter': '$projectResult.costcenter', 
                                    "customerName": "$projectResult.customerName",
                                    'projectIdName': '$projectResult.projectId', 
                                    'CreatedAt': {
                                        '$toDate': '$CreatedAt'
                                    }, 
                                    'claimType': '$claimResults.claimType',
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
                            }, 
                                }
                            }, {
                                '$addFields': {
                                    'expenseDate': {
                                        '$dateToString': {
                                            'format': '%d-%m-%Y', 
                                            'date': '$expenseDate',
                                            'timezone': 'Asia/Kolkata'
                                        }
                                    }, 
                                    'expensemonth': {
                                        '$dateToString': {
                                            'format': '%m', 
                                            'date': '$CreatedAt'
                                        }
                                    }, 
                                    'projectId': {
                                        '$toString': '$projectId'
                                    }, 
                                    'uniqueId': {
                                        '$toString': '$_id'
                                    }, 
                                    'addedFor': {
                                        '$toString': '$addedFor'
                                    }
                                }
                            }, {
                                '$project': {
                                    'siteReSult': 0, 
                                    'TaskResults': 0, 
                                    '_id': 0, 
                                    'taskName': 0, 
                                    'Site Id': 0, 
                                    'CreatedAt': 0, 
                                    'l1approver': 0, 
                                    'claimResults': 0, 
                                    'projectResult': 0
                                }
                            }
                        ], 
                        'as': 'ExpenseDetails'
                    }
                }, {
                    '$addFields': {
                        'length': {
                            '$size': '$ExpenseDetails'
                        }
                    }
                }, {
                    '$match': {
                        'length': {
                            '$ne': 0
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$ExpenseDetails', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'Site_Id': '$ExpenseDetails.Site_Id', 
                        'Task': '$ExpenseDetails.Task', 
                        'actionAt': {
                            '$toDate': '$actionAt'
                        }, 
                        'expenseDate': '$ExpenseDetails.expenseDate', 
                        'expensemonth': '$ExpenseDetails.expensemonth', 
                        'empName': '$ExpenseDetails.empName', 
                        'ustCode': '$ExpenseDetails.ustCode',
                        'circle': '$ExpenseDetails.circle', 
                        'approvedAmount': '$ExpenseDetails.approvedAmount', 
                        'costcenter': '$ExpenseDetails.costcenter', 
                        'customerName': '$ExpenseDetails.customerName', 
                        'projectIdName': '$ExpenseDetails.projectIdName', 
                        'empCode': '$ExpenseDetails.empCode', 
                        'designation': '$ExpenseDetails.designation', 
                        'categories': '$ExpenseDetails.categories', 
                        'billNumber': '$ExpenseDetails.billNumber', 
                        'additionalInfo': '$ExpenseDetails.additionalInfo', 
                        'Amount': '$ExpenseDetails.Amount', 
                        'attachment': '$ExpenseDetails.attachment', 
                        'uniqueId': {
                            '$toString': '$ExpenseUniqueId'
                        }, 
                        'totaldays': '$ExpenseDetails.totaldays', 
                        'checkOutDate': '$ExpenseDetails.checkOutDate', 
                        'checkInDate': '$ExpenseDetails.checkInDate', 
                        'startKm': '$ExpenseDetails.startKm', 
                        'endKm': '$ExpenseDetails.endKm', 
                        'Total_distance': '$ExpenseDetails.Total_distance', 
                        'startLocation': '$ExpenseDetails.startLocation', 
                        'endLocation': '$ExpenseDetails.endLocation', 
                        'claimType': '$ExpenseDetails.claimType'
                    }
                }]
            # print('arpp3arpp3',arpp3)         
            arr=arr+arpp2+arpp3+[    {
                    "$addFields": {
                        "actionAt": {
                            "$dateToString": {"format": "%d-%m-%Y", "date": "$actionAt"}
                        },
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                    }
                },
                {
                    "$project": {
                        "ExpenseDetails": 0,
                        "_id": 0,
                        "actionBy": 0,
                        "length": 0,
                        "ExpenseUniqueId": 0,
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                # {
                #     '$group': {
                #         '_id': '$ExpenseNo',
                #         'data': {
                #             '$first': '$$ROOT'
                #         }
                #     }
                # }, {
                #     '$replaceRoot': {
                #         'newRoot': '$data'
                #     }
                # },
                {
        '$addFields': {
            'Site Id': {
                '$toString': '$Site Id'
            }, 
            'claimType': {
                '$toString': '$claimType'
            }, 
            'taskName': {
                '$toString': '$taskName'
            },
            'projectId': {
                '$toString': '$projectId'
            }
        }
    },
                {"$sort": {"ExpenseNo": 1}},
            ]
            # print("aruuuurrrgggr", arr)
            Response = cmo.finding_aggregate("Approval", arr)
            return respond(Response)
        
        
        else:
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        "customStatus": status,
                       
                    }
                },
                {
                    "$lookup": {
                        "from": "Expenses",
                        "localField": "ExpenseUniqueId",
                        "foreignField": "_id",
                        "pipeline": [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {
                                "$lookup": {
                                    "from": "userRegister",
                                    "localField": "addedFor",
                                    "foreignField": "_id",
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "deleteStatus": {"$ne": 1},
                                                "financeApprover": current_user["userUniqueId"],
                                            }
                                        },
                                        {
                                            "$addFields": {
                                                "designation": {
                                                    "$toObjectId": "$designation"
                                                }
                                            }
                                        },
                                        # {
                                        #     "$lookup": {
                                        #         "from": "designation",
                                        #         "localField": "designation",
                                        #         "foreignField": "_id",
                                        #         "pipeline": [
                                        #             {
                                        #                 "$match": {
                                        #                     "deleteStatus": {"$ne": 1}
                                        #                 }
                                        #             }
                                        #         ],
                                        #         "as": "designationResult",
                                        #     }
                                        # },
                                        # {
                                        #     "$unwind": {
                                        #         "path": "$designationResult",
                                        #         "preserveNullAndEmptyArrays": True,
                                        #     }
                                        # },
                                        # {
                                        #     "$addFields": {
                                        #         "designation": "$designationResult.designation"
                                        #     }
                                        # },
                                        {
                                "$addFields": {
                                    "designation": "$band"
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
                                                'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
                                            }
                                        },
                                        
                                        {
                                            "$lookup": {
                                                "from": "circle",
                                                "localField": "circle",
                                                "foreignField": "_id",
                                                "pipeline": [
                                                    {
                                                        "$match": {
                                                            "deleteStatus": {"$ne": 1}
                                                        }
                                                    }
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
                                         {
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
                                            }
                                        }
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
                                    "ustCode": "$l1approver.ustCode",
                                    "designation": "$l1approver.designation",
                                    "circle": "$projectResult.circle",
                                    "costcenter": "$projectResult.costcenter",
                                    "customerName": "$projectResult.customerName",
                                    "projectIdName": "$projectResult.projectId",
                                    "CreatedAt": {"$toDate": "$CreatedAt"},
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
                            }, 
                                    "claimType": "$claimResults.claimType",
                                }
                            },
                            {
                                "$addFields": {
                                    "expenseDate": {
                                        "$dateToString": {
                                            "format": "%d-%m-%Y",
                                            "date": "$expenseDate",
                                            'timezone': 'Asia/Kolkata'
                                        }
                                    },
                                    "expensemonth": {
                                        "$dateToString": {
                                            "format": "%m",
                                            "date": "$CreatedAt",
                                        }
                                    },
                                    "projectId": {"$toString": "$projectId"},
                                    "uniqueId": {"$toString": "$_id"},
                                    "addedFor": {"$toString": "$addedFor"},
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "taskName": 0,
                                    "Site Id": 0,
                                    "CreatedAt": 0,
                                    "l1approver": 0,
                                    "claimResults": 0,
                                    "projectResult": 0,
                                }
                            },
                        ],
                        "as": "ExpenseDetails",
                    }
                },
                {"$addFields": {"length": {"$size": "$ExpenseDetails"}}},
                {"$match": {"length": {"$ne": 0}}},
                {
                    "$unwind": {
                        "path": "$ExpenseDetails",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        "actionAt": {"$toDate": "$actionAt"},
                        "expenseDate": "$ExpenseDetails.expenseDate",
                        "expensemonth": "$ExpenseDetails.expensemonth",
                        "empName": "$ExpenseDetails.empName",
                        "ustCode": "$ExpenseDetails.ustCode",
                        "circle": "$ExpenseDetails.circle",
                        "approvedAmount": "$ExpenseDetails.approvedAmount",
                        "costcenter": "$ExpenseDetails.costcenter",
                        "projectIdName": "$ExpenseDetails.projectIdName",
                        "empCode": "$ExpenseDetails.empCode",
                        "designation": "$ExpenseDetails.designation",
                        "customerName": "$ExpenseDetails.customerName",
                        "categories": "$ExpenseDetails.categories",
                        "billNumber": "$ExpenseDetails.billNumber",
                        "Amount": "$ExpenseDetails.Amount",
                        "attachment": "$ExpenseDetails.attachment",
                        "uniqueId": {"$toString": "$ExpenseUniqueId"},
                    }
                }]
            arr=arr+arpp2+arpp3+arpp4+[
                {
                    "$addFields": {
                        "actionAt": {
                            "$dateToString": {"format": "%d-%m-%Y", "date": "$actionAt"}
                        },
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                    }
                },
             
                {
                    "$project": {
                        "ExpenseDetails": 0,
                        "_id": 0,
                        "actionBy": 0,
                        "length": 0,
                        "ExpenseUniqueId": 0,
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
                {"$sort": {"ExpenseNo": 1}},
                {
                '$addFields': {
                    'Site Id': {
                        '$toString': '$Site Id'
                    }, 
                    'claimType': {
                        '$toString': '$claimType'
                    }, 
                    'taskName': {
                        '$toString': '$taskName'
                    },
                    'projectId': {
                        '$toString': '$projectId'
                    }
                }
                },
                {
                    "$project": {
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                
            ]
            Response = cmo.finding_aggregate("Approval", arr)
            return respond(Response)
 

@approval_blueprint.route("/export/l1Approval",methods=['GET','POSt']) 
@token_required
def l1approvalExport(current_user,id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    filterstatus = request.args.get('status')
    empName = request.args.get('empName')
    empCode = request.args.get('empCode')
    costCenter = request.args.get('costCenter')
    arpp4=[]
    if costCenter not in ['',None,'undefined']:
        arpp4=[{
            '$match':{
                "costcenter":{
                    '$regex':costCenter.strip(),
                    '$options':'i'
                }
            }
        }]
    arpp3=[]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    arpp2=[]
    status=['L1-Approved','L1-Rejected','Submitted']
    arpp1=[{
            '$match':{
                "status":{
                    '$in':status
                }
            }
        }]
    # status=['L1-Approved','L1-Rejected','Submitted','L2-Approved','L2-Rejected','L3-Approved','L3-Rejected']
    
    if filterstatus not in ['',None,'undefined']:
        arpp1=[{
            '$match':{
                "status":filterstatus
            }
        }]   
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        arr=[
                {"$match": {'deleteStatus':{'$ne':1}}},
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "L1Approver": current_user["userUniqueId"],
                                }
                            },
                            {
                                "$addFields": {
                                    "designation": {"$toObjectId": "$designation"}
                                }
                            },
                            # {
                            #     "$lookup": {
                            #         "from": "designation",
                            #         "localField": "designation",
                            #         "foreignField": "_id",
                            #         "pipeline": [
                            #             {"$match": {"deleteStatus": {"$ne": 1}}}
                            #         ],
                            #         "as": "designationResult",
                            #     }
                            # },
                            # {
                            #     "$unwind": {
                            #         "path": "$designationResult",
                            #         "preserveNullAndEmptyArrays": True,
                            #     }
                            # },
                            # {
                            #     "$addFields": {
                            #         "designation": "$designationResult.designation"
                            #     }
                            # },
                            {
                                "$addFields": {
                                    "designation": "$band"
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
                                    "costcenter": "$projectGroupResult.costcenter",
                                    "circle": {"$toObjectId": "$circle"},
                                    'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
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
                             {
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
                                            }
                                        }
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
                        "ustCode": "$l1approver.ustCode",
                        "bankName": "$l1approver.bankName",
                        "ifscCode": "$l1approver.ifscCode",
                        "benificiaryname": "$l1approver.benificiaryname",
                        "accountNumber": "$l1approver.accountNumber",
                        "empCode": "$l1approver.empCode",
                        "designation": "$l1approver.designation",
                        "circle": "$projectResult.circle",
                        "customerName": "$projectResult.customerName",
                        "costcenter": "$projectResult.costcenter",
                        "projectIdName": "$projectResult.projectId",
                        "CreatedAt": {"$toDate": "$CreatedAt"},
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
                            }, 
                        "claimType": "$claimResults.claimType",
                    }
                }]
        
        
        arr=arr+arpp1+arpp2+arpp3+arpp4+[
                {
                    "$addFields": {
                        'accountNumber':{
                          '$toString':"$accountNumber"  
                        },
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%d-%m-%Y",
                                "date": "$expenseDate",
                                'timezone': 'Asia/Kolkata'
                            }
                        },
                        "expensemonth": {
                            "$dateToString": {"format": "%m", "date": "$CreatedAt"}
                        },
                        "projectId": {"$toString": "$projectId"},
                        "uniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "taskName": 0,
                        "Site Id": 0,
                        "CreatedAt": 0,
                        "l1approver": 0,
                        "claimResults": 0,
                        "projectResult": 0,
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        "L1-Approver": 0,
                        "L2-Approver": 0,
                        "L3-Approver": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {
                '$group': {
            '_id': '$ExpenseNo', 
            'data': {
                '$first': '$$ROOT'
            }, 
            'uniqueCustomisedStatus': {
                '$addToSet': '$customisedStatus'
            }, 
            'Amount': {
                '$sum': '$Amount'
            }, 
            'ApprovedAmount': {
                '$sum': '$ApprovedAmount'
            }
        }
    }, {
        '$addFields': {
            'data.Amount': '$Amount', 
            'data.ApprovedAmount': '$ApprovedAmount'
        }
            }, {
                '$match': {
                    '$expr': {
                        '$eq': [
                            {
                                '$size': '$uniqueCustomisedStatus'
                            }, 1
                        ]
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$data'
                }
            }, {
                '$addFields': {
                    'Site Id': {
                        '$toString': '$Site Id'
                    }, 
                    'claimType': {
                        '$toString': '$claimType'
                    }, 
                    'taskName': {
                        '$toString': '$taskName'
                    }, 
                    'projectId': {
                        '$toString': '$projectId'
                    }
                }
            }, {
                '$sort': {
                    'ExpenseNo': 1
                }
            }, {
                '$project': {
                    'Expense ID': '$ExpenseNo', 
                    'Submission Date': '$expenseDate',
                    'Customer': '$customerName',  
                    'Circle': '$circle', 
                    'Cost Center': '$costcenter', 
                    'Project ID': '$projectIdName', 
                    'Employee Name': '$empName', 
                    'Employee Code': '$empCode',
                    'UST Code': '$ustCode',
                    'Bank Name':"$bankName",
                    'Account Number':"$accountNumber",
                    'IFSC Code':"$ifscCode",
                    'Benificiary Name':'$benificiaryname',
                    'Designation': '$designation', 
                    'Category': '$categories', 
                    'Bill Number': '$billNumber', 
                    'Additional Info':"$additionalInfo",
                    'Claimed Amount': '$Amount', 
                    'Current Status': '$status', 
                    'Approved Amount': '$ApprovedAmount'
                }
            }
        ]
        response = cmo.finding_aggregate("Expenses", arr)
        response = response["data"]
        if len(response):
            dataframe = pd.DataFrame(response)
            datefields=['Submission Date']
            for col in datefields:
                dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Expenses", "Expenses And Advance"
            )
            # print("fullPathfullPathfullPath", fullPath)
            return send_file(fullPath)
        else:
            columns = [
                'Expense ID', 'Submission Date', 'Circle', 'Cost Center', 'Project ID',
                'Employee Name', 'Employee Code', 'Bank Name', 'Account Number',
                'IFSC Code', 'Benificiary Name', 'Designation', 'Category',
                'Bill Number', 'Claimed Amount', 'Current Status', 'Approved Amount'
            ]
            df = pd.DataFrame(columns=columns)
            fullPath = excelWriteFunc.excelFileWriter(
                df, "Export_Expenses", "Expenses And Advance"
            )
            return send_file(fullPath)
        
        
        

@approval_blueprint.route("/export/l2Approval",methods=['GET','POSt']) 
@token_required
def l2approvalExport(current_user,id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    filterstatus = request.args.get('status')
    empName = request.args.get('empName')
    empCode = request.args.get('empCode')
    costCenter = request.args.get('costCenter')
    arpp4=[]
    if costCenter not in ['',None,'undefined']:
        arpp4=[{
            '$match':{
                "costcenter":{
                    '$regex':costCenter.strip(),
                    '$options':'i'
                }
            }
        }]
    arpp3=[]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    
    arpp2=[]
    status=['L1-Approved','L2-Rejected','L2-Approved']
    arpp1=[{
            '$match':{
                "status":{
                    '$in':status
                }
            }
        }]
    
    if filterstatus not in ['',None,'undefined']:
        arpp1=[{
            '$match':{
                "status":filterstatus
            }
        }]   
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        arr=[
                {"$match": {'deleteStatus':{'$ne':1}}},
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "L2Approver": current_user["userUniqueId"],
                                }
                            },
                            {
                                "$addFields": {
                                    "designation": {"$toObjectId": "$designation"}
                                }
                            },
                            # {
                            #     "$lookup": {
                            #         "from": "designation",
                            #         "localField": "designation",
                            #         "foreignField": "_id",
                            #         "pipeline": [
                            #             {"$match": {"deleteStatus": {"$ne": 1}}}
                            #         ],
                            #         "as": "designationResult",
                            #     }
                            # },
                            # {
                            #     "$unwind": {
                            #         "path": "$designationResult",
                            #         "preserveNullAndEmptyArrays": True,
                            #     }
                            # },
                            # {
                            #     "$addFields": {
                            #         "designation": "$designationResult.designation"
                            #     }
                            # },
                            {
                                "$addFields": {
                                    "designation": "$band"
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
                                    "costcenter": "$projectGroupResult.costcenter",
                                    "circle": {"$toObjectId": "$circle"},
                                    'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
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
                             {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'circle': '$circle.circleName', 
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
                                }
                            }
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
                        "bankName": "$l1approver.bankName",
                        "ifscCode": "$l1approver.ifscCode",
                        "benificiaryname": "$l1approver.benificiaryname",
                        "accountNumber": "$l1approver.accountNumber",
                        "empCode": "$l1approver.empCode",
                        "ustCode": "$l1approver.ustCode",
                        "designation": "$l1approver.designation",
                        "circle": "$projectResult.circle",
                        "costcenter": "$projectResult.costcenter",
                        "customerName": "$projectResult.customerName",
                        "projectIdName": "$projectResult.projectId",
                        "CreatedAt": {"$toDate": "$CreatedAt"},
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
                            }, 
                        "claimType": "$claimResults.claimType",
                    }
                }]
            # print(type(arr),type(arpp2),'ghjklkjhghjkl')
        arr=arr+arpp1+arpp2+arpp3+arpp4+[
                        {
                            "$addFields": {
                                'accountNumber':{
                          '$toString':"$accountNumber"  
                        },
                                "expenseDate": {
                                    "$dateToString": {
                                        "format": "%d-%m-%Y",
                                        "date": "$expenseDate",
                                        'timezone': 'Asia/Kolkata'
                                    }
                                },
                                "expensemonth": {
                                    "$dateToString": {"format": "%m", "date": "$CreatedAt"}
                                },
                                "projectId": {"$toString": "$projectId"},
                                "uniqueId": {"$toString": "$_id"},
                                "addedFor": {"$toString": "$addedFor"},
                                "addedBy": {"$toString": "$addedBy"},
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "taskName": 0,
                                "Site Id": 0,
                                "CreatedAt": 0,
                                "l1approver": 0,
                                "claimResults": 0,
                                "projectResult": 0,
                                "L1Approver": 0,
                                "L2Approver": 0,
                                "L3Approver": 0,
                                "L1-Approver": 0,
                                "L2-Approver": 0,
                                "L3-Approver": 0,
                                "ExpenseUniqueId": 0,
                                "actionBy": 0,
                            }
                        },
                        {
                '$group': {
                    '_id': '$ExpenseNo', 
                    'data': {
                        '$first': '$$ROOT'
                    }, 
                    'uniqueCustomisedStatus': {
                        '$addToSet': '$customisedStatus'
                    }, 
                    'Amount': {
                        '$sum': '$Amount'
                    }, 
                    'ApprovedAmount': {
                        '$sum': '$ApprovedAmount'
                    }
                }
            }, {
                '$addFields': {
                    'data.Amount': '$Amount', 
                    'data.ApprovedAmount': '$ApprovedAmount'
                }
            }, {
                '$match': {
                    '$expr': {
                        '$eq': [
                            {
                                '$size': '$uniqueCustomisedStatus'
                            }, 1
                        ]
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$data'
                }
            }, {
                '$addFields': {
                    'Site Id': {
                        '$toString': '$Site Id'
                    }, 
                    'claimType': {
                        '$toString': '$claimType'
                    }, 
                    'taskName': {
                        '$toString': '$taskName'
                    }, 
                    'projectId': {
                        '$toString': '$projectId'
                    }
                }
            }, {
                '$sort': {
                    'ExpenseNo': 1
                }
            }, 
            {
        '$sort': {
            'actionAt': -1
        }
    },
            {
                '$project': {
                    'Expense ID': '$ExpenseNo', 
                    'Submission Date': '$expenseDate',
                    'Customer': '$customerName',  
                    'Circle': '$circle', 
                    'Cost Center': '$costcenter', 
                    'Project ID': '$projectIdName', 
                    'Employee Name': '$empName', 
                    'Employee Code': '$empCode', 
                    'UST Code': '$ustCode', 
                    'Bank Name':"$bankName",
                    'Account Number':"$accountNumber",
                    'IFSC Code':"$ifscCode",
                    'Benificiary Name':'$benificiaryname',
                    'Designation': '$designation', 
                    'Category': '$categories', 
                    'Bill Number': '$billNumber', 
                    'Additional Info':'$additionalInfo',
                    'Claimed Amount': '$Amount', 
                    'Current Status': '$status', 
                    'Approved Amount': '$ApprovedAmount'
                }
            }
        ]
        response = cmo.finding_aggregate("Expenses", arr)
        response = response["data"]
        if len(response):
            dataframe = pd.DataFrame(response)
            datefields=['Submission Date']
            for col in datefields:
                dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Expenses", "Expenses And Advance"
            )
            # print("fullPathfullPathfullPath", fullPath)
            return send_file(fullPath)
        else:
            columns = [
                'Expense ID', 'Submission Date','Customer', 'Circle', 'Cost Center', 'Project ID',
                'Employee Name', 'Employee Code', 'Bank Name', 'Account Number',
                'IFSC Code', 'Benificiary Name', 'Designation', 'Category',
                'Bill Number', 'Claimed Amount', 'Current Status', 'Approved Amount'
            ]
            df = pd.DataFrame(columns=columns)
            fullPath = excelWriteFunc.excelFileWriter(
                df, "Export_Expenses", "Expenses And Advance"
            )
            return send_file(fullPath)
        
        
@approval_blueprint.route("/export/l3Approval",methods=['GET','POSt']) 
@token_required
def l3approvalExport(current_user,id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    filterstatus = request.args.get('status')
    empName = request.args.get('empName')
    empCode = request.args.get('empCode')
    arpp2=[]
    arpp3=[]
    status=['L2-Approved','L3-Rejected','L3-Approved']
    costCenter = request.args.get('costCenter')
    arpp4=[]
    if costCenter not in ['',None,'undefined']:
        arpp4=[{
            '$match':{
                "costcenter":{
                    '$regex':costCenter.strip(),
                    '$options':'i'
                }
            }
        }]
    arpp1=[{
            '$match':{
                "status":{
                    '$in':status
                }
            }
        }]
    if filterstatus not in ['',None,'undefined']:
        arpp1=[{
            '$match':{
                "status":filterstatus
            }
        }]   
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        arr=[
                {"$match": {'deleteStatus':{'$ne':1}}},
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "financeApprover": current_user["userUniqueId"],
                                }
                            },
                            {
                                "$addFields": {
                                    "designation": {"$toObjectId": "$designation"}
                                }
                            },
                            # {
                            #     "$lookup": {
                            #         "from": "designation",
                            #         "localField": "designation",
                            #         "foreignField": "_id",
                            #         "pipeline": [
                            #             {"$match": {"deleteStatus": {"$ne": 1}}}
                            #         ],
                            #         "as": "designationResult",
                            #     }
                            # },
                            # {
                            #     "$unwind": {
                            #         "path": "$designationResult",
                            #         "preserveNullAndEmptyArrays": True,
                            #     }
                            # },
                            # {
                            #     "$addFields": {
                            #         "designation": "$designationResult.designation"
                            #     }
                            # },
                            {
                                "$addFields": {
                                    "designation": "$band"
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
                                    "costcenter": "$projectGroupResult.costcenter",
                                    "circle": {"$toObjectId": "$circle"},
                                    'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
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
                             {
                            '$lookup': {
                                'from': 'customer', 
                                'localField': 'customerName', 
                                'foreignField': '_id', 
                                'as': 'customerName'
                            }
                        }, {
                            '$addFields': {
                                'circle': '$circle.circleName', 
                                'customerName': {
                                    '$arrayElemAt': [
                                        '$customerName.customerName', 0
                                    ]
                                }
                            }
                        }
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
                        "ustCode": "$l1approver.ustCode",
                        "bankName": "$l1approver.bankName",
                        "ifscCode": "$l1approver.ifscCode",
                        "benificiaryname": "$l1approver.benificiaryname",
                        "accountNumber": "$l1approver.accountNumber",
                        "designation": "$l1approver.designation",
                        "circle": "$projectResult.circle",
                        "costcenter": "$projectResult.costcenter",
                        "customerName": "$projectResult.customerName",
                        "projectIdName": "$projectResult.projectId",
                        "CreatedAt": {"$toDate": "$CreatedAt"},
                        "claimType": "$claimResults.claimType",
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
                            }, 
                    }
                }]
            # print(type(arr),type(arpp2),'ghjklkjhghjkl')
        arr=arr+arpp1+arpp2+arpp3+arpp4+[
                {
                    "$addFields": {
                        'accountNumber':{
                          '$toString':"$accountNumber"  
                        },
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%d-%m-%Y",
                                "date": "$expenseDate",
                                'timezone': 'Asia/Kolkata'
                            }
                        },
                        "expensemonth": {
                            "$dateToString": {"format": "%m", "date": "$CreatedAt"}
                        },
                        "projectId": {"$toString": "$projectId"},
                        "uniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "taskName": 0,
                        "Site Id": 0,
                        "CreatedAt": 0,
                        "l1approver": 0,
                        "claimResults": 0,
                        "projectResult": 0,
                        "L1Approver": 0,
                        "L2Approver": 0,
                        "L3Approver": 0,
                        "L1-Approver": 0,
                        "L2-Approver": 0,
                        "L3-Approver": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {
        '$group': {
            '_id': '$ExpenseNo', 
            'data': {
                '$first': '$$ROOT'
            }, 
            'uniqueCustomisedStatus': {
                '$addToSet': '$customisedStatus'
            }, 
            'Amount': {
                '$sum': '$Amount'
            }, 
            'ApprovedAmount': {
                '$sum': '$ApprovedAmount'
            }
        }
    }, {
        '$addFields': {
            'data.Amount': '$Amount', 
            'data.ApprovedAmount': '$ApprovedAmount'
        }
    }, {
        '$match': {
            '$expr': {
                '$eq': [
                    {
                        '$size': '$uniqueCustomisedStatus'
                    }, 1
                ]
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$data'
        }
    }, {
        '$addFields': {
            'Site Id': {
                '$toString': '$Site Id'
            }, 
            'claimType': {
                '$toString': '$claimType'
            }, 
            'taskName': {
                '$toString': '$taskName'
            }, 
            'projectId': {
                '$toString': '$projectId'
            }
        }
    }, {
        '$sort': {
            'ExpenseNo': 1
        }
    }, {
        '$project': {
            'Expense ID': '$ExpenseNo', 
            'Submission Date': '$expenseDate', 
            'Customer': '$customerName', 
            'Circle': '$circle', 
            'Cost Center': '$costcenter', 
            'Project ID': '$projectIdName', 
            'Employee Name': '$empName', 
            'Employee Code': '$empCode',
            'UST Code': '$ustCode',
            'Bank Name':"$bankName",
            'Account Number':"$accountNumber",
            'IFSC Code':"$ifscCode",
            'Benificiary Name':'$benificiaryname', 
            'Designation': '$designation', 
            'Category': '$categories', 
            'Bill Number': '$billNumber', 
            'Additional Info':'$additionalInfo',
            'Claimed Amount': '$Amount', 
            'Current Status': '$status', 
            'Approved Amount': '$ApprovedAmount'
        }
    }
        ]
        response = cmo.finding_aggregate("Expenses", arr)
        response = response["data"]
        if len(response):
            dataframe = pd.DataFrame(response)
            datefields=['Submission Date']
            for col in datefields:
                dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Expenses", "Expenses And Advance"
            )
            # print("fullPathfullPathfullPath", fullPath)
            return send_file(fullPath)
        else:
            columns = [
                'Expense ID', 'Submission Date', 'Circle', 'Cost Center', 'Project ID',
                'Employee Name', 'Employee Code', 'Bank Name', 'Account Number',
                'IFSC Code', 'Benificiary Name', 'Designation', 'Category',
                'Bill Number', 'Claimed Amount', 'Current Status', 'Approved Amount'
            ]
            df = pd.DataFrame(columns=columns)
            fullPath = excelWriteFunc.excelFileWriter(
                df, "Export_Expenses", "Expenses And Advance"
            )
            return send_file(fullPath)
        
           
def exp_approval_mail(data,current_user):
    approver = data['approver']
    type = data['type']
    for i in data['data']:
        arra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'ExpenseNo': i['ExpenseNo']
                }
            }
        ]
        response = cmo.finding_aggregate("Expenses",arra)['data']
        if len (response):
            userId = response[0]['addedFor']
            totalAmount = response[0]['Amount']
            Id = i['ExpenseNo']
            if data['customisedStatus'] == 4:
                approveAmount = totalAmount
            else:
                approveAmount = 0
            user = cmo.finding("userRegister",{'_id':ObjectId(userId)})['data']
            if len(user):
                user = user[0]
                userMail = user['email'].strip()
                userName = user['empName']
                userCode = user['empCode']
                # approvermail = cmo.finding("userRegister",{'_id':ObjectId(current_user['userUniqueId'])})['data'][0]['email'].strip()
                cmailer.formatted_sendmail(to=[userMail],cc=[],subject=Id+" "+approver,message=cmt.ClaimType_mail(userName,userCode,Id,totalAmount,type,approver,approveAmount),type="user") 
           
def adv_approval_mail(data,current_user):
    approver = data['approver']
    type = data['type']
    
    for i in data['data']:
        totalAmount = i['Amount']
        approveAmount = i['ApprovedAmount']
        Id = i['AdvanceNo']
        user = cmo.finding("userRegister",{'_id':ObjectId(i['addedFor'])})['data'][0]
        userMail = user['email'].strip()
        userName = user['empName']
        userCode = user['empCode']
        approvermail = cmo.finding("userRegister",{'_id':ObjectId(current_user['userUniqueId'])})['data'][0]['email'].strip()
        cmailer.formatted_sendmail(to=[userMail],cc=[],subject=Id+" "+approver,message=cmt.ClaimType_mail(userName,userCode,Id,totalAmount,type,approver,approveAmount)) 
        
@approval_blueprint.route("/approval/status", methods=["GET", "POST"])
@approval_blueprint.route("/approval/status/<id>", methods=["GET", "POST"])
@token_required
def approvalStatus(current_user, id=None):
    if request.method == "POST":
        data = request.get_json()
        data["customStatus"] = data["approver"]
        if data["approver"] != None:
            if data["approver"] == "L1-Approved":
                data["customisedStatus"] = 2
            elif data["approver"] == "L1-Rejected":
                data["customisedStatus"] = 3
            elif data["approver"] == "L2-Approved":
                data["customisedStatus"] = 4
            elif data["approver"] == "L2-Rejected":
                data["customisedStatus"] = 5
            elif data["approver"] == "L3-Approved":
                data["customisedStatus"] = 6
            elif data["approver"] == "L3-Rejected":
                data["customisedStatus"] = 7    

        #code added by Giriraj   
        if data["customisedStatus"] == 4:
            data['customStatus'] = "Approved"
            data['status'] = "Approved"
            data['approver'] = "Approved"
 
        if data["customisedStatus"] == 5:
            data['customStatus'] = "Rejected"
            data['status'] = "Rejected"
            data['approver'] = "Rejected"

        if data["type"] != "Advance":
            for i in data["data"]:
                if 'ApprovedAmount' in i:
                    if (i["ApprovedAmount"] == None or i["ApprovedAmount"] == "undefined" or i["ApprovedAmount"] == 0):
                        if data["customisedStatus"] not in [3,5,7]:
                            return respond(
                                    {
                                        "status": 400,
                                        "msg": f"You can approve more than zero Amount",
                                        "icon": "error",
                                    }
                                )
                    
                if i["ApprovedAmount"] != None:
                    if float(i["ApprovedAmount"]) < 1 and data["customisedStatus"] not in [3,5,7]:
                        return respond(
                            {
                                "status": 400,
                                "msg": f"You can  approve amount less than or equal to {i['Amount']}  and greater than 0",
                                "icon": "error",
                            }
                        )
                if float(i["ApprovedAmount"]) > float(i["Amount"]) or float(i["Amount"]) < 1: 
                    
                    return respond(
                        {
                            "status": 400,
                            "msg": f"You can  approve amount less than or equal to {i['Amount']} not for {i['ApprovedAmount']}",
                            "icon": "error",
                        }
                    )
            
            
            for i in data["data"]:
                
                newData = {
                    "ExpenseNo": data["expenseId"],
                    "ExpenseUniqueId": ObjectId(i["_id"]),
                    "actionBy": ObjectId(current_user["userUniqueId"]),
                    "action": data["status"],
                    "actionAt": get_current_date_timestamp(),
                    "remark": i["remark"],
                    "customisedStatus": data["customisedStatus"],
                    "customStatus": data["customStatus"],
                    "status": data["customStatus"],
                    "type": "Expense",
                    "addedFor": ObjectId(data["addedFor"]),
                    "ApprovedAmount": i["ApprovedAmount"],
                    "Amount": i["Amount"],
                    
                }
                data["status"] = data["customStatus"]
                if data["customisedStatus"] in [2, 3]:
                    newData["L1Approver"] = []
                    newData["L1Approver"].insert(
                        0,
                        {
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "actionAt": get_current_date_timestamp(),
                            "status": data["approver"],
                            "customisedStatus": data["customisedStatus"],
                            "ip":get_client_ip(),
                            'ApprovedAmount':newData['ApprovedAmount']
                        },
                    )

                if data["customisedStatus"] in [4, 5]:
                    newData["L2Approver"] = []
                    newData["L2Approver"].insert(
                        0,
                        {
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "actionAt": get_current_date_timestamp(),
                            "status": data["approver"],
                            "customisedStatus": data["customisedStatus"],
                            "ip":get_client_ip(),
                            'ApprovedAmount':newData['ApprovedAmount']
                        },
                    )

                if data["customisedStatus"] in [6, 7]:
                    newData["L3Approver"] = []
                    newData["L3Approver"].insert(
                        0,
                        {
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "actionAt": get_current_date_timestamp(),
                            "status": data["approver"],
                            "customisedStatus": data["customisedStatus"],
                            "ip":get_client_ip(),
                            'ApprovedAmount':newData['ApprovedAmount']
                        },
                    )
                cmo.insertion("ApprovalLogs",{
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "actionAt": current_time(),
                            "status": data["approver"],
                            "ExpenseId": newData['ExpenseUniqueId'],
                            'ApprovedAmount':newData['ApprovedAmount'],
                            'Amount':newData['Amount'],
                            'type':'Expense',
                            'ExpenseNo':newData['ExpenseNo'],
                            'userId':newData['addedFor']
                        })
                cmo.updating("Approval", {"ExpenseUniqueId": ObjectId(i["_id"])}, newData, True)
                lookData = {"_id": ObjectId(i["_id"])}
                if 'addedFor' in newData:
                    del newData['addedFor'] 
                
                Response = cmo.updating("Expenses", lookData, newData, False)

            mailAggregation = [
                {
                    '$match': {
                        '_id': ObjectId(data['addedFor'])
                    }
                }, {
                    '$project': {
                        'empName': 1, 
                        'ustCode': {
                            '$toString': '$ustCode'
                        }, 
                        'email': 1, 
                        'L1Approver': {
                            '$toObjectId': '$L1Approver'
                        }, 
                        'L2Approver': {
                            '$toObjectId': '$L2Approver'
                        }, 
                        '_id': 0
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'L1Approver', 
                        'foreignField': '_id', 
                        'as': 'l1result'
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'L2Approver', 
                        'foreignField': '_id', 
                        'as': 'l2result'
                    }
                }, {
                    '$addFields': {
                        'l2Name': {
                            '$arrayElemAt': [
                                '$l2result.empName', 0
                            ]
                        }, 
                        'l2Email': {
                            '$arrayElemAt': [
                                '$l2result.email', 0
                            ]
                        }, 
                        'l2Code': {
                            '$arrayElemAt': [
                                '$l2result.ustCode', 0
                            ]
                        }, 
                        'l1Name': {
                            '$arrayElemAt': [
                                '$l1result.empName', 0
                            ]
                        }, 
                        'l1Email': {
                            '$arrayElemAt': [
                                '$l1result.email', 0
                            ]
                        }, 
                        'l1Code': {
                            '$arrayElemAt': [
                                '$l1result.ustCode', 0
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'L1Approver': 0, 
                        'L2Approver': 0, 
                        'l1result': 0, 
                        'l2result': 0
                    }
                }
            ] 
            user = cmo.finding_aggregate("userRegister",mailAggregation)['data']  
            try:
                user = user[0]
                approver = data['approver']
                type = data['type']
                Id = data['expenseId']
                totalAmount = sum(item['Amount'] for item in data['data'])
                approveAmount = sum(item['ApprovedAmount'] for item in data['data'])
                userMail = user['email'].strip()
                userName = user['empName']
                userCode = user['ustCode']
                l2ApproverName = user['l2Name']
                l2ApproverEmail = user['l2Email'].strip()
                l2ApproverCode = user['l2Code']
                l1ApproverName = user['l1Name']
                l1ApproverEmail = user['l1Email'].strip()
                l1ApproverCode = user['l1Code']
                if data['customisedStatus'] == 2:
                    cmailer.formatted_sendmail(to=[l2ApproverEmail],cc=[],subject=Id+" "+approver,message=cmt.approver_mail(l2ApproverName,userName,userCode,Id,totalAmount,type,approver,approveAmount,l1ApproverName,l1ApproverCode),type="L1")
                elif data['customisedStatus'] in [3,4,5]:
                    cmailer.formatted_sendmail(to=[userMail],cc=[],subject=Id+" "+approver,message=cmt.ClaimType_mail(userName,userCode,Id,totalAmount,type,approver,approveAmount),type="user")
            except Exception as e:
                pass
            return respond(Response)

        if data["type"] == "Advance":
            
            if len(data["data"]) < 1:
                return respond(
                    {
                        "status": 400,
                        "msg": f"Please select At least One",
                        "icon": "error",
                    }
                )

            if len(data["data"]) > 0:
                
                for i in data["data"]:
                    if "Amount" not in i:
                        i["Amount"] = 0
                    
                    
                    if 'ApprovedAmount' in i:
                        if float(i["ApprovedAmount"]) > float(i["Amount"]):
                            return respond(
                                {
                                    "status": 400,
                                    "msg": f"You can approve amount less than or equal to {i['Amount']}",
                                    "icon": "error",
                                }
                            )
                    if 'ApprovedAmount' in i:
                       
                        if i['ApprovedAmount'] not in ['',None,'undefined']:
                            # if float(i["ApprovedAmount"] < 1 and data['approver'] not in  ["L1-Rejected", "L2-Rejected", "L3-Rejected"]):
                            if float(i["ApprovedAmount"] < 1 and data['approver'] not in  ["L1-Rejected","Rejected"]):
                                return respond(
                                {
                                    "status": 400,
                                    "msg": f"You can not approve amount less than 1",
                                    "icon": "error",
                                }
                            )
                    
                        
                    
                        
                    
                for i in data["data"]:
                    if "AdvanceNo" in i:
                        if i["ApprovedAmount"] is None:
                            # checking = ["L1-Rejected", "L2-Rejected", "L3-Rejected"]
                            checking = ["L1-Rejected", "Rejected"]
                            if data["approver"] not in checking:
                                if i["Amount"] is not None:
                                    i["ApprovedAmount"] = float(i["Amount"])
                            else:
                                i["ApprovedAmount"] = 0
                        newData = {
                            "AdvanceNo": i["AdvanceNo"],
                            "AdvanceUniqueId": ObjectId(i["_id"]),
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "action": data["status"],
                            "actionAt": get_current_date_timestamp(),
                            "remark": i["remark"],
                            "customisedStatus": data["customisedStatus"],
                            "customStatus": data["customStatus"],
                            "status": data["customStatus"],
                            "type": "Advance",
                            "addedFor": ObjectId(i["addedFor"]),
                            "ApprovedAmount": i["ApprovedAmount"],
                            "Amount": i["Amount"],
                        }

                        if data["customisedStatus"] in [2, 3]:
                            newData["L1Approver"] = [
                                {
                                    "actionBy": ObjectId(current_user["userUniqueId"]),
                                    "actionAt": get_current_date_timestamp(),
                                    "status": data["approver"],
                                    "customisedStatus": data["customisedStatus"],
                                    "ip":get_client_ip()
                                }
                            ]

                        if data["customisedStatus"] in [4, 5]:
                            newData["L2Approver"] = [
                                {
                                    "actionBy": ObjectId(current_user["userUniqueId"]),
                                    "actionAt": get_current_date_timestamp(),
                                    "status": data["approver"],
                                    "customisedStatus": data["customisedStatus"],
                                    "ip":get_client_ip()
                                }
                            ]

                        if data["customisedStatus"] in [6, 7]:
                            newData["L3Approver"] = [
                                {
                                    "actionBy": ObjectId(current_user["userUniqueId"]),
                                    "actionAt": get_current_date_timestamp(),
                                    "status": data["approver"],
                                    "customisedStatus": data["customisedStatus"],
                                    "ip":get_client_ip()
                                }
                            ]
                        data["status"] = data["customStatus"]
                        cmo.insertion("ApprovalLogs",{
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "actionAt": current_time(),
                            "status":newData['customStatus'],
                            "AdvanceId": newData['AdvanceUniqueId'],
                            'ApprovedAmount':newData['ApprovedAmount'],
                            'Amount':newData['Amount'],
                            'type':'Advance',
                            'AdvanceNo':newData['AdvanceNo'],
                            'userId':newData['addedFor']
                        })
                        cmo.updating("Approval",{"AdvanceUniqueId": ObjectId(i["_id"])},newData,True)
                        lookData = {"_id": ObjectId(i["_id"])}
                        if 'addedFor' in newData:
                            del newData['addedFor'] 
                        Response = cmo.updating("Advance", lookData, newData, False)
                    else:
                        return respond(
                            {
                                "status": 400,
                                "msg": f"Advance No not found",
                                "icon": "error",
                            }
                        )
                # if data['customisedStatus'] in [3,5,6,7]:
                #     thread = Thread(target=adv_approval_mail, args=(data,current_user))
                #     thread.start()
                return respond(Response)


@approval_blueprint.route("/approval/statusBulk", methods=["GET", "POST"])
@approval_blueprint.route("/approval/statusBulk/<id>", methods=["GET", "POST"])
@token_required
def approvalStatusBulk(current_user, id=None):
    if request.method == "POST":
        data = request.get_json()
        data["customStatus"] = data["approver"]
        if data["approver"] != None:
            if data["approver"] == "L1-Approved":
                data["customisedStatus"] = 2
            elif data["approver"] == "L1-Rejected":
                data["customisedStatus"] = 3
            elif data["approver"] == "L2-Approved":
                data["customisedStatus"] = 4
            elif data["approver"] == "L2-Rejected":
                data["customisedStatus"] = 5
            elif data["approver"] == "L3-Approved":
                data["customisedStatus"] = 6
            elif data["approver"] == "L3-Rejected":
                data["customisedStatus"] = 7  
            
            #code added By Giriraj   
        if data["customisedStatus"] == 4:
            data['customStatus'] = "Approved"
            data['status'] = "Approved"
            data['approver'] = "Approved"
 
        if data["customisedStatus"] == 5:
            data['customStatus'] = "Rejected"
            data['status'] = "Rejected"
            data['approver'] = "Rejected"
            
        if data["type"] != "Advance":
            if data["approver"] in ['L1-Approved'] :
                return respond(
                            {
                                "status": 400,
                                "msg": f"You can approve by clicking Expense Number",
                                "icon": "error",
                            }
                        )
            
            for i in data["data"]:
                if len(data["data"]) < 1:
                    return respond(
                        {
                            "status": 400,
                            "msg": f"Please Select Atleast One",
                            "icon": "error",
                        }
                    )
            for i in data["data"]:
                checkingArray=['','undefined',None]
                if i["ExpenseNo"] not in checkingArray:
                    arr =[
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'ExpenseNo':i["ExpenseNo"]
                            }
                        }, {
                            '$lookup': {
                                'from': 'userRegister', 
                                'localField': 'addedFor', 
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
                                'expenseId': {
                                    '$toString': '$_id'
                                }, 
                                'addedFor': {
                                    '$toString': '$addedFor'
                                }, 
                                'l1Approver': '$approverResults.L1Approver', 
                                'l2Approver': '$approverResults.L2Approver', 
                                'l3Approver': '$approverResults.financeApprover'
                            }
                        }, {
                            '$match': {
                                '$or': [
                                    {
                                        'l1Approver':current_user['userUniqueId']
                                    }, {
                                        'l2Approver':current_user['userUniqueId']
                                    }, {
                                        'l3Approver':current_user['userUniqueId']
                                    }
                                ]
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'expenseId': 1, 
                                'addedFor': 1, 
                                'Amount': 1
                            }
                        }]
                    ResponseData = cmo.finding_aggregate("Expenses", arr)["data"]
                    if len(ResponseData):
                        for j in ResponseData:
                            newData = {
                                "ExpenseNo": i["ExpenseNo"],
                                "ExpenseUniqueId": ObjectId(j["expenseId"]),
                                "actionBy": ObjectId(current_user["userUniqueId"]),
                                "action": data["status"],
                                "actionAt": get_current_date_timestamp(),
                                "remark": "",
                                "customisedStatus": data["customisedStatus"],
                                "customStatus": data["customStatus"],
                                "status": data["customStatus"],
                                "type": "Expense",
                                "addedFor": ObjectId(j["addedFor"]),
                                "Amount": j["Amount"],   
                            }
                            data["status"] = data["customStatus"]
                            if data["customisedStatus"] in [2, 3]:                               
                                if data["customisedStatus"] in [2]:
                                    newData['ApprovedAmount'] = j['Amount']
                                else:
                                    newData['ApprovedAmount']=None
                                newData["L1Approver"] = []
                                newData["L1Approver"].insert(
                                    0,
                                    {
                                        "actionBy": ObjectId(
                                            current_user["userUniqueId"]
                                        ),
                                        "actionAt": get_current_date_timestamp(),
                                        "status": data["approver"],
                                        "customisedStatus": data["customisedStatus"],
                                        "ip":get_client_ip(),
                                        'ApprovedAmount':newData['ApprovedAmount']
                                    },
                                )
                            if data["customisedStatus"] in [4, 5]:
                                newData["L2Approver"] = []
                                newData["L2Approver"].insert(
                                    0,
                                    {
                                        "actionBy": ObjectId(
                                            current_user["userUniqueId"]
                                        ),
                                        "actionAt": get_current_date_timestamp(),
                                        "status": data["approver"],
                                        "customisedStatus": data["customisedStatus"],
                                        "ip":get_client_ip(),
                                        'ApprovedAmount':None,
                                        'ApprovedType':'Directly'
                                    },
                                )
                            if data["customisedStatus"] in [6, 7]:
                                newData["L3Approver"] = []
                                newData["L3Approver"].insert(
                                    0,
                                    {
                                        "actionBy": ObjectId(
                                            current_user["userUniqueId"]
                                        ),
                                        "actionAt": get_current_date_timestamp(),
                                        "status": data["approver"],
                                        "customisedStatus": data["customisedStatus"],
                                        "ip":get_client_ip(),
                                        'ApprovedType':'Directly',
                                        'ApprovedAmount':None,
                                    },
                                )
                            cmo.updating("Approval",{"ExpenseUniqueId": ObjectId(j["expenseId"])},newData,True)
                            lookData = {"_id": ObjectId(j["expenseId"])}
                            if 'addedFor' in newData:
                                del newData['addedFor'] 
                                
                                
                                
                            #####logs Logic  
                            logsApprovedAmount=None
                            if  'ApprovedAmount' in newData:
                                logsApprovedAmount=newData['ApprovedAmount']
                                print('logsApprovedAmountlogsApprovedAmount1',logsApprovedAmount)
                            else:
                                artt=[
                                        {
                                            '$match': {
                                                '_id':newData['ExpenseUniqueId']
                                            }
                                        }, {
                                            '$project': {
                                                'ApprovedAmount': 1, 
                                                '_id': 0
                                            }
                                        }
                                    ]
                                print('arttartt',artt)
                                Responser=cmo.finding_aggregate("Expenses",artt)['data'] 
                                if len(Responser):
                                    if 'ApprovedAmount' in Responser[0]:
                                        logsApprovedAmount=Responser[0]['ApprovedAmount']
                                        print('logsApprovedAmountlogsApprovedAmount2',logsApprovedAmount)
                            print('logsApprovedAmountlogsApprovedAmount99',logsApprovedAmount)  
                                      
                            cmo.insertion("ApprovalLogs",{
                            "actionBy": ObjectId(current_user["userUniqueId"]),
                            "actionAt": current_time(),
                            "status":newData['status'],
                            "ExpenseId": newData['ExpenseUniqueId'],
                            'ApprovedAmount':logsApprovedAmount,
                            'Amount':newData['Amount'],
                            'type':'Expense',
                            'ApprovedType':'Directly',
                            'ExpenseNo':newData['ExpenseNo'],
                            'userId': ObjectId(j["addedFor"])
                                })
                            Response = cmo.updating("Expenses", lookData, newData, False)  
                            
                                        
                                                                
                                
                    if len(ResponseData) < 1 :
                        return respond(
                            {
                                "status": 400,
                                "msg": f"Expense with this no {i['ExpenseNo']} not found",
                                "icon": "error",
                            }
                        )
                if i["ExpenseNo"] in checkingArray: 
                    return respond(
                        {
                            "status": 400,
                            "msg": f"Expense No not found",
                            "icon": "error",
                        }
                    )
            if data['customisedStatus'] in [4,5]:
                thread = Thread(target=exp_approval_mail, args=(data,current_user))
                thread.start()
            return respond(Response)
        
        

        # if data["type"] == "Advance":
        #     if len(data["data"]) < 1:
        #         return respond(
        #             {
        #                 "status": 400,
        #                 "msg": f"Please select At least One",
        #                 "icon": "error",
        #             }
        #         )

        #     if len(data["data"]) > 0:
        #         for i in data["data"]:
        #             if "Amount" not in i:
        #                 i["Amount"] = 0
        #             if int(i["ApprovedAmount"]) > int(i["Amount"]):
        #                 return respond(
        #                     {
        #                         "status": 400,
        #                         "msg": f"You can approve amount less than or equal to {i['Amount']}",
        #                         "icon": "error",
        #                     }
        #                 )
        #         for i in data["data"]:
        #             if "AdvanceNo" in i:
        #                 if i["ApprovedAmount"] is None or i["ApprovedAmount"] == 0:
        #                     checking = ["L1-Rejected", "L2-Rejected", "L3-Rejected"]
        #                     if data["approver"] not in checking:
        #                         if i["Amount"] is not None:
        #                             i["ApprovedAmount"] = int(i["Amount"])
        #                     else:
        #                         i["ApprovedAmount"] = 0
        #                 newData = {
        #                     "AdvanceNo": i["AdvanceNo"],
        #                     "AdvanceUniqueId": ObjectId(i["_id"]),
        #                     "actionBy": ObjectId(current_user["userUniqueId"]),
        #                     "action": data["status"],
        #                     "actionAt": get_current_date_timestamp(),
        #                     "remark": i["remark"],
        #                     "customisedStatus": data["customisedStatus"],
        #                     "customStatus": data["customStatus"],
        #                     "status": data["customStatus"],
        #                     "type": "Advance",
        #                     "addedFor": ObjectId(i["addedFor"]),
        #                     "ApprovedAmount": i["ApprovedAmount"],
        #                     "Amount": i["Amount"],
        #                 }

        #                 if data["customisedStatus"] in [2, 3]:
        #                     newData["L1Approver"] = [
        #                         {
        #                             "actionBy": ObjectId(current_user["userUniqueId"]),
        #                             "actionAt": get_current_date_timestamp(),
        #                             "status": data["approver"],
        #                             "customisedStatus": data["customisedStatus"],
        #                         }
        #                     ]

        #                 if data["customisedStatus"] in [4, 5]:
        #                     newData["L2Approver"] = [
        #                         {
        #                             "actionBy": ObjectId(current_user["userUniqueId"]),
        #                             "actionAt": get_current_date_timestamp(),
        #                             "status": data["approver"],
        #                             "customisedStatus": data["customisedStatus"],
        #                         }
        #                     ]

        #                 if data["customisedStatus"] in [6, 7]:
        #                     newData["L3Approver"] = [
        #                         {
        #                             "actionBy": ObjectId(current_user["userUniqueId"]),
        #                             "actionAt": get_current_date_timestamp(),
        #                             "status": data["approver"],
        #                             "customisedStatus": data["customisedStatus"],
        #                         }
        #                     ]
        #                 data["status"] = data["customStatus"]
        #                 cmo.updating(
        #                     "Approval",
        #                     {"AdvanceUniqueId": ObjectId(i["_id"])},
        #                     newData,
        #                     True,
        #                 )
        #                 lookData = {"_id": ObjectId(i["_id"])}
        #                 Response = cmo.updating("Advance", lookData, newData, False)
        #             else:
        #                 return respond(
        #                     {
        #                         "status": 400,
        #                         "msg": f"Advance No not found",
        #                         "icon": "error",
        #                     }
        #                 )

        # return respond(Response)


#####################################Advance############################
@approval_blueprint.route("/Advance/approval/l1Approval", methods=["GET", "POST", "PATCH", "DELETE"])
@approval_blueprint.route("/Advance/approval/l1Approval/<id>", methods=["GET", "POST", "PATCH", "DELETE"])
@token_required
def AdvanceApprovalL1(current_user, id=None):
    filterstatus=request.args.get('status')
    empName=request.args.get('empName')
    empCode=request.args.get('empCode')
    arpp2=[]
    arpp3=[]
    status='Submitted'
    if filterstatus not in ['',None,'undefined']:
        status=filterstatus  
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
    if request.method == "GET":
        
        arr=[
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "status": status,
                }
            },
            {
                "$lookup": {
                    "from": "userRegister",
                    "localField": "addedFor",
                    "foreignField": "_id",
                    "pipeline": [
                        {
                            "$match": {
                                "deleteStatus": {"$ne": 1},
                                "L1Approver": current_user["userUniqueId"],
                            }
                        }
                    ],
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
            # {
            #     "$lookup": {
            #         "from": "designation",
            #         "localField": "designation",
            #         "foreignField": "_id",
            #         "as": "designationResult",
            #     }
            # },
            # {
            #     "$unwind": {
            #         "path": "$designationResult",
            #         "preserveNullAndEmptyArrays": True,
            #     }
            # },
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
                                "costcenter": "$projectGroupResult.costcenter",
                                "circle": {"$toObjectId": "$circle"},
                                'customerName': {
                                    '$toObjectId': "$custId"
                                    }
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
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
                                            }
                                        }
                    ],
                    "as": "projectResult",
                }
            },
            {"$unwind": {"path": "$projectResult", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "CreatedAt": {"$toDate": "$CreatedAt"},
                    "projectId": {"$toString": "$projectId"},
                    "circle": "$projectResult.circle",
                    "costcenter": "$projectResult.costcenter",
                    "customerName": "$projectResult.customerName",
                    "empName": "$approverDetails.empName",
                    "empCode": "$approverDetails.empCode",
                    "ustCode": "$approverDetails.ustCode",
                    "designation":"$approverDetails.band",
                }
            }]
        
        arr=arr+arpp2+arpp3+[{
                "$addFields": {
                    "projectIdName": "$projectResult.projectId",
                    "costcenter": "$projectResult.costcenter",
                    "customerName": "$projectResult.customerName",
                    "uniqueId": {"$toString": "$_id"},
                    "CreatedAt": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$CreatedAt"}
                    },
                    "addedFor": {"$toString": "$addedFor"},
                    "advanceTypeId": {"$toString": "$advanceTypeId"},
                     'projectId': {
                '$toString': '$projectId'
            }
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
                    "L1Approver": 0,
                    "L2Approver": 0,
                    "L3Approver": 0,
                    "AdvanceUniqueId": 0,
                    "actionBy": 0,
                }
            },
        ]
        # print('arrarrarr',arr)
        Response = cmo.finding_aggregate("Advance", arr)
        return respond(Response)


@approval_blueprint.route("/Advance/approval/l2Approval", methods=["GET", "POST", "DELETE", "PUT"])
@approval_blueprint.route("/Advance/approval/l2Approval/<id>", methods=["GET", "POST", "DELETE", "PUT"])
@token_required
def advancel2approval(current_user, id=None):
    if request.method == "GET":
        filterstatus=request.args.get('status')
        empName=request.args.get('empName')
        empCode=request.args.get('empCode')
        # print('empNameempNameempName',empName)
        arpp2=[]
        arpp3=[]
        status='L1-Approved'
        if filterstatus not in ['',None,'undefined']:
            status=filterstatus  
        if empName not in ['',None,'undefined']:
            arpp2=[{
                '$match':{
                    "empName":{
                        '$regex':empName.strip(),
                        '$options':'i'
                    }
                }
            }]
        if empCode not in ['',None,'undefined']:
            arpp3=[{
                '$match':{
                    "empCode":{
                        '$regex':empCode.strip(),
                        '$options':'i'
                    }
                }
            }]
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "customStatus": status,
                    "type": "Advance",
                }
            },
            {
                "$lookup": {
                    "from": "Advance",
                    "localField": "AdvanceUniqueId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$lookup": {
                                "from": "userRegister",
                                "localField": "addedFor",
                                "foreignField": "_id",
                                "pipeline": [
                                    {
                                        "$match": {
                                            "deleteStatus": {"$ne": 1},
                                            "L2Approver": current_user["userUniqueId"],
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "designation": {
                                                "$toObjectId": "$designation"
                                            }
                                        }
                                    },
                                    # {
                                    #     "$lookup": {
                                    #         "from": "designation",
                                    #         "localField": "designation",
                                    #         "foreignField": "_id",
                                    #         "pipeline": [
                                    #             {"$match": {"deleteStatus": {"$ne": 1}}}
                                    #         ],
                                    #         "as": "designationResult",
                                    #     }
                                    # },
                                    # {
                                    #     "$unwind": {
                                    #         "path": "$designationResult",
                                    #         "preserveNullAndEmptyArrays": True,
                                    #     }
                                    # },
                                    {
                                "$addFields": {
                                    "designation": "$band"
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
                                            'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
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
                                     {
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
                                            }
                                        }
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
                                "ustCode": "$l1approver.ustCode",
                                "designation": "$l1approver.designation",
                                "circle": "$projectResult.circle",
                                "customerName": "$projectResult.customerName",
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
                                        "format": "%m",
                                        "date": "$CreatedAt",
                                    }
                                },
                                "projectId": {"$toString": "$projectId"},
                                "uniqueId": {"$toString": "$_id"},
                                "addedFor": {"$toString": "$addedFor"},
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "taskName": 0,
                                "Site Id": 0,
                                "CreatedAt": 0,
                                "l1approver": 0,
                                "claimResults": 0,
                                "projectResult": 0,
                            }
                        },
                    ],
                    "as": "AdvanceDetails",
                }
            },
            {"$addFields": {"length": {"$size": "$AdvanceDetails"}}},
            {"$match": {"length": {"$ne": 0}}},
            {
                "$unwind": {
                    "path": "$AdvanceDetails",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$addFields": {
                    "actionAt": {"$toDate": "$actionAt"},
                    "advanceDate": "$AdvanceDetails.expenseDate",
                    "empName": "$AdvanceDetails.empName",
                    "ustCode": "$AdvanceDetails.ustCode",
                    "circle": "$AdvanceDetails.circle",
                    "approvedAmount": "$AdvanceDetails.approvedAmount",
                    "costcenter": "$AdvanceDetails.costcenter",
                    "projectIdName": "$AdvanceDetails.projectIdName",
                    "customerName": "$AdvanceDetails.customerName",
                    "empCode": "$AdvanceDetails.empCode",
                    "designation": "$AdvanceDetails.designation",
                    "categories": "$AdvanceDetails.categories",
                    "billNumber": "$AdvanceDetails.billNumber",
                    "Amount": "$AdvanceDetails.Amount",
                    "attachment": "$AdvanceDetails.attachment",
                    "uniqueId": {"$toString": "$AdvanceUniqueId"},
                    "additionalInfo":"$AdvanceDetails.additionalInfo"
                }
            }]
        arr = arr+arpp2+arpp3+[{
                "$addFields": {
                    "actionAt": {
                        "$dateToString": {"format": "%d-%m-%Y", "date": "$actionAt"}
                    },
                    "expensemonth": {
                        "$dateToString": {"format": "%m", "date": "$actionAt"},
                    },
                    "addedFor": {"$toString": "$addedFor"},
                    "advanceTypeId": {"$toString": "$advanceTypeId"},
                     'projectId': {
                '$toString': '$projectId'
            }
                }
            },
                               {
                '$sort': {
                    'actionAt': -1
                }
            },
            {
                "$project": {
                    "AdvanceDetails": 0,
                    "_id": 0,
                    "actionBy": 0,
                    "length": 0,
                    "AdvanceUniqueId": 0,
                    "L1Approver": 0,
                    "L2Approver": 0,
                    "L3Approver": 0,
                }
            },
        ]
        
        Response = cmo.finding_aggregate("Approval", arr)
        return respond(Response)


@approval_blueprint.route("/Advance/approval/l3Approval", methods=["GET", "POST", "DELETE", "PUT"])
@approval_blueprint.route("/Advance/approval/l3Approval/<id>", methods=["GET", "POST", "DELETE", "PUT"])
@token_required
def advancel3approval(current_user, id=None):
    if request.method == "GET":
        filterstatus=request.args.get('status')
        empName=request.args.get('empName')
        empCode=request.args.get('empCode')
        # print('empNameempNameempName',empName)
        arpp2=[]
        arpp3=[]
        status='L2-Approved'
        if filterstatus not in ['',None,'undefined']:
            status=filterstatus  
        if empName not in ['',None,'undefined']:
            arpp2=[{
                '$match':{
                    "empName":{
                        '$regex':empName.strip(),
                        '$options':'i'
                    }
                }
            }]
        if empCode not in ['',None,'undefined']:
            arpp2=[{
                '$match':{
                    "empCode":{
                        '$regex':empCode.strip(),
                        '$options':'i'
                    }
                }
            }]
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "customStatus":status,
                    # "customisedStatus": 4,
                    "type": "Advance",
                }
            },
            {
                "$lookup": {
                    "from": "Advance",
                    "localField": "AdvanceUniqueId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$lookup": {
                                "from": "userRegister",
                                "localField": "addedFor",
                                "foreignField": "_id",
                                "pipeline": [
                                    {
                                        "$match": {
                                            "deleteStatus": {"$ne": 1},
                                            "financeApprover": current_user["userUniqueId"],
                                        }
                                    },
                                    {
                                        "$addFields": {
                                            "designation": {
                                                "$toObjectId": "$designation"
                                            }
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
                                    "designation": "$band"
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
                                            'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
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
                                    {
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
                                            }
                                        }
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
                                "ustCode": "$l1approver.ustCode",
                                "designation": "$l1approver.designation",
                                "circle": "$projectResult.circle",
                                "costcenter": "$projectResult.costcenter",
                                "customerName": "$projectResult.customerName",
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
                                        "format": "%m",
                                        "date": "$CreatedAt",
                                    }
                                },
                                "projectId": {"$toString": "$projectId"},
                                "uniqueId": {"$toString": "$_id"},
                                "addedFor": {"$toString": "$addedFor"},
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "taskName": 0,
                                "Site Id": 0,
                                "CreatedAt": 0,
                                "l1approver": 0,
                                "claimResults": 0,
                                "projectResult": 0,
                            }
                        },
                    ],
                    "as": "AdvanceDetails",
                }
            },
            {"$addFields": {"length": {"$size": "$AdvanceDetails"}}},
            {"$match": {"length": {"$ne": 0}}},
            {
                "$unwind": {
                    "path": "$AdvanceDetails",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$addFields": {
                    "actionAt": {"$toDate": "$actionAt"},
                    "advanceDate": "$AdvanceDetails.expenseDate",
                    "empName": "$AdvanceDetails.empName",
                    "circle": "$AdvanceDetails.circle",
                    "approvedAmount": "$AdvanceDetails.approvedAmount",
                    "costcenter": "$AdvanceDetails.costcenter",
                    "projectIdName": "$AdvanceDetails.projectIdName",
                    "customerName": "$AdvanceDetails.customerName",
                    "empCode": "$AdvanceDetails.empCode",
                    "ustCode": "$AdvanceDetails.ustCode",
                    "designation": "$AdvanceDetails.designation",
                    "categories": "$AdvanceDetails.categories",
                    "billNumber": "$AdvanceDetails.billNumber",
                    "Amount": "$AdvanceDetails.Amount",
                    "attachment": "$AdvanceDetails.attachment",
                    "additionalInfo": "$AdvanceDetails.additionalInfo",
                    "uniqueId": {"$toString": "$AdvanceUniqueId"},
                }
            }]
        
        arr=arr+arpp2+arpp3+[{
                "$addFields": {
                    "actionAt": {
                        "$dateToString": {"format": "%d-%m-%Y", "date": "$actionAt"}
                    },
                    "expensemonth": {
                        "$dateToString": {"format": "%m", "date": "$actionAt"}
                    },
                    "addedFor": {"$toString": "$addedFor"},
                    "advanceTypeId": {"$toString": "$advanceTypeId"},
                     'projectId': {
                '$toString': '$projectId'
            }
                }
            },
            {
                "$project": {
                    "AdvanceDetails": 0,
                    "_id": 0,
                    "actionBy": 0,
                    "length": 0,
                    "AdvanceUniqueId": 0,
                    "L1Approver": 0,
                    "L2Approver": 0,
                    "L3Approver": 0,
                }
            },
        ]
        Response = cmo.finding_aggregate("Approval", arr)
        return respond(Response)


@approval_blueprint.route("/export/Advance/l1Approval",methods=['GET','POST'])
@token_required
def advanceexportL1approval(current_user):
    filterstatus=request.args.get('status')
    empName=request.args.get('empName')
    empCode=request.args.get('empCode')
    arpp2=[]
    status=['L1-Approved','L1-Rejected','Submitted']
    arpp1=[{
            '$match':{
                "status":{
                    '$in':status
                }
            }
        }]
    arpp3=[]
    if filterstatus not in ['',None,'undefined']:
        arpp1=[{
            '$match':{
                "status":filterstatus
            }
        }]
        
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
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
                        'localField': 'addedFor', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'L1Approver':current_user["userUniqueId"]
                                }
                            }
                        ], 
                        'as': 'approverDetails'
                    }
                }, {
                    '$addFields': {
                        'length': {
                            '$size': '$approverDetails'
                        }
                    }
                }, {
                    '$match': {
                        'length': {
                            '$ne': 0
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$approverDetails', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'designation', 
                        'localField': 'designation', 
                        'foreignField': '_id', 
                        'as': 'designationResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$designationResult', 
                        'preserveNullAndEmptyArrays': True
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
                            },
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
                            },
                            {
                                '$unwind': {
                                    'path': '$projectGroupResult', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'costcenter': '$projectGroupResult.costcenter', 
                                    'circle': {
                                        '$toObjectId': '$circle'
                                    },
                                    'customerName': {
                                                    '$toObjectId': "$custId"
                                                    }
                                }
                            },
                            {
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
                            },  {
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
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
                        'CreatedAt': {
                            '$toDate': '$CreatedAt'
                        }, 
                        'actionAt': {
                            '$toDate': '$actionAt'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }, 
                        'circle': '$projectResult.circle', 
                        'costcenter': '$projectResult.costcenter', 
                        "customerName": "$projectResult.customerName",
                        'empName': '$approverDetails.empName', 
                        'empCode': '$approverDetails.empCode', 
                        'ustCode': '$approverDetails.ustCode', 
                        "bankName": "$approverDetails.bankName",
                        "ifscCode": "$approverDetails.ifscCode",
                        "benificiaryname": "$approverDetails.benificiaryname",
                        "accountNumber": "$approverDetails.accountNumber",
                        'designation': '$approverDetails.band'
                    }
                }]
        arr=arr+arpp1+arpp2+arpp3+[{
                    '$addFields': {
                        'accountNumber':{
                          '$toString':"$accountNumber"  
                        },
                        'projectIdName': '$projectResult.projectId', 
                        'costcenter': '$projectResult.costcenter', 
                        "customerName": "$projectResult.customerName",
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'CreatedAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$CreatedAt'
                            }
                        }, 
                        'actionAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$actionAt'
                            }
                        }, 
                        'addedFor': {
                            '$toString': '$addedFor'
                        }, 
                        'advanceTypeId': {
                            '$toString': '$advanceTypeId'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'projectResult': 0, 
                        'addedAt': 0, 
                        'approverDetails': 0, 
                        'designationResult': 0, 
                        'length': 0, 
                        'L1Approver': 0, 
                        'L2Approver': 0, 
                        'L3Approver': 0, 
                        'AdvanceUniqueId': 0, 
                        'actionBy': 0
                    }
                }, {
                    '$project': {
                        'Advance ID': '$AdvanceNo', 
                        'Advance Date': '$CreatedAt', 
                        'Customer': '$customerName', 
                        'Circle': '$circle', 
                        'Cost Center': '$costcenter', 
                        'Project ID': '$projectIdName', 
                        'Employee Name': '$empName', 
                        'Employee Code': '$empCode',
                        'UST Code':"$ustCode",
                        'Bank Name':"$bankName",
                        'Account Number':"$accountNumber",
                        'IFSC Code':"$ifscCode",
                        'Benificiary Name':'$benificiaryname', 
                        'Designation': '$designation', 
                        'Claimed Amount': '$Amount', 
                        'Current Status': '$status', 
                        'Last Action Date': '$actionAt', 
                        'Approved Amount': '$ApprovedAmount', 
                        'Additional Info': '$additionalInfo',
                        'Remarks': '$remark'
                    }
                }
            ]
        response = cmo.finding_aggregate("Advance",arr)
        response = response["data"]
        if len(response):
            dataframe = pd.DataFrame(response)
            datecolumns=['Last Action Date','Advance Date']
            for col in datecolumns:
                if col in dataframe.columns:
                    dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Advance", "Advances"
            )
            # print("fullPathfullPathfullPath", fullPath)
            return send_file(fullPath)
        else:
            columns = [
                    'Expense ID', 'Submission Date', 'Circle', 'Cost Center', 'Project ID', 
                    'Employee Name', 'Employee Code', 'Bank Name', 'Account Number', 
                    'IFSC Code', 'Benificiary Name', 'Designation', 'Category', 
                    'Bill Number', 'Claimed Amount', 'Current Status', 'Approved Amount'
                ]
            df = pd.DataFrame(columns=columns)
            fullPath = excelWriteFunc.excelFileWriter(
                df, "Export_Advance", "Advances"
            )
            return send_file(fullPath)
    
    

@approval_blueprint.route("/export/Advance/l2Approval",methods=['GET','POST'])
@token_required
def advanceexportL2approval(current_user):
    filterstatus=request.args.get('status')
    empName=request.args.get('empName')
    empCode=request.args.get('empCode')
    arpp2=[]
    arpp3=[]
    status=['L1-Approved','L2-Rejected','L2-Approved']
    arpp1=[{
            '$match':{
                "status":{
                    '$in':status
                }
            }
        }]
    if filterstatus not in ['',None,'undefined']:
        arpp1=[{
            '$match':{
                "status":filterstatus
            }
        }]
        
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        arpp3=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
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
                        'localField': 'addedFor', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'L2Approver':current_user["userUniqueId"]
                                }
                            }
                        ], 
                        'as': 'approverDetails'
                    }
                }, {
                    '$addFields': {
                        'length': {
                            '$size': '$approverDetails'
                        }
                    }
                }, {
                    '$match': {
                        'length': {
                            '$ne': 0
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$approverDetails', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'designation', 
                        'localField': 'designation', 
                        'foreignField': '_id', 
                        'as': 'designationResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$designationResult', 
                        'preserveNullAndEmptyArrays': True
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
                                    },
                                    'customerName': {
                                                    '$toObjectId': "$custId"
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
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
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
                        'CreatedAt': {
                            '$toDate': '$CreatedAt'
                        }, 
                        'actionAt': {
                            '$toDate': '$actionAt'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }, 
                        'circle': '$projectResult.circle', 
                        'costcenter': '$projectResult.costcenter',
                        "customerName": "$projectResult.customerName", 
                        'empName': '$approverDetails.empName', 
                        'empCode': '$approverDetails.empCode', 
                        'ustCode': '$approverDetails.ustCode', 
                        "bankName": "$approverDetails.bankName",
                        "ifscCode": "$approverDetails.ifscCode",
                        "benificiaryname": "$approverDetails.benificiaryname",
                        "accountNumber": "$approverDetails.accountNumber",
                        'designation': '$approverDetails.band'
                    }
                }]
        arr=arr+arpp1+arpp2+arpp3+[{
                    '$addFields': {
                        'accountNumber':{
                          '$toString':"$accountNumber"  
                        },
                        'projectIdName': '$projectResult.projectId', 
                        "customerName": "$projectResult.customerName",
                        'costcenter': '$projectResult.costcenter', 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'CreatedAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$CreatedAt'
                            }
                        }, 
                        'actionAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$actionAt'
                            }
                        }, 
                        'addedFor': {
                            '$toString': '$addedFor'
                        }, 
                        'advanceTypeId': {
                            '$toString': '$advanceTypeId'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'projectResult': 0, 
                        'addedAt': 0, 
                        'approverDetails': 0, 
                        'designationResult': 0, 
                        'length': 0, 
                        'L1Approver': 0, 
                        'L2Approver': 0, 
                        'L3Approver': 0, 
                        'AdvanceUniqueId': 0, 
                        'actionBy': 0
                    }
                }, {
                    '$project': {
                        'Advance ID': '$AdvanceNo', 
                        'Advance Date': '$CreatedAt',
                        'Customer': '$customerName',  
                        'Circle': '$circle', 
                        'Cost Center': '$costcenter', 
                        'Project ID': '$projectIdName', 
                        'Employee Name': '$empName', 
                        'Employee Code': '$empCode', 
                        'UST Code':"$ustCode",
                        'Bank Name':"$bankName",
                        'Account Number':"$accountNumber",
                        'IFSC Code':"$ifscCode",
                        'Benificiary Name':'$benificiaryname',
                        'Designation': '$designation', 
                        'Claimed Amount': '$Amount', 
                        'Current Status': '$status', 
                        'Last Action Date': '$actionAt', 
                        'Approved Amount': '$ApprovedAmount', 
                        'Additional Info': '$additionalInfo',
                        'Remarks': '$remark'
                    }
                }
            ]
        response = cmo.finding_aggregate("Advance",arr)
        response = response["data"]
        if len(response):
            dataframe = pd.DataFrame(response)
            datecolumns=['Last Action Date','Advance Date']
            for col in datecolumns:
                if col in dataframe.columns:
                    dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Advance", "Advances"
            )
            # print("fullPathfullPathfullPath", fullPath)
            return send_file(fullPath)
        else:
            columns = [
                    'Expense ID', 'Submission Date', 'Circle', 'Cost Center', 'Project ID', 
                    'Employee Name', 'Employee Code', 'Bank Name', 'Account Number', 
                    'IFSC Code', 'Benificiary Name', 'Designation', 'Category', 
                    'Bill Number', 'Claimed Amount', 'Current Status', 'Approved Amount'
                ]
            df = pd.DataFrame(columns=columns)
            fullPath = excelWriteFunc.excelFileWriter(
                df, "Export_Advance", "Advances"
            )
            return send_file(fullPath)
    

@approval_blueprint.route("/export/Advance/l3Approval",methods=['GET','POST'])
@token_required
def advanceexportL3approval(current_user):
    filterstatus=request.args.get('status')
    empName=request.args.get('empName')
    empCode=request.args.get('empCode')
    arpp2=[]
    arpp3=[]
    status=['L2-Approved','L3-Rejected','L3-Approved']
    arpp1=[{
            '$match':{
                "status":{
                    '$in':status
                }
            }
        }]
    if filterstatus not in ['',None,'undefined']:
        arpp1=[{
            '$match':{
                "status":filterstatus
            }
        }]
        
    if empName not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empName":{
                    '$regex':empName.strip(),
                    '$options':'i'
                }
            }
        }]
    if empCode not in ['',None,'undefined']:
        arpp2=[{
            '$match':{
                "empCode":{
                    '$regex':empCode.strip(),
                    '$options':'i'
                }
            }
        }]
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
                        'localField': 'addedFor', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'financeApprover':current_user["userUniqueId"]
                                }
                            }
                        ], 
                        'as': 'approverDetails'
                    }
                }, {
                    '$addFields': {
                        'length': {
                            '$size': '$approverDetails'
                        }
                    }
                }, {
                    '$match': {
                        'length': {
                            '$ne': 0
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$approverDetails', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'designation', 
                        'localField': 'designation', 
                        'foreignField': '_id', 
                        'as': 'designationResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$designationResult', 
                        'preserveNullAndEmptyArrays': True
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
                            }, 
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
                                    },
                                    'customerName': {
                                                    '$toObjectId': "$custId"
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
                                            '$lookup': {
                                                'from': 'customer', 
                                                'localField': 'customerName', 
                                                'foreignField': '_id', 
                                                'as': 'customerName'
                                            }
                                        }, {
                                            '$addFields': {
                                                'circle': '$circle.circleName', 
                                                'customerName': {
                                                    '$arrayElemAt': [
                                                        '$customerName.customerName', 0
                                                    ]
                                                }
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
                        'CreatedAt': {
                            '$toDate': '$CreatedAt'
                        }, 
                        'actionAt': {
                            '$toDate': '$actionAt'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }, 
                        'circle': '$projectResult.circle', 
                        'costcenter': '$projectResult.costcenter', 
                        'empName': '$approverDetails.empName', 
                        'empCode': '$approverDetails.empCode',
                        "customerName": "$projectResult.customerName",
                        'ustCode': '$approverDetails.ustCode', 
                        "bankName": "$approverDetails.bankName",
                        "ifscCode": "$approverDetails.ifscCode",
                        "benificiaryname": "$approverDetails.benificiaryname",
                        "accountNumber": "$approverDetails.accountNumber",
                        'designation': '$approverDetails.band'
                    }
                }]
        arr=arr+arpp1+arpp2+arpp3+[
                {
                    '$addFields': {
                        'accountNumber':{
                          '$toString':"$accountNumber"  
                        },
                        'projectIdName': '$projectResult.projectId',
                        "customerName": "$projectResult.customerName", 
                        'costcenter': '$projectResult.costcenter', 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'CreatedAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$CreatedAt'
                            }
                        }, 
                        'actionAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$actionAt'
                            }
                        }, 
                        'addedFor': {
                            '$toString': '$addedFor'
                        }, 
                        'advanceTypeId': {
                            '$toString': '$advanceTypeId'
                        }, 
                        'projectId': {
                            '$toString': '$projectId'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'projectResult': 0, 
                        'addedAt': 0, 
                        'approverDetails': 0, 
                        'designationResult': 0, 
                        'length': 0, 
                        'L1Approver': 0, 
                        'L2Approver': 0, 
                        'L3Approver': 0, 
                        'AdvanceUniqueId': 0, 
                        'actionBy': 0
                    }
                }, {
                    '$project': {
                        'Advance ID': '$AdvanceNo', 
                        'Advance Date': '$CreatedAt',
                        'Customer': '$customerName',  
                        'Circle': '$circle', 
                        'Cost Center': '$costcenter', 
                        'Project ID': '$projectIdName', 
                        'Employee Name': '$empName', 
                        'Employee Code': '$empCode', 
                        'UST Code': '$ustCode',
                        'Bank Name':"$bankName",
                    'Account Number':"$accountNumber",
                    'IFSC Code':"$ifscCode",
                    'Benificiary Name':'$benificiaryname',
                        'Designation': '$designation', 
                        'Claimed Amount': '$Amount', 
                        'Current Status': '$status', 
                        'Last Action Date': '$actionAt', 
                        'Approved Amount': '$ApprovedAmount',
                        'Additional Info': '$additionalInfo', 
                        'Remarks': '$remark'
                    }
                }
            ]
        response = cmo.finding_aggregate("Advance",arr)
        response = response["data"]
        if len(response):
            dataframe = pd.DataFrame(response)
            datecolumns=['Last Action Date','Advance Date']
            for col in datecolumns:
                if col in dataframe.columns:
                    dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_Advance", "Advances"
            )
            # print("fullPathfullPathfullPath", fullPath)
            return send_file(fullPath)
        else:
            columns = [
                    'Expense ID', 'Submission Date', 'Circle', 'Cost Center', 'Project ID', 
                    'Employee Name', 'Employee Code', 'Bank Name', 'Account Number', 
                    'IFSC Code', 'Benificiary Name', 'Designation', 'Category', 
                    'Bill Number', 'Claimed Amount', 'Current Status', 'Approved Amount'
                ]
            df = pd.DataFrame(columns=columns)
            fullPath = excelWriteFunc.excelFileWriter(
                df, "Export_Advance", "Advances"
            )

@approval_blueprint.route("/expenses/claimAndAdvance", methods=["GET", "POST", "PUT", "DELETE"])
@approval_blueprint.route("/expenses/claimAndAdvance/<id>", methods=["GET", "POST", "PUT", "DELETE"])
@token_required
def claimsAndAdvance(current_user, id=None):
    Number = request.args.get("Number")
    if request.method == "GET":
        if Number != None and Number != "undefined":
            type = Number[:3]
            if type == "EXP":
                arr = [
                    {"$match": {"ExpenseNo": Number,'addedFor':ObjectId(current_user['userUniqueId'])}},
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
                                    "designation": "$band"
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
                                        'circle': '$circle.circleName', 
                                        'customerName': {
                                            '$toObjectId': '$custId'
                                        }
                                    }
                                }, {
                                    '$lookup': {
                                        'from': 'customer', 
                                        'localField': 'customerName', 
                                        'foreignField': '_id', 
                                        'as': 'customerName'
                                    }
                                }, {
                                    '$addFields': {
                                        'customerName': {
                                            '$arrayElemAt': [
                                                '$customerName.customerName', 0
                                            ]
                                        }
                                    }
                                }
                            ], 
                            'as': 'projectResult'
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
                            'customerName': '$projectResult.customerName'
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
                Response = cmo.finding_aggregate("Expenses", arr)
                return respond(Response)
            if type=="ADV":
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
                                {"$addFields":
                                    {"circle": "$circle.circleName",
                                     'customerName': {
                                        '$toObjectId': '$custId'
                                    }
                                     }
                                 },
                                {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
                                }
                            }
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
                            # "designation": "$band",
                            "designation":"$approverDetails.band",
                            'customerName': '$projectResult.customerName'
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
                

                Response = cmo.finding_aggregate("Advance", arr)
                return respond(Response)
            else:
                arr=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            'SettlementID':Number
                        }
                    }, {
                        '$lookup': {
                            'from': 'userRegister', 
                            'localField': 'empID', 
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
                                        'costCenter': {
                                            '$toObjectId': '$costCenter'
                                        }, 
                                        'designation': {
                                            '$toObjectId': '$designation'
                                        }
                                    }
                                }, {
                                    '$lookup': {
                                        'from': 'costCenter', 
                                        'localField': 'costCenter', 
                                        'foreignField': '_id', 
                                        'as': 'costCenter'
                                    }
                                }, {
                                    '$lookup': {
                                        'from': 'designation', 
                                        'localField': 'designation', 
                                        'foreignField': '_id', 
                                        'as': 'designation'
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$designation', 
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$costCenter', 
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$project': {
                                        'costCenter': '$costCenter.costCenter', 
                                        '_id': 0, 
                                        'empName': 1, 
                                        'empCode': 1, 
                                        'ustCode': 1, 
                                        'designation': '$designation.designation'
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
                        '$addFields': {
                            'CreatedAt': {
                                '$toDate': '$approvalDate'
                            }
                        }
                    }, {
                        '$addFields': {
                            'expensemonth': {
                                '$dateToString': {
                                    'format': '%m-%Y', 
                                    'date': '$CreatedAt'
                                }
                            }, 
                            'expenseDate': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y', 
                                    'date': '$CreatedAt'
                                }
                            }
                        }
                    }, {
                        '$project': {
                            'types': 'Settlement', 
                            'name': '$SettlementID', 
                            'designation': '$userResults.designation', 
                            'empCode': '$userResults.empCode', 
                            'empName': '$userResults.empName', 
                            'ustCode': '$userResults.ustCode', 
                            'costcenter': '$userResults.costCenter', 
                            'ApprovedAmount': '$Amount', 
                            'customStatus': 'L3-Approved', 
                            'remark': '$remarks', 
                            'uniqueId': {
                                '$toString': '$_id'
                            }, 
                            'expensemonth': 1, 
                            'expenseDate': 1, 
                            '_id': 0,
                            'type':"Settlement"
                        }
                    }
                ]
                Response = cmo.finding_aggregate("Settlement", arr)
                return respond(Response)
        else:
            arr =[
                {"$match": {"_id": ObjectId(current_user["userUniqueId"])}}, 
                        {
                            '$addFields': {
                                'costCenter': {
                                    '$toObjectId': '$costCenter'
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'costCenter', 
                                'localField': 'costCenter', 
                                'foreignField': '_id', 
                                'as': 'costCenter'
                            }
                        }, {
                            '$unwind': {
                                'path': '$costCenter', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
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
                                                        }, 
                                                        'customerName': {
                                                            '$toObjectId': '$custId'
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
                                                }, {
                                                    '$lookup': {
                                                        'from': 'customer', 
                                                        'localField': 'customerName', 
                                                        'foreignField': '_id', 
                                                        'as': 'customerName'
                                                    }
                                                }, {
                                                    '$addFields': {
                                                        'customerName': {
                                                            '$arrayElemAt': [
                                                                '$customerName.customerName', 0
                                                            ]
                                                        }
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
                                            'customerName': '$projectResult.customerName', 
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
                                                        }, 
                                                        'customerName': {
                                                            '$toObjectId': '$custId'
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
                                                }, {
                                                    '$lookup': {
                                                        'from': 'customer', 
                                                        'localField': 'customerName', 
                                                        'foreignField': '_id', 
                                                        'as': 'customerName'
                                                    }
                                                }, {
                                                    '$addFields': {
                                                        'customerName': {
                                                            '$arrayElemAt': [
                                                                '$customerName.customerName', 0
                                                            ]
                                                        }
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
                                            }, 
                                            'customerName': '$projectResult.customerName'
                                        }
                                    }
                                ], 
                                'as': 'AdvanceResults'
                            }
                        }, {
                            '$lookup': {
                                'from': 'Settlement', 
                                'localField': '_id', 
                                'foreignField': 'empID', 
                                'pipeline': [
                                    {
                                        '$match': {
                                            'deleteStatus': {
                                                '$ne': 1
                                            }
                                        }
                                    }, {
                                        '$addFields': {
                                            'approvalDate': {
                                                '$cond': {
                                                    'if': {
                                                        '$or': [
                                                            {
                                                                '$eq': [
                                                                    '$approvalDate', ''
                                                                ]
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{2}:\\d{2}:\\d{4}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{4}:\\d{2}:\\d{2}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{1}:\\d{2}:\\d{4}$'
                                                                }
                                                            }
                                                        ]
                                                    }, 
                                                    'then': None, 
                                                    'else': {
                                                        '$toDate': '$approvalDate'
                                                    }
                                                }
                                            }, 
                                            'SettlementRequisitionDate': {
                                                '$cond': {
                                                    'if': {
                                                        '$or': [
                                                            {
                                                                '$eq': [
                                                                    '$SettlementRequisitionDate', ''
                                                                ]
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$SettlementRequisitionDate', 
                                                                    'regex': '^\\d{2}:\\d{2}:\\d{4}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$SettlementRequisitionDate', 
                                                                    'regex': '^\\d{4}:\\d{2}:\\d{2}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$SettlementRequisitionDate', 
                                                                    'regex': '^\\d{1}:\\d{2}:\\d{4}$'
                                                                }
                                                            }
                                                        ]
                                                    }, 
                                                    'then': None, 
                                                    'else': {
                                                        '$toDate': '$SettlementRequisitionDate'
                                                    }
                                                }
                                            }, 
                                            'type': 'Settlement', 
                                            'name': '$SettlementID', 
                                            'ApprovedAmount': '$Amount', 
                                            'customisedStatus': 6, 
                                            'status': 'L3-Approved', 
                                            'customStatus': 'L3-Approved', 
                                            'uniqueId': {
                                                '$toString': '$_id'
                                            }, 
                                            'AddedAt': {
                                                '$cond': {
                                                    'if': {
                                                        '$or': [
                                                            {
                                                                '$eq': [
                                                                    '$approvalDate', ''
                                                                ]
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{2}:\\d{2}:\\d{4}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{4}:\\d{2}:\\d{2}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{1}:\\d{2}:\\d{4}$'
                                                                }
                                                            }
                                                        ]
                                                    }, 
                                                    'then': None, 
                                                    'else': {
                                                        '$toDate': '$approvalDate'
                                                    }
                                                }
                                            }, 
                                            'submissionDate': {
                                                '$cond': {
                                                    'if': {
                                                        '$or': [
                                                            {
                                                                '$eq': [
                                                                    '$approvalDate', ''
                                                                ]
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{2}:\\d{2}:\\d{4}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{4}:\\d{2}:\\d{2}$'
                                                                }
                                                            }, {
                                                                '$regexMatch': {
                                                                    'input': '$approvalDate', 
                                                                    'regex': '^\\d{1}:\\d{2}:\\d{4}$'
                                                                }
                                                            }
                                                        ]
                                                    }, 
                                                    'then': None, 
                                                    'else': {
                                                        '$toDate': '$approvalDate'
                                                    }
                                                }
                                            }, 
                                            'empID': {
                                                '$toString': '$empID'
                                            }
                                        }
                                    }
                                ], 
                                'as': 'SettlementResults'
                            }
                        }, {
                            '$addFields': {
                                'newData': {
                                    '$concatArrays': [
                                        '$AdvanceResults', '$ExpenseDetails', '$SettlementResults'
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
                                }, 
                                'SettlementRequisitionDate': {
                                    '$dateToString': {
                                        'format': '%d-%m-%Y', 
                                        'date': '$newData.SettlementRequisitionDate', 
                                        'timezone': 'Asia/Kolkata'
                                    }
                                }, 
                                'approvalDate': {
                                    '$dateToString': {
                                        'format': '%d-%m-%Y', 
                                        'date': '$newData.approvalDate', 
                                        'timezone': 'Asia/Kolkata'
                                    }
                                }, 
                                'costCenter': '$newData.projectResult.costcenter', 
                                'customerName': '$newData.projectResult.customerName', 
                                'costCenter2': '$costCenter.costCenter'
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
                                'customerName': '$customerName', 
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
                                'costCenter': 1, 
                                'remark': '$newData.remark', 
                                'ApprovedAmount': '$newData.ApprovedAmount', 
                                'customStatus': '$newData.customStatus', 
                                'uniqueId': 1, 
                                'customisedStatus': '$newData.customisedStatus', 
                                'type': '$newData.type', 
                                'approvalDate': 1, 
                                'SettlementRequisitionDate': 1, 
                                'costCenter2': 1
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
                                }, 
                                'costCenter': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$type', 'Settlement'
                                            ]
                                        }, 
                                        'then': '$costCenter2', 
                                        'else': '$costCenter'
                                    }
                                }
                            }
                        }, {
                            '$addFields': {
                                'submissionDate': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$type', 'Settlement'
                                            ]
                                        }, 
                                        'then': '$approvalDate', 
                                        'else': '$submissionDate'
                                    }
                                }, 
                                'AddedAt': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$type', 'Settlement'
                                            ]
                                        }, 
                                        'then': '$approvalDate', 
                                        'else': '$AddedAt'
                                    }
                                }, 
                                'actionAt': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$type', 'Settlement'
                                            ]
                                        }, 
                                        'then': '$approvalDate', 
                                        'else': '$actionAt'
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
                            '$sort': {
                                'AddedAt': -1
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
                                'empID': {
                                    '$toObjectId': '$_id'
                                }
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
                            '$project': {
                                'empID': 0
                            }
                        }, {
                            '$sort': {
                                'AddedAt': -1
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
                                                        '$ne': [
                                                            '$type', 'Settlement'
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
                                'SettleAmountTotal': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$eq': [
                                                            '$type', 'Settlement'
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
                                'AdvanceAmountTotal': {
                                    '$cond': {
                                        'if': {
                                            '$gt': [
                                                '$Openingbalance', 0
                                            ]
                                        }, 
                                        'then': {
                                            '$add': [
                                                '$AdvanceAmountTotal', '$SettleAmountTotal'
                                            ]
                                        }, 
                                        'else': '$AdvanceAmountTotal'
                                    }
                                }
                            }
                        }, {
                            '$addFields': {
                                'data': {
                                    '$sortArray': {
                                        'input': '$data', 
                                        'sortBy': {
                                            'AddedAt': -1
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
            Response = cmo.finding_aggregate("userRegister", arr)
            return respond(Response)
    
    



@approval_blueprint.route("/Advance/AllAdvance", methods=["GET", "POST"])
@approval_blueprint.route("/Advance/AllAdvance/<id>", methods=["GET", "POST", "DELETE"])
@token_required
def AllAdvances(current_user, id=None):
    if request.method == "GET":
        AdvanceNo = request.args.get("AdvanceNo")
        filterstatus = request.args.get('status')
        claimType = request.args.get('claimType')
        empName = request.args.get('empName')
        empCode = request.args.get('empCode')
        month=request.args.get("month")
        year=request.args.get("year")
        # if year in ['',None,'undefined']:
        #     year = str(datetime.now().year)
        arpp1=[]
        arpp2=[]
        arpp3=[]
        arpp4=[]
        arrp5=[]
        arrp6=[]
        arrp7=[]
        
        
        if filterstatus not in ['',None,'undefined']:
            arpp1=[{
                '$match':{
                    "status":filterstatus
                }
            }]
            
        if empName not in ['',None,'undefined']:
            arpp2=[{
                '$match':{
                    "empName":{
                        '$regex':empName.strip(),
                        '$options':'i'
                    }
                }
            }]
        if empCode not in ['',None,'undefined']:
            arpp3=[{
                '$match':{
                    "empCode":{
                        '$regex':empCode.strip(),
                        '$options':'i'
                    }
                }
            }]
        
        if claimType not in ['',None,'undefined']:
            arrp5=[{
                '$match':{
                    "advanceTypeId":ObjectId(claimType)
                }
            }]
        
        if AdvanceNo not in ['',None,'undefined']:
            
            
            arpp4=[{
                '$match':{
                    "AdvanceNo":{
                        '$regex':AdvanceNo.strip(),
                        '$options':'i'
                    }
                }
            }]
        if month not in ['',None,'undefined']:
            arrp6=[{
                '$match':{
                    "month":{
                        '$regex':month.strip(),
                        '$options':'i'
                    },
                    'status':"L3-Approved"
                }
            }]
        if year not in ['',None,'undefined']:
            arrp7=[{
                '$match':{
                    "year":year,
                     'status':"L3-Approved"
                }
            }]

        
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
            arr = arpp1+arpp4+arrp5+[
                {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    
                                }
                            },
                 {
                    '$sort': {
                        '_id': -1
                    }
                },
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                   
                                }
                            }
                        ],
                        "as": "userInfo",
                    }
                }]
            arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
            arr=arr+[
                        {
                            '$addFields': {
                                'len': {
                                    '$size': '$userInfo'
                                }
                            }
                        }, {
                            '$match': {
                                'len': {
                                    '$ne': 0
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
                                                }, {
                                                    '$project': {
                                                        '_id': 0, 
                                                        'circleName': 1, 
                                                        'circleCode': 1
                                                    }
                                                }
                                            ], 
                                            'as': 'circleResult'
                                        }
                                    }, {
                                        '$unwind': {
                                            'path': '$circleResult', 
                                            'preserveNullAndEmptyArrays': True
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
                            '$addFields': {
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
                                            'empName': 1,
                                            
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
                            '$addFields': {
                                'CreatedAt': {
                                    '$toDate': '$CreatedAt'
                                }, 
                                'actionAt': {
                                    '$toDate': '$actionAt'
                                }, 
                                'empName': '$userInfo.empName', 
                                'empCode': '$userInfo.empCode', 
                                'mobile': '$userInfo.mobile',
                                'ustCode':'$userInfo.ustCode'
                            }
                        }]
            
            arr=arr+arpp3+[
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
                                                                'then': 'Jan'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 2
                                                                    ]
                                                                }, 
                                                                'then': 'Feb'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 3
                                                                    ]
                                                                }, 
                                                                'then': 'Mar'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 4
                                                                    ]
                                                                }, 
                                                                'then': 'Apr'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 5
                                                                    ]
                                                                }, 
                                                                'then': 'May'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 6
                                                                    ]
                                                                }, 
                                                                'then': 'Jun'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 7
                                                                    ]
                                                                }, 
                                                                'then': 'Jul'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 8
                                                                    ]
                                                                }, 
                                                                'then': 'Aug'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 9
                                                                    ]
                                                                }, 
                                                                'then': 'Sep'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 10
                                                                    ]
                                                                }, 
                                                                'then': 'Oct'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 11
                                                                    ]
                                                                }, 
                                                                'then': 'Nov'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 12
                                                                    ]
                                                                }, 
                                                                'then': 'Dec'
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
                                                                'then': 'Feb'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 2
                                                                    ]
                                                                }, 
                                                                'then': 'Mar'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 3
                                                                    ]
                                                                }, 
                                                                'then': 'Apr'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 4
                                                                    ]
                                                                }, 
                                                                'then': 'May'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 5
                                                                    ]
                                                                }, 
                                                                'then': 'Jun'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 6
                                                                    ]
                                                                }, 
                                                                'then': 'Jul'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 7
                                                                    ]
                                                                }, 
                                                                'then': 'Aug'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 8
                                                                    ]
                                                                }, 
                                                                'then': 'Sep'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 9
                                                                    ]
                                                                }, 
                                                                'then': 'Oct'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 10
                                                                    ]
                                                                }, 
                                                                'then': 'Nov'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 11
                                                                    ]
                                                                }, 
                                                                'then': 'Dec'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 12
                                                                    ]
                                                                }, 
                                                                'then': 'Jan'
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
                            '$addFields': {
                                'year': {
                                    '$toString': '$year'
                                }
                            }
                        }]
            arr=arr+arrp6+arrp7+[{
                            '$addFields': {
                                'AdvanceDate': {
                                    '$dateToString': {
                                        'format': '%Y-%m-%d', 
                                        'date': '$CreatedAt'
                                    }
                                }, 
                                'ActionAt': {
                                    '$dateToString': {
                                        'format': '%Y-%m-%d', 
                                        'date': '$actionAt'
                                    }
                                }, 
                                'Month': {
                                    '$concat': [
                                        '$month', '-', '$year'
                                    ]
                                }, 
                                'circleCode': '$projectResult.circleResult.circleCode', 
                                'circleName': '$projectResult.circleResult.circleName', 
                                'projectIdName': '$projectResult.projectId', 
                                'costcenter': '$projectResult.costcenter'
                            }
                        }, {
                            '$lookup': {
                                'from': 'claimType', 
                                'localField': 'advanceTypeId', 
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
                                'as': 'advanceTypeResult'
                            }
                        }, {
                            '$unwind': {
                                'path': '$advanceTypeResult', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$addFields': {
                                'advanceType': '$advanceTypeResult.claimType'
                            }
                        }, {
                            '$sort': {
                                '_id': -1
                            }
                        }, {
                            '$project': {
                                'overall_table_count': 1, 
                                'Month': '$Month', 
                                'Employee Name': '$empName', 
                                'Employee Code': '$empCode',
                                'UST Employee Code':"$ustCode",
                                'Contact Number': '$mobile', 
                                'Advance number': '$AdvanceNo', 
                                'Advance Date': '$AdvanceDate', 
                                'Advance Type': '$advanceType', 
                                'Circle': '$circleName', 
                                'Project ID': '$projectIdName', 
                                'Cost Center': '$costcenter', 
                                'Amount': '$Amount', 
                                'Submission Date': '$AdvanceDate', 
                                'Approved Amount': '$ApprovedAmount', 
                                'Last Action Date': '$ActionAt', 
                                'Status':'$status',
                                'L1 Status': '$L1ApproverStatus', 
                                'L2 Status': '$L2ApproverStatus', 
                                'L3 Status': '$L3ApproverStatus', 
                                'L1 Approver': '$L1ApproverResult.empName', 
                                'L2 Approver': '$L2ApproverResult.empName', 
                                'L3 Approver': '$L3ApproverResult.empName', 
                                'Remarks': '$remark', 
                                'uniqueId': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
            Response = cmo.finding_aggregate("Advance", arr)
            return respond(Response)
        else:
            arr= arpp1+arpp4+arrp5+[
                {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    
                                }
                            },
                
                
                            {
                    '$addFields': {
                        'L1ids': {
                            '$arrayElemAt': [
                                '$L1Approver.actionBy', 0
                            ]
                        }, 
                        'L2ids': {
                            '$arrayElemAt': [
                                '$L2Approver.actionBy', 0
                            ]
                        }, 
                        'L3ids': {
                            '$arrayElemAt': [
                                '$L3Approver.actionBy', 0
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'ids': [
                            {
                                '$ifNull': [
                                    '$L1ids', None
                                ]
                            }, {
                                '$ifNull': [
                                    '$L2ids', None
                                ]
                            }, {
                                '$ifNull': [
                                    '$L3ids', None
                                ]
                            }
                        ]
                    }
                }, {
                    '$match': {
                        'ids': ObjectId(current_user['userUniqueId'])
                    }
                }, {
                    '$project': {
                        'ids': 0, 
                        'L1ids': 0, 
                        'L2ids': 0, 
                        'L3ids': 0
                    }
                },
                 {
                    '$sort': {
                        '_id': -1
                    }
                },
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "addedFor",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                   
                                }
                            }
                        ],
                        "as": "userInfo",
                    }
                }]
            arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
            arr=arr+[
                        {
                            '$addFields': {
                                'len': {
                                    '$size': '$userInfo'
                                }
                            }
                        }, {
                            '$match': {
                                'len': {
                                    '$ne': 0
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
                                                }, {
                                                    '$project': {
                                                        '_id': 0, 
                                                        'circleName': 1, 
                                                        'circleCode': 1
                                                    }
                                                }
                                            ], 
                                            'as': 'circleResult'
                                        }
                                    }, {
                                        '$unwind': {
                                            'path': '$circleResult', 
                                            'preserveNullAndEmptyArrays': True
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
                            '$addFields': {
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
                            '$addFields': {
                                'CreatedAt': {
                                    '$toDate': '$CreatedAt'
                                }, 
                                'actionAt': {
                                    '$toDate': '$actionAt'
                                }, 
                                'empName': '$userInfo.empName', 
                                'empCode': '$userInfo.empCode', 
                                'mobile': '$userInfo.mobile',
                                'ustCode':'$userInfo.ustCode'
                            }
                                }]
            arr=arr+arpp3+[
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
                                                                'then': 'Jan'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 2
                                                                    ]
                                                                }, 
                                                                'then': 'Feb'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 3
                                                                    ]
                                                                }, 
                                                                'then': 'Mar'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 4
                                                                    ]
                                                                }, 
                                                                'then': 'Apr'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 5
                                                                    ]
                                                                }, 
                                                                'then': 'May'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 6
                                                                    ]
                                                                }, 
                                                                'then': 'Jun'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 7
                                                                    ]
                                                                }, 
                                                                'then': 'Jul'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 8
                                                                    ]
                                                                }, 
                                                                'then': 'Aug'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 9
                                                                    ]
                                                                }, 
                                                                'then': 'Sep'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 10
                                                                    ]
                                                                }, 
                                                                'then': 'Oct'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 11
                                                                    ]
                                                                }, 
                                                                'then': 'Nov'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 12
                                                                    ]
                                                                }, 
                                                                'then': 'Dec'
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
                                                                'then': 'Feb'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 2
                                                                    ]
                                                                }, 
                                                                'then': 'Mar'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 3
                                                                    ]
                                                                }, 
                                                                'then': 'Apr'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 4
                                                                    ]
                                                                }, 
                                                                'then': 'May'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 5
                                                                    ]
                                                                }, 
                                                                'then': 'Jun'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 6
                                                                    ]
                                                                }, 
                                                                'then': 'Jul'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 7
                                                                    ]
                                                                }, 
                                                                'then': 'Aug'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 8
                                                                    ]
                                                                }, 
                                                                'then': 'Sep'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 9
                                                                    ]
                                                                }, 
                                                                'then': 'Oct'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 10
                                                                    ]
                                                                }, 
                                                                'then': 'Nov'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 11
                                                                    ]
                                                                }, 
                                                                'then': 'Dec'
                                                            }, {
                                                                'case': {
                                                                    '$eq': [
                                                                        {
                                                                            '$month': '$actionAtKolkata'
                                                                        }, 12
                                                                    ]
                                                                }, 
                                                                'then': 'Jan'
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
                                    '$year': '$actionAt'
                                }
                            }
                        }, {
                            '$addFields': {
                                'year': {
                                    '$toString': '$year'
                                }
                            }
                        }]
            arr=arr+arrp6+arrp7+[{
                            '$addFields': {
                                'AdvanceDate': {
                                    '$dateToString': {
                                        'format': '%Y-%m-%d', 
                                        'date': '$CreatedAt'
                                    }
                                }, 
                                'ActionAt': {
                                    '$dateToString': {
                                        'format': '%Y-%m-%d', 
                                        'date': '$actionAt'
                                    }
                                }, 
                                'Month': {
                                    '$concat': [
                                        '$month', '-', '$year'
                                    ]
                                }, 
                                'circleCode': '$projectResult.circleResult.circleCode', 
                                'circleName': '$projectResult.circleResult.circleName', 
                                'projectIdName': '$projectResult.projectId', 
                                'costcenter': '$projectResult.costcenter'
                            }
                        }, {
                            '$lookup': {
                                'from': 'claimType', 
                                'localField': 'advanceTypeId', 
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
                                'as': 'advanceTypeResult'
                            }
                        }, {
                            '$unwind': {
                                'path': '$advanceTypeResult', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$addFields': {
                                'advanceType': '$advanceTypeResult.claimType'
                            }
                        }, {
                            '$sort': {
                                '_id': -1
                            }
                        }, {
                            '$project': {
                                'overall_table_count': 1, 
                                'Month': '$Month', 
                                'Employee Name': '$empName', 
                                'Employee Code': '$empCode', 
                                'UST Employee Code':'$ustCode',
                                'Contact Number': '$mobile', 
                                'Advance number': '$AdvanceNo', 
                                'Advance Date': '$AdvanceDate', 
                                'Advance Type': '$advanceType', 
                                'Circle': '$circleName', 
                                'Project ID': '$projectIdName', 
                                'Cost Center': '$costcenter', 
                                'Amount': '$Amount', 
                                'Submission Date': '$AdvanceDate', 
                                'Approved Amount': '$ApprovedAmount', 
                                'Last Action Date': '$ActionAt', 
                                'L1 Status': '$L1ApproverStatus', 
                                'L2 Status': '$L2ApproverStatus', 
                                'L3 Status': '$L3ApproverStatus', 
                                'L1 Approver': '$L1ApproverResult.empName', 
                                'L2 Approver': '$L2ApproverResult.empName', 
                                'L3 Approver': '$L3ApproverResult.empName', 
                                'Remarks': '$remark', 
                                'uniqueId': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
            Response = cmo.finding_aggregate("Advance", arr)
            return respond(Response)
    if request.method == "DELETE":
        if id != None and id != "undefined":
            cmo.deleting("Approval", id)
            Response = cmo.deleting("Advance", id)
            cmo.insertion("ExpenseDeleted",{'AdvanceId':ObjectId(id),'deletedBy':ObjectId(current_user['userUniqueId']),'deletedAt':int(get_current_date_timestamp()),'type':'Advance'})
            return respond(Response)


@approval_blueprint.route("/export/AllAdvance", methods=["GET", "POST"])
@token_required
def AllAdvancesExport(current_user, id=None):
    if request.method == "GET":
        AdvanceNo = request.args.get("AdvanceNo")
        filterstatus = request.args.get('status')
        claimType = request.args.get('claimType')
        empName = request.args.get('empName')
        empCode = request.args.get('empCode')
        month=request.args.get("month")
        year=request.args.get("year")
        # if year in ['',None,'undefined']:
        #     year = str(datetime.now().year)
        arpp1=[]
        arpp2=[]
        arpp3=[]
        arpp4=[]
        arrp5=[]
        arrp6=[]
        arrp7=[]
        if filterstatus not in ['',None,'undefined']:
            arpp1=[{
                '$match':{
                    "status":filterstatus
                }
            }]
            
        if empName not in ['',None,'undefined']:
            arpp2=[{
                '$match':{
                    "empName":{
                        '$regex':empName.strip(),
                        '$options':'i'
                    }
                }
            }]
        if empCode not in ['',None,'undefined']:
            arpp3=[{
                '$match':{
                    "empCode":{
                        '$regex':empCode.strip(),
                        '$options':'i'
                    }
                }
            }]
        
        if claimType not in ['',None,'undefined']:
            arrp5=[{
                '$match':{
                    "advanceTypeId":ObjectId(claimType)
                }
            }]
        
        if AdvanceNo not in ['',None,'undefined']:
            
            
            arpp4=[{
                '$match':{
                    "AdvanceNo":{
                        '$regex':AdvanceNo.strip(),
                        '$options':'i'
                    }
                }
            }]
        if month not in ['',None,'undefined']:
            arrp6=[{
                '$match':{
                    "month":{
                        '$regex':month.strip(),
                        '$options':'i'
                    },
                    'status':"L3-Approved"
                }
            }]
        if year not in ['',None,'undefined']:
            arrp7=[{
                '$match':{
                    "year":year,
                     'status':"L3-Approved"
                }
            }]
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
            arr = arpp1+arpp4+arrp5+[
                {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'addedFor', 
                        'foreignField': '_id', 
                        'as': 'userInfo'
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
                                        }, {
                                            '$project': {
                                                '_id': 0, 
                                                'circleName': 1, 
                                                'circleCode': 1
                                            }
                                        }
                                    ], 
                                    'as': 'circleResult'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$circleResult', 
                                    'preserveNullAndEmptyArrays': True
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
                    '$addFields': {
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
                    '$addFields': {
                        'CreatedAt': {
                            '$toDate': '$CreatedAt'
                        }, 
                        'actionAt': {
                            '$toDate': '$actionAt'
                        }, 
                        'empName': '$userInfo.empName', 
                        'empCode': '$userInfo.empCode', 
                        'mobile': '$userInfo.mobile', 
                        'ustCode': '$userInfo.ustCode', 
                        'bankName': '$userInfo.bankName', 
                        'accountNumber': '$userInfo.accountNumber', 
                        'ifscCode': '$userInfo.ifscCode', 
                        'benificiaryname': '$userInfo.benificiaryname'
                    }
                }]
            arr=arr+arpp3+[{
                    '$addFields': {
                        'accountNumber':{
                            '$toString':"$accountNumber"
                        },
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
                                                        'then': 'Jan'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 2
                                                            ]
                                                        }, 
                                                        'then': 'Feb'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 3
                                                            ]
                                                        }, 
                                                        'then': 'Mar'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 4
                                                            ]
                                                        }, 
                                                        'then': 'Apr'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 5
                                                            ]
                                                        }, 
                                                        'then': 'May'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 6
                                                            ]
                                                        }, 
                                                        'then': 'Jun'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 7
                                                            ]
                                                        }, 
                                                        'then': 'Jul'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 8
                                                            ]
                                                        }, 
                                                        'then': 'Aug'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 9
                                                            ]
                                                        }, 
                                                        'then': 'Sep'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 10
                                                            ]
                                                        }, 
                                                        'then': 'Oct'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 11
                                                            ]
                                                        }, 
                                                        'then': 'Nov'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 12
                                                            ]
                                                        }, 
                                                        'then': 'Dec'
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
                                                        'then': 'Feb'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 2
                                                            ]
                                                        }, 
                                                        'then': 'Mar'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 3
                                                            ]
                                                        }, 
                                                        'then': 'Apr'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 4
                                                            ]
                                                        }, 
                                                        'then': 'May'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 5
                                                            ]
                                                        }, 
                                                        'then': 'Jun'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 6
                                                            ]
                                                        }, 
                                                        'then': 'Jul'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 7
                                                            ]
                                                        }, 
                                                        'then': 'Aug'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 8
                                                            ]
                                                        }, 
                                                        'then': 'Sep'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 9
                                                            ]
                                                        }, 
                                                        'then': 'Oct'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 10
                                                            ]
                                                        }, 
                                                        'then': 'Nov'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 11
                                                            ]
                                                        }, 
                                                        'then': 'Dec'
                                                    }, {
                                                        'case': {
                                                            '$eq': [
                                                                {
                                                                    '$month': '$actionAtKolkata'
                                                                }, 12
                                                            ]
                                                        }, 
                                                        'then': 'Jan'
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
                    '$addFields': {
                        'year': {
                            '$toString': '$year'
                        }
                    }
                }]
            arr=arr+arrp6+arrp7+[
                {
                    '$lookup': {
                        'from': 'claimType', 
                        'localField': 'advanceTypeId', 
                        'foreignField': '_id', 
                        'as': 'AdvanceTypeResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$AdvanceTypeResult', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'advanceType': '$AdvanceTypeResult.claimType', 
                        'AdvanceDate': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$CreatedAt'
                            }
                        }, 
                        'ActionAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$actionAt'
                            }
                        }, 
                        'Month': {
                            '$concat': [
                                '$month', '-', '$year'
                            ]
                        }, 
                        'circleCode': '$projectResult.circleResult.circleCode', 
                        'circleName': '$projectResult.circleResult.circleName', 
                        'projectIdName': '$projectResult.projectId', 
                        'costcenter': '$projectResult.costcenter'
                    }
                }, {
                    '$project': {
                        'Advance Month': '$Month', 
                        'Employee Name': '$empName', 
                        'Employee Code': '$empCode', 
                        'UST Employee Code': '$ustCode', 
                        'Contact Number': '$mobile', 
                        'Advance number': '$AdvanceNo', 
                        'Advance Date': '$AdvanceDate', 
                        'Advance Type': '$advanceType', 
                        'Circle': '$circleName', 
                        'Project ID': '$projectIdName', 
                        'Cost Center': '$costcenter', 
                        'Amount': '$Amount', 
                        'Submission Date': '$AdvanceDate', 
                        'Approved Amount': '$ApprovedAmount', 
                        'Last Action Date': '$ActionAt', 
                        'Current Status': '$status', 
                        'L1 Status': '$L1ApproverStatus', 
                        'L2 Status': '$L2ApproverStatus', 
                        'L3 Status': '$L3ApproverStatus', 
                        'L1 Approver': '$L1ApproverResult.empName', 
                        'L2 Approver': '$L2ApproverResult.empName', 
                        'L3 Approver': '$L3ApproverResult.empName', 
                        'Bank Name': '$bankName', 
                        'Bank Account Number': '$accountNumber', 
                        'IFSC Code': '$ifscCode', 
                        'Benificiary Name': '$benificiaryname', 
                        'Additional Info': '$additionalInfo',
                        'Remarks': '$remark', 
                        '_id': 0
                    }
                }
            ]
            response = cmo.finding_aggregate("Advance", arr)
            if len(response["data"]):
                response = response["data"]
                dataframe = pd.DataFrame(response)
                dateFields=['Submission Date','Last Action Date','Advance Date']
                for col in dateFields:
                    dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
                fullPath = excelWriteFunc.excelFileWriter(
                    dataframe, "Export_AllAdvance", "Advances"
                )
                return send_file(fullPath)
            else:
                return respond(
                    {
                        "status": 400,
                        "msg": f"No Data Found",
                        "icon": "error",
                    }
                )
        else:
            arr = arpp1+arpp4+arrp5+[
                
                {
                '$addFields': {
                    'L1ids': {
                        '$arrayElemAt': [
                            '$L1Approver.actionBy', 0
                        ]
                    }, 
                    'L2ids': {
                        '$arrayElemAt': [
                            '$L2Approver.actionBy', 0
                        ]
                    }, 
                    'L3ids': {
                        '$arrayElemAt': [
                            '$L3Approver.actionBy', 0
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'ids': [
                        {
                            '$ifNull': [
                                '$L1ids', None
                            ]
                        }, {
                            '$ifNull': [
                                '$L2ids', None
                            ]
                        }, {
                            '$ifNull': [
                                '$L3ids', None
                            ]
                        }
                    ]
                }
            }, {
                '$match': {
                    'ids': ObjectId(current_user['userUniqueId'])
                }
            }, {
                '$project': {
                    'ids': 0, 
                    'L1ids': 0, 
                    'L2ids': 0, 
                    'L3ids': 0
                }
            },
                
                    {
                        '$lookup': {
                            'from': 'userRegister', 
                            'localField': 'addedFor', 
                            'foreignField': '_id', 
                            'as': 'userInfo'
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
                                            }, {
                                                '$project': {
                                                    '_id': 0, 
                                                    'circleName': 1, 
                                                    'circleCode': 1
                                                }
                                            }
                                        ], 
                                        'as': 'circleResult'
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$circleResult', 
                                        'preserveNullAndEmptyArrays': True
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
                        '$addFields': {
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
                        '$addFields': {
                            'CreatedAt': {
                                '$toDate': '$CreatedAt'
                            }, 
                            'actionAt': {
                                '$toDate': '$actionAt'
                            }, 
                            'empName': '$userInfo.empName', 
                            'empCode': '$userInfo.empCode', 
                            'mobile': '$userInfo.mobile'
                        }
                    }]
            arr=arr+arpp3+[
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
                                                            'then': 'Jan'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 2
                                                                ]
                                                            }, 
                                                            'then': 'Feb'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 3
                                                                ]
                                                            }, 
                                                            'then': 'Mar'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 4
                                                                ]
                                                            }, 
                                                            'then': 'Apr'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 5
                                                                ]
                                                            }, 
                                                            'then': 'May'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 6
                                                                ]
                                                            }, 
                                                            'then': 'Jun'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 7
                                                                ]
                                                            }, 
                                                            'then': 'Jul'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 8
                                                                ]
                                                            }, 
                                                            'then': 'Aug'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 9
                                                                ]
                                                            }, 
                                                            'then': 'Sep'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 10
                                                                ]
                                                            }, 
                                                            'then': 'Oct'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 11
                                                                ]
                                                            }, 
                                                            'then': 'Nov'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 12
                                                                ]
                                                            }, 
                                                            'then': 'Dec'
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
                                                            'then': 'Feb'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 2
                                                                ]
                                                            }, 
                                                            'then': 'Mar'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 3
                                                                ]
                                                            }, 
                                                            'then': 'Apr'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 4
                                                                ]
                                                            }, 
                                                            'then': 'May'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 5
                                                                ]
                                                            }, 
                                                            'then': 'Jun'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 6
                                                                ]
                                                            }, 
                                                            'then': 'Jul'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 7
                                                                ]
                                                            }, 
                                                            'then': 'Aug'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 8
                                                                ]
                                                            }, 
                                                            'then': 'Sep'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 9
                                                                ]
                                                            }, 
                                                            'then': 'Oct'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 10
                                                                ]
                                                            }, 
                                                            'then': 'Nov'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 11
                                                                ]
                                                            }, 
                                                            'then': 'Dec'
                                                        }, {
                                                            'case': {
                                                                '$eq': [
                                                                    {
                                                                        '$month': '$actionAtKolkata'
                                                                    }, 12
                                                                ]
                                                            }, 
                                                            'then': 'Jan'
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
                                '$year': '$actionAt'
                            }
                        }
                    }, {
                        '$addFields': {
                            'year': {
                                '$toString': '$year'
                            }
                        }
                    }]
            arr=arr+arrp6+arrp7+[{
                        '$lookup': {
                            'from': 'claimType', 
                            'localField': 'advanceTypeId', 
                            'foreignField': '_id', 
                            'as': 'AdvanceTypeResult'
                        }
                    }, {
                        '$unwind': {
                            'path': '$AdvanceTypeResult', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$addFields': {
                            'advanceType': '$AdvanceTypeResult.claimType', 
                            'AdvanceDate': {
                                '$dateToString': {
                                    'format': '%Y-%m-%d', 
                                    'date': '$CreatedAt'
                                }
                            }, 
                            'ActionAt': {
                                '$dateToString': {
                                    'format': '%Y-%m-%d', 
                                    'date': '$actionAt'
                                }
                            }, 
                            'Month': {
                                '$concat': [
                                    '$month', '-', '$year'
                                ]
                            }, 
                            'circleCode': '$projectResult.circleResult.circleCode', 
                            'circleName': '$projectResult.circleResult.circleName', 
                            'projectIdName': '$projectResult.projectId', 
                            'costcenter': '$projectResult.costcenter'
                        }
                    }, {
                        '$project': {
                            'Advance Month': '$Month', 
                            'Employee Name': '$empName', 
                            'Employee Code': '$empCode', 
                            'Contact Number': '$mobile', 
                            'Advance number': '$AdvanceNo', 
                            'Advance Date': '$AdvanceDate', 
                            'Advance Type': '$advanceType', 
                            'Circle': '$circleName', 
                            'Project ID': '$projectIdName', 
                            'Cost Center': '$costcenter', 
                            'Amount': '$Amount', 
                            'Submission Date': '$AdvanceDate', 
                            'Approved Amount': '$ApprovedAmount', 
                            'Last Action Date': '$ActionAt', 
                            'L1 Status': '$L1ApproverStatus', 
                            'L2 Status': '$L2ApproverStatus', 
                            'L3 Status': '$L3ApproverStatus', 
                            'L1 Approver': '$L1ApproverResult.empName', 
                            'L2 Approver': '$L2ApproverResult.empName', 
                            'L3 Approver': '$L3ApproverResult.empName', 
                            'Remarks': '$remark', 
                            '_id': 0
                        }
                    }
                ]
            # print("ghjkllkjhghjkl", arr)
            response = cmo.finding_aggregate("Advance", arr)
            if len(response["data"]):
                response = response["data"]

                dataframe = pd.DataFrame(response)
                dateFields=['Submission Date','Last Action Date','Advance Date']
                for col in dateFields:
                    dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
                # for col in dataframe.columns:
                #     dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
                fullPath = excelWriteFunc.excelFileWriter(
                    dataframe, "Export_AllAdvance", "Advances"
                )
                return send_file(fullPath)
            else:
                return respond(
                    {
                        "status": 400,
                        "msg": f"No Data Found",
                        "icon": "error",
                    }
                )



@approval_blueprint.route("/export/userAdvances", methods=["GET", "POST"])
@token_required
def userAdvancesExport(current_user, id=None):
    if request.method == "GET":
        # AdvanceNo = request.args.get("AdvanceNo")
        # filterstatus = request.args.get('status')
        # claimType = request.args.get('claimType')
        # empName = request.args.get('empName')
        # empCode = request.args.get('empCode')
        # month=request.args.get("month")
        # year=request.args.get("year")
        # if year in ['',None,'undefined']:
        #     year = str(datetime.now().year)
        # arpp1=[]
        # arpp2=[]
        # arpp3=[]
        # arpp4=[]
        # arrp5=[]
        # arrp6=[]
        # arrp7=[]
        # if filterstatus not in ['',None,'undefined']:
        #     arpp1=[{
        #         '$match':{
        #             "status":filterstatus
        #         }
        #     }]
        # if empName not in ['',None,'undefined']:
        #     arpp2=[{
        #         '$match':{
        #             "empName":{
        #                 '$regex':empName.strip(),
        #                 '$options':'i'
        #             }
        #         }
        #     }]
        # if empCode not in ['',None,'undefined']:
        #     arpp3=[{
        #         '$match':{
        #             "empCode":{
        #                 '$regex':empCode.strip(),
        #                 '$options':'i'
        #             }
        #         }
        #     }]
        # if claimType not in ['',None,'undefined']:
        #     arrp5=[{
        #         '$match':{
        #             "advanceTypeId":ObjectId(claimType)
        #         }
        #     }]       
        # if AdvanceNo not in ['',None,'undefined']:
            
            
        #     arpp4=[{
        #         '$match':{
        #             "AdvanceNo":{
        #                 '$regex':AdvanceNo.strip(),
        #                 '$options':'i'
        #             }
        #         }
        #     }]
        # if month not in ['',None,'undefined']:
        #     arrp6=[{
        #         '$match':{
        #             "month":{
        #                 '$regex':month.strip(),
        #                 '$options':'i'
        #             },
        #             'status':"L3-Approved"
        #         }
        #     }]
        # if year not in ['',None,'undefined']:
        #     arrp7=[{
        #         '$match':{
        #             "year":year,
        #              'status':"L3-Approved"
        #         }
        #     }]
        arr =[
            {
                '$match': {
                    'addedFor': ObjectId(current_user["userUniqueId"])
                }
            },
            {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'addedFor', 
                    'foreignField': '_id', 
                    'as': 'userInfo'
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
                        },
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
                                'customerName': {
                                    '$toObjectId': "$custId"
                                    } ,
                                'circle': {
                                    '$toObjectId': '$circle'
                                }
                            }
                        }, 
                        {
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
                                            '_id': 0, 
                                            'circleName': 1, 
                                            'circleCode': 1
                                        }
                                    }
                                ], 
                                'as': 'circleResult'
                            }
                        }, 
                        {
                            '$unwind': {
                                'path': '$circleResult', 
                                'preserveNullAndEmptyArrays': True
                            }
                        },
                         {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerName', 
                                    'foreignField': '_id', 
                                    'as': 'customerName'
                                }
                            }, {
                                '$addFields': {
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerName.customerName', 0
                                        ]
                                    }
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
                '$addFields': {
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
                '$addFields': {
                    'CreatedAt': {
                        '$toDate': '$CreatedAt'
                    }, 
                    'actionAt': {
                        '$toDate': '$actionAt'
                    }, 
                    'empName': '$userInfo.empName', 
                    'empCode': '$userInfo.empCode', 
                    'mobile': '$userInfo.mobile', 
                    'ustCode': '$userInfo.ustCode', 
                    'bankName': '$userInfo.bankName', 
                    'accountNumber': '$userInfo.accountNumber', 
                    'ifscCode': '$userInfo.ifscCode', 
                    'benificiaryname': '$userInfo.benificiaryname'
                }
            },
        {
                '$addFields': {
                    'accountNumber':{
                        '$toString':"$accountNumber"
                    },
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
                                                    'then': 'Jan'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 2
                                                        ]
                                                    }, 
                                                    'then': 'Feb'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 3
                                                        ]
                                                    }, 
                                                    'then': 'Mar'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 4
                                                        ]
                                                    }, 
                                                    'then': 'Apr'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 5
                                                        ]
                                                    }, 
                                                    'then': 'May'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 6
                                                        ]
                                                    }, 
                                                    'then': 'Jun'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 7
                                                        ]
                                                    }, 
                                                    'then': 'Jul'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 8
                                                        ]
                                                    }, 
                                                    'then': 'Aug'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 9
                                                        ]
                                                    }, 
                                                    'then': 'Sep'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 10
                                                        ]
                                                    }, 
                                                    'then': 'Oct'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 11
                                                        ]
                                                    }, 
                                                    'then': 'Nov'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 12
                                                        ]
                                                    }, 
                                                    'then': 'Dec'
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
                                                    'then': 'Feb'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 2
                                                        ]
                                                    }, 
                                                    'then': 'Mar'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 3
                                                        ]
                                                    }, 
                                                    'then': 'Apr'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 4
                                                        ]
                                                    }, 
                                                    'then': 'May'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 5
                                                        ]
                                                    }, 
                                                    'then': 'Jun'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 6
                                                        ]
                                                    }, 
                                                    'then': 'Jul'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 7
                                                        ]
                                                    }, 
                                                    'then': 'Aug'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 8
                                                        ]
                                                    }, 
                                                    'then': 'Sep'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 9
                                                        ]
                                                    }, 
                                                    'then': 'Oct'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 10
                                                        ]
                                                    }, 
                                                    'then': 'Nov'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 11
                                                        ]
                                                    }, 
                                                    'then': 'Dec'
                                                }, {
                                                    'case': {
                                                        '$eq': [
                                                            {
                                                                '$month': '$actionAtKolkata'
                                                            }, 12
                                                        ]
                                                    }, 
                                                    'then': 'Jan'
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
                        '$year': '$actionAt'
                    }
                }
            }, {
                '$addFields': {
                    'year': {
                        '$toString': '$year'
                    }
                }
            },
        
            {
                '$lookup': {
                    'from': 'claimType', 
                    'localField': 'advanceTypeId', 
                    'foreignField': '_id', 
                    'as': 'AdvanceTypeResult'
                }
            }, {
                '$unwind': {
                    'path': '$AdvanceTypeResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'advanceType': '$AdvanceTypeResult.claimType', 
                    'AdvanceDate': {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': '$CreatedAt'
                        }
                    }, 
                    'ActionAt': {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': '$actionAt'
                        }
                    }, 
                    'Month': {
                        '$concat': [
                            '$month', '-', '$year'
                        ]
                    }, 
                    'circleCode': '$projectResult.circleResult.circleCode', 
                    'customerName': '$projectResult.customerName', 
                    'circleName': '$projectResult.circleResult.circleName', 
                    'projectIdName': '$projectResult.projectId', 
                    'costcenter': '$projectResult.costcenter'
                }
            }, {
                '$project': {
                    'Advance Month': '$Month', 
                    'Employee Name': '$empName', 
                    'Employee Code': '$empCode', 
                    'UST Employee Code': '$ustCode', 
                    'Contact Number': '$mobile', 
                    'Advance number': '$AdvanceNo', 
                    'Advance Date': '$AdvanceDate', 
                    'Advance Type': '$advanceType', 
                    'Customer':'$customerName',
                    'Circle': '$circleName', 
                    'Project ID': '$projectIdName', 
                    
                    'Cost Center': '$costcenter', 
                    'Amount': '$Amount', 
                    'Submission Date': '$AdvanceDate', 
                    'Approved Amount': '$ApprovedAmount', 
                    'Last Action Date': '$ActionAt', 
                    'Current Status': '$status', 
                    'L1 Status': '$L1ApproverStatus', 
                    'L2 Status': '$L2ApproverStatus', 
                    'L3 Status': '$L3ApproverStatus', 
                    'L1 Approver': '$L1ApproverResult.empName', 
                    'L2 Approver': '$L2ApproverResult.empName', 
                    'L3 Approver': '$L3ApproverResult.empName', 
                    'Bank Name': '$bankName', 
                    'Bank Account Number': '$accountNumber', 
                    'IFSC Code': '$ifscCode', 
                    'Benificiary Name': '$benificiaryname', 
                    'Additional Info': '$additionalInfo',
                    'Remarks': '$remark', 
                    '_id': 0
                }
            }
        ]
        # print('diididi',arr)
        response = cmo.finding_aggregate("Advance", arr)
        # print('jdjdjdjdjjd',response)
        if len(response["data"]):
            response = response["data"]
            dataframe = pd.DataFrame(response)
            dateFields=['Submission Date','Last Action Date','Advance Date']
            for col in dateFields:
                dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_AllAdvance", "Advances"
            )
            return send_file(fullPath)
        else:
            return respond(
                {
                    "status": 400,
                    "msg": f"No Data Found",
                    "icon": "error",
                }
            )
    
    
@approval_blueprint.route("/Approval/Logs", methods=["GET", "POST"])
@token_required
def ApprovalLogs(current_user):
    if request.method== "GET":
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }]
        
        arr=arr+[
        {
        '$lookup': {
            'from': 'userRegister', 
            'localField': 'userId', 
            'foreignField': '_id', 
            'pipeline': [
                {
                    '$project': {
                        'empName': 1, 
                        'email': 1, 
                        'ustCode': 1, 
                        'empCode': 1, 
                        '_id': 0
                    }
                }
            ], 
            'as': 'userResults'
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
                        'email': 1, 
                        'ustCode': 1, 
                        'empCode': 1, 
                        '_id': 0
                    }
                }
            ], 
            'as': 'ApproverResults'
        }
    }, {
        '$unwind': {
            'path': '$userResults', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$unwind': {
            'path': '$ApproverResults', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'actionAt': {
                '$toDate': '$actionAt'
            }, 
            'Number': {
                '$cond': {
                    'if': {
                        '$eq': [
                            '$type', 'Advance'
                        ]
                    }, 
                    'then': '$AdvanceNo', 
                    'else': '$ExpenseNo'
                }
            }, 
            'action': '$status', 
            'actionBy': '$ApproverResults.empName', 
            'empName': '$userResults.empName'
        }
    }, {
        '$sort': {
            '_id': -1
        }
    }, {
        '$addFields': {
            'actionAt': {
                '$dateToString': {
                    'format': '%d-%m-%Y %H:%M:%S', 
                    'date': '$actionAt'
                }
            }
        }
    }, {
        '$project': {
            'uniqueId': {
                '$toString': '$_id'
            }, 
            'actionBy': 1, 
            'Number': 1, 
            'actionAt': 1, 
            'Amount': 1, 
            'ApprovedAmount': 1, 
            '_id': 0, 
            'action': 1, 
            'empName': 1, 
            
        }
    }, {
        '$sort': {
            'uniqueId': -1
        }
    }
]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        Response=cmo.finding_aggregate("ApprovalLogs",arr)
        return respond(Response)
  
  
  
   
@approval_blueprint.route("/export/currentBalance",methods=['GET','POST'])
def exportcurrentBalance():
    arrt=[
            {
                '$match': {
                    'type': {
                        '$ne': 'Partner'
                    }, 
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }, {
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
                    'from': 'Settlement', 
                    'localField': '_id', 
                    'foreignField': 'empID', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$addFields': {
                                'type': 'Settlement', 
                                'name': '$SettlementID', 
                                'ApprovedAmount': '$Amount', 
                                'customisedStatus': 6, 
                                'status': 'L3-Approved', 
                                'customStatus': 'L3-Approved', 
                                'uniqueId': {
                                    '$toString': '$_id'
                                }, 
                                'empID': {
                                    '$toString': '$empID'
                                }
                            }
                        }
                    ], 
                    'as': 'SettlementResults'
                }
            }, {
                '$addFields': {
                    'newData': {
                        '$concatArrays': [
                            '$AdvanceResults', '$ExpenseDetails', '$SettlementResults'
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
                    'uniqueId': '$newData.uniqueId'
                }
            }, {
                '$project': {
                    'name': '$newData.name', 
                    'ApprovedAmount': '$newData.ApprovedAmount', 
                    'customStatus': '$newData.customStatus', 
                    'uniqueId': 1, 
                    'customisedStatus': '$newData.customisedStatus', 
                    'type': '$newData.type', 
                    'empCode': 1, 
                    'email': 1
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
                    }
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
                    'empID': {
                        '$toObjectId': '$_id'
                    }
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
                '$project': {
                    'empID': 0
                }
            }, {
                '$sort': {
                    'AddedAt': -1
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
                                            '$ne': [
                                                '$type', 'Settlement'
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
                    'SettleAmountTotal': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$type', 'Settlement'
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
                    'AdvanceAmountTotal': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    '$Openingbalance', 0
                                ]
                            }, 
                            'then': {
                                '$add': [
                                    '$AdvanceAmountTotal', '$SettleAmountTotal'
                                ]
                            }, 
                            'else': '$AdvanceAmountTotal'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'data': {
                        '$sortArray': {
                            'input': '$data', 
                            'sortBy': {
                                'AddedAt': -1
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
                '$addFields': {
                    'email': {
                        '$arrayElemAt': [
                            '$data.email', 0
                        ]
                    }, 
                    'empCode': {
                        '$arrayElemAt': [
                            '$data.empCode', 0
                        ]
                    }, 
                    'currentBalance': '$finalAmount'
                }
            }, {
                '$project': {
                    'AdvanceAmountTotal1': 0, 
                    'ExpenseAmountTotal1': 0, 
                    'data': 0, 
                    '_id': 0, 
                    'finalAmount': 0
                }
            }
        ]
    Response=cmo.finding_aggregate("userRegister",arrt)
    dataframe = pd.DataFrame(Response) 
    fullPath = excelWriteFunc.excelFileWriter(
        dataframe, "Export_CurrentBalance", "CurrentBalance"
    )
    return send_file(fullPath)


    
    
@approval_blueprint.route("/export/ApprovalLogs",methods=['GET','POST'])
@token_required
def exportApprovalLogs(current_user):
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
                'localField': 'userId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$project': {
                            'empName': 1, 
                            'email': 1, 
                            'ustCode': 1, 
                            'empCode': 1, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'userResults'
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
                            'email': 1, 
                            'ustCode': 1, 
                            'empCode': 1, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'ApproverResults'
            }
        }, {
            '$unwind': {
                'path': '$userResults', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$unwind': {
                'path': '$ApproverResults', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'actionAt': {
                    '$toDate': '$actionAt'
                }, 
                'Number': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$type', 'Advance'
                            ]
                        }, 
                        'then': '$AdvanceNo', 
                        'else': '$ExpenseNo'
                    }
                }, 
                'action': '$status', 
                'actionBy': '$ApproverResults.empName', 
                'empName': '$userResults.empName'
            }
        }, {
            '$addFields': {
                'actionAt': {
                    '$dateToString': {
                        'format': '%d-%m-%Y %H:%M:%S', 
                        'date': '$actionAt'
                    }
                }
            }
        }, 
        {
        '$sort': {
            '_id': -1
        }
    },{
            '$project': {
                'uniqueId': {
                    '$toString': '$_id'
                }, 
                'actionBy': 1, 
                'Number': 1, 
                'actionAt': 1, 
                'Amount': 1, 
                'ApprovedAmount': 1, 
                '_id': 0, 
                'action': 1, 
                'empName': 1
            }
        }, {
            '$project': {
                'Number': '$Number', 
                'Action': '$action', 
                'Action By': '$actionBy', 
                'Employee Name': '$empName', 
                 "Action At":"$actionAt",
                'Amount': '$Amount', 
                'Approved Amount': '$ApprovedAmount'
            }
        }
    ]
    response=cmo.finding_aggregate("ApprovalLogs",arr)
    if len(response["data"]):
            response = response["data"]
            dataframe = pd.DataFrame(response)
            # dateFields=['Action At']
            # for col in dateFields:
            #     dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_ApprovalLogs", "ApprovalLogs"
            )
            return send_file(fullPath)
    else:
        return respond(
            {
                "status": 400,
                "msg": f"No Data Found",
                "icon": "error",
            }
        )