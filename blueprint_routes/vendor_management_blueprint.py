from base import *
from datetime import datetime as dtt
vendor_management_blueprint = Blueprint("vendor_management_blueprint", __name__)


def current_year():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_year = dtt.now(kolkata_tz).year
    return current_year

def current_month():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    current_month = dtt.now(kolkata_tz).month
    return current_month



def getCustomMonthYear():
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    today = dtt.now(kolkata_tz)
    if today.day >= 26:
        if today.month == 12:
            next_month = 1
            year = today.year + 1
        else:
            next_month = today.month + 1
            year = today.year
    else:
        next_month = today.month
        year = today.year
    if today.day >= 26:
        days_spent = today.day - 25
    else:
        previous_month = today.replace(
            month=(today.month-1) if today.month > 1 else 12)
        days_spent = today.day
    if next_month == 1:
        total_days_in_custom_month = 31
    else:
        from calendar import monthrange
        if today.month == 12:
            total_days_in_custom_month = monthrange(
                year, next_month-1)[1] - 25 + 25
        else:
            total_days_in_custom_month = monthrange(year, next_month)[1] + 25
    return next_month, year, days_spent, total_days_in_custom_month





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




def current_time():
    ist = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time


@vendor_management_blueprint.route("/vendorDetails", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@vendor_management_blueprint.route("/vendorDetails/<id>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def vendordetails(current_user, id=None):

    if request.method == "GET":
        arra = [
            {
                "$lookup": {
                    "from": "userRole",
                    "localField": "userRole",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"roleName": "Vendor", "deleteStatus": {"$ne": 1}}}
                    ],
                    "as": "result",
                }
            },
            {"$addFields": {"length": {"$size": "$result"}}},
            {"$match": {"length": {"$ne": 0}}},
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "_id": {"$toString": "$_id"},
                    "userRole": {"$toString": "$userRole"},
                }
            },
            {"$project": {"result": 0, "length": 0}},
        ]
        response = cmo.finding_aggregate("userRegister", arra)
        return respond(response)

    elif request.method == "POST":
        formData = request.form
        allData = {}
        for i in formData:
            allData[i] = formData[i]
        arra = [{"$match": {"roleName": "Vendor"}}, {"$project": {"_id": 1}}]
        response = cmo.finding_aggregate("userRole", arra)
        roleId = response["data"][0]["_id"]
        allData["userRole"] = roleId
        response = cmo.insertion("userRegister", allData)
        return respond(response)

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting(
                "userRegister", id, current_user["userUniqueId"])
            return respond(response)
        else:
            return {
                "status": 400,
                "icon": "eroor",
                "msg": "Please Provide A Valid Unique Id",
            }, 400


@vendor_management_blueprint.route("/vendorProjectAllocation", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@vendor_management_blueprint.route("/vendorProjectAllocation/<id>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def vendorprojectallocation(current_user, id=None):
    if request.method == "GET":

        arra = [
            {"$match": {"type": "Partner", "status": "Active"}},
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "vendorCode": {"$toString": "$vendorCode"},
                }
            },
            {
                "$addFields": {
                    "vendor": {"$concat": ["$vendorName", "(", "$vendorCode", ")"]}
                }
            },
        ]
        if request.args.get("vendor") != None and request.args.get("vendor") != "":
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
        arra = arra + [
            {"$project": {"vendor": 1, "uniqueId": 1, "_id": 0}},
            {
                "$lookup": {
                    "from": "projectAllocation",
                    "localField": "uniqueId",
                    "foreignField": "empId",
                    "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                    "as": "result",
                }
            },
            {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
            {"$addFields": {"projectIds": "$result.projectIds"}},
            {
                "$lookup": {
                    "from": "project",
                    "localField": "projectIds",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": 0, "projectId": 1}},
                    ],
                    "as": "projectIdName",
                }
            },
            {"$addFields": {"projectIdName": "$projectIdName.projectId"}},
            {
                "$addFields": {
                    "projectIdName": {
                        "$reduce": {
                            "input": "$projectIdName",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [
                                        {"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "project": {
                        "$map": {
                            "input": "$projectIds",
                            "as": "objectId",
                            "in": {"$toString": "$$objectId"},
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "project": {
                        "$reduce": {
                            "input": "$project",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    {"$cond": [
                                        {"$eq": ["$$value", ""]}, "", ","]},
                                    "$$this",
                                ]
                            },
                        }
                    }
                }
            },
            {"$project": {"_id": 0, "result": 0, "projectIds": 0}},
        ]
        if request.args.get("project") != None and request.args.get("project") != "":
            arra = arra + [
                {
                    "$match": {
                        "projectIdName": {
                            "$regex": request.args.get("project"),
                            "$options": "i",
                        }
                    }
                }
            ]

        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("userRegister", arra)
        return respond(response)

    elif request.method == "POST":
        if id != None:
            project = request.json.get("project")
            value = []
            for i in project.split(","):
                value.append(ObjectId(i))
            allData = {"projectIds": value, "empId": id, "type": "Partner"}
            updateBy = {"empId": id, "type": "Partner"}

            try:
                Message = None
                AssignedProjects = ""
                artt = [
                    {
                        "$match": {
                            "deleteStatus": {"$ne": 1},
                            "_id": {"$in": allData["projectIds"]},
                        }
                    },
                    {"$project": {"_id": 1, "projectId": 1}},
                ]
                responsedData = cmo.finding_aggregate("project", artt)["data"]
                # changedThings=changedThings2(allData,allData2)
                arty = [
                    {"$match": {"_id": ObjectId(
                        id), "deleteStatus": {"$ne": 1}}},
                    {"$project": {"empCode": 1, "empName": 1, "_id": 0}},
                ]
                userDetails = cmo.finding_aggregate(
                    "userRegister", arty)["data"]
                for i in responsedData:
                    AssignedProjects = AssignedProjects + i["projectId"] + ","

                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added = (
                    " these projects allocated to "
                    + userDetails[0]["empName"]
                    + ":-"
                    + AssignedProjects
                )
                cmo.insertion(
                    "AdminLogs",
                    {
                        "type": "Update",
                        "module": "Partner Project Allocation",
                        "actionAt": current_time(),
                        "actionBy": ObjectId(current_user["userUniqueId"]),
                        "action": Added,
                    },
                )
            except Exception as e:
                print(e, "dhhdhdhhdh")

            response = cmo.updating(
                "projectAllocation", updateBy, allData, True)
            return respond(response)


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



@vendor_management_blueprint.route("/vendor/myTask", methods=["GET"])
@token_required
def vendor_mytask(current_user):
    taskStatus = ["Open", "In Process", "Submit", "Approve", "Reject","Submit to Airtel", "Closed"]
    subProjectArray = (sub_project())[0]["uid"]
    year = current_year()
    months = [current_month()]
    if request.args.get("year"):
        year = request.args.get("year")
    if request.args.get("Month"):
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
          
    if not start and not end:
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
    arra = (arra+ apireq.countarra("milestone", arra) + apireq.args_pagination(request.args))
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
                                            '$eq': [
                                                '$$item.milestoneArray.mileStoneStatus', 'Closed'
                                            ]
                                        }
                                    }
                                }
                            }, {"$size" : "$items"}
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
                    '$itemCodeResults.itemCode', 0
                ]
            },
            'vendorRate': {
                '$arrayElemAt': [
                    '$itemCodeResults.rate', 0
                ]
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
                            '$itemCodeResults.rate', 0
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
                            '$itemCodeResults.itemCode', 0
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
        '$project': {
            'projectArray': 0, 
            'SubTypeResult': 0
        }
    },
        {"$sort": {"_id": 1}},
    ]
    response = cmo.finding_aggregate("milestone", arra)
    return respond(response)


@vendor_management_blueprint.route("/filter/vendor/subProject", methods=["GET", "POST"])
@vendor_management_blueprint.route("/filter/vendor/subProject/<uid>", methods=["GET", "POST"])
@token_required
def filter_vendor_subProject(current_user, uid=None):
    type = request.args.get("type")
    customer = request.args.get("customer")
    if type and type == "All":
        ary = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "custId": "667d593927f39f1ac03d7863",
                }
            },
            {
                "$project": {
                    "subProjectId": {"$toString": "$_id"},
                    "projectTypeId": {"$toString": "$_id"},
                    "subProject": 1,
                    "projectType": 1,
                    "_id": 0,
                }
            },
        ]
        Response = cmo.finding_aggregate("projectType", ary)
        return respond(Response)
    arra = [
        {
            "$match": {
                "assignerId": ObjectId(current_user["userUniqueId"]),
                "deleteStatus": {"$ne": 1},
            }
        },
        {"$addFields": {"SubProjectId": {"$toObjectId": "$SubProjectId"}}},
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
                "subprojectName": "$result.subProject",
                "SubProjectId": {"$toString": "$SubProjectId"},
            }
        },
        {
            "$group": {
                "_id": "$subprojectName",
                "subprojectName": {"$first": "$subprojectName"},
                "subProjectId": {"$first": "$SubProjectId"},
            }
        },
        {"$match": {"_id": {"$ne": None}}},
        {"$project": {"_id": 0}},
    ]
    response = cmo.finding_aggregate("milestone", arra)
    return respond(response)

@vendor_management_blueprint.route("/filter/vendorActivity/projectType", methods=["GET", "POST"])
@token_required
def filter_work_done_projectType(current_user):
    arra = sub_project()
    return respond({"status": 200, "msg": "Data Updated successfully", "data": arra})


@vendor_management_blueprint.route("/vendor/milestone", methods=["GET", "POST"])
@vendor_management_blueprint.route("/vendor/milestone/<id>", methods=["GET", "POST"])
@token_required
def vendor_milestone(current_user, id=None):
    if request.method == "GET":
        # aggr = [
        #         {
        #             '$match': {
        #                 'type': 'Partner'
        #             }
        #         }, {
        #             '$unwind': {
        #                 'path': '$projectIds'
        #             }
        #         }, {
        #             '$lookup': {
        #                 'from': 'project',
        #                 'localField': 'projectIds',
        #                 'foreignField': '_id',
        #                 'as': 'project'
        #             }
        #         }, {
        #             '$addFields': {
        #                 'empId': {
        #                     '$toObjectId': '$empId'
        #                 },
        #                 'projectId': {
        #                     '$arrayElemAt': [
        #                         '$project.projectId', 0
        #                     ]
        #                 },
        #                 'projectType': {
        #                     '$arrayElemAt': [
        #                         '$project.projectType', 0
        #                     ]
        #                 },
        #                 'projectGroup': {
        #                     '$arrayElemAt': [
        #                         '$project.projectGroup', 0
        #                     ]
        #                 },
        #                 'custId': {
        #                     '$arrayElemAt': [
        #                         '$project.custId', 0
        #                     ]
        #                 }
        #             }
        #         }, {
        #             '$match': {
        #                 'project': {
        #                     '$ne': []
        #                 }
        #             }
        #         }, {
        #             '$lookup': {
        #                 'from': 'userRegister',
        #                 'localField': 'empId',
        #                 'foreignField': '_id',
        #                 'as': 'userResults'
        #             }
        #         }, {
        #             '$addFields': {
        #                 'vendorId': {
        #                     '$toString': '$empId'
        #                 },
        #                 'vendorName': {
        #                     '$arrayElemAt': [
        #                         '$userResults.vendorName', 0
        #                     ]
        #                 },
        #                 'vendorCode': {
        #                     '$arrayElemAt': [
        #                         '$userResults.vendorCode', 0
        #                     ]
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
        #                             'from': 'customer',
        #                             'localField': 'customerId',
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
        #                             'as': 'customer'
        #                         }
        #                     }, {
        #                         '$unwind': {
        #                             'path': '$customer',
        #                             'preserveNullAndEmptyArrays': True
        #                         }
        #                     }, {
        #                         '$lookup': {
        #                             'from': 'zone',
        #                             'localField': 'zoneId',
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
        #                             'as': 'zone'
        #                         }
        #                     }, {
        #                         '$unwind': {
        #                             'path': '$zone',
        #                             'preserveNullAndEmptyArrays': True
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
        #                             'as': 'costCenter'
        #                         }
        #                     }, {
        #                         '$unwind': {
        #                             'path': '$costCenter',
        #                             'preserveNullAndEmptyArrays': True
        #                         }
        #                     }, {
        #                         '$addFields': {
        #                             'costCenter': '$costCenter.costCenter',
        #                             'zone': '$zone.shortCode',
        #                             'customer': '$customer.shortName',
        #                             'Customer': '$customer.customerName'
        #                         }
        #                     }, {
        #                         '$addFields': {
        #                             'projectGroupName': {
        #                                 '$concat': [
        #                                     '$customer', '-', '$zone', '-', '$costCenter'
        #                                 ]
        #                             },
        #                             'projectGroup': {
        #                                 '$toString': '$_id'
        #                             }
        #                         }
        #                     }, {
        #                         '$project': {
        #                             '_id': 0,
        #                             'projectGroupName': 1,
        #                             'projectGroup': 1,
        #                             'Customer': 1,
        #                             'costCenter': 1
        #                         }
        #                     }
        #                 ],

        #                 'as': 'projectGroupResult'
        #             }
        #         }, {
        #             '$lookup': {
        #                 'from': 'projectType',
        #                 'localField': 'projectType',
        #                 'foreignField': '_id',
        #                 'pipeline': [
        #                     {
        #                         '$lookup': {
        #                             'from': 'projectType',
        #                             'localField': 'projectType',
        #                             'foreignField': 'projectType',
        #                             'as': 'AllprojectTypeResult'
        #                         }
        #                     }, {
        #                         '$unwind': {
        #                             'path': '$AllprojectTypeResult'
        #                         }
        #                     }, {
        #                         '$replaceRoot': {
        #                             'newRoot': '$AllprojectTypeResult'
        #                         }
        #                     }
        #                 ],
        #                 'as': 'projectTypeResult'
        #             }
        #         }, {
        #             '$unwind': {
        #                 'path': '$projectTypeResult'
        #             }
        #         }, {
        #             '$project': {
        #                 '_id': 0,
        #                 'empId': {
        #                     '$toString': '$empId'
        #                 },
        #                 'vendorId': 1,
        #                 'vendorName': 1,
        #                 'vendorCode': 1,
        #                 'uniqueId': {
        #                     '$toString': '$_id'
        #                 },
        #                 'projectGroup': {
        #                     '$toString': '$projectGroup'
        #                 },
        #                 'SubProjectId': {
        #                     '$toString': '$projectTypeResult._id'
        #                 },
        #                 'subProject': '$projectTypeResult.subProject',
        #                 'projectTypeName': '$projectTypeResult.projectType',
        #                 'projectTypeId': {
        #                     '$toString': '$projectType'
        #                 },
        #                 'custId': 1,
        #                 'costCenter': {
        #                     '$arrayElemAt': [
        #                         '$projectGroupResult.costCenter', 0
        #                     ]
        #                 },
        #                 'Customer': {
        #                     '$arrayElemAt': [
        #                         '$projectGroupResult.Customer', 0
        #                     ]
        #                 },
        #                 'projectGroupName': {
        #                     '$arrayElemAt': [
        #                         '$projectGroupResult.projectGroupName', 0
        #                     ]
        #                 }
        #             }
        #         }, {
        #             '$group': {
        #                 '_id': {
        #                     'empId': '$empId',
        #                     'projectId': '$projectId',
        #                     'SubProjectId': '$SubProjectId',
        #                     'projectGroup': '$projectGroup',
        #                     'custId': '$custId'
        #                 },
        #                 'data': {
        #                     '$first': '$$ROOT'
        #                 }
        #             }
        #         }, {
        #             '$replaceRoot': {
        #                 'newRoot': '$data'
        #             }
        #         }
        #     ]

        arty = [
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

        arty = arty+apireq.commonarra + apireq.args_pagination(request.args)
        fetchData = cmo.finding_aggregate("vendorCostMilestone", arty)
        return respond(fetchData)
    if request.method == "POST":
        if id == None:
            milestoneList = []
            reqData = request.get_json()
            if 'customerId' in reqData:
                try:
                    reqData['customerId'] = ObjectId(reqData['customerId'])
                except Exception as e:
                    reqData['customerId'] = reqData['customerId']
            if 'projectGroup' in reqData:
                try:
                    reqData['projectGroup'] = ObjectId(reqData['projectGroup'])
                except Exception as e:
                    reqData['projectGroup'] = reqData['projectGroup']
            if 'subProjectId' in reqData:
                try:
                    reqData['subProjectId'] = ObjectId(reqData['subProjectId'])
                except Exception as e:
                    reqData['subProjectId'] = reqData['subProjectId']
            if 'vendorId' in reqData:
                try:
                    reqData['vendorId'] = ObjectId(reqData['vendorId'])
                except Exception as e:
                    reqData['vendorId'] = reqData['vendorId']
            if 'milestone' in reqData:
                if reqData['milestone'] not in ['', None, 'undefined']:
                    milestoneList = reqData['milestone'].split(",")
            reqData['milestoneList'] = milestoneList
            try:
                artty = [
                    {
                        '$match': {
                            'customerId': reqData['customerId'],
                            'projectGroup': reqData['projectGroup'],
                            'vendorId': reqData['vendorId'],
                            'subProjectId': reqData['subProjectId'],
                            'itemCode': reqData['itemCode'],
                            'rate': reqData['rate'],
                            'GBPA': reqData['GBPA'],
                            'airtelItemCode': reqData['airtelItemCode'],
                            'activityName': reqData['activityName']
                        }
                    }
                ]
                Respone2 = cmo.finding_aggregate(
                    "vendorCostMilestone", artty)['data']
                if len(Respone2):
                    return respond({"msg": "Already Declared!", "status": 400, "icon": "error"})
            except Exception as e:
                print(e, 'dhghghghryh')
            Response = cmo.insertion("vendorCostMilestone", reqData)
            return respond(Response)
        if id != None:
            reqData = request.get_json()
            deletingKeys = ["milestone", "projectGroupName",
                            "projectType", "subProject", "vendorName", 'customer']
            for i in deletingKeys:
                if i in reqData:
                    print("hjbdbhbdhbdhb", i)
                    del reqData[i]
            Response = cmo.updating("vendorCostMilestone", {
                                    "_id": ObjectId(id)}, reqData, False)
            return respond(Response)

        # return respond({"msg": "Currently working", "status": 400, "icon": "error"})
    if request.method == "DELETE":
        if id != None:
            response = cmo.deleting(
                "vendorCostMilestone", id, current_user["userUniqueId"])
            return respond(response)
        else:
            return {
                "status": 400,
                "icon": "eroor",
                "msg": "Please Provide A Valid Unique Id",
            }, 400


@vendor_management_blueprint.route("/vendorCost/projectGroupList", methods=["GET"])
@token_required
def vendorprojectGroupList(current_user):
    customerId = request.args.get("customerId")
    if request.method == "GET":
        arrr = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    },
                    'customerId': ObjectId(customerId)
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
        ]
        Response = cmo.finding_aggregate("projectGroup", arrr)
        return respond(Response)


@vendor_management_blueprint.route("/vendorCost/projectTypeList", methods=["GET"])
@token_required
def vendorCostprojectTypeList(current_user):
    customerId = request.args.get("customerId")
    projectType = request.args.get("projectType")
    if request.method == "GET":
        artt = []
        if projectType and projectType not in ['', 'undefined', None]:
            artt = [{
                '$match': {
                    'projectType': projectType
                }
            },
                {
                '$match': {
                    'custId': customerId
                }
            }, {
                    '$project': {

                        'subProject': 1,

                        'subProjectId': {
                            '$toString': '$_id'
                        },
                        '_id': 0
                    }
            }
            ]
            Response = cmo.finding_aggregate("projectType", artt)
            return respond(Response)
        arrr = [
            {
                '$match': {
                    'custId': customerId
                }
            }, {
                '$group': {
                    '_id': '$projectType',
                    'data': {
                        '$first': '$$ROOT'
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$data'
                }
            }, {
                '$project': {
                    'projectType': 1,
                    'projectTypeId': {
                        '$toString': '$_id'
                    },
                    '_id': 0
                }
            }
        ]
        Response = cmo.finding_aggregate("projectType", arrr)
        return respond(Response)


@vendor_management_blueprint.route("/vendorCost/vendorsList", methods=["GET"])
@token_required
def vendorCostVendorList(current_user):
    # customerId=request.args.get("customerId")
    # projectType=request.args.get("projectType")
    if request.method == "GET":
        arrr = [
            {
                '$match': {
                    'type': 'Partner'
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
                    'as': 'vendorResults'
                }
            }, {
                '$unwind': {
                    'path': '$vendorResults'
                }
            }, {
                '$project': {
                    'vendorName': '$vendorResults.vendorName',
                    'vendorCode': '$vendorResults.vendorCode',
                    'vendorId': {
                        '$toString': '$empId'
                    },
                    '_id': 0
                }
            }
        ]
        Response = cmo.finding_aggregate("projectAllocation", arrr)
        return respond(Response)


@vendor_management_blueprint.route("/vendor/milestoneList", methods=["GET"])
@token_required
def vendor_milestoneList(current_user):
    allArgs = request.args
    aggr = [
        {
            '$match': {
                '_id': ObjectId(allArgs['SubProjectId']),
                'custId': '667d593927f39f1ac03d7863'
            }
        }, {
            '$project': {
                'MileStone': 1,
                'custId': 1
            }
        }, {
            '$unwind': {
                'path': '$MileStone'
            }
        }, {
            '$project': {
                'MileStone': '$MileStone.fieldName',
                'custId': 1,
                'subProject': {
                    '$toString': '$_id'
                },
                '_id': 0
            }
        }
    ]

    fetchData = cmo.finding_aggregate("projectType", aggr)
    return respond(fetchData)
        #  filepath=os.path.join("uploads",file.filename)



@vendor_management_blueprint.route("/vendor/ProjectType/<id>", methods=["GET"])
@token_required
def vendor_project_type_customer(current_user,id=None):
    arra = [
        {
            '$match': {
                'custId': id
            }
        }, {
            '$group': {
                '_id': '$projectType', 
                'uid': {
                    '$push': {
                        '$toString': '$_id'
                    }
                }
            }
        }, {
            '$project': {
                'projectType': '$_id', 
                'uid': 1, 
                '_id': 0
            }
        }, {
            '$sort': {
                'projectType': 1
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)
    return respond(response)