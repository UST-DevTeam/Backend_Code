from base import *
from common.config import unique_timestamp
import datetime
from datetime import datetime as dtt
import pytz
from blueprint_routes.currentuser_blueprint import projectId_str
from common.config import is_valid_mongodb_objectid as is_valid_mongodb_objectid
from common.mongo_operations import db as database
import zipfile, io, os

def current_time():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%d-%m-%Y")
    current_time = asia_now.strftime("%d-%m-%YT%H:%M:%S")
    return current_time

def current_time1():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%d-%m-%Y")
    return current_date

def new_current_time():
    # utc_now = datetime.datetime.utcnow()
    # asia_timezone = pytz.timezone("Asia/Kolkata")
    # asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    # current_date = asia_now.strftime("%d-%m-%Y")
    current_time = datetime.datetime.now().strftime("%m-%d-%Y")
    return current_time


project_blueprint = Blueprint("project_blueprint", __name__)


def logger_add(data, currentuser):

    cmo.insertion("projectLogger", data)


@project_blueprint.route("/financialData", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@project_blueprint.route("/financialData/<uniqueId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
# @token_required
def finacialdata(uniqueId=None):
    if request.method == "GET":
        arra = [
            {"$match": {"_id": ObjectId(uniqueId)}},
            {"$addFields": {"_id": {"$toString": "$_id"}}},
        ]
        response = cmo.finding_aggregate("financialData", arra)
        return respond(response)

    if request.method == "POST":
        if uniqueId == None:
            allData = request.get_json()
            Response = cmo.insertion("finance", allData)
            return respond(Response)

        if uniqueId != None:
            # print(allData)
            lookData = {"_id": ObjectId(uniqueId)}
            Response = cmo.updating("finance", lookData, allData, False)

            return respond(Response)

    elif request.method == "DELETE":
        if id != None:
            Response = cmo.deleting("Finance", id)
            return respond(Response)

@project_blueprint.route("/subprojects/<custId>", methods=['GET', 'POST'])
@token_required
def subprojects(current_user, custId=None):
    chekinarray = [None, '', 'undefined']
    projectTypeId = request.args.get('projectType')
    if projectTypeId:
        try:
            projectTypeId = json.loads(projectTypeId)
        except Exception as e:
            return respond({
                "status": 400,
                "icon": "error",
                "msg": "Please Select Project Type"
            })
    projectObjectId = []
    if projectTypeId and isinstance(projectTypeId, list):
        for i in projectTypeId:
            try:
                projectObjectId.append(ObjectId(i))
            except Exception as e:
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Invalid Project Type ID"
                })
    if projectObjectId:
        arr = [
            {
                '$match': {
                    '_id': {
                        '$in': projectObjectId
                    },
                    'custId': custId
                }
            }, {
                '$project': {
                    'projectType': 1, 
                    'custId': 1, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'let': {
                        'projectType': '$projectType', 
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
                    'subProject': '$result.subProject', 
                    'subProjectId': {
                        '$toString': '$result._id'
                    }
                }
            }, {
                '$project': {
                    'subProject': {
                        '$concat': [
                            '$subProject', '(', '$projectType', ')'
                        ]
                    }, 
                    'subProjectId': 1, 
                    '_id': 0
                }
            }
        ]
        Response = cmo.finding_aggregate("projectType", arr)
        return respond(Response)
    else:
        if custId not in chekinarray:
            arr = [
                {
                    '$match': {
                        'custId': custId
                    }
                },
                {
                    '$addFields': {
                        'subProjectId': {
                            '$toString': '$_id'
                        },
                        'subProject': {
                            '$concat': ['$subProject', '(', '$projectType', ')']
                        }
                    }
                },
                {
                    '$project': {
                        '_id': 0
                    }
                }
            ]
            Response = cmo.finding_aggregate("projectType", arr)
            return respond(Response)
        else:
            return respond({
                'status': 400,
                "msg": "Please Provide Customer or ProjectType ID",
                "icon": "error"
            })
   
@project_blueprint.route("/projectsType/<custId>",methods=['GET','POST'])
@project_blueprint.route("/projectsType/<custId>/<projectTypeId>",methods=['GET','POST'])
@token_required
def projectsType(current_user,custId=None,projectTypeId=None):
        arra=[
            {
                '$match': {
                    'deleteStatus':{"$ne":1},
                    'custId': custId
                }
            }
        ]
        if projectTypeId!= None and projectTypeId!="undefined":
            arra = arra + [
                {
                    '$match':{
                        '_id':ObjectId(projectTypeId)
                    }
                }
            ]
        arra = arra + [{
                '$group': {
                    '_id': '$projectType', 
                    'doc': {
                        '$first': '$$ROOT'
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$doc'
                }
            }, {
                '$addFields': {
                    'projectTypeId': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'projectTypeId':1,
                    'projectType':1
                }
            }, {
                '$sort':{
                    'projectType':1
                }
            }
        ]
        Response=cmo.finding_aggregate("projectType",arra)
        return respond(Response)
            
        
@project_blueprint.route("/issueData", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@project_blueprint.route("/issueData/<uniqueId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def issuedata(current_user,uniqueId=None):

    if request.method == "GET":
        arra = [
            {"$match": {"_id": ObjectId(uniqueId)}},
            {"$addFields": {"_id": {"$toString": "$_id"}}},
        ]
        response = cmo.finding_aggregate("SystemIssue", arra)
        return respond(response)

    if request.method == "POST":
        if uniqueId == None:
            allData = request.get_json()
            Response = cmo.insertion("SystemIssue", allData)
            return respond(Response)

        if uniqueId != None:
            allData = request.get_json()
            # print(allData)
            lookData = {"_id": ObjectId(uniqueId)}
            Response = cmo.updating("SystemIssue", lookData, allData, False)

            return respond(Response)

    elif request.method == "DELETE":
        if uniqueId != None:
            Response = cmo.deleting("SystemIssue", uniqueId)
            return respond(Response)


@project_blueprint.route("/trackingData", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@project_blueprint.route("/trackingData/<uniqueId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
# @token_required
def trackingdata(uniqueId=None):

    if request.method == "GET":
        arra = [
            {"$match": {"_id": ObjectId(uniqueId)}},
            {"$addFields": {"_id": {"$toString": "$_id"}}},
        ]
        response = cmo.finding_aggregate("Tracking", arra)
        return respond(response)

    if request.method == "POST":
        if uniqueId == None:
            allData = request.get_json()
            Response = cmo.insertion("Tracking", allData)
            return respond(Response)

        if uniqueId != None:
            allData = request.get_json()
            # print(allData)
            lookData = {"_id": ObjectId(uniqueId)}
            Response = cmo.updating("Tracking", lookData, allData, False)

            return respond(Response)

    elif request.method == "DELETE":
        if uniqueId != None:
            Response = cmo.deleting("Tracking", uniqueId)
            return respond(Response)


@project_blueprint.route("/siteEngineer", methods=["GET", "POST","DELETE"])
@project_blueprint.route("/siteEngineer/<uniqueId>", methods=["GET", "POST","DELETE"])
@token_required
def siteengineer(current_user,uniqueId=None):
    
    if request.method == "GET":
        if request.args.get("mileStoneName") == None:
            arra = [
                {
                    '$match':{
                        'deleteStatus':{'$ne':1},
                        "projectuniqueId": uniqueId
                    }
                }, {
                    '$sort':{
                        '_id':-1
                    }
                }
            ]
            if request.args.get('subProject')!=None and request.args.get("subProject")!="undefined":
                arra = arra + [
                    {
                        '$match':{
                            'SubProjectId':request.args.get('subProject')
                        }
                    }
                ]
            if request.args.get('siteId')!=None and request.args.get("siteId")!="undefined":
                arra = arra + [
                    {
                        '$match':{
                            'FA Code':{
                                '$regex':request.args.get('siteId').strip(),
                                '$options': 'i'
                            }
                        }
                    }
                ]
            if request.args.get('siteStatus')!=None and request.args.get("siteStatus")!="undefined":
                arra = arra + [
                    {
                        '$match':{
                            'siteStatus':request.args.get('siteStatus')
                        }
                    }
                ]
            if request.args.get('siteBillingStatus')!=None and request.args.get("siteBillingStatus")!="undefined":
                arra = arra + [
                    {
                        '$match':{
                            'siteBillingStatus':request.args.get('siteBillingStatus')
                        }
                    }
                ]
            arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
            arra1 = arra + [
                {
                    '$addFields': {
                        'SubProjectId': {
                            '$toObjectId': '$SubProjectId'
                        }, 
                        # 'Site_Completion Date1': {
                        #     '$cond': {
                        #         'if': {
                        #             '$or': [
                        #                 {
                        #                     '$eq': [
                        #                         '$Site_Completion Date', None
                        #                     ]
                        #                 }, {
                        #                     '$eq': [
                        #                         '$Site_Completion Date', ''
                        #                     ]
                        #                 }
                        #             ]
                        #         }, 
                        #         'then': None, 
                        #         'else': {
                        #             '$toDate': '$Site_Completion Date'
                        #         }
                        #     }
                        # }, 
                        # 'siteEndDate1': {
                        #     '$cond': {
                        #         'if': {
                        #             '$or': [
                        #                 {
                        #                     '$eq': [
                        #                         '$siteEndDate', None
                        #                     ]
                        #                 }, {
                        #                     '$eq': [
                        #                         '$siteEndDate', ''
                        #                     ]
                        #                 }
                        #             ]
                        #         }, 
                        #         'then': None, 
                        #         'else': {
                        #             '$toDate': '$siteEndDate'
                        #         }
                        #     }
                        # }
                    }
                }, 
                # {
                #     '$addFields': {
                        # 'Site_Completion Date': {
                        #     '$cond': {
                        #         'if': {
                        #             '$eq': [
                        #                 {
                        #                     '$type': '$Site_Completion Date1'
                        #                 }, 'date'
                        #             ]
                        #         }, 
                        #         'then': {
                        #             '$dateToString': {
                        #                 'date': '$Site_Completion Date1', 
                        #                 'format': '%d-%m-%Y', 
                        #                 'timezone': 'Asia/Kolkata'
                        #             }
                        #         }, 
                        #         'else': ''
                        #     }
                        # }, 
                        # 'siteageing': {
                        #     '$cond': {
                        #         'if': {
                        #             '$eq': [
                        #                 {
                        #                     '$type': '$Site_Completion Date1'
                        #                 }, 'date'
                        #             ]
                        #         }, 
                        #         'then': {
                        #             '$round': {
                        #                 '$divide': [
                        #                     {
                        #                         '$subtract': [
                        #                             '$siteEndDate1', '$Site_Completion Date1'
                        #                         ]
                        #                     }, 86400000
                        #                 ]
                        #             }
                        #         }, 
                        #         'else': ''
                        #     }
                        # }
                    # }
                # },
                {
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
                                }
                            }, {
                                '$addFields': {
                                    '_id': {
                                        '$toString': '$_id'
                                    }, 
                                    'siteId': {
                                        '$toString': '$siteId'
                                    }, 
                                    'uniqueId': {
                                        '$toString': '$_id'
                                    }, 
                                    # 'mileStoneStartDate1': {
                                    #     '$toDate': '$mileStoneStartDate'
                                    # }, 
                                    # 'mileStoneEndtDate1': {
                                    #     '$toDate': '$mileStoneEndDate'
                                    # }, 
                                    'CC_Completion Date1': {
                                        '$cond': {
                                            'if': {
                                                '$or': [
                                                    {
                                                        '$eq': [
                                                            '$CC_Completion Date', None
                                                        ]
                                                    }, {
                                                        '$eq': [
                                                            '$CC_Completion Date', ''
                                                        ]
                                                    }
                                                ]
                                            }, 
                                            'then': None, 
                                            'else': {
                                                '$toDate': '$CC_Completion Date'
                                            }
                                        }
                                    },
                                    'assignDate': {
                                        '$dateFromString': {
                                            'dateString': '$assignDate', 
                                            'format': '%m-%d-%Y', 
                                        }
                                    },
                                }
                            }, {
                                '$addFields': {
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
                                                    'format': '%m-%d-%Y', 
                                                }
                                            }, 
                                            'else': ''
                                        }
                                    }, 
                                    # 'taskageing': {
                                    #     '$cond': {
                                    #         'if': {
                                    #             '$eq': [
                                    #                 {
                                    #                     '$type': '$CC_Completion Date1'
                                    #                 }, 'date'
                                    #             ]
                                    #         }, 
                                    #         'then': {
                                    #             '$round': {
                                    #                 '$divide': [
                                    #                     {
                                    #                         '$subtract': [
                                    #                             '$mileStoneEndtDate1', '$CC_Completion Date1'
                                    #                         ]
                                    #                     }, 86400000
                                    #                 ]
                                    #             }
                                    #         }, 
                                    #         'else': {
                                    #             '$round': {
                                    #                 '$divide': [
                                    #                     {
                                    #                         '$subtract': [
                                    #                             '$mileStoneEndtDate1', '$$NOW'
                                    #                         ]
                                    #                     }, 86400000
                                    #                 ]
                                    #             }
                                    #         }
                                    #     }
                                    # }, 
                                    'taskageing': {
                                        '$cond': {
                                            'if': {
                                                '$eq': ["$mileStoneStatus","Closed"]
                                            }, 
                                            'then': {
                                                '$round': {
                                                    '$divide': [
                                                        {
                                                            '$subtract': [
                                                                '$CC_Completion Date1',"$assignDate"
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
                                                                '$$NOW',"$assignDate"
                                                            ]
                                                        }, 86400000
                                                    ]
                                                }
                                            }
                                        }
                                    },
                                    "assignDate":{
                                        '$dateToString':{
                                            'date':"$assignDate",
                                            'format': '%m-%d-%Y'
                                        }
                                    } 
                                    # 'mileStoneStartDate': {
                                    #     '$dateToString': {
                                    #         'date': {
                                    #             '$toDate': '$mileStoneStartDate'
                                    #         }, 
                                    #         'format': '%d-%m-%Y', 
                                    #         'timezone': 'Asia/Kolkata'
                                    #     }
                                    # }, 
                                    # 'mileStoneEndDate': {
                                    #     '$dateToString': {
                                    #         'date': {
                                    #             '$toDate': '$mileStoneEndDate'
                                    #         }, 
                                    #         'format': '%d-%m-%Y', 
                                    #         'timezone': 'Asia/Kolkata'
                                    #     }
                                    # }
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
                                            '$project': {
                                                '_id': 0, 
                                                'assignerName': {
                                                    '$ifNull': [
                                                        '$empName', '$vendorName'
                                                    ]
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
                                    'assignerId': 0, 
                                    'mileStoneStartDate1': 0, 
                                    'mileStoneEndtDate1': 0, 
                                    'CC_Completion Date1': 0
                                }
                            }
                        ], 
                        'as': 'milestoneArray'
                    }
                }
            ]
            if current_user['roleName']  in ['Field Resource', "Field Employee","Partner"]:
                a = 10
                if request.args.get('subProject')!=None and request.args.get("subProject")!="undefined":
                    a = a+1
                if request.args.get('siteId')!=None and request.args.get("siteId")!="undefined":
                    a = a+1
                if request.args.get('siteStatus')!=None and request.args.get("siteStatus")!="undefined":
                    a = a+1
                if request.args.get('siteBillingStatus')!=None and request.args.get("siteBillingStatus")!="undefined":
                    a = a+1
                arra1[a]['$lookup']['pipeline'].insert(0, {
                        "$match": {
                            'assignerId': ObjectId(current_user['userUniqueId'])
                        }
                    })
                arra1 = arra1 + [
                    {
                        '$match':{
                            'milestoneArray':{
                                '$ne':[]
                            }
                        }
                    }
                ]
            arra = arra1 +[
                {
                    "$lookup": {
                        "from": "projectType",
                        "localField": "SubProjectId",
                        "foreignField": "_id",
                        'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                }, {
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
                                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                            {"$project": {"PMName": 1,"projectId":1, "_id": 0}},
                        ],
                        "as": "projectArray",
                    }
                }, {
                    "$unwind": {
                        "path": "$projectArray",
                        "preserveNullAndEmptyArrays": True,
                    }
                }, {
                    "$addFields": {
                        "PMName": "$projectArray.PMName",
                        "projectId":'$projectArray.projectId',
                        "siteId": "$siteid",
                        "projectuniqueId": {"$toString": "$projectuniqueId"},
                        "uniqueId": "$_id",
                        # "siteStartDate": {
                        #     "$dateToString": {
                        #         "date": {"$toDate": "$siteStartDate"},
                        #         "format": "%d-%m-%Y",
                        #         "timezone": "Asia/Kolkata",
                        #     }
                        # },
                        # "siteEndDate": {
                        #     "$dateToString": {
                        #         "date": {"$toDate": "$siteEndDate"},
                        #         "format": "%d-%m-%Y",
                        #         "timezone": "Asia/Kolkata",
                        #     }
                        # },
                    }
                }, {
                    "$project": {
                        "projectArray": 0,
                        "assignerId": 0,
                        "result": 0,
                        "Site_Completion Date1": 0,
                        "siteEndDate1": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("SiteEngineer", arra)
            return respond(response)
        else:
            arra = [
                {
                    '$match':{
                        'deleteStatus':{'$ne':1},
                        "projectuniqueId": uniqueId
                   
                    }
                }
            ]
            if current_user['roleName']  in ['Field Resource', "Field Employee","Partner"]:
                arra = [
                    {
                        '$match':{
                            'assignerId':ObjectId(current_user['userUniqueId'])
                        }
                    }
                ]
            arra = arra + [
                {"$match": {"Name": {"$regex": request.args.get("mileStoneName"),'$options': 'i'}}},
                {
                    "$addFields": {
                        "_id": {"$toString": "$_id"},
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
                                    "$eq": [{"$type": "$CC_Completion Date1"}, "date"]
                                },
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
                                "if": {
                                    "$eq": [{"$type": "$CC_Completion Date1"}, "date"]
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
                            {
                                "$match":{
                                    'deleteStatus':{'$ne':1}
                                }
                            }, {
                                "$project": {
                                    "_id": 0,
                                    "assignerName": "$empName",
                                    "assignerId": {"$toString": "$_id"},
                                }
                            }
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
                        "let": {"projectuniqueId": "$projectuniqueId"},
                        "localField": "siteId",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "projectuniqueId": uniqueId,
                                    'deleteStatus':{'$ne':1}
                                }
                            },
                            {
                                "$addFields": {
                                    "SubProjectId": {"$toObjectId": "$SubProjectId"}
                                }
                            },
                            {
                                "$lookup": {
                                    "from": "projectType",
                                    "localField": "SubProjectId",
                                    "foreignField": "_id",
                                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                                    "as": "SubProjectId",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$SubProjectId",
                                    "preserveNullAndEmptyArrays": True,
                                }
                            },
                            {
                                "$addFields": {
                                    "SubProject": "$SubProjectId.subProject",
                                    "projectuniqueId": {
                                        "$toObjectId": "$projectuniqueId"
                                    },
                                }
                            },
                            {"$project": {"SubProjectId": 0}},
                            {
                                "$lookup": {
                                    "from": "project",
                                    "localField": "projectuniqueId",
                                    "foreignField": "_id",
                                    "pipeline": [
                                        {"$match": {"deleteStatus": {"$ne": 1}}},
                                        {
                                            "$addFields": {
                                                "PMId": {"$toObjectId": "$PMId"}
                                            }
                                        },
                                        {
                                            "$lookup": {
                                                "from": "userRegister",
                                                "localField": "PMId",
                                                "foreignField": "_id",
                                                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                {"$project": {"siteResult._id": 0, "_id": 0}},
                {
                    "$lookup": {
                        "from": "milestone",
                        "localField": "siteId",
                        "foreignField": "siteId",
                        'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                        "as": "result",
                    }
                },
                {
                    "$addFields": {
                        "uniqueId": {"$toString": "$siteId"},
                        "siteResult.projectuniqueId": {
                            "$toString": "$siteResult.projectuniqueId"
                        },
                        "siteStartDate": {
                            "$dateToString": {
                                "date": {"$toDate": "$siteResult.siteStartDate"},
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "siteEndDate": {
                            "$dateToString": {
                                "date": {"$toDate": "$siteResult.siteEndDate"},
                                "format": "%d-%m-%Y",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "PMName": "$siteResult.projectArray.PMName",
                        "Site Id": "$siteResult.Site Id",
                        "subProject": "$siteResult.SubProject",
                        "siteName": "$siteResult.sitename",
                        "milestoneArray": {
                            "$map": {
                                "input": "$milestoneArray",
                                "as": "milestone",
                                "in": {
                                    "$mergeObjects": [
                                        "$$milestone",
                                        {"siteId": {"$toString": "$$milestone.siteId"}},
                                    ]
                                },
                            }
                        },
                        "totalCount": {"$size": "$result"},
                        "milestoneCount": {"$size": "$milestoneArray"},
                    }
                },
                {"$project": {"result": 0, "siteId":0 }},
            ]
            arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
            response = cmo.finding_aggregate("milestone", arra)
            return respond(response)

    if request.method == "POST":
        if uniqueId == None:
            allData = request.get_json()
            Response = cmo.insertion("SiteEngineer", allData)
            return respond(Response)
        if uniqueId != None:
            allData = request.get_json()
            lookData = {"_id": ObjectId(uniqueId)}
            Response = cmo.updating("SiteEngineer", lookData, allData, False)
            return respond(Response)

    elif request.method == "DELETE":
        allData = request.json.get("ids")
        if (len(allData)>0):
            for i in allData:
                arra = [
                    {"$match": {"_id": ObjectId(i)}},
                    {"$project": {"projectuniqueId": 1, "_id": 0}},
                ]
                response = cmo.finding_aggregate("SiteEngineer", arra)
                response = response["data"][0]
                projectuniqueId = response["projectuniqueId"]
                cmo.deleting_m("milestone",{'siteId':ObjectId(i)},current_user['userUniqueId'])
                cmo.deleting_m("invoice",{'siteId':i},current_user['userUniqueId'])
                cmo.deleting_m("complianceApproverSaver",{'siteuid':ObjectId(i)},current_user['userUniqueId'])
                # evl.siteDeleteLogs(i, current_user["userUniqueId"], projectuniqueId, "SiteEngineer")
                cmo.deleting("SiteEngineer", i, current_user["userUniqueId"])
            return respond({
                'status':200,
                "msg":"Data Deleted Successfully",
                "icon":"error"
            })
        else:
            return respond({
                'msg':'Please Select At least one Site',
                "status":400,
                "icon":"error"
            })


@project_blueprint.route("/milestone", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@project_blueprint.route("/milestone/<uniqueId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
# @token_required
def milestone(uniqueId=None):
    if request.method == "GET":
        arra = [
            {"$match": {"projectuniqueId": uniqueId}},
            {"$addFields": {"SubProjectId": {"$toObjectId": "$SubProjectId"}}},
            {
                "$lookup": {
                    "from": "projectType",
                    "localField": "SubProjectId",
                    "foreignField": "_id",
                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                }
            },
            {"$project": {"result": 0}},
        ]
        # print(arra)
        response = cmo.finding_aggregate("SiteEngineer", arra)
        return respond(response)

    if request.method == "POST":
        if uniqueId == None:
            allData = request.get_json()
            # print(allData,"allData")
            newData = copy.deepcopy(allData)
            del newData["data"]
            for i in allData["data"]:
                # print(i,"milestone")
                Response = cmo.insertion("milestone", {**i, **newData})
            return respond(Response)

        if uniqueId != None:
            allData = request.get_json()
            # print(allData)
            lookData = {"_id": ObjectId(uniqueId)}
            Response = cmo.updating("SiteEngineer", lookData, allData, False)

            return respond(Response)

    elif request.method == "DELETE":
        if uniqueId != None:
            Response = cmo.deleting("SiteEngineer", uniqueId)
            return respond(Response)

def Task_allocation_mail(assigningTo,uid,finalData):
    for oneas in uid:
        arra = [
            {
                '$match': {
                    '_id': ObjectId(oneas)
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteId', 
                    'foreignField': '_id', 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'SiteId': {
                        '$arrayElemAt': [
                            '$result.Site Id', 0
                        ]
                    }, 
                    'projectuniqueId': {
                        '$toObjectId': '$projectuniqueId'
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
                '$addFields': {
                    'ProjectId': {
                        '$arrayElemAt': [
                            '$result.projectId', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'Name': 1, 
                    'SiteId': 1, 
                    'ProjectId': 1, 
                    '_id': 0
                }
            }
        ]
        mailData = cmo.finding_aggregate("milestone",arra)['data'][0]
        TaskName = mailData['Name']
        SiteID = mailData['SiteId']
        ProjectID = mailData['ProjectId']
        userArra = [{"$match": {"_id": {"$in": finalData}}}]
        dtewformail = cmo.finding_aggregate("userRegister", userArra)["data"]
        for smail in dtewformail:
            if assigningTo == "vendor":
                smail['empName'] = smail['vendorName']
            cmailer.formatted_sendmail(to=[smail["email"]],cc=[],subject="Task Allocation",message=cmt.TaskAllocation_mail(smail['empName'],TaskName,SiteID,ProjectID))


def form_changer(text):
    return str(text)

@project_blueprint.route("/globalSaver", methods=["POST", "PATCH"])
@project_blueprint.route("/globalSaver/<uniqueId>", methods=["POST", "PATCH"])
@token_required
def globalSaver(current_user,uniqueId=None):


    if request.method == "POST":
        allData = request.get_json()
       
        if (len(list(allData["siteEngineer"].keys())) == 0 and len(list(allData["mileStone"].keys())) == 0):
            response = {
                "status": 400,
                "icon": "error",
                "msg": "Please Add FA Code and Milestone",
            }
            return respond(response)
        
        if len(list(allData["siteEngineer"].keys())) == 0:
            response = {"status": 400, "icon": "error", "msg": "Please Add FA Code"}
            return respond(response)
        
        if len(list(allData["mileStone"].keys())) == 0:
            response = {
                "status": 400,
                "icon": "error",
                "msg": "Please Add at least one Milestone",
            }
            return respond(response)
        
        siteEngineerAllData = {
            **allData["siteEngineer"],
            # **allData["t_issues"],
            **allData["t_tracking"],
            # **allData["t_sFinancials"],
        }

        # finaler = ctm.datetimeObj()

        # rfaidateandallocationdate = 0
        # cond = True

        # if "RFAI Date" in siteEngineerAllData and cond:
        #     if siteEngineerAllData["RFAI Date"] != None:
        #         finaler = ctm.strtodate(siteEngineerAllData["RFAI Date"])
        #         rfaidateandallocationdate = rfaidateandallocationdate + 1
        #         cond = False

        # if "ALLOCATION DATE" in siteEngineerAllData and cond:
        #     if 'RFAI Date' not in siteEngineerAllData and siteEngineerAllData['ALLOCATION DATE'] not in ['',None,'undefined']:
        #         siteEngineerAllData['RFAI Date'] = siteEngineerAllData['ALLOCATION DATE']
        #         if siteEngineerAllData["RFAI Date"] != None:
        #             finaler = ctm.strtodate(siteEngineerAllData["ALLOCATION DATE"])
        #             rfaidateandallocationdate = rfaidateandallocationdate + 1
        #             cond = False
            
        # if rfaidateandallocationdate == 0:
        #     return respond(
        #         {
        #             "status": 400,
        #             "icon": "error",
        #             "msg": f"Please provide at least one RFAI Date or Site Allocation Date ",
        #         }
        #     )
        
        
        # arra = [
        #     {"$match": {"_id": ObjectId(siteEngineerAllData["SubProjectId"])}},
        #     {
        #         "$project": {
        #             "concatenatedArray": {
        #                 "$concatArrays": [
        #                     {
        #                         "$cond": {
        #                             "if": {"$isArray": "$t_sengg"},
        #                             "then": "$t_sengg",
        #                             "else": [],
        #                         }
        #                     }
        #                 ]
        #             },
        #             "custId":1,
        #         }
        #     }, {
        #         "$unwind": {
        #             "path": "$concatenatedArray",
        #             "preserveNullAndEmptyArrays": True,
        #         }
        #     }, {
        #         '$addFields': {
        #             'concatenatedArray.custId': '$custId'
        #         }
        #     }, {
        #         "$replaceRoot": {
        #             "newRoot": "$concatenatedArray"
        #         }
        #     }, {
        #         "$match": {
        #             "dataType": "Auto Created",
        #             "fieldName":'Unique ID'
        #         }
        #     }
        # ]
        # response = cmo.finding_aggregate("projectType", arra)["data"]

        # if len(response)>0:

        #     try:
        #         for i in response:
        #                 valeGame = []
        #                 for j in i["dropdownValue"].split(","):
        #                     valeGame.append(siteEngineerAllData[form_changer(j)])
        #                 siteEngineerAllData[form_changer(i["fieldName"])] = "-".join(valeGame)
        #     except:
        #         return respond({
        #             'icon':'error',
        #             "status":400,
        #             "msg":"Please check Unique ID Combination, May be Some filed not found"
        #         })
        # else:
        #     return respond({
        #         'status':400,
        #         "icon":"error",
        #         "msg":'Unique ID is not Define in Template for this Project Sub Type'
        #     })
        # if "Unique ID" in siteEngineerAllData:
        #     arra = [
        #         {
        #             '$match':{"Unique ID": siteEngineerAllData["Unique ID"]}
        #         }
        #     ]

        #     dalenta = cmo.finding_aggregate("SiteEngineer",arra)["data"]
        #     if len(dalenta) > 0:
        #         return respond(
        #             {
        #                 "status": 400,
        #                 "msg": f"This Unique ID '{siteEngineerAllData['Unique ID'] }' is already exists.",
        #                 "icon":"error"
        #             }
        #         )

        # arra = [
        #     {
        #         '$match':{
        #             '_id':ObjectId(siteEngineerAllData['projectuniqueId'])
        #         }
        #     }, {
        #         '$addFields':{
        #             "projectGroup":{
        #                 '$toString':"$projectGroup"
        #             }
        #         }
        #     }
        # ]
        # fetchProjectData = cmo.finding_aggregate("project",arra)['data']
            
        siteEngineerAllData["siteBillingStatus"] = "Unbilled"
        siteEngineerAllData["siteStatus"] = "Open"
        # siteEngineerAllData["customerId"] = response[0]['custId']
        counter = database.fileDetail.find_one_and_update({"id": "systemIdCounter"},{"$inc": {"sequence_value": 1}},return_document=True,upsert=True)
        # sequence_value = counter["sequence_value"]
        # systemId = "SSID" + str(sequence_value).zfill(8)
        # siteEngineerAllData["systemId"] = systemId
        systemId = f"SSID{counter['sequence_value']:08d}"
        siteEngineerAllData["systemId"] = systemId
        Response = cmo.insertion("SiteEngineer", siteEngineerAllData)
        uId = Response["operation_id"]
        projectuniqueId = siteEngineerAllData["projectuniqueId"]
        evl.newSite(uId, current_user["userUniqueId"], projectuniqueId,"")


        # Milestone

        newData = copy.deepcopy(allData["mileStone"])
        del newData["data"]
        # finalerEnd = None
        # finalerStart = finaler
        ind = 0
        milestone = allData["mileStone"]['data']
        for i in milestone:
            # finalerEnd = ctm.add_hour_in_udate(finalerStart, int(i["Estimated Time (Days)"]))
            if "Predecessor" in i:
                if i["Predecessor"] is None:
                    i["Predecessor"] = ""
            if not  "Predecessor" in i:
                    i["Predecessor"] = ""
            # finalerEnd = ctm.add_hour_in_udate(finalerStart, int(i["Estimated Time (Days)"]))
            dataInsert = {
                "Name": i["Name"],
                "Estimated Time (Days)": i["Estimated Time (Days)"],
                # "WCC Sign Off": i["WCC Sign off"],
                "Predecessor": i["Predecessor"],
                "Completion Criteria": i["Completion Criteria"],
                "SubProjectId": allData["mileStone"]["SubProjectId"],
                "projectuniqueId": allData["mileStone"]["projectuniqueId"],
                "siteId": ObjectId(uId),
                "systemId": systemId,
                "estimateDay": int(i["Estimated Time (Days)"]),
                "mileStoneStatus": "Open",
                # "mileStoneStartDate": finalerStart.strftime("%Y-%m-%d")+"T00:00:00",
                # "mileStoneEndDate": finalerEnd.strftime("%Y-%m-%d")+"T00:00:00",
                "addedAt": new_current_time(),
                # "customerId":response[0]['custId'],
                # "projectGropupId":fetchProjectData[0]['projectGroup'],
                # "circleId":fetchProjectData[0]['circle']
            }
            # if ind == 0:
            #     dataInsert["Predecessor"] = ""
            Response = cmo.insertion("milestone", dataInsert)
            evl.newMileStone(Response['operation_id'],current_user['userUniqueId'],allData["mileStone"]["projectuniqueId"],i["Name"],uId,msg="New MileStone Created")
            # finalerStart = finalerEnd
            # ind = ind + 1
        # upby = {"_id": ObjectId(uId)}
        # upAt = {
        #     "siteStartDate": finaler.strftime("%Y-%m-%d")+"T00:00:00",
        #     "siteEndDate": finalerEnd.strftime("%Y-%m-%d")+"T00:00:00",
        # }
        # cmo.updating("SiteEngineer", upby, upAt, False)
        return respond({
            'status':201,
            "icon":"success",
            "msg":"Data Addedd Successfully"
        })

    if request.method == "PATCH":
        
        allData = request.get_json()

        name = allData["name"]
        uoData = allData["data"]
        uid = allData["from"]["uid"]


        if name == "updateSiteEngg":
            updatingData = allData["data"]
            uid = allData["from"]["uid"]
            
            # arra = [
            #     {"$match": {"_id": ObjectId(uid)}},
            #     {"$project": {"_id": 0, "SubProjectId": 1}},
            # ]
            # subprojectid = cmo.finding_aggregate("SiteEngineer", arra)['data']
            # if len(subprojectid):
            #     subprojectid=subprojectid[0]['SubProjectId']
            #     arry = [
            #         {"$match": {"_id": ObjectId(subprojectid)}},
            #         {
            #             "$project": {
            #                 "Commercial": 0,
            #                 "_id": 0,
            #                 "t_tracking": 0,
            #                 "t_sFinancials": 0,
            #                 "t_issues": 0,
            #                 "MileStone": 0,
            #             }
            #         },
            #     ]
            #     datas=cmo.finding_aggregate("projectType",arry)['data']
            #     if len(datas):
            #         for i in datas[0]['t_sengg']:
            #             if i['fieldName']=="Unique ID" and i['dataType']=="Auto Created":
            #                 uniqueIdDropDownvalues=i['dropdownValue']
            #                 keys = uniqueIdDropDownvalues.split(',')
            #                 extracted_values = [updatingData[key] for key in keys if key in updatingData]
            #                 extracted_values = [str(element) for element in extracted_values]
            #                 changedUniqueId = '-'.join(extracted_values)
            #                 updatingData['Unique ID'] = changedUniqueId
            #                 if changedUniqueId == "":
            #                     del updatingData['Unique ID']
            #                 if changedUniqueId != "":
            #                     arra = [
            #                         {
            #                             '$match':{
            #                                 '_id':ObjectId(uid)
            #                             }
            #                         }
            #                     ]
            #                     currentUniqueId = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['Unique ID']
            #                     if currentUniqueId!=changedUniqueId:
            #                         duplicate = cmo.finding("SiteEngineer",{"Unique ID":changedUniqueId})['data']
            #                         if duplicate:
            #                             duplicateId = duplicate[0]['Unique ID']
            #                             return respond({
            #                                 'status':400,
            #                                 "msg":f'This Unique Id "{duplicateId}" is already exist in Database',
            #                                 "icon":"error"
            #                             })
                                            
            updateBy = {"_id": ObjectId(uid)}

            # if "RFAI Date" in updatingData:
            #     mileArra = [
            #         {"$match": {"siteId": ObjectId(uid)}},
            #         {"$sort": {"indexing": 1}},
            #     ]
            #     mileList = cmo.finding_aggregate("milestone", mileArra)["data"]
            #     finaler = ctm.strtodate(updatingData["RFAI Date"])
            #     finalerStart = finaler
            #     for milei in mileList:
            #         finalerEnd = ctm.add_hour_in_udate(finalerStart, int(milei["estimateDay"]))
            #         dataMileList = {
            #             "mileStoneStartDate": finalerStart.strftime("%Y-%m-%d")+ "T00:00:00",
            #             "mileStoneEndDate": finalerEnd.strftime("%Y-%m-%d")+ "T00:00:00",
            #         }
            #         upby = {"_id": ObjectId(milei["_id"])}
            #         cmo.updating("milestone", upby, dataMileList, False)
            #         finalerStart = finalerEnd
            #         updatingData["siteStartDate"] = finaler.strftime("%Y-%m-%d") + "T00:00:00"
            #         updatingData["siteEndDate"] = finalerEnd.strftime("%Y-%m-%d") + "T00:00:00"

            response = cmo.updating("SiteEngineer", updateBy, updatingData, False)
            return respond(response)

        if name == "bulktask":

            assigningTo = allData['data']['assigningTo']
            finalData = []

            if not uoData.get("assignerId"):
                return respond({
                    'status': 400,
                    'icon': 'warning',
                    'msg': 'Please select at least one user'
                })

                
            for oneas in uoData["assignerId"].split(","):
                finalData.append(ObjectId(oneas))
            
            dtwq = []
            # partnerAssignTask = []
            response = {}
            for oneas in uid:
                arra = [
                    {
                        '$match':{
                            'deleteStatus':{'$ne':1},
                            "_id": ObjectId(oneas)
                        }
                    }, {
                        '$lookup': {
                            'from': 'SiteEngineer', 
                            'localField': 'siteId', 
                            'foreignField': '_id', 
                            'as': 'result'
                        }
                    }, {
                        '$addFields': {
                            'FA Code': {
                                '$arrayElemAt': [
                                    '$result.FA Code', 0
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'Name': {
                                '$toString': '$Name'
                            }, 
                            'FA Code': {
                                '$toString': '$FA Code'
                            }
                        }
                    }, {
                        '$addFields': {
                            'newField': {
                                '$concat': [
                                    '$Name', '(', '$FA Code', ')'
                                ]
                            }
                        }
                    }
                    
                ]
                resping = cmo.finding_aggregate("milestone", arra)["data"][0]
                if resping["mileStoneStatus"] != "Open":
                    dtwq.append(resping["newField"])
                # if "typeAssigned" in resping and resping['typeAssigned'] == "Partner":
                #     partnerAssignTask.append(resping["newField"])
                    
            if len(dtwq) > 0:
                return respond(
                    {
                        "status": 400,
                        "icon": "warning",
                        "msg": "The Task and Site ID pair cannot be reassigned because it is not in an open status.\n" + ", ".join(dtwq),
                        "data": [],
                    }
                )
                
            # if len(partnerAssignTask) > 0:
            #     return respond(
            #         {
            #             "status": 400,
            #             "icon": "warning",
            #             "msg": "The Task and Site ID pair allocated to a partner should be deallocated first.\n" + ", ".join(partnerAssignTask),
            #             "data": [],
            #         }
            #     )

            for oneas in uid:
                data = {"assignerId": finalData}
                data['assignDate'] = new_current_time()
                data['typeAssigned'] = assigningTo
                updateBy = {"_id":ObjectId(oneas)}
                cmo.updating("milestone", updateBy, data, False)
                # userArra = [{"$match": {"_id": {"$in": finalData}}}]
                # dtewformail = cmo.finding_aggregate("userRegister", userArra)["data"]
                # resping = cmo.finding_aggregate("milestone",[{'$match':{'_id':ObjectId(oneas)}}])["data"][0]
                # for smail in dtewformail:
                #     if assigningTo == "vendor":
                #         smail['empName'] = smail['vendorName']
                #     evl.newMileStone(oneas,current_user['userUniqueId'],allData["projectuniqueId"],resping["Name"],str(resping['siteId']),msg="Allocate a new task to "+ smail["empName"] + " using task allocation.")
            # thread = Thread(target=Task_allocation_mail, args=(assigningTo,uid,finalData))
            # thread.start()
            return respond({
                "status": 200,
                "icon": "success",
                "msg": "Data Updated Successfully"
            })
                
        if name == "bulksite":
            finalData = []
            for oneas in uoData["assignerId"].split(","):
                finalData.append(ObjectId(oneas))

            response = {}

            for oneas in uid:
                updateBy = {"siteId": ObjectId(oneas)}
                data = {"assignerId": finalData}
                data['assignDate']=current_time()
                response = cmo.updating_m("milestone", updateBy, data, False)

                userArra = [{"$match": {"_id": {"$in": finalData}}}]
                dtewformail = cmo.finding_aggregate("userRegister", userArra)

                listingew = []
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

                # if oneas != None:
                #     evl.siteEngineerLogsDirect(
                #         oneas, listingew, useruniqueId, "milestone"
                #     )

                # for smail in dtewformail:
                #     # cmailer.formatted_sendmail([smail["email"].replace("@","#")],["shubham.singh@fourbrick.com","vishal.yadav@fourbrick.com"],"New Task Allocate","Hello User you have allocate new task")
                #     cmailer.formatted_sendmail(["shubham.singh@fourbrick.com"],["vishal.yadav@fourbrick.com"],"New Task Allocate","Hello User you have allocate new task")

            return respond(response)

        updateBy = {"_id": ObjectId(uid)}

        if name == "mileStone":
            if "assignerId" in uoData:
                finalData = []
                for oneas in uoData["assignerId"].split(","):
                    finalData.append(ObjectId(oneas))
                data = {"assignerId": finalData}
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

                oldDting = cmo.finding_aggregate("milestone", getdata)["data"]

                for i in oldDting:
                    print(i)
                data = {"assignerId": uoData["assignerId"]}
                # data['assignDate']=current_time()
                response = cmo.updating("milestone", updateBy, data, False)
                return respond(response)


@project_blueprint.route("/projectAllocationList/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def projectallocationlist(id=None):
    arra = [
        {
            '$match': {
                '_id': ObjectId(id)
            }
        }, {
            '$lookup': {
                'from': 'projectAllocation', 
                'localField': '_id', 
                'foreignField': 'projectIds', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }
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
            '$addFields': {
                'emp': '$result.empId'
            }
        }, {
            '$addFields': {
                'emp': {
                    '$map': {
                        'input': '$emp', 
                        'as': 'stringId', 
                        'in': {
                            '$toObjectId': '$$stringId'
                        }
                    }
                }
            }
        }, {
            '$project': {
                'emp': 1, 
                'projectId': 1
            }
        }, {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'emp', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            'status':"Active"
                        }
                    }, {
                        '$project': {
                            'empName': {
                                '$toString': '$empName'
                            }, 
                            'userRole': 1, 
                            'empCode': {
                                '$toString': '$empCode'
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
                            'roleName': '$role.roleName'
                        }
                    }, {
                        '$match': {
                            'roleName': {
                                '$in': [
                                    'Project Support', 'Circle Support', 'Field Resource', 'QE', 'Admin', 'Project Manager', 'PMO',"Field Support"
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'role': 0, 
                            'userRole': 0
                        }
                    }
                ], 
                'as': 'empDetails'
            }
        }, {
            '$unwind': {
                'path': '$empDetails', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$group': {
                '_id': None, 
                'empDeatils': {
                    '$push': {
                        'name': {
                            '$concat': [
                                '$empDetails.empName', '(', '$empDetails.empCode', ')'
                            ]
                        }, 
                        'id': '$empDetails._id'
                    }
                }, 
                'PId': {
                    '$first': '$projectId'
                }
            }
        }, {
            '$lookup': {
                'from': 'projectAllocation', 
                'localField': 'field', 
                'foreignField': 'field', 
                'pipeline': [
                    {
                        '$match': {
                            'projectIds': ObjectId(id), 
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            'type': 'Partner',
                            
                        }
                    }, {
                        '$addFields': {
                            'vendorId': {
                                '$toObjectId': '$empId'
                            }
                        }
                    }, {
                        '$lookup': {
                            'from': 'userRegister', 
                            'localField': 'vendorId', 
                            'foreignField': '_id', 
                            'pipeline': [
                                {
                                    '$match': {
                                        'deleteStatus': {
                                            '$ne': 1
                                        }, 
                                        'type': 'Partner',
                                        'status':"Active"
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
                            'empName': {
                                '$toString': '$result.vendorName'
                            }, 
                            'empCode': {
                                '$toString': '$result.vendorCode'
                            }
                        }
                    }, {
                        '$addFields': {
                            'name': {
                                '$concat': [
                                    '$empName', '(', '$empCode', ')'
                                ]
                            }, 
                            'id': {
                                '$toString': '$vendorId'
                            }
                        }
                    }, {
                        '$project': {
                            'name': 1, 
                            'id': 1, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'vendorDetails'
            }
        }
    ]
    response = cmo.finding_aggregate("project", arra)
    return respond(response)


@project_blueprint.route("/closeMilestone/<id>", methods=["POST"])
@token_required
def closeMilestone(current_user, id=None):
    
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
            
            
    




@project_blueprint.route("/changeTaskStatus/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def changeTaskStatus(current_user, id=None):

    uniwq = {"_id": ObjectId(id)}

    resp = {}
    if request.method == "PATCH":

        data = request.get_json()

        data = {"mileStoneStatus": "Open"}
        unset = {"CC_Completion Date": "","Task Closure":""}

        cmo.updating("milestone", uniwq, data, False, unset)

        ressp = cmo.finding("milestone", uniwq)["data"][0]["siteId"]["$oid"]

        uniwq = {"_id": ObjectId(ressp)}

        data = {
            "siteStatus": "Open",
        }

        unset = {"Site_Completion Date": ""}
        resp = cmo.updating("SiteEngineer", uniwq, data, False, unset)
        # evl.milestoneCloseLogs(id, current_user["userUniqueId"], "milestone", "Open")
        cmo.updating("complianceApproverSaver",{"siteuid":ObjectId(ressp),"milestoneuid":ObjectId(id)},{'currentStatus':'Open'},False,{'formSubmitDate':1,'L1ActionDate':1,"L2ActionDate":1})
        return respond(resp)

    if request.method == "DELETE":
        data = {"deleteStatus": 1}
        resp = cmo.updating("milestone", uniwq, data, False)
        return respond(resp)


@project_blueprint.route("/getOneSiteEngg/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def onesiteengg(id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
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
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'siteId': 0, 
                                'assignerId': 0
                            }
                        }
                    ], 
                    'as': 'Milestone'
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
                '$lookup': {
                    'from': 'invoice', 
                    'localField': '_id', 
                    'foreignField': 'siteId', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
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
                                }, 
                                'unitRate': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$unitRate', ''
                                            ]
                                        }, 
                                        'then': 0, 
                                        'else': '$unitRate'
                                    }
                                }, 
                                'wccSignOffdate': {
                                    '$cond': [
                                        {
                                            '$eq': [
                                                '$wccSignOffdate', ''
                                            ]
                                        }, '', {
                                            '$dateToString': {
                                                'date': {
                                                    '$toDate': '$wccSignOffdate'
                                                }, 
                                                'format': '%d-%m-%Y', 
                                                'timezone': 'Asia/Kolkata'
                                            }
                                        }
                                    ]
                                }, 
                                'invoiceDate': {
                                    '$cond': [
                                        {
                                            '$eq': [
                                                '$invoiceDate', ''
                                            ]
                                        }, '', {
                                            '$dateToString': {
                                                'date': {
                                                    '$toDate': '$invoiceDate'
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
                                'amount': {
                                    '$multiply': [
                                        '$qty', '$unitRate'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'WCC No': '$wccNumber', 
                                'WCC SignOff Date': '$wccSignOffdate', 
                                'PO Number': '$poNumber', 
                                'Item Code': '$itemCode', 
                                'Invoiced Quantity': '$qty', 
                                'Invoice Number': '$invoiceNumber', 
                                'Invoice Date': '$invoiceDate', 
                                'Unit Rate': '$unitRate', 
                                'Amount': '$amount', 
                                'Status': '$status', 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'invoice'
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
    
@project_blueprint.route("/getOneCompliance/<id>/<mName>", methods=["GET"])
def onecompliance(id=None,mName=None):
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
    
@project_blueprint.route("/admin/getOneComplianceL1List/<id>/<mName>", methods=["GET"])
@token_required
def onecompliancel1list(current_user,id=None,mName=None):
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

@project_blueprint.route("/admin/getOneComplianceL2List/<id>/<mName>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def onecompliancel2list(current_user,id=None,mName=None):
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
                    'approverType': 'L2Approver', 
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



@project_blueprint.route("/uamView", methods=["GET", "POST", "PATCH"])
def uamview(id=None):
    if request.method == "PATCH":
        allData = request.get_json()
        updateBy = {"moduleName": allData["moduleName"], "roleId": allData["roleId"]}
        response = cmo.updating("uamManagement", updateBy, allData, True)
        return respond(response)

    if request.method == "GET":
        aggr = [
            {"$project": {"_id": 0}},
            {
                "$group": {
                    "_id": {"typeVal": "$typeVal", "roleId": "$roleId"},
                    "data": {"$push": "$$ROOT"},
                }
            },
            {"$addFields": {"typeVal": "$_id.typeVal", "roleId": "$_id.roleId"}},
        ]
        response = cmo.finding_aggregate("uamManagement", aggr)
        return respond(response)


@project_blueprint.route("/project/circle", methods=["GET"])
def projectcircle():

    arra = []

    id = request.args.get("projectGroupId")

    if id != None and id != "undefined":
        arra = arra + [{"$match": {"_id": ObjectId(id)}}]
    arra = arra + [
        {
            "$lookup": {
                "from": "zone",
                "localField": "zoneId",
                "foreignField": "_id",
                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                "as": "result",
            }
        },
        {"$addFields": {"circle": {"$arrayElemAt": ["$result.circle", 0]}}},
        {
            "$lookup": {
                "from": "circle",
                "localField": "circle",
                "foreignField": "_id",
                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                "as": "result",
            }
        },
        {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "circle": "$result.circleName",
                "uniqueId": {"$toString": "$result._id"},
            }
        },
        {"$project": {"circle": 1, "uniqueId": 1, "_id": 0}},
    ]
    print(arra,"arra")
    response = cmo.finding_aggregate("projectGroup", arra)
    return respond(response)


@project_blueprint.route("/siteProjectType/<id>", methods=["GET"])
def siteProjectType(id=None):
    arra = [
        {"$match": {"custId": id}},
        {
            "$group": {
                "_id": "$projectType",
                "uniqueId": {"$first": {"$toString": "$_id"}},
            }
        },
        {"$project": {"projectType": "$_id", "uniqueId": 1, "_id": 0}},
        {"$sort": {"projectType": 1}},
    ]
    response = cmo.finding_aggregate("projectType", arra)
    return respond(response)


# @project_blueprint.route("/projectEventLog/<id>", methods=["GET", "POST"])
# def projecteventlog(id=None):
#     if request.method == "GET":
#         arra = [
#             {
#                 '$match': {
#                     'projectuid': id
#                 }
#             }
#         ]
#         arra = arra + apireq.countarra("projectEventlog",arra)+ apireq.args_pagination(request.args)
#         arra = arra + [
#             {
#                 '$sort':{
#                     '_id':1
#                 }
#             }, {
#                 '$addFields': {
#                     'projectuid': {
#                         '$toObjectId': '$projectuid'
#                     }, 
#                     'siteuid': {
#                         '$toObjectId': '$siteuid'
#                     }, 
#                     'milestoneuid': {
#                         '$toObjectId': '$milestoneuid'
#                     }, 
#                     'empId': {
#                         '$toObjectId': '$UpdatedBy'
#                     },
#                     'newField': {
#                         '$dateFromString': {
#                             'dateString': '$UpdatedAt', 
#                             'format': '%Y-%m-%d %H:%M:%S'
#                         }
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'project', 
#                     'localField': 'projectuid', 
#                     'foreignField': '_id', 
#                     'as': 'projectResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$projectResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'SiteEngineer', 
#                     'localField': 'siteuid', 
#                     'foreignField': '_id', 
#                     'as': 'siteResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$siteResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'milestone', 
#                     'localField': 'milestoneuid', 
#                     'foreignField': '_id', 
#                     'as': 'milestoneResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$milestoneResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'userRegister', 
#                     'localField': 'empId', 
#                     'foreignField': '_id', 
#                     'as': 'empResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$empResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$sort':{
#                     'newField':-1
#                 }
#             }, {
#                 '$project': {
#                     'projectName': '$projectResult.projectId', 
#                     'siteName': '$siteResult.Site Id', 
#                     'milestoneName': '$milestoneResult.Name', 
#                     'empemail': '$empResult.email', 
#                     'UpdatedAt': 1, 
#                     'updatedData': 1, 
#                     'uniqueId': '1', 
#                     '_id': 0,
#                     'overall_table_count': 1
#                 }
#             }
#         ]
#         response = cmo.finding_aggregate("projectEventlog", arra)
#         return respond(response)

# @project_blueprint.route("/siteEventLog/<id>", methods=["GET", "POST"])
# def siteeventlog(id=None):
#     if request.method == "GET":
#         arra = [
#             {
#                 '$match': {
#                     'siteuid': id
#                 }
#             }
#         ]
#         arra = arra + apireq.countarra("projectEventlog",arra)+ apireq.args_pagination(request.args)
#         arra = arra +[
#             {
#                 '$addFields': {
#                     'siteuid': {
#                         '$toObjectId': '$siteuid'
#                     }, 
#                     'milestoneuid': {
#                         '$toObjectId': '$milestoneuid'
#                     }, 
#                     'empId': {
#                         '$toObjectId': '$UpdatedBy'
#                     },
#                     'newField': {
#                         '$dateFromString': {
#                             'dateString': '$UpdatedAt', 
#                             'format': '%Y-%m-%d %H:%M:%S'
#                         }
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'SiteEngineer', 
#                     'localField': 'siteuid', 
#                     'foreignField': '_id', 
#                     'as': 'Siteresult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$Siteresult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'milestone', 
#                     'localField': 'milestoneuid', 
#                     'foreignField': '_id', 
#                     'as': 'milestoneresult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$milestoneresult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'userRegister', 
#                     'localField': 'empId', 
#                     'foreignField': '_id', 
#                     'as': 'empresult'
#                 }
#             }, {
#                 '$unwind': '$empresult'
#             }, {
#                 '$sort':{
#                     'newField':-1
#                 }
#             }, {
#                 '$project': {
#                     'siteName': '$Siteresult.Site Id', 
#                     'milestoneName': '$milestoneresult.Name', 
#                     'empemail': '$empresult.email', 
#                     'UpdatedAt': 1, 
#                     'updatedData': 1, 
#                     '_id': 0,
#                     "uniqueId":"1",
#                     'overall_table_count':1,
#                 }
#             }
#         ]
#         response = cmo.finding_aggregate("projectEventlog", arra)
#         return respond(response)
    
# @project_blueprint.route("/milestoneEventLog/<id>", methods=["GET", "POST"])
# def milestoneeventlog(id=None):
#     if request.method == "GET":
#         arra = [
#             {
#                 '$match': {
#                     'milestoneuid': id
#                 }
#             }
#         ]
#         arra = arra + apireq.countarra("projectEventlog",arra)+ apireq.args_pagination(request.args)
#         arra = arra +[{
#                 '$addFields': {
#                     'siteuid': {
#                         '$toObjectId': '$siteuid'
#                     }, 
#                     'milestoneuid': {
#                         '$toObjectId': '$milestoneuid'
#                     }, 
#                     'empId': {
#                         '$toObjectId': '$UpdatedBy'
#                     },
#                     'newField': {
#                         '$dateFromString': {
#                             'dateString': '$UpdatedAt', 
#                             'format': '%Y-%m-%d %H:%M:%S'
#                         }
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'SiteEngineer', 
#                     'localField': 'siteuid', 
#                     'foreignField': '_id', 
#                     'as': 'siteResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$siteResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'milestone', 
#                     'localField': 'milestoneuid', 
#                     'foreignField': '_id', 
#                     'as': 'milestoneResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$milestoneResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'userRegister', 
#                     'localField': 'empId', 
#                     'foreignField': '_id', 
#                     'as': 'empResult'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$empResult', 
#                     'preserveNullAndEmptyArrays': True
#                 }
#             }, {
#                 '$sort':{
#                     'newField':-1
#                 }
#             }, {
#                 '$project': {
#                     'siteName': '$siteResult.Site Id', 
#                     'milestoneName': '$milestoneResult.Name', 
#                     'empemail': '$empResult.email', 
#                     'UpdatedAt': 1, 
#                     'updatedData': 1, 
#                     'uniqueId': '1', 
#                     '_id': 0,
#                     'overall_table_count':1,
#                 }
#             }
#         ]
#         response = cmo.finding_aggregate("projectEventlog", arra)
#         return respond(response)


@project_blueprint.route("/projectEventLog/<id>", methods=["GET", "POST"])
def projecteventlog(id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'projectuid': id
                }
            },{
                '$sort':{
                    'UpdatedAt':-1
                }
            }
        ]
        arra = arra + apireq.countarra("projectEventlog",arra)+ apireq.args_pagination(request.args)
        arra = arra + [
            # {
            #     '$sort':{
            #         '_id':1
            #     }
            # }, 
            {
                '$addFields': {
                    'projectuid': {
                        '$toObjectId': '$projectuid'
                    }, 
                    'siteuid': {
                        '$toObjectId': '$siteuid'
                    }, 
                    'milestoneuid': {
                        '$toObjectId': '$milestoneuid'
                    }, 
                    'empId': {
                        '$toObjectId': '$UpdatedBy'
                    },
                    # 'newField': {
                    #     '$dateFromString': {
                    #         'dateString': '$UpdatedAt', 
                    #         'format': '%Y-%m-%d %H:%M:%S'
                    #     }
                    # }
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectuid', 
                    'foreignField': '_id', 
                    'as': 'projectResult'
                }
            }, {
                '$unwind': {
                    'path': '$projectResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteuid', 
                    'foreignField': '_id', 
                    'as': 'siteResult'
                }
            }, {
                '$unwind': {
                    'path': '$siteResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'milestoneuid', 
                    'foreignField': '_id', 
                    'as': 'milestoneResult'
                }
            }, {
                '$unwind': {
                    'path': '$milestoneResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'empId', 
                    'foreignField': '_id', 
                    'as': 'empResult'
                }
            }, {
                '$unwind': {
                    'path': '$empResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    'projectName': '$projectResult.projectId', 
                    'siteName': '$siteResult.Site Id', 
                    'milestoneName': '$milestoneResult.Name', 
                    'empemail': '$empResult.email', 
                    'UpdatedAt': 1, 
                    'updatedData': 1, 
                    'uniqueId': '1', 
                    '_id': 0,
                    'overall_table_count': 1
                }
            }
        ]
        # print(arra,"ytgctyedvtcvu")
        response = cmo.finding_aggregate("projectEventlog", arra)
        return respond(response)


@project_blueprint.route("/siteEventLog/<id>", methods=["GET", "POST"])
def siteeventlog(id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'siteuid': id
                }
            },
            {
                '$sort':{
                    'UpdatedAt':-1
                }
            }
        ]
        arra = arra + apireq.countarra("projectEventlog",arra)+ apireq.args_pagination(request.args)
        arra = arra +[
            {
                '$addFields': {
                    'siteuid': {
                        '$toObjectId': '$siteuid'
                    }, 
                    'milestoneuid': {
                        '$toObjectId': '$milestoneuid'
                    }, 
                    'empId': {
                        '$toObjectId': '$UpdatedBy'
                    },
                    # 'newField': {
                    #     '$dateFromString': {
                    #         'dateString': '$UpdatedAt', 
                    #         'format': '%Y-%m-%d %H:%M:%S'
                    #     }
                    # }
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteuid', 
                    'foreignField': '_id', 
                    'as': 'Siteresult'
                }
            }, {
                '$unwind': {
                    'path': '$Siteresult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'milestoneuid', 
                    'foreignField': '_id', 
                    'as': 'milestoneresult'
                }
            }, {
                '$unwind': {
                    'path': '$milestoneresult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'empId', 
                    'foreignField': '_id', 
                    'as': 'empresult'
                }
            }, {
                '$unwind': '$empresult'
            }, 
            # {
            #     '$sort':{
            #         'newField':-1
            #     }
            # }, 
            {
                '$project': {
                    'siteName': '$Siteresult.Site Id', 
                    'milestoneName': '$milestoneresult.Name', 
                    'empemail': '$empresult.email', 
                    'UpdatedAt': 1, 
                    'updatedData': 1, 
                    '_id': 0,
                    "uniqueId":"1",
                    'overall_table_count':1,
                }
            }
        ]
        # print(arra,"arrabdbcjydbcbdi")
        response = cmo.finding_aggregate("projectEventlog", arra)
        return respond(response)
    


@project_blueprint.route("/milestoneEventLog/<id>", methods=["GET", "POST"])
def milestoneeventlog(id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'milestoneuid': id
                }
            },
            {
                '$sort':{
                    'UpdatedAt':-1
                }
            }
        ]
        arra = arra + apireq.countarra("projectEventlog",arra)+ apireq.args_pagination(request.args)
        arra = arra +[{
                '$addFields': {
                    'siteuid': {
                        '$toObjectId': '$siteuid'
                    }, 
                    'milestoneuid': {
                        '$toObjectId': '$milestoneuid'
                    }, 
                    'empId': {
                        '$toObjectId': '$UpdatedBy'
                    },
                    # 'newField': {
                    #     '$dateFromString': {
                    #         'dateString': '$UpdatedAt', 
                    #         'format': '%Y-%m-%d %H:%M:%S'
                    #     }
                    # }
                }
            }, {
                '$lookup': {
                    'from': 'SiteEngineer', 
                    'localField': 'siteuid', 
                    'foreignField': '_id', 
                    'as': 'siteResult'
                }
            }, {
                '$unwind': {
                    'path': '$siteResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'milestoneuid', 
                    'foreignField': '_id', 
                    'as': 'milestoneResult'
                }
            }, {
                '$unwind': {
                    'path': '$milestoneResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'empId', 
                    'foreignField': '_id', 
                    'as': 'empResult'
                }
            }, {
                '$unwind': {
                    'path': '$empResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, 
            # {
            #     '$sort':{
            #         'newField':-1
            #     }
            # }, 
            {
                '$project': {
                    'siteName': '$siteResult.Site Id', 
                    'milestoneName': '$milestoneResult.Name', 
                    'empemail': '$empResult.email', 
                    'UpdatedAt': 1, 
                    'updatedData': 1, 
                    'uniqueId': '1', 
                    '_id': 0,
                    'overall_table_count':1,
                }
            }
        ]
        print(arra,"arrabdbcjywgdggdbcbdi")
        response = cmo.finding_aggregate("projectEventlog", arra)
        return respond(response)
 

@project_blueprint.route("/mappedData/<projectuid>", methods=["GET"])
def mapped_data(projectuid=None):
    arra = [
        {
            '$match': {
                '_id': ObjectId(projectuid)
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
                'projectType': '$result.projectType'
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)['data'][0]
    projectTypeName = response['projectType']
    
    
    
    arra = [
        {
            '$match': {
                'projectTypeName': projectTypeName
            }
        }, {
            '$project': {
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("mappedData",arra)
    return respond(response)


@project_blueprint.route("/circlewithPG/<projectuid>", methods=["GET"])
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


@project_blueprint.route("/notification", methods=["GET"])
def notification():
    arra = [
        {
            '$sort':{
                '_id':-1
            }
        }, {
            '$project': {
                'msg': {
                    '$concat': [
                        '$msg', ' ', '$status'
                    ]
                }, 
                'time': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("notification",arra)
    return respond(response)

@project_blueprint.route("/project/accuralRevenueMaster",methods=['GET','POST','DELETE'])
@project_blueprint.route("/project/accuralRevenueMaster/<id>",methods=['GET','POST','DELETE']) 
@token_required  
def accuralRevenueMaster(current_user,id=None):
    if request.method == "GET":
        arr = []
        if request.args.get("customer")!=None and request.args.get("customer")!='undefined':
            arr = arr + [
                {
                    '$match':{
                        'customer':ObjectId(request.args.get("customer"))
                    }
                }
            ]
        if request.args.get("projectType")!=None and request.args.get("projectType")!='undefined':
            projectType = request.args.get("projectType").split(",")
            subproject = []
            for i in projectType:
                subproject.append(ObjectId(i))
            arr = arr + [
                {
                    '$match':{
                        'subProject':{
                            '$in':subproject
                        }
                    }
                }
            ]
        arr=arr+[
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
                '$project': {
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
                    },
                    'customerName':{
                        '$arrayElemAt':['$customerResult.customerName',0]
                    },
                    '_id':0
                }
            }, {
                '$sort': {
                    'projectTypeName': 1
                }
            }
        ]
        if request.args.get("projectId")!=None and request.args.get("projectId")!='undefined':
            arr = arr + [
                {
                    '$match':{
                        'projectId':{
                            '$regex':request.args.get("projectId").strip(),
                            '$options': 'i'
                        }
                    }
                }
            ]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        Response=cmo.finding_aggregate("AccuralRevenueMaster",arr)
        return respond(Response)
    
    if request.method == "POST":
        if id == None:
            allData=request.get_json()
            allData = {key:value.strip() if isinstance(value,str) else value for key,value in allData.items()}
            ObjectArray = ['customer',"projectType","project","subProject"]
            for i in ObjectArray:
                if i in allData:
                    allData[i] = ObjectId(allData[i])
            try:
                allData['rate'] = int(allData['rate'])
            except Exception as e:
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Please Provide a valid Rate"
                })
            Response=cmo.insertion('AccuralRevenueMaster',allData)
            return respond(Response)    
         
        if id != None:
            data=request.get_json()   
            tempKeys = ["customerName",'projectId', 'projectTypeName', 'subProjectName']
            for i in tempKeys:
                if i in data:
                    del data[i]
            try:
                data['rate']= int(data['rate'])
            except Exception as e:
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Please Provide a valid Rate"
                })
            Response=cmo.updating('AccuralRevenueMaster',{'_id':ObjectId(id)},data,False)
            return respond(Response)
            
    if request.method == "DELETE":
        if id != None:
            res = cmo.deleting("AccuralRevenueMaster",id,current_user['userUniqueId'])
            return respond(res)
        else:
            return respond({
                'status':400,
                "icon":"error",
                "msg":"Please provide a valid id"
            })
            
@project_blueprint.route("/project/accuralRevenueMaster/projectType",methods=['GET','POST'])
@project_blueprint.route("/project/accuralRevenueMaster/projectType/<id>",methods=['GET','POST'])
@token_required
def accuralRevenueMasterProjectType(current_user,id=None):
    if request.method == "GET":
        arr=[
            {
                '$match': {
                    'deleteStatus': {'$ne': 1},
                    "custId":id
                }
            }, {
                '$group': {
                    '_id': '$projectType', 
                    'doc': {
                        '$first': '$$ROOT'
                    }
                }
            }, {
                '$project': {
                    'projectType': {
                        '$toString': '$doc._id'
                    }, 
                    'projectTypeName': '$_id', 
                    '_id': 0
                }
            },{
                '$sort':{
                    'projectTypeName':1
                }
            }
        ]
        Response=cmo.finding_aggregate("projectType",arr)
        return respond(Response)
    
@project_blueprint.route("/project/accuralRevenueMaster/subProjectType",methods=['GET','POST'])
@project_blueprint.route("/project/accuralRevenueMaster/subProjectType/<id>",methods=['GET','POST'])
@token_required
def accuralRevenueMasterSubProjects(current_user,id=None):
    if request.method == "GET":
        arra=[
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$project': {
                    'projectType': 1, 
                    "custId":1,
                    '_id': 0
                }
            },{
                '$lookup':{
                    'from':"projectType",
                    'let':{
                        'projectType':"$projectType",
                        "customerId":"$custId"
                    },
                    'pipeline':[
                        {
                            '$match':{
                                'deleteStatus':{'$ne':1}
                            }
                        }, {
                            '$match':{
                                '$expr':{
                                    '$and':[
                                        {
                                            '$eq':['$projectType','$$projectType']
                                        },
                                        {
                                            '$eq':['$custId',"$$customerId"]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    "as":"result"
                }
            },{
                '$unwind':{
                    'path':'$result',
                    "preserveNullAndEmptyArrays":True
                }
            }, {
                '$replaceRoot':{
                    'newRoot':'$result'
                }
            }, {
                '$project':{
                    'subProject': {
                        '$toString': '$_id'
                    }, 
                    'subProjectName': "$subProject",
                    "_id":0
                }
            }
        ]
        Response=cmo.finding_aggregate("projectType",arra)
        return respond(Response)
        
            
             
@project_blueprint.route("/project/accuralRevenueMaster/projects",methods=['GET','POST'])
@project_blueprint.route("/project/accuralRevenueMaster/projects/<id>",methods=['GET','POST'])
@token_required
def accuralRevenueMasterprojects(current_user,id=None):
    if request.method == "GET":
        arra=[
            {
                '$match': {
                    'deleteStatus': {'$ne': 1},
                    "projectType":ObjectId(id)
                }
            }, {
                '$project': {
                    'project': {
                        '$toString': '$_id'
                    }, 
                    'projectId': 1, 
                    '_id': 0
                }
            }, {
                '$sort':{
                    'projectId':1
                }
            }
        ]
        Response=cmo.finding_aggregate("project",arra)
        return respond(Response)



# @project_blueprint.route("/compliance/globalSaver", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
# @project_blueprint.route("/compliance/globalSaver/<siteId>/<milestoneId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
# @token_required
# def compliance_global_saver(current_user,siteId=None,milestoneId=None):
#     if request.method == "GET":
#         arra = [
#             {
#                 '$match': {
#                     'siteuid': ObjectId(siteId), 
#                     'milestoneuid': ObjectId(milestoneId)
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'userRegister', 
#                     'localField': 'L1UserId', 
#                     'foreignField': '_id', 
#                     'pipeline': [
#                         {
#                             '$addFields': {
#                                 'empName': {
#                                     '$toString': '$empName'
#                                 }, 
#                                 'empCode': {
#                                     '$toString': '$empCode'
#                                 }
#                             }
#                         }, {
#                             '$addFields': {
#                                 'approverName': {
#                                     '$concat': [
#                                         '$empName', '(', '$empCode', ')'
#                                     ]
#                                 }
#                             }
#                         }
#                     ], 
#                     'as': 'result'
#                 }
#             }, {
#                 '$addFields': {
#                     'L1UserName': {
#                         '$arrayElemAt': [
#                             '$result.approverName', 0
#                         ]
#                     },
#                     "L1UserId":{
#                         '$toString':"$L1UserId"
#                     }
#                 }
#             }, {
#                 '$project': {
#                     'TemplateData': 1, 
#                     'PlanDetailsData': 1, 
#                     'SiteDetailsData': 1, 
#                     'RanCheckListData': 1, 
#                     'AcceptanceLogData': 1, 
#                     'L1UserId': 1, 
#                     "SnapData":1,
#                     "currentStatus":1,
#                     "L1UserName":1,
#                     "subProjectName":1,
#                     "tbAnteena":1,
#                     "existingAntenna":1,
#                     "radio":1,
#                     "bbuCard":1,
#                     "miscMaterial":1,
#                     'Template': 1,
#                     '_id': 0
#                 }
#             }
#         ]
#         res = cmo.finding_aggregate("complianceApproverSaver",arra)
#         return respond(res)
    
#     if request.method == "PATCH":
#         forFile=request.args.get("forFile") == "true"
#         allData={}
#         imagePath=""

#         if forFile:
#             allData= request.form.to_dict()
#             imagePath=cform.singleFileSaver(request.files.get("image"),"uploads/uploadSnapImage",['png','jpg','jpeg',"PNG"])
#             if imagePath['status'] != 200:
#                 return respond({
#                     'status':imagePath['status'],
#                     "icon":"error",
#                     "msg":imagePath['msg']
#                 })
#             imagePath = imagePath['msg']
#             aggr=[
#                 {
#                 '$match': {
#                     'siteuid':ObjectId(allData['siteuid']),
#                     'milestoneuid':ObjectId(allData['milestoneuid'])
#                 }
#             }
#             ]
#             fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)

#             allImage=[]
#             isExist=False
#             approved=[]
#             if len(fetchData['data']):
                
#                 if "SnapData" in fetchData['data'][0]:
                    
#                     if allData['fieldName'] in fetchData['data'][0]['SnapData']:
                        
#                         if "approvedIndex" in fetchData['data'][0]['SnapData'][allData['fieldName']]:
#                             approved=fetchData['data'][0]['SnapData'][allData['fieldName']]['approvedIndex']
#                         for item in fetchData['data'][0]['SnapData'][allData['fieldName']]['images']:
                            
#                             if item["index"] == allData['index']:
#                                 if fetchData['data'][0]['SnapData'][allData['fieldName']].get('approvedIndex',[]) and item['index'] in fetchData['data'][0]['SnapData'][allData['fieldName']].get('approvedIndex',[]):
#                                         return respond({"status": 400, "msg": f"The Image at Index-{item['index']} can't be updated because it has already been approved.", "icon": "error"})
#                                 fullPathOfImage = os.path.join(os.getcwd(),item["image"])
#                                 if os.path.exists(fullPathOfImage):
#                                     os.remove(fullPathOfImage)
                                    

#                                 isExist=True
#                                 item.update({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                 })
#                             allImage.append(item)
#                         if isExist == False:
#                             allImage.append({
#                                         "index": allData['index'],
#                                         "image": imagePath
#                                         })
#                         # allData[f"SnapData.{allData['fieldName']}"] = allImage
#                     else:
#                         # allData[f"SnapData.{allData['fieldName']}"] = [{
#                                     # "index": allData['index'],
#                                     # "image": imagePath
#                                     # }]
#                         allImage.append({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                     })
#                 else:
#                     # allData[f"SnapData.{allData['fieldName']}"] = [{
#                     #                 "index": allData['index'],
#                     #                 "image": imagePath
#                     #                 }]
#                     allImage.append({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                     })
#             else:
#                 allImage.append({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                     })
#             allData[f"SnapData.{allData['fieldName']}"]={"images":allImage,"approvedIndex":approved}
        
#             del allData['index']
#             del allData['fieldName']
            
        
#         else:
#             allData = request.get_json()
#         if "formType" in allData and allData['formType'] == "Static":
#             convertObjectIdKeys=["milestoneuid","siteuid","projectuniqueId","subprojectId","userId"]
#         else:
#             convertObjectIdKeys=["milestoneuid","siteuid","projectuniqueId","subprojectId","userId","L1UserId"] 
#         for key in convertObjectIdKeys:
#             if key in allData:
#                 try:
#                     allData[key]=ObjectId(allData[key])
#                 except :
#                     return respond({"status":400,"msg":"Provide Valid Id","icon":"error"})
#             elif key not in allData and key !="userId":
#                 return respond({"status":400,"msg":"Please Fill All Required Fields","icon":"error"})

#         updateBy = {
#             'siteuid':ObjectId(allData['siteuid']),
#             'milestoneuid':ObjectId(allData['milestoneuid'])
#         }
#         res = cmo.updating("complianceApproverSaver",updateBy,allData,True,unset={"L1ActionDate":1,"L2ActionDate":1,"formSubmitDate":1,"AirtelActionDate":1})
#         res['imagePath'] =imagePath
#         cmo.updating("milestone",{"_id":ObjectId(allData['milestoneuid'])},{'mileStoneStatus':allData['currentStatus']},False)
#         return respond(res)
    
@project_blueprint.route("/compliance/globalSaver", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@project_blueprint.route("/compliance/globalSaver/<siteId>/<milestoneId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
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

# @project_blueprint.route("/compliance/globalSaver/Approved/<globalSaverId>", methods=["GET","POST","PATCH"])
# @token_required
# def compliance_global_saver_Approved(current_user,globalSaverId=None):
    
#     if request.method == "GET":
#         arra = [
#             {
#                 '$match': {
#                     '_id': ObjectId(globalSaverId),
#                 }
#             }, {
#                 '$addFields':{
#                     "L1UserId":{
#                         '$toString':"$L1UserId"
#                     }
#                 }
#             }, {
#                 '$project': {
#                     'TemplateData': 1, 
#                     'PlanDetailsData': 1, 
#                     'SiteDetailsData': 1, 
#                     'RanCheckListData': 1, 
#                     'AcceptanceLogData': 1, 
#                     'L1UserId': 1, 
#                     "SnapData":1,
#                     "currentStatus":1,
#                     '_id': 0
#                 }
#             }
#         ]
#         res = cmo.finding_aggregate("complianceApproverSaver",arra)
#         return respond(res)
    
#     elif request.method == "POST":
#         reqData = request.get_json()
#         if not all([reqData.get("fieldName"),reqData.get("approvedIndex"),globalSaverId]) :
#             return respond({"status":400,"msg":"Provide Valid Data","icon":"error"})
#         aggr=[
#             {
#                 '$match': {
#                     '_id': ObjectId(globalSaverId)
#                 }
#             }, {
#                 '$addFields': {
#                      'L2UserId': {
#                 '$ifNull': [
#                     {
#                         '$toString': '$L2UserId'
#                     }, ''
#                 ]
#             }, 
#             'L1UserId': {
#                 '$ifNull': [
#                     {
#                         '$toString': '$L1UserId'
#                     }, ''
#                 ]
#             },
#                     'approvedIndex': {"$ifNull":[f"$SnapData.{reqData['fieldName']}.approvedIndex",[]]}, 
#                     'isExist': {
#                         '$cond': [
#                             {
#                                 '$in': [
#                                     reqData['approvedIndex'], {"$ifNull":[f"$SnapData.{reqData['fieldName']}.approvedIndex",[]]}
#                                 ]
#                             }, True, False
#                         ]
#                     }
#                 }
#             }
#         ]
#         fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)
#         if not len(fetchData['data']):
#             return respond({"status":204,"msg":"Data not exist","icon":"error"})
#         approvedIndex=fetchData['data'][0]['approvedIndex']
#         print(reqData,"reqDatareqDatareqData")
#         setObjForLog={
#             "userId":ObjectId(current_user['userUniqueId']),
#             "globalSaverId":ObjectId(globalSaverId),
#             "type":reqData.get("type",""),
#             "event":"",
#             "currentTime":current_time().replace("T"," ")
#         }
        
#         if fetchData['data'][0]['isExist']:
#             approvedIndex.pop(approvedIndex.index(reqData['approvedIndex']))
#             setObjForLog['event'] = f"The image with field name '{reqData['fieldName']}' at index {reqData['approvedIndex']} has been disapproved."
#         else:
#             approvedIndex.append(reqData['approvedIndex'])
#             setObjForLog['event'] = f"The image with field name '{reqData['fieldName']}' at index {reqData['approvedIndex']} has been approved."
#         cmo.insertion("complianceLog",setObjForLog)
#         res=cmo.updating("complianceApproverSaver",{"_id":ObjectId(globalSaverId)},{f"SnapData.{reqData['fieldName']}.approvedIndex":approvedIndex})
#         return respond(res)

#     elif request.method == "PATCH":
#         forFile=request.args.get("forFile") == "true"
#         allData={}
#         imagePath=""

#         if forFile:
#             allData= request.form.to_dict()
#             # imagePath=cform.singleFileSaver(request.files.get("image"),"uploads/uploadSnapImage",['png'])['msg']
#             imagePath=cform.singleFileSaver(request.files.get("image"),"uploads/uploadSnapImage",['png','jpg','jpeg',"PNG"])
#             if imagePath['status'] != 200:
#                 return respond({
#                     'status':imagePath['status'],
#                     "icon":"error",
#                     "msg":imagePath['msg']
#                 })
#             imagePath = imagePath['msg']
#             aggr=[
#                 {
#                     '$match': {
#                         '_id':ObjectId(globalSaverId),
#                     }
#                 }
#             ]
#             fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)

#             allImage=[]
#             isExist=False
#             approved=[]
#             if len(fetchData['data']):
                
#                 if "SnapData" in fetchData['data'][0]:
                    
#                     if allData['fieldName'] in fetchData['data'][0]['SnapData']:
                        
#                         if "approvedIndex" in fetchData['data'][0]['SnapData'][allData['fieldName']]:
#                             approved=fetchData['data'][0]['SnapData'][allData['fieldName']]['approvedIndex']
#                         for item in fetchData['data'][0]['SnapData'][allData['fieldName']]['images']:
                            
#                             if item["index"] == allData['index']:
#                                 if fetchData['data'][0]['SnapData'][allData['fieldName']].get('approvedIndex',[]) and item['index'] in fetchData['data'][0]['SnapData'][allData['fieldName']].get('approvedIndex',[]):
#                                         return respond({"status": 400, "msg": f"The image at Index-{item['index']} can't be updated because it has already been approved.", "icon": "error"})
                                
#                                 fullPathOfImage = os.path.join(os.getcwd(),item["image"])
#                                 if os.path.exists(fullPathOfImage):
#                                     os.remove(fullPathOfImage)
#                                 isExist=True
#                                 item.update({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                 })
#                             allImage.append(item)
#                         if isExist == False:
#                             allImage.append({
#                                         "index": allData['index'],
#                                         "image": imagePath
#                                         })
#                         # allData[f"SnapData.{allData['fieldName']}"] = allImage
#                     else:
#                         # allData[f"SnapData.{allData['fieldName']}"] = [{
#                                     # "index": allData['index'],
#                                     # "image": imagePath
#                                     # }]
#                         allImage.append({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                     })
#                 else:
#                     # allData[f"SnapData.{allData['fieldName']}"] = [{
#                     #                 "index": allData['index'],
#                     #                 "image": imagePath
#                     #                 }]
#                     allImage.append({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                     })
#             else:
#                 allImage.append({
#                                     "index": allData['index'],
#                                     "image": imagePath
#                                     })
#             allData[f"SnapData.{allData['fieldName']}"]={"images":allImage,"approvedIndex":approved}
        
#             del allData['index']
#             del allData['fieldName']
            
        
#         else:
#             allData = request.get_json()
#         # convertObjectIdKeys=["milestoneuid","siteuid","projectuniqueId","subprojectId","userId","L1UserId"] 
#         # for key in convertObjectIdKeys:
#         #     if key in allData:
#         #         try:
#         #             allData[key]=ObjectId(allData[key])
#         #         except :
#         #             return respond({"status":400,"msg":"Provide Valid Id","icon":"error"})
#         #     elif key not in allData and key !="userId":
#         #         return respond({"status":400,"msg":"Please Fill All Required Fields","icon":"error"})

#         updateBy = {
#             '_id':ObjectId(globalSaverId),
#         }
#         res = cmo.updating("complianceApproverSaver",updateBy,allData,True)
#         res['imagePath']=imagePath
#         return respond(res)
    
   

@project_blueprint.route("/compliance/globalSaver/Approved/<globalSaverId>", methods=["GET","POST","PATCH"])
@token_required
def compliance_global_saver_Approved(current_user,globalSaverId=None):
    
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(globalSaverId),
                }
            }, {
                '$addFields':{
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
                    '_id': 0
                }
            }
        ]
        res = cmo.finding_aggregate("complianceApproverSaver",arra)
        return respond(res)
    
    elif request.method == "POST":
        reqData = request.get_json()
        if not all([reqData.get("fieldName"),reqData.get("approvedIndex") or (reqData.get("disApprovedIndex") and reqData.get("remark")) ,globalSaverId]) :
            return respond({"status":400,"msg":"Provide Valid Data","icon":"error"})
        
        forApprover = reqData.get("approvedIndex",False)
        forDisapprover = reqData.get("disApprovedIndex",False)
        approvedIndex = []
        disApprovedIndex = []
        remarks = []

        aggr = [
            {
                '$match': {
                    '_id': ObjectId(globalSaverId)
                }
            }, {
                '$project': {
                    'SnapData': 1, 
                    '_id': 0
                }
            }
        ]
        fetchData = cmo.finding_aggregate("complianceApproverSaver",aggr)
        if not len(fetchData['data']):
            return respond({"status":204,"msg":"Snap Not found, Contact to PMO Team","icon":"error"})
        
        setObjForLog={
            "userId":ObjectId(current_user['userUniqueId']),
            "globalSaverId":ObjectId(globalSaverId),
            "type":reqData.get("type",""),
            "event":"",
            "currentTime":current_time().replace("T"," ")
        }

        if forApprover:
            if 'approvedIndex' in fetchData['data'][0]['SnapData'][reqData["fieldName"]]:
                approvedIndex=fetchData['data'][0]['SnapData'][reqData["fieldName"]]['approvedIndex']
            if "disApprovedIndex" in fetchData['data'][0]['SnapData'][reqData["fieldName"]]:
                disApprovedIndex=fetchData['data'][0]['SnapData'][reqData["fieldName"]]['disApprovedIndex']
            if "remarks" in fetchData['data'][0]['SnapData'][reqData["fieldName"]]:
                remarks=fetchData['data'][0]['SnapData'][reqData["fieldName"]]['remarks']
            if reqData['approvedIndex'] in disApprovedIndex:
                disApprovedIndex.remove(reqData['approvedIndex'])
            if remarks:
                for i, item in enumerate(remarks):
                    if item['index'] == reqData['approvedIndex']:
                        del remarks[i]
                        break
            approvedIndex.append(reqData['approvedIndex'])
            setObjForLog['event'] = f"The image with field name '{reqData['fieldName']}' at index {reqData['approvedIndex']} has been approved."
            cmo.insertion("complianceLog",setObjForLog)
            res=cmo.updating("complianceApproverSaver",{"_id":ObjectId(globalSaverId)},{f"SnapData.{reqData['fieldName']}.approvedIndex":approvedIndex,f"SnapData.{reqData['fieldName']}.disApprovedIndex":disApprovedIndex,f"SnapData.{reqData['fieldName']}.remarks":remarks})
            return respond(res)
        
        elif forDisapprover:
            if 'approvedIndex' in fetchData['data'][0]['SnapData'][reqData["fieldName"]]:
                approvedIndex=fetchData['data'][0]['SnapData'][reqData["fieldName"]]['approvedIndex']
            if "disApprovedIndex" in fetchData['data'][0]['SnapData'][reqData["fieldName"]]:
                disApprovedIndex=fetchData['data'][0]['SnapData'][reqData["fieldName"]]['disApprovedIndex']
            if "remarks" in fetchData['data'][0]['SnapData'][reqData["fieldName"]]:
                remarks=fetchData['data'][0]['SnapData'][reqData["fieldName"]]['remarks']

            if reqData['disApprovedIndex'] in approvedIndex:
                approvedIndex.remove(reqData['disApprovedIndex'])

            disApprovedIndex.append(reqData['disApprovedIndex'])

            remarks.append({
                'index':reqData['disApprovedIndex'],
                "remark":reqData['remark']
            })
            
            setObjForLog['event'] = f"The image with field name '{reqData['fieldName']}' at index {reqData['disApprovedIndex']} has been Rejected."
            cmo.insertion("complianceLog",setObjForLog)
            res=cmo.updating("complianceApproverSaver",{"_id":ObjectId(globalSaverId)},{f"SnapData.{reqData['fieldName']}.approvedIndex":approvedIndex,f"SnapData.{reqData['fieldName']}.disApprovedIndex":disApprovedIndex,f"SnapData.{reqData['fieldName']}.remarks":remarks})
            return respond(res)
        else:
            return respond({
                'status':400,
                "msg":"Something went wrong, Please Contact to PMO Team",
                "icon":"error"
            })

    elif request.method == "PATCH":
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
                        '_id':ObjectId(globalSaverId),
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

        updateBy = {
            '_id':ObjectId(globalSaverId),
        }
        res = cmo.updating("complianceApproverSaver",updateBy,allData,True)
        res['imagePath']=imagePath
        return respond(res) 
  
    
    
@project_blueprint.route("/admin/complianceMilestoneL1Approver/<mName>/<approverType>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def admin_compllinace_milestone_L1_approver(current_user,mName=None,approverType=None):
    arra = [
        {
            '$match': {
                'milestoneName': mName, 
                'L1UserId': ObjectId(current_user['userUniqueId']),
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
                "pipeline":[
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
                'siteIdName': {
                    '$arrayElemAt': [
                        '$siteResult.Site Id', 0
                    ]
                }, 
                'activity': {
                    '$arrayElemAt': [
                        '$siteResult.ACTIVITY', 0
                    ]
                }, 
                'systemId': {
                    '$arrayElemAt': [
                        '$siteResult.systemId', 0
                    ]
                }, 
                'Unique ID': {
                    '$arrayElemAt': [
                        '$siteResult.Unique ID', 0
                    ]
                }, 
                'milestoneName': {
                    '$arrayElemAt': [
                        '$milestoneResult.Name', 0
                    ]
                }, 
                'projectTypeName': {
                    '$arrayElemAt': [
                        '$pResult.projectType', 0
                    ]
                }, 
                'subTypeName': {
                    '$arrayElemAt': [
                        '$pResult.subProject', 0
                    ]
                }, 
                'projectIdName': {
                    '$arrayElemAt': [
                        '$projectResult.projectId', 0
                    ]
                }, 
                'projectGroupName': {
                    '$arrayElemAt': [
                        '$projectResult.projectGroup', 0
                    ]
                }, 
                'siteuid': {
                    '$toString': '$siteuid'
                }, 
                'milestoneuid': {
                    '$toString': '$milestoneuid'
                }, 
                'srNumber': '$TemplateData.SR Number',
                "L1Age" :{"$divide":[{"$subtract":[{
                                '$toDate': {"$ifNull":['$L1ActionDate',current_time1()]}
                            },{"$toDate":"$formSubmitDate"}]},86400000]
                },
                "L2Age" :{"$divide":[{"$subtract":[{
                                '$toDate': {"$ifNull":['$L2ActionDate',current_time1()]}
                            },{"$toDate":"$L1ActionDate"}]},86400000]
                },
            }
        }, {
            '$project': {
                'siteuid': 1, 
                'milestoneuid': 1, 
                'siteIdName': 1, 
                'milestoneName': 1, 
                'srNumber': 1, 
                'projectTypeName': 1, 
                'subTypeName': 1, 
                'projectIdName': 1, 
                "projectGroupName":1,
                'activity': 1, 
                'systemId': 1, 
                'Unique ID': 1, 
                'uniqueId': {
                    '$toString': '$_id'
                }, 
                "SnapData":1,
                "currentStatus":1,
                "formSubmitDate":1,
                "L1ActionDate":1,
                "L2ActionDate":1,
                "L1Age":1,
                "L2Age":1,
                '_id': 0
            }
        }
    ]
    arra = arra + apireq.countarra("complianceApproverSaver",arra) + apireq.args_pagination(request.args)
    res = cmo.finding_aggregate("complianceApproverSaver",arra)
    return respond(res)



@project_blueprint.route("/admin/complianceMilestoneL2Approver/<mName>/<approverType>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def admin_compllinace_milestone_L2_approver(current_user,mName=None,approverType=None):
    arra = [
        {
            '$match': {
                'milestoneName': mName, 
                'L2UserId': ObjectId(current_user['userUniqueId']),
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
                'as': 'projectResult'
            }
        }, {
            '$addFields': {
                'siteIdName': {
                    '$arrayElemAt': [
                        '$siteResult.Site Id', 0
                    ]
                }, 
                'activity': {
                    '$arrayElemAt': [
                        '$siteResult.ACTIVITY', 0
                    ]
                }, 
                'systemId': {
                    '$arrayElemAt': [
                        '$siteResult.systemId', 0
                    ]
                }, 
                'Unique ID': {
                    '$arrayElemAt': [
                        '$siteResult.Unique ID', 0
                    ]
                }, 
                'milestoneName': {
                    '$arrayElemAt': [
                        '$milestoneResult.Name', 0
                    ]
                }, 
                'projectTypeName': {
                    '$arrayElemAt': [
                        '$pResult.projectType', 0
                    ]
                }, 
                'subTypeName': {
                    '$arrayElemAt': [
                        '$pResult.subProject', 0
                    ]
                }, 
                'projectIdName': {
                    '$arrayElemAt': [
                        '$projectResult.projectId', 0
                    ]
                }, 
                'siteuid': {
                    '$toString': '$siteuid'
                }, 
                'milestoneuid': {
                    '$toString': '$milestoneuid'
                }, 
                'srNumber': '$TemplateData.Parent SR No',
                "L1Age" :{"$divide":[{"$subtract":[{
                                '$toDate': {"$ifNull":['$L1ActionDate',current_time1()]}
                            },{"$toDate":"$formSubmitDate"}]},86400000]
                },
                "L2Age" :{"$divide":[{"$subtract":[{
                                '$toDate': {"$ifNull":['$L2ActionDate',current_time1()]}
                            },{"$toDate":"$L1ActionDate"}]},86400000]
                },
            }
        }, {
            '$project': {
                'siteuid': 1, 
                'milestoneuid': 1, 
                'siteIdName': 1, 
                'milestoneName': 1, 
                'srNumber': 1, 
                'projectTypeName': 1, 
                'subTypeName': 1, 
                'projectIdName': 1, 
                'activity': 1, 
                'systemId': 1, 
                'Unique ID': 1, 
                'uniqueId': {
                    '$toString': '$_id'
                }, 
                "SnapData":1,
                "currentStatus":1,
                "formSubmitDate":1,
                "L1ActionDate":1,
                "L2ActionDate":1,
                "L1Age":1,
                "L2Age":1,
                '_id': 0
            }
        }
            
    ]
    arra = arra + apireq.countarra("complianceApproverSaver",arra) + apireq.args_pagination(request.args)
    res = cmo.finding_aggregate("complianceApproverSaver",arra)
    return respond(res)


@project_blueprint.route("/admin/approverAction/<uniqueId>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"])
@token_required
def admin_approver_action(current_user,uniqueId=None):
    setObjForLog={
            "userId":ObjectId(current_user['userUniqueId']),
            "globalSaverId":ObjectId(uniqueId),
            "type":"",
            "event":"",
            "currentTime":current_time().replace("T"," ")
        }
    if request.method == "PATCH":
        allData = request.get_json()
        setObjForLog["type"] = allData.get("type","")
        if allData['type'] == "L1":
            milestoneId = allData['milestoneuid']
            allData['L2UserId'] = ObjectId(allData['L2UserId'])
            setObjForLog['event'] = f"This compliance has been approved and forward to L2 Approver ({allData['approverName']})."
            setObjForLog['forwadToId'] = ObjectId(allData['L2UserId'])
            for key in ["milestoneuid","type","approverName"]:
                if key in allData:
                    del allData[key]
            allData['L1ActionDate'] = current_time1()
            res = cmo.updating("complianceApproverSaver",{'_id':ObjectId(uniqueId)},allData,False)
            cmo.updating("milestone",{'_id':ObjectId(milestoneId)},{"mileStoneStatus":"Submit"},False)
            cmo.insertion("complianceLog",setObjForLog)
            return respond(res)

        elif allData['type'] == "L2":
            data = {
                'L2ActionDate':current_time1(),
                "currentStatus":allData['currentStatus'],
            }
            setObjForLog['event'] = f"This compliance has been approved and forward to Airtel."
            res = cmo.updating("complianceApproverSaver",{'_id':ObjectId(uniqueId)},data,False)
            cmo.updating("milestone",{'_id':ObjectId(allData['milestoneuid'])},{"mileStoneStatus":allData['currentStatus']},False)
            cmo.insertion("complianceLog",setObjForLog)
            return respond(res)

    if request.method == "POST":
        allData = request.get_json()
        data = {
            'rejectedBy':allData['type'],
            'currentStatus':allData['currentStatus'],
            "rejectedById":ObjectId(current_user['userUniqueId']),
        }
        setObjForLog["type"] = allData.get("type","")
        setObjForLog['event'] = f"This compliance has been rejected by {allData['type']} Approver."
        setObjForLog["rejectedById"] = ObjectId(current_user['userUniqueId'])
        res = cmo.updating("complianceApproverSaver",{'_id':ObjectId(uniqueId)},data,False)
        cmo.updating("milestone",{'_id':ObjectId(allData['milestoneuid'])},{'mileStoneStatus':allData['currentStatus']},False)
        cmo.insertion("complianceLog",setObjForLog)
        return respond(res)
    
    
@project_blueprint.route("/complianceLog/<globalSaverId>", methods=["GET"])
@token_required
def compliance_log(current_user,globalSaverId=None):
    arra = [
        {
            '$match': {
                'globalSaverId': ObjectId(globalSaverId)
            }
        }, {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'userId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            'status': 'Active'
                        }
                    }, {
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
                            'userName': {
                                '$concat': [
                                    '$empName', '(', '$empCode', ')'
                                ]
                            }
                        }
                    }
                ], 
                'as': 'userResult'
            }
        }, {
            '$lookup': {
                'from': 'complianceApproverSaver', 
                'localField': 'globalSaverId', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$project': {
                            'siteuid': 1, 
                            'milestoneName': 1, 
                            '_id': 0
                        }
                    }, {
                        '$lookup': {
                            'from': 'SiteEngineer', 
                            'localField': 'siteuid', 
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
                            'as': 'siteResult'
                        }
                    }, {
                        '$addFields': {
                            'siteIdName': {
                                '$arrayElemAt': [
                                    '$siteResult.Site Id', 0
                                ]
                            }
                        }
                    }
                ], 
                'as': 'ComplianceResult'
            }
        }, {
            '$addFields': {
                'userName': {
                    '$arrayElemAt': [
                        '$userResult.userName', 0
                    ]
                }, 
                'siteIdName': {
                    '$arrayElemAt': [
                        '$ComplianceResult.siteIdName', 0
                    ]
                }, 
                'milestoneName': {
                    '$arrayElemAt': [
                        '$ComplianceResult.milestoneName', 0
                    ]
                },
                'uniqueId': {
                    '$toString': '$_id'
                },
            }
        }, {
            '$project': {
                'siteIdName': 1, 
                'milestoneName': 1, 
                'userName': 1, 
                'currentTime': 1, 
                'event': 1, 
                "uniqueId":1,
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("complianceLog",arra)
    return respond(response)


@project_blueprint.route("/partner_group_milestone", methods=["PATCH","POST"])
@token_required
def partner_group_milestone(current_user):
    if request.method == "PATCH":
        allData = request.get_json()
        allData['groupMilestone'] = allData['groupMilestone'].split(',')
        notOpenTask = []
        notFoundTask = []
        for siteuid in allData['siteId']:
            for name in allData['groupMilestone']:
                arra = [
                    {
                        '$match':{
                            'siteId':ObjectId(siteuid),
                            "Name":name
                        }
                    }
                ]
                resp = cmo.finding_aggregate("milestone",arra)['data']
                if len(resp)>0:
                    taskStatus = resp[0]['mileStoneStatus']
                    if taskStatus!="Open":
                        arra = [
                            {
                                '$match':{
                                    '_id':ObjectId(siteuid)
                                }
                            }
                        ]
                        siteName = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['Site Id']
                        notOpenTask.append(f"{siteName} -> {name}")
                else:
                    arra = [
                        {
                            '$match':{
                                '_id':ObjectId(siteuid)
                            }
                        }
                    ]
                    siteName = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['Site Id']
                    notFoundTask.append(f"{siteName} - {name}")
                    
        if len(notFoundTask)>0:
            return respond({
                'status':400,
                "icon":"error",
                "msg": "Pair Of Site Id And Task is not found in DataBase\n" + "\n".join(notFoundTask)
            })
            
        if len(notOpenTask)>0:
            return respond({
                'status':400,
                "icon":"error",
                "msg": "Pair Of Site Id And Task is not Open\n" + "\n".join(notOpenTask)
            })
        
        for siteuid in allData['siteId']:
            for name in allData['groupMilestone']:
                lookData = {
                    'siteId':ObjectId(siteuid),
                    "Name":name
                }
                setData = {
                    'assignerId':[ObjectId(allData['vendorId'])],
                    'typeAssigned': 'Partner',
                    "assignDate":current_time().split("T")[0],
                    "workDescription":allData['workDescription']
                }
                cmo.updating("milestone",lookData,setData,False)
        return respond({
            'status':200,
            "icon":"success",
            "msg":"All Task Assigned successfully",
            "data":[]
        })
        
    if request.method == "POST":
        allData = request.get_json()
        allData['groupMilestone'] = allData['groupMilestone'].split(',')
        notOpenTask = []
        notFoundTask = []
        for siteuid in allData['siteId']:
            for name in allData['groupMilestone']:
                arra = [
                    {
                        '$match':{
                            'siteId':ObjectId(siteuid),
                            "Name":name,
                            "typeAssigned":"Partner"
                        }
                    }
                ]
                resp = cmo.finding_aggregate("milestone",arra)['data']
                if len(resp)>0:
                    taskStatus = resp[0]['mileStoneStatus']
                    if taskStatus!="Open":
                        arra = [
                            {
                                '$match':{
                                    '_id':ObjectId(siteuid)
                                }
                            }
                        ]
                        siteName = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['Site Id']
                        notOpenTask.append(f"{siteName} -> {name}")
                else:
                    arra = [
                        {
                            '$match':{
                                '_id':ObjectId(siteuid)
                            }
                        }
                    ]
                    siteName = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['Site Id']
                    notFoundTask.append(f"{siteName} - {name}")
                    
        if len(notFoundTask)>0:
            return respond({
                'status':400,
                "icon":"error",
                "msg": "The pair of Site ID and Task has not been allocated to a vendor.\n" + "\n".join(notFoundTask)
            })
            
        

        if len(notOpenTask)>0:
            return respond({
                'status':400,
                "icon":"error",
                "msg": "Pair Of Site Id And Task is not Open\n" + "\n".join(notOpenTask)
            })
        
        for siteuid in allData['siteId']:
            for name in allData['groupMilestone']:
                lookData = {
                    'siteId':ObjectId(siteuid),
                    "Name":name
                }
                setData = {}
                unset = {
                    'assignerId':1,
                    'typeAssigned': 1,
                    "assignDate":1,
                    "workDescription":1
                }
                cmo.updating("milestone",lookData,setData,False,unset)
        return respond({
            'status':200,
            "icon":"success",
            "msg":"All Task Deallocate successfully",
            "data":[]
        })
        
       
        
        
    
    
        
            
        





# @project_blueprint.route("/upload", methods=["GET"])
# @project_blueprint.route("/script", methods=["GET", "POST"])
# def script():
#     if request.method == "GET":
#         milestone1 = [
#             "Survey",
#             "SACFA Check",
#             "Material Dispatch",
#             "Material Delivered",
#             "Material Pre-Check",
#             "Installation",
#             "Integration",
#             "EMF",
#             "SCFT",
#             "MS1",
#             "KPI AT",
#             "Physical AT",
#             "Soft AT",
#             "Cluster AT",
#             "MS2",
#             "MW PAT AT",
#             "MW SAT",
#         ]
#         header1 = [
#             "POST_RFAI_SURVEY_DATE",
#             "SACFA_APPLIED_DATE",
#             "MATERIAL_DISPATCH_(MD)_DATE",
#             "MATERIAL_DELIVERY_(MOS)_DATE",
#             "INSTALLATION_START_DATE",
#             "INSTALLATION_END_DATE",
#             "Integration Date",
#             "EMF_SUBMISSION_DATE",
#             "SCFT_COMPLETION_DATE",
#             "OA_(COMMERCIAL_TRAFFIC_PUT_ON_AIR)_(MS1)_DATE",
#             "KPI_AT_ACCEPTANCE_DATE",
#             "PHYSICAL_AT_ACCEPTANCE_DATE",
#             "SOFT_AT_ACCEPTANCE_DATE",
#             "CLuster_AT_ACCEPTANCE_DATE",
#             "MAPA_INCLUSION_DATE",
#             "MW_PHYSICAL_AT_ACCEPTANCE_DATE",
#             "MW_SOFT_AT_ACCEPTANCE_DATE",
#         ]
#         milestone2 = [
#             "Material Dispatch",
#             "Material Delivered",
#             "Installation",
#             "NMS Visibility",
#             "Physical AT",
#             "Soft AT",
#             "MS2",
#         ]
#         header2 = [
#             "MATERIAL_DISPATCH_(MD)_DATE",
#             "MATERIAL_DELIVERY_(MOS)_DATE",
#             "INSTALLATION_END_DATE",
#             "NOC_AT_ACCEPTANCE_DATE",
#             "PHYSICAL_AT_ACCEPTANCE_DATE",
#             "SOFT_AT_ACCEPTANCE_DATE",
#             "MS2 Date",
#         ]
#         milestone3 = [
#             "Survey",
#             "SRQ Raise",
#             "Delivery Challan",
#             "Installation",
#             "Pre KPI",
#             "Dismantle",
#             "KPI AT",
#             "Reverse Logistics",
#             "OCI",
#             "MS2",
#         ]
#         header3 = [
#             "Survey date",
#             "SREQ Date",
#             "DC date",
#             "INSTALLATION_END_DATE",
#             "PRE KPI Captured ( Yes/No)",
#             "Dismantle date",
#             "Post KPI Captured ( Yes/No)",
#             "WH SubmissionDate",
#             "OCI",
#             "MS2",
#         ]
#         milestone4 = [
#             "RFAI Date",
#             "Material Pre-Check",
#             "Installation",
#             "MW PAT AT",
#             "MW SAT",
#             "MS1",
#             "MS2",
#         ]
#         header4 = [
#             "ACTUAL HOP RFAI OFFERED DATE",
#             "INSTALLATION_START_DATE",
#             "INSTALLATION_END_DATE",
#             "MW_PHYSICAL_AT_ACCEPTANCE_DATE",
#             "MW_SOFT_AT_ACCEPTANCE_DATE",
#             "MS1",
#             "MS2",
#         ]
#         milestone5 = [
#             "RFAI Date",
#             "Material Dispatch",
#             "Material Delivered",
#             "MS1",
#             "Physical AT",
#             "Soft AT",
#             "MS2",
#         ]
#         header5 = [
#             "Survey date",
#             "MATERIAL_DISPATCH_(MD)_DATE",
#             "MATERIAL_DELIVERY_(MOS)_DATE",
#             "INSTALLATION_END_DATE",
#             "PHYSICAL_AT_ACCEPTANCE_DATE",
#             "SOFT_AT_ACCEPTANCE_DATE",
#             "MS2",
#         ]
#         milestone6 = [
#             "RFAI Date",
#             "Material Dispatch",
#             "Material Delivered",
#             "MS1",
#             "Integration",
#             "MS2",
#         ]
#         header6 = [
#             "Survey",
#             "MATERIAL_DISPATCH_(MD)_DATE",
#             "MATERIAL_DELIVERY_(MOS)_DATE",
#             "INSTALLATION_END_DATE",
#             "Integration Date",
#             "MS2",
#         ]
#         milestone7 = [
#             "RFAI Date",
#             "MS1",
#             "MS2"
#         ]
#         header7 = [
#             "Survey",
#             "Report Submission",
#             "MS2"
#         ]
#         milestone8 = [
#             "Survey",
#             "SACFA Check",
#             "Material Dispatch",
#             "Material Delivered",
#             "Material Pre-Check",
#             "Installation",
#             "Integration",
#             "EMF",
#             "SCFT",
#             "MS1",
#             "KPI AT",
#             "Physical AT",
#             "Soft AT",
#             "MS2",
#         ]
#         header8 = [
#             "POST_RFAI_SURVEY_DATE",
#             "SACFA_APPLIED_DATE",
#             "MATERIAL_DISPATCH_(MD)_DATE",
#             "MATERIAL_DELIVERY_(MOS)_DATE",
#             "INSTALLATION_START_DATE",
#             "INSTALLATION_END_DATE",
#             "Integration Date",
#             "EMF_SUBMISSION_DATE",
#             "SCFT_COMPLETION_DATE",
#             "OA_(COMMERCIAL_TRAFFIC_PUT_ON_AIR)_(MS1)_DATE",
#             "KPI_AT_ACCEPTANCE_DATE",
#             "PHYSICAL_AT_ACCEPTANCE_DATE",
#             "SOFT_AT_ACCEPTANCE_DATE",
#             "MAPA_INCLUSION_DATE",
#         ]

#         for i, header in enumerate(header8):
#             allData = {
#                 "projectTypeName": "SW",
#                 "milestoneName": milestone8[i],
#                 "headerName": header,
#             }
#             response = cmo.insertion("mappedData", allData)
#         return respond(response)



# to download zip of Physical At snap           
@project_blueprint.route('/downloadSnapZip/<globalSaverId>', methods=['GET'])
# @token_required
def download_compliance_snap_zip(globalSaverId):
    try:
        # Step 1: Fetch milestone + SnapData from complianceApproverSaver
        pipeline_main = [
            {'$match': {'_id': ObjectId(globalSaverId)}},
            {'$project': {
                '_id': 0,
                'milestoneName': 1,
                'SnapData': 1,
                'siteIdName': 1,
                'siteuid': {'$toString': '$siteuid'},
                'subprojectId': {'$toString': '$subprojectId'}
            }}
        ]
        response = cmo.finding_aggregate('complianceApproverSaver', pipeline_main)['data']
        if not response:
            return respond({"status": 404, "icon": "error", "msg": "No Data Found"})

        siteId = response[0].get('siteIdName', 'UnknownSite')
        milestone_name = response[0].get('milestoneName')
        snap_data_dict = response[0].get("SnapData", {})
        siteuid = response[0].get("siteuid")

        # Step 2: Fetch SiteEngineer.snap matching subproject/activity/oem/milestone
        pipeline_site_engg = [
            {'$match': {'_id': ObjectId(siteuid)}},
            {'$addFields': {'SubProjectId': {'$toObjectId': "$SubProjectId"}}},
            {'$lookup': {
                'from': 'complianceForm',
                'let': {
                    'subProjectId': '$SubProjectId',
                    'activity': {'$trim': {'input': '$ACTIVITY'}},
                    'oem': {'$trim': {'input': '$OEM NAME'}},
                    'mName': milestone_name
                },
                'pipeline': [
                    {'$match': {'deleteStatus': {'$ne': 1}}},
                    {'$match': {
                        '$expr': {
                            '$and': [
                                {'$eq': ['$subProject', '$$subProjectId']},
                                {'$eq': ['$activity', '$$activity']},
                                {'$eq': ['$oem', '$$oem']},
                                {'$eq': ['$complianceMilestone', '$$mName']}
                            ]
                        }
                    }},
                    {'$project': {'snap': 1, '_id': 0}}
                ],
                'as': 'result'
            }},
            {'$unwind': {'path': '$result', 'preserveNullAndEmptyArrays': True}},
            {'$project': {'snap': '$result.snap', '_id': 0}}
        ]
        responseSiteEngg = cmo.finding_aggregate("SiteEngineer", pipeline_site_engg)['data']
        site_snap_array = responseSiteEngg[0].get("snap", []) if responseSiteEngg else []

        # Step 3: Generate ZIP from matching snap fields + SnapData
        valid_extensions = (".jpg", ".jpeg", ".png", ".webp")
        zip_stream = io.BytesIO()
        with zipfile.ZipFile(zip_stream, 'w', zipfile.ZIP_STORED) as zipf:
            for snap in site_snap_array:
                snap_key = snap.get("fieldName", "")
                safe_folder = "".join(c if c.isalnum() or c in (' ', '_', '-') else "_" for c in snap_key)
                folder_path = os.path.join(milestone_name, safe_folder)
                snap_entry = snap_data_dict.get(snap_key, {})

                if snap_entry:
                    for img in snap_entry.get("images", []):
                        img_path = img.get("image", "")
                        if img_path.lower().endswith(valid_extensions):
                            try:
                                arcname = os.path.join(folder_path, os.path.basename(img_path))
                                zipf.write(img_path, arcname)
                            except FileNotFoundError:
                                continue  
                else:
                    # Add empty folder if no images found
                    zipf.writestr(zipfile.ZipInfo(folder_path + "/"), "")

        zip_stream.seek(0)
        return send_file(
            zip_stream,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f"{siteId}.zip"
        )

    except Exception as e:
        return respond({"status": 500, "icon": "error", "msg": str(e)})
    