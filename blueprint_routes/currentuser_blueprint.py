from base import *

currentuser_blueprint = Blueprint('currentuser_blueprint', __name__)


def projectId_Object(empId):
    arra = [
        {
            '$match': {
                'empId': empId
            }
        }, {
            '$unwind': {
                'path': '$projectIds', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$project': {
                'projectIds': 1, 
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

def projectId_str(empId):
    arra = [
        {
            '$match': {
                'empId': empId
            }
        }, {
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


def projectGroup_str(empId):
    arra = [
        {
            '$match': {
                'empId': empId
            }
        }, {
            '$unwind': {
                'path': '$projectIds', 
                'preserveNullAndEmptyArrays': True
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
                            'projectGroup': {
                                '$toString': '$projectGroup'
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
                'projectGroup': '$result.projectGroup'
            }
        }, {
            '$group': {
                '_id': '$projectGroup', 
                'projectGroup': {
                    '$first': '$projectGroup'
                }
            }
        }, {
            '$match': {
                'projectGroup': {
                    '$ne': None
                }
            }
        }
    ]
    response = cmo.finding_aggregate("projectAllocation",arra)['data']
    projectGroup = []
    if len(response):
        for i in response:
            projectGroup.append(i['projectGroup'])
    return projectGroup


def sub_project(empId,customerId=None):
    arra = []
    if customerId!=None:
        arra = arra + [
            {
                '$match':{
                    'custId':customerId
                }
            }
        ]
    arra = arra + [
        {
            '$match': { 
                '_id': {
                    '$in': projectId_Object(empId)
                },
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
        return response
    else:
        return[{'projectType':'','uid':[]}]

def costCenter_str(empId):
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
            '$match': {
                'result': {
                    '$ne': []
                }
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
                'uniqueId': 1
            }
        }
    ]
    response = cmo.finding_aggregate("projectAllocation",arra)['data']
    if len(response)>0:
        return response
    else:
        return [{'uniqueId':None}]

@currentuser_blueprint.route("/currentuser/ProjectGroup",methods=['GET'])
@token_required
def currentuser_projectgroup(current_user):
    
    arra = []
    if request.args.get("customer")!=None and request.args.get("customer")!="undefined":
        arra = arra + [
            {
                '$match':{
                    'custId':request.args.get("customer")
                }
            }
        ]
        
    if request.args.get("customerId")!=None and request.args.get("customerId")!="undefined":
        customerId = request.args.get("customerId").split(",")
        arra = arra + [
            {
                '$match':{
                    'custId':{
                        '$in':customerId
                    }
                }
            }
        ]
    
    arra = arra + [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }, 
                '_id': {
                    '$in': projectId_Object(current_user['userUniqueId'])
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
                            'customer': '$customer.shortName'
                        }
                    }, {
                        '$addFields': {
                            'projectGroupId': {
                                '$concat': [
                                    '$customer', '-', '$zone', '-', '$costCenter'
                                ]
                            }, 
                            'uniqueId': {
                                '$toString': '$_id'
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0, 
                            'projectGroupId': 1, 
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
            '$project': {
                '_id': 0, 
                'projectGroup': '$result.projectGroupId', 
                'uniqueId': '$result.uniqueId'
            }
        }, {
            '$group': {
                '_id': '$projectGroup', 
                'projectGroup': {
                    '$first': '$projectGroup'
                }, 
                'uniqueId': {
                    '$first': '$uniqueId'
                }
            }
        }, {
            '$match': {
                '_id': {
                    '$ne': None
                }
            }
        },{
            '$sort':{
                'projectGroup':1
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)






@currentuser_blueprint.route("/currentuser/ProjectType",methods=['GET'])
@token_required
def currentuser_projecttype(current_user):
    arra = []
    
    if request.args.get("customer")!=None and request.args.get("customer")!="undefined":
        arra = arra + [
            {
                '$match':{
                    'custId':request.args.get("customer")
                }
            }
        ]
    arra = arra + [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }, 
                '_id': {
                    '$in': projectId_Object(current_user['userUniqueId'])
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
                        '$addFields': {
                            'uniqueId': {
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
                'projectType': '$result.projectType', 
                'subProjectType': '$result.subProject', 
                'uniqueId': '$result.uniqueId'
            }
        }, {
            '$project': {
                'projectType': {
                    '$concat': [
                        '$projectType', '(', '$subProjectType', ')'
                    ]
                }, 
                'uniqueId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)



@currentuser_blueprint.route("/currentuser/ProjectId",methods=['GET'])
@token_required
def currentuser_projectId(current_user):
    
    arra = [
        {
            '$addFields':{
                'projectGroup':{
                    '$toString':"$projectGroup"
                },
                'projectType':{
                    '$toString':"$projectType"
                }
            }
        }
    ]
    
    if request.args.get("projectGroup")!=None and request.args.get("projectGroup")!="undefind":
        arra = arra + [
            {
                '$match':{
                    'projectGroup':request.args.get("projectGroup")
                }
            }
        ]
        
    if request.args.get("projectType")!=None and request.args.get("projectType")!="undefind":
        arra = arra + [
            {
                '$match':{
                    'projectType':request.args.get("projectType")
                }
            }
        ]
    
    arra = arra + [
        {
            '$match':{
                '_id':{
                    '$in':projectId_Object(current_user['userUniqueId'])
                }
            }
        }, {
            '$project':{
                'uniqueId':{
                    '$toString':'$_id'
                },
                'projectId':1,
                '_id':0
            }
        }
    ]
    
    response = cmo.finding_aggregate("project",arra)
    return respond(response)



@currentuser_blueprint.route("/currentuser/Circle/projectId",methods=['GET'])
@currentuser_blueprint.route("/currentuser/Circle/projectId/<id>",methods=['GET'])
@token_required
def currentuser_circle_projectId(current_user,id=None):
    arra =[
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
        }, {
            '$addFields': {
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$group': {
                '_id': '$circle', 
                'projectuid': {
                    '$push': '$uniqueId'
                }
            }
        }, {
            '$addFields': {
                '_id': {
                    '$toObjectId': '$_id'
                }
            }
        }, {
            '$lookup': {
                'from': 'circle', 
                'localField': '_id', 
                'foreignField': '_id',
                "pipeline":[
                    {
                        '$match':{
                            'deleteStatus':{"$ne":1}
                        }
                    }, {
                        '$addFields':{
                            'circleId':{
                                '$toString':'$_id'
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
                    '$arrayElemAt': [
                        '$result.circleId', 0
                    ]
                }
            }
        }, {
            '$project': {
                'projectuid': 1, 
                'circle': 1, 
                "circleId":1,
                '_id': 0
            }
        }, {
            '$sort': {
                'circle': 1
            }
        }
    ]
    if id!=None:
        arra[1]['$lookup']['pipeline'].insert(0,{
            '$match':{
                'custId':id
            }
        })
    response = cmo.finding_aggregate("projectAllocation",arra)
    return respond(response)


@currentuser_blueprint.route("/currentuser/CostCenter",methods=['GET'])
@currentuser_blueprint.route("/currentuser/CostCenter/<customerId>",methods=['GET'])
@token_required
def currentuser_cost_center(current_user,customerId=None):
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
                            'deleteStatus': { '$ne': 1}
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
                                        'businessUnit':{
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
                            'uniqueId': {
                                '$arrayElemAt': [
                                    '$result.uniqueId', 0
                                ]
                            }, 
                            'businessUnit':{
                                '$arrayElemAt': [
                                    '$result.businessUnit', 0
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
            '$sort':{
                'costCenter':1
            }
        }
    ]
    if customerId!=None:
        arra[1]['$lookup']['pipeline'].insert(0,{
            "$match":{
                'custId':customerId
            }
        })
    if request.args.get("customerId")!=None and request.args.get("customerId")!="":
        customer = request.args.get("customerId").split(",")
        arra[1]['$lookup']['pipeline'].insert(0,{
            "$match":{
                'custId':{
                    '$in':customer
                }
            }
        })
    response = cmo.finding_aggregate("projectAllocation",arra)
    return respond(response)

@currentuser_blueprint.route("/currentuser/businessUnit",methods=['GET'])
@token_required
def currentuser_business_unit(current_user):
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
                                        'as': 'businessUnit'
                                    }
                                }, {
                                    '$project': {
                                        'businessUnit': {
                                            '$arrayElemAt': [
                                                '$businessUnit.businessUnit', 0
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
                            'businessUnit': {
                                '$arrayElemAt': [
                                    '$result.businessUnit', 0
                                ]
                            }, 
                            '_id': 0
                        }
                    }, {
                        '$group': {
                            '_id': '$businessUnit'
                        }
                    }, {
                        '$match': {
                            '_id': {
                                '$ne': None
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
        }, {
            '$addFields': {
                'businessUnit': '$_id'
            }
        }, {
            '$sort': {
                'businessUnit': 1
            }
        }
    ]
    response = cmo.finding_aggregate("projectAllocation",arra)
    return respond(response)


@currentuser_blueprint.route("/currentuser/customer",methods=['GET'])
@token_required
def current_user_customer(current_user):
    if request.method == "GET":
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
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$group': {
                    '_id': '$custId'
                }
            }, {
                '$lookup': {
                    'from': 'customer', 
                    'localField': '_id', 
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
                    'customerName': {
                        '$arrayElemAt': [
                            '$result.customerName', 0
                        ]
                    }, 
                    'index': {
                        '$arrayElemAt': [
                            '$result.index', 0
                        ]
                    }, 
                    'customerId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }, {
                '$sort': {
                    'index': 1
                }
            }
        ]
        response = cmo.finding_aggregate("projectAllocation",arra)
        return respond(response)