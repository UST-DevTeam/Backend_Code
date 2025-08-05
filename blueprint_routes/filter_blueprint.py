from base import *
from blueprint_routes.currentuser_blueprint import sub_project
from blueprint_routes.currentuser_blueprint import projectGroup_str,projectId_Object

filter_blueprint = Blueprint('filter_blueprint', __name__)


@filter_blueprint.route("/autosuggestion/projectManger",methods=['GET','POST'])
def autosuggestion_PM():
    arra = [
        {
            '$lookup': {
                'from': 'userRole', 
                'localField': 'userRole', 
                'foreignField': '_id', 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'roleName': {
                    '$arrayElemAt': [
                        '$result.roleName', 0
                    ]
                }
            }
        }, {
            '$match': {
                'roleName': 'Project Manager'
            }
        }, {
            '$project': {
                'empName': 1, 
                'uniqueId': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("userRegister",arra)
    return respond(response)


@filter_blueprint.route("/filter/project/circle",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/circle/<customerId>",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/circle/<customerId>/<projectType>",methods = ['GET','POST'])
def filter_circle(customerId=None,projectType=None):

    if request.method == "GET":

        arra = []

        arra = arra + [
            {
                '$match': {
                    'custId': customerId,
                }
            }
        ]
        if projectType!=None and projectType!='undefined':
            arra = arra + [
                {
                    '$match': {
                        'projectType': ObjectId(projectType)
                    }
                }
            ] 
        arra = arra + [
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
                    'Circle': '$result.circleName'
                }
            }, {
                '$group': {
                    '_id': '$Circle', 
                    'circle': {
                        '$first': '$Circle'
                    }
                }
            }, {
                "$sort":{
                    'circle':1
                }
            }, {
                '$match':{
                    'circle':{
                        '$ne':None}
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("project",arra)
        return respond(response)


@filter_blueprint.route("/filter/project/projectId",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/projectId/<customer>",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/projectId/<customer>/<projectType>",methods = ['GET','POST'])
def filter_project_projectId(customer=None,projectType=None):
    arra = []
    arra = arra+ [
        {
            '$match': {
                'custId': customer,
            }
        }
    ]
    if projectType!=None and projectType!='undefined':
        arra = arra + [{
                '$match': {
                    'projectType': ObjectId(projectType)
                }
            }
        ]
    arra = arra +[
        {
            '$project': {
                'projectId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)



@filter_blueprint.route("/filter/project/projectGroup",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/projectGroup/<customer>",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/projectGroup/<customer>/<projectType>",methods = ['GET','POST'])
def filter_project_projectGroup(customer=None,projectType=None):
    arra = []
    arra = arra + [
        {
            '$match': {
                'custId': customer,
            }
        }
    ]
    if projectType!=None and projectType!='undefined':
        arra = arra + [
            {
                '$match': {
                    'projectType': ObjectId(projectType)
                }
            }
        ] 
    arra = arra + [
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
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'ProjectGroup': {
                    '$arrayElemAt': [
                        '$result.projectGroupId', 0
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$ProjectGroup', 
                'ProjectGroup': {
                    '$first': '$ProjectGroup'
                }
            }
        }, {
            '$match': {
                '_id': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                'ProjectGroup': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)


@filter_blueprint.route("/filter/project/projectType",methods = ['GET','POST'])
# @filter_blueprint.route("/filter/project/projectType/<customer>",methods = ['GET','POST'])
# @filter_blueprint.route("/filter/project/projectType/<customer>/<projectType>",methods = ['GET','POST'])
@token_required
def filter_project_projectType(current_user):
    arra = []
    if current_user['roleName'] not in ['Admin','PMO']:
        arra = arra + [
            {
                '$match':{
                    '_id':{
                        '$in':projectId_Object(current_user['userUniqueId'])
                    }
                }
            }
        ]   
    arra = arra + [
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
            '$match': {
                '_id': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                '_id': 0
            }
        }, { 
            '$sort':{
                'projectType':1
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)




@filter_blueprint.route("/filter/project/projectManager",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/projectManager/<customer>",methods = ['GET','POST'])
@filter_blueprint.route("/filter/project/projectManager/<customer>/<projectType>",methods = ['GET','POST'])
def filter_project_PM(customer=None,projectType=None):
    arra = []
    arra = arra + [
        {
            '$match': {
                'custId': customer,
            }
        }
    ]
    if projectType!=None and projectType!='undefined':
        arra = arra + [
            {
                '$match': {
                    'projectType': ObjectId(projectType)
                }
            }
        ] 
    arra = arra +[
        {
            '$addFields': {
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
                'as': 'result'
            }
        }, {
            '$addFields': {
                'projectManager': {
                    '$arrayElemAt': [
                        '$result.empName', 0
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$projectManager', 
                'projectManager': {
                    '$first': '$projectManager'
                }
            }
        }, {
            '$match': {
                '_id': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                'projectManager': 1, 
                '_id': 0
            }
        }, {
            '$sort':{
               'projectManager':1 
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)


@filter_blueprint.route("/filter/site/subProject",methods = ['GET','POST'])
@filter_blueprint.route("/filter/site/subProject/<uid>",methods = ['GET','POST'])
def filter_site_subProject(uid=None):
    arra = [
        {
            '$match': {
                '_id': ObjectId(uid)
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'projectType', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        '$match': {'deleteStatus': {'$ne': 1}}
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
                '_id': {
                    '$toString': '$_id'
                }, 
                'projectGroup': {
                    '$toString': '$projectGroup'
                }
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'let': {
                    'custId': '$custId'
                }, 
                'localField': 'projectType', 
                'foreignField': 'projectType', 
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
                                '$eq': [
                                    '$$custId', '$custId'
                                ]
                            }
                        }
                    }, {
                        '$addFields': {
                            'value': {
                                '$toString': '$_id'
                            }, 
                            '_id': {
                                '$toString': '$_id'
                            }, 
                            'label': {
                                '$toString': '$subProject'
                            }
                        }
                    }
                ], 
                'as': 'subprojectresult'
            }
        }, {
            '$unwind': {
                'path': '$subprojectresult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'subproject': '$subprojectresult.subProject',
                'subprojectId':{
                    '$toString':'$subprojectresult._id'
                }
            }
        }, {
            '$project': {
                'subproject': 1, 
                'subprojectId':1,
                '_id': 0
            }
        }, {
            '$sort': {
                'subproject': 1
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra)
    return respond(response)

@filter_blueprint.route("/filter/myTask/subProject",methods = ['GET'])
@filter_blueprint.route("/filter/myTask/subProject/<id>",methods = ['GET'])
@token_required
def filter_myTask_subProject(current_user,id=None):
    arra = [
        {
            '$match': {
                'assignerId': ObjectId(current_user['userUniqueId']),
                'deleteStatus':{'$ne':1},
                "customerId":id
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
                'pipeline':[{
                    "$match":{'deleteStatus':{'$ne':1}}
                    }],
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'subprojectName': '$result.subProject', 
                'SubProjectId': {
                    '$toString': '$SubProjectId'
                }
            }
        }, {
            '$group': {
                '_id': '$subprojectName', 
                'subprojectName': {
                    '$first': '$subprojectName'
                }, 
                'subProjectId': {
                    '$first': '$SubProjectId'
                }
            }
        }, {
            '$match': {
                '_id': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("milestone",arra)
    return respond(response)


@filter_blueprint.route("/filter/financial/pomanagement/customer",methods = ['GET','POST'])
def filter_financial_customer():
    arra = [
        {
            '$addFields': {
                'customer': {
                    '$toObjectId': '$customer'
                }
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
                'customer': '$result.customerName'
            }
        }, {
            '$group': {
                '_id': '$customer', 
                'customer': {
                    '$first': '$customer'
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
                'customer': 1
            }
        }, {
            '$project': {
                'customer': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("PoInvoice",arra)
    return respond(response)


@filter_blueprint.route("/filter/financial/pomanagement/projectGroup",methods = ['GET','POST'])
@token_required
def filter_financial_projectGroup(current_user):
    
    arra = [
        
        {
            '$addFields': {
                '_id': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$match': {
                '_id': {
                    '$in': projectGroup_str(current_user['userUniqueId'])
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
                'projectGroupId': 1, 
                'uniqueId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectGroup",arra)
    return respond(response)


@filter_blueprint.route("/filter/financial/pomanagement/projectId",methods = ['GET','POST'])
def filter_financial_projectId():
    arra = [
        {
            '$addFields': {
                'projectId': {
                    '$cond': [
                        {
                            '$eq': [
                                '$projectId', ''
                            ]
                        }, '', {
                            '$toObjectId': '$projectId'
                        }
                    ]
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
                'projectId': '$result.projectId'
            }
        }, {
            '$group': {
                '_id': '$projectId', 
                'projectId': {
                    '$first': '$projectId'
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
                'projectId': 1
            }
        }, {
            '$project': {
                'projectId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("PoInvoice",arra)
    return respond(response)



@filter_blueprint.route("/filter/financial/revenueManagement/customer",methods = ['GET','POST'])
def filter_revenue_customer():
    arra = [
        {
            '$addFields': {
                'customer': {
                    '$toObjectId': '$customer'
                }
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
                'customer': '$result.customerName'
            }
        }, {
            '$group': {
                '_id': '$customer', 
                'customer': {
                    '$first': '$customer'
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
                'customer': 1
            }
        }, {
            '$project': {
                'customer': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("invoice",arra)
    return respond(response)





@filter_blueprint.route("/filter/financial/revenueManagement/projectGroup",methods = ['GET','POST'])
def filter_revenue_projectGroup():
    arra = [
        {
            '$addFields': {
                'projectGroup': {
                    '$toObjectId': '$projectGroup'
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
                'projectGroup': '$result.projectGroupId'
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
                '_id': {
                    '$ne': None
                }
            }
        }, {
            '$sort': {
                'projectGroup': 1
            }
        }, {
            '$project': {
                'projectGroup': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("invoice",arra)
    return respond(response)





@filter_blueprint.route("/filter/financial/poWorkDone/customer",methods = ['GET','POST'])
def filter_poworkdone_customer():
    arra = [
        {
            '$addFields': {
                'projectuniqueId': {
                    '$toObjectId': '$projectuniqueId'
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
                            'as': 'result'
                        }
                    }, {
                        '$addFields': {
                            'customer': {
                                '$arrayElemAt': [
                                    '$result.customerName', 0
                                ]
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'customer': {
                    '$arrayElemAt': [
                        '$result.customer', 0
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$customer', 
                'customer': {
                    '$first': '$customer'
                }
            }
        }, {
            '$match': {
                'customer': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                'customer': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("SiteEngineer",arra)
    return respond(response)



@filter_blueprint.route("/filter/EVM-Delivery/projectId",methods = ['GET','POST'])
@token_required
def filter_EVM_Delivery_projectid(current_user):
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
        }, {
            '$project': {
                'projectId': 1, 
                'uniqueId': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    dataResp = cmo.finding_aggregate("projectAllocation", arra)
    return respond(dataResp)
    
@filter_blueprint.route("/filter/EVM-Delivery/projectType",methods = ['GET','POST'])
@token_required
def filter_EVM_Delivery_projectType(current_user):
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
            '$replaceRoot': {
                'newRoot': '$result'
            }
        }, {
            '$group': {
                '_id': '$projectType', 
                'projectType': {
                    '$first': '$projectType'
                }, 
                'uniqueId': {
                    '$first': {
                        '$toString': '$_id'
                    }
                }
            }
        }, {
            '$sort': {
                'projectType': 1
            }
        }, {
            '$project': {
                'projectType': 1, 
                'uniqueId': 1, 
                '_id': 0
            }
        }
    ]
    dataResp = cmo.finding_aggregate("projectAllocation", arra)
    return respond(dataResp)



@filter_blueprint.route("/filter/work-done/projectType/<id>",methods = ['GET'])
@token_required
def filter_work_done_projectType(current_user,id=None):
    arra = sub_project(current_user['userUniqueId'],id)
    return respond({
        'status':200,
        "msg":"Data Updated successfully",
        "data":arra
    })
    