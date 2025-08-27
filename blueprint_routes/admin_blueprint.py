from base import *
from common.config import changedThings2 as changedThings2
from common.config import changedThings3 as changedThings3
from common.config import changedThings4 as changedThings4
from blueprint_routes.currentuser_blueprint import projectId_Object
from common.config import unique_timestampexpense
admin_blueprint = Blueprint('admin_blueprint', __name__)


profileStatus = {

    1: "User Registered",
    2: "Password Setup Successfully",
    3: "KYC Submitted",
    11: "User Rejected due to email expire"
}


def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


@admin_blueprint.route('/admin/roles', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/roles/<uniqueId>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def admin_roles(current_user, uniqueId=None):

    if (request.method == "GET"):

        argu = request.args
        arew = apireq.argstostr(argu)

        userRoleAggr = [
            {
                '$addFields': {
                    'value': {
                        '$toString': '$_id'
                    },
                    'label': '$roleName'
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        return cmo.finding_aggregate("userRole", userRoleAggr)

    elif (request.method == "POST"):
        dataAll = request.get_json()
        dataAll["createdBy"] = current_user['userUniqueId']
        userData = cmo.insertion("userRole", dataAll)
        return respond(userData)

    elif (request.method == "PUT"):
        if uniqueId != None:
            dataAll = request.get_json()

            dataAll = del_itm(dataAll, ["label", "value", "roleName"])
            userData = cmo.updating(
                "userRole", {"_id": ObjectId(uniqueId)}, dataAll, False)
            return respond(userData)

    elif (request.method == "DELETE"):
        response = cmo.updating(
            "userRole", {"_id": ObjectId(uniqueId)}, {"deleteStatus": 1})
        return respond(response)


@admin_blueprint.route('/admin/users', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/users/<uniqueId>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def admin_users(current_user, uniqueId=None):

    if (request.method == "GET"):

        argu = request.args
        arew = apireq.argstostr(argu)
        loginAggr = [
            {
                '$lookup': {
                    'from': 'userRole',
                    'localField': 'userRole',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'userRole'
                }
            }, {
                '$unwind': {
                    'path': '$userRole',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'loginType': "",
                    'mobile': {
                        '$concat': ['$countryCode', " ", '$mobile']
                    },
                    'profileStatus': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$profileStatus', 1
                                        ]
                                    },
                                    'then': 'User Registered'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$profileStatus', 2
                                        ]
                                    },
                                    'then': 'Password Setup Successfully'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$profileStatus', 3
                                        ]
                                    },
                                    'then': 'KYC Submitted'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$profileStatus', 10
                                        ]
                                    },
                                    'then': 'User not Submitted their document'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$profileStatus', 11
                                        ]
                                    },
                                    'then': 'User Rejected due to email expire'
                                }
                            ],
                            'default': '--'
                        }
                    },
                    'rolename': '$userRole.roleName',
                    'permission': '$userRole.permission',
                    'roleId': {
                        '$toString': '$userRole._id'
                    },
                    'id': {
                        '$toString': '$_id'
                    }
                }
            }, {
                "$match": {
                    "rolename": {
                        "$nin": ["Operation", "Admin"]
                    }
                }
            }, {
                '$project': {
                    'userRole': 0,
                    '_id': 0
                }
            }, {
                '$sort': {
                    'id': -1
                }
            }
        ]
        userData = cmo.finding_aggregate("userRegister", loginAggr)
        return respond(userData)

    elif (request.method == "POST"):
        dataAll = request.get_json()
        userData = cmo.insertion("userRegister", dataAll)
        return respond(userData)

    elif (request.method == "PUT"):
        dataAll = request.get_json()
        if (dataAll["password"] == "********"):
            del dataAll["password"]
        dataAll = del_itm(dataAll, ["create_time", "update_time", "id"])

        updateBy = {
            "_id": ObjectId(uniqueId)
        }
        userData = cmo.updating("users", updateBy, dataAll)
        return respond(userData)

    elif (request.method == "PATCH"):
        dataAll = request.get_json()
        userData = cmo.updating("users", {"_id": ObjectId(uniqueId)}, dataAll)
        return respond(userData)

    elif (request.method == "DELETE"):

        updateBy = {"_id": ObjectId(uniqueId)}
        cmo.updating("userRegister", updateBy, {"deleteStatus": 1})
        return ({'msg': 'User has deleted successfully'})

# pre using api
# @admin_blueprint.route('/admin/cardCustomer', methods=['GET'])
# @token_required
# def cardcustomer(current_user):

#     if request.method == "GET":
#         arra = [
#             {
#                 '$match': {
#                     'status': "Active"
#                 }
#             }, {
#                 '$addFields': {
#                     '_id': {
#                         '$toString': '$_id'
#                     },
#                     'uniqueId': {
#                         '$toString': '$_id'
#                     }
#                 }
#             }
#         ]
#         response = cmo.finding_aggregate("customer", arra)
#         return respond(response)


# By Giriraj ---
@admin_blueprint.route('/admin/cardCustomer', methods=['GET'])
@token_required
def cardcustomer(current_user):
 
    if request.method == "GET":
        if current_user['roleName'] == "Admin":
            arra = [
                {
                    '$match': {
                        'status': "Active"
                    }
                }, {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'uniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }
            ]
            response = cmo.finding_aggregate("customer", arra)
            return respond(response)
        else:
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
                        'path': '$result'
                    }
                }, {
                    '$addFields': {
                        'customerId': '$result.custId'
                    }
                }, {
                    '$group': {
                        '_id': '$customerId',
                        'customerId': {
                            '$first': '$customerId'
                        }
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
                        'as': 'customerResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$customerResult'
                    }
                }, {
                    '$addFields': {
                        'customerId': {
                            '$toString': '$customerId'
                        },
                        'customerName': '$customerResult.customerName',
                        'companyimg': '$customerResult.companyimg',
                        'index': '$customerResult.index'
                    }
                }, {
                    '$project': {
                        'customerId': 1,
                        'customerName': 1,
                        'companyimg': 1,
                        '_id': 0,
                        'index': 1
                    }
                }, {
                    '$sort': {
                        'index': 1
                    }
                }
            ]
            response = cmo.finding_aggregate("projectAllocation", arra)
            return respond(response)
            

@admin_blueprint.route('/admin/manageCustomer', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageCustomer/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def managecustomer(current_user, id=None):

    if request.method == "GET":
        if request.args.get("empId") != None and request.args.get("empId") != "undefined":
            arra = [
                {
                    '$match': {
                        'empId': request.args.get("empId")
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
                        'path': '$result'
                    }
                }, {
                    '$addFields': {
                        'customerId': '$result.custId'
                    }
                }, {
                    '$group': {
                        '_id': '$customerId',
                        'customerId': {
                            '$first': '$customerId'
                        }
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
                        'as': 'customerResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$customerResult'
                    }
                }, {
                    '$addFields': {
                        'customerId': {
                            '$toString': '$customerId'
                        },
                        'customerName': '$customerResult.customerName',
                        "companyimg": "$customerResult.companyimg",
                        "index":"$customerResult.index",
                    }
                }, {
                    '$project': {
                        'customerId': 1,
                        'customerName': 1,
                        "companyimg": 1,
                        '_id': 0,
                        "index":1
                    }
                }, {
                    '$sort': {
                        'index': 1
                    }
                }
            ]
            response = cmo.finding_aggregate("projectAllocation", arra)
            return respond(response)
        else:
            arra = [
                {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'uniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }
            ]
            arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
            response = cmo.finding_aggregate("customer", arra)
            return respond(response)

    elif request.method == "POST":

        if id == None:
            companyimg = request.files.get("companyimg")
            attachment = request.files.get("attachment")
            allData = {}
            if companyimg:
                pathing = cform.singleFileSaver(companyimg, "", "")
                if (pathing["status"] == 422):
                    return respond(pathing)
                elif (pathing["status"] == 200):
                    allData['companyimg'] = pathing['msg']
            formData = request.form
            for i in formData:
                allData[i] = formData[i]
            arra = [
                {
                    "$match": {
                        "customerName": allData["customerName"],
                        "shortName": allData["shortName"]
                    }
                }
            ]
            response = cmo.finding_aggregate("customer", arra)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "This customer is Already Exist",
                    "icon": "error",
                }, 400

            if 'customerName' in allData:
                allData['customerName'] = allData['customerName']
            if 'shortName' in allData:
                allData['shortName'] = allData['shortName']
            Added = "New Customer "+allData['customerName']+" " + " Added"
            cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'Manage Customer', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.insertion("customer", allData)
            return respond(response)

        elif id != None:
            companyimg = request.files.get("companyimg")
            allData = {}
            if companyimg:
                pathing = cform.singleFileSaver(companyimg, "", "")
                if (pathing["status"] == 422):
                    return respond(pathing)
                allData['companyimg'] = pathing['msg']
            formData = request.form
            for i in formData:
                allData[i] = formData[i]
            lookData = {
                '_id': ObjectId(id)
            }
            try:
                arr = [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': ObjectId(id)
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'uniqueId': 0
                        }
                    }
                ]
                Responsed = cmo.finding_aggregate("customer", arr)['data']
                allData2 = Responsed[0]
                changedThings = changedThings2(allData, allData2)

                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added = changedThings+" updated for this " + \
                    allData['customerName'] + " Customer"
                cmo.insertion("AdminLogs", {'type': 'Update', 'module': 'Manage Customer', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            except Exception as e:
                print(e, 'dhhdhdhhdh')

            response = cmo.updating("customer", lookData, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            arra = [
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }, {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'projectType',
                        'localField': '_id',
                        'foreignField': 'custId',
                        'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                        'as': 'projectTypeResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$projectTypeResult',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'projectTypeStatus': '$projectTypeResult.status'
                    }
                }, {
                    '$match': {
                        'projectTypeStatus': 'Active'
                    }
                }
            ]
            response = cmo.finding_aggregate("customer", arra)['data']

            if (len(response)):
                return respond({
                    'status': 400,
                    "msg": 'This Customer Contains Active Project Type',
                    "icon": "error"
                })
            arry = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        '_id': ObjectId(id)
                    }
                }, {
                    '$project': {
                        'customerName': 1,
                        '_id': 0
                    }
                }
            ]
            Responsert = cmo.finding_aggregate("customer", arry)['data']
            Circle = None

            if len(Responsert):
                Circle = Responsert[0]['customerName']
                Added = "Deleted This Customer "+Circle
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Manage Customer', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.deleting(
                "customer", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return respond({
                "status": 400,
                "msg": 'Please Provide a Valid UniqueID',
                "icon": "error"
            })








@admin_blueprint.route('/admin/manageCostCenter', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageCostCenter/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def costcenter(current_user, id=None):

    if request.method == "GET":
        arra = []
        if request.args.get("zone") != None and request.args.get("zone") != "":
            arra = arra + [
                {
                    '$match': {
                        'zone': ObjectId(request.args.get("zone"))
                    }
                }
            ]
        if id!=None:
            arra = arra + [
                {
                    '$match':{
                        'customer':ObjectId(id)
                    }
                }
            ]
        arra = arra + [
            {
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
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'zoneName': '$result.shortCode',
                    '_id': {
                        '$toString': '$_id'
                    },
                    'zone': {
                        '$toString': '$zone'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'customer',
                    'localField': 'customer',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'customerName': '$result.customerName',
                    'customer': {
                        '$toString': '$customer'
                    }
                }
            }, {
                '$project': {
                    'result': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("costCenter", arra)
        return respond(response)

    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            arra = [
                {
                    '$match': {
                        'customer': ObjectId(allData['customer']),
                        'zone': ObjectId(allData['zone']),
                        'costCenter': allData['costCenter']
                    }
                }
            ]
            response = cmo.finding_aggregate("costCenter", arra)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "This Cost center is Already Exist",
                    "icon": "error",
                }, 400

            arra = [
                {
                    '$match': {
                        'description': allData['description']
                    }
                }
            ]
            response = len(cmo.finding_aggregate("costCenter", arra)['data'])
            if response:
                return respond({
                    'msg': "This Description is already found in Database",
                    "icon": "error",
                    "status": 400,
                })

            if 'customer' in allData:
                allData['customer'] = ObjectId(allData['customer'])
            if allData['zone']:
                allData['zone'] = ObjectId(allData['zone'])

            try:

                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added = "The Cost Center "+" "+allData['costCenter'] + " Added"
                cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'costCenter', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            except Exception as e:
                print('Hello', e)
            response = cmo.insertion("costCenter", allData)
            return respond(response)

        elif id != None:

            # arra = [
            #     {
            #         '$match': {
            #             'description': request.json.get("description")
            #         }
            #     }
            # ]
            # response = len(cmo.finding_aggregate("costCenter", arra)['data'])
            # if response:
            #     return respond({
            #         'msg': "This Description is already found in Database",
            #         "icon": "error",
            #         "status": 400,
            #     })

            allData = {
                "businessUnit": request.json.get("businessUnit"),
                "ustProjectId": request.json.get("ustProjectId")
            }
            updateBy = {
                '_id': ObjectId(id)
            }

            try:
                arr = [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': ObjectId(id)
                        }
                    }, {
                        '$project': {
                            '_id': 0
                        }
                    }
                ]
                Responsed = cmo.finding_aggregate("costCenter", arr)['data']
                allData2 = Responsed[0]
                changedThings = changedThings2(allData, allData2)
                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added = changedThings+" updated in this costCenter " + \
                    allData2['costCenter']
                cmo.insertion("AdminLogs", {'type': 'Update', 'module': 'costCenter', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            except Exception as e:
                print(e, 'dhhdhdhhdh')

            response = cmo.updating("costCenter", updateBy, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:

            arr = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        '_id': ObjectId(id)
                    }
                }, {
                    '$project': {
                        'costCenter': 1,
                        '_id': 0
                    }
                }
            ]
            Response = cmo.finding_aggregate("costCenter", arr)['data']
            Zone = None
            if len(Response):
                Zone = Response[0]['costCenter']
                Added = "Deleted This Cost Center "+Zone
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Cost Center', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            response = cmo.deleting(
                "costCenter", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({'msg': "Please Provide Valid Unique Id"})


@admin_blueprint.route('/admin/manageProjectGroup', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageProjectGroup/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def projectgroup(current_user, id=None):

    if request.method == "GET":
        arra = []
        if request.args.get("customer") != "undefined" and request.args.get("customer") != None:
            arra = arra + [
                {
                    '$match': {
                        'customerId': ObjectId(request.args.get("customer"))
                    }
                }
            ]
        if id != "undefined" and id != None:
            arra = arra + [
                {
                    '$match': {
                        'customerId': ObjectId(id)
                    }
                }
            ]
        arra = arra + [
            {
                '$lookup': {
                    'from': 'customer',
                    'localField': 'customerId',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                    'customerId': 1,
                    'zoneId': 1,
                    'costCenterId': 1,
                    'projectGroupId': 1,
                    'uniqueId': 1
                }
            }
        ]
        print(arra,'arraarraarra7')
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        
        response = cmo.finding_aggregate("projectGroup", arra)
        return respond(response)

    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            arra = [
                {
                    '$match': {
                        'customerId': ObjectId(allData['customerId']),
                        'costCenterId': ObjectId(allData['costCenterId']),
                        'zoneId': ObjectId(allData['zoneId']),

                    }
                }
            ]
            response = cmo.finding_aggregate("projectGroup", arra)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "This Project Group is Already Exist",
                    "icon": "error",
                }, 400

            if "customerId" in allData:
                allData['customerId'] = ObjectId(allData['customerId'])
            if "costCenterId" in allData:
                allData['costCenterId'] = ObjectId(allData['costCenterId'])
            if "zoneId" in allData:
                allData['zoneId'] = ObjectId(allData['zoneId'])

            try:
                CUSTOMER = None
                ZONE = None
                COSTCENTER = None
                # customer+AIR-RAJ-MCT0361
                arr1 = [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': allData['customerId']
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'shortName': 1
                        }
                    }
                ]
                CUSTOMER = cmo.finding_aggregate("customer", arr1)[
                    'data'][0]['shortName']
                arr2 = [
                    {
                        '$match': {
                            '_id': allData['zoneId']
                        }
                    }, {
                        '$project': {
                            'shortCode': 1,
                            '_id': 0
                        }
                    }
                ]

                ZONE = cmo.finding_aggregate("zone", arr2)[
                    'data'][0]['shortCode']
                arr3 = [
                    {
                        '$match': {
                            '_id': allData['costCenterId']
                        }
                    }, {
                        '$project': {
                            'costCenter': 1,
                            '_id': 0
                        }
                    }
                ]
                COSTCENTER = cmo.finding_aggregate("costCenter", arr3)[
                    'data'][0]['costCenter']
                PROJECTGROUP = CUSTOMER+"-"+ZONE+"-"+COSTCENTER
                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added = "The Project Group "+" "+PROJECTGROUP + " Added"
                cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'Project Group', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            except Exception as e:
                print('Hello', e)
            response = cmo.insertion("projectGroup", allData)
            return respond(response)

        elif id != None:
            customerId = request.json.get("customerId")
            costCenterId = request.json.get("costCenterId")
            zoneId = request.json.get("zoneId")
            # customerId = ObjectId(request.json.get("customerId"))
            # costCenterId = ObjectId(request.json.get("costCenterId"))
            # zoneId = ObjectId(request.json.get("zoneId"))

            arra = [
                {
                    '$match': {
                        'customerId': ObjectId(customerId),
                        'costCenterId': ObjectId(costCenterId),
                        'zoneId': ObjectId(zoneId)

                    }
                }
            ]
            response = cmo.finding_aggregate("projectGroup", arra)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "This ProjectGroup is Already Exist",
                    "icon": "error",
                }, 400

            allData = {
                'customerId': ObjectId(customerId),
                "costCenterId": ObjectId(costCenterId),
                "zoneId": ObjectId(zoneId)
            }

            updateBy = {
                '_id': ObjectId(id)
            }

            try:
                arr = [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': ObjectId(id)
                        }
                    }, {
                        '$project': {
                            '_id': 0
                        }
                    }
                ]
                Responsed = cmo.finding_aggregate("projectGroup", arr)['data']
                allData2 = Responsed[0]
                changedThings = changedThings2(allData, allData2)
                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '

                CUSTOMER = None
                ZONE = None
                COSTCENTER = None
                # customer+AIR-RAJ-MCT0361
                arr1 = [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': allData['customerId']
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'shortName': 1
                        }
                    }
                ]
                CUSTOMER = cmo.finding_aggregate("customer", arr1)[
                    'data'][0]['shortName']
                arr2 = [
                    {
                        '$match': {
                            '_id': allData['zoneId']
                        }
                    }, {
                        '$project': {
                            'shortCode': 1,
                            '_id': 0
                        }
                    }
                ]

                ZONE = cmo.finding_aggregate("zone", arr2)[
                    'data'][0]['shortCode']
                arr3 = [
                    {
                        '$match': {
                            '_id': allData['costCenterId']
                        }
                    }, {
                        '$project': {
                            'costCenter': 1,
                            '_id': 0
                        }
                    }
                ]
                COSTCENTER = cmo.finding_aggregate("costCenter", arr3)[
                    'data'][0]['costCenter']
                PROJECTGROUP = CUSTOMER+"-"+ZONE+"-"+COSTCENTER

                Added = changedThings+" updated in this Project Group "+PROJECTGROUP
                cmo.insertion("AdminLogs", {'type': 'Update', 'module': 'costCenter', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            except Exception as e:
                print(e, 'dhhdhdhhdh')

            response = cmo.updating("projectGroup", updateBy, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            try:

                arr0 = [
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
                ]
                Responses = cmo.finding_aggregate(
                    "projectGroup", arr0)['data'][0]

                CUSTOMER = None
                ZONE = None
                COSTCENTER = None
                # customer+AIR-RAJ-MCT0361
                arr1 = [
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': Responses['customerId']
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'shortName': 1
                        }
                    }
                ]
                CUSTOMER = cmo.finding_aggregate("customer", arr1)[
                    'data'][0]['shortName']
                arr2 = [
                    {
                        '$match': {
                            '_id': Responses['zoneId']
                        }
                    }, {
                        '$project': {
                            'shortCode': 1,
                            '_id': 0
                        }
                    }
                ]

                ZONE = cmo.finding_aggregate("zone", arr2)[
                    'data'][0]['shortCode']
                arr3 = [
                    {
                        '$match': {
                            '_id': Responses['costCenterId']
                        }
                    }, {
                        '$project': {
                            'costCenter': 1,
                            '_id': 0
                        }
                    }
                ]
                COSTCENTER = cmo.finding_aggregate("costCenter", arr3)[
                    'data'][0]['costCenter']
                PROJECTGROUP = CUSTOMER+"-"+ZONE+"-"+COSTCENTER
                Added = "Deleted This Project Group "+PROJECTGROUP
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Project Group', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            except Exception as e:
                print('djjdjd', e)

            response = cmo.deleting(
                "projectGroup", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({'msg': "Please Provide Valid Unique Id"})





@admin_blueprint.route('/admin/manageProjectType/<id>', methods=['GET', "POST","PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageProjectType/<id>/<rowId>', methods=['GET', "POST","PATCH", "DELETE"])
@token_required
def manageprojecttype(current_user, id=None, rowId=None):

    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'custId': id
                }
            }
        ]

        if (rowId != None):

            arra = arra+[
                {
                    '$match': {
                        '_id': ObjectId(rowId)
                    }
                }
            ]
        arra = arra + [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$sort': {
                    'projectType': 1
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("projectType", arra)
        return respond(response)

    elif request.method == "POST":

        if rowId == None:
            projectType = request.form.get("projectType").strip()
            subProject = request.form.get("subProject").strip()
            status = request.form.get("status")
            arra = [
                {
                    '$match': {
                        'projectType': request.form.get("projectType"),
                        "subProject": request.form.get("subProject"),
                        "custId":id
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType", arra)['data']
            if (len(response)):
                return respond({
                    'status': 400,
                    "msg": "The Combination of Customer Project Type And Sub Project Type is already present in Database!",
                    "icon": "error",
                })
            allData = {
                'projectType': projectType,
                'subProject': subProject,
                'status': status,
                'custId': id
            }
            Added = "New Project  Type  " + \
                allData['projectType']+" Added with Sub Project " + \
                    allData['subProject']
            cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'Manage Project Type', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.insertion("projectType", allData)
            return respond(response)

        elif rowId != None:
            projectType = request.form.get("projectType")
            subProject = request.form.get("subProject")
            status = request.form.get("status")

            allData = {
                'projectType': projectType,
                'subProject': subProject,
                'status': status,
            }

            updateBy = {
                '_id': ObjectId(rowId)
            }

            response = cmo.updating("projectType", updateBy, allData, False)
            return respond(response)

    elif request.method == "PATCH":
        if id != None:
            allData = request.get_json()
            updateBy = {
                '_id': ObjectId(id)
            }

            if ("t_sengg" in allData):

                # siteId = False
                # prAll_rfi = False
                # auto_created = True
                # drop_down = True

                for datew in allData["t_sengg"]:

                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                    # if (datew["fieldName"] == "Site Id"):
                    #     siteId = True

                    # if (datew["fieldName"] == "RFAI Date" or datew["fieldName"] == "Allocation Date"):
                    #     prAll_rfi = True

                    # try:
                    #     if (datew["dataType"] == "Auto Created" and datew["dropdownValue"] == ''):
                    #         auto_created = False
                    # except:
                    #     auto_created = False
                    # try:
                    #     if (datew["dataType"] == "Dropdown" and datew["dropdownValue"] == ''):
                    #         drop_down = False
                    # except:
                    #     drop_down = False

                # if (siteId and prAll_rfi and auto_created and drop_down):

                response = cmo.updating("projectType", updateBy, allData, False)
                return respond(response)
                # else:

                    # finData = {
                    #     "status": 422,
                    #     "msg": ""
                    # }
                    # if (siteId == False):
                    #     finData["msg"] = 'The "Site Id" field does not exist. Please create it.'

                    # if (prAll_rfi == False):
                    #     finData["msg"] = 'The fields for "Allocation Date" or "RFAI Date" do not exist. Please create at least one of these fields.'

                    # if (auto_created == False):
                    #     finData["msg"] = 'Please select the value in Auto Created Field'

                    # if (drop_down == False):
                    #     finData["msg"] = 'Please fill the value in DropDown Field'

                    # return respond(finData)

            elif ("t_tracking" in allData):

                auto_created = True
                drop_down = True

                for datew in allData["t_tracking"]:

                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                    try:
                        if (datew["dataType"] == "Auto Created" and datew["dropdownValue"] == ''):
                            auto_created = False
                    except:
                        auto_created = False
                    try:
                        if (datew["dataType"] == "Dropdown" and datew["dropdownValue"] == ''):
                            drop_down = False
                    except:
                        drop_down = False

                if (auto_created and drop_down):
                    response = cmo.updating("projectType", updateBy, allData, False)
                    return respond(response)
                else:

                    finData = {
                        "status": 422,
                        "msg": ""
                    }
                    if (auto_created == False):
                        finData["msg"] = 'Please select the value in Auto Created Field'

                    if (drop_down == False):
                        finData["msg"] = 'Please fill the value in DropDown Field'

                    return respond(finData)

            elif ("t_issues" in allData):

                auto_created = True
                drop_down = True

                for datew in allData["t_issues"]:

                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                    try:
                        if (datew["dataType"] == "Auto Created" and datew["dropdownValue"] == ''):
                            auto_created = False
                    except:
                        auto_created = False
                    try:
                        if (datew["dataType"] == "Dropdown" and datew["dropdownValue"] == ''):
                            drop_down = False
                    except:
                        drop_down = False

                if (auto_created and drop_down):

                    response = cmo.updating("projectType", updateBy, allData, False)
                    return respond(response)
                else:

                    finData = {
                        "status": 422,
                        "msg": ""
                    }
                    if (auto_created == False):
                        finData["msg"] = 'Please select the value in Auto Created Field'

                    if (drop_down == False):
                        finData["msg"] = 'Please fill the value in DropDown Field'

                    return respond(finData)

            elif ("t_sFinancials-survey" in allData):

                for datew in allData["t_sFinancials-survey"]:
                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                response = cmo.updating("projectType", updateBy, allData, False)
                return respond(response)
            
            elif ("t_sFinancials-signange" in allData):

                for datew in allData["t_sFinancials-signange"]:
                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                response = cmo.updating("projectType", updateBy, allData, False)
                return respond(response)
            
            elif ("t_sFinancials-revisit" in allData):

                for datew in allData["t_sFinancials-revisit"]:
                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                response = cmo.updating("projectType", updateBy, allData, False)
                return respond(response)
            
            elif ("t_sFinancials-colo" in allData):

                for datew in allData["t_sFinancials-colo"]:
                    for key, value in datew.items():
                        if key in ["fieldName", "dropdownValue"]:
                            datew[key] = value.strip()

                response = cmo.updating("projectType", updateBy, allData, False)
                return respond(response)

            elif ("MileStone" in allData):
                for datew in allData["MileStone"]:
                    for key, value in datew.items():
                        if key == "fieldName":
                            datew[key] = value.strip()

                    if "Completion Criteria" in datew:
                        if "Completion Criteria" == "":
                            return respond({
                                'status': 422,
                                "msg": "Please fill Completion Criteria Field"
                            })
                    else:
                        return respond({
                            'status': 422,
                            "msg": "Please fill Completion Criteria Field"
                        })

                response = cmo.updating("projectType", updateBy, allData, False)
                return respond(response)

            elif ("Commercial" in allData):

                for datew in allData["Commercial"]:

                    if "ItemCode" not in datew or "UnitRate" not in datew:
                        return respond({
                            'msg': "Unit rate and item code are mandatory fields and must not be empty",
                            "icon": "error",
                            "status": 400
                        })
                    if "ItemCode" in datew:
                        if datew['ItemCode'] == "" or datew['ItemCode'] == None:
                            return respond({
                                'msg': "Unit rate and item code are mandatory fields and must not be empty",
                                "icon": "error",
                                "status": 400
                            })
                    if "UnitRate" in datew:
                        if datew['UnitRate'] == "" or datew['UnitRate'] == None:
                            return respond({
                                'msg': "Unit rate and item code are mandatory fields and must not be empty",
                                "icon": "error",
                                "status": 400
                            })
                        else:
                            datew['UnitRate'] = int(datew['UnitRate'])
                    for key, value in datew.items():
                        if key == "ItemCode":
                            datew[key] = value.strip()
                response = cmo.updating(
                    "projectType", updateBy, allData, False)
                return respond(response)
        else:
            return respond({
                'status': 400,
                "icon": "error",
                "msg": "Something went wrong"
            })

    elif request.method == "DELETE":
        if rowId != None:
            arra = [
                {
                    '$match': {
                        '_id': ObjectId(rowId)
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType", arra)['data'][0]
            projectType = response['projectType']
            customer = response['custId']
            arra = [
                {
                    '$match': {
                        'projectType': projectType,
                        'custId': customer
                    }
                }, {
                    '$lookup': {
                        'from': 'project',
                        'localField': '_id',
                        'foreignField': 'projectType',
                        'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                        'as': 'projectTypeResult'
                    }
                }, {
                    '$unwind': {
                        'path': '$projectTypeResult',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'projectStatus': '$projectTypeResult.status'
                    }
                }, {
                    '$match': {
                        'projectStatus': 'Active'
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType", arra)['data']
            if (len(response)):
                return respond({
                    'msg': 'This Project Type Contains Active Project',
                    "icon": 'error',
                    "status": 400
                })
            arrttss = [
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        '_id': ObjectId(rowId)
                    }
                }, {
                    '$project': {
                        'subProject': 1,
                        'projectType': 1,
                        '_id': 0
                    }
                }
            ]
            Responsert = cmo.finding_aggregate("projectType", arrttss)['data']
            Zone = None
            if len(Responsert):
                projectTypes = Responsert[0]['projectType']
                subProjects = Responsert[0]['subProject']
                Added = "Deleted This Sub Project "+subProjects + \
                    " with Project Type " + projectTypes
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Manage Project Type', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            response = cmo.deleting("projectType", rowId)
            return respond(response)
        else:
            return respond({
                "status": 400,
                "msg": 'Please Provide a Valid UniqueID',
                "icon": "error"
            })


@admin_blueprint.route('/admin/cardProjectType/<id>')
@admin_blueprint.route('/admin/cardProjectType/<id>/<projectTypeId>')
@admin_blueprint.route('/admin/cardProjectType/')
@token_required
def projecttype(current_user, id=None, projectTypeId=None):
    arra = []
    if request.args.get('customer') != "undefined" and request.args.get('customer') != None:
        arra = arra + [
            {
                '$match': {
                    'custId': request.args.get('customer')
                }
            }
        ]
    if id != None and id != "undefined":
        arra = arra + [
            {
                '$match': {
                    'custId': id
                }
            }
        ]
    if projectTypeId != None and projectTypeId != "undefined":
        arra = arra + [
            {
                '$match': {
                    '_id': ObjectId(projectTypeId)
                }
            }
        ]
    if current_user['roleName'] not in ['Admin', 'PMO']:
        dataArra = [
            {
                '$match': {
                    'empId': current_user['userUniqueId']
                }
            }, {
                '$lookup': {
                    'from': 'project',
                    'localField': 'projectIds',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'project'
                }
            }, {
                '$unwind': {
                    'path': '$project',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'projectType': '$project.projectType'
                }
            }, {
                '$lookup': {
                    'from': 'projectType',
                    'localField': 'projectType',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'ProjectType'
                }
            }, 
            {
                '$unwind': {
                    'path': '$ProjectType',
                    # 'preserveNullAndEmptyArrays': True
                }
            }, 
            {
                '$replaceRoot': {
                    'newRoot': '$ProjectType'
                }
            }
        ]
        response = cmo.finding_aggregate("projectAllocation", dataArra)
        projectType = []
        for i in response['data']:
            projectType.append(i['_id'])

        arra = arra + [
            {
                '$match': {
                    '_id': {
                        '$in': projectType
                    }
                }
            }
        ]

    arra = arra + [
        {
            '$addFields': {
                'uniqueId': {
                    '$toString': '$_id'
                },
                '_id': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$group': {
                '_id': '$projectType',
                'uniqueId': {
                    '$first': '$uniqueId'
                },
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
    response = cmo.finding_aggregate("projectType", arra)
    return respond(response)


@admin_blueprint.route('/admin/manageProject/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageProject/<id>/<rowId>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def project(current_user, id=None, rowId=None):

    if request.method == "GET":
        statusType = request.args.get("statusType")
        arra = []
        if id != None and id != "undefined":
            arra = arra + [
                {
                    "$match": {
                        'custId': id
                    }
                }
            ]
        if current_user['roleName'] not in ['Admin', 'PMO']:
            dataArra = [
                {
                    '$match': {
                        'empId':  current_user['userUniqueId']
                    }
                }, {
                    '$lookup': {
                        'from': 'project',
                        'localField': 'projectIds',
                        'foreignField': '_id',
                        'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                        'as': 'project'
                    }
                }, {
                    '$unwind': {
                        'path': '$project',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$replaceRoot': {
                        'newRoot': '$project'
                    }
                }
            ]
            response = cmo.finding_aggregate("projectAllocation", dataArra)
            project = []
            for i in response['data']:
                project.append(i['_id'])
            arra = arra + [
                {
                    '$match': {
                        '_id': {
                            '$in': project
                        }
                    }
                }
            ]

        if (rowId != None):
            arra = arra+[
                {
                    '$match': {
                        'projectType': ObjectId(rowId)
                    }
                }
            ]
        arra = arra+[
            {
                '$lookup': {
                    'from': 'projectType',
                    'localField': 'projectType',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                                'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                                'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                                'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                    'subProjectName': {
                        '$arrayElemAt': [
                            '$projectTypeResult.subProject', 0
                        ]
                    },
                    'PMId': {
                        '$toObjectId': '$PMId'
                    },
                    'projectGroupId': {
                        '$arrayElemAt': [
                            '$result.projectGroupId', 0
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'userRegister',
                    'localField': 'PMId',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
            }, {
                '$lookup': {
                    'from': 'circle',
                    'localField': 'circle',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'circleResult'
                }
            }, {
                '$unwind': {
                    'path': '$circleResult',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'circleName': '$circleResult.circleName',
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    '_id': {
                        '$toString': '$_id'
                    },
                    'circle': {
                        '$toString': '$circle'
                    },
                    'PMId': {
                        '$toString': '$PMId'
                    }
                }
            }, {
                '$addFields': {
                    'projectGroup': {
                        '$toString': '$projectGroup'
                    },
                    'projectType': {
                        '$toString': '$projectType'
                    }
                }
            }, {
                '$project': {
                    'circleResult': 0,
                    'PM': 0,
                    'result': 0,
                    'projectTypeResult': 0
                }
            }
        ]

        if (statusType):
            arra = arra+[
                {
                    '$match': {
                        'status': statusType
                    }
                }
            ]
        else:
            arra = arra+[
                {
                    '$match': {
                        'status': "Active"
                    }
                }
            ]
        # if (request.args.get('projectId')!=None and request.args.get("projectId")!='undefined'):
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'projectId':request.args.get('projectId')
        #             }
        #         }
        #     ]

        # if (request.args.get('projectGroup')!=None and request.args.get("projectGroup")!='undefined'):
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'projectGroupId':request.args.get('projectGroup')
        #             }
        #         }
        #     ]

        # if (request.args.get('projectType')!=None and request.args.get("projectType")!='undefined'):
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'projectTypeName':request.args.get('projectType')
        #             }
        #         }
        #     ]

        # if (request.args.get('projectManager')!=None and request.args.get("projectManager")!='undefined'):
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'PMName':request.args.get('projectManager')
        #             }
        #         }
        #     ]

        # if (request.args.get('circle')!=None and request.args.get("circle")!='undefined'):
        #     arra = arra + [
        #         {
        #             '$match':{
        #                 'circleName':request.args.get('circle')
        #             }
        #         }
        #     ]
        if (request.args.get('searvhView') != None and request.args.get("searvhView") != 'undefined'):
            searchView = request.args.get('searvhView').strip()
            arra = arra + [
                {
                    '$match': {
                        '$or': [
                            {
                                'projectId': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'projectGroupId': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'projectTypeName': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'PMName': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'circleName': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }
                        ]
                    }
                }
            ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("project", arra)
        return respond(response)

    elif request.method == "POST":
        if rowId == None:
            allData = request.get_json()
            allData['projectId'] = allData['projectId'].strip()
            arra = [
                {
                    '$match': {
                        'projectId': allData['projectId']
                    }
                }
            ]
            response = cmo.finding_aggregate("project", arra)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "This Project Id is Already Exist",
                    "icon": "error",
                }, 400
            if allData['startDate'] == "Invalid Date" or allData['endDate'] == "Invalid Date":
                return respond({
                    "status": 400,
                    "msg": "Start Date and End Date can't be Empty",
                    "icon": "error"
                })
            allData['projectType'] = ObjectId(allData['projectType'])
            allData['custId'] = id
            response = cmo.insertion("project", allData)
            evl.newproject(response['operation_id'], allData['projectId'], current_user['userUniqueId'])
            return respond(response)

        elif rowId != None:
            allData = request.get_json()
            if allData['startDate'] == "Invalid Date" or allData['endDate'] == "Invalid Date":
                return respond({
                    "status": 400,
                    "msg": "Start Date and End Date cannot be null",
                    "icon": "error"
                })
            if "projectTypeName" in allData:
                del allData['projectTypeName']
            if "projectType" in allData:
                allData['projectType'] = ObjectId(allData['projectType'])
            updateBy = {
                '_id': ObjectId(rowId)
            }
            response = cmo.updating("project", updateBy, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        if rowId != None:
            arra = [
                {
                    '$match': {
                        '_id': ObjectId(rowId)
                    }
                }, {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'SiteEngineer',
                        'localField': '_id',
                        'foreignField': 'projectuniqueId',
                        'as': 'site'
                    }
                }, {
                    '$unwind': {
                        'path': '$site',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'siteId': '$site._id'
                    }
                }, {
                    '$addFields': {
                        'siteId': {
                            '$toString': '$siteId'
                        }
                    }
                }, {
                    '$project': {
                        'siteId': 1,
                        '_id': 0
                    }
                }
            ]
            siteuid = cmo.finding_aggregate("project", arra)['data']
            cmo.deleting("project", rowId, current_user['userUniqueId'])
            cmo.deleting_m("SiteEngineer", {
                           'projectuniqueId': rowId}, current_user['userUniqueId'])
            cmo.deleting_m(
                "milestone", {'projectuniqueId': rowId}, current_user['userUniqueId'])
            if (len(siteuid)):
                for i in siteuid:
                    cmo.deleting_m(
                        "invoice", {'siteId': i['siteId']}, current_user['userUniqueId'])
            return respond({
                'status': 201,
                'msg': "Data Deleted Successfully",
                'data': []
            })
        else:
            return jsonify({'msg': 'Please Provide A Valid Unique Id'}), 404


@admin_blueprint.route('/admin/manageDepartment', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageDepartment/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def managedepartment(current_user, id=None):
    if request.method == "GET":
        arra = []
        if id!=None:
            arra = arra + [
                {
                    '$match':{
                        'customer':ObjectId(id)
                    }
                }
            ]
            
        arra = arra + [
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
                '$addFields': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    'customerResult': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("department", arra)
        return respond(response)

    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            if "customer" in allData:
                allData['customer'] = ObjectId(allData['customer'])
            arra = [
                {
                    '$match':{
                        'department':allData['department'],
                        'customer':allData['customer']
                    }
                }
            ]
            response = cmo.finding_aggregate("department",arra)['data']
            if len(response):
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer and Department  is already exist in Database"
                })
            Added = "The Department "+" "+allData['department'] + " Added"
            cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'Department', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.insertion("department", allData)
            return respond(response)
        elif id != None:
            allData = request.get_json()
            if "customer" in allData:
                allData['customer'] = ObjectId(allData['customer'])
            arra = [
                {
                    '$match':{
                        'department':allData['department'],
                        'customer':allData['customer']
                    }
                }
            ]
            response = cmo.finding_aggregate("department",arra)['data']
            if len(response):
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer and Department  is already exist in Database"
                })
            updateBy = {
                '_id': ObjectId(id)
            }
            Added = "The Department "+" "+allData['department'] + " Updated"
            cmo.insertion("AdminLogs", {'type': 'Update', 'module': 'Department', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.updating("department", updateBy, allData, False)
            return respond(response)
    elif request.method == "DELETE":
        if id != None:

            arr = [
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }, {
                    '$project': {
                        'department': 1,
                        '_id': 0
                    }
                }
            ]
            Response = cmo.finding_aggregate("department", arr)['data']
            Circle = None

            if len(Response):
                Circle = Response[0]['department']
                Added = "Deleted This department "+Circle
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Department', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.deleting("department", id)
            return respond(response)
        else:
            return jsonify({'msg': 'Please Provide A Vaild Unique Id'})


@admin_blueprint.route('/admin/manageDesignation', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/manageDesignation/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def managedesignation(current_user, id=None):
    
    if request.method == "GET":
        arra = []
        if id!=None:
            arra = arra + [
                {
                    '$match':{
                        'customer':ObjectId(id)
                    }
                }
            ]
        arra = arra + [
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
                '$addFields': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    'customerResult': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("designation", arra)
        return respond(response)
    
    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            if "customer" in allData:
                allData['customer'] = ObjectId(allData['customer'])
            arra = [
                {
                    '$match':{
                        'designation':allData['designation'],
                        'customer':allData['customer']
                    }
                }
            ]
            response = cmo.finding_aggregate("designation",arra)['data']
            if len(response):
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer and Designation is already exist in Database"
                })

            Added = "The Grade "+" "+allData['designation'] + " Added"
            cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'Grade', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.insertion("designation", allData)
            return respond(response)
        elif id != None:
            allData = request.get_json()
            if "customer" in allData:
                allData['customer'] = ObjectId(allData['customer'])
            arra = [
                {
                    '$match':{
                        'designation':allData['designation'],
                        'customer':allData['customer']
                    }
                }
            ]
            response = cmo.finding_aggregate("designation",arra)['data']
            if len(response):
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer and Designation is already exist in Database"
                })
            updateBy = {
                '_id': ObjectId(id)
            }
            Added = "The Grade "+" "+allData['designation'] + " Updated"
            cmo.insertion("AdminLogs", {'type': 'Update', 'module': 'Grade', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.updating("designation", updateBy, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            arr = [
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }, {
                    '$project': {
                        'designation': 1,
                        '_id': 0
                    }
                }
            ]
            Response = cmo.finding_aggregate("designation", arr)['data']
            Circle = None

            if len(Response):
                Circle = Response[0]['designation']
                Added = "Deleted This Grade "+Circle
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Grade', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            response = cmo.deleting("designation", id)
            return respond(response)
        else:
            return jsonify({'msg': 'Please Provide A Vaild Unique Id'})


@admin_blueprint.route('/admin/manageSite/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def managesite(current_user, id=None):
    if request.method == "GET":
        return "Hello"

    if request.method == "POST":
        formData = request.form
        formFile = request.files

        allData = {}

        allData['projectId'] = id

        for i in formData:
            allData[i] = formData[i]

        for k in formFile:
            path = cform.singleFileSaver(request.files.get(k), "", "")
            if path["status"] == 200:
                allData[k] = path["msg"]
            elif path["status"] == 422:
                return respond(path)

        response = cmo.insertion("sites", allData)
        return respond(response)








@admin_blueprint.route('/admin/getTemplate/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def template(current_user, id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("projectType", arra)
        return respond(response)


@admin_blueprint.route('/admin/projectAllocationList/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def projectallocationlist(current_user, id=None):
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
                        "$match": {
                            'deleteStatus': {'$ne': 1}
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
                'emp': 1
            }
        }, {
            '$lookup': {
                'from': 'userRegister',
                'localField': 'emp',
                'foreignField': '_id',
                'pipeline': [
                    {
                        '$match': {
                            'deleteStatus': {'$ne': 1},
                            'status': "Active"
                        }
                    }, {
                        '$project': {
                            'empName': 1
                        }
                    }, {
                        '$addFields': {
                            '_id': {
                                '$toString': '$_id'
                            }
                        }
                    }
                ],
                'as': 'empDetails'
            }
        }, {
            '$project': {
                '_id': 0,
                'emp': 0
            }
        }
    ]
    response = cmo.finding_aggregate("project", arra)
    return respond(response)


@admin_blueprint.route('/admin/manageSubProjectType/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
# @token_required
def subprojecttype(id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("projectType", arra)
        if (len(response['data']) > 0):
            customerId = response['data'][0]['custId']
            projectType = response['data'][0]['projectType']
            arra = [
                {
                    '$match': {
                        'projectType': projectType,
                        'custId': customerId
                    }
                }, {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }
            ]
            response = cmo.finding_aggregate("projectType", arra)
            return respond(response)
        else:
            return {'status': 400, 'msg': 'Data not found', 'data': []}


@admin_blueprint.route("/admin/userAccess", methods=["GET", "POST"])
def headers():
    if request.method == "GET":
        aggr = [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    'option': [
                        {
                            "label": "R",
                            "value": "R"
                        },
                        {
                            "label": "W",
                            "value": "W"
                        },
                        {
                            "label": "H",
                            "value": "H"
                        }
                    ],
                    'type': "select"
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("headers", aggr)
        return respond(response)


@admin_blueprint.route("/admin/complectionCriteria", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route("/admin/complectionCriteria/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def compectioncriteria(current_user, id=None):
    if request.method == "GET":
        arra = [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("complectionCriteria", arra)
        return respond(response)

    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            allData['completion'] = allData['completion'].strip()
            if "dropdown" in allData:
                allData['dropdown'] = allData['dropdown'].strip()
            Added = "The Completion Criteria " + \
                " "+allData['completion'] + " Added"
            cmo.insertion("AdminLogs", {'type': 'Add', 'module': 'Complection Criteria', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})
            response = cmo.insertion("complectionCriteria", allData)
            return respond(response)
        elif id != None:
            allData = request.get_json()
            allData['completion'] = allData['completion'].strip()
            if "dropdown" in allData:
                allData['dropdown'] = allData['dropdown'].strip()
            updateBy = {
                '_id': ObjectId(id)
            }

            Added = "The Completion Criteria "+" " + \
                allData['completion'] + " Updated"
            cmo.insertion("AdminLogs", {'type': 'Update', 'module': 'Completion Criteria', 'actionAt': current_time(
            ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            response = cmo.updating(
                "complectionCriteria", updateBy, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:

            arr = [
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }, {
                    '$project': {
                        'completion': 1,
                        '_id': 0
                    }
                }
            ]
            Response = cmo.finding_aggregate(
                "complectionCriteria", arr)['data']
            Circle = None

            if len(Response):
                Circle = Response[0]['completion']
                Added = "Deleted This Completion Criteria "+Circle
                cmo.insertion("AdminLogs", {'type': 'Delete', 'module': 'Completion Criteria', 'actionAt': current_time(
                ), 'actionBy': ObjectId(current_user['userUniqueId']), 'action': Added})

            response = cmo.deleting("complectionCriteria", id)
            return respond(response)
        else:
            return jsonify({'msg': 'Please Provide A Vaild Unique Id'})


@admin_blueprint.route("/admin/AdminLogs", methods=['GET', 'POST'])
@token_required
def AdminLogs(current_user):
    if request.method == "GET":
        arr = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
                }
            }]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        arr = arr+[
            {
                '$lookup': {
                    'from': 'userRegister',
                    'localField': 'actionBy',
                    'foreignField': '_id',
                    'pipeline': [
                        {
                            '$project': {
                                'empName': 1,
                                'ustCode': 1,
                                'email': 1,
                                '_id': 0
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
                '$project': {
                    'actionType': '$type',
                    'module': 1,
                    'actionBy': '$userResults.empName',
                    'action': 1,
                    'actionAt': 1,
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    '_id': 0,
                    'overall_table_count': 1
                }
            }
        ]

        Response = cmo.finding_aggregate("AdminLogs", arr)
        return respond(Response)


@admin_blueprint.route("/admin/projectType", methods=["GET"])
@token_required
def AdminProjectType(current_user):
    customerId = request.args.get("customerId").split(",")
    if request.args.get("partnerActivity") == "Yes":
        arra = [
            {
                '$match':{
                    'custId':{
                        '$in':customerId
                    }
                }
            }, {
                '$group':{
                    '_id':'$projectType'
                }
            }, {
                '$addFields': {
                    'projectType': '$_id'
                }
            }, {
                '$sort': {
                    'projectType': 1
                }
            }, {
                '$project': {
                    '_id': 0,
                    'projectType': 1,
                }
            }
        ]
        fetchData = cmo.finding_aggregate("projectType", arra)
        return respond(fetchData)
    else:
        arra = [
            {
                '$match': {
                    'custId': {
                        '$in': customerId
                    }
                }
            },
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
                '$group': {
                    '_id': '$projectType'
                }
            }, {
                '$lookup': {
                    'from': 'projectType',
                    'localField': '_id',
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
                '$sort': {
                    'projectType': 1
                }
            }, {
                '$project': {
                    '_id': 0,
                    'projectType': 1,
                    # 'custId': 1
                }
            }
        ]
        print('arraarraarra',arra)
        fetchData = cmo.finding_aggregate("project", arra)
        return respond(fetchData)


@admin_blueprint.route("/admin/projectSubType/<custId>/<projectTypeName>", methods=["GET"])
@token_required
def AdminProjectTypeName(current_user, custId=None, projectTypeName=None):
    aggr = [
        {"$match": {"custId": custId, "projectType": projectTypeName}},
        {"$project": {"_id": 0, "subProject": 1,
                      "custId": 1, 'uniqueId': {'$toString': '$_id'}}}
    ]
    fetchData = cmo.finding_aggregate("projectType", aggr)
    return respond(fetchData)


@admin_blueprint.route("/admin/projectSubTypeFieldName", methods=["GET"])
@token_required
def AdminSubTypeName(current_user):
    milestoneArgs = request.args.get("milestoneArgs")
    if milestoneArgs == "YES":
        subId = request.args.get("subProjectType")
        arra = [
            {
                '$match': {
                    '_id': ObjectId(subId),
                }
            }, {
                '$project': {
                    'MileStone': 1,
                    't_tracking':1,
                    '_id': 0
                }
            }
        ]
    elif request.args.get("partnerActivity") == "Yes":
        subId = request.args.get("subProjectType")
        arra = [
            {
                '$match': {
                    '_id': ObjectId(subId)
                }
            }, {
                '$unwind': {
                    'path': '$MileStone', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'fieldName': '$MileStone.fieldName'
                }
            }, {
                '$project': {
                    'fieldName': 1, 
                    '_id': 0
                }
            }
        ]
        
    else:
        customerId = request.args.get("customerId").split(",")
        projectType = request.args.get("projectType").split(",")
        arra = [
            {
                '$match': {
                    'custId': {
                        '$in': customerId
                    },
                    'projectType': {
                        '$in': projectType
                    }
                }
            }, {
                '$unwind': {
                    'path': '$MileStone'
                }
            }, {
                '$addFields': {
                    'milestoneName': '$MileStone.fieldName'
                }
            }, {
                '$match': {
                    'milestoneName': {
                        '$ne': 'Revenue Recognition'
                    }
                }
            }, {
                '$group': {
                    '_id': '$milestoneName',
                    'milestoneName': {
                        '$first': '$milestoneName'
                    }
                }
            }, {
                '$sort': {
                    'milestoneName': 1
                }
            }
        ]
    fetchData = cmo.finding_aggregate("projectType", arra)
    return respond(fetchData)


@admin_blueprint.route("/admin/addComplianceForm", methods=["POST", "GET", "PATCH","DELETE"])
@admin_blueprint.route("/admin/addComplianceForm/<id>", methods=["POST", "GET", "PATCH","DELETE"])
@token_required
def AddcomplianceForm(current_user, id=None):
    allMsg1111 = []

    if request.method == "POST":

        data = request.get_json()
        
        data = {key: value.strip() if isinstance(value, str) else value for key, value in data.items()}
        

        if "customer" in data:
            data['customer'] = ObjectId(data['customer'])

        if "subProject" in data:
            data['subProject'] = ObjectId(data['subProject'])

        arra = [
            {
                '$match': {
                    "customer": data['customer'],
                    "projectType": data['projectType'],
                    "subProject": data['subProject'],
                    "activity": data['activity'],
                    "oem": data['oem'],
                    "complianceMilestone": data['complianceMilestone']
                }
            }
        ]
        response = cmo.finding_aggregate("complianceForm", arra)['data']
        if len(response) > 0:
            return respond({
                'status': 400,
                "msg": "The Combination of Customer,Project Type,Sub-Type,Activity,OEM and Milestone is Already found in Database",
                "icon": "error"
            })
        res = cmo.insertion("complianceForm", data)
        return respond(res)

    elif request.method == "GET":
        arra = [
            {
                "$match":{
                    'formType':{'$ne':"Static"}
                }
            }, {
                '$lookup': {
                    'from': 'customer',
                    'localField': 'customer',
                    'foreignField': '_id',
                    'as': 'customerResult'
                }
            }, {
                '$lookup': {
                    'from': 'projectType',
                    'localField': 'subProject',
                    'foreignField': '_id',
                    'as': 'subTypeResult'
                }
            }, {
                '$addFields': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    },
                    'subProjectName': {
                        '$arrayElemAt': [
                            '$subTypeResult.subProject', 0
                        ]
                    },
                    'customer': {
                        '$toString': '$customer'
                    },
                    'subProject': {
                        '$toString': '$subProject'
                    },
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'customerResult': 0,
                    'subTypeResult': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        fetchData = cmo.finding_aggregate("complianceForm", arra)
        return respond(fetchData)

    elif request.method == "PATCH":
        if id != None:
            allData = request.get_json()
            setAllMsg=[]
            
            
            def checkData(data):
                if isinstance(data, list):
                    
                    return [checkData(item) for item in data]

                elif isinstance(data, dict):
                    if data.get("dataType","") != "Dropdown" and "dropdownValue" in data:
                        del data['dropdownValue']
                    for key, value in data.items():
                        print(key,"aefsjfbksjbfkj",value)
                        if data.get("dataType","") == "Dropdown" and key == "dropdownValue" and isinstance(value, str):
                            value= [(i+"*#@"+key+"*#@"+f'{data.get("index","")}') for i in value.split(",")]
                            
                            value = checkData(value)

                            data[key] = ",".join(value)
                        else:
                            if isinstance(value,str):
                                value=value+"*#@"+key+"*#@"+f'{data.get("index","")}'
                            data[key] = checkData(value)


                            

                    return data

                elif isinstance(data, str):
                    
                    
                    print(data,"asfjkhskajfhksdfh")
                    keyName=""
                    index=""
                    if "*#@" in data:
                        
                        
                        splitData=data.split("*#@")
                        data= splitData[0]
                        if len(splitData)>0:
                            keyName+=splitData[1]
                        if len(splitData)>1:
                            index = splitData[2]
                        

                    data = data.strip()
                    if data.lower() in ['undefined', '', "null"]:
                        msg = f"Please provide valid data for key {keyName}" if not index else f"Please provide valid data for key {keyName} at index {index}"
                        setAllMsg.append(msg)
                    return data

                elif isinstance(data, int):
                    if data < 0:
                        setAllMsg.append("Integer value cannot be negative")
                    return data

                elif isinstance(data, bool):
                    return data

                elif isinstance(data, float):
                    if data < 0.0:
                        setAllMsg.append("Float value cannot be negative")
                    return data

                elif data is None:
                    setAllMsg.append("Data cannot be None")

                return data

            allData = checkData(allData)
            
            if len(setAllMsg):
                return respond({"status": 400, "msg": ", ".join(list(set(setAllMsg))), "icon": "error"})
            updateBy = {
                '_id': ObjectId(id)
            }
            response = cmo.updating("complianceForm", updateBy, allData, False)
            return respond(response)

        else:
            return respond({
                'status': 400,
                "icon": "error",
                "msg": "Something went wrong"
            })
        
    elif request.method == "DELETE":
        if id!=None:
            response = cmo.deleting("complianceForm",id,current_user['userUniqueId'])
            return respond(response)
        else:
            return respond({
                'status': 400,
                "icon": "error",
                "msg": "Something went wrong"
            })


@admin_blueprint.route("/admin/addComplianceApprover", methods=["POST", "GET", "PATCH", "DELETE"])
@admin_blueprint.route("/admin/addComplianceApprover/<id>", methods=["POST", "GET", "PATCH", "DELETE"])
@token_required
def complianceApprover(current_user, id=None):

    if request.method == "POST":

        data = request.get_json()

        customerArray = []
        if "customer" in data and len(data['customer']) > 0:
            for i in data['customer']:
                customerArray.append(ObjectId(i))
        projectGroupArray = []
        if "projectGroup" in data and len(data['projectGroup']) > 0:
            for i in data['projectGroup']:
                projectGroupArray.append(ObjectId(i))

        data['customer'] = customerArray
        data['projectGroup'] = projectGroupArray
        data['empId'] = id

        updateBy = {
            'empId': id,
            "approverType": data['approverType']
        }
        res = cmo.updating("complianceApprover", updateBy, data, True)
        return respond(res)

    elif request.method == "GET":

        arra = [
            {
                '$match': {
                    'type': {'$ne': 'Partner'},
                    "status": "Active"
                }
            }, {
                "$addFields": {
                    "emp": {"$concat": ["$empName", "(", "$empCode", ")"]},
                    "uniqueId": {"$toString": "$_id"},
                    "_id": {"$toString": "$_id"},
                }
            }
        ]
        if (request.args.get("empUniqueId") != None and request.args.get("empUniqueId") != ""):
            arra = arra + [
                {
                    "$match": {
                        "emp": {
                            "$regex": cdc.regexspecialchar(request.args.get("empUniqueId")),
                            "$options": "i",
                        }
                    }
                }
            ]
        if request.args.get("profile") != None and request.args.get("profile") != "":
            profile = ObjectId(request.args.get("profile"))
            arra = arra + [
                {
                    "$match": {
                        "userRole": profile
                    }
                }
            ]
        arra = arra + [
            {
                '$project': {
                    'emp': 1,
                    'userRole': 1,
                    'uniqueId': 1
                }
            }, {
                '$lookup': {
                    'from': 'complianceApprover',
                    'localField': 'uniqueId',
                    'foreignField': 'empId',
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {'$ne': 1},
                                "approverType": request.args.get("approverType")
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
                    'complianceMilestone': '$result.complianceMilestone',
                    'customer': '$result.customer',
                    'projectGroup': '$result.projectGroup',
                    'projectType': '$result.projectType',
                    'userRole': '$role.roleName',
                    'approverType': '$result.approverType'
                }
            }, {
                '$lookup': {
                    'from': 'customer',
                    'localField': 'customer',
                    'foreignField': '_id',
                    'as': 'customerResult'
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
                        }, {
                            '$project': {
                                '_id': 0,
                                'projectGroupId': 1
                            }
                        }
                    ],
                    'as': 'projectGroupResult'
                }
            }, {
                '$addFields': {
                    'customerName': '$customerResult.customerName',
                    'projectGroupName': '$projectGroupResult.projectGroupId'
                }
            }, {
                '$addFields': {
                    'customerName': {
                        '$reduce': {
                            'input': '$customerName',
                            'initialValue': '',
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                    'projectGroupName': {
                        '$reduce': {
                            'input': '$projectGroupName',
                            'initialValue': '',
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                    'customer': {
                        '$map': {
                            'input': '$customer',
                            'as': 'customer',
                            'in': {
                                '$toString': '$$customer'
                            }
                        }
                    },
                    'projectGroup': {
                        '$map': {
                            'input': '$projectGroup',
                            'as': 'projectGroup',
                            'in': {
                                '$toString': '$$projectGroup'
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'customer': {
                        '$reduce': {
                            'input': '$customer',
                            'initialValue': '',
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                    'projectGroup': {
                        '$reduce': {
                            'input': '$projectGroup',
                            'initialValue': '',
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                    'complianceMilestone': {
                        '$reduce': {
                            'input': '$complianceMilestone',
                            'initialValue': '',
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                    'projectType': {
                        '$reduce': {
                            'input': '$projectType',
                            'initialValue': '',
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'projectGroupResult': 0,
                    'customerResult': 0,
                    'result': 0,
                    'role': 0,
                    '_id': 0,
                    'approverType': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("userRegister", arra)
        return respond(response)

    elif request.method == "PATCH":
        if id != None:
            allData = request.get_json()
            updateBy = {
                '_id': ObjectId(id)
            }
            response = cmo.updating(
                "complianceApprover", updateBy, allData, False)
            return respond(response)

        else:
            return respond({
                'status': 400,
                "icon": "error",
                "msg": "Something went wrong"
            })

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting(
                "complianceApprover", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({'msg': "Please Provide Valid Unique Id"})


@admin_blueprint.route("/admin/complainceMilestoneCard", methods=["GET"])
@token_required
def compliance_milestone_card(current_user):
    arra = [
        {
            '$match':{
                'formType':{'$ne':"Static"}
            }
        }, {
            '$group': {
                '_id': '$complianceMilestone',
                'complianceMilestone': {
                    '$first': '$complianceMilestone'
                }
            }
        }, {
            '$project': {
                '_id': 0
            }
        }, {
            '$sort': {
                'complianceMilestone': 1
            }
        }
    ]
    response = cmo.finding_aggregate("complianceForm", arra)
    return respond(response)


@admin_blueprint.route("/admin/repositorySiteId/<projectId>", methods=["GET"])
@token_required
def repository(current_user, projectId=None):
    arra = [
        {
            '$match': {
                'projectuniqueId': ObjectId(projectId)
            }
        }, {
            '$addFields': {
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$group': {
                '_id': '$siteuid',
                'siteuid': {
                    '$first': '$siteuid'
                },
                'subprojectId': {
                    '$first': '$subprojectId'
                },
                'milestoneArray': {
                    '$push': {
                        'Name': '$milestoneName',
                        'uniqueId': '$uniqueId'
                    }
                }
            }
        }, {
            '$sort': {
                '_id': -1
            }
        }, {
            '$project': {
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
            '$lookup': {
                'from': 'projectType',
                'localField': 'subprojectId',
                'foreignField': '_id',
                'as': 'subTypeResult'
            }
        }, {
            '$addFields': {
                'siteIdName': {
                    '$arrayElemAt': [
                        '$siteResult.Site Id', 0
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
                'projectType': {
                    '$arrayElemAt': [
                        '$subTypeResult.projectType', 0
                    ]
                },
                'subProjectType': {
                    '$arrayElemAt': [
                        '$subTypeResult.subProject', 0
                    ]
                }
            }
        }, {
            '$project': {
                'siteIdName': 1,
                'systemId': 1,
                'Unique ID': 1,
                'projectType': 1,
                'subProjectType': 1,
                'uniqueId': 1,
                '_id': 0,
                'milestoneArray': 1,
                'uniqueId': '1'
            }
        }
    ]
    res = cmo.finding_aggregate("complianceApproverSaver", arra)
    return respond(res)

@admin_blueprint.route("/admin/complianceDegrowTemplateData/<projectType>/<subProject>", methods=["GET"])
@token_required
def admin_complianceDegrowTemplateData(current_user,projectType=None,subProject=None):
    
    if request.args.get("milestone") !=None and request.args.get("milestone") == "Dismantle":
        arra = [
            {
                "$match":{
                    "siteuid":ObjectId(request.args.get("siteId")),
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
                if value=="YES" and key in fetchData['data'][1]['allObj']:
                    setObj[key] = fetchData['data'][1]['allObj'][key]
        return respond({
            'status':200,
            "data":[setObj]
        })
    
    elif request.args.get("milestone") != None and request.args.get("milestone") == "SRQ Raise":
        arra = [
            {
                '$match':{
                    'projectType': projectType, 
                    'subProject': subProject, 
                    "milestoneName":request.args.get("milestone")
                }
            }
        ]
    else:
        arra = [
            {
                '$match': {
                    'projectType': projectType, 
                    'subProject': subProject, 
                    'formType': 'Static'
                }
            }
        ] 
    arra = arra + [
        {
            '$project': {
                'tbAnteena': 1, 
                'existingAntenna': 1, 
                'radio': 1, 
                'bbuCard': 1, 
                'miscMaterial': 1, 
                "Template":1,
                '_id': 0
            }
        }
    ]
    res = cmo.finding_aggregate("complianceForm",arra)
    return respond(res)


@admin_blueprint.route("/admin/partnerWorkDescription", methods=["GET","POST","DELETE"])
@admin_blueprint.route("/admin/partnerWorkDescription/<id>", methods=["GET","POST","DELETE"])
@token_required
def admin_partner_work_description(current_user,id=None):
    if request.method == "GET":
        arra = []
        if request.args.get("customerId")!=None and request.args.get("customerId")!="undefined":
            arra = arra + [
                {
                    '$match':{
                        'customer':ObjectId(request.args.get("customerId"))
                    }
                }
            ]
        arra = arra + [
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
                '$addFields': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'customerResult': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("partnerWorkDescription",arra)
        return respond(response)
    
    if request.method == "POST":
        if id == None:
            allData = {k:v.strip() if isinstance(v,str) else v for k,v in request.get_json().items()}
            if "customer" in allData:
                allData['customer'] = ObjectId(allData['customer'])
            arra = [
                {
                    '$match':{
                        'customer':allData['customer'],
                        "workDescription":allData['workDescription']
                    }
                }
            ]
            res = cmo.finding_aggregate("partnerWorkDescription",arra)['data']
            if len(res)>0:
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer and Work Description Pair is already exist in Database."
                })
            response = cmo.insertion("partnerWorkDescription",allData)
            return respond(response)
        if id !=None:
            allData = {k:v.strip() if isinstance(v,str) else v for k,v in request.get_json().items()}
            if "customer" in allData:
                allData['customer'] = ObjectId(allData['customer'])
            arra = [
                {
                    '$match':{
                        'customer':allData['customer'],
                        "workDescription":allData['workDescription']
                    }
                }
            ]
            res = cmo.finding_aggregate("partnerWorkDescription",arra)['data']
            if len(res)>0:
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer and Work Description Pair is already exist in Database."
                })
            updateBy = {
                '_id':ObjectId(id)
            }
            response = cmo.updating("partnerWorkDescription",updateBy,allData,False)
            return respond(response)

    if request.method == "DELETE":
        if id!=None:
            response = cmo.deleting("partnerWorkDescription",id,current_user["userUniqueId"])
            return respond(response)
        else:
            return respond({
                'status':400,
                "msg":"Please provide valid Unique Id"
            })
            
@admin_blueprint.route("/admin/partnerActivity", methods=["GET","POST","DELETE"])
@admin_blueprint.route("/admin/partnerActivity/<id>", methods=["GET","POST","DELETE"])
@token_required
def admin_partner_activity(current_user,id=None):
    if request.method == "GET":
        arra = []
        if request.args.get("subProjectId")!=None and request.args.get("subProjectId")!='undefined':
            arra = arra + [
                {
                    '$match':{
                        'subProject':ObjectId(request.args.get("subProjectId"))
                    }
                }
            ]
        arra = arra + [
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
                    'as': 'subResult'
                }
            }, {
                '$lookup': {
                    'from': 'partnerWorkDescription', 
                    'localField': 'workDescription', 
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
                    'as': 'workResult'
                }
            }, {
                '$addFields': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'subProjectTypeName': {
                        '$arrayElemAt': [
                            '$subResult.subProject', 0
                        ]
                    }, 
                    'workDescriptionName': {
                        '$arrayElemAt': [
                            '$workResult.workDescription', 0
                        ]
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }, 
                    'subProject': {
                        '$toString': '$subProject'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'workDescription': {
                        '$toString': '$workDescription'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'customerResult': 0, 
                    'subResult': 0, 
                    'workResult': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("partnerActivity",arra)
        return respond(response)
    
    if request.method == "POST":
        if id == None:
            allData = {k:v.strip() if isinstance(v,str) else v for k,v in request.get_json().items()}
            for key in ['customer',"subProject","workDescription"]:
                if key in allData:
                    allData[key] = ObjectId(allData[key])
            arra = [
                {
                    '$match':{
                        "subProject":allData['subProject'],
                        "customer":allData['customer'],
                        "workDescription":allData['workDescription']
                    }
                }
            ]
            res = cmo.finding_aggregate("partnerActivity",arra)['data']
            if len(res)>0:
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"The Combination of Customer,Project Type,Sub Project Type and Work Description is already exist in Database."
                })
            response = cmo.insertion("partnerActivity",allData)
            return respond(response)
        if id !=None:
            allData = {k:v.strip() if isinstance(v,str) else v for k,v in request.get_json().items()}
            data = {
                'milestone':allData['milestone'],
            }
            updateBy = {
                '_id':ObjectId(id)
            }
            response = cmo.updating("partnerActivity",updateBy,data,False)
            return respond(response)

    if request.method == "DELETE":
        if id!=None:
            response = cmo.deleting("partnerActivity",id,current_user["userUniqueId"])
            return respond(response)
        else:
            return respond({
                'status':400,
                "msg":"Please provide valid Unique Id"
            })
            
@admin_blueprint.route("/admin/deliveryPva", methods=["GET","POST"])
@admin_blueprint.route("/admin/deliveryPva/<id>", methods=["GET","POST"])
@token_required
def admin_delivery_pva(current_user,id=None):
    if request.method == "GET":
        arra = [
            {
                '$group': {
                    '_id': {
                        'customer': '$custId', 
                        'projectType': '$projectType'
                    }, 
                    'projectTypeId': {
                        '$first': '$_id'
                    }
                }
            }, {
                '$addFields': {
                    'customer': {
                        '$toObjectId': '$_id.customer'
                    }, 
                    'projectType': '$_id.projectType'
                }
            }, {
                '$lookup': {
                    'from': 'masterSubProject', 
                    'let': {
                        'projectType': '$projectType', 
                        'customer': '$customer'
                    }, 
                    'pipeline': [
                        {
                            "$match":{
                                'deleteStatus':{"$ne":1}
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
                                                '$customer', '$$customer'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'projectType', 
                                'localField': 'subProjectIds', 
                                'foreignField': '_id', 
                                'pipeline':[
                                    {
                                        "$match":{
                                            'deleteStatus':{"$ne":1}
                                        }
                                    }
                                ],
                                'as': 'result'
                            }
                        }, {
                            '$addFields': {
                                'subProjectName': '$result.subProject', 
                                'subProjectId': '$subProjectId'
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
                    'subProjectId': {
                        '$arrayElemAt': [
                            '$result.subProjectId', 0
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'subProjectName': {
                        '$reduce': {
                            'input': '$subProjectName', 
                            'initialValue': '', 
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                    'subProjectId': {
                        '$reduce': {
                            'input': '$subProjectId', 
                            'initialValue': '', 
                            'in': {
                                '$concat': [
                                    '$$value', {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$$value', ''
                                                ]
                                            }, '', ','
                                        ]
                                    }, '$$this'
                                ]
                            }
                        }
                    },
                }
            }, {
                '$sort': {
                    'projectTypeId': 1
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
                    'as': 'customerResult'
                }
            }, {
                '$project': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'projectType': 1, 
                    'customerId': {
                        '$toString': '$customer'
                    }, 
                    'projectTypeId': {
                        '$toString': '$projectTypeId'
                    }, 
                    'uniqueId': {
                        '$toString': '$projectTypeId'
                    }, 
                    'subProjectName': 1, 
                    'subProjectId': 1, 
                    '_id': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("projectType",arra)
        return respond(response)
    
    if request.method == "POST":
        allData = request.get_json()
        subProject = []
        for i in allData['subProjectIds']:
            subProject.append(ObjectId(i))
        allData['customer'] = ObjectId(allData['customer'])
        allData['subProjectId'] = allData['subProjectIds']
        allData['subProjectIds'] = subProject
        
        updateBy = {
            'customer':ObjectId(allData['customer']),
            "projectType":allData['projectType']
        }
        res = cmo.updating("masterSubProject",updateBy,allData,True)
        return respond(res)
        
    
@admin_blueprint.route("/admin/deliveryPva/subProject/<id>", methods=["GET"])
@token_required
def admin_delivery_pva_subProject(current_user,id=None):
    arra = [
        {
            '$match': {
                '_id': ObjectId(id)
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
                        '$match':{
                            'deleteStatus':{'$ne':1}
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
            '$replaceRoot': {
                'newRoot': '$result'
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }, {
            '$project': {
                'subProject': '$subProject', 
                'subProjectId': {
                    '$toString': '$_id'
                }, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)
    return respond(response)


@admin_blueprint.route("/admin/deliveryPva/masterSubProject/<id>", methods=["GET"])
@token_required
def admin_masterSubProject_list_table(current_user,id=None):
    arra = [
        {
            '$match': {
                'customer': ObjectId(id)
            }
        }, {
            '$unwind': {
                'path': '$subProjectIds', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'subProjectIds', 
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
                'subProjectName': {
                    '$arrayElemAt': [
                        '$result.subProject', 0
                    ]
                }, 
                'subProjectId': {
                    '$toString': '$subProjectIds'
                }, 
                '_id': 0, 
                'projectType': 1
            }
        }, {
            '$addFields': {
                'subProjectName': {
                    '$concat': [
                        '$subProjectName', '(', '$projectType', ')'
                    ]
                }
            }
        }
    ]
    res = cmo.finding_aggregate("masterSubProject",arra)
    return respond(res)

    

# # Global Notification
# @admin_blueprint.route("/globalNotify",methods=['GET','POST'])
# @admin_blueprint.route("/globalNotify/<userId>",methods=['GET','POST'])
# @token_required
# def globalNotification(current_user,userId=None):
#         if request.method == 'GET':   
#             if userId==None:
#                 aggr = [
#                     {
#                         '$project': {
#                             'type': 1, 
#                             'message': 1, 
#                             '_id': {
#                                 '$toString': '$_id'
#                             }, 
#                             'createdAt': {
#                                 '$dateToString': {
#                                     'format': '%d-%m-%Y %H:%M', 
#                                     'date': {
#                                         '$toDate': '$createdAt'
#                                     }, 
#                                     'timezone': 'Asia/Kolkata'
#                                 }
#                             }
#                         }
#                     }
#                 ]
#                 response = cmo.finding_aggregate('globalNotification',aggr)
#                 return respond(response)
#             aggr=[
#                 {
#                     '$match': {
#                         '_id': ObjectId(userId)
#                     }
#                 }, {
#                     '$addFields': {
#                         'globalNotificationObjId': {
#                             '$convert': {
#                                 'input': '$globalNotification', 
#                                 'to': 'objectId', 
#                                 'onError': None, 
#                                 'onNull': None
#                             }
#                         }
#                     }
#                 }, {
#                     '$lookup': {
#                         'from': 'globalNotification', 
#                         'localField': 'globalNotificationObjId', 
#                         'foreignField': '_id', 
#                         'as': 'Notification'
#                     }
#                 }, {
#                     '$unwind': '$Notification'
#                 }, {
#                     '$project': {
#                         '_id': 0, 
#                         'file': '$Notification.file', 
#                         'msg': '$Notification.message', 
#                         'type': '$Notification.type'
#                     }
#                 }
#             ]
#             # aggr = [
#             #     {
#             #         '$match': {
#             #             '_id': ObjectId(userId)
#             #         }
#             #     }, {
#             #         '$addFields': {
#             #             'globalNotificationObjId': {
#             #                 '$toObjectId': '$globalNotification'
#             #             }
#             #         }
#             #     }, {
#             #         '$lookup': {
#             #             'from': 'globalNotification', 
#             #             'localField': 'globalNotificationObjId', 
#             #             'foreignField': '_id', 
#             #             'as': 'Notification'
#             #         }
#             #     }, {
#             #         '$unwind': '$Notification'
#             #     }, {
#             #         '$project': {
#             #             '_id': 0, 
#             #             'file': '$Notification.file', 
#             #             'msg': '$Notification.message', 
#             #             'type': '$Notification.type'
#             #         }
#             #     }
#             # ]
#             response = cmo.finding_aggregate('userRegister',aggr)
#             return respond(response)
#         elif request.method=='POST':

#             if request.args.get("File", "").lower() == "true":

#                 allData ={}
#                 for key in request.files:
#                     file = request.files.get(key)
#                     if file:
#                         pathing = cform.singleFileSaver(file, "", ["pdf"])
#                         if pathing["status"] == 422:
#                             return respond(pathing)
#                         elif pathing["status"] == 200:
#                             allData[key] = pathing["msg"]
#                     else:
#                         return respond({"status": 400, "msg": "Only PDF files are allowed", "icon": "error"})

#                 allData = {
#                     'message':allData.get('msg'),
#                     'createdAt':int(unique_timestampexpense()),
#                     'createdBy':current_user['userUniqueId'],
#                     'type':'File'
#                 }
#                 response = cmo.insertion("globalNotification", allData)
#                 if response.get('operation_id'):
#                     cmo.updating_m("userRegister", {}, {"globalNotification": response['operation_id']})
#                     return respond(response)
#             else:

#                 allData = request.get_json()
#                 if not allData:
#                     return respond({
#                             'show':True,
#                             'status': 400,
#                             'text': 'No JSON data provided',
#                             'icon': 'error'
#                         })
#                 allData = {
#                     'message':allData.get('msg'),
#                     'createdAt':int(unique_timestampexpense()),
#                     'createdBy':current_user['userUniqueId'],
#                     'type':'message'
#                 }
#                 response = cmo.insertion('globalNotification',allData)
#                 if response.get('operation_id'):
#                     cmo.updating_m("userRegister",{},{"globalNotification": response.get('operation_id')})
#                     return respond(response)

        
# # api for clear notification
# @admin_blueprint.route('/clearNotify',methods=['PATCH'])
# @token_required
# def clearNotify(current_user):
#     if current_user['userUniqueId']==None:
#         return respond({
#                 'show':True,
#                 'status': 400,
#                 'text': ' User Id Required',
#                 'icon': 'error'
#             })
#     updateBy={
#         '_id':ObjectId(current_user['userUniqueId'])
#     }
#     allData = request.get_json()
#     response = cmo.updating('userRegister', updateBy,allData,False)
#     return respond(response)

            
# Global Notification
@admin_blueprint.route("/globalNotify",methods=['GET','POST'])
@admin_blueprint.route("/globalNotify/<userId>",methods=['GET','POST'])
@token_required
def globalNotification(current_user,userId=None):
        if request.method == 'GET':   
            if userId==None:
                aggr = [
                    {
                        '$project': {
                            'type': 1, 
                            'message': 1, 
                            '_id': {
                                '$toString': '$_id'
                            }, 
                            'createdAt': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$createdAt'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }
                        }
                    }
                ]
                # pagination added with total count
                aggr+=apireq.commonarra
                # print("thisis aggr",arra)
                if request.args.get('page') and request.args.get('limit'):
                    aggr = aggr+apireq.args_pagination(request.args)
                # pagination added with total count
                response = cmo.finding_aggregate('globalNotification',aggr)
                return respond(response)
            aggr=[
                {
                    '$match': {
                        '_id': ObjectId(userId)
                    }
                }, {
                    '$addFields': {
                        'globalNotificationObjId': {
                            '$convert': {
                                'input': '$globalNotification', 
                                'to': 'objectId', 
                                'onError': None, 
                                'onNull': None
                            }
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'globalNotification', 
                        'localField': 'globalNotificationObjId', 
                        'foreignField': '_id', 
                        'as': 'Notification'
                    }
                }, {
                    '$unwind': '$Notification'
                }, {
                    '$project': {
                        '_id': 0, 
                        'file': '$Notification.file', 
                        'msg': '$Notification.message', 
                        'type': '$Notification.type'
                    }
                }
            ]
            # [
            #     {
            #         '$match': {
            #             '_id': ObjectId(userId)
            #         }
            #     }, {
            #         '$addFields': {
            #             'globalNotificationObjId': {
            #                 '$toObjectId': '$globalNotification'
            #             }
            #         }
            #     }, {
            #         '$lookup': {
            #             'from': 'globalNotification', 
            #             'localField': 'globalNotificationObjId', 
            #             'foreignField': '_id', 
            #             'as': 'Notification'
            #         }
            #     }, {
            #         '$unwind': '$Notification'
            #     }, {
            #         '$project': {
            #             '_id': 0, 
            #             'file': '$Notification.file', 
            #             'msg': '$Notification.message', 
            #             'type': '$Notification.type'
            #         }
            #     }
            # ]
            response = cmo.finding_aggregate('userRegister',aggr)
            return respond(response)
        elif request.method=='POST':

            if request.args.get("File", "").lower() == "true":

                allData ={}
                for key in request.files:
                    file = request.files.get(key)
                    if file:
                        pathing = cform.singleFileSaver(file, "", ["pdf"])
                        if pathing["status"] == 422:
                            return respond(pathing)
                        elif pathing["status"] == 200:
                            allData[key] = pathing["msg"]
                    else:
                        return respond({"status": 400, "msg": "Only PDF files are allowed", "icon": "error"})

                allData = {
                    'message':allData.get('msg'),
                    'createdAt':int(unique_timestampexpense()),
                    'createdBy':current_user['userUniqueId'],
                    'type':'File'
                }
                response = cmo.insertion("globalNotification", allData)
                if response.get('operation_id'):
                    cmo.updating_m("userRegister", {}, {"globalNotification": response['operation_id']})
                    return respond(response)
            else:

                allData = request.get_json()
                if not allData:
                    return respond({
                            'show':True,
                            'status': 400,
                            'text': 'No JSON data provided',
                            'icon': 'error'
                        })
                allData = {
                    'message':allData.get('msg'),
                    'createdAt':int(unique_timestampexpense()),
                    'createdBy':current_user['userUniqueId'],
                    'type':'message'
                }
                response = cmo.insertion('globalNotification',allData)
                if response.get('operation_id'):
                    cmo.updating_m("userRegister",{},{"globalNotification": response.get('operation_id')})
                    return respond(response)

        
# api for clear notification
@admin_blueprint.route('/clearNotify',methods=['PATCH'])
@token_required
def clearNotify(current_user):
    if current_user['userUniqueId']==None:
        return respond({
                'show':True,
                'status': 400,
                'text': ' User Id Required',
                'icon': 'error'
            })
    updateBy={
        '_id':ObjectId(current_user['userUniqueId'])
    }
    allData = request.get_json()
    response = cmo.updating('userRegister', updateBy,allData,False)
    return respond(response)


#api for adding snap template of Physical At
@admin_blueprint.route("/admin/snap/<id>", methods=["PATCH"])
@token_required
def add_snap(current_user,id=None):
    updateBy = {'_id': ObjectId(id)}

    aggr = [
        {"$match": {'_id': ObjectId(id)}},
        {"$project": {"snap": 1}}
    ]
    
    result = cmo.finding_aggregate('complianceForm', aggr)
    existing_data = result.get('data', [])
    
    if not existing_data:
        return respond({"status": 404, "msg": "Document not found", "icon": "error"})


    existing_snap = existing_data[0].get("snap", [])
    uploaded_files = request.files 
    file_type = ["jpg", "jpeg", "png"]

    # if not uploaded_files:
    #     return respond({"status": 400, "msg": "No data in Paylaod", "icon": "error"})

    snap_dict = {item.get("index"): item for item in existing_snap}
    for key, file in uploaded_files.items():

        print(key,"key",file,"file")

        if file:
            pathing = cform.singleFileSaver(file, "", file_type)
            if pathing["status"] != 200:
                return respond(pathing)

            if key in snap_dict:

                snap_dict[key]["image"] = pathing["msg"]

    updated_snap = list(snap_dict.values())
    updated_snap.sort(key=lambda x: x["index"])


    print(updated_snap,"updated_snap")

    update_payload = {"snap": updated_snap}
    response = cmo.updating('complianceForm', updateBy, update_payload, False)
    return respond(response)

@admin_blueprint.route("/sample/snap", methods=["GET"])
def add_snap_sample():
    not_created = []
    id_list = [
        "687f19731565a8c19ec11c35",
        "687f19f11565a8c19ec11c6c",
        "687f1a4e1565a8c19ec11ca6",
        "687f1a621565a8c19ec11ca7",
        "687f1b0a1565a8c19ec11d5d",
        "687f682a1565a8c19ec14177",
        "687f68401565a8c19ec1417f",
        "687f68701565a8c19ec1419a",
        "687f68801565a8c19ec1419f",
        "687f689c1565a8c19ec141a6",
        "687f6a2c1565a8c19ec14224",
        "687f6a571565a8c19ec14227",
        "687f6a781565a8c19ec1422a",
        "687f6a7f1565a8c19ec1422d",
        "687f6a941565a8c19ec1422f",
        "687f6a951565a8c19ec14230",
        "687f6aa11565a8c19ec14233",
        "687f6abb1565a8c19ec1439c",
        "687f6abe1565a8c19ec1439d",
        "687f6acc1565a8c19ec1439e",
        "687f6af41565a8c19ec143ac",
        "687f6b071565a8c19ec143bd",
        "687f6b1c1565a8c19ec143ca",
        "687f6b1f1565a8c19ec143cc",
        "687f6b311565a8c19ec143d3",
        "687f6b3f1565a8c19ec143db",
        "687f6b4e1565a8c19ec143e0",
        "687f6b531565a8c19ec143e1",
        "687f6b6a1565a8c19ec143e5",
        "687f6b7d1565a8c19ec143e9",
        "687f6b9b1565a8c19ec143f0",
        "687f6bad1565a8c19ec143f3",
        "687f6bdf1565a8c19ec1440d",
        "687f6bed1565a8c19ec14410",
        "687f6bf71565a8c19ec14412",
        "687f6bfa1565a8c19ec14413",
        "687f6c101565a8c19ec14417",
        "687f6c101565a8c19ec14418",
        "687f6c1f1565a8c19ec1441d",
        "687f6c261565a8c19ec1441e",
        "687f6c361565a8c19ec14422",
        "687f6c421565a8c19ec14424",
        "687f6c551565a8c19ec1442c",
        "687f6c5c1565a8c19ec1442e",
        "687f6c6d1565a8c19ec14437",
        "687f6c6f1565a8c19ec14438",
        "687f6c7e1565a8c19ec1443a",
        "687f6cb41565a8c19ec14441",
        "687f6cb61565a8c19ec14443",
        "687f6cc01565a8c19ec14445",
        "687f6cc81565a8c19ec14447",
        "687f6cee1565a8c19ec14449",
        "687f6cfc1565a8c19ec1445e",
        "687f6d011565a8c19ec1445f",
        "687f6d141565a8c19ec14470",
        "687f6d171565a8c19ec14471",
        "687f6d2b1565a8c19ec14481",
        "687f6d481565a8c19ec14490",
        "687f6d551565a8c19ec14492",
        "687f6d5a1565a8c19ec14494",
        "687f6d6a1565a8c19ec144b0",
        "687f6d6e1565a8c19ec144b1",
        "687f6d791565a8c19ec144b6",
        "687f6dd31565a8c19ec144f7",
        "687f6df01565a8c19ec1450c",
        "687f6df81565a8c19ec1450d",
        "687f6e071565a8c19ec1450e",
        "687f6e361565a8c19ec14523",
        "687f6e3d1565a8c19ec14536",
        "687f6e4e1565a8c19ec14538",
        "687f6e551565a8c19ec1454a",
        "687f6e5d1565a8c19ec1454f",
        "687f6e651565a8c19ec14550",
        "687f6e6e1565a8c19ec1455f",
        "687f6e791565a8c19ec145f8",
        "687f6e911565a8c19ec14612",
        "687f6ea31565a8c19ec14618",
        "687f6eb21565a8c19ec1461e",
        "687f6ec31565a8c19ec14626",
        "687f6edd1565a8c19ec14632",
        "687f6ee91565a8c19ec14637",
        "687f6f741565a8c19ec14662",
        "687f6f881565a8c19ec14666",
        "687f6f9e1565a8c19ec14673",
        "687f6fab1565a8c19ec14677",
        "687f6fc31565a8c19ec14680",
        "687f762d1565a8c19ec14b36",
        "687f763e1565a8c19ec14b38",
        "687f76521565a8c19ec14b3b",
        "687f766f1565a8c19ec14b52",
        "687f767c1565a8c19ec14b59",
        "687f768b1565a8c19ec14b60",
        "687f76981565a8c19ec14b64",
        "687f76ad1565a8c19ec14b69",
        "687f76cc1565a8c19ec14b79",
        "687f76e31565a8c19ec14b8b",
        "687f76f81565a8c19ec14b9c",
        "687f770c1565a8c19ec14bac",
        "687f77241565a8c19ec14bd0",
        "687f773c1565a8c19ec14be5",
        "687f77501565a8c19ec14be9",
        "687f776a1565a8c19ec14bf8",
        "687f77961565a8c19ec14c12",
        "687f77aa1565a8c19ec14c1e",
        "687f77cc1565a8c19ec14c33",
        "687f77e01565a8c19ec14c3c",
        "687f780b1565a8c19ec14c56",
        "687f78191565a8c19ec14c5c",
        "687f782d1565a8c19ec14c61",
        "687f78311565a8c19ec14c64",
        "687f78681565a8c19ec14c79",
        "687f787b1565a8c19ec14c8f",
        "687f789b1565a8c19ec14cb4",
        "687f78af1565a8c19ec14cc5",
        "687f78f51565a8c19ec14cea",
        "687f793a1565a8c19ec14d0f",
        "687f79511565a8c19ec14d17",
        "687f79851565a8c19ec14d40",
        "687f79991565a8c19ec14d46",
        "687f79b51565a8c19ec14d4e",
        "687f79d81565a8c19ec14d54",
        "687f7a0c1565a8c19ec14d57",
        "687f7a221565a8c19ec14d58",
        "687f7a7f1565a8c19ec14d6f",
        "687f7a911565a8c19ec14d70",
        "687f7aa81565a8c19ec14d75",
        "687f7acf1565a8c19ec14d78",
        "687f7adc1565a8c19ec14d79",
        "687f7af61565a8c19ec14ddc",
        "687f7b021565a8c19ec14dde",
        "687f7b381565a8c19ec14de7",
        "687f7b591565a8c19ec14df3",
        "687f7b691565a8c19ec14df8",
        "687f7be01565a8c19ec14e37",
        "687f7bf61565a8c19ec14e3c",
        "687f7c4a1565a8c19ec14e67",
        "687f7c5a1565a8c19ec14e6b",
        "687f7c6c1565a8c19ec14e84",
        "687f7c7f1565a8c19ec14e87",
        "687f7c8f1565a8c19ec14e9a",
        "687f7ca41565a8c19ec14eb0",
        "687f7cca1565a8c19ec14ec5",
        "687f7cf51565a8c19ec14edc",
        "687f7d051565a8c19ec14eec",
        "687f7d161565a8c19ec14efc",
        "687f7d261565a8c19ec14efd",
        "687f7d331565a8c19ec14f0c",
        "687f7d4b1565a8c19ec14f0d",
        "687f7d761565a8c19ec14f31",
        "687f7d861565a8c19ec14f36",
        "687f7d9b1565a8c19ec14f49",
        "687f7db11565a8c19ec14f5a",
        "687f7dcd1565a8c19ec14f6b",
        "687f7def1565a8c19ec14f7e",
        "687f7e021565a8c19ec14f8f",
        "687f7e0f1565a8c19ec14f9e",
        "687f7e1f1565a8c19ec14fa0",
        "687f7e301565a8c19ec14fb1",
        "687f7e5b1565a8c19ec14fc9",
        "687f7e6d1565a8c19ec14fcd",
        "687f7e7e1565a8c19ec14fdc",
        "687f7e8e1565a8c19ec14fdd",
        "687f7eba1565a8c19ec15015",
        "687f7ee81565a8c19ec1503a",
        "687f7f081565a8c19ec15054",
        "687f7f1a1565a8c19ec15055",
        "687f7f2d1565a8c19ec15058",
        "687f7f421565a8c19ec1507b",
        "687f7f581565a8c19ec15090",
        "687f7f661565a8c19ec150a3",
        "687f7f771565a8c19ec150a9",
        "687f7f941565a8c19ec150b0",
        "687f7fcf1565a8c19ec150bb",
        "687f7fe11565a8c19ec150c1",
        "687f7fef1565a8c19ec150c4",
        "687f7ffb1565a8c19ec150cb",
        "687f80111565a8c19ec150d1",
        "687f80211565a8c19ec150d5",
    ]
    # count = 0
    # for i in id_list:
    #     count = count+1
    #     updateBy = {'_id': ObjectId(i)}

    #     aggr = [
    #         {"$match": {'_id': ObjectId(i)}},
    #         {"$project": {"snap": 1}}
    #     ]
    
    #     result = cmo.finding_aggregate('complianceForm', aggr)
    #     existing_data = result.get('data', [])

    #     if len(existing_data) == 0:
    #         not_created.append(i)
    #         continue
    
    

    #     existing_snap = existing_data[0].get("snap", [])
        
    #     if  len(existing_snap) == 0:
    #         not_created.append(i)
    #         continue

    #     image_dict = {
    #         1: "uploads/S1.jpeg",
    #         2: "uploads/S2.jpeg",
    #         3: "uploads/S3.jpeg",
    #         4: "uploads/S4.jpeg",
    #         5: "uploads/S5.jpeg",
    #         6: "uploads/S6.jpeg",
    #         7: "uploads/S7.jpeg",
    #         8: "uploads/S8.jpeg",
    #         9: "uploads/S9.jpeg",
    #         11: "uploads/S11.jpeg",
    #         12: "uploads/S12.jpeg",
    #         14: "uploads/S14.jpeg",
    #         15: "uploads/S15.jpeg",
    #         16: "uploads/S16.jpeg",
    #         18: "uploads/S18.jpeg",
    #         20: "uploads/S20.jpeg",
    #         21: "uploads/S21.jpeg",
    #         22: "uploads/S22.jpeg",
    #         23: "uploads/S23.jpeg",
    #         24: "uploads/S24.jpeg",
    #         25: "uploads/S25.jpeg",
    #         26: "uploads/S26.jpeg"
    #     }

    #     snap_dict = {item.get("index"): item for item in existing_snap}
    #     for key, item in image_dict.items():


    #         if key in snap_dict:
    #             snap_dict[key]["image"] = item

    #     updated_snap = list(snap_dict.values())
    #     updated_snap.sort(key=lambda x: x["index"])


    #     update_payload = {"snap": updated_snap}
    #     response = cmo.updating('complianceForm', updateBy, update_payload, False)
    # print(not_created,"not_created")
    # print(count,"count")
    # return respond(response)

    allData = {
        # "snap": [
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S1: Cabinet is properly mounted & secured on wall/pole and acceciable to technician",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 1,
        #     "image":"uploads/S1.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S2: IP 55 Cabinet Filter( whereever appicable) are in place and properly cleaned",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 2,
        #     "image":"uploads/S2.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S3: Base Station is securely installed & is perfect at Horizontal/vertical position",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 3,
        #     "image":"uploads/S3.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S4: IP seals /plugs must be installed in all module",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 4,
        #     "image":"uploads/S4.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S5: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 5,
        #     "image":"uploads/S5.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S5a: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 6
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S5b: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 7
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S6: BBU and DCDU are grounded to EGB or Tower leg",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 8,
        #     "image":"uploads/S6.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S7: 1st sector RRU is grounded and the grounding cable of RRUs",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 9,
        #     "image":"uploads/S7.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S7a: 1st sector RRU is grounded and the grounding cable of RRUs",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 10
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S7b: 1st sector RRU is grounded and the grounding cable of RRUs",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 11
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S8: RRU power cables are arranged in neat and straight way without any crossing",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 12,
        #     "image":"uploads/S8.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S8a: RRU power cables are arranged in neat and straight way without any crossing",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 13
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S8b: RRU power cables are arranged in neat and straight way without any crossing",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 14
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S9: CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 15,
        #     "image":"uploads/S9.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S9a: CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 16
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S9b: CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 17
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S10: CPRI/eCPRI - All FO Cable extra length kept/fixed safely (Sector wise)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 18
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S11: CPRI/eCPRI Routing at AMOB/ACOC end (Labelling should be clearly Visible)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 19,
        #     "image":"uploads/S11.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S12: CPRI/eCPRI connectivity at ABIx end in AMOB/AMIA",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 20,
        #     "image":"uploads/S12.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S13: Invertor Make and Model (Small Cell)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 21
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S14: The Optical / Ethernet Lan cables are routed and connected securely at BBU",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 22,
        #     "image":"uploads/S14.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S15: CPRI cables of sector RRU are arranged in neat and straight way",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 23,
        #     "image":"uploads/S15.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S15a: CPRI cables of sector RRU are arranged in neat and straight way",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 24
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S15b: CPRI cables of sector RRU are arranged in neat and straight way",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 25
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S16: Cables are routed properly inside and outside the cabinet and bound with ties with uniform spacing & moderate tightness",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 26,
        #     "image":"uploads/S16.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S16a: Cables are routed properly inside and outside the cabinet and bound with ties with uniform spacing & moderate tightness",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 27
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S16b: Cables are routed properly inside and outside the cabinet and bound with ties with uniform spacing & moderate tightness",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 28
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S17: Conduit used for Fiber cable properly",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 29
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S18: GPS antenna and cable installed properly",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 30,
        #     "image":"uploads/S18.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S19: TMA/Combiner/Duplexer/Triplexer",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 31
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S20: Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 32,
        #     "image":"uploads/S20.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S20a: Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 33
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S20b: Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 34
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S21: GGSM Installation Showing Jumper Weather Proofing & Labeling",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 35,
        #     "image":"uploads/S21.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S21a: GGSM Installation Showing Jumper Weather Proofing & Labeling",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 36
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S21b: GGSM Installation Showing Jumper Weather Proofing & Labeling",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 37
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S22: Site access is safe and as per OHS",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 38,
        #     "image":"uploads/S22.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S22a: Site access is safe and as per OHS",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 39
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S22b: Site access is safe and as per OHS",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 40
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S23: EMF Signage Board",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 41,
        #     "image":"uploads/S23.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S23a: EMF Signage Board",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 42
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S23b: EMF Signage Board",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 43
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S24: Site Clean",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 44,
        #     "image":"uploads/S24.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S24a: Site Clean",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 45
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S24b: Site Clean",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 46
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S25: Complete Site View",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 47,
        #     "image":"uploads/S25.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S25a: Complete Site View",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 48
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S25b: Complete Site View",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 49
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S26: Submit Photograph of Alarm patch panel showing alarm extension ( Both cable from BTS and INFRA and interconnection) and labelling on both side in Physical AT",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 50,
        #     "image":"uploads/S26.jpeg"
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S26a: Submit Photograph of Alarm patch panel showing alarm extension ( Both cable from BTS and INFRA and interconnection) and labelling on both side in Physical AT",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 51
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S26b: Submit Photograph of Alarm patch panel showing alarm extension ( Both cable from BTS and INFRA and interconnection) and labelling on both side in Physical AT",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 52
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S27: 1st Sector GSM Installation Make and Model",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 53
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S27a: 1st Sector GSM Installation Make and Model",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 54
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S27b: 1st Sector GSM Installation Make and Model",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 55
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S28: Cable entry FODA end (Closed View Labelling should be clearly Visible)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 56
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S28a: Cable entry FODA end (Closed View Labelling should be clearly Visible)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 57
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S28b: Cable entry FODA end (Closed View Labelling should be clearly Visible)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 58
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S29: Cable connectivity inside FODA (Open View)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 59
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S29a: Cable connectivity inside FODA (Open View)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 60
        #     },
        #     {
        #     "dataType": "Text",
        #     "Status": "Active",
        #     "fieldName": "S29b: Cable connectivity inside FODA (Open View)",
        #     "required": "Yes",
        #     "childView": False,
        #     "index": 61
        #     }

        # ]

        "snap": [
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S1: Cabinet is properly mounted & secured on wall/pole and acceciable to technician",
            "childView": False,
            "index": 1,
            "image":"uploads/S1.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S2: IP 55 Cabinet Filter( whereever appicable) are in place and properly cleaned",
            "childView": False,
            "index": 2,
            "image":"uploads/S2.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S3: Base Station is securely installed & is perfect at Horizontal/vertical position",
            "childView": False,
            "index": 3,
            "image":"uploads/S3.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S4: IP seals /plugs must be installed in all module",
            "childView": False,
            "index": 4,
            "image":"uploads/S4.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S5 A: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
            "childView": False,
            "index": 5,
            "image":"uploads/S5.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S5 B: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
            "childView": False,
            "index": 6,
            "image":"uploads/S5.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S5 C: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
            "childView": False,
            "index": 7,
            "image":"uploads/S5.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S5 D: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
            "childView": False,
            "index": 8,
            "image":"uploads/S5.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S6: BBU and DCDU are grounded to EGB or Tower leg",
            "childView": False,
            "index": 9,
            "image":"uploads/S6.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S7 A : 1st sector RRU is grounded and the grounding cable of RRUs",
            "childView": False,
            "index": 10,
            "image":"uploads/S7.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S7 B : 1st sector RRU is grounded and the grounding cable of RRUs",
            "childView": False,
            "index": 11,
            "image":"uploads/S7.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S7 C : 1st sector RRU is grounded and the grounding cable of RRUs",
            "childView": False,
            "index": 12,
            "image":"uploads/S7.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S7 D : 1st sector RRU is grounded and the grounding cable of RRUs",
            "childView": False,
            "index": 13,
            "image":"uploads/S7.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S8 A : RRU power cables are arranged in neat and straight way without any crossing",
            "childView": False,
            "index": 14,
            "image":"uploads/S8.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S8 B : RRU power cables are arranged in neat and straight way without any crossing",
            "childView": False,
            "index": 15,
            "image":"uploads/S8.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S8 C : RRU power cables are arranged in neat and straight way without any crossing",
            "childView": False,
            "index": 16,
            "image":"uploads/S8.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S8 D : RRU power cables are arranged in neat and straight way without any crossing",
            "childView": False,
            "index": 17,
            "image":"uploads/S8.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S9 A :CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
            "childView": False,
            "index": 18,
            "image":"uploads/S9.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S9 B :CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
            "childView": False,
            "index": 19,
            "image":"uploads/S9.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S9 C :CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
            "childView": False,
            "index": 20,
            "image":"uploads/S9.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S9 D :CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
            "childView": False,
            "index": 21,
            "image":"uploads/S9.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S10 A :CPRI/eCPRI - All FO Cable extra length kept/fixed safely (Sector wise)",
            "childView": False,
            "index": 22
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S10 B :CPRI/eCPRI - All FO Cable extra length kept/fixed safely (Sector wise)",
            "childView": False,
            "index": 23
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S10 C :CPRI/eCPRI - All FO Cable extra length kept/fixed safely (Sector wise)",
            "childView": False,
            "index": 24
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S10 D :CPRI/eCPRI - All FO Cable extra length kept/fixed safely (Sector wise)",
            "childView": False,
            "index": 25
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S11: CPRI/eCPRI Routing at AMOB/ACOC end (Labelling should be clearly Visible)",
            "childView": False,
            "index": 26,
            "image":"uploads/S11.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S12: CPRI/eCPRI connectivity at ABIx end in AMOB/AMIA",
            "childView": False,
            "index": 27,
            "image":"uploads/S12.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S13: Invertor Make and Model (Small Cell)",
            "childView": False,
            "index": 28
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S14: The Optical / Ethernet Lan cables are routed and connected securely at BBU",
            "childView": False,
            "index": 29,
            "image":"uploads/S14.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S15 A :CPRI cables of sector RRU are arranged in neat and straight way",
            "childView": False,
            "index": 30,
            "image":"uploads/S15.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S15 B :CPRI cables of sector RRU are arranged in neat and straight way",
            "childView": False,
            "index": 31,
            "image":"uploads/S15.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S15 C :CPRI cables of sector RRU are arranged in neat and straight way",
            "childView": False,
            "index": 32,
            "image":"uploads/S15.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S15 D :CPRI cables of sector RRU are arranged in neat and straight way",
            "childView": False,
            "index": 33,
            "image":"uploads/S15.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S16: Cables are routed properly inside and outside the cabinet and bound with ties with uniform spacing & moderate tightness",
            "childView": False,
            "index": 34,
            "image":"uploads/S16.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S17: Conduit used for Fiber cable properly",
            "childView": False,
            "index": 35
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S18: GPS antenna and cable installed properly",
            "childView": False,
            "index": 36,
            "image":"uploads/S18.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S19: TMA/Combiner/Duplexer/Triplexer",
            "childView": False,
            "index": 37
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S20 A :Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
            "childView": False,
            "index": 38,
            "image":"uploads/S20.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S20 B :Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
            "childView": False,
            "index": 39,
            "image":"uploads/S20.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S20 C :Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
            "childView": False,
            "index": 40,
            "image":"uploads/S20.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S20 D :Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
            "childView": False,
            "index": 41,
            "image":"uploads/S20.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S21 A :GGSM Installation Showing Jumper Weather Proofing & Labeling",
            "childView": False,
            "index": 42,
            "image":"uploads/S21.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S21 B :GGSM Installation Showing Jumper Weather Proofing & Labeling",
            "childView": False,
            "index": 43,
            "image":"uploads/S21.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S21 C :GGSM Installation Showing Jumper Weather Proofing & Labeling",
            "childView": False,
            "index": 44,
            "image":"uploads/S21.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S21 D :GGSM Installation Showing Jumper Weather Proofing & Labeling",
            "childView": False,
            "index": 45,
            "image":"uploads/S21.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S22: Site access is safe and as per OHS",
            "childView": False,
            "index": 46,
            "image":"uploads/S22.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S23: EMF Signage Board",
            "childView": False,
            "index": 47,
            "image":"uploads/S23.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S24: Site Clean",
            "childView": False,
            "index": 48,
            "image":"uploads/S24.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S25: Complete Site View",
            "childView": False,
            "index": 49,
            "image":"uploads/S25.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S26: Submit Photograph of Alarm patch panel showing alarm extension ( Both cable from BTS and INFRA and interconnection) and labelling on both side in Physical AT",
            "childView": False,
            "index": 50,
            "image":"uploads/S26.jpeg"
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S27: 1st Sector GSM Installation Make and Model",
            "childView": False,
            "index": 51
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S28: Cable entry FODA end (Closed View Labelling should be clearly Visible)",
            "childView": False,
            "index": 52
            },
            {
            "dataType": "Text",
            "required": "Yes",
            "Status": "Active",
            "fieldName": "S29: Cable connectivity inside FODA (Open View)",
            "childView": False,
            "index": 53
            }
        ]
    }
    for i in id_list:
        cmo.updating("complianceForm",{'_id':ObjectId(i)},allData,False)
    return respond({
        'status':201,
        "msg":"Data created Successfully",
        "icon":"success"
    })









@admin_blueprint.route("/sample/checklist", methods=["GET"])
def add_sample_checklist():
    myList = [
        "687f6a781565a8c19ec1422a",
        "687f6cc01565a8c19ec14445",
        "687f767c1565a8c19ec14b59",
        "687f78311565a8c19ec14c64",
        "687f78681565a8c19ec14c79",
        "687f787b1565a8c19ec14c8f",
        "687f789b1565a8c19ec14cb4",
        "687f78af1565a8c19ec14cc5",
        "687f78f51565a8c19ec14cea",
        "687f793a1565a8c19ec14d0f",
        "687f79511565a8c19ec14d17",
        "687f79851565a8c19ec14d40",
        "687f79991565a8c19ec14d46",
        "687f79b51565a8c19ec14d4e",
        "687f79d81565a8c19ec14d54",
        "687f7a0c1565a8c19ec14d57",
        "687f7a221565a8c19ec14d58",
        "687f7a7f1565a8c19ec14d6f",
        "687f7a911565a8c19ec14d70",
        "687f7aa81565a8c19ec14d75",
        "687f7acf1565a8c19ec14d78",
        "687f7adc1565a8c19ec14d79",
        "687f7cca1565a8c19ec14ec5",
    ]
    for i in myList:
        allData = {

            "snap": [
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S1: Cabinet is properly mounted & secured on wall/pole and acceciable to technician",
                "Status": "Active",
                "childView": False,
                "index": 1,
                "image": "uploads/S1.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S2: IP 55 Cabinet Filter( whereever appicable) are in place and properly cleaned",
                "Status": "Active",
                "childView": False,
                "index": 2,
                "image": "uploads/S2.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S3: Base Station is securely installed & is perfect at Horizontal/vertical position",
                "Status": "Active",
                "childView": False,
                "index": 3,
                "image": "uploads/S3.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S4: IP seals /plugs must be installed in all module",
                "Status": "Active",
                "childView": False,
                "index": 4,
                "image": "uploads/S4.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S5: RRU Installation Showing GND Jumper Clamping Weather Proofing & Labeling",
                "Status": "Active",
                "childView": False,
                "index": 5,
                "image": "uploads/S5.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S6: BBU and DCDU are grounded to EGB or Tower leg",
                "Status": "Active",
                "childView": False,
                "index": 6,
                "image": "uploads/S6.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S7: 1st sector RRU is grounded and the grounding cable of RRUs",
                "Status": "Active",
                "childView": False,
                "index": 7,
                "image": "uploads/S7.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S8: RRU power cables are arranged in neat and straight way without any crossing",
                "Status": "Active",
                "childView": False,
                "index": 8,
                "image": "uploads/S8.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S9: CPRI - FO Cable looped in ring at least 2-3 times at RRU end",
                "Status": "Active",
                "childView": False,
                "index": 9,
                "image": "uploads/S9.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S10: CPRI/eCPRI - All FO Cable extra length kept/fixed safely (Sector wise)",
                "Status": "Active",
                "childView": False,
                "index": 10
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S11: CPRI/eCPRI Routing at AMOB/ACOC end (Labelling should be clearly Visible)",
                "Status": "Active",
                "childView": False,
                "index": 11,
                "image": "uploads/S11.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S12: CPRI/eCPRI connectivity at ABIx end in AMOB/AMIA",
                "Status": "Active",
                "childView": False,
                "index": 12,
                "image": "uploads/S12.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S13: Invertor Make and Model (Small Cell)",
                "Status": "Active",
                "childView": False,
                "index": 13
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S14: The Optical / Ethernet Lan cables are routed and connected securely at BBU",
                "Status": "Active",
                "childView": False,
                "index": 14,
                "image": "uploads/S14.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S15: CPRI cables of sector RRU are arranged in neat and straight way",
                "Status": "Active",
                "childView": False,
                "index": 15,
                "image": "uploads/S15.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S16: Cables are routed properly inside and outside the cabinet and bound with ties with uniform spacing & moderate tightness",
                "Status": "Active",
                "childView": False,
                "index": 16,
                "image": "uploads/S16.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S17: Conduit used for Fiber cable properly",
                "Status": "Active",
                "childView": False,
                "index": 17
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S18: GPS antenna and cable installed properly",
                "Status": "Active",
                "childView": False,
                "index": 18,
                "image": "uploads/S18.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S19: TMA/Combiner/Duplexer/Triplexer",
                "Status": "Active",
                "childView": False,
                "index": 19
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S20: Obstructions if any ( Optimization purpose) Antenna Clamp should not be blocked to allow Antenna for proper optimization (+/-30 Degree)",
                "Status": "Active",
                "childView": False,
                "index": 20,
                "image": "uploads/S20.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S21: GGSM Installation Showing Jumper Weather Proofing & Labeling",
                "Status": "Active",
                "childView": False,
                "index": 21,
                "image": "uploads/S21.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S22: Site access is safe and as per OHS",
                "Status": "Active",
                "childView": False,
                "index": 22,
                "image": "uploads/S22.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S23: EMF Signage Board",
                "Status": "Active",
                "childView": False,
                "index": 23,
                "image": "uploads/S23.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S24: Site Clean",
                "Status": "Active",
                "childView": False,
                "index": 24,
                "image": "uploads/S24.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S25: Complete Site View",
                "Status": "Active",
                "childView": False,
                "index": 25,
                "image": "uploads/S25.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S26: Submit Photograph of Alarm patch panel showing alarm extension ( Both cable from BTS and INFRA and interconnection) and labelling on both side in Physical AT",
                "Status": "Active",
                "childView": False,
                "index": 26,
                "image": "uploads/S26.jpeg"
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S27: 1st Sector GSM Installation Make and Model",
                "Status": "Active",
                "childView": False,
                "index": 27
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S28: Cable entry FODA end (Closed View Labelling should be clearly Visible)",
                "Status": "Active",
                "childView": False,
                "index": 28
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "S29: Cable connectivity inside FODA (Open View)",
                "Status": "Active",
                "childView": False,
                "index": 29
                }
            ],
            "ranChecklist": [
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Cabinet is grouted/mounted firmly on ground/wall/pole and accessiable to technician",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 1
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "IP 55 Cabinet Filter( wherever applicable) are in place  and properly cleaned",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 2
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "IP 55 cabinet fans are working properly",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 3
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "IP 55 cabinet alarms are extended",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 4
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "No of RF Module- Technology wise",
                "Status": "Active",
                "childView": False,
                "index": 5
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "No of Baseband Units- Technology wise",
                "Status": "Active",
                "childView": False,
                "index": 6
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "BBU Card upgraded",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 7
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Base Station is securely installed",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 8
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Base Station is installed at vertical position (not tilted)",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 9
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "IP seals/plugs must be installed in all module",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 10
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RRUs are securely installed",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 11
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RRU Position",
                "dropdownValue": "BEHIND ANTENNA,TOWER LEG,BELOW ANTENNA,BELOW CABINET,MAIN POLE",
                "Status": "Active",
                "childView": True,
                "index": 12
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RRU Jumper Cable Length",
                "Status": "Active",
                "childView": False,
                "index": 13
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RRU Jumper Cable routing without any twist",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 14
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Dummy Plates Installed In All Empty Slots",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 15
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RF Units Securely Installed",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 16
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RFUnits Jumper Cable Routing Without Any Twist",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 17
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Units Jumper Cable Length",
                "Status": "Active",
                "childView": False,
                "index": 18
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Patching Cable For X2 Handover Routing",
                "Status": "Active",
                "childView": False,
                "index": 19
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Tower/DG and Shelter grounding is connected to earth pit",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 20
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Potential between Earth to Neutral is zero",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 21
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "MCB for BBU and RRU meets the design specifications",
                "Status": "Active",
                "childView": False,
                "index": 22
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "MCB for BBU and RRU meets the design specifications2",
                "Status": "Active",
                "childView": False,
                "index": 23
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "MCB for BBU and RRU meets the design specifications3",
                "Status": "Active",
                "childView": False,
                "index": 24
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "MCB for BBU and RRU meets the design specifications4",
                "Status": "Active",
                "childView": False,
                "index": 25
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "BBU and DCDU are grounded to EGB",
                "dropdownValue": "IGB,OGB,EARTHING STRIP,EGB,TOWER,EXISTING,NA",
                "Status": "Active",
                "childView": True,
                "index": 26
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "All Power cable should be as per specification",
                "Status": "Active",
                "childView": False,
                "index": 27
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "All Power cable should be as per specification2",
                "Status": "Active",
                "childView": False,
                "index": 28
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "All Power cable should be as per specification3",
                "Status": "Active",
                "childView": False,
                "index": 29
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "All Power cable should be as per specification4",
                "Status": "Active",
                "childView": False,
                "index": 30
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "All the power cables and protection ground cables are connected firmly with correct polarity",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 31
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RRU is grounded & grounding cables fixed to the earth strip/tower leg in downword direction",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 32
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RRU power cables are arranged in neat and straight way without any crossing",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 33
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "IGB is properly grounded to earth pit",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 34
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "OGB is properly installed & grounded to earth pit",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 35
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "The E1 / Ethernet Lan cables are routed and connected securely at BBU",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 36
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "QOS Implementation as per guideline",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 37
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "CPRI cables of RRU are arranged in neat and straight way without any crossing",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 38
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "The fixing clips of the CPRI cables of RRU are spaced evenly and secured and oriented in the same direction",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 39
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "CPRI - FO Cable looped in ring at least 2-3 times but not more than 4 rings at RRU end",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 40
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Any visible stretch/Cut/damage on FO Cable",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 41
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Cables are routed properly inside and outside the cabinet and bound with ties with uniform spacing & moderate tightness",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 42
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Correct CPRI Cable / SFP Port is there as per FDD-1000/TDD-10G Recommendation",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 43
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Fiber cable routing properly",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 44
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Conduit used for Fiber cable properly",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 45
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Feeder should be traced for Swap / Cyclic Swap / Proper Mapping",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 46
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "GPS antenna and cable installed properly",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 47
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "BTS Clock Configuration-Primary",
                "dropdownValue": "GPS,IP,TOP,TDM,NTP,NA",
                "Status": "Active",
                "childView": True,
                "index": 48
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "BTS Clock Configuration-Secondary",
                "dropdownValue": "GPS,IP,TOP,TDM,NTP,NA",
                "Status": "Active",
                "childView": True,
                "index": 49
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Active alarm on Site",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 50
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Any fluctuating alarm in last 24 hours",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 51
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Last 24 Hr Alarm",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 52
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "For All BTS Cabinet",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 53
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "For all Cables & Accessories",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 54
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "For Antenna",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 55
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Labeling of DDF Done as per specifications",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 56
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "For TMA Duplexer Combiner Triplexer",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 57
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RF Jumper Cables Labelled Correctly",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 58
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "DG Fails to Start",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 59
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Mains Fail",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 60
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "High Temperature",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 61
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Site on DG",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 62
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Site on Battery",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 63
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Low Battery Voltage",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 64
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "DG low fuel",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 65
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Fire & Smoke",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 66
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Rectifer Fail",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 67
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "TI Partner Company Name",
                "dropdownValue": "MOBILECOMM",
                "Status": "Active",
                "childView": True,
                "index": 68
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "TI Partner Representative Name",
                "Status": "Active",
                "childView": False,
                "index": 69
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Site is cleaned properly & housekeeping is done",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 70
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Any project left over material lying at site",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 71
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Site access is safe and as per OHS",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 72
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "EMF Signage Board",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 73
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Pole mount is in acceptable criteria (<06m from tower leg) and not violating OHS guidelines",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 74
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "CATS Lables",
                "dropdownValue": "YES,NO,NA,EXISTING",
                "Status": "Active",
                "childView": True,
                "index": 75
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Antenna Make",
                "Status": "Active",
                "childView": False,
                "index": 76
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Antenna Type",
                "Status": "Active",
                "childView": False,
                "index": 77
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Antenna Ports",
                "Status": "Active",
                "childView": False,
                "index": 78
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Antenna Quantity",
                "Status": "Active",
                "childView": False,
                "index": 79
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "RET Cable Connected with RRU",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 80
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "TMA/Combiner/Duplexer/Triplexer",
                "dropdownValue": "TMA,COMBINER,DUPLEXER,TRIPLEXER,NA",
                "Status": "Active",
                "childView": True,
                "index": 81
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "LTE/NR Antenna should be Coplaner",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 82
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Antenna should be free of any obstruction to allow movement for Optimization",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 83
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Antenna Clamp should not be damaged/Up Tilted",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 84
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Antenna should not be damaged",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 85
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "FDDTDD Antenna Should be in Same Direction",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 86
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "LTENR Antenna Installed As Per Planned Azimuth",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 87
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Mechanical Rollover",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 88
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "RF Antenna Calibration Port Connected With RRH",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 89
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "RET Cable Installed And Connected As Per plan",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 90
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Antenna Mounting Clamps Checked All Screws Tightened",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 91
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 1",
                "Status": "Active",
                "childView": False,
                "index": 92
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 2",
                "Status": "Active",
                "childView": False,
                "index": 93
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 3",
                "Status": "Active",
                "childView": False,
                "index": 94
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 4",
                "Status": "Active",
                "childView": False,
                "index": 95
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 5",
                "Status": "Active",
                "childView": False,
                "index": 96
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 6",
                "Status": "Active",
                "childView": False,
                "index": 97
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 7",
                "Status": "Active",
                "childView": False,
                "index": 98
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 8",
                "Status": "Active",
                "childView": False,
                "index": 99
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 9",
                "Status": "Active",
                "childView": False,
                "index": 100
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 10",
                "Status": "Active",
                "childView": False,
                "index": 101
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 11",
                "Status": "Active",
                "childView": False,
                "index": 102
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 12",
                "Status": "Active",
                "childView": False,
                "index": 103
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 13",
                "Status": "Active",
                "childView": False,
                "index": 104
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 14",
                "Status": "Active",
                "childView": False,
                "index": 105
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 15",
                "Status": "Active",
                "childView": False,
                "index": 106
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 16",
                "Status": "Active",
                "childView": False,
                "index": 107
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 17",
                "Status": "Active",
                "childView": False,
                "index": 108
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "RF Port 18",
                "Status": "Active",
                "childView": False,
                "index": 109
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Path Imbalance For FDDTDD Ports",
                "Status": "Active",
                "childView": False,
                "index": 110
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Weather Proofing on All Joints",
                "dropdownValue": "YES,NO,NA",
                "Status": "Active",
                "childView": True,
                "index": 111
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "RRU1",
                "Status": "Active",
                "childView": False,
                "index": 112
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "RRU2",
                "Status": "Active",
                "childView": False,
                "index": 113
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "RRU3",
                "Status": "Active",
                "childView": False,
                "index": 114
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "RRU4",
                "Status": "Active",
                "childView": False,
                "index": 115
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "RRU5",
                "Status": "Active",
                "childView": False,
                "index": 116
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Grounding Cable Dimension Sqmm",
                "Status": "Active",
                "childView": False,
                "index": 117
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Power Cable Dimension Sqmm",
                "Status": "Active",
                "childView": False,
                "index": 118
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Power Cable Length Mtrs",
                "Status": "Active",
                "childView": False,
                "index": 119
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "CPRI eCPRI Mtrs",
                "Status": "Active",
                "childView": False,
                "index": 120
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Media Cable Mtrs",
                "Status": "Active",
                "childView": False,
                "index": 121
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Others Mtrs",
                "Status": "Active",
                "childView": False,
                "index": 122
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RF Units Position",
                "dropdownValue": "BEHIND ANTENNA,BELOW CABINET,ABOVE CABINET,NA",
                "Status": "Active",
                "childView": True,
                "index": 123
                }
            ],
            "siteDetails": [
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "SR No",
                "Status": "Active",
                "childView": False,
                "index": 1
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Circle",
                "Status": "Active",
                "childView": False,
                "index": 2
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "TSP Ref Number",
                "Status": "Active",
                "childView": False,
                "index": 3
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "AT Ref Number",
                "Status": "Active",
                "childView": False,
                "index": 4
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Site ID",
                "Status": "Active",
                "childView": False,
                "index": 5
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "BTS Manufacturer",
                "dropdownValue": "NOKIA,ZTE,SAMSUNG,HUAWEI,ERICSSON",
                "Status": "Active",
                "childView": True,
                "index": 6
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Name of OEM/TSP",
                "dropdownValue": "MOBILECOMM",
                "Status": "Active",
                "childView": True,
                "index": 7
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Site Type",
                "dropdownValue": "IM,TTI,TTO,TBO,OM,TBI,MI,IB,OD,COW,FSC,ISC,TO,IS,HS,UL,AD,CO,AS,AR,FM,OR,SC",
                "Status": "Active",
                "childView": True,
                "index": 8
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Tech ID",
                "Status": "Active",
                "childView": False,
                "index": 9
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Technology",
                "dropdownValue": "2G,FDD,TDD,5G",
                "Status": "Active",
                "childView": True,
                "index": 10
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Band",
                "dropdownValue": "G900,G1800,L850,L900,L1800,L2100,L2300,N3500",
                "Status": "Active",
                "childView": True,
                "index": 11
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Site type",
                "dropdownValue": "New Built,Sharing",
                "Status": "Active",
                "childView": True,
                "index": 12
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Project Type",
                "dropdownValue": "New_TOWER,NEW_TOWER_RELOCATION,UPGRADE,SECTOR ADDITION,NEW_TOWER_ULS,UPGRADE_ULS,TWIN_BEAM",
                "Status": "Active",
                "childView": True,
                "index": 13
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Physical Site ID",
                "Status": "Active",
                "childView": False,
                "index": 14
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "Tower Type As Dpr",
                "dropdownValue": "Angular,Tabular,Pole,Monopole,IBS,NA",
                "Status": "Active",
                "childView": True,
                "index": 15
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Tower Height",
                "Status": "Active",
                "childView": False,
                "index": 16
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Building Height",
                "Status": "Active",
                "childView": False,
                "index": 17
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Activity",
                "dropdownValue": "NEW,TOWER,TDD,UPGRADE,FDD,SEC,ADDITION,TDD,SEC,ADDITION,L2100,UPGRADE,NEW,TOWER,ULS,TDD,TWIN,BEAM,UPGRADE,ULS,FDD,TWIN,BEAM,FDD,UPGRADE,L900,UPGRADE,2G,UPGRADE,UPGRADE,SW,ONLY,M,MIMO,2G,SEC,ADDITION,UPGRADE,SECTOR,ADDITION,NEW,TOWER,RELOCATION,5G,COMBO,RELOCATION,5G,NEW,5G,SEC,ADDITION",
                "Status": "Active",
                "childView": True,
                "index": 18
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Layer Planned",
                "dropdownValue": "G900,G1800,L850,L900,L1800,L2100,L2300,N3500",
                "Status": "Active",
                "childView": True,
                "index": 19
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Number of Logical cells",
                "Status": "Active",
                "childView": False,
                "index": 20
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "BBU Type",
                "dropdownValue": "Indoor,Outdoor",
                "Status": "Active",
                "childView": True,
                "index": 21
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "RRU Type",
                "Status": "Active",
                "childView": False,
                "index": 22
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "MMIMO Power Configuration",
                "Status": "Active",
                "childView": False,
                "index": 23
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "SM",
                "Status": "Active",
                "childView": False,
                "index": 24
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Type of Media",
                "dropdownValue": "MW,FTTH,FIBER,UBR",
                "Status": "Active",
                "childView": True,
                "index": 25
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "MW Link Id",
                "Status": "Active",
                "childView": False,
                "index": 26
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Media RA Number",
                "Status": "Active",
                "childView": False,
                "index": 27
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "MW Link AT Status",
                "dropdownValue": "Operational,Project,NA",
                "Status": "Active",
                "childView": True,
                "index": 28
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "EB Availablity",
                "dropdownValue": "YES,NO",
                "Status": "Active",
                "childView": True,
                "index": 29
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "DG Availablity",
                "dropdownValue": "YES,NO",
                "Status": "Active",
                "childView": True,
                "index": 30
                },
                {
                "required": "Yes",
                "dataType": "Dropdown",
                "fieldName": "Tower Type",
                "dropdownValue": "GBT,RTP,GBM,RTT",
                "Status": "Active",
                "childView": True,
                "index": 31
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Tower Height",
                "Status": "Active",
                "childView": False,
                "index": 32
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Building Height",
                "Status": "Active",
                "childView": False,
                "index": 33
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Invertor Make and Model",
                "Status": "Active",
                "childView": False,
                "index": 34
                },
                {
                "required": "No",
                "dataType": "Dropdown",
                "fieldName": "RFI Check Status",
                "dropdownValue": "Accepted,Accepted with Punch Point,Yes,No,NA,Rejected",
                "Status": "Active",
                "childView": True,
                "index": 35
                },
                {
                "required": "No",
                "dataType": "Text",
                "fieldName": "Remarks",
                "Status": "Active",
                "childView": False,
                "index": 36
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Name",
                "Status": "Active",
                "childView": False,
                "index": 37
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Email ID",
                "Status": "Active",
                "childView": False,
                "index": 38
                },
                {
                "required": "Yes",
                "dataType": "Text",
                "fieldName": "Contact number",
                "Status": "Active",
                "childView": False,
                "index": 39
                }
            ]
        
        }
        cmo.updating("complianceForm",{'_id':ObjectId(i)},allData,False)




#########====================INTERNAL========================###########
#########====================PROJECT========================############


@admin_blueprint.route('/admin/manageMarket', methods=['GET', "POST"])
@admin_blueprint.route('/admin/manageMarket/<id>', methods=["POST","DELETE"])
@token_required
def manageMarket(current_user, id=None):
    if request.method == "GET":
        arra = []
        if request.args.get("customerId")!=None and request.args.get("customerId")!="undefined":
            arra = arra + [
                {
                    '$match':{
                        'customer':ObjectId(request.args.get("customerId"))
                    }
                }
            ]
        arra = arra + [
            {
                '$lookup': {
                    'from': 'customer',
                    'localField': 'customer',
                    'foreignField': '_id',
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
                    'as': 'result'
                }
            }, 
            # {
            #     '$lookup': {
            #         'from': 'project',
            #         'localField': 'project',
            #         'foreignField': '_id',
            #         'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
            #         'as': 'projectResult'
            #     }
            # }, 
            {
                '$unwind': {
                    'path': '$result',
                    'preserveNullAndEmptyArrays': True
                }
            }, 
            # {
            #     '$unwind': {
            #         'path': '$projectResult',
            #         'preserveNullAndEmptyArrays': True
            #     }
            # }, 
            {
                '$addFields': {
                    'customerName': '$result.customerName',
                    # 'projectName':"$projectResult.projectId",
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    'customer': {
                        '$toString': '$customer'
                    },
                    # 'project': {
                    #     '$toString': '$project'
                    # },
                }
            }, {
                '$project': {
                    '_id': 0,
                    'result': 0,
                    # 'projectResult':0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("market", arra)
        return respond(response)

    elif request.method == "POST":

        allData = request.get_json()
        required_fields = ["customer", "marketName", "marketCode"]
        if not all(field in allData for field in required_fields):
            return respond({
                "status": 400,
                "icon": "error",
                "msg": "Please provide all required fields"
            })
        
        if id == None:
            arra = [
                {
                    '$match': {
                        'customer': ObjectId(allData['customer']),
                        # 'project':ObjectId(allData['project']),
                        'marketCode': allData['marketCode']

                    }
                }
            ]
            response = cmo.finding_aggregate("market", arra)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "This Market already exists for the selected customer",
                    "icon": "error",
                }, 400
            insertData = {
                'customer':ObjectId(allData['customer']),
                # 'project':ObjectId(allData['project']),
                'marketName':allData['marketName'],
                'marketCode':allData['marketCode']
            }
            response = cmo.insertion("market", insertData)
            return respond(response)

        elif id != None:
            updateData = {
                "marketName": allData['marketName']
            }
            lookData = {
                '_id': ObjectId(id)
            }
            response = cmo.updating("market", lookData, updateData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("market", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({'msg': 'Please provide valid Unique Id'})


@admin_blueprint.route('/customers/projects/<customer_id>', methods=['GET'])
@token_required
def customer_project(current_user,customer_id=None):
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'custId': customer_id
                }
            }, {
                '$project': {
                    'projectIdName': '$projectId', 
                    'projectIdUid': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("project",arra)
        return respond(response)
    
@admin_blueprint.route('/admin/manageResource', methods=['GET', "POST"])
@admin_blueprint.route('/admin/manageResource/<id>', methods=["POST","DELETE"])
@token_required
def manageResource(current_user, id=None):

    if request.method == "GET":
        arra = [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("resource", arra)
        return respond(response)

    elif request.method == "POST":

        allData = request.get_json()
        required_fields = ["fieldName", "fieldType","mandatory","fieldKey"]

        if id == None:
            if not all(field in allData for field in required_fields):
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Please provide all required fields"
                })
            insertData = {
                "fieldName":allData['fieldName'],
                "fieldType":allData['fieldType'],
                "mandatory":allData['mandatory'],
                "fieldKey":allData['fieldKey']
            }
            if "dropdown" in allData:
                insertData['dropdown'] = allData['dropdown']
            response = cmo.insertion("resource", insertData)
            return respond(response)

        elif id != None:
            if not all(field in allData for field in required_fields):
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Please provide all required fields"
                })
            updateData = {
                "fieldName":allData['fieldName'],
                "fieldType":allData['fieldType'],
                "mandatory":allData['mandatory'],
                "fieldKey":allData['fieldKey']
            }
            if "dropdown" in allData:
                updateData['dropdown'] = allData['dropdown']
            updateBy = {
                '_id':ObjectId(id)
            }
            response = cmo.updating("resource", updateBy, updateData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("resource", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({"msg": 'Please Provide Valid Unique ID'})
        
@admin_blueprint.route('/vishal', methods=['GET'])
@token_required
def vishal(current_user):
    if request.method == "GET":
        arra = [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    'projectId': 1,
                    'uniqueId': 1,
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("project", arra)
        return respond(response)
    

@admin_blueprint.route("/admin/getProjectSubType/<uid>", methods=['GET'])
@token_required
def getProjectSubType(current_user, uid):
    if request.method == "GET":
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
                    'pipeline': [{'$match': {'deleteStatus': {'$ne': 1}}}],
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
                                'deleteStatus': {'$ne': 1}
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$$custId', '$custId'
                                    ]
                                }
                            }
                        },
                        {
                            '$addFields': {
                                'value': {
                                    '$toString': '$_id'
                                },
                                '_id': {
                                    '$toString': '$_id'
                                },
                                'label': {
                                    '$toString': '$subProject'
                                },
                            }
                        }
                    ],
                    'as': 'subprojectresult'
                }
            }, {
                '$project': {
                    "result": 0
                }
            }
        ]
        response = cmo.finding_aggregate("project", arra)
        return respond(response)
    
@admin_blueprint.route('/admin/getProjectTypeDyform/<id>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@admin_blueprint.route('/admin/getProjectTypeDyform/<id>/<rowId>', methods=['GET', "POST", "PUT", "PATCH", "DELETE"])
@token_required
def getProjectTypeDyform(current_user, id=None, rowId=None):

    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'custId': id
                }
            }
        ]

        if (rowId != None):

            arra = arra+[
                {
                    '$match': {
                        '_id': ObjectId(rowId)
                    }
                }
            ]
        arra = arra+[
            {
                '$addFields': {
                    't_sengg': {
                        '$filter': {
                            'input': '$t_sengg',
                            'as': 'item',
                            'cond': {
                                '$eq': [
                                    '$$item.Status', 'Active'
                                ]
                            }
                        }
                    },
                    't_tracking': {
                        '$filter': {
                            'input': '$t_tracking',
                            'as': 'item',
                            'cond': {
                                '$eq': [
                                    '$$item.Status', 'Active'
                                ]
                            }
                        }
                    },
                    't_issues': {
                        '$filter': {
                            'input': '$t_issues',
                            'as': 'item',
                            'cond': {
                                '$eq': [
                                    '$$item.Status', 'Active'
                                ]
                            }
                        }
                    }, 't_sFinancials': {
                        '$filter': {
                            'input': '$t_sFinancials',
                            'as': 'item',
                            'cond': {
                                '$eq': [
                                    '$$item.Status', 'Active'
                                ]
                            }
                        }
                    },
                    'MileStone': {
                        '$filter': {
                            'input': '$MileStone',
                            'as': 'item',
                            'cond': {
                                '$eq': [
                                    '$$item.Status', 'Active'
                                ]
                            }
                        }
                    }
                }
            },
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    '_id': {
                        '$toString': '$_id'
                    },
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
                    'as': 'custResult'
                }
            }, {
                '$addFields': {
                    'clientName': {
                        '$arrayElemAt': [
                            '$custResult.customerName', 0
                        ]
                    }, 
                    'custId': {
                        '$toString': '$custId'
                    }
                }
            },{
                '$project':{
                    'custResult':0
                }
            }
    
        ]
        response = cmo.finding_aggregate("projectType", arra)
        return respond(response)
    

@admin_blueprint.route('/admin/manageMappedMilestone', methods=['GET', "POST"])
@admin_blueprint.route('/admin/manageMappedMilestone/<id>', methods=["POST","DELETE"])
@token_required
def manageMappedMilestone(current_user, id=None):

    if request.method == "GET":
        arra = [
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
                    'from': 'projectType', 
                    'localField': 'subProject', 
                    'foreignField': '_id', 
                    'as': 'subTypeResult'
                }
            }, {
                '$project': {
                    'customerName': {
                        '$arrayElemAt': [
                            '$customerResult.customerName', 0
                        ]
                    }, 
                    'projectTypeName': {
                        '$arrayElemAt': [
                            '$subTypeResult.projectType', 0
                        ]
                    }, 
                    'subTypeName': {
                        '$arrayElemAt': [
                            '$subTypeResult.subProject', 0
                        ]
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }, 
                    'projectType': {
                        '$toString': '$subProject'
                    }, 
                    'subProject': {
                        '$toString': '$subProject'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'milestoneName': 1, 
                    'trackingField': 1, 
                    '_id': 0
                }
            }
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("mappedMilestone", arra)
        return respond(response)

    elif request.method == "POST":

        allData = request.get_json()
        required_fields = ["customer","projectType","subProject","milestoneName", "trackingField"]

        if id == None:
            if not all(field in allData for field in required_fields):
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Please provide all required fields"
                })
            arra = [
                {
                    '$match':{
                        'customer':ObjectId(allData['customer']),
                        "subProject":ObjectId(allData['subProject']),
                        "milestoneName":allData['milestoneName']
                    }
                }
            ]
            response = cmo.finding_aggregate("mappedMilestone",arra)['data']
            if response:
                return respond({
                    'status':400,
                    "icon":'error',
                    "msg":"Pair Of Customer,Scope,Sub Scope and Milestone is Already Exist in Database"
                })
            insertData = {
                "customer":ObjectId(allData['customer']),
                "subProject":ObjectId(allData['subProject']),
                "milestoneName":allData['milestoneName'],
                "trackingField":allData['trackingField']
            }
            response = cmo.insertion("mappedMilestone", insertData)
            return respond(response)

        elif id != None:
            if not all(field in allData for field in required_fields):
                return respond({
                    "status": 400,
                    "icon": "error",
                    "msg": "Please provide all required fields"
                })
            updateData = {
                "milestoneName":allData['milestoneName'],
                "trackingField":allData['trackingField']
            }
            updateBy = {
                '_id':ObjectId(id)
            }
            response = cmo.updating("mappedMilestone", updateBy, updateData, False)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("mappedMilestone", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({"msg": 'Please Provide Valid Unique ID'})