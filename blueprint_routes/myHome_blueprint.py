from base import *
from blueprint_routes.currentuser_blueprint import projectId_str
myHome_blueprint = Blueprint('myHome_blueprint', __name__)


@myHome_blueprint.route("/myHome/Cards",methods=["GET","POST"])
def card():
    if request.method == "GET":
        arra = [{"uniqueId":101,"name":'Personal Info'},{'uniqueId':102,"name":"Claims and Advance"},{"uniqueId":103,"name":"Asset"},{'uniqueId':104,"name":"Approvals"},{'uniqueId':105,"name":"PTW"}]
        return jsonify({"data":arra,"status":200})
    


@myHome_blueprint.route('/myHome/getPersonalInfo', methods=['GET'])
@token_required
def manageemployee(current_user):

    if request.method=='GET':
        arra = [
            {
                '$match':{
                    '_id':ObjectId(current_user['userUniqueId'])
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    },
                    'joiningDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$joiningDate'
                            }, 
                            'timezone': 'Asia/Kolkata', 
                            'format': '%d-%m-%Y'
                        }
                    },
                    'dob': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$dob'
                            }, 
                            'timezone': 'Asia/Kolkata', 
                            'format': '%d-%m-%Y'
                        }
                    },
                    'resignDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$resignDate'
                            }, 
                            'timezone': 'Asia/Kolkata', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'lastWorkingDay': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$lastWorkingDay'
                            }, 
                            'timezone': 'Asia/Kolkata', 
                            'format': '%d-%m-%Y'
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'userRole', 
                    'localField': 'userRole', 
                    'foreignField': '_id', 
                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                    'as': 'userRole'
                }
            }, {
                '$unwind': {
                    'path': '$userRole', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'userRole': '$userRole.roleName'
                }
            }, {
                '$addFields': {
                    'circle': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$circle', ''
                                ]
                            }, '', {
                                '$toObjectId': '$circle'
                            }
                        ]
                    }, 
                    'designation': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$designation', ''
                                ]
                            }, '', {
                                '$toObjectId': '$designation'
                            }
                        ]
                    }, 
                    'role': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$role', ''
                                ]
                            }, '', {
                                '$toObjectId': '$role'
                            }
                        ]
                    }, 
                    'costCenter': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$costCenter', ''
                                ]
                            }, '', {
                                '$toObjectId': '$costCenter'
                            }
                        ]
                    }, 
                    'department': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$department', ''
                                ]
                            }, '', {
                                '$toObjectId': '$department'
                            }
                        ]
                    }, 
                    'reportingManager': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$reportingManager', ''
                                ]
                            }, '', {
                                '$toObjectId': '$reportingManager'
                            }
                        ]
                    }, 
                    'L1Approver': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L1Approver', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L1Approver'
                            }
                        ]
                    }, 
                    'L2Approver': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L2Approver', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L2Approver'
                            }
                        ]
                    }, 
                    'financeApprover': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$financeApprover', ''
                                ]
                            }, '', {
                                '$toObjectId': '$financeApprover'
                            }
                        ]
                    }, 
                    'reportingHrManager': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$reportingHrManager', ''
                                ]
                            }, '', {
                                '$toObjectId': '$reportingHrManager'
                            }
                        ]
                    }, 
                    'assetManager': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$assetManager', ''
                                ]
                            }, '', {
                                '$toObjectId': '$assetManager'
                            }
                        ]
                    }, 
                    'L1Vendor': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L1Vendor', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L1Vendor'
                            }
                        ]
                    }, 
                    'L2Vendor': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L2Vendor', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L2Vendor'
                            }
                        ]
                    }, 
                    'L1Compliance': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L1Compliance', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L1Compliance'
                            }
                        ]
                    }, 
                    'L2Compliance': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L2Compliance', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L2Compliance'
                            }
                        ]
                    }, 
                    'L1Commercial': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L1Commercial', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L1Commercial'
                            }
                        ]
                    }, 
                    'L1Sales': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L1Sales', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L1Sales'
                            }
                        ]
                    }, 
                    'L2Sales': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$L2Sales', ''
                                ]
                            }, '', {
                                '$toObjectId': '$L2Sales'
                            }
                        ]
                    },
                    'customer': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$customer', ''
                                ]
                            }, '', {
                                '$toObjectId': '$customer'
                            }
                        ]
                    },
                }
            }, {
                '$lookup': {
                    'from': 'circle', 
                    'localField': 'circle', 
                    'foreignField': '_id', 
                    'as': 'circle'
                }
            }, {
                '$lookup': {
                    'from': 'designation', 
                    'localField': 'designation', 
                    'foreignField': '_id', 
                    'as': 'designation'
                }
            }, {
                "$lookup": {
                    "from": "costCenter",
                    "localField": "costCenter",
                    "foreignField": "_id",
                    "as": "costCenter",
                }
            }, {
                '$lookup': {
                    'from': 'userRole', 
                    'localField': 'role', 
                    'foreignField': '_id', 
                    'as': 'role'
                }
            }, {
                '$lookup': {
                    'from': 'department', 
                    'localField': 'department', 
                    'foreignField': '_id', 
                    'as': 'department'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'reportingManager', 
                    'foreignField': '_id', 
                    'as': 'reportingManager'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L1Approver', 
                    'foreignField': '_id', 
                    'as': 'L1Approver'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L2Approver', 
                    'foreignField': '_id', 
                    'as': 'L2Approver'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'financeApprover', 
                    'foreignField': '_id', 
                    'as': 'financeApprover'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'reportingHrManager', 
                    'foreignField': '_id', 
                    'as': 'reportingHrManager'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'assetManager', 
                    'foreignField': '_id', 
                    'as': 'assetManager'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L1Vendor', 
                    'foreignField': '_id', 
                    'as': 'L1Vendor'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L2Vendor', 
                    'foreignField': '_id', 
                    'as': 'L2Vendor'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L1Compliance', 
                    'foreignField': '_id', 
                    'as': 'L1Compliance'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L2Compliance', 
                    'foreignField': '_id', 
                    'as': 'L2Compliance'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L1Commercial', 
                    'foreignField': '_id', 
                    'as': 'L1Commercial'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L1Sales', 
                    'foreignField': '_id', 
                    'as': 'L1Sales'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'L2Sales', 
                    'foreignField': '_id', 
                    'as': 'L2Sales'
                }
            }, {
                '$lookup': {
                    'from': 'customer', 
                    'localField': 'customer', 
                    'foreignField': '_id', 
                    'as': 'customer'
                }
            }, {
                '$addFields': {
                    'circle': {
                        '$arrayElemAt': [
                            '$circle.circleName', 0
                        ]
                    }, 
                    'designation': {
                        '$arrayElemAt': [
                            '$designation.designation', 0
                        ]
                    }, 
                    'role': {
                        '$arrayElemAt': [
                            '$role.roleName', 0
                        ]
                    }, 
                    'costCenter': {
                        '$arrayElemAt': [
                            '$costCenter.costCenter', 0
                        ]
                    },
                    'department': {
                        '$arrayElemAt': [
                            '$department.department', 0
                        ]
                    }, 
                    'reportingManager': {
                        '$arrayElemAt': [
                            '$reportingManager.empName', 0
                        ]
                    }, 
                    'L1Approver': {
                        '$arrayElemAt': [
                            '$L1Approver.empName', 0
                        ]
                    }, 
                    'L2Approver': {
                        '$arrayElemAt': [
                            '$L2Approver.empName', 0
                        ]
                    }, 
                    'financeApprover': {
                        '$arrayElemAt': [
                            '$financeApprover.empName', 0
                        ]
                    }, 
                    'reportingHrManager': {
                        '$arrayElemAt': [
                            '$reportingHrManager.empName', 0
                        ]
                    }, 
                    'assetManager': {
                        '$arrayElemAt': [
                            '$assetManager.empName', 0
                        ]
                    }, 
                    'L1Vendor': {
                        '$arrayElemAt': [
                            '$L1Vendor.empName', 0
                        ]
                    }, 
                    'L2Vendor': {
                        '$arrayElemAt': [
                            '$L2Vendor.empName', 0
                        ]
                    }, 
                    'L1Compliance': {
                        '$arrayElemAt': [
                            '$L1Compliance.empName', 0
                        ]
                    }, 
                    'L2Compliance': {
                        '$arrayElemAt': [
                            '$L2Compliance.empName', 0
                        ]
                    }, 
                    'L1Commercial': {
                        '$arrayElemAt': [
                            '$L1Commercial.empName', 0
                        ]
                    }, 
                    'L1Sales': {
                        '$arrayElemAt': [
                            '$L1Sales.empName', 0
                        ]
                    }, 
                    'L2Sales': {
                        '$arrayElemAt': [
                            '$L2Sales.empName', 0
                        ]
                    },
                    'customer': {
                        '$arrayElemAt': [
                            '$customer.customerName', 0
                        ]
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("userRegister",arra)
        return respond(response)




# @myHome_blueprint.route('/myHome/myTask', methods=["GET","POST","PUT","PATCH","DELETE"])
# @token_required
# def myhome_mytask(current_user):
#     taskStatus = ["Open","In Process","Reject"]
#     if request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined":
#         taskStatus = ["Open","In Process","Submit","Approve","Reject","Submit to Airtel","Closed"]
#     if request.args.get("mileStoneStatus")!=None and request.args.get("mileStoneStatus")!='undefined':
#         taskStatus = [request.args.get("mileStoneStatus")]
#         if taskStatus == ["All"]:
#             taskStatus = ["Open","In Process","Submit","Approve","Reject","Submit to Airtel","Closed"]
#     arra = [
#         {
#             '$match':{
#                 'deleteStatus':{'$ne':1},
#                 "assignerId":ObjectId(current_user['userUniqueId']),
#                 "mileStoneStatus":{
#                     '$in':taskStatus
#                 }
#             }
#         }
#     ]
#     if request.args.get("mileStoneName") != None and request.args.get("mileStoneName") != "undefined":
#         arra = arra + [
#             {
#                 '$match': {
#                     'Name': {
#                         '$regex': re.escape(request.args.get("mileStoneName")), 
#                         '$options': 'i'
#                     }
#                 }
#             }
#         ]
#     if request.args.get("subProject") != None and request.args.get("subProject") != "undefined":
#         arra = arra + [
#             {
#                 '$match': {
#                     'SubProjectId':request.args.get("subProject")
#                 }
#             }
#         ]
#     if request.args.get("customer")!=None and request.args.get("customer")!="undefined":
#         arra = arra + [
#             {
#                 '$match':{
#                     'customerId':request.args.get('customer')
#                 }
#             }
#         ]
#     arra = arra + [
#         {
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
#                 }, 
#                 'taskageing': {
#                     '$cond': {
#                         'if': {
#                             '$eq': [
#                                 {
#                                     '$type': '$CC_Completion Date1'
#                                 }, 'date'
#                             ]
#                         }, 
#                         'then': {
#                             '$round': {
#                                 '$divide': [
#                                     {
#                                         '$subtract': [
#                                             '$mileStoneEndtDate1', '$CC_Completion Date1'
#                                         ]
#                                     }, 86400000
#                                 ]
#                             }
#                         }, 
#                         'else': {
#                             '$round': {
#                                 '$divide': [
#                                     {
#                                         '$subtract': [
#                                             '$mileStoneEndtDate1', '$$NOW'
#                                         ]
#                                     }, 86400000
#                                 ]
#                             }
#                         }
#                     }
#                 }, 
#                 'mileStoneStartDate': {
#                     '$dateToString': {
#                         'date': {
#                             '$toDate': '$mileStoneStartDate'
#                         }, 
#                         'format': '%d-%m-%Y', 
#                         'timezone': 'Asia/Kolkata'
#                     }
#                 }, 
#                 'mileStoneEndDate': {
#                     '$dateToString': {
#                         'date': {
#                             '$toDate': '$mileStoneEndDate'
#                         }, 
#                         'format': '%d-%m-%Y', 
#                         'timezone': 'Asia/Kolkata'
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
#                             }
#                         }
#                     }, {
#                         '$project': {
#                             '_id': 0, 
#                             'assignerName': '$empName', 
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
#                             "projectType":'$SubTypeResult.projectType',
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
#                                         'PMName': '$PMarray.empName'
#                                     }
#                                 }, {
#                                     '$project': {
#                                         'PMName': 1, 
#                                         'projectId': 1, 
#                                         '_id': 0
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
#         }
#     ]
#     if request.args.get("siteName") != None and request.args.get("siteName") != "undefined":
#         arra = arra + [
#             {
#                 '$match': {
#                     'Site Id':{
#                         '$regex':re.escape(request.args.get("siteName").strip()),
#                         '$options':"i"
#                     }
#                 }
#             }
#         ]
#     if request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined":
#         siteStatus = [request.args.get("siteStatus")]
#         if siteStatus == ['all']:
#             siteStatus = ['Open',"Close","Drop"]
#         arra = arra + [
#             {
#                 '$match': {
#                     'siteStatus':{
#                         '$in':siteStatus
#                     }
#                 }
#             }
#         ]
#     arra = arra + apireq.countarra("milestone",arra) + apireq.args_pagination(request.args)
#     arra = arra + [
#         {
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
#                 'customerId':{
#                     '$toObjectId':'$customerId'
#                 }
#             }
#         }, {
#             '$lookup': {
#                 'from': 'customer', 
#                 'localField': 'customerId', 
#                 'foreignField': '_id', 
#                 'pipeline': [
#                     {
#                         '$match': {
#                             'deleteStatus': {
#                                 '$ne': 1
#                             }
#                         }
#                     }
#                 ], 
#                 'as': 'customerResult'
#             }
#         }, {
#             '$addFields': {
#                 'customerName': {
#                     '$arrayElemAt': [
#                         '$customerResult.customerName', 0
#                     ]
#                 }
#             }
#         },
#         {
#             '$project': {
#                 'projectArray': 0, 
#                 'milestoneArray.siteId': 0,
#                 "SubTypeResult":0,
#                 "customerResult":0,
#                 "customerId":0
                
#             }
#         }, {
#             '$sort':{
#                 '_id':1
#             }
#         }
#     ]
#     response = cmo.finding_aggregate("milestone", arra)
#     return respond(response)

@myHome_blueprint.route('/myHome/myTask', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def myhome_mytask(current_user):
    taskStatus = ["Open","In Process","Reject"]
    if request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined":
        taskStatus = ["Open","In Process","Submit","Approve","Reject","Submit to Airtel","Closed"]
    if request.args.get("mileStoneStatus")!=None and request.args.get("mileStoneStatus")!='undefined':
        taskStatus = [request.args.get("mileStoneStatus")]
        if taskStatus == ["All"]:
            taskStatus = ["Open","In Process","Submit","Approve","Reject","Submit to Airtel","Closed"]
    arra = [
        {
            '$match':{
                'deleteStatus':{'$ne':1},
                # 'ptwDeleteStatus':{'$ne':1},
                "assignerId":ObjectId(current_user['userUniqueId']),
                "mileStoneStatus":{
                    '$in':taskStatus
                }
            }
        },
         {
        '$addFields': {
            'newID': {
                '$toString': '$_id'
            }
        }
    },{
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
                        'ptwDeleteStatus':{'$ne':1},
                    }
                }
            ], 
            'as': 'ptwData'
        }
    }, {
        '$addFields': {
            'isPtwRaise': {
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
            'isAutoClose': {
                '$ifNull': [
                    '$isAutoClose', False
                ]
            }
        }
    }
    ]
    if request.args.get("mileStoneName") != None and request.args.get("mileStoneName") != "undefined":
        arra = arra + [
            {
                '$match': {
                    'Name': {
                        '$regex': re.escape(request.args.get("mileStoneName")), 
                        '$options': 'i'
                    }
                }
            }
        ]
    if request.args.get("subProject") != None and request.args.get("subProject") != "undefined":
        arra = arra + [
            {
                '$match': {
                    'SubProjectId':request.args.get("subProject")
                }
            }
        ]
    if request.args.get("customer")!=None and request.args.get("customer")!="undefined":
        arra = arra + [
            {
                '$match':{
                    'customerId':request.args.get('customer')
                }
            }
        ]
    arra = arra + [
        {
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
            'ptwStatus': {
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
                    'else': ''
                }
            }, 
             'ptwType': {
                '$cond': {
                    'if': '$isPtwRaise', 
                    'then': {
                        '$arrayElemAt': [
                            '$ptwData.formType', 0
                        ]
                    }, 
                    'else': False
                }
            },
            'ptwNumber': {
                '$cond': {
                    'if': {
                        '$eq': [
                            '$isPtwRaise', True
                        ]
                    }, 
                    'then': {
                        '$arrayElemAt': [
                            '$ptwData.ptwNumber', 0
                        ]
                    }, 
                    'else': ''
                }
            }
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
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0, 
                            'assignerName': '$empName', 
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
                            "projectType":'$SubTypeResult.projectType',
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
                                        'PMName': '$PMarray.empName'
                                    }
                                }, {
                                    '$project': {
                                        'PMName': 1, 
                                        'projectId': 1, 
                                        '_id': 0
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
        }
    ]
    if request.args.get("siteName") != None and request.args.get("siteName") != "undefined":
        arra = arra + [
            {
                '$match': {
                    'Site Id':{
                        '$regex':re.escape(request.args.get("siteName").strip()),
                        '$options':"i"
                    }
                }
            }
        ]
    if request.args.get("siteStatus") != None and request.args.get("siteStatus") != "undefined":
        siteStatus = [request.args.get("siteStatus")]
        if siteStatus == ['all']:
            siteStatus = ['Open',"Close","Drop"]
        arra = arra + [
            {
                '$match': {
                    'siteStatus':{
                        '$in':siteStatus
                    }
                }
            }
        ]
    arra = arra + apireq.countarra("milestone",arra) + apireq.args_pagination(request.args)
    arra = arra + [
        {
            '$addFields': {
                'siteStartDate': {
                    '$toDate': '$siteStartDate'
                }, 
                'siteEndDate': {
                    '$toDate': '$siteStartDate'
                }, 
                'Site_Completion Date': {
                    '$toDate': '$siteStartDate'
                },
                'siteUid':{
                '$toString':'$_id'
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
                'customerId':{
                    '$toObjectId':'$customerId'
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
                'as': 'customerResult'
            }
        }, {
            '$addFields': {
                'customerName': {
                    '$arrayElemAt': [
                        '$customerResult.customerName', 0
                    ]
                }
            }
        },
        {
            '$project': {
                'projectArray': 0, 
                'milestoneArray.siteId': 0,
                'milestoneArray.ptwData': 0,
                "SubTypeResult":0,
                "customerResult":0,
                "customerId":0
                
            }
        }, {
            '$sort':{
                '_id':1
            }
        }
    ]

    response = cmo.finding_aggregate("milestone", arra)
    return respond(response)



@myHome_blueprint.route('/myHome/myPolicy', methods=["GET"])
@token_required
def myHome_myPolicy(current_user):
    arra = [
        {
            '$match':{
                '_id':ObjectId(current_user['userUniqueId'])
            }
        }
    ]
    resp = cmo.finding_aggregate("userRegister",arra)['data'][0]
    if "designation" in resp:
        designationId = resp['designation']
        arra = [
            {
                '$match':{
                    'Designation':designationId
                }
            }, {
                '$addFields':{
                    "Site Id":'$siteId',
                   "Milestone":"$taskName",
                }
            }, {
                '$project':{
                   "_id":0,
                   "Designation":0,
                   "uniqueId":0,
                   "designation":0,
                   "siteId":0,
                   "taskName":0
                }
            }
        ]
        response = cmo.finding_aggregate("DesignationclaimType",arra)
        return respond(response)
    else:
        return respond({
            'status':200,
            "data":[]
        })