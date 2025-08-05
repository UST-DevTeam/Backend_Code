from base import *
from common.config import is_valid_mongodb_objectid as is_valid_mongodb_objectid

employee_blueprint = Blueprint("employee_blueprint", __name__)

@employee_blueprint.route("/hr/allEmployee", methods=["GET", "POST"])
@employee_blueprint.route("/hr/allEmployee/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def employeeAllEMployee(current_user, id=None):
    if request.method == "GET":
        if id == None:
            arra = [
                {
                    '$match': {
                        'type': {
                            '$ne': 'Partner'
                        }
                    }
                }, {
                    '$addFields': {
                        'empName': {
                            '$concat': [
                                '$empName', '(', '$empCode', ')'
                            ]
                        }, 
                        'uniqueId': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$project': {
                        'uniqueId': 1, 
                        '_id': 0, 
                        'empName': 1,
                        'empCode':1,
                        'ustCode':1,
                        'empID':"$uniqueId"
                    }
                }
            ]
            Response = cmo.finding_aggregate("userRegister", arra)
            return respond(Response)


@employee_blueprint.route("/hr/allHr", methods=["GET", "POST"])
@employee_blueprint.route("/hr/allHr/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def employeeAllHr(current_user, id=None):
    if request.method == "GET":
        if id == None:
            arr = [
                {"$match": {"roleName": "HR Head"}},
                {
                    "$lookup": {
                        "from": "userRegister",
                        "localField": "_id",
                        "foreignField": "userRole",
                        "pipeline": [{"$match": {"deleteStatus": {"$ne": 1}}}],
                        "as": "users",
                    }
                },
                {"$unwind": {"path": "$users", "preserveNullAndEmptyArrays": True}},
                {
                    "$addFields": {
                        "uniqueId": {"$toString": "$users._id"},
                        "empName": {
                            "$concat": ["$users.empName", "(", "$users.empCode", ")"]
                        },
                    }
                },
                {"$project": {"uniqueId": 1, "_id": 0, "empName": 1}},
            ]
            Response = cmo.finding_aggregate("userRole", arr)
            return respond(Response)


@employee_blueprint.route("/hr/vendor", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@employee_blueprint.route("/hr/vendor/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def managevendor(current_user, id=None):
    
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'type': 'Partner', 
                    "deleteStatus":{"$ne":1}
                    
                }
            }
        ]
           
        if (request.args.get("vendorName") != None and request.args.get("vendorName") != "undefined"):
            arra = arra + [
                {"$match": {"vendorName": {"$regex": request.args.get("vendorName"), '$options': 'i' }}}
            ]
        if (request.args.get("vendorCode") != None and request.args.get("vendorCode") != "undefined"):
            arra = arra + [{"$match": {"vendorCode": {"$regex":cdc.regexspecialchar(request.args.get("vendorCode")), '$options': 'i' }}}]
        status = "Active"
        if (request.args.get("status") != None and request.args.get("status") != "undefined"):
            status = request.args.get("status")
        if id == None or id == "undefined":
            arra = arra + [
                {
                    '$match':{
                        'status':status
                    }
                }
            ]
        if id!= None and id != "undefined":
                arra = arra + [
                    {
                        '$match':{
                            '_id':ObjectId(id)
                        }
                    }
                ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        arra = arra + [
            {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'dateOfRegistration': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$dateOfRegistration', ''
                                ]
                            }, 
                            'then': '', 
                            'else': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y', 
                                    'date': {
                                        '$dateFromString': {
                                            'dateString': '$dateOfRegistration'
                                        }
                                    }
                                }
                            }
                        }
                    }, 
                    'validityUpto': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$validityUpto', ''
                                ]
                            }, 
                            'then': '', 
                            'else': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y', 
                                    'date': {
                                        '$dateFromString': {
                                            'dateString': '$validityUpto'
                                        }
                                    }
                                }
                            }
                        }
                    }, 
                    'Circle': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$Circle', ''
                                ]
                            }, '', {
                                '$toObjectId': '$Circle'
                            }
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'circle', 
                    'localField': 'Circle', 
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'circleName': 1
                            }
                        }
                    ], 
                    'as': 'Circleresult'
                }
            }, {
                '$unwind': {
                    'path': '$Circleresult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'Circle': '$Circleresult._id', 
                    'circleName': '$Circleresult.circleName', 
                    'userRole': {
                        '$toString': '$userRole'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'Circleresult': 0
                }
            }, {
                '$sort': {
                    'vendorCode': 1
                }
            }
        ]
        Response = cmo.finding_aggregate("userRegister", arra)
        return respond(Response)
    
    elif request.method == "POST":
        if id == None:
            formData = request.form
            formFile = request.files
            vendorCode=request.form.get('vendorCode')
            userRole=request.form.get('userRole')
            allData = {}
            
            for i in formData:
                allData[i] = formData[i].strip()
                if i == "userRole":
                    allData['userRole'] = ObjectId(formData[i])
            arra = [
                {
                    '$match': {
                        '$or': [
                            {
                                'email': allData['email']
                            }, {
                                'vendorCode': allData['vendorCode']
                            }
                        ]
                    }
                }
            ]
            if len((cmo.finding_aggregate("userRegister",arra))['data']):
                return respond({
                    'msg':'Official Email-ID OR Vendor Code is already found in database',
                    "icon":"error",
                    "status":400
                })
            
                
            for i in formFile:
                path = cform.singleFileSaver(request.files.get(i), "", "")
                if path["status"] == 200:
                    allData[i] = path["msg"]
                elif path["status"] == 422:
                    return respond(path)
            allData["type"] = "Partner"
            allData["empName"]=allData["vendorName"]
            response = cmo.insertion("userRegister", allData)
            return respond(response)
        elif id != None:
            formData = request.form
            formFile = request.files
            allData = {}
            for i in formData:
                allData[i] = formData[i].strip()
                if i == "userRole":
                    allData[i] = ObjectId(formData[i])
            for i in formFile:
                path = cform.singleFileSaver(request.files.get(i), "", "")
                if path["status"] == 200:
                    allData[i] = path["msg"]
                elif path["status"] == 422:
                    return respond(path)
            
            ttt=[
            "SecContactDetails",
            "accounctNumber",
            "bankAddress",
            "bankName",
            "cbt",
            "companyType",
            "contactDetails",
            "contactPerson",
            "dateOfRegistration",
            "email",
            "epfNumber",
            "esiNumber",
            "financialTurnover",
            "formToci",
            "gstNumber",
            "ifscCode",
            "panNumber",
            "password",
            "paymentTerms",
            "ranking",
            "status",
            "stnNumber",
            "tanNumber",
            "teamCapacity",
            "type",
            "uniqueId",
            "userRole",
            "ustCode",
            "validityUpto",
            "vendorCategory",
            "vendorCode",
            "vendorName",
            "vendorRegistered",
            "registeredAddress",
            "vendorSubCategory",
            "Circle",
            "otherInfo",
            "technology",
            "pan[]",
            "tan[]",
            "esi[]",
            "epf[]",
            "stn[]",
            "contactCopy[]",
            "partnerRateCard[]",
            "cheque[]",
            "cbtCertificate[]",
            "tociCertificate[]",
            "gst[]"]
            delItem = []
            for i in allData:
                if i not in ttt:
                   delItem.append(i)
            if (len(delItem)):
                for i in delItem:
                    del allData[i]    
            
            allData['empName']=allData['vendorName']
            updateBy = {"_id": ObjectId(id)}
            print('djjdjdjdj',allData)
            response = cmo.updating("userRegister", updateBy, allData, False)
            return respond(response)
    elif request.method == "DELETE":
        allData = request.json.get('ids')
        for i in allData:
            response = cmo.deleting("userRegister", i,current_user['userUniqueId'])
        return respond({
            "status":400,
            "msg":"Data Deleted Successfully",
            "icon":"success"
        })