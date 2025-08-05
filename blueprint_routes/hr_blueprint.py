from base import *
from common.config import is_valid_mongodb_objectid as is_valid_mongodb_objectid
from common.config import checkngStringtimeStamp as checkngStringtimeStamp
from common.config import changedThings as changedThings
from common.config import get_current_date_timestamp as get_current_date_timestamp
from common.config import changedThings2 as changedThings2
from common.config import changedThings5 as changedThings5
hr_blueprint = Blueprint("hr_blueprint", __name__)


def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time   



@hr_blueprint.route("/hr/manageEmployee", methods=["GET", "POST","DELETE"])
@hr_blueprint.route("/hr/manageEmployee/<id>", methods=["GET", "POST", "DELETE"])
@token_required
def manageemployee(current_user, id=None):
    if request.method == "GET":
        arra = [
            {
                '$match':{
                    'type':{
                        '$ne':"Partner"
                    },
                    "deleteStatus":{
                        "$ne": 1
                    }
                }
            }
        ]
        if (request.args.get("empName") != None and request.args.get("empName") != "undefined"):
            arra = arra + [
                {"$match": {"empName": {"$regex": request.args.get("empName"), '$options': 'i' }}}
            ]
        if (request.args.get("empCode") != None and request.args.get("empCode") != "undefined"):
            arra = arra + [{"$match": {"empCode": {"$regex": request.args.get("empCode"), '$options': 'i' }}}]
        if (request.args.get("pmisRole") != None and request.args.get("pmisRole") != "undefined"):
            arra = arra + [{"$match": {"userRole": ObjectId(request.args.get("pmisRole"))}}]
        status = "Active"
        if (request.args.get("status") != None and request.args.get("status") != "undefined"):
            status = request.args.get("status")
        if id == None or id == "true":
            arra = arra + [
                {
                    '$match':{
                        'status':status
                    }
                }
            ]
        if id != None and id != "true":
            arra = arra + [{"$match": {"_id": ObjectId(id)}}]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        arra = arra + [
            {
                "$addFields": {
                    "designation": {
                        "$cond": [
                            {"$eq": ["$designation", ""]},
                            "",
                            {"$toObjectId": "$designation"},
                        ]
                    },
                    "userRoleId": {
                        "$cond": [{"$eq": ["$role", ""]}, "", {"$toObjectId": "$role"}]
                    },
                }
            },
            {
                "$lookup": {
                    "from": "designation",
                    "localField": "designation",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": {"$toString": "$_id"}, "designation": 1}},
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
                    "designation": "$designationResult._id",
                    "designationName": "$designationResult.designation",
                }
            },
            {
                "$lookup": {
                    "from": "userRole",
                    "localField": "userRole",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": {"$toString": "$_id"}, "roleName": 1}},
                    ],
                    "as": "role",
                }
            },
            {"$unwind": {"path": "$role", "preserveNullAndEmptyArrays": True}},
            {"$addFields": {"userRoleName": "$role.roleName", "role": "$role._id"}},
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "_id": {"$toString": "$_id"},
                }
            },
            {"$project": {"designationResult": 0}},
        ]
        if request.args.get("userRole") != None and request.args.get("userRole") != "":
            matchArra = [
                {"$match": {"roleName": request.args.get("userRole")}},
                {"$project": {"userRole": "$_id", "_id": 0}},
            ]
            response = cmo.finding_aggregate("userRole", matchArra)
            userRole = response["data"][0]["userRole"]
            # print(userRole,"userRoleuserRole")
            arra = arra + [{"$match": {"userRole": userRole}}]
        arra = arra + [
            {"$addFields": {"userRole": {"$toString": "$userRole"}}},
            {"$project": {"_id": 0, "role": 0}},
            {
                "$lookup": {
                    "from": "userRole",
                    "localField": "userRoleId",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": {"$toString": "$_id"}, "roleName": 1}},
                    ],
                    "as": "userRolesresult",
                }
            },
            {
                "$unwind": {
                    "path": "$userRolesresult",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$addFields": {
                    "role": "$userRolesresult._id",
                    "roleName": "$userRolesresult.roleName",
                }
            },
            {"$project": {"userRoleId": 0, "userRolesresult": 0}},
        ]
        response = cmo.finding_aggregate("userRegister", arra)
        return respond(response)
    
    

    elif request.method == "POST":
        if id == None: 
            formData = request.form
            formFile = request.files
            allData = {}
            for i in formData:
                allData[i] = formData[i].strip()
                if i == "userRole":
                    allData['userRole'] = ObjectId(formData[i])
            for i in formFile:
                path = cform.singleFileSaver(request.files.get(i), "", "")
                if path["status"] == 200:
                    allData[i] = path["msg"]
                elif path["status"] == 422:
                    return respond(path) 
            for i in allData:
                if allData[i] == "":
                    allData[i]=None
            arra = [
                {
                    '$match': {
                        '$or': [
                            {
                                'email': allData['email']
                            }, {
                                'empCode': allData['empCode']
                            }
                        ]
                    }
                }
            ]
            if len((cmo.finding_aggregate("userRegister",arra))['data']):
                return respond({
                    'msg':'Official Email-ID OR Employee Code is already found in database',
                    "icon":"error",
                    "status":400
                })
            
            response = cmo.insertion("userRegister", allData)
            Added="New Employee Added "+allData['empName']+" and Employee Code is "+allData['empCode']
            cmo.insertion("AdminLogs",{'type':'Add','module':'Manage Employee','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            # cmo.insertion("AdminLogs",{'type':'Added','email':allData['email'],'empCode': allData['empCode'],'userID':ObjectId(response['operation_id']),'AddedAt':int(get_current_date_timestamp()),'AddedBy':ObjectId(current_user['userUniqueId'])})
            return respond(response)
        elif id != None:
            formData = request.form
            formFile = request.files
            allData = {}
            for i in formData:
                

                allData[i] = formData[i].strip()
                if i == "userRole":
                    allData[i] = ObjectId(formData[i])
                if i == 'dob':
                    
                    if checkngStringtimeStamp(allData['dob']):
                        allData[i]=int(allData[i])
                if i == 'lastWorkingDay':
                    if checkngStringtimeStamp(allData['lastWorkingDay']):
                        allData[i]=int(allData[i])
                if i == 'joiningDate':
                    if checkngStringtimeStamp(allData['joiningDate']):
                        allData[i]=int(allData[i])
 
            for i in formFile:
                path = cform.singleFileSaver(request.files.get(i), "", "")
                if path["status"] == 200:
                    allData[i] = path["msg"]
                elif path["status"] == 422:
                    return respond(path)
            ttt = [
                "title",
                "empName",
                "empCode",
                "resignDate",
                "costCenter",
                "ustCode",
                "fatherName",
                "motherName",
                "martialStatus",
                "costCenter",
                "resignDate",
                "email",
                "dob",
                "mobile",
                "blood",
                "country",
                "state",
                "city",
                "pincode",
                "address",
                "fillAddress",
                "pcountry",
                "pstate",
                "pcity",
                "ppincode",
                "paddress",
                "panNumber",
                "adharNumber",
                "circle",
                "experience",
                "salaryCurrency",
                "monthlySalary",
                "grossCtc", 
                "joiningDate",
                "samePerAdd",
                "lastWorkingDay",
                "resignDate",
                "passport",
                "passportNumber",
                "bankName",
                "accountNumber",
                "ifscCode" ,
                "benificiaryname",
                "orgLevel",
                "designation",
                "role",
                "userRole",
                "band",
                "department",
                "reportingManager",
                "L1Approver",
                "L2Approver",
                "financeApprover",
                "reportingHrManager",
                "assetManager",
                "L1Vendor",
                "L2Vendor", 
                "L1Compliance",
                "L2Compliance",
                "L1Commercial",
                "L1Sales",
                "L2Sales",
                "status",
                "password",
                "img",
                "img[]", 
                "cv",
                "zip",
                "personalEmailId",
                "customer",
                "ustProjectId",
                "ustJobCode",
                'businesssUnit',
                'allocationPercentage',
            ]
            delItem = []
            for i in allData:
                if allData[i] == "":
                    allData[i]=None
                    

                if i not in ttt:
                   delItem.append(i)
            if (len(delItem)):
                for i in delItem:
                    del allData[i]
            
            
            updateBy = {"_id": ObjectId(id)}
            ###### employee Logs
            
            try:
                
                arro=[
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
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
                Responser=cmo.finding_aggregate('userRegister',arro)['data']
                allData2=Responser[0]
                
                dataChanged2=changedThings5(allData,allData2)
                Tempkeys = [
                        "email",
                        "password",
                        "userRole",
                        "empName",
                        "empCode",
                        "mobile",
                        "status",
                        "designation",
                        "L1Approver",
                        "L1Commercial",
                        "L1Compliance",
                        "L1Sales",
                        "L1Vendor",
                        "L2Approver",
                        "L2Compliance",
                        "L2Vendor",
                        "accountNumber",
                        "address",
                        "adharNumber",
                        "assetManager",
                        "band",
                        "bankName",
                        "benificiaryname",
                        "blood",
                        "city",
                        "compliance",
                        "country",
                        "department",
                        "experience",
                        "fatherName",
                        "fillAddress",
                        "financeApprover",
                        "grossCtc",
                        "ifscCode",
                        "martialStatus",
                        "monthlySalary",
                        "motherName",
                        "orgLevel",
                        "panNumber",
                        "passport",
                        "personalEmailId",
                        "pincode",
                        "reportingHrManager",
                        "reportingManager",
                        "salaryCurrency",
                        "state",
                        "title",
                        "uniqueId",
                        "samePerAdd",
                        "cv[]",
                        "img[]",
                        "paddress",
                        "pcity",
                        "pcountry",
                        "ppincode",
                        "pstate",
                        "cv",
                        "img",
                        "userRoleName",
                        "passportNumber",
                        "L2Sales",
                        "circle",
                        "role",
                        "ustCode",
                        "dob",
                        "joiningDate",
                        "lastWorkingDay",
                        "costCenter"
                    ]

                originalNames={"email":"Email",
                "password":"Password",
                "userRole":"PMIS Role",
                "empName":"Employee Name",
                "empCode":"Employee Code",
                "mobile":"Mobile",
                "status":"Status",
                "designation":"Grade",
                "L1Approver":"L1-Approver",
                "L1Commercial":"L1-Commercial",
                "L1Compliance":"L1-Compliance",
                "L1Sales":"L1-Sales",
                "L1Vendor":"L1-Vendor",
                "L2Approver":"L2-Approver",
                "L2Compliance":"L2-Compliance",
                "L2Vendor":"L2-Vendor",
                "accountNumber":"Account Number",
                "address":"Address",
                "adharNumber":"Adhar Number",
                "assetManager":"assetManager",
                "band":"Designation",
                "bankName":"Bank Name",
                "benificiaryname":"Benificiary Name",
                "blood":"Blood Group",
                "city":"City",
                "compliance":"Compliance",
                "country":"Country",
                "department":"Department",
                "experience":"Experience",
                "fatherName":"Father's Name",
                "fillAddress":"fillAddress",
                "financeApprover":"Finance Approver",
                "grossCtc":"Gross Ctc",
                "ifscCode":"Ifsc Code",
                "martialStatus":"Martial Status",
                "monthlySalary":"Monthly Salary",
                "motherName":"Mother's Name",
                "orgLevel":"Organization Level",
                "panNumber":"Pan Number",
                "passport":"Passport",
                "personalEmailId":"Personal Email-Id",
                "pincode":"Pincode",
                "reportingHrManager":"Reporting Hr Manager",
                "reportingManager":"Reporting Manager",
                "salaryCurrency":"Salary Currency",
                "state":"State",
                "title":"Title",
                "uniqueId":"uniqueId",
                "samePerAdd":"samePerAdd",
                "cv[]":"cv",
                "img[]":"img",
                "paddress":"Permanent Address",
                "pcity":"Permanent City",
                "pcountry":"Permanent Country",
                "ppincode":"Permanent Pincode",
                "pstate":"Permanent State",
                "cv":"CV",
                "img":"Image",
                "userRoleName":"User Role Name",
                "passportNumber":"Passport Number",
                "L2Sales":"L2 Sales",
                "circle":"Circle",
                "role":"Role",
                "ustCode":"UST Employee Code",
                "dob":"Date Of Birth",
                "joiningDate":"Joining Date",
                "lastWorkingDay":"Last Working Date",
                "costCenter":"Cost Center"}
                
                changedArray=[]
                for i in dataChanged2:
                    if i in Tempkeys:
                        changedArray.append(originalNames[i])
                    else:
                        changedArray.append(i)
                        # changedArray=changedArray+i+","
                
                Added=str(changedArray)+" updated for employee whose name is  "+allData['empName']
                
                cmo.insertion("AdminLogs",{'type':'Update','module':'Manage Employee','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
                
            
            except Exception as e:
                print(e,'eeeee') 
                
                
                
            # for i in dataChanged:
            #     insertionData={}
            #     insertionData['changedKey']=i['changedKey']
            #     insertionData['changedBy']=ObjectId(current_user['userUniqueId'])
            #     insertionData['changedAt']=int(get_current_date_timestamp())
            #     insertionData['Oldvalue']=i['Oldvalue']
            #     insertionData['Newvalue']=i['Newvalue']
            #     insertionData['userID']=ObjectId(id)
            #     insertionData['type']='Update'
            #     cmo.insertion('EmployeeLogs',insertionData)
            
            response = cmo.updating("userRegister", updateBy, allData, False)
            return respond(response)

    elif request.method == "DELETE":
        allData = request.json.get('ids')
        deletedIDS=[]
        for i in allData:
            deletedIDS.append(ObjectId(i))
            
            arrss=[
                {
                    '$match': {
                        '_id': ObjectId(i)
                    }
                }, {
                    '$project': {
                        'empCode': 1, 
                        'empName': 1, 
                        '_id': 0
                    }
                }
            ]
            Response=cmo.finding_aggregate("userRegister",arrss)['data']
            Circle=None
            
            if len(Response):
                Circle=Response[0]
                Added="Deleted This employee whose name is  "+Circle['empName']+" and Employee Code is "+Circle['empCode']
                cmo.insertion("AdminLogs",{'type':'Delete','module':'Manage Employee','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            
            
            
            
            
            response = cmo.deleting("userRegister", i)
        # cmo.insertion("AdminLogs",{'type':'Delete','userID':ObjectId(id),'DeletedAt':int(get_current_date_timestamp()),'DeletedBy':ObjectId(current_user['userUniqueId']),'DeletedUsers':deletedIDS})  
        return respond(response)
      
@hr_blueprint.route("/hr/manageProfile", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@hr_blueprint.route("/hr/manageProfile/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def manageprofile(current_user, id=None):

    if request.method == "GET":
        arra = [
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "_id": {"$toString": "$_id"},
                }
            },
            {"$project": {"code": 0, "permission": 0}},
            {'$sort':{'roleName':1}}
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("userRole", arra)
        return respond(response)

    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            arra = [{"$match": {"roleName": allData["roleName"]}}]
            response = cmo.finding_aggregate("userRole", arra)
            if len(response["data"]):
                return {
                    "status": 400,
                    "msg": "Profile is Already Exist",
                    "icon": "error",
                }, 400
            else:
                allData["permission"] = "{}"
                Added="The Profile "+" "+allData['roleName'] +" Added"
                cmo.insertion("AdminLogs",{'type':'Add','module':'Profile','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
                
                
                response = cmo.insertion("userRole", allData)
                return respond(response)

        elif id != None:
            allData = request.get_json()
            arra = [{"$match": {"roleName": allData["roleName"]}}]
            response = cmo.finding_aggregate("userRole", arra)
            if len(response["data"]):
                return {
                    "status": 400,
                    "msg": "Profile is Already Exist",
                    "icon": "error",
                }, 400
            else:
                updateBy = {"_id": ObjectId(id)}
                Added="The Profile "+" "+allData['roleName'] +" Updated"
                cmo.insertion("AdminLogs",{'type':'Update','module':'Profile','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
                
                response = cmo.updating("userRole", updateBy, allData, False)
                return respond(response)

    elif request.method == "DELETE":
        if id != None:
            
            
            arr=[
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }, {
                    '$project': {
                        'roleName': 1, 
                        '_id': 0
                    }
                }
            ]
            Response=cmo.finding_aggregate("userRole",arr)['data']
            Circle=None
            
            if len(Response):
                Circle=Response[0]['roleName']
                Added="Deleted This Proile "+Circle
                cmo.insertion("AdminLogs",{'type':'Delete','module':'Profile','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            
            
            
            
            response = cmo.deleting("userRole", id, current_user["userUniqueId"])
            return respond(response)


@hr_blueprint.route("/hr/projectAllocation", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@hr_blueprint.route("/hr/projectAllocation/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def projectallocation(current_user,id=None):

    if request.method == "GET":
        arra = [
            {
                '$match':{
                    'type':{'$ne':'Partner'},
                    "status":"Active"
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
                    'from': 'projectAllocation', 
                    'localField': '_id', 
                    'foreignField': 'empId', 
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
                    'projectIds': '$result.projectIds'
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
                            '$project': {
                                '_id': 0, 
                                'projectId': 1
                            }
                        }
                    ], 
                    'as': 'projectIdName'
                }
            }, {
                '$addFields': {
                    'projectIdName': '$projectIdName.projectId'
                }
            }]
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
        arra = arra + [
            {
                '$addFields': {
                    'projectIdName': {
                        '$reduce': {
                            'input': '$projectIdName', 
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
                    'userRole': '$role.roleName'
                }
            }, {
                '$addFields': {
                    'projectIds': {
                        '$map': {
                            'input': '$projectIds', 
                            'as': 'objectId', 
                            'in': {
                                '$toString': '$$objectId'
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'projectIds': {
                        '$reduce': {
                            'input': '$projectIds', 
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
                    'role': 0, 
                    '_id': 0, 
                    'result': 0
                }
            }
        ]
        response = cmo.finding_aggregate("userRegister", arra)
        return respond(response)

    elif request.method == "POST":
        if id != None:
            project = request.json.get("projectIds")
            if project != "" and project != None:
                value = []
                for i in project.split(","):
                    value.append(ObjectId(i))
                allData = {
                    "projectIds": value,
                    "empId": id,
                }
                updateBy = {"empId": id}
                try:
                    Message=None
                    AssignedProjects=""
                    artt=[
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    '_id': {
                                        '$in': allData['projectIds']
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 1, 
                                    'projectId': 1
                                }
                            }
                        ]
                    responsedData=cmo.finding_aggregate("project",artt)['data']
                    arty=[
                        {
                            '$match': {
                                '_id': ObjectId(id), 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'empCode': 1, 
                                'empName': 1, 
                                '_id': 0
                            }
                        }
                    ]
                    userDetails=cmo.finding_aggregate("userRegister",arty)['data']
                    for i in responsedData:
                         AssignedProjects=AssignedProjects+i['projectId']+","
                    Added=" these projects allocated to "+userDetails[0]['empName']+":-"+AssignedProjects
                    cmo.insertion("AdminLogs",{'type':'Update','module':'Project Allocation','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
                except Exception as e:
                    print(e)
                response = cmo.updating("projectAllocation", updateBy, allData, True)
                return respond(response)
            else:
                return respond({
                    'status':400,
                    "msg":"Please Select At least One Project",
                    "icon":"Error"
                })
                
    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("projectAllocation", id)
            return respond(response)
        else:
            return jsonify({"msg": "Please Provide Valid Unique Id"})
        
        
@hr_blueprint.route("/hr/L1ApproverAllocation", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@hr_blueprint.route("/hr/L1ApproverAllocation/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def L1ApproverAllocation(current_user,id=None):

    if request.method == "GET":
        arra = [
            {
                '$match':{
                    'type':{'$ne':'Partner'},
                    "status":"Active"
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
                    'from': 'projectAllocation', 
                    'localField': '_id', 
                    'foreignField': 'empId', 
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
                    'projectIds': '$result.projectIds'
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
                            '$project': {
                                '_id': 0, 
                                'projectId': 1
                            }
                        }
                    ], 
                    'as': 'projectIdName'
                }
            }, {
                '$addFields': {
                    'projectIdName': '$projectIdName.projectId'
                }
            }]
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
        arra = arra + [{
                '$addFields': {
                    'projectIdName': {
                        '$reduce': {
                            'input': '$projectIdName', 
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
                    'userRole': '$role.roleName'
                }
            }, {
                '$addFields': {
                    'projectIds': {
                        '$map': {
                            'input': '$projectIds', 
                            'as': 'objectId', 
                            'in': {
                                '$toString': '$$objectId'
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'projectIds': {
                        '$reduce': {
                            'input': '$projectIds', 
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
                    'role': 0, 
                    '_id': 0, 
                    'result': 0
                }
            }
        ]
        response = cmo.finding_aggregate("userRegister", arra)
        return respond(response)

    elif request.method == "POST":
        if id != None:
            project = request.json.get("projectIds")
            if project != "" and project != None:
                value = []
                for i in project.split(","):
                    value.append(ObjectId(i))
                allData = {
                    "projectIds": value,
                    "empId": id,
                }
                updateBy = {"empId": id}
                try:
                    Message=None
                    AssignedProjects=""
                    artt=[
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    '_id': {
                                        '$in': allData['projectIds']
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 1, 
                                    'projectId': 1
                                }
                            }
                        ]
                    responsedData=cmo.finding_aggregate("project",artt)['data']
                    arty=[
                        {
                            '$match': {
                                '_id': ObjectId(id), 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'empCode': 1, 
                                'empName': 1, 
                                '_id': 0
                            }
                        }
                    ]
                    userDetails=cmo.finding_aggregate("userRegister",arty)['data']
                    for i in responsedData:
                         AssignedProjects=AssignedProjects+i['projectId']+","
                    Added=" these projects allocated to "+userDetails[0]['empName']+":-"+AssignedProjects
                    cmo.insertion("AdminLogs",{'type':'Update','module':'Project Allocation','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
                except Exception as e:
                    print(e)
                response = cmo.updating("projectAllocation", updateBy, allData, True)
                return respond(response)
            else:
                return respond({
                    'status':400,
                    "msg":"Please Select At least One Project",
                    "icon":"Error"
                })
                
    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("projectAllocation", id)
            return respond(response)
        else:
            return jsonify({"msg": "Please Provide Valid Unique Id"})
