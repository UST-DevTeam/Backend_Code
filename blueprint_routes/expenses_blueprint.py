from base import *
from datetime import datetime as dtt
from datetime import timedelta
import pytz
from common.config import generate_new_ExpenseNo as generate_new_ExpenseNo
from common.config import *
import common.excel_write as excelWriteFunc
from common.config import convertToDateBulkExport as convertToDateBulkExport
from common.config import changedThings2 as changedThings2
from common.mongo_operations import db as database


def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time   




tz = pytz.timezone("Asia/Kolkata")
current_date = datetime.now(tz)
def three_days_ago():
    tz = pytz.timezone("Asia/Kolkata")
    current_date = datetime.now(tz)
    three_days_ago = current_date - timedelta(days=3)
    return three_days_ago
current_date = dtt.now()
# current_year = current_date.year
# current_month = current_date.strftime("%b").upper()
# if current_date.month >= 4:
#     fiscal_year = f"{current_year % 100}-{(current_year + 1) % 100}"
# else:
#     fiscal_year = f"{(current_year - 1) % 100}-{current_year % 100}"
# currentFinancialMonth = f"{fiscal_year}/{current_month}"


integernumber = []


def temPfunction2():
    arr = [{"$match": {"ExpenseNo": {"$exists": True}}}]
    response = cmo.finding_aggregate_with_deleteStatus("Expenses", arr)["data"]
    ExpenseNumber = "EXP/24-25/000000"
    varible = 1
    for i in response:
        # ExpenseNumber = generate_new_ExpenseNo(ExpenseNumber)
        pattern = r"^(EXP/\d{2}-\d{2}/)(\d{6})$"
        match = re.match(pattern, ExpenseNumber)
        base_part = match.group(1)
        numerical_part = match.group(2)
        new_numerical_part = int(numerical_part) + 1
        new_numerical_part_str = f"{new_numerical_part:06d}"  
        new_id = f"{base_part}{new_numerical_part_str}"
        ExpenseNumber=new_id
        
        
        ggg = ExpenseNumber.split("/")
        financialyear = ggg[1]
        varible = ggg[-1]
        varibleInt = int(varible)
        updateby = {"_id": (i["_id"])}
        updating = {
            "ExpenseNo": ExpenseNumber,
            "expenseNumberInt": varibleInt,
            "FinancialYear": financialyear,
            "expenseNumberStr": varible,
        }
        # print('dhdhdhd')
        cmo.updating("Expenses", updateby, updating, False)
        cmo.updating("Approval", {"ExpenseUniqueId": i["_id"]}, updating, False)
    print("Done!")


integernumber = []


def temPfunction3():
    arr = [{"$match": {"AdvanceNo": {"$exists": True}}}]
    response = cmo.finding_aggregate_with_deleteStatus("Advance", arr)["data"]
    AdvanceNumber = "ADV/24-25/000000"
    varible = 1
    for i in response:
        AdvanceNumber = generate_new_AdvanceNo(AdvanceNumber)
        ggg = AdvanceNumber.split("/")
        financialyear = ggg[1]
        varible = ggg[-1]
        varibleInt = int(varible)
        updateby = {"_id": (i["_id"])}
        updating = {
            "AdvanceNo": AdvanceNumber,
            "advanceNumberInt": varibleInt,
            "FinancialYear": financialyear,
            "advanceNumberStr": varible,
        }
        cmo.updating("Advance", updateby, updating, False)
        cmo.updating("Approval", {"AdvanceUniqueId": i["_id"]}, updating, False)


# temPfunction3()
# temPfunction2()


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



def currentYear(): 
    tz = pytz.timezone("Asia/Kolkata")
    current_date = datetime.now(tz)
    current_year = current_date.year
    return current_year


# print(currentFinancialMonth(),'fghjkl;gfghjkl;')


def current_date_str():
    utc_now = dtt.utcnow()
    asia_timezone = pytz.timezone("Asia/Kolkata")
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%d-%m-%Y")
    current_time = asia_now.strftime("%d/%m/%Y, %H:%M:%S")
    return current_time


expenses_blueprint = Blueprint("expenses_blueprint", __name__)


@expenses_blueprint.route("/expenses/updateattachment", methods=["POST", "Get"])
def updateattachment():
    upload = request.files.get("uploadedFile[]")
    allData = {}
    supportFile = ["xlsx", "xls"]
    pathing = cform.singleFileSaver(upload, "", supportFile)
    if pathing["status"] == 200:
        allData["pathing"] = pathing["msg"]
    elif pathing["status"] == 422:
        return respond(pathing)
    excel_file_path = os.path.join(os.getcwd(), allData["pathing"])
    exceldata = pd.read_excel(excel_file_path)

    # exceldata = request.files.get("uploadedFile[]")
    # fileType=request.files.get("fileType")

    jsonData = cfc.dfjson(exceldata)
    alreadyexists = []
    for i in jsonData:
        
        # arr = [
        #     {"$match": {"ExpenseNo": i["Expense number"], "deleteStatus": {"$ne": 1}}}
        # ]
        # response = cmo.finding_aggregate("Expenses", arr)["data"]
        # ExpenseNumber=i["New Exp ID"]
        # ggg = ExpenseNumber.split("/")
        # financialyear = ggg[1]
        # varible = ggg[-1]
        # varibleInt = int(varible)
        # updateby = {"_id": (i["_id"])}
        # updating = {
        #     "ExpenseNo": ExpenseNumber,
            
        # }
        
        #     # print(i["Expense number"])
        # cmo.updating(
        #     "Expenses",
        #     {"_id": ObjectId(i["_id"])},
        #     {"ExpenseNo": ExpenseNumber,
        #      "expenseNumberInt": varibleInt,
        #     "FinancialYear": financialyear,
        #     "expenseNumberStr": varible,
        #      },
            
        #     False,
        # )
        # cmo.updating(
        #     "Approval",
        #     {"ExpenseUniqueId": ObjectId(i["_id"])},
        #     {"ExpenseNo": ExpenseNumber,
        #      "expenseNumberInt": varibleInt,
        #     "FinancialYear": financialyear,
        #     "expenseNumberStr": varible,
        #      },
        #     False,
        # )
        
            # print(i["Expense number"])
            
            
        # ExpenseNumber=i["Advance number"]
        # ggg = ExpenseNumber.split("/")
        # financialyear = ggg[1]
        # varible = ggg[-1]
        # varibleInt = int(varible)
        updateby = {"AdvanceNo": (i["Advance number"])}
        updating = {
            "deleteStatus":1,
            "deletedAuthorised":"Pradeep Kumar",
            # "expenseNumberInt":varibleInt
        }
        cmo.updating(
            "Advance",
            
            updateby,
            updating,
            False,
        )
        cmo.updating(
            "Approval",
            updateby,
            updating,
            False,
        )

    print('fileCompleted')


def ExpenseNonewLogic():
    ExpenseNo2 = "EXP"
    ExpenseNo2 = f"{ExpenseNo2}{currentYear()}"
    counter = database.fileDetail.find_one_and_update({"id": "expenseIdCounter"},{"$inc": {"sequence_value": 1}},return_document=True,upsert=True)
    sequence_value = counter["sequence_value"]
    sequence_value=str(sequence_value).zfill(9)
    ExpenseNo2=ExpenseNo2+str(sequence_value)
    return ExpenseNo2




def AdvanceNonewLogic():
    # AdvanceNo = "ADV"
    # AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
    # newArra = [{"$sort": {"advanceNumberInt": -1}}]
    # responseData = cmo.finding_aggregate_with_deleteStatus("Advance", newArra)["data"]
    # if len(responseData) > 0 and "AdvanceNo" in responseData[0]:
    #     oldAdvanceNo = responseData[0]["AdvanceNo"]
    #     # print("AdvanceNoAdvanceNo", oldAdvanceNo)
    #     AdvanceNo = generate_new_AdvanceNo(oldAdvanceNo)
    #     # print("AdvanceNoAdvanceNo2", AdvanceNo)
    #     return AdvanceNo
    
    
    # AdvanceNo = "ADV"
    # AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
    # AdvanceNo2 = "ADV"
    # AdvanceNo2 = f"{AdvanceNo2}/{currentFinancialMonth()}/"
    # sortByAdvanceNumber = [{"$sort": {"advanceNumberInt": -1}}]
    # sortById = [{"$sort": {"_id": -1}}]
    # responseData = cmo.finding_aggregate_with_deleteStatus("Advance", sortByAdvanceNumber)["data"]
    # responseData2 = cmo.finding_aggregate_with_deleteStatus("Advance", sortById)["data"]
    # if len(responseData) > 0 and "AdvanceNo" in responseData[0]:
    #     oldAdvanceNo = responseData[0]["AdvanceNo"]
    #     AdvanceNo=generate_new_AdvanceNo(oldAdvanceNo)  
    # if len(responseData2) > 0 and "AdvanceNo" in responseData2[0]:
    #     oldAdvanceNo2 = responseData2[0]["AdvanceNo"]
    #     AdvanceNo2 = generate_new_AdvanceNo(oldAdvanceNo2)
    # if AdvanceNo2 == AdvanceNo:
    #     return AdvanceNo2
        
    # else:
    #     AdvanceNo2 = "ADV"
    #     AdvanceNo2 = f"{AdvanceNo2}/{currentFinancialMonth()}/"
    #     sortById = [{"$sort": {"_id": -1}}]
    #     responseData2 = cmo.finding_aggregate_with_deleteStatus("Advance", sortById)["data"]
    #     if len(responseData2) > 0 and "AdvanceNo" in responseData2[0]:
    #         oldAdvanceNo2 = responseData2[0]["AdvanceNo"]
    #         AdvanceNo2=generate_new_AdvanceNo(oldAdvanceNo) 
    #     return AdvanceNo2
    AdvanceNo2 = "ADV"
    AdvanceNo2 = f"{AdvanceNo2}/{currentFinancialMonth()}/"
    # sortById = [{"$sort": {"_id": -1}}]
    counter = database.fileDetail.find_one_and_update({"id": "AdvanceIdCounter"},{"$inc": {"sequence_value": 1}},return_document=True,upsert=True)
    
    sequence_value = counter["sequence_value"]
    sequence_value=str(sequence_value).zfill(6)
    AdvanceNo2=AdvanceNo2+str(sequence_value)
    return AdvanceNo2
    
def SettlementIDnewLogic():
    SettlementID = "SET"
    SettlementID = f"{SettlementID}/{currentFinancialMonth()}/"
    newArra = [{"$sort": {"_id": -1}}]
    responseData = cmo.finding_aggregate_with_deleteStatus("Settlement", newArra)["data"]
    if len(responseData) > 0 and "SettlementID" in responseData[0]:
        oldSettlementID = responseData[0]["SettlementID"]
        SettlementID = generate_new_SettlementID(oldSettlementID)
        return SettlementID
    else:
        SettlementID="SET/24-25/000001"
        return SettlementID
        


@expenses_blueprint.route("/expenses/ClaimType", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@expenses_blueprint.route("/expenses/ClaimType/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def manageExpenseClaimType(current_user, id=None):
    claimType = request.args.get("claimTypeId")
    if request.method == "GET":

        arra = [
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "attachment": {
                        "$cond": {
                            "if": {"$eq": ["$attachment", True]},
                            "then": "Yes",
                            "else": "No",
                        }
                    },
                }
            },
            {"$project": {"_id": 0}},
        ]
        arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("claimType", arra)
        return respond(response)
    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            arra = [
                {
                    "$match": {
                        "claimType": allData["claimType"],
                    }
                }
            ]
            response = cmo.finding_aggregate("claimType", arra)
            if len(response["data"]):
                return respond(
                    {
                        "status": 400,
                        "msg": "This claimType is Already Exist",
                        "icon": "error",
                    }
                )
            if "attachment" in allData:
                allData["attachment"] = allData["attachment"].lower()
                if allData["attachment"] == "yes":
                    allData["attachment"] = True
                elif allData["attachment"] == "no":
                    allData["attachment"] = False
                else:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": f"Attchment Accepts only Yes or No",
                        }
                    )

            Added="The CLaim Type "+" "+allData['claimType'] +" Added"
            cmo.insertion("AdminLogs",{'type':'Add','module':'CLaim Type','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            response = cmo.insertion("claimType", allData)
            return respond(response)
        elif id != None:
            allData = request.get_json()
            # print("fghjklhghj", allData)
            claimType = allData["claimType"]
            if "attachment" in allData:
                allData["attachment"] = allData["attachment"].lower()
                if allData["attachment"] == "yes":
                    allData["attachment"] = True
                elif allData["attachment"] == "no":
                    allData["attachment"] = False
                else:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": f"Attchment Accepts only Yes or No",
                        }
                    )

            lookData = {"_id": ObjectId(id)}
            
            try:
                arr=[
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
                Responsed=cmo.finding_aggregate("claimType",arr)['data']
                allData2=Responsed[0]
                changedThings=changedThings2(allData,allData2)
                print('changedThingschangedThingschangedThings',changedThings)
                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added=changedThings+" updated in this Claim Type "+allData['claimType']
                cmo.insertion("AdminLogs",{'type':'Update','module':'Claim Type','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            except Exception as e:
                print(e,'dhhdhdhhdh')
            
            
            
            
            
            response = cmo.updating("claimType", lookData, allData, False)
            cmo.updating_m(
                "mergerDesignationClaim",
                {"claimTypeId": ObjectId(id)},
                {"name": claimType},
                False,
            )
            return respond(response)
    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("claimType", id)
            return respond(response)
        else:
            return jsonify({"msg": "Please provide valid Unique Id"})



@expenses_blueprint.route("/expenses/expensesClaimType",methods=['GET','POST'])
@token_required
def expensesClaimTypes(current_user):
    if request.method == "GET":
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'categoriesType': {
                        '$ne': 'Advance'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'claimType': 1, 
                    'claimTypeId': {
                        '$toString': '$_id'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }
        ]
        Response=cmo.finding_aggregate("claimType",arr)
        return respond(Response)


@expenses_blueprint.route("/expenses/AdvanceClaimType",methods=['GET','POST'])
@token_required
def AdvanceClaimTypes(current_user):
    if request.method == "GET":
        arr=[
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'categoriesType': {
                        '$eq': 'Advance'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'claimType': 1, 
                    'claimTypeId': {
                        '$toString': '$_id'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }
                }
            }
        ]
        Response=cmo.finding_aggregate("claimType",arr)
        return respond(Response)


@expenses_blueprint.route("/expenses/unitRate", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@expenses_blueprint.route("/expenses/unitRate/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def manageExpenseunitRate(current_user, id=None):
    if request.method == "GET":
        arra = [
            {"$addFields": {"uniqueId": {"$toString": "$_id"}}},
            {"$project": {"_id": 0}},
        ]
        response = cmo.finding_aggregate("unitRate", arra)
        return respond(response)
    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            arra = [
                {
                    "$match": {
                        "categories": allData["categories"],
                        "deleteStatus": {"$ne": 1},
                    }
                }
            ]
            response = cmo.finding_aggregate("unitRate", arra)
            if len(response["data"]):
                return respond(
                    {
                        "status": 400,
                        "msg": "This category is Already Exist",
                        "icon": "error",
                    }
                )
            if "unitRate" in allData:
                if allData["unitRate"] != None or allData["unitRate"] != "undefined":
                    allData["unitRate"] = int(allData["unitRate"])
            Added="The Unit rate for this category "+" "+allData['categories'] +" Added"
            cmo.insertion("AdminLogs",{'type':'Add','module':'Unit Rate','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            response = cmo.insertion("unitRate", allData)
            return respond(response)
        elif id != None:
            allData = request.get_json()
            lookData = {"_id": ObjectId(id)}
            if "unitRate" in allData:
                if allData["unitRate"] != None or allData["unitRate"] != "undefined":
                    allData["unitRate"] = int(allData["unitRate"])
                    
                    
            try:
                arr=[
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
                            'uniqueId':0
                        }
                    }
                ]
                Responsed=cmo.finding_aggregate("unitRate",arr)['data']
                allData2=Responsed[0]
                changedThings=changedThings2(allData,allData2)
                print('changedThingschangedThingschangedThings',changedThings)
                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added=changedThings+" updated for this "+allData['categories'] +"unit Rate"
                cmo.insertion("AdminLogs",{'type':'Update','module':'Unit Rate','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            except Exception as e:
                print(e,'dhhdhdhhdh')
            response = cmo.updating("unitRate", lookData, allData, False)
            return respond(response)
    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("unitRate", id)
            return respond(response)
        else:
            return jsonify({"msg": "Please provide valid Unique Id"})


@expenses_blueprint.route(
    "/expenses/unitRateClaimType", methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
)
@expenses_blueprint.route(
    "/expenses/unitRateClaimType/<id>",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
@token_required
def manageExpenseunitRateClaimType(current_user, id=None):
    if request.method == "GET":
        claimTypeId = request.args.get("claimType")
        if claimTypeId != None:
            arra = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(claimTypeId)}},
                {"$addFields": {"claimTypeId": {"$toString": "$_id"}}},
                {"$project": {"_id": 0}},
            ]
            response = cmo.finding_aggregate("claimType", arra)
            return respond(response)
        else:
            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "categories": {"$ne": ""}}},
                {"$addFields": {"claimTypeId": {"$toString": "$_id"}}},
                {"$project": {"_id": 0}},
            ]
            response = cmo.finding_aggregate("claimType", arr)
            return respond(response)


@expenses_blueprint.route("/expenses/projectDetails", methods=["GET"])
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


@expenses_blueprint.route("/expenses/projectSite", methods=["GET"])
@expenses_blueprint.route("/expenses/projectSite/<projectUniqueId>", methods=["GET"])
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
                        {"$addFields": {"Site Id": {"$toString": "$Site Id"}}},
                        {"$project": {"_id": 0, "Site Id": 1, "systemId": 1}},
                    ],
                    "as": "siteResult",
                }
            },
            {"$match": {"siteResult": {"$exists": True, "$gt": {"$size": 0}}}},
            {"$unwind": {"path": "$siteResult", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "Site Id": {
                        "$concat": [
                            "$siteResult.Site Id",
                            "(",
                            "$siteResult.systemId",
                            ")",
                        ]
                    },
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


@expenses_blueprint.route("/expenses/projectSiteTask", methods=["GET"])
@expenses_blueprint.route("/expenses/projectSiteTask/<siteUniqueId>", methods=["GET"])
@token_required
def expensesprojectSiteTask(current_user, siteUniqueId=None):
    siteUniqueId = request.args.get("siteId")
    if siteUniqueId != None or siteUniqueId != "undefined":
        arr = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "siteId": ObjectId(siteUniqueId),
                    "assignerId": {"$in": [ObjectId(current_user["userUniqueId"])]},
                    "CC_Completion Date": {"$gte": str(three_days_ago())},
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
                    "CC_Completion Date": {"$gte": str(three_days_ago())},
                }
            },
            {"$project": {"taskUniqueId": {"$toString": "$_id"}, "Name": 1, "_id": 0}},
        ]
        Response = cmo.finding_aggregate("milestone", arr)
        return respond(Response)


@expenses_blueprint.route("/claimType", methods=["GET", "POST"])
@expenses_blueprint.route("/claimType/<claimTypeId>", methods=["GET", "POST"])
def claimType(claimTypeId=None):
    if request.method == "GET":
        if claimTypeId != None or claimTypeId != "undefined":
            arr = [
                {
                    "$addFields": {
                        "uniqueId": {"$toString": "$_id"},
                        "attachment": {
                            "$cond": {
                                "if": {"$eq": ["$attachment", True]},
                                "then": "Yes",
                                "else": "No",
                            }
                        },
                    }
                },
                {"$project": {"_id": 0}},
            ]
            Response = cmo.finding_aggregate("claimType", arr)
            return respond(Response)
        else:
            arr = [
                {"$match": {"_id": ObjectId(claimTypeId)}},
                {
                    "$addFields": {
                        "uniqueId": {"$toString": "$_id"},
                        "attachment": {
                            "$cond": {
                                "if": {"$eq": ["$attachment", True]},
                                "then": "Yes",
                                "else": "No",
                            }
                        },
                    }
                },
                {"$project": {"_id": 0}},
            ]
            Response = cmo.finding_aggregate("claimType", arr)
            return respond(Response)


@expenses_blueprint.route("/expenses/claimTypeRole", methods=["GET", "POST"])
@token_required
def claimTypeRole(current_user):
    claimTypeId = request.args.get("categories")
    claimtypeDa = request.args.get("claimtypeDa")
    if request.method == "GET":
        if claimTypeId != None and claimTypeId != "undefined":
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
        elif claimtypeDa != None and claimtypeDa != "undefined":
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        "designation": current_user["designation"],
                        "value": {"$ne": False},
                    }
                },
                {
                    "$lookup": {
                        "from": "claimType",
                        "localField": "claimTypeId",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "categoriesType": "Daily Allowance",
                                }
                            }
                        ],
                        "as": "claimResult",
                    }
                },
                {
                    "$unwind": {
                        "path": "$claimResult",
                        "preserveNullAndEmptyArrays": False,
                    }
                },
                {
                    "$addFields": {
                        "DesignationClaimid": {"$toString": "$DesignationClaimid"},
                        "claimTypeId": {"$toString": "$claimTypeId"},
                        "mergerDesignationClaimId": {"$toString": "$_id"},
                    }
                },
                {"$group": {"_id": "$claimTypeId", "doc": {"$first": "$$ROOT"}}},
                {"$replaceRoot": {"newRoot": "$doc"}},
                {"$project": {"_id": 0, "claimResult": 0}},
            ]

            Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
            return respond(Response)
        else:
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        "designation": current_user["designation"],
                        "value": {"$ne": False},
                    }
                },
                {
                    "$lookup": {
                        "from": "claimType",
                        "localField": "claimTypeId",
                        "foreignField": "_id",
                        "pipeline": [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "categoriesType": "Expense",
                                }
                            }
                        ],
                        "as": "claimTypeResult",
                    }
                },
                {
                    "$unwind": {
                        "path": "$claimTypeResult",
                        "preserveNullAndEmptyArrays": False,
                    }
                },
                {
                    "$addFields": {
                        "DesignationClaimid": {"$toString": "$DesignationClaimid"},
                        "claimTypeId": {"$toString": "$claimTypeId"},
                        "mergerDesignationClaimId": {"$toString": "$_id"},
                        "categories": "$claimTypeResult.categories",
                    }
                },
                {"$group": {"_id": "$claimTypeId", "doc": {"$first": "$$ROOT"}}},
                {"$replaceRoot": {"newRoot": "$doc"}},
                {"$project": {"_id": 0, "claimTypeResult": 0}},
            ]

            Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
            # print("ResponseResponseResponsepp", Response)
            return respond(Response)


@expenses_blueprint.route(
    "/expenses/ClaimTypeDesignation", methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
)
@expenses_blueprint.route(
    "/expenses/ClaimTypeDesignation/<id>",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
@token_required
def manageExpenseClaimTypeDesignation(current_user, id=None):
    claimType = request.args.get("claimTypeId")
    # print("dfghjklkjgg", claimType)
    if request.method == "GET":
        arr = []
        if claimType != None and claimType != "undefined":
            arr = arr + [{"$match": {"_id": ObjectId(claimType)}}]
        arra = arr + [
            {
                "$addFields": {
                    "uniqueId": {"$toString": "$_id"},
                    "designation": {"$toObjectId": "$Designation"},
                }
            },
            {
                "$lookup": {
                    "from": "designation",
                    "localField": "designation",
                    "foreignField": "_id",
                    "pipeline": [{"$project": {"_id": 0}}],
                    "as": "designation",
                }
            },
            {"$unwind": {"path": "$designation", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "designation": "$designation.designation",
                    "taskName": {
                        "$cond": {
                            "if": {"$eq": ["$taskName", True]},
                            "then": "Yes",
                            "else": "No",
                        }
                    },
                    "siteId": {
                        "$cond": {
                            "if": {"$eq": ["$siteId", True]},
                            "then": "Yes",
                            "else": "No",
                        }
                    },
                }
            },
            {"$project": {"_id": 0}},
        ]

        response = cmo.finding_aggregate("DesignationclaimType", arra)

        # if len(response['data']):
        #     for key,value in response['data'][0].items():
        #         if value == True:
        #             response['data'][0][key] = 'Yes'
        #         if value == False:
        #             response['data'][0][key] = 'Yes'
        if len(response["data"]) > 0:
            for item in response["data"]:
                for key, value in item.items():
                    if value is True:
                        item[key] = "Yes"
                    elif value is False:
                        item[key] = "No"

        return respond(response)
    if request.method == "POST":
        if id == None:
            modifiedData = {}
            allData = request.get_json()

            if allData["Designation"] != None:
                arrt = [
                    {
                        "$match": {
                            "Designation": allData["Designation"],
                            "deleteStatus": {"$ne": 1},
                        }
                    },
                    {"$project": {"_id": {"$toString": "$_id"}}},
                ]
                datsr = cmo.finding_aggregate("DesignationclaimType", arrt)["data"]
                if len(datsr):
                    return respond(
                        {
                            "status": 400,
                            "msg": "This category is Already Exist",
                            "icon": "error",
                        }
                    )
            alldata2 = {}
            alldata2["Designation"] = allData["Designation"]
            if "siteId" in allData:
                if allData["siteId"] == "Yes":
                    allData["siteId"] = True
                    alldata2["siteId"] = True
                if allData["siteId"] == "No":
                    allData["siteId"] = False
                    alldata2["siteId"] = False
            if "taskName" in allData:
                if allData["taskName"] == "Yes":
                    allData["taskName"] = True
                    alldata2["taskName"] = True
                if allData["taskName"] == "No":
                    allData["taskName"] = False
                    alldata2["taskName"] = False
            for key, value in allData.items():
                if "__" in key:
                    field_name, field_id = key.split("__")
                    modifiedData[field_name] = {"id": field_id, "value": value}
                    alldata2[field_name] = value
                # if "__" not in key:
                #     print('pppppp',key,value)
                #     modifiedData[field_name]=value
                if value == "":
                    modifiedData[key] = True
                else:
                    modifiedData[key] = value

            for key, value in alldata2.items():
                if value == "" or value == None:
                    alldata2[key] = True
                if type(value) != bool:
                    if value.lower() == "no":
                        alldata2[key] = False
                    if value.lower() == "yes":
                        alldata2[key] = True

            arrpt=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        '_id': ObjectId(alldata2['Designation'])
                    }
                }, {
                    '$project': {
                        'designation': 1, 
                        '_id': 0
                    }
                }
            ]
            Responsed=cmo.finding_aggregate("designation",arrpt)['data']
            
            
            Added="The CLaim Type Grade"+" "+Responsed[0]['designation'] +" Added"
            cmo.insertion("AdminLogs",{'type':'Add','module':'CLaim Type Grade','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            
            
            response = cmo.insertion("DesignationclaimType", alldata2)
            uId = response["operation_id"]
            yyy = []
            setNewData = []
            for key, value in modifiedData.items():
                pp = {}
                if isinstance(value, dict):
                    pp["name"] = key
                    pp["DesignationClaimid"] = ObjectId(uId)
                    pp["claimTypeId"] = ObjectId(value["id"])
                    pp["designation"] = allData["Designation"]
                    # print(yyy.append(value["value"]))
                    # print(value["value"], 'value["value"]value["value"]value["value"]')
                    if value["value"] == "" or value["value"] == None:
                        pp["value"] = True
                    elif value["value"].lower() == "yes":
                        pp["value"] = True
                    elif value["value"].lower() == "no":
                        pp["value"] = False
                    else:
                        pp["value"] = value["value"]
                    setNewData.append(pp)

            cmo.insertion_all("mergerDesignationClaim", setNewData)
            return respond(response)
        if id != None:
            allData = request.get_json()
            print("allDataallData",allData)
            
            if "siteId" in allData:
                if allData["siteId"] == "Yes":
                    allData["siteId"] = True
                elif allData["siteId"] == "No":
                    allData["siteId"] = False
            if "taskName" in allData:
                if allData["taskName"] == "Yes":
                    allData["taskName"] = True
                elif allData["taskName"] == "No":
                    allData["taskName"] = False
            for i in list(allData.keys()):

                if isinstance(allData[i], str):

                    if allData[i].lower() == "yes" or allData[i] == "":
                        allData[i] = True
                    elif allData[i].lower() == "no":
                        allData[i] = False
            keys_to_delete = []

            for key in list(allData.keys()):
                if "__" in key:
                    base_key = key.split("__")[0]
                    if base_key in allData:
                        allData[base_key] = allData[key]
                    else:
                        allData[base_key] = allData[key]
                    keys_to_delete.append(key)
            for key in keys_to_delete:
                del allData[key]

            for key, value in allData.items():
                if value == "" or value == None:
                    allData[key] = True
                if type(value) != bool:
                    if value.lower() == "no":
                        allData[key] = False
                    if value.lower() == "yes":
                        allData[key] = True

            lookData = {"_id": ObjectId(id)}
            
            
            try:
                # print('hbhhhh',allData['Designation'])  
                arrpt=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': ObjectId(allData['Designation'])
                        }
                    }, {
                        '$project': {
                            'designation': 1, 
                            '_id': 0
                        }
                    }
                ]
                Responsedd=cmo.finding_aggregate("designation",arrpt)['data']
                
                # print('hbhhhyyyh',Responsedd)  
                
                
                arr=[
                        {
                            '$match': {
                                '_id': ObjectId(id), 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'designation': 0, 
                                'uniqueId': 0, 
                                
                                '_id': 0
                            }
                        }
                    ]
                Responsed=cmo.finding_aggregate("DesignationclaimType",arr)['data']
                allData2=Responsed[0]
                changedThings=changedThings2(allData,allData2)
                # print('changedThingschangedThingschangedThings',changedThings)
                # Added="The circle Name "+allData['circleName'] +' with circle Code '+allData['circleCode']+' and band '+Band+' added in '+customere+' Customer '
                Added=changedThings+" updated in this Claim Type  Grade changed for this grade"+Responsedd[0]['designation']
                cmo.insertion("AdminLogs",{'type':'Update','module':'Claim Type  Grade','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            except Exception as e:
                print(e,'dhhdhdhhdh')
            
            
            
            
            
            response = cmo.updating("DesignationclaimType", lookData, allData, False)
            ppp = ["siteId", "taskName", "uniqueId", "Designation", "designation"]

            #####logic for update data in mergerDesignationClaim
            result = []

            for key in allData:

                if key not in ppp:
                    result.append({"name": key, "value": allData[key]})

            for i in result:
                arrpp = [
                    {"$match": {"claimType": i["name"]}},
                    {"$project": {"_id": {"$toString": "$_id"}}},
                ]
                Responsess = cmo.finding_aggregate("claimType", arrpp)["data"]
                # print("fghjklkjhgjk", Responsess)
                if len(Responsess):
                    claimTypeId = Responsess[0]["_id"]
                else:
                    claimTypeId = None
                lookupdata = {
                    "name": i["name"],
                    "DesignationClaimid": ObjectId(id),
                }
                updateData = {
                    "value": i["value"],
                    "designation": allData["Designation"],
                    "claimTypeId": ObjectId(claimTypeId),
                }

                cmo.updating("mergerDesignationClaim", lookupdata, updateData, True)

            return respond(response)
    elif request.method == "DELETE":
        if id != None and id != "undefined":
            arr=[
                        {
                            '$match': {
                                '_id': ObjectId(id), 
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'Designation': 1, 
                                
                                
                                '_id': 0
                            }
                        }
                    ]
            Responsed=cmo.finding_aggregate("DesignationclaimType",arr)['data']
            designations=Responsed[0]['Designation']
            arrpt=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            },
                            '_id': ObjectId(designations)
                        }
                    }, {
                        '$project': {
                            'designation': 1, 
                            '_id': 0
                        }
                    }
                ]
            Responsedd=cmo.finding_aggregate("designation",arrpt)['data']
            
            Circle=None
            
            if len(Responsedd):
                Circle=Responsedd[0]['designation']
                Added="Deleted claim type for this grade "+Circle
                cmo.insertion("AdminLogs",{'type':'Delete','module':'Claim Type Grade','actionAt':current_time(),'actionBy':ObjectId(current_user['userUniqueId']),'action':Added})
            
            
            response = cmo.real_deleting("DesignationclaimType", {"_id": ObjectId(id)})
            response = cmo.real_deleting(
                "mergerDesignationClaim", {"DesignationClaimid": ObjectId(id)}
            )
            return respond(response)
        else:
            return jsonify({"msg": "Please provide valid Unique Id"})


@expenses_blueprint.route("/expenses/fillExpense", methods=["GET", "POST", "DELETE"])
@expenses_blueprint.route("/expenses/fillExpense/<id>", methods=["GET", "POST", "DELETE"])
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
                                "costcenter": "$projectGroupResult.costcenter",
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
                    'customerName': '$projectResult.customerName',
                    "Claim_Date": {"$toDate": "$addedAt"},
                    "empCode": "$userInfo.empCode",
                    "empName": "$userInfo.empName",
                    "mobile": "$userInfo.mobile",
                    "Site_Id": "$siteResults.Site Id",
                    "Task": "$milestoneresult.Name",
                    "claimType": "$claimTyperesult.claimType",
                    "projectId": {"$toString": "$projectId"},
                    # "expenseDate": {"$toDate": "$expenseDate"},
                    "expenseDate": {
                        "$cond": {
                            "if": {
                                "$or": [
                                    {"$eq": ["$expenseDate", ""]},
                                    {
                                        "$regexMatch": {
                                            "input": "$expenseDate",
                                            "regex": re.compile(
                                                r"^\\d{2}:\d{2}:\d{4}$"
                                            ),
                                        }
                                    },
                                    {
                                        "$regexMatch": {
                                            "input": "$expenseDate",
                                            "regex": re.compile(
                                                r"^\\d{4}:\d{2}:\d{2}$"
                                            ),
                                        }
                                    },
                                    {
                                        "$regexMatch": {
                                            "input": "$expenseDate",
                                            "regex": re.compile(
                                                r"^\\d{1}:\d{2}:\d{4}$"
                                            ),
                                        }
                                    },
                                ]
                            },
                            "then": None,
                            "else": {"$toDate": "$expenseDate"},
                        }
                    },
                    "claimTypeId": {"$toString": "$claimTyperesult._id"},
                }
            },
            {
                "$addFields": {
                    "SubmissionDate": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$Claim_Date",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "expenseDate": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$expenseDate",
                            "timezone": "Asia/Kolkata",
                        }
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
                Amount = float(Amount)
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
                # print("jjjjd", arr)
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
            if (siteandTaskmatchingData["Amount"] is None and siteandTaskmatchingData["Total_distance"] is None):
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
                                    if float(Amount) > int(hotelTotalValue):
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
                                    if float(Amount) > int(dataTomatch["value"]):
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
                    ExpenseNo = ExpenseNonewLogic()
                else:
                    return respond(
                        {"icon": "error", "status": 400, "msg": "Claim Type not Found!"}
                    )
            financialyear = None
            varible = None
            varibleInt = None
            ggg = ExpenseNo.split("/")
            financialyear = ExpenseNo[3:7]
            varible = ExpenseNo[7:]
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
                "Amount": float(Amount),
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
            Response = cmo.insertion("Expenses", datatoinsert)
            mailAggregation = [
                {
                    '$match': {
                        '_id': ObjectId(current_user["userUniqueId"])
                    }
                }, {
                    '$project': {
                        'empName': 1, 
                        'ustCode': {
                            '$toString': '$ustCode'
                        }, 
                        'L1Approver': {
                            '$toObjectId': '$L1Approver'
                        }, 
                        '_id': 0
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'L1Approver', 
                        'foreignField': '_id', 
                        'as': 'result'
                    }
                }, {
                    '$addFields': {
                        'l1Name': {
                            '$arrayElemAt': [
                                '$result.empName', 0
                            ]
                        }, 
                        'liEmail': {
                            '$arrayElemAt': [
                                '$result.email', 0
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'L1Approver': 0, 
                        'result': 0
                    }
                }
            ]
            user = cmo.finding_aggregate("userRegister",mailAggregation)['data']
            try:
                user = user[0]
                userName = user['empName']
                userCode = user['ustCode']
                approverName = user['l1Name']
                approverEmail = user['liEmail'].strip()
                amount = str(Amount)
                cmailer.formatted_sendmail(to=[approverEmail],cc=[],subject=ExpenseNo,message=cmt.expense_mail(userName,userCode,approverName,ExpenseNo,amount),type="L1")
            except Exception as e:
                pass
            
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
                                            if float(Amount) > int(hotelTotalValue):
                                                return respond(
                                                    {
                                                        "status": 400,
                                                        "icon": "error",
                                                        "msg": f"You can fill amount only equal to or less than {hotelTotalValue} not for this {Amount}",
                                                    }
                                                )
                                    else:
                                        if float(Amount) > int(dataTomatch["value"]):
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
                        "Amount": float(Amount),
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


@expenses_blueprint.route("/expenses/fillAdvance", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@expenses_blueprint.route("/expenses/fillAdvance/<id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@token_required
def expensesBlueprint(current_user, id=None):
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
                                "costcenter": "$projectGroupResult.costcenter",
                                'customerName': {
                      '$toObjectId': "$custId"
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
                    'customerName':"$projectResult.customerName",
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
                
                amount = float(amount)
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
                    if float(amount) > int(Responsess[0]["value"]):
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
            print('arrppparrppparrppparrppp',arrppp)
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
                Responsess = cmo.finding_aggregate("mergerDesignationClaim", arr)["data"]
                if len(Responsess):
                    print("ResponsessResponsess", Responsess, dynamicCheckingAmount)
                    dynamicCheckingAmount = dynamicCheckingAmount + int(
                        Responsess[0]["value"]
                    )
                if is_valid_mongodb_objectid(projectId):
                    projectId = ObjectId(projectId)
                data = {
                    "projectId": projectId,
                    # "advanceType": advanceType,
                    "advanceTypeId": ObjectId(advanceTypeId),
                    "Amount": float(amount),
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
                    print('datattadatattadatatta',datatta)
                    approvedAdvances = 0
                    claimedAdvances = int(datatta[0]["AdvanceAmountTotal"])
                    approvedAdvances = int(datatta[0]["AdvanceAmountTotalApproved"])
                    ApprovedExpenses = int(datatta[0]["ExpenseAmountTotalApproved"])
                    totallimit = int(dynamicCheckingAmount)
                    x = totallimit - (
                        (claimedAdvances + approvedAdvances) - ApprovedExpenses
                    )
                    # print('hhjjjj',claimedAdvances,ApprovedExpenses,totallimit)
                    if (int(datatta[0]["checkingAmount"]) + int(data["Amount"]) > dynamicCheckingAmount):
                        print('jjkkkk',int(datatta[0]["checkingAmount"]) + int(data["Amount"]),dynamicCheckingAmount)
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"You can fill only upto{x}",
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
                    Amount = float(Amount)
                    if Amount < 0:
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
                                        '_id': {
                                            '$ne': ObjectId('671b4e9bfec89926f265b8d2')
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
                print('arrppparrppparrppparrppp',arrppp)
                datatta = cmo.finding_aggregate("userRegister", arrppp)["data"]
                
                if len(datatta):
                    if int(datatta[0]["checkingAmount"]) + int(setData["Amount"]) > int(totalAdvancelimit):
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


@expenses_blueprint.route("/expenses/fillDAEmpData", methods=["GET", "POST"])
@expenses_blueprint.route("/expenses/fillDAEmpData/<id>", methods=["GET", "POST"])
@token_required
def expensesfillDAEmpData(current_user, id=None):
    if request.method == "GET":
        arr = [
            {
                "$match": {
                    "L1Approver": current_user["userUniqueId"],
                    "status": "Active",
                }
            },
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


@expenses_blueprint.route("/expenses/fillDAEmpName", methods=["GET", "POST"])
@expenses_blueprint.route("/expenses/fillDAEmpName/<id>", methods=["GET", "POST"])
@token_required
def expensesfillDAEmpName(current_user, id=None):
    EmpCode = request.args.get("empCode")
    # print("EmpCodeEmpCode", EmpCode)
    errors = [None, "undefined", ""]
    if request.method == "GET":
        if EmpCode not in errors:
            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(EmpCode)}},
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
        else:
            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": f"Employee Code not found",
                }
            )


@expenses_blueprint.route(
    "/expenses/DAFillcostCenter", methods=["GET", "POST", "PATCH", "PUT", "DELETE"]
)
@expenses_blueprint.route(
    "/expenses/DAFillcostCenter/<id>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"]
)
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


@expenses_blueprint.route(
    "/expenses/DAFillProjectId", methods=["GET", "POST", "PATCH", "PUT", "DELETE"]
)
@expenses_blueprint.route(
    "/expenses/DAFillProjectId/<id>", methods=["GET", "POST", "PATCH", "PUT", "DELETE"]
)
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


@expenses_blueprint.route("/expenses/fillDA", methods=["GET", "POST","DELETE"])
@expenses_blueprint.route("/expenses/fillDA/<id>", methods=["GET", "POST","DELETE"])
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
                                'customerName': {
                      '$toObjectId': "$custId"
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
                    "customerName": "$projectResult.customerName",
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
        Response = cmo.finding_aggregate("Expenses", arr)
        return respond(Response)
    

    if request.method == "POST":
        if id == None:
            data = request.get_json()
            claimType = data["claimType"]
            Amount = data["Amount"]
            projectId = data["projectId"]
            if data['totalDays'] in ["","undefined",None]:
                data['totalDays'] = 1

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

                if Amount != None or Amount != "undefined":
                    if float(Amount) > (int(dataTomatch["value"])*data['totalDays']):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"You can fill amount only equal to or less than {int(dataTomatch["value"])*data['totalDays']} not for this {Amount}",
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
                financialyear = ExpenseNo[3:7]
                varible = ExpenseNo[7:]
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
                'ApprovedAmount':float(Amount),
                "totalDays":data['totalDays'],
                "startAt":data['startAt'],
                "endAt":data['endAt']
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
                'Amount':float(Amount),
                "totalDays":data['totalDays'],
                "startAt":data['startAt'],
                "endAt":data['endAt']
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
            datatoAproveris = cmo.insertion("Approval", datatoinsert2)
            return respond(Response)
        
        if id != None:
            data = request.get_json()
            claimType = data["claimType"]
            Amount = data["Amount"]
            projectId = data["projectId"]
            if data['totalDays'] in ["","undefined",None]:
                data['totalDays'] = 1
            siteandTaskmatchingData = {
                "Amount": Amount,
            }
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
                    if float(Amount) > (int(dataTomatch["value"])*data['totalDays']):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"You can fill amount only equal to or less than {int(dataTomatch["value"])*data['totalDays']} not for this {Amount}",
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
                "totalDays":data['totalDays'],
                "startAt":data['startAt'],
                "endAt":data['endAt']
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
                "totalDays":data['totalDays'],
                "startAt":data['startAt'],
                "endAt":data['endAt']
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




@expenses_blueprint.route("/expenses/ExpenseNo", methods=["GET", "POST", "PUT", "DELETE"])
@expenses_blueprint.route("/expenses/ExpenseNo/<id>", methods=["GET", "POST", "PUT", "DELETE"])
@token_required
def getExpenseByExpenseNo(current_user, id=None):
    ExpenseNo = request.args.get("ExpenseNo")
    if request.method == "GET":
        if ExpenseNo != None:
            arr =  [
            {
                    "$match": {
                        "addedFor": ObjectId(current_user["userUniqueId"]),
                        "deleteStatus": {"$ne": 1},
                        "ExpenseNo": ExpenseNo,
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
        '$addFields': {
            'checkInDate': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$eq': [
                                    '$checkInDate', ''
                                ]
                            }, {
                                '$eq': [
                                    '$checkInDate', 0
                                ]
                            }, {
                                '$eq': [
                                    '$checkInDate', '0'
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': '$checkOutDate'
                }
            }, 
            'checkOutDate': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$eq': [
                                    '$checkOutDate', ''
                                ]
                            }, {
                                '$eq': [
                                    '$checkOutDate', 0
                                ]
                            }, {
                                '$eq': [
                                    '$checkOutDate', '0'
                                ]
                            }
                        ]
                    }, 
                    'then': None, 
                    'else': '$checkOutDate'
                }
            }
        }
    }, {
        '$addFields': {
            'projectIdName': '$projectResult.projectId', 
            'costcenter': '$projectResult.costcenter', 
            'Claim_Date': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$Claim_Date', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'projectId': {
                '$toString': '$projectId'
            }, 
            'empCode': '$userInfo.empCode', 
            'empName': '$userInfo.empName', 
            'mobile': '$userInfo.mobile', 
            'Site_Id': '$siteResults.Site Id', 
            'Task': '$milestoneresult.Name', 
            'ClaimType': '$claimTyperesult.claimType', 
            'expenseDate': {
                '$dateToString': {
                    'date': {
                        '$toDate': '$expenseDate'
                    }, 
                    'format': '%d-%m-%Y', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'checkOutDate': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$ne': [
                                    '$checkOutDate', ''
                                ]
                            }, {
                                '$ne': [
                                    '$checkOutDate', None
                                ]
                            }, {
                                '$regexMatch': {
                                    'input': '$checkOutDate', 
                                    'regex': '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}$'
                                }
                            }
                        ]
                    }, 
                    'then': {
                        '$toDate': '$checkOutDate'
                    }, 
                    'else': None
                }
            }, 
            'checkInDate': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$ne': [
                                    '$checkInDate', ''
                                ]
                            }, {
                                '$ne': [
                                    '$checkInDate', None
                                ]
                            }, {
                                '$regexMatch': {
                                    'input': '$checkInDate', 
                                    'regex': '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}$'
                                }
                            }
                        ]
                    }, 
                    'then': {
                        '$toDate': '$checkInDate'
                    }, 
                    'else': None
                }
            }
        }
    }, {
        '$addFields': {
            'SubmissionDate': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$Claim_Date', 
                    'timezone': 'Asia/Kolkata'
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
            'claimType': '$ClaimType', 
            'checkInDate': {
                '$dateToString': {
                    'date': {
                        '$toDate': '$checkInDate'
                    }, 
                    'format': '%d-%m-%Y', 
                    'timezone': 'Asia/Kolkata'
                }
            }, 
            'checkOutDate': {
                '$dateToString': {
                    'date': {
                        '$toDate': '$checkOutDate'
                    }, 
                    'format': '%d-%m-%Y', 
                    'timezone': 'Asia/Kolkata'
                }
            }
        }
    }, {
        '$project': {
            'Site Id': 0, 
            'taskName': 0, 
            'milestoneresult': 0, 
            'siteResults': 0, 
            'userInfo': 0, 
            'projectResult': 0, 
            '_id': 0, 
            'claimTyperesult': 0, 
            'ExpenseUniqueId': 0, 
            'actionBy': 0, 
            'L1Approver': 0, 
            'L2Approver': 0, 
            'L3Approver': 0
        }
    }
]

            Response = cmo.finding_aggregate("Expenses", arr)
            return respond(Response)
        else:
            return jsonify({"msg": "ID Not Found"})


@expenses_blueprint.route("/export/ExpensesAndAdvance", methods=["GET", "POST"])
@expenses_blueprint.route("/export/ExpensesAndAdvance/<id>", methods=["GET", "POST"])
@token_required
def exportExpensesAndAdvance(current_user, id=None):
    if request.method == "GET":
        arr =[
            {
                                '$match': {
                                    '_id': ObjectId(current_user["userUniqueId"])
                                }
                            }, {
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
                                            'customerName': {
                                                '$toObjectId': '$custId'
                                            }, 
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
                                        '$lookup': {
                                            'from': 'customer', 
                                            'localField': 'customerName', 
                                            'foreignField': '_id', 
                                            'as': 'customerName'
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
                    'customerName': '$newData.customerName', 
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
                    }, 
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
                '$project': {
                    'Expanse/Advance/Settlement ID': '$name', 
                    'Cost Center': '$costCenter', 
                    'Customer': '$customerName', 
                    'Claim Date': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$type', 'Advance'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$type', 'Settlement'
                                        ]
                                    }
                                ]
                            }, 
                            'then': '$AddedAt', 
                            'else': '$submissionDate'
                        }
                    }, 
                    'Submission Date': '$AddedAt', 
                    'Last Action Date': '$actionAt', 
                    'Debit(Advance)': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$type', 'Advance'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$type', 'Settlement'
                                        ]
                                    }
                                ]
                            }, 
                            'then': '$totalApprovedAmountRow', 
                            'else': 0
                        }
                    }, 
                    'Credit (Expanse)': {
                        '$cond': {
                            'if': {
                                '$and': [
                                    {
                                        '$ne': [
                                            '$type', 'Advance'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$type', 'Settlement'
                                        ]
                                    }
                                ]
                            }, 
                            'then': '$totalApprovedAmountRow', 
                            'else': 0
                        }
                    }, 
                    'Status': '$customStatus', 
                    '_id': 0
                }
            }
            ]
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


@expenses_blueprint.route("/expenses/DownloadAttachment", methods=["GET", "POST"])
@expenses_blueprint.route("/expenses/DownloadAttachment/<id>", methods=["GET", "POST"])
@token_required
def DownloadAttachment(id=None):
    expenseId = request.args.get("expenseId")
    if request.method == "GET":
        if expenseId != None:
            if expenseId.startswith("EXP"):
                arr = [
                    {"$match": {"ExpenseNo": expenseId}},
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
                        "$unwind": {
                            "path": "$siteResults",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
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
                            "Site_Id": "$siteResults.Site Id",
                            "Task": "$milestoneresult.Name",
                            "ClaimType": "$claimTyperesult.claimType",
                            # "expenseDate": {
                            #     "$cond": {
                            #         "if": {
                            #             "$or": [
                            #                 {"$eq": ["$expenseDate", None]},
                            #                 {"$eq": ["$expenseDate", ""]},
                            #             ]
                            #         },
                            #         "then": None,
                            #         "else": {"$toDate": "$expenseDate"},
                            #     }
                            # },
                            "expenseDate": {
                                "$cond": {
                                    "if": {
                                        "$or": [
                                            {"$eq": ["$expenseDate", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": "$expenseDate",
                                                    "regex": re.compile(
                                                        r"^\\d{2}:\d{2}:\d{4}$"
                                                    ),
                                                }
                                            },
                                            {
                                                "$regexMatch": {
                                                    "input": "$expenseDate",
                                                    "regex": re.compile(
                                                        r"^\\d{4}:\d{2}:\d{2}$"
                                                    ),
                                                }
                                            },
                                            {
                                                "$regexMatch": {
                                                    "input": "$expenseDate",
                                                    "regex": re.compile(
                                                        r"^\\d{1}:\d{2}:\d{4}$"
                                                    ),
                                                }
                                            },
                                        ]
                                    },
                                    "then": None,
                                    "else": {"$toDate": "$expenseDate"},
                                }
                            },
                            "actionAt": {"$toDate": "$actionAt"},
                        }
                    },
                    {
                        "$addFields": {
                            "claimType": "$ClaimType",
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
                            "expenseDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$expenseDate",
                                    "timezone": "Asia/Kolkata",
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
                            "L1ApproverDate": {
                                "$arrayElemAt": ["$L1Approver.actionAt", 0]
                            },
                            "L2ApproverDate": {
                                "$arrayElemAt": ["$L2Approver.actionAt", 0]
                            },
                            "L3ApproverDate": {
                                "$arrayElemAt": ["$L3Approver.actionAt", 0]
                            },
                        }
                    },
                    {
                        "$addFields": {
                            "L1ApproverDate": {"$toDate": "$L1ApproverDate"},
                            "L2ApproverDate": {"$toDate": "$L2ApproverDate"},
                            "L3ApproverDate": {"$toDate": "$L3ApproverDate"},
                        }
                    },
                    {
                        "$addFields": {
                            "L1ApproverDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$L1ApproverDate",
                                    "timezone": "Asia/Kolkata",
                                }
                            },
                            "L2ApproverDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$L2ApproverDate",
                                    "timezone": "Asia/Kolkata",
                                }
                            },
                            "L3ApproverDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$L3ApproverDate",
                                    "timezone": "Asia/Kolkata",
                                }
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
                            "Expense No": "$ExpenseNo",
                            "claimType": "$ClaimType",
                            "Expense Date": "$expenseDate",
                            "Claim Type": "$ClaimType",
                            "Category": "$categories",
                            "Start KM": "$startKm",
                            "End KM": "$endKm",
                            "Total Distance": "$Total_distance",
                            "Cost Center": "$costcenter",
                            "Project ID": "$projectId",
                            "Site ID": "$Site_Id",
                            "Task Name": "$Task",
                            "Employee Name": "$empName",
                            "Employee Code": "$empCode",
                            "Claimed Amount": "$Amount",
                            "Approved Amount": "$ApprovedAmount",
                            "Bill Number": "$billNumber",
                            "Check-In Date": "$checkInDate",
                            "Check-Out Date": "$checkOutDate",
                            "Total Days": "$totaldays",
                            "Remark": "$remark",
                            "Status": "$customStatus",
                            "Attachment": "$attachment",
                            "Signature": 1,
                            "Approved At": "$actionAt",
                            "User Signature": "$empName",
                            "L1-Approver": "$L1ApproverResult.empName",
                            "L2-Approver": "$L2ApproverResult.empName",
                            "L3-Approver": "$L3ApproverResult.empName",
                            "L1-Approver Date": "$L1ApproverDate",
                            "L2-Approver Date": "$L2ApproverDate",
                            "L3-Approver Date": "$L3ApproverDate",
                            "L1ApproverStatus": "$L1ApproverStatus",
                            "L2ApproverStatus": "$L2ApproverStatus",
                            "L3ApproverStatus": "$L3ApproverStatus",
                        }
                    },
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
                                    "timezone": "Asia/Kolkata",
                                }
                            },
                            "expenseuniqueId": {"$toString": "$_id"},
                            "addedFor": {"$toString": "$addedFor"},
                            "addedBy": {"$toString": "$addedBy"},
                            "expenseDate": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$expenseDate",
                                    "timezone": "Asia/Kolkata",
                                }
                            },
                            "actionAt": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$actionAt",
                                    "timezone": "Asia/Kolkata",
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
                # try:
                title = "Expense/Advance Report"
                output_path = os.path.join(os.getcwd(), "uploads", "Expense_Report.pdf")
                generate_pdf_from_dict(Response, output_path, title)
                return send_file(output_path)
                # except Exception as e:
                #     return respond(
                #         {
                #             "status": 400,
                #             "icon": "error",
                #             "msg": "Some thing went Wrong",
                #         }
                #     )

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


@expenses_blueprint.route(
    "/expenses/claimTypeAdvance", methods=["GET", "POST", "DELETE"]
)
@expenses_blueprint.route(
    "/expenses/claimTypeAdvance/<id>", methods=["GET", "POST", "DELETE"]
)
@token_required
def claimTypeAdvance(current_user, id=None):

    if request.method == "GET":
        arr = [
            {
                "$match": {
                    "value": {"$ne": False},
                    "designation": current_user["designation"],
                }
            },
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
                    "as": "claimResult",
                }
            },
            {"$unwind": {"path": "$claimResult", "preserveNullAndEmptyArrays": False}},
            {
                "$addFields": {
                    "DesignationClaimid": {"$toString": "$DesignationClaimid"},
                    "claimTypeId": {"$toString": "$claimTypeId"},
                    "mergerDesignationClaimId": {"$toString": "$_id"},
                }
            },
            {"$group": {"_id": "$claimTypeId", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {"_id": 0, "claimResult": 0}},
        ]

        Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
        return respond(Response)


@expenses_blueprint.route("/expenses/AllExpenses", methods=["GET", "POST"])
@expenses_blueprint.route("/expenses/AllExpenses/<id>", methods=["GET", "POST", "DELETE"])
@token_required
def AllExpenses(current_user, id=None):
    if request.method == "GET":
        ExpenseNo = request.args.get("ExpenseNo")
        month=request.args.get("month")
        year=request.args.get("year")
        # if year in ['',None,'undefined']:
        #     year = str(datetime.now().year)
        filterstatus = request.args.get('status')
        empName = request.args.get('empName')
        empCode = request.args.get('empCode')
        claimType = request.args.get('claimType')
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
                    "claimType":ObjectId(claimType)
                }
            }]
        if ExpenseNo not in ['',None,'undefined']:
            arpp4=[{
                '$match':{
                    "ExpenseNo":{
                        '$regex':ExpenseNo.strip(),
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


        arry =[
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "type": {"$ne": "Partner"},
                    "_id": ObjectId(current_user["userUniqueId"]),
                }
            },
            {
                "$lookup": {
                    "from": "userRole",
                    "localField": "userRole",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"roleName": 1, "_id": 0}},
                    ],
                    "as": "result",
                }
            },
            {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
            {"$project": {"userRole": "$result.roleName"}},
        ]
        userRole = cmo.finding_aggregate("userRegister", arry)["data"]
        if len(userRole):
            current_user["userRoleName"] = userRole[0]["userRole"]
        Response = None
        if current_user["userRoleName"] in ["Admin", "Finance", "PMO"]:
            arr = arpp1+arpp4+arrp5+[
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                    }
                },
                {"$sort": {"_id": -1}},
                {
                    '$project': {
                        'attachment': 0, 
                        'designation': 0, 
                        'FinancialYear': 0, 
                        'expenseNumberInt': 0, 
                        'expenseNumberStr': 0, 
                        'email': 0, 
                        'additionalInfo': 0, 
                        'fillBy': 0, 
                        'customStatus': 0, 
                        'type': 0, 
                        'CreatedAt': 0
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
                            },
                            {
                            '$project': {
                                'ustCode': 1, 
                                'empCode': 1, 
                                'empName': 1, 
                                'mobile': 1
                            }
                        }
                        ],
                        "as": "userInfo",
                    }
                },
            ]
            arr = arr + apireq.countarra("Expenses",arr) + apireq.args_pagination(request.args)
            arr = arr + [
                {"$addFields": {"len": {"$size": "$userInfo"}}},
                {"$match": {"len": {"$ne": 0}}},
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
                                        {"$match": {"deleteStatus": {"$ne": 1}}},
                                        {
                                            "$project": {
                                                "_id": 0,
                                                "circleName": 1,
                                                "circleCode": 1,
                                            }
                                        },
                                    ],
                                    "as": "circleResult",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$circleResult",
                                    "preserveNullAndEmptyArrays": True,
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
                {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
                {
                    "$unwind": {
                        "path": "$siteResults",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
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
                        "actionAt": {"$toDate": "$actionAt"},
                        "empCode": "$userInfo.empCode",
                        "empName": "$userInfo.empName",
                        "mobile": "$userInfo.mobile",
                        "Site_Id": "$siteResults.Site Id",
                        "Task": "$milestoneresult.Name",
                        "ustCode": "$userInfo.ustCode",
                        "ClaimType": "$claimTyperesult.claimType",
                        "projectId": {"$toString": "$projectId"},
                        "expenseDate": {
                            "$cond": {
                                "if": {
                                    "$or": [
                                        {"$eq": ["$expenseDate", ""]},
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{2}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{4}:\\d{2}:\\d{2}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{1}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                    ]
                                },
                                "then": None,
                                "else": {"$toDate": "$expenseDate"},
                            }
                        },
                        "Circle": "$projectResult.circleResult.circleName",
                        "circleCode": "$projectResult.circleResult.circleCode",
                    }
                }]
            arr=arr+arpp3+[
                {
                    "$addFields": {
                        "actionAt2": {
                            "$cond": {
                                "if": {"$in": ["$customisedStatus", [4,6]]},
                                "then": "$actionAt",
                                "else": None,
                            }
                        }
                    }
                },
                {
                    "$addFields": {
                        "actionAtKolkata": {
                            "$dateAdd": {
                                "startDate": "$actionAt2",
                                "unit": "minute",
                                "amount": 330,
                            }
                        },
                        "dayOfMonth": {
                            "$dayOfMonth": {
                                "date": {
                                    "$dateAdd": {
                                        "startDate": "$actionAt2",
                                        "unit": "minute",
                                        "amount": 330,
                                    }
                                }
                            }
                        },
                    }
                },
                {
                    "$addFields": {
                        "month": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$lt": ["$dayOfMonth", 26]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                    {
                                        "case": {"$gte": ["$dayOfMonth", 25]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                ],
                                "default": None,
                            }
                        }
                    }
                },
                {"$addFields": {"year": {
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
            }}},
            
                {"$addFields": {"year": {"$toString": "$year"}}}]
            arr=arr+arrp6+arrp7+[
                {
                    "$addFields": {
                        "SubmissionDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$Claim_Date",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseMonth": {"$concat": ["$month", "/", "$year"]},
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$expenseDate",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "ActionAt": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$actionAt",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseuniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                        "L1ApproverStatus": {"$arrayElemAt": ["$L1Approver.status", 0]},
                        "L1ApproverId": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                        "L2ApproverStatus": {"$arrayElemAt": ["$L2Approver.status", 0]},
                        "L2ApproverId": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                        "L3ApproverStatus": {"$arrayElemAt": ["$L3Approver.status", 0]},
                        "L3ApproverId": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
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
                        "claimTyperesult": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {"$sort": {"_id": -1}},
                {
                    "$project": {
                        "overall_table_count": 1,
                        "Month": "$expenseMonth",
                        "Employee Name": "$empName",
                        "Employee Code": "$empCode",
                        "UST Employee Code": "$ustCode",
                        "Contact Number": "$mobile",
                        "Expense number": "$ExpenseNo",
                        "Claim Date": "$expenseDate",
                        "Claim Type": "$ClaimType",
                        "Category": "$categories",
                        "Check-IN Date": "$checkInDate",
                        "Check-OUT Date": "$checkOutDate",
                        "Total Days": "$totaldays",
                        "Circle": "$Circle",
                        "Project ID": "$projectIdName",
                        "Cost Center": "$costcenter",
                        "Site ID": "$Site_Id",
                        "Task Name": "$Task",
                        "Amount": "$Amount",
                        "Submission Date": "$SubmissionDate",
                        "Approved Amount": "$ApprovedAmount",
                        "Status":"$status",
                        "Bill No": "$billNumber",
                        "Start KM": "$startKm",
                        "End KM": "$endKm",
                        "Transport Mode": "$categories",
                        "Start Location": "$startLocation",
                        "End Location": "$endLocation",
                        "Last Action Date": "$ActionAt",
                        "L1 Status": "$L1ApproverStatus",
                        "L2 Status": "$L2ApproverStatus",
                        "L3 Status": "$L3ApproverStatus",
                        "L1 Approver": "$L1ApproverResult.empName",
                        "L2 Approver": "$L2ApproverResult.empName",
                        "L3 Approver": "$L3ApproverResult.empName",
                        "Total KM": "$Total_distance",
                        "Remarks": "$remark",
                        "uniqueId": {"$toString": "$_id"},
                        "_id": 0,
                    }
                },
            ]

            print(arr,'gtdvgdggghdhyhyuhjd')
            Response = cmo.finding_aggregate("Expenses", arr)
            if len(Response):
                for i in Response["data"]:
                    checkingCondition = ["", "undefined", None]
                    if "Check-IN Date" in i:
                        if i["Check-IN Date"] not in checkingCondition:
                            i["Check-IN Date"] = convertToDateBulkExport2(
                                i["Check-IN Date"]
                            )
                    if "Check-OUT Date" in i:
                        if i["Check-OUT Date"] not in checkingCondition:
                            i["Check-OUT Date"] = convertToDateBulkExport2(
                                i["Check-OUT Date"]
                            )
            return respond(Response)

        else:

            arr = arpp1+arpp4+arrp5+[
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                    }
                },
                {
                    "$addFields": {
                        "L1ids": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                        "L2ids": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                        "L3ids": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
                    }
                },
                {
                    "$addFields": {
                        "ids": [
                            {"$ifNull": ["$L1ids", None]},
                            {"$ifNull": ["$L2ids", None]},
                            {"$ifNull": ["$L3ids", None]},
                        ]
                    }
                },
                {"$match": {"ids": ObjectId(current_user["userUniqueId"])}},
                {"$project": {"ids": 0, "L1ids": 0, "L2ids": 0, "L3ids": 0}},
                {"$sort": {"_id": -1}},
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
                },
            ]
            arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
            arr = arr + [
                {"$addFields": {"len": {"$size": "$userInfo"}}},
                {"$match": {"len": {"$ne": 0}}},
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
                                        {"$match": {"deleteStatus": {"$ne": 1}}},
                                        {
                                            "$project": {
                                                "_id": 0,
                                                "circleName": 1,
                                                "circleCode": 1,
                                            }
                                        },
                                    ],
                                    "as": "circleResult",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$circleResult",
                                    "preserveNullAndEmptyArrays": True,
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
                {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
                {
                    "$unwind": {
                        "path": "$siteResults",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
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
                        "actionAt": {"$toDate": "$actionAt"},
                        "empCode": "$userInfo.empCode",
                        "empName": "$userInfo.empName",
                        "ustCode": "$userInfo.ustCode",
                        "mobile": "$userInfo.mobile",
                        "Site_Id": "$siteResults.Site Id",
                        "Task": "$milestoneresult.Name",
                        "ClaimType": "$claimTyperesult.claimType",
                        "projectId": {"$toString": "$projectId"},
                        "expenseDate": {
                            "$cond": {
                                "if": {
                                    "$or": [
                                        {"$eq": ["$expenseDate", ""]},
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{2}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{4}:\\d{2}:\\d{2}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{1}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                    ]
                                },
                                "then": None,
                                "else": {"$toDate": "$expenseDate"},
                            }
                        },
                        "Circle": "$projectResult.circleResult.circleName",
                        "circleCode": "$projectResult.circleResult.circleCode",
                    }
                }]
            arr=arr+arpp3+[
                {
                    "$addFields": {
                        "actionAt2": {
                            "$cond": {
                                "if": {"$in": ["$customisedStatus", [4,6]]},
                                "then": "$actionAt",
                                "else": None,
                            }
                        }
                    }
                },
                {
                    "$addFields": {
                        "actionAtKolkata": {
                            "$dateAdd": {
                                "startDate": "$actionAt2",
                                "unit": "minute",
                                "amount": 330,
                            }
                        },
                        "dayOfMonth": {
                            "$dayOfMonth": {
                                "date": {
                                    "$dateAdd": {
                                        "startDate": "$actionAt2",
                                        "unit": "minute",
                                        "amount": 330,
                                    }
                                }
                            }
                        },
                    }
                },
                {
                    "$addFields": {
                        "month": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$lt": ["$dayOfMonth", 26]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                    {
                                        "case": {"$gte": ["$dayOfMonth", 25]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                ],
                                "default": None,
                            }
                        }
                    }
                },
                {"$addFields": {"year": {"$year": "$actionAt"}}},
                {"$addFields": {"year": {"$toString": "$year"}}}]
            arr=arr+arrp6+arrp7+[
                {
                    "$addFields": {
                        "SubmissionDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$Claim_Date",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseMonth": {"$concat": ["$month", "/", "$year"]},
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$expenseDate",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "ActionAt": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$actionAt",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseuniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                        "L1ApproverStatus": {"$arrayElemAt": ["$L1Approver.status", 0]},
                        "L1ApproverId": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                        "L2ApproverStatus": {"$arrayElemAt": ["$L2Approver.status", 0]},
                        "L2ApproverId": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                        "L3ApproverStatus": {"$arrayElemAt": ["$L3Approver.status", 0]},
                        "L3ApproverId": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
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
                        "claimTyperesult": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {"$sort": {"_id": -1}},
                {
                    "$project": {
                        "overall_table_count": 1,
                        "Month": "$expenseMonth",
                        "Employee Name": "$empName",
                        "Employee Code": "$empCode",
                        "UST Employee Code": "$ustCode",
                        "Contact Number": "$mobile",
                        "Expense number": "$ExpenseNo",
                        "Claim Date": "$expenseDate",
                        "Claim Type": "$ClaimType",
                        "Category": "$categories",
                        "Check-IN Date": "$checkInDate",
                        "Check-OUT Date": "$checkOutDate",
                        "Total Days": "$totaldays",
                        "Circle": "$Circle",
                        "Project ID": "$projectIdName",
                        "Cost Center": "$costcenter",
                        "Site ID": "$Site_Id",
                        "Task Name": "$Task",
                        "Amount": "$Amount",
                        "Submission Date": "$SubmissionDate",
                        "Approved Amount": "$ApprovedAmount",
                        "Bill No": "$billNumber",
                        "Start KM": "$startKm",
                        "End KM": "$endKm",
                        "Transport Mode": "$categories",
                        "Start Location": "$startLocation",
                        "End Location": "$endLocation",
                        "Last Action Date": "$ActionAt",
                        "L1 Status": "$L1ApproverStatus",
                        "L2 Status": "$L2ApproverStatus",
                        "L3 Status": "$L3ApproverStatus",
                        "L1 Approver": "$L1ApproverResult.empName",
                        "L2 Approver": "$L2ApproverResult.empName",
                        "L3 Approver": "$L3ApproverResult.empName",
                        "Total KM": "$Total_distance",
                        "Remarks": "$remark",
                        "uniqueId": {"$toString": "$_id"},
                        "_id": 0,
                    }
                },
            ]

            # print('arrarrarrjjj',arr)
            Response = cmo.finding_aggregate("Expenses", arr)
            if len(Response):
                for i in Response["data"]:
                    checkingCondition = ["", "undefined", None]
                    if "Check-IN Date" in i:
                        if i["Check-IN Date"] not in checkingCondition:
                            i["Check-IN Date"] = convertToDateBulkExport2(
                                i["Check-IN Date"]
                            )
                    if "Check-OUT Date" in i:
                        if i["Check-OUT Date"] not in checkingCondition:
                            i["Check-OUT Date"] = convertToDateBulkExport2(
                                i["Check-OUT Date"]
                            )
            return respond(Response)

            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": f"You have no permission !",
                }
            )
    if request.method == "DELETE":
        if id != None and id != "undefined":
            arr = [
                {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(id)}},
                {"$project": {"ExpenseNo": 1, "_id": {"$toString": "$_id"}}},
            ]
            Response = cmo.finding_aggregate("Expenses", arr)["data"]
            if len(Response):
                ExpenseNo = Response[0]["ExpenseNo"]
                print('ididid',id)
                cmo.insertion("ExpenseDeleted",{'Number':ExpenseNo,'ExpenseID':ObjectId(id),'deletedBy':ObjectId(current_user['userUniqueId']),'deletedAt':int(get_current_date_timestamp()),'type':'Expense'})
                cmo.deleting_m2("Approval", {"ExpenseNo": ExpenseNo}, current_user["userUniqueId"])
                Response = cmo.deleting_m2("Expenses", {"ExpenseNo": ExpenseNo}, current_user["userUniqueId"])
                return respond(Response)


@expenses_blueprint.route("/export/AllExpenses", methods=["GET", "POST"])
@expenses_blueprint.route("/export/AllExpenses/<id>", methods=["GET", "POST"])
@token_required
def ExportAllExpenses(current_user, id=None):
    if request.method == "GET":
        ExpenseNo = request.args.get("ExpenseNo")
        month=request.args.get("month")
        year=request.args.get("year")
        # if year in ['',None,'undefined']:
        #     year = str(datetime.now().year)
        filterstatus = request.args.get('status')
        empName = request.args.get('empName')
        empCode = request.args.get('empCode')
        claimType = request.args.get('claimType')
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
                    "claimType":ObjectId(claimType)
                }
            }]
        
            
        if ExpenseNo not in ['',None,'undefined']:
            arpp4=[{
                '$match':{
                    "ExpenseNo":{
                        '$regex':ExpenseNo.strip(),
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


        
        
        
        
        
        
        arry = [
            {
                "$match": {
                    "deleteStatus": {"$ne": 1},
                    "type": {"$ne": "Partner"},
                    "_id": ObjectId(current_user["userUniqueId"]),
                }
            },
            {
                "$lookup": {
                    "from": "userRole",
                    "localField": "userRole",
                    "foreignField": "_id",
                    "pipeline": [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"roleName": 1, "_id": 0}},
                    ],
                    "as": "result",
                }
            },
            {"$unwind": {"path": "$result", "preserveNullAndEmptyArrays": True}},
            {"$project": {"userRole": "$result.roleName"}},
        ]
        
        userRole = cmo.finding_aggregate("userRegister", arry)["data"]
        if len(userRole):
            current_user["userRoleName"] = userRole[0]["userRole"]
        Response = None
        if current_user["userRoleName"] in ["Admin", "Finance", "PMO"]:
            arr = arpp1+arpp4+arrp5+[
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
                            {"$project": {"_id": 0, "Site Id": 1, "systemId": 1}},
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
                                        {"$match": {"deleteStatus": {"$ne": 1}}},
                                        {
                                            "$project": {
                                                "_id": 0,
                                                "circleName": 1,
                                                "circleCode": 1,
                                            }
                                        },
                                    ],
                                    "as": "circleResult",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$circleResult",
                                    "preserveNullAndEmptyArrays": True,
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
                {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
                {
                    "$unwind": {
                        "path": "$siteResults",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
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
                        "actionAt": {"$toDate": "$actionAt"},
                        "empCode": "$userInfo.empCode",
                        "empName": "$userInfo.empName",
                        "mobile": "$userInfo.mobile",
                        "Site_Id": "$siteResults.Site Id",
                        "Task": "$milestoneresult.Name",
                        "ClaimType": "$claimTyperesult.claimType",
                        "projectId": {"$toString": "$projectId"},
                        "ustCode": "$userInfo.ustCode",
                        "bankName": "$userInfo.bankName",
                        "accountNumber": "$userInfo.accountNumber",
                        "ifscCode": "$userInfo.ifscCode",
                        "benificiaryname": "$userInfo.benificiaryname",
                        "expenseDate": {
                            "$cond": {
                                "if": {
                                    "$or": [
                                        {"$eq": ["$expenseDate", ""]},
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{2}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{4}:\\d{2}:\\d{2}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{1}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                    ]
                                },
                                "then": None,
                                "else": {"$toDate": "$expenseDate"},
                            }
                        },
                        "Circle": "$projectResult.circleResult.circleName",
                        "circleCode": "$projectResult.circleResult.circleCode",
                    }
                }]
            arr=arr+arpp3+[
                {
                    "$addFields": {
                        "actionAt2": {
                            "$cond": {
                                "if": {"$in": ["$customisedStatus", [4,6]]},
                                "then": "$actionAt",
                                "else": None,
                            }
                        }
                    }
                },
                {
                    "$addFields": {
                        "actionAtKolkata": {
                            "$dateAdd": {
                                "startDate": "$actionAt2",
                                "unit": "minute",
                                "amount": 330,
                            }
                        },
                        "dayOfMonth": {
                            "$dayOfMonth": {
                                "date": {
                                    "$dateAdd": {
                                        "startDate": "$actionAt2",
                                        "unit": "minute",
                                        "amount": 330,
                                    }
                                }
                            }
                        },
                    }
                },
                {
                    "$addFields": {
                        "month": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$lt": ["$dayOfMonth", 26]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                    {
                                        "case": {"$gte": ["$dayOfMonth", 25]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                ],
                                "default": None,
                            }
                        }
                    }
                },
                {"$addFields": {"year": {
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
            }}},
                {"$addFields": {"year": {"$toString": "$year"}}}]
            arr=arr+arrp6+arrp7+[
                {
                    "$addFields": {
                        "SubmissionDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$Claim_Date",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseMonth": {"$concat": ["$month", "/", "$year"]},
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$expenseDate",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "systemId": "$siteResults.systemId",
                        "ActionAt": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$actionAt",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseuniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                        "L1ApproverStatus": {"$arrayElemAt": ["$L1Approver.status", 0]},
                        "L1ApproverId": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                        "L2ApproverStatus": {"$arrayElemAt": ["$L2Approver.status", 0]},
                        "L2ApproverId": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                        "L3ApproverStatus": {"$arrayElemAt": ["$L3Approver.status", 0]},
                        "L3ApproverId": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
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
                        "claimTyperesult": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {
                    "$addFields": {
                        "accountNumber": {"$toString": "$accountNumber"},
                        "Check-IN Date": "$checkInDate",
                        "Check-OUT Date": "$checkOutDate",
                        "Total Days": "$totaldays",
                    }
                },
                {
                    "$project": {
                        "Claim Month": "$expenseMonth",
                        "Employee Name": "$empName",
                        "Employee Code": "$empCode",
                        "UST Employee Code": "$ustCode",
                        "Contact Number": "$mobile",
                        "Expense number": "$ExpenseNo",
                        "Claim Date": "$expenseDate",
                        "Claim Type": "$ClaimType",
                        "Category": "$categories",
                        "Circle": "$Circle",
                        "Check-IN Date": "$Check-IN Date",
                        "Check-OUT Date": "$Check-OUT Date",
                        "Total Days": "$Total Days",
                        "Project ID": "$projectIdName",
                        "System ID": "$systemId",
                        "Cost Center": "$costcenter",
                        "Site ID": "$Site_Id",
                        "Task Name": "$Task",
                        "Amount": "$Amount",
                        "Submission Date": "$SubmissionDate",
                        "Approved Amount": "$ApprovedAmount",
                        "Bill No": "$billNumber",
                        "Start KM": "$startKm",
                        "End KM": "$endKm",
                        "Total KM": "$Total_distance",
                        "Transport Mode": "$categories",
                        "Start Location": "$startLocation",
                        "End Location": "$endLocation",
                        "Last Action Date": "$ActionAt",
                        "Current Status": "$status",
                        "L1 Status": "$L1ApproverStatus",
                        "L2 Status": "$L2ApproverStatus",
                        "L3 Status": "$L3ApproverStatus",
                        "L1 Approver": "$L1ApproverResult.empName",
                        "L2 Approver": "$L2ApproverResult.empName",
                        "L3 Approver": "$L3ApproverResult.empName",
                        "Bank Name": "$bankName",
                        "Account Number": "$accountNumber",
                        "IFSC Code": "$ifscCode",
                        "Benificiary Name": "$benificiaryname",
                        "Remarks": "$remark",
                        "Additional Info": "$additionalInfo",
                        "_id": 0,
                    }
                },
            ]
            
            
            response = cmo.finding_aggregate("Expenses", arr)
            response = response["data"]
            dataframe = pd.DataFrame(response)
            datecols = [
                "Check-IN Date",
                "Check-OUT Date",
                "Claim Date",
                "Submission Date",
                "Last Action Date",
            ]
            for col in datecols:
                dataframe[col] = dataframe[col].apply(convertToDateBulkExport)

            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_AllExpenses", "Expenses"
            )
            return send_file(fullPath)
        else:
            arr = arpp1+arpp4+arrp5+[
                {
                    "$addFields": {
                        "L1ids": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                        "L2ids": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                        "L3ids": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
                    }
                },
                {
                    "$addFields": {
                        "ids": [
                            {"$ifNull": ["$L1ids", None]},
                            {"$ifNull": ["$L2ids", None]},
                            {"$ifNull": ["$L3ids", None]},
                        ]
                    }
                },
                {"$match": {"ids": ObjectId(current_user["userUniqueId"])}},
                {"$project": {"ids": 0, "L1ids": 0, "L2ids": 0, "L3ids": 0}},
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
                            {"$project": {"_id": 0, "Site Id": 1, "systemId": 1}},
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
                                        {"$match": {"deleteStatus": {"$ne": 1}}},
                                        {
                                            "$project": {
                                                "_id": 0,
                                                "circleName": 1,
                                                "circleCode": 1,
                                            }
                                        },
                                    ],
                                    "as": "circleResult",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$circleResult",
                                    "preserveNullAndEmptyArrays": True,
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
                {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
                {
                    "$unwind": {
                        "path": "$siteResults",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
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
                        "actionAt": {"$toDate": "$actionAt"},
                        "empCode": "$userInfo.empCode",
                        "empName": "$userInfo.empName",
                        "mobile": "$userInfo.mobile",
                        "Site_Id": "$siteResults.Site Id",
                        "Task": "$milestoneresult.Name",
                        "ClaimType": "$claimTyperesult.claimType",
                        "projectId": {"$toString": "$projectId"},
                        "ustCode": "$userInfo.ustCode",
                        "bankName": "$userInfo.bankName",
                        "accountNumber": "$userInfo.accountNumber",
                        "ifscCode": "$userInfo.ifscCode",
                        "benificiaryname": "$userInfo.benificiaryname",
                        "expenseDate": {
                            "$cond": {
                                "if": {
                                    "$or": [
                                        {"$eq": ["$expenseDate", ""]},
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{2}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{4}:\\d{2}:\\d{2}$",
                                            }
                                        },
                                        {
                                            "$regexMatch": {
                                                "input": "$expenseDate",
                                                "regex": "^\\d{1}:\\d{2}:\\d{4}$",
                                            }
                                        },
                                    ]
                                },
                                "then": None,
                                "else": {"$toDate": "$expenseDate"},
                            }
                        },
                        "Circle": "$projectResult.circleResult.circleName",
                        "circleCode": "$projectResult.circleResult.circleCode",
                    }
                }]
            arr=arr+arpp3+[
                
                {
                    "$addFields": {
                        "accountNumber": {"$toString": "$accountNumber"},
                        "actionAt2": {
                            "$cond": {
                                "if": {"$in": ["$customisedStatus", [4,6]]},
                                "then": "$actionAt",
                                "else": None,
                            }
                        },
                    }
                },
                {
                    "$addFields": {
                        "actionAtKolkata": {
                            "$dateAdd": {
                                "startDate": "$actionAt2",
                                "unit": "minute",
                                "amount": 330,
                            }
                        },
                        "dayOfMonth": {
                            "$dayOfMonth": {
                                "date": {
                                    "$dateAdd": {
                                        "startDate": "$actionAt2",
                                        "unit": "minute",
                                        "amount": 330,
                                    }
                                }
                            }
                        },
                    }
                },
                {
                    "$addFields": {
                        "month": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$lt": ["$dayOfMonth", 26]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                    {
                                        "case": {"$gte": ["$dayOfMonth", 25]},
                                        "then": {
                                            "$switch": {
                                                "branches": [
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                        "then": "Feb",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                2,
                                                            ]
                                                        },
                                                        "then": "Mar",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                3,
                                                            ]
                                                        },
                                                        "then": "Apr",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                4,
                                                            ]
                                                        },
                                                        "then": "May",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                5,
                                                            ]
                                                        },
                                                        "then": "Jun",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                6,
                                                            ]
                                                        },
                                                        "then": "Jul",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                7,
                                                            ]
                                                        },
                                                        "then": "Aug",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                8,
                                                            ]
                                                        },
                                                        "then": "Sep",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                9,
                                                            ]
                                                        },
                                                        "then": "Oct",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                10,
                                                            ]
                                                        },
                                                        "then": "Nov",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                11,
                                                            ]
                                                        },
                                                        "then": "Dec",
                                                    },
                                                    {
                                                        "case": {
                                                            "$eq": [
                                                                {
                                                                    "$month": "$actionAtKolkata"
                                                                },
                                                                12,
                                                            ]
                                                        },
                                                        "then": "Jan",
                                                    },
                                                ],
                                                "default": None,
                                            }
                                        },
                                    },
                                ],
                                "default": None,
                            }
                        }
                    }
                },
                {"$addFields": {"year": {"$year": "$actionAt"}}},
                {"$addFields": {"year": {"$toString": "$year"}}}]
            arr=arr+arrp6+arrp7+[
                {
                    "$addFields": {
                        "SubmissionDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$Claim_Date",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseMonth": {"$concat": ["$month", "/", "$year"]},
                        "expenseDate": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$expenseDate",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "systemId": "$siteResults.systemId",
                        "ActionAt": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$actionAt",
                                "timezone": "Asia/Kolkata",
                            }
                        },
                        "expenseuniqueId": {"$toString": "$_id"},
                        "addedFor": {"$toString": "$addedFor"},
                        "addedBy": {"$toString": "$addedBy"},
                        "L1ApproverStatus": {"$arrayElemAt": ["$L1Approver.status", 0]},
                        "L1ApproverId": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                        "L2ApproverStatus": {"$arrayElemAt": ["$L2Approver.status", 0]},
                        "L2ApproverId": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                        "L3ApproverStatus": {"$arrayElemAt": ["$L3Approver.status", 0]},
                        "L3ApproverId": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
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
                        "claimTyperesult": 0,
                        "ExpenseUniqueId": 0,
                        "actionBy": 0,
                    }
                },
                {
                    "$addFields": {
                        "Check-IN Date": "$checkInDate",
                        "Check-OUT Date": "$checkOutDate",
                        "Total Days": "$totaldays",
                    }
                },
                {
                    "$project": {
                        "Claim Month": "$expenseMonth",
                        "Employee Name": "$empName",
                        "Employee Code": "$empCode",
                        "UST Employee Code": "$ustCode",
                        "Contact Number": "$mobile",
                        "Expense number": "$ExpenseNo",
                        "Claim Date": "$expenseDate",
                        "Claim Type": "$ClaimType",
                        "Category": "$categories",
                        "Circle": "$Circle",
                        "Check-IN Date": "$Check-IN Date",
                        "Check-OUT Date": "$Check-OUT Date",
                        "Total Days": "$Total Days",
                        "Project ID": "$projectIdName",
                        "System ID": "$systemId",
                        "Cost Center": "$costcenter",
                        "Site ID": "$Site_Id",
                        "Task Name": "$Task",
                        "Amount": "$Amount",
                        "Submission Date": "$SubmissionDate",
                        "Approved Amount": "$ApprovedAmount",
                        "Bill No": "$billNumber",
                        "Start KM": "$startKm",
                        "End KM": "$endKm",
                        "Total KM": "$Total_distance",
                        "Transport Mode": "$categories",
                        "Start Location": "$startLocation",
                        "End Location": "$endLocation",
                        "Last Action Date": "$ActionAt",
                        "Current Status": "$status",
                        "L1 Status": "$L1ApproverStatus",
                        "L2 Status": "$L2ApproverStatus",
                        "L3 Status": "$L3ApproverStatus",
                        "L1 Approver": "$L1ApproverResult.empName",
                        "L2 Approver": "$L2ApproverResult.empName",
                        "L3 Approver": "$L3ApproverResult.empName",
                        "Bank Name": "$bankName",
                        "Account Number": "$accountNumber",
                        "IFSC Code": "$ifscCode",
                        "Benificiary Name": "$benificiaryname",
                        "Additional Info": "$additionalInfo",
                        "Remarks": "$remark",
                        "_id": 0,
                    }
                },
            ]
            response = cmo.finding_aggregate("Expenses", arr)
            response = response["data"]
            dataframe = pd.DataFrame(response)
            datecols = [
                "Check-IN Date",
                "Check-OUT Date",
                "Claim Date",
                "Submission Date",
                "Last Action Date",
            ]
            for col in datecols:
                dataframe[col] = dataframe[col].apply(convertToDateBulkExport)

            fullPath = excelWriteFunc.excelFileWriter(
                dataframe, "Export_AllExpenses", "Expenses"
            )
            return send_file(fullPath)




@expenses_blueprint.route("/export/UserExpenses", methods=["GET", "POST"])
@expenses_blueprint.route("/export/UserExpenses/<id>", methods=["GET", "POST"])
@token_required
def ExportUserExpenses(current_user, id=None):
    if request.method == "GET":
        ExpenseNo = request.args.get("ExpenseNo")
        month=request.args.get("month")
        year=request.args.get("year")
        # if year in ['',None,'undefined']:
        #     year = str(datetime.now().year)
        filterstatus = request.args.get('status')
        empName = request.args.get('empName')
        empCode = request.args.get('empCode')
        claimType = request.args.get('claimType')
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
                    "claimType":ObjectId(claimType)
                }
            }] 
        if ExpenseNo not in ['',None,'undefined']:
            arpp4=[{
                '$match':{
                    "ExpenseNo":{
                        '$regex':ExpenseNo.strip(),
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
        arr = arpp1+arpp4+arrp5+[
            {
                '$match': {
                    'addedFor': ObjectId(current_user["userUniqueId"])
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
                        {"$project": {"_id": 0, "Site Id": 1, "systemId": 1}},
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
                                    {"$match": {"deleteStatus": {"$ne": 1}}},
                                    {
                                        "$project": {
                                            "_id": 0,
                                            "circleName": 1,
                                            "circleCode": 1,
                                        }
                                    },
                                ],
                                "as": "circleResult",
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$circleResult",
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
            {"$unwind": {"path": "$userInfo", "preserveNullAndEmptyArrays": True}},
            {
                "$unwind": {
                    "path": "$siteResults",
                    "preserveNullAndEmptyArrays": True,
                }
            },
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
                    'customerName': "$projectResult.customerName",
                    "costcenter": "$projectResult.costcenter",
                    "Claim_Date": {"$toDate": "$addedAt"},
                    "actionAt": {"$toDate": "$actionAt"},
                    "empCode": "$userInfo.empCode",
                    "empName": "$userInfo.empName",
                    "mobile": "$userInfo.mobile",
                    "Site_Id": "$siteResults.Site Id",
                    "Task": "$milestoneresult.Name",
                    "ClaimType": "$claimTyperesult.claimType",
                    "projectId": {"$toString": "$projectId"},
                    "ustCode": "$userInfo.ustCode",
                    "bankName": "$userInfo.bankName",
                    "accountNumber": "$userInfo.accountNumber",
                    "ifscCode": "$userInfo.ifscCode",
                    "benificiaryname": "$userInfo.benificiaryname",
                    "expenseDate": {
                        "$cond": {
                            "if": {
                                "$or": [
                                    {"$eq": ["$expenseDate", ""]},
                                    {
                                        "$regexMatch": {
                                            "input": "$expenseDate",
                                            "regex": "^\\d{2}:\\d{2}:\\d{4}$",
                                        }
                                    },
                                    {
                                        "$regexMatch": {
                                            "input": "$expenseDate",
                                            "regex": "^\\d{4}:\\d{2}:\\d{2}$",
                                        }
                                    },
                                    {
                                        "$regexMatch": {
                                            "input": "$expenseDate",
                                            "regex": "^\\d{1}:\\d{2}:\\d{4}$",
                                        }
                                    },
                                ]
                            },
                            "then": None,
                            "else": {"$toDate": "$expenseDate"},
                        }
                    },
                    "Circle": "$projectResult.circleResult.circleName",
                    "circleCode": "$projectResult.circleResult.circleCode",
                }
            }]
        arr=arr+arpp3+[
            {
                "$addFields": {
                    "actionAt2": {
                        "$cond": {
                            "if": {"$in": ["$customisedStatus", [4,6]]},
                            "then": "$actionAt",
                            "else": None,
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "actionAtKolkata": {
                        "$dateAdd": {
                            "startDate": "$actionAt2",
                            "unit": "minute",
                            "amount": 330,
                        }
                    },
                    "dayOfMonth": {
                        "$dayOfMonth": {
                            "date": {
                                "$dateAdd": {
                                    "startDate": "$actionAt2",
                                    "unit": "minute",
                                    "amount": 330,
                                }
                            }
                        }
                    },
                }
            },
            {
                "$addFields": {
                    "month": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$lt": ["$dayOfMonth", 26]},
                                    "then": {
                                        "$switch": {
                                            "branches": [
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            1,
                                                        ]
                                                    },
                                                    "then": "Jan",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            2,
                                                        ]
                                                    },
                                                    "then": "Feb",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            3,
                                                        ]
                                                    },
                                                    "then": "Mar",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            4,
                                                        ]
                                                    },
                                                    "then": "Apr",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            5,
                                                        ]
                                                    },
                                                    "then": "May",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            6,
                                                        ]
                                                    },
                                                    "then": "Jun",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            7,
                                                        ]
                                                    },
                                                    "then": "Jul",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            8,
                                                        ]
                                                    },
                                                    "then": "Aug",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            9,
                                                        ]
                                                    },
                                                    "then": "Sep",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            10,
                                                        ]
                                                    },
                                                    "then": "Oct",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            11,
                                                        ]
                                                    },
                                                    "then": "Nov",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            12,
                                                        ]
                                                    },
                                                    "then": "Dec",
                                                },
                                            ],
                                            "default": None,
                                        }
                                    },
                                },
                                {
                                    "case": {"$gte": ["$dayOfMonth", 25]},
                                    "then": {
                                        "$switch": {
                                            "branches": [
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            1,
                                                        ]
                                                    },
                                                    "then": "Feb",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            2,
                                                        ]
                                                    },
                                                    "then": "Mar",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            3,
                                                        ]
                                                    },
                                                    "then": "Apr",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            4,
                                                        ]
                                                    },
                                                    "then": "May",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            5,
                                                        ]
                                                    },
                                                    "then": "Jun",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            6,
                                                        ]
                                                    },
                                                    "then": "Jul",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            7,
                                                        ]
                                                    },
                                                    "then": "Aug",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            8,
                                                        ]
                                                    },
                                                    "then": "Sep",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            9,
                                                        ]
                                                    },
                                                    "then": "Oct",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            10,
                                                        ]
                                                    },
                                                    "then": "Nov",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            11,
                                                        ]
                                                    },
                                                    "then": "Dec",
                                                },
                                                {
                                                    "case": {
                                                        "$eq": [
                                                            {
                                                                "$month": "$actionAtKolkata"
                                                            },
                                                            12,
                                                        ]
                                                    },
                                                    "then": "Jan",
                                                },
                                            ],
                                            "default": None,
                                        }
                                    },
                                },
                            ],
                            "default": None,
                        }
                    }
                }
            },
            {"$addFields": {"year": {"$year": "$actionAt"}}},
            {"$addFields": {"year": {"$toString": "$year"}}}]
        arr=arr+arrp6+arrp7+[
            {
                "$addFields": {
                    "SubmissionDate": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$Claim_Date",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "expenseMonth": {"$concat": ["$month", "/", "$year"]},
                    "expenseDate": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$expenseDate",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "systemId": "$siteResults.systemId",
                    "ActionAt": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$actionAt",
                            "timezone": "Asia/Kolkata",
                        }
                    },
                    "expenseuniqueId": {"$toString": "$_id"},
                    "addedFor": {"$toString": "$addedFor"},
                    "addedBy": {"$toString": "$addedBy"},
                    "L1ApproverStatus": {"$arrayElemAt": ["$L1Approver.status", 0]},
                    "L1ApproverId": {"$arrayElemAt": ["$L1Approver.actionBy", 0]},
                    "L2ApproverStatus": {"$arrayElemAt": ["$L2Approver.status", 0]},
                    "L2ApproverId": {"$arrayElemAt": ["$L2Approver.actionBy", 0]},
                    "L3ApproverStatus": {"$arrayElemAt": ["$L3Approver.status", 0]},
                    "L3ApproverId": {"$arrayElemAt": ["$L3Approver.actionBy", 0]},
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
                    "claimTyperesult": 0,
                    "ExpenseUniqueId": 0,
                    "actionBy": 0,
                }
            },
            {
                "$addFields": {
                    "accountNumber": {"$toString": "$accountNumber"},
                    "Check-IN Date": "$checkInDate",
                    "Check-OUT Date": "$checkOutDate",
                    "Total Days": "$totaldays",
                }
            },
            {
                "$project": {
                    "Claim Month": "$expenseMonth",
                    "Employee Name": "$empName",
                    "Employee Code": "$empCode",
                    "UST Employee Code": "$ustCode",
                    "Contact Number": "$mobile",
                    "Expense number": "$ExpenseNo",
                    "Claim Date": "$expenseDate",
                    "Claim Type": "$ClaimType",
                    "Category": "$categories",
                    "Circle": "$Circle",
                    "Check-IN Date": "$Check-IN Date",
                    "Check-OUT Date": "$Check-OUT Date",
                    "Total Days": "$Total Days",
                    "Customer":"$customerName",
                    "Project ID": "$projectIdName",
                    "System ID": "$systemId",
                    "Cost Center": "$costcenter",
                    "Site ID": "$Site_Id",
                    "Task Name": "$Task",
                    "Amount": "$Amount",
                    "Submission Date": "$SubmissionDate",
                    "Approved Amount": "$ApprovedAmount",
                    "Bill No": "$billNumber",
                    "Start KM": "$startKm",
                    "End KM": "$endKm",
                    "Total KM": "$Total_distance",
                    "Transport Mode": "$categories",
                    "Start Location": "$startLocation",
                    "End Location": "$endLocation",
                    "Last Action Date": "$ActionAt",
                    "Current Status": "$status",
                    "L1 Status": "$L1ApproverStatus",
                    "L2 Status": "$L2ApproverStatus",
                    "L3 Status": "$L3ApproverStatus",
                    "L1 Approver": "$L1ApproverResult.empName",
                    "L2 Approver": "$L2ApproverResult.empName",
                    "L3 Approver": "$L3ApproverResult.empName",
                    "Bank Name": "$bankName",
                    "Account Number": "$accountNumber",
                    "IFSC Code": "$ifscCode",
                    "Benificiary Name": "$benificiaryname",
                    "Remarks": "$remark",
                    "Additional Info": "$additionalInfo",
                    "_id": 0,
                }
            },
        ]
        response = cmo.finding_aggregate("Expenses", arr)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        datecols = [
            "Check-IN Date",
            "Check-OUT Date",
            "Claim Date",
            "Submission Date",
            "Last Action Date",
        ]
        for col in datecols:
            dataframe[col] = dataframe[col].apply(convertToDateBulkExport)

        fullPath = excelWriteFunc.excelFileWriter(
            dataframe, "Export_AllExpenses", "Expenses"
        )
        return send_file(fullPath)
        





@expenses_blueprint.route("/expenses/userLimit", methods=["GET", "POST"])
@expenses_blueprint.route("/expenses/userLimit/<id>", methods=["GET", "POST"])
@token_required
def userLimit(current_user, id=None):
    if request.method == "GET":
        if id != None and id != "undefined":
            arr = [
                {
                    "$match": {
                        "deleteStatus": {"$ne": 1},
                        "claimTypeId": ObjectId(id),
                        "designation": current_user["designation"],
                        "value": {"$nin": [False, True, "", "undefined"]},
                    }
                },
                {"$project": {"value": 1, "name": 1, "_id": 0}},
            ]
            Response = cmo.finding_aggregate("mergerDesignationClaim", arr)
            return respond(Response)
        else:
            return jsonify({"msg": "Id not Found"})


@expenses_blueprint.route("/expenses/downloadFile", methods=["GET", "POST"])
def testing2():
    file_name = request.args.get("attachment")
    if request.method == "GET":
        file_path = os.path.join(current_app.root_path, file_name)
        if os.path.exists(file_path):
            try:
                return send_file(file_path)
            except Exception as e:
                return jsonify({"error": str(e)})
        else:
            return respond(
                {
                    "status": 400,
                    "icon": "error",
                    "msg": f"No Attachment Given!",
                }
            )


def makeExpenseNo():
    ExpenseNo = "EXP"
    ExpenseNo = f"{ExpenseNo}/{currentFinancialMonth()}/"
    newArra = [{"$sort": {"_id": -1}}]
    responseData = cmo.finding_aggregate_with_deleteStatus("Expenses", newArra)["data"]
    if len(responseData) > 0 and "ExpenseNo" in responseData[0]:
        oldexpenseNo = responseData[0]["ExpenseNo"]
        ExpenseNo = generate_new_ExpenseNo(oldexpenseNo)
    else:
        ExpenseNo = ExpenseNo + "000001"
    return ExpenseNo


def makeadvanceNo():
    AdvanceNo = "ADV"
    AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
    newArra = [{"$sort": {"_id": -1}}]
    responseData = cmo.finding_aggregate_with_deleteStatus("Advance", newArra)["data"]
    if len(responseData) > 0 and "AdvanceNo" in responseData[0]:
        oldadvanceNo = responseData[0]["AdvanceNo"]
        AdvanceNo = generate_new_AdvanceNo(oldadvanceNo)
    else:
        AdvanceNo = AdvanceNo + "000001"
    return AdvanceNo


def background_Claims_upload(dict_data, pathing):
    cmo.insertion(
        "notification",
        {
            "msg": f"{pathing['fnamemsg']}",
            "status": "Uploading Start On",
            "typem": "old",
            "time": current_date_str(),
        },
    )
    for datauploaded in dict_data:
        datauploaded["CreatedAt"] = int(get_current_date_timestamp())
        datauploaded["addedAt"] = int(get_current_date_timestamp())
        if is_valid_mongodb_objectid(datauploaded["claimType"]):
            datauploaded["claimType"] = ObjectId(datauploaded["claimType"])
        if is_valid_mongodb_objectid(datauploaded["addedFor"]):
            datauploaded["addedFor"] = ObjectId(datauploaded["addedFor"])
            datauploaded["addedFor"] = ObjectId(datauploaded["addedFor"])
        if len(datauploaded["L1Approver"]):
            if is_valid_mongodb_objectid(datauploaded["L1Approver"][0]["actionBy"]):
                datauploaded["L1Approver"][0]["actionBy"] = ObjectId(
                    datauploaded["L1Approver"][0]["actionBy"]
                )
        if len(datauploaded["L2Approver"]):
            if is_valid_mongodb_objectid(datauploaded["L2Approver"][0]["actionBy"]):
                datauploaded["L2Approver"][0]["actionBy"] = ObjectId(
                    datauploaded["L2Approver"][0]["actionBy"]
                )
        if len(datauploaded["L3Approver"]):
            if is_valid_mongodb_objectid(datauploaded["L3Approver"][0]["actionBy"]):
                datauploaded["L3Approver"][0]["actionBy"] = ObjectId(
                    datauploaded["L3Approver"][0]["actionBy"]
                )
        if is_valid_mongodb_objectid(datauploaded["projectId"]):
            datauploaded["projectId"] = ObjectId(datauploaded["projectId"])
        if datauploaded["ApprovedAmount"] in [None, "", "undefined"]:
            datauploaded["ApprovedAmount"] = datauploaded["Amount"]

        datauploaded["submissionDate"] = check_and_convert_date(
            datauploaded["submssionDate"]
        )
        # print('datauploadeddatauploadeddatauploadeddatauploaded',datauploaded)

        if datauploaded["type"] in ["Expense", "Daily Allowance"]:
            datauploaded["expenseDate"] = check_and_convert_date2(
                (datauploaded["expenseDate"])
            )
            datauploaded["ExpenseNo"]=ExpenseNonewLogic()
            financialyear = None
            varible = None
            varibleInt = None

            if datauploaded["ExpenseNo"] not in ["", "undefined", None]:
                ggg = datauploaded["ExpenseNo"].split("/")
                financialyear = ggg[1]
                varible = ggg[-1]
                varibleInt = int(varible)
            datauploaded['FinancialYear']=financialyear
            datauploaded['expenseNumberStr']=varible
            datauploaded['expenseNumberInt']=varibleInt
            
            if 'taskName' in datauploaded:
                if is_valid_mongodb_objectid(datauploaded["taskName"]):
                    datauploaded["taskName"] = ObjectId(datauploaded["taskName"])
            if 'Site Id' in datauploaded:
                if is_valid_mongodb_objectid(datauploaded["Site Id"]):
                    datauploaded["Site Id"] = ObjectId(datauploaded["Site Id"])

            Response = cmo.insertion("Expenses", datauploaded)
            datauploaded["ExpenseUniqueId"] = ObjectId(Response["operation_id"])
            cmo.updating(
                "Approval",
                {"ExpenseUniqueId": ObjectId(Response["operation_id"])},
                datauploaded,
                True,
            )
        else:

            if is_valid_mongodb_objectid(datauploaded["claimType"]):
                datauploaded["advanceTypeId"] = ObjectId(datauploaded["claimType"])
            if "claimType" in datauploaded:
                del datauploaded["claimType"]
            if "expenseDate" in datauploaded:
                del datauploaded["expenseDate"]
            datauploaded["AdvanceNo"] = AdvanceNonewLogic()
            financialyear = None
            varible = None
            varibleInt = None

            if datauploaded["AdvanceNo"] not in ["", "undefined", None]:
                ggg = datauploaded["AdvanceNo"].split("/")
                financialyear = ggg[1]
                varible = ggg[-1]
                varibleInt = int(varible)
            datauploaded['FinancialYear']=financialyear
            datauploaded['advanceNumberStr']=varible
            datauploaded['advanceNumberInt']=varibleInt
            
            
            
            
            
            
            Response = cmo.insertion("Advance", datauploaded)
            datauploaded["AdvanceUniqueId"] = Response["operation_id"]
            cmo.updating(
                "Approval",
                {"AdvanceUniqueId": ObjectId(Response["operation_id"])},
                datauploaded,
                True,
            )

        # if is_valid_mongodb_objectid(datauploaded['addedBy']):
        #     datauploaded['addedFor']=ObjectId(datauploaded['addedFor'])

    # print('dict_data2dict_data2',dict_data)

    # Response = cmo.insertion("SiteEngineer", datauploaded)

    cmo.insertion(
        "notification",
        {
            "msg": f"{pathing['fnamemsg']}",
            "status": "Complete",
            "typem": "old",
            "time": current_date_str(),
        },
    )

    return respond(
        {
            "status": 200,
            "icon": "success",
            "msg": f"Your File {pathing['fnamemsg']} is successfully uploaded",
        }
    )
    



def background_Settlement_Uploads(dict_data, pathing):
    cmo.insertion("notification",{"msg": f"{pathing['fnamemsg']}","status": "Uploading Start On","typem": "old","time": current_date_str(),},)
    print('datauploadeddatauploadeddatauploaded',dict_data)
    for datauploaded in dict_data:
        datauploaded["approvalDate"] = check_and_convert_date2((datauploaded["approvalDate"]))
        datauploaded["SettlementRequisitionDate"] = check_and_convert_date2((datauploaded["SettlementRequisitionDate"]))
        datauploaded['SettlementID']=SettlementIDnewLogic()
        if is_valid_mongodb_objectid(datauploaded['empID']):
            datauploaded['empID']=ObjectId(datauploaded['empID'])
        Response = cmo.insertion("Settlement", datauploaded)
    cmo.insertion("notification",{"msg": f"{pathing['fnamemsg']}","status": "Complete","typem": "old","time": current_date_str(),},)

    return respond(
        {
            "status": 200,
            "icon": "success",
            "msg": f"Your File  is successfully uploaded",
        }
    )  
# @expenses_blueprint.route("/Expenses/SettlementAmount",methods=['GET','POST'])
# @expenses_blueprint.route("/Expenses/SettlementAmount/<id>",methods=['GET','POST'])
# @token_required
# def SettlementAmount(current_user,id=None):
#     if request.method == "GET":
#         arr=[]
#         Response=cmo.finding_aggregate("SettlementAmount",arr)
#         return respond(Response)
#     if request.method == "POST":
#         if id!= None

@expenses_blueprint.route("/Expenses/SettlementAmountEmp",methods=['GET','POST','DELETE'])
@expenses_blueprint.route("/Expenses/SettlementAmountEmp/<id>",methods=['GET','POST','DELETE'])
@token_required
def SettlementAmountEmp(current_user,id=None):
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
                    'localField': 'empID', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'type': {
                                    '$ne': 'Partner'
                                }
                            }
                        }, {
                            '$project': {
                                'empCode': 1, 
                                'empName': 1, 
                                'ustCode': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'Empresult'
                }
            }, {
                '$unwind': {
                    'path': '$Empresult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'approvalDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$approvalDate'
                            }, 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'SettlementRequisitionDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$SettlementRequisitionDate'
                            }, 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
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
                    'empName': '$Empresult.empName',
                    'emp':'$Empresult.empName',
                    'empCode': '$Empresult.empCode', 
                    'ustCode': '$Empresult.ustCode', 
                    'Amount': 1, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'SettlementRequisitionDate': 1, 
                    'approvalDate': 1, 
                    'remarks': 1, 
                    'SettlementID': 1, 
                    '_id': 0,
                    "empID":{
                        "$toString":"$empID"
                    }
                }
            }
        ]
        arr = arr + apireq.commonarra + apireq.args_pagination(request.args)
        Response=cmo.finding_aggregate("Settlement",arr)
        return respond(Response)
    if request.method == "POST":
        if id == None:
            SettlementID=None
            data=request.get_json()
            if 'SettlementID' in data:
                data['SettlementID']=data['SettlementID']
            else:
                SettlementID="SET/24-25/000000"
                print('SettlementIDSettlementID1',SettlementID)
                SettlementID = SettlementIDnewLogic()
                print('SettlementIDSettlementID2',SettlementID)
                isWrongExpense = cmo.checkSettlementID(SettlementID)
                while isWrongExpense:
                    SettlementID = SettlementIDnewLogic()
                    print('SettlementIDSettlementID3',SettlementID)
                    isWrongExpense = cmo.checkSettlementID(SettlementID)
                print('SettlementIDSettlementID',SettlementID)
                
                data['SettlementID']=SettlementID
            
            if 'empID' in data:
                if is_valid_mongodb_objectid(data['empID']):
                    data['empID']=ObjectId(data['empID'])  
            if 'Amount' in data:
                try:
                    data['Amount'] = int(data['Amount'])
                except Exception as e:
                    data['Amount'] = 0
            Response=cmo.insertion('Settlement',data)
            return respond(Response)
        if id != None:
            data=request.get_json()
            if 'empID' in data:
                if is_valid_mongodb_objectid(data['empID']):
                    data['empID']=ObjectId(data['empID'])  
            if 'Amount' in data:
                try:
                    data['Amount'] = int(data['Amount'])
                except Exception as e:
                    data['Amount'] = 0
            Response=cmo.updating('Settlement',{'_id':ObjectId(id)},data,False)
            return respond(Response)
    if request.method == 'DELETE':
        if is_valid_mongodb_objectid(id):
            Response=cmo.deleting("Settlement",id)
            return respond(Response)
        else:
            return respond(
                {
                    "status": 400,
                    "msg": "Please Provide a valid ID",
                    "icon": "error",
                }
            )
            

# def TestingFunctionProjectID():
#     arr=[
#     {
#         '$match': {
#             'deleteStatus': {
#                 '$ne': 1
#             }
#         }
#     }, {
#         '$addFields': {
#             'costCenter': {
#                 '$toObjectId': '$costCenter'
#             }
#         }
#     }, {
#         '$lookup': {
#             'from': 'costCenter', 
#             'localField': 'costCenter', 
#             'foreignField': '_id', 
#             'pipeline': [
#                 {
#                     '$match': {
#                         'deleteStatus': {
#                             '$ne': 1
#                         }
#                     }
#                 }
#             ], 
#             'as': 'costCenterResults'
#         }
#     }, {
#         '$unwind': {
#             'path': '$costCenterResults'
#         }
#     }, {
#         '$lookup': {
#             'from': 'Expenses', 
#             'localField': '_id', 
#             'foreignField': 'addedFor', 
#             'pipeline': [
#                 {
#                     '$project': {
#                         'ExpenseNo': 1, 
#                         'status': 1, 
#                         'isEmptyProjectId': {
#                             '$cond': {
#                                 'if': {
#                                     '$or': [
#                                         {
#                                             '$eq': [
#                                                 '$projectId', None
#                                             ]
#                                         }, {
#                                             '$eq': [
#                                                 '$projectId', ''
#                                             ]
#                                         }, {
#                                             '$eq': [
#                                                 {
#                                                     '$type': '$projectId'
#                                                 }, 'missing'
#                                             ]
#                                         }
#                                     ]
#                                 }, 
#                                 'then': True, 
#                                 'else': False
#                             }
#                         }
#                     }
#                 }, {
#                     '$match': {
#                         'isEmptyProjectId': {
#                             '$ne': False
#                         }
#                     }
#                 }
#             ], 
#             'as': 'ExpenseResult'
#         }
#     }, {
#         '$unwind': {
#             'path': '$ExpenseResult'
#         }
#     }, {
#         '$project': {
#             '_id': 0, 
#             'empName': 1, 
#             'email': 1, 
#             'empCode': 1, 
#             'ustCode': 1, 
#             'costCenter': '$costCenterResults.costCenter', 
#             'costCenterId': {
#                 '$toString': '$costCenter'
#             }, 
#             'ExpenseNo': '$ExpenseResult.ExpenseNo', 
#             'ExpenseId': {
#                 '$toString': '$ExpenseResult._id'
#             }, 
#             'Status': {
#                 '$toString': '$ExpenseResult.status'
#             }
#         }
#     }
# ]

@expenses_blueprint.route("/export/SettlementAmount", methods=["GET", "POST"])
@expenses_blueprint.route("/export/SettlementAmount/<id>", methods=["GET", "POST"])
@token_required
def SettlementAmount(current_user, id=None):
    if request.method == "GET":
        arr = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }
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
                                }, 
                                'type': {
                                    '$ne': 'Partner'
                                }
                            }
                        }, {
                            '$project': {
                                'empCode': 1, 
                                'empName': 1, 
                                'ustCode': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'Empresult'
                }
            }, {
                '$unwind': {
                    'path': '$Empresult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'approvalDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$approvalDate'
                            }, 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'SettlementRequisitionDate': {
                        '$dateToString': {
                            'date': {
                                '$toDate': '$SettlementRequisitionDate'
                            }, 
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$sort': {
                    '_id': -1
                }
            }, {
                '$project': {
                    'empName': '$Empresult.empName', 
                    'emp': '$Empresult.empName', 
                    'empCode': '$Empresult.empCode', 
                    'ustCode': '$Empresult.ustCode', 
                    'Amount': 1, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'SettlementRequisitionDate': 1, 
                    'approvalDate': 1, 
                    'remarks': 1, 
                    'SettlementID': 1, 
                    '_id': 0, 
                    'empID': {
                        '$toString': '$empID'
                    }
                }
            }, {
                '$project': {
                    'Settlement ID': '$SettlementID', 
                    'Employee Code': '$empCode', 
                    'UST Employee ID': '$ustCode', 
                    'Employee Name': '$empName', 
                    'Settlement Requisition Date': '$SettlementRequisitionDate', 
                    'Approval Date': '$approvalDate', 
                    'Amount': '$Amount', 
                    'Remarks': '$remarks'
                }
            }
        ]
        response = cmo.finding_aggregate("Settlement", arr)
        response = response["data"]
        dataframe = pd.DataFrame(response)
        for col in dataframe.columns:
            dataframe[col] = dataframe[col].apply(convertToDateBulkExport)
        fullPath = excelWriteFunc.excelFileWriter(
            dataframe, "Export_SettlementAmount", "Settlement Amount"
        )
        # print("fullPathfullPathfullPath", fullPath)
        return send_file(fullPath)