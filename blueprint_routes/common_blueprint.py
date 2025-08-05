from base import *
from common.config import *
from datetime import datetime as dtt
from datetime import timedelta
import pytz
from common.config import generate_new_ExpenseNo as generate_new_ExpenseNo
from common.config import *
import common.excel_write as excelWriteFunc
from colorama import Fore, Style
from blueprint_routes.expenses_blueprint import background_Claims_upload as background_Claims_upload
from blueprint_routes.expenses_blueprint import makeadvanceNo,makeExpenseNo
from blueprint_routes.sample_blueprint import sample_projectId,sample_pg
from blueprint_routes.expenses_blueprint import background_Settlement_Uploads as  background_Settlement_Uploads
from blueprint_routes.poLifeCycle_blueprint import projectGroup
from common.mongo_operations import db as database






current_time = dtt.now().strftime("%H:%M:%S")
tz = pytz.timezone("Asia/Kolkata")
current_date = datetime.now(tz)
three_days_ago = current_date - timedelta(days=3)
current_date = dtt.now()
valid_months = [
    'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
    'September', 'October', 'November', 'December'
]
monthMapping = {month: i + 1 for i, month in enumerate(valid_months)}
def is_valid_month_year(row):
    if row['month'] not in valid_months:
        print('invalid Month',row['month'])
        return False
    try:
        year = int(row['year'])
        if year <= 2018:
            print('invalid Year')
            return False
    except ValueError:
        return False
    return True
# current_year = current_date.year
# current_month = current_date.strftime("%b").upper()
# if current_date.month >= 4:
#     fiscal_year = f"{current_year % 100}-{(current_year + 1) % 100}"
# else:
#     fiscal_year = f"{(current_year - 1) % 100}-{current_year % 100}"
# currentFinancialMonth = f"{fiscal_year}/{current_month}"


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
checkingarray2 = [None, '', 'undefined']
def get_next_expense_no():
    ExpenseNo2 = "EXP"
    ExpenseNo2 = f"{ExpenseNo2}/{currentFinancialMonth()}/"
    # sortById = [{"$sort": {"_id": -1}}]
    counter = database.fileDetail.find_one_and_update({"id": "expenseIdCounter"},{"$inc": {"sequence_value": 1}},return_document=True,upsert=True)
    sequence_value = counter["sequence_value"]
    sequence_value=str(sequence_value).zfill(6)
    ExpenseNo2=ExpenseNo2+sequence_value
    return ExpenseNo2

def get_next_advance_no():
    # Fetch latest advance number
    AdvanceNo = "ADV"
    AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
    AdvanceNo2 = "ADV"
    AdvanceNo2 = f"{AdvanceNo2}/{currentFinancialMonth()}/"
    sortByAdvanceNumber = [{"$sort": {"advanceNumberInt": -1}}]
    sortById = [{"$sort": {"_id": -1}}]
    responseData = cmo.finding_aggregate_with_deleteStatus("Advance", sortByAdvanceNumber)["data"]
    responseData2 = cmo.finding_aggregate_with_deleteStatus("Advance", sortById)["data"]
    if len(responseData) > 0 and "AdvanceNo" in responseData[0]:
        oldAdvanceNo = responseData[0]["AdvanceNo"]
        AdvanceNo=generate_new_AdvanceNo(oldAdvanceNo)  
    if len(responseData2) > 0 and "AdvanceNo" in responseData2[0]:
        oldAdvanceNo2 = responseData2[0]["AdvanceNo"]
        AdvanceNo2 = generate_new_AdvanceNo(oldAdvanceNo2)
    if AdvanceNo2 == AdvanceNo:
        return AdvanceNo2
        
    else:
        AdvanceNo2 = "ADV"
        AdvanceNo2 = f"{AdvanceNo2}/{currentFinancialMonth()}/"
        sortById = [{"$sort": {"_id": -1}}]
        responseData2 = cmo.finding_aggregate_with_deleteStatus("Advance", sortById)["data"]
        if len(responseData2) > 0 and "AdvanceNo" in responseData2[0]:
            oldAdvanceNo2 = responseData2[0]["AdvanceNo"]
            AdvanceNo2=generate_new_AdvanceNo(oldAdvanceNo) 
        return AdvanceNo2

# Get the next available number for both Expense and Advance
# next_expense_no = get_next_expense_no()
# next_advance_no = get_next_advance_no()
# next_expense_no = None
# next_advance_no = None

# def generate_number(row):
#     global next_expense_no, next_advance_no
#     if row['type'] == 'Advance':
#         number = next_advance_no
#         next_advance_no = generate_new_AdvanceNo(next_advance_no) 
#     else:
#         number = next_expense_no
#         next_expense_no = generate_new_ExpenseNo(next_expense_no) 
#     return number



df_current_date = pd.to_datetime(datetime.now(tz=pytz.timezone("Asia/Kolkata")))
current_date = df_current_date.replace(hour=0, minute=0, second=0, microsecond=0)
df_current_date = pd.to_datetime(current_date)



common_blueprint = Blueprint("common_blueprint", __name__)



def my_function(row, circleDatadp):

    dteq = []
    for kk in row["circle"].split(","):
        datewq = circleDatadp[circleDatadp["circle"] == kk]
        if len(datewq) > 0:
            dteq.append(ObjectId(datewq["uniqueId"].iloc[0]))
        else:
            return {
                "status": 400,
                "circle": kk,
            }

    row["circle"] = dteq
    row["status"] = 200
    return row

def my_function2(row, projectidDatadp):

    dteq = []
    for kk in row["projectId"].split(","):
        datewq = projectidDatadp[projectidDatadp["projectId"] == kk]
        if len(datewq) > 0:
            dteq.append(ObjectId(datewq["projectuid"].iloc[0]))
        else:
            return {
                "status":400,
                "msg":kk
            }
            
    row["projectIds"] = dteq
    row["status"] = 200
    return row


def validate_year(year):
    try:
        year_int = int(year)
        return 1900 <= year_int <= 2100
    except ValueError:
        return False
    
def validate_month(month):
    try:
        month = int(month)
        return 1 <= month <= 12
    except ValueError:
        return False

def is_valid_number_or_empty(value):
    if pd.isna(value) or value == '':
        return True
    try:
        float(value)
        return True
    except ValueError:
        return False

def is_valid_number(value):
    return isinstance(value, (int, float)) and value >= 0

@common_blueprint.route("/commonUploadFile", methods=["POST"])
@common_blueprint.route("/commonUploadFile/<id>/<MSType>", methods=["POST"])
@token_required
def uploadfile(current_user,id=None,MSType=None):
    if request.method == "POST":
        uploadedFile = request.files.get("uploadedFile[]")
        fileType = request.form.get("fileType")
        allData = {}
        supportFile = ["xlsx", "xls"]
        pathing = cform.singleFileSaver(uploadedFile, "", supportFile)
        if pathing["status"] == 200:
            allData["filePath"] = pathing["msg"]
            

        elif pathing["status"] == 422:
            return respond(pathing)

        fileTeamCheck = {
            "ManageCircle": {
                "validate": cdcm.validate_Upload_ManageCircle,
                "rename": cdcm.rename_Upload_ManageCircle,
                "collection": "circle",
            },
            "ManageZone": {
                "validate": cdcm.validate_Upload_ManageZone,
                "rename": cdcm.rename_Upload_ManageZone,
                "collection": "zone",
            },
            "ManageCostCenter": {
                "validate": cdcm.validate_Upload_ManageCostCenter,
                "rename": cdcm.rename_Upload_ManageCostCenter,
                "collection": "costCenter",
            },
            "ManageEmployee": {
                "validate": cdcm.validate_Upload_ManageEmployee,
                "rename": cdcm.rename_Upload_ManageEmployee,
            },
            "invoice": {
                "validate": cdcm.validate_Upload_invoice,
                "rename": cdcm.rename_Upload_invoice,
                "collection": "invoice",
            },
            "ManageTemplate": {
                "validate": cdcm.validate_Upload_Template,
                # "rename":cdcm.rename_Upload_Template,
                "collection": "template",
            },
            "PoInvoice": {
                "validate": cdcm.validate_Upload_PoInvoice,
                "rename": cdcm.rename_Upload_PoInvoice,
                "collection": "PoInvoice",
            },
            "PoUpgrade": {
                "validate": cdcm.validate_Upgrade_PoInvoice,
                "rename": cdcm.rename_Upgrade_PoInvoice,
            },
            "ItemCodeforWork": {
                "validate": cdcm.validate_Upload_ItemCode,
                "rename": cdcm.rename_Upload_ItemCode,
            },
            "ManageSiteBulkUpload": {
                "validate": cdcm.validate_Upload_SiteIdBulk,
                "rename": cdcm.rename_Upload_SiteIdBulk,
                "collection": "SiteEngineer",
            },
            "ManageVendor": {
                "validate": cdcm.validate_upload_vendor,
                "rename": cdcm.rename_upload_vendor,
            },
            "UpgradeEmployee": {
                "validate": cdcm.validateUpgradeBulkEmployee,
                "rename": cdcm.renameUpgradeBulkEmployee,
            },
            "UpgradeEmployeeWithEmpCode": {
                "validate": cdcm.validateUpgradeBulkEmployee,
                "rename": cdcm.renameUpgradeBulkEmployee,
            },
            "UpgradeVendor": {
                "validate": cdcm.validateUpgradeVendor,
                "rename": cdcm.renameUpgradeVendor,
            },
            "userProjectAllocation": {
                "validate": cdcm.validateuserProjectAllocation,
                "rename": cdcm.renameuserProjectAllocation,
            },
            "partnerProjectAllocation": {
                "validate": cdcm.validatepartnerProjectAllocation,
                "rename": cdcm.renamepartnerProjectAllocation,
            },
            "ManageClaims":{
              "validate": cdcm.validateBulkUploadExpense,
                "rename": cdcm.renameBulkUploadExpense,  
            },
            "UploadCurrentBalance":{
              "validate": cdcm.validateBulkUploadCurrentBalance,
                "rename": cdcm.renameBulkUploadCurrentBalance,  
            },
            "EVMFinancial":{
              "validate": cdcm.validateEVMFinancial,
                "rename": cdcm.renameEVMFinancial,  
            },
            'EVMDelivery':{
                'validate':cdcm.validateEVMDelivery,
                "rename": cdcm.renameEVMDelivery,
            },
            "UploadAccuralRevenueMaster":{
              "validate": cdcm.validate_Upload_AccuralRevenueMaster,
                "rename": cdcm.rename_upload_AccuralRevenueMaster,  
            },
            "SettlementAmount":{
              "validate": cdcm.validateBulkUploadSettlementAmount,
                "rename": cdcm.renameBulkUploadSettlementAmount,  
            },
            "profitloss":{
              "validate": cdcm.validate_profit_loss,
                "rename": cdcm.rename_profit_loss,  
            },
            "TEST":{
                "validate":cdcm.validate_test2,
                "rename": cdcm.rename_test2, 
            },
            "ManageSalaryDB":{
                "validate":cdcm.validate_SalaryDB,
                "rename":cdcm.rename_SalaryDB
            },
            "OtherFixedCost":{
                "validate":cdcm.validate_OtherFixedCost,
                "rename":cdcm.rename_OtherFixedCost
            },
            "vendorCost" : {
                "validate" : cdcm.validate_vendorCost ,
                "rename": cdcm.rename_Upload_vendorCost
            
            },
           "AOP":{
                "validate":cdcm.validate_AOPDB,
                "rename":cdcm.rename_AOPDB
            },
           "L1_Approver_MDB":{
                "validate":cdcm.validate_L1Approver_MDB,
                "rename":cdcm.rename_L1Approver_MDB
            },
           "L2_Approver_MDB":{
                "validate":cdcm.validate_L2Approver_MDB,
                "rename":cdcm.rename_L2Approver_MDB
            },

        }
        # 
        if fileType in fileTeamCheck:
            excel_file_path = os.path.join(os.getcwd(), allData["filePath"])
            rename = fileTeamCheck[fileType]["rename"]
            validate = fileTeamCheck[fileType]["validate"]
            if fileType in ["ManageClaims","UpgradeEmployeeWithEmpCode","UploadAccuralRevenueMaster",'updateAttchmentPath','SettlementAmount','ManageSalaryDB','OtherFixedCost','vendorCost','AOP','L1_Approver_MDB','L2_Approver_MDB']:
                data = cfc.exceltodfnoval2(excel_file_path, rename, validate)
            if fileType in ["ManageVendor","ManageEmployee","UpgradeEmployee","UpgradeVendor"]:
                data = cfc.exceltodfnoval3(excel_file_path, rename, validate)
            else:
                data = cfc.exceltodfnoval(excel_file_path, rename, validate)
            if data["status"] != 400:   
                exceldata = data["data"]
                if exceldata.empty:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "The uploaded Excel file is empty. Please check the file and try again.",
                        }
                    )
                #######   
                
                  # code MBD Bulk Upload
                if fileType == "L1_Approver_MDB":
                    checkingFieldsArray=['empName', 'email', 'customerName', 'projectGroupName', 'projectTypeName','circleName']
                    errorlist=[]
                    checkingFieldsArray2=['empName', 'email', 'customerName', 'projectGroupName', 'projectTypeName','circleName']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    
                    dbData = [
                            {
                                '$match': {
                                    'type': {
                                        '$ne': 'Partner'
                                    }, 
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'status': 'Active'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'userRole', 
                                    'localField': 'userRole', 
                                    'foreignField': '_id', 
                                    'pipeline': [
                                        {
                                            '$project': {
                                                'roleName': 1, 
                                                '_id': 0
                                            }
                                        }
                                    ], 
                                    'as': 'userResults'
                                }
                            }, {
                                '$project': {
                                    'email': 1, 
                                    'ustCode': 1, 
                                    'empName': 1, 
                                    'empCode': 1, 
                                    'employeeId': {
                                        '$toString': '$_id'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])  

                        empNameCodeDatadp = empNameCodeDatadp.drop_duplicates(subset=["empName", "email"])
                                                       
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empName", "email"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["employeeId"].isna()]
                        unique_combinations = cresult2[["empName", "email"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Employee Name and Email Not found! {unique_c}",
                                }
                            )
                        else:
                            exceldata['employee']=projectGroupmermerged['employeeId']
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    dbData = [
                            {
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
                                    'as': 'customerResults'
                                }
                            }, {
                                '$project': {
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerResults.customerName', 0
                                        ]
                                    }, 
                                    'projectType': {
                                        '$toString': '$_id'
                                    }, 
                                    'projectTypeName': '$projectType', 
                                    'customer': {
                                        '$toString': '$custId'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("projectType", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])        

                        empNameCodeDatadp = empNameCodeDatadp.drop_duplicates(subset=["customerName", "projectTypeName"])

                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["customerName", "projectTypeName"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["projectType"].isna()]
                        unique_combinations = cresult2[["customerName", "projectTypeName"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Customer Name and Project Type Not found! {unique_c}",
                                }
                            )
                        else:
                            exceldata['projectType']=projectGroupmermerged['projectType']
                            exceldata['customer']=projectGroupmermerged['customer']
                            exceldata['projectTypeName']=projectGroupmermerged['projectTypeName']
                            
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    arr=[
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
                                'customerName':'$Customer'
                            }
                        }
                    ]
                    projectsdata=cmo.finding_aggregate("projectGroup",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])

                    projectsdataCodeDatadp = projectsdataCodeDatadp.drop_duplicates(subset=["customerName", "projectGroupName"])
                    
                    
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customerName", "projectGroupName"], how="left")
                    
                    # print('projectmermergedprojectmermerged',projectmermerged)
                    # print('projectmermergedprojectmermerged',projectmermerged.columns)
                    cresult2 = projectmermerged[projectmermerged["projectGroup"].isna()]
                    unique_combinations = cresult2[["customerName", "projectGroupName"]].drop_duplicates()
                    print('unique_combinationsunique_combinations',unique_combinations)
                    
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['projectGroup']=projectmermerged['projectGroup']
                    
                    
                    arr=[
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
                                }, 
                                'customerName': '$Customer'
                            }
                        }, {
                            '$lookup': {
                                'from': 'zone', 
                                'localField': 'zoneId', 
                                'foreignField': '_id', 
                                'as': 'zoneResult'
                            }
                        }, {
                            '$addFields': {
                                'circle': {
                                    '$arrayElemAt': [
                                        '$zoneResult.circle', 0
                                    ]
                                }
                            }
                        }, {
                            '$unwind': {
                                'path': '$circle'
                            }
                        }, {
                            '$lookup': {
                                'from': 'circle', 
                                'localField': 'circle', 
                                'foreignField': '_id', 
                                'as': 'circleResults'
                            }
                        }, {
                            '$project': {
                                'circle': {
                                    '$toString': '$circle'
                                }, 
                                'circleName': {
                                    '$arrayElemAt': [
                                        '$circleResults.circleName', 0
                                    ]
                                }, 
                                '_id': 0, 
                                'projectGroupName': 1
                            }
                        }
                    ]
                    projectsdata=cmo.finding_aggregate("projectGroup",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    
                    projectsdataCodeDatadp = projectsdataCodeDatadp.drop_duplicates(subset=["circleName", "projectGroupName"])
                    
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["circleName", "projectGroupName"], how="left")
                    
                    # print('projectmermergedprojectmermerged',projectmermerged)
                    # print('projectmermergedprojectmermerged',projectmermerged.columns)
                    cresult2 = projectmermerged[projectmermerged["circle"].isna()]
                    unique_combinations = cresult2[["circleName", "projectGroupName"]].drop_duplicates()
                    
                    
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Circle Name and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['circle']=projectmermerged['circle']
                    
                    jsonData = cfc.dfjson(exceldata)
                    Response=None
                    
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['projectGroup']):
                            i['projectGroup']=ObjectId(i['projectGroup'])
                        if is_valid_mongodb_objectid(i['projectType']):
                            i['projectType']=ObjectId(i['projectType'])
                        if is_valid_mongodb_objectid(i['customer']):
                            i['customer']=ObjectId(i['customer'])
                        if is_valid_mongodb_objectid(i['employee']):
                            i['employee']=ObjectId(i['employee'])
                        if is_valid_mongodb_objectid(i['circle']):
                            i['circle']=ObjectId(i['circle'])
                        i['ApproverType']="L1-Approver"
                        
                        Response=cmo.updating("ptwMdb",{'customer':i['customer'],'projectGroup':i['projectGroup'],'projectType':i['projectType'],'employee':i['employee'],'circle':i['circle'],'ApproverType':i['ApproverType'],'deleteStatus':{'$ne':1}},i,True)
                    return respond(Response)
                
                if fileType == "L2_Approver_MDB":
                    checkingFieldsArray=['empName', 'email', 'customerName', 'projectGroupName', 'projectTypeName','circleName']
                    errorlist=[]
                    checkingFieldsArray2=['empName', 'email', 'customerName', 'projectGroupName', 'projectTypeName','circleName']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    
                    dbData = [
                            {
                                '$match': {
                                    'type': {
                                        '$ne': 'Partner'
                                    }, 
                                    'deleteStatus': {
                                        '$ne': 1
                                    }, 
                                    'status': 'Active'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'userRole', 
                                    'localField': 'userRole', 
                                    'foreignField': '_id', 
                                    'pipeline': [
                                        {
                                            '$project': {
                                                'roleName': 1, 
                                                '_id': 0
                                            }
                                        }
                                    ], 
                                    'as': 'userResults'
                                }
                            }, {
                                '$project': {
                                    'email': 1, 
                                    'ustCode': 1, 
                                    'empName': 1, 
                                    'empCode': 1, 
                                    'employeeId': {
                                        '$toString': '$_id'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])             

                        empNameCodeDatadp = empNameCodeDatadp.drop_duplicates(subset=["empName", "email"])

                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empName", "email"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["employeeId"].isna()]
                        unique_combinations = cresult2[["empName", "email"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Employee Name and Email Not found! {unique_c}",
                                }
                            )
                        else:
                            exceldata['employee']=projectGroupmermerged['employeeId']
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    dbData = [
                            {
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
                                    'as': 'customerResults'
                                }
                            }, {
                                '$project': {
                                    'customerName': {
                                        '$arrayElemAt': [
                                            '$customerResults.customerName', 0
                                        ]
                                    }, 
                                    'projectType': {
                                        '$toString': '$_id'
                                    }, 
                                    'projectTypeName': '$projectType', 
                                    'customer': {
                                        '$toString': '$custId'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("projectType", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])           

                        empNameCodeDatadp = empNameCodeDatadp.drop_duplicates(subset=["customerName", "projectTypeName"])

                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["customerName", "projectTypeName"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["projectType"].isna()]
                        unique_combinations = cresult2[["customerName", "projectTypeName"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Customer Name and Project Type Not found! {unique_c}",
                                }
                            )
                        else:
                            exceldata['projectType']=projectGroupmermerged['projectType']
                            exceldata['customer']=projectGroupmermerged['customer']
                            exceldata['projectTypeName']=projectGroupmermerged['projectTypeName']
                            
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    arr=[
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
                                'customerName':'$Customer'
                            }
                        }
                    ]
                    projectsdata=cmo.finding_aggregate("projectGroup",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])

                    projectsdataCodeDatadp = projectsdataCodeDatadp.drop_duplicates(subset=["customerName", "projectGroupName"])
                    
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customerName", "projectGroupName"], how="left")
                    
                    # print('projectmermergedprojectmermerged',projectmermerged)
                    # print('projectmermergedprojectmermerged',projectmermerged.columns)
                    cresult2 = projectmermerged[projectmermerged["projectGroup"].isna()]
                    unique_combinations = cresult2[["customerName", "projectGroupName"]].drop_duplicates()
                    print('unique_combinationsunique_combinations',unique_combinations)
                    
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['projectGroup']=projectmermerged['projectGroup']
                    print(exceldata,'exceldataexceldataexceldataexceldata')
                    
                    arr=[
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
                                }, 
                                'customerName': '$Customer'
                            }
                        }, {
                            '$lookup': {
                                'from': 'zone', 
                                'localField': 'zoneId', 
                                'foreignField': '_id', 
                                'as': 'zoneResult'
                            }
                        }, {
                            '$addFields': {
                                'circle': {
                                    '$arrayElemAt': [
                                        '$zoneResult.circle', 0
                                    ]
                                }
                            }
                        }, {
                            '$unwind': {
                                'path': '$circle'
                            }
                        }, {
                            '$lookup': {
                                'from': 'circle', 
                                'localField': 'circle', 
                                'foreignField': '_id', 
                                'as': 'circleResults'
                            }
                        }, {
                            '$project': {
                                'circle': {
                                    '$toString': '$circle'
                                }, 
                                'circleName': {
                                    '$arrayElemAt': [
                                        '$circleResults.circleName', 0
                                    ]
                                }, 
                                '_id': 0, 
                                'projectGroupName': 1
                            }
                        }
                    ]
                   
                    projectsdata=cmo.finding_aggregate("projectGroup",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    
                    projectsdataCodeDatadp = projectsdataCodeDatadp.drop_duplicates(subset=["circleName", "projectGroupName"])
                    
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["circleName", "projectGroupName"], how="left")

                    cresult2 = projectmermerged[projectmermerged["circle"].isna()]
                    unique_combinations = cresult2[["circleName", "projectGroupName"]].drop_duplicates()
                    
                    
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Circle Name and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['circle']=projectmermerged['circle']
                    
                    jsonData = cfc.dfjson(exceldata)
                    Response=None
                    
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['projectGroup']):
                            i['projectGroup']=ObjectId(i['projectGroup'])
                        if is_valid_mongodb_objectid(i['projectType']):
                            i['projectType']=ObjectId(i['projectType'])
                        if is_valid_mongodb_objectid(i['customer']):
                            i['customer']=ObjectId(i['customer'])
                        if is_valid_mongodb_objectid(i['employee']):
                            i['employee']=ObjectId(i['employee'])
                        if is_valid_mongodb_objectid(i['circle']):
                            i['circle']=ObjectId(i['circle'])
                        i['ApproverType']="L2-Approver"
                        print(i,'shjhjshjs')
                        Response=cmo.updating("ptwMdb",{'customer':i['customer'],'projectGroup':i['projectGroup'],'projectType':i['projectType'],'employee':i['employee'],'circle':i['circle'],'ApproverType':i['ApproverType'],'deleteStatus':{'$ne':1}},i,True)
                    return respond(Response)

                
                 
                if fileType == "vendorCost":
                    checkingFieldsArray=['customer', 'projectGroup', 'vendorEmail', 'vendorId', 'projectType','subProject','rate']
                    errorlist=[]
                    checkingFieldsArray2=['customer', 'projectGroup', 'vendorEmail', 'vendorId', 'projectType','subProject','rate']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    
                    dbData = [
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
                                    'as': 'costCenterResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerId', 
                                    'foreignField': '_id', 
                                    'as': 'customerResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'zone', 
                                    'localField': 'zoneId', 
                                    'foreignField': '_id', 
                                    'as': 'zoneResult'
                                }
                            }, {
                                '$project': {
                                    'customerId': {
                                        '$toString': '$customerId'
                                    }, 
                                    'costCenterId': {
                                        '$toString': '$costCenterId'
                                    }, 
                                    'costCenter': {
                                        '$arrayElemAt': [
                                            '$costCenterResults.costCenter', 0
                                        ]
                                    }, 
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$customerResults.customerName', 0
                                        ]
                                    }, 
                                    'customerShort': {
                                        '$arrayElemAt': [
                                            '$customerResults.shortName', 0
                                        ]
                                    }, 
                                    'zone': {
                                        '$arrayElemAt': [
                                            '$zoneResult.shortCode', 0
                                        ]
                                    }, 
                                    '_id': 0, 
                                    'projectGroupId': {
                                        '$toString': '$_id'
                                    }
                                }
                            }, {
                                '$project': {
                                    'projectGroup': {
                                        '$concat': [
                                            '$customerShort', '-', '$zone', '-', '$costCenter'
                                        ]
                                    }, 
                                    'customer': 1, 
                                    'customerId': 1, 
                                    'projectGroupId': 1
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("projectGroup", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])                                 
                       
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["projectGroup", "customer"], how="left")
                        
                        cresult2 = projectGroupmermerged[projectGroupmermerged["projectGroupId"].isna()]
                        
                        unique_combinations = cresult2[["projectGroup", "customer"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Project Group and Customer not Found {unique_c}",
                                }
                            )
                        

                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    array=["rate"]
                    empty_rows = exceldata[array].apply(lambda x: x < 0 ).any(axis=1)
                    invalid_columns = exceldata.index[empty_rows].tolist()
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows Do not have a valid Rate {invalid_columns}",
                            }
                        )
                    arr=[
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
                                    'as': 'costCenterResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customerId', 
                                    'foreignField': '_id', 
                                    'as': 'customerResults'
                                }
                            }, {
                                '$lookup': {
                                    'from': 'zone', 
                                    'localField': 'zoneId', 
                                    'foreignField': '_id', 
                                    'as': 'zoneResult'
                                }
                            }, {
                                '$project': {
                                    'customerId': {
                                        '$toString': '$customerId'
                                    }, 
                                    'costCenterId': {
                                        '$toString': '$costCenterId'
                                    }, 
                                    'costCenter': {
                                        '$arrayElemAt': [
                                            '$costCenterResults.costCenter', 0
                                        ]
                                    }, 
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$customerResults.customerName', 0
                                        ]
                                    }, 
                                    'customerShort': {
                                        '$arrayElemAt': [
                                            '$customerResults.shortName', 0
                                        ]
                                    }, 
                                    'zone': {
                                        '$arrayElemAt': [
                                            '$zoneResult.shortCode', 0
                                        ]
                                    }, 
                                    '_id': 0, 
                                    'projectGroupId': {
                                        '$toString': '$_id'
                                    }
                                }
                            }, {
                                '$project': {
                                    'projectGroup': {
                                        '$concat': [
                                            '$customerShort', '-', '$zone', '-', '$costCenter'
                                        ]
                                    }, 
                                    'customer': 1, 
                                    'customerId': 1, 
                                    'projectGroupId': 1
                                }
                            }
                        ]
                    projectsdata=cmo.finding_aggregate("projectGroup",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customer", "projectGroup"], how="left")
                    cresult2 = projectmermerged[projectmermerged["projectGroupId"].isna()]
                    unique_combinations = cresult2[["customer", "projectGroup"]].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['customer']=projectmermerged['customerId']
                        exceldata['projectGroup']=projectmermerged['projectGroupId']
                        
                        
                    arr=[
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }
                                }
                            }, {
                                '$project': {
                                    'projectType': 1, 
                                    'subProject': 1, 
                                    'subProjectId': {
                                        '$toString': '$_id'
                                    }, 
                                    '_id': 0, 
                                    'customer': '$custId'
                                }
                            }
                        ]
                    projectsdata=cmo.finding_aggregate("projectType",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customer", "projectType",'subProject'], how="left")
                    cresult2 = projectmermerged[projectmermerged["subProjectId"].isna()]
                    unique_combinations = cresult2[["customer", "projectType",'subProject']].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['subProject']=projectmermerged['subProjectId']
                        exceldata['projectType']=projectmermerged['subProjectId']
                    
                    
                    
                    arr=[
                            {
                                '$match': {
                                    'type': 'Partner'
                                }
                            }, {
                                '$project': {
                                    'vendorId': {
                                        '$toString': '$vendorCode'
                                    }, 
                                    'vendorEmail': '$email', 
                                    'userId': {
                                        '$toString': '$_id'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    projectsdata=cmo.finding_aggregate("userRegister",arr)
                    exceldata['vendorId'] = exceldata['vendorId'].apply(lambda x: str(x) if pd.notna(x) and x != '' else x)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["vendorEmail", "vendorId"], how="left")
                    cresult2 = projectmermerged[projectmermerged["userId"].isna()]
                    unique_combinations = cresult2[["vendorEmail", "vendorId"]].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Vendor Email and Vendor Code not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['vendorId']=projectmermerged['userId']
                        
                    jsonData = cfc.dfjson(exceldata)
                    Response=None
                    
                    
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['customer']):
                            i['customerId']=ObjectId(i['customer'])
                        if is_valid_mongodb_objectid(i['projectGroup']):
                            i['projectGroup']=ObjectId(i['projectGroup'])
                        if is_valid_mongodb_objectid(i['subProject']):
                            i['subProjectId']=ObjectId(i['subProject'])
                        if is_valid_mongodb_objectid(i['vendorId']):
                            i['vendorId']=ObjectId(i['vendorId'])
                        if 'milestone' in i:
                            if i['milestone'] not in ['',None,'undefined']:
                                i['milestoneList']=i['milestone'].split(",")
                                
                                
                                
                        
                        Response=cmo.updating("vendorCostMilestone",{
                                        'customerId': i['customerId'],
                                        'projectGroup': i['projectGroup'],
                                        'vendorId': i['vendorId'],
                                        'subProjectId': i['subProjectId'],
                                        'itemCode': i['itemCode'],
                                        'rate': i['rate'],
                                        'GBPA': i['GBPA'],
                                        'customerItemCode': i['customerItemCode'],
                                        'activityName': i['activityName']
                                    },i,True)
                    return respond(Response)
                    
                if fileType == "AOP":
                    checkingFieldsArray=['month', 'year', 'customer', 'costCenter','ustProjectID','COGS','SGNA','actualRevenue','actualSalary','otherFixedCost','employeeExpanse']
                    errorlist=[]
                    checkingFieldsArray2=['month', 'year', 'customer', 'costCenter','ustProjectID','COGS','SGNA','actualRevenue','actualSalary','otherFixedCost','employeeExpanse']
                    #to check empty required columns 
                    requierscolumns=[]
                    print(exceldata['ustProjectID'],'djdjhjdhdhhjjhd')
                    for i in checkingFieldsArray:
                        if i not in exceldata.columns:
                            requierscolumns.append(i)
                    if len(requierscolumns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have in the excel {requierscolumns}",
                            }
                        )        
                            
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    error_row_numbers = [index + 2 for index in error_rows_indices]

                    if len(error_row_numbers):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have some value missing on these rows {error_row_numbers} for the fields: {checkingFieldsArray}",
                            }
                        )

                    exceldata['IsValidMonth&Year'] = exceldata.apply(is_valid_month_year, axis=1)
                    invalidMonth_Year = (exceldata.index[~exceldata['IsValidMonth&Year']].tolist())
                    invalidMonth_Year = [idx + 2 for idx in invalidMonth_Year] 
                    if len(invalidMonth_Year):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows No do not have valid Month and Year Value  {invalidMonth_Year} Month should be like this January,February... and Year 2025,2026,2027..",
                            }
                        )
                    exceldata['month_serial'] = exceldata['month'].map(monthMapping)
                    dbData = [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
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
                                    'as': 'customerResults'
                                }
                            }, {
                                '$project': {
                                    'costCenter': 1, 
                                    'costCenterId': {
                                        '$toString': '$_id'
                                    }, 
                                    'businessUnit': 1, 
                                    
                                    'ustProjectID':'$ustProjectId',
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$customerResults.customerName', 0
                                        ]
                                    }, 
                                    'customerId': {
                                        '$toString': '$customer'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("costCenter", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])                                 
                        # print('empNameCodeDatadpempNameCodeDatadpempNameCodeDatadp',empNameCodeDatadp)
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["customer", "costCenter",'businessUnit','ustProjectID'], how="left")
                        # print(projectGroupmermerged,'projectGroupmermergedprojectGroupmermerged')
                        cresult2 = projectGroupmermerged[projectGroupmermerged["customerId"].isna()]
                        # print('dhbbjhjbjhjhyduddyh',cresult2)
                        unique_combinations = cresult2[["customer", "costCenter",'businessUnit','ustProjectID']].drop_duplicates()
                        # print('jdhhyudek2kgyu74jh',unique_combinations)
                        unique_c = unique_combinations.values
                        # print('hbjrhuy4747374r87',unique_c)
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Cost Center and Customer,Business Unit and UST Project ID not Found {unique_c}",
                                }
                            )
                        
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    array=["planRevenue",'COGS','SGNA','actualRevenue','actualSalary','actualSGNA','actualVendorCost','otherFixedCost','employeeExpanse','miscellaneousExpenses','miscellaneousExpensesSecond']
                    for col in array:
                        if col in exceldata.columns:
                            exceldata[col] = exceldata[col].apply(lambda x: 0 if x is None or x == '' else x)
                    
                    
                    rows = []
                    for idx, row in exceldata.iterrows():
                        if any(not isinstance(row[col], (int, float)) for col in array):
                            rows.append(idx)
                    rows = [idx + 2 for idx in rows] 
                    if rows:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows do not have a valid number: {rows}.",
                            }
                        )

                    arr=[
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, 
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
                                'as': 'customerResults'
                            }
                        }, {
                            '$project': {
                                'costCenter': 1, 
                                'costCenterId': {
                                    '$toString': '$_id'
                                }, 
                                'businessUnit': 1,
                                'ustProjectID':'$ustProjectId',
                                
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customerResults.customerName', 0
                                    ]
                                }, 
                                'customerId': {
                                    '$toString': '$customer'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    projectsdata=cmo.finding_aggregate("costCenter",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customer", "costCenter",'businessUnit','ustProjectID'], how="left")
                    cresult2 = projectmermerged[projectmermerged["costCenterId"].isna()]
                    unique_combinations = cresult2[["customer", "costCenter",'businessUnit','ustProjectID']].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer,Cost Center,Business Unit and UST Project ID not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['customer']=projectmermerged['customerId']
                        exceldata['costCenter']=projectmermerged['costCenterId']
                    exceldata['month']=exceldata['month_serial']
                    exceldata['customerId']=exceldata['customer']
                    exceldata['costCenterId']=exceldata['costCenter']
                    # exceldata.drop("Actual SGNA")
                    if 'Actual SGNA' in exceldata:
                        exceldata = exceldata.drop('Actual SGNA', axis=1)
                    jsonData = cfc.dfjson(exceldata)
                    # print("fkmoefjoperifjkioerjfer=",jsonData)
                    Response=None
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['customerId']):
                            i['customerId']=ObjectId(i['customerId'])
                        if is_valid_mongodb_objectid(i['costCenterId']):
                            i['costCenterId']=ObjectId(i['costCenterId'])
                        Response=cmo.updating("AOP",{'customerId':ObjectId(i['customerId']),'costCenterId':ObjectId(i['costCenterId']),'year':i['year'],'month':i['month'],'deleteStatus':{'$ne':1}},i,True)
                    return respond(Response)
                    
                
                
                ######
                if fileType == "TEST":
                    arrr=[
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'projectId': 1, 
                                'projectUniqueID': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    Response=cmo.finding_aggregate("project",arrr)['data']
                    projectdb=None
                    if len(Response):
                        projectdb=pd.DataFrame(Response)
                        print(exceldata,'djjdjjdkdkj')
                        # projectmermerged = exceldata.merge(projectdb, on=["projectId"], how="left")
                        # print(projectmermerged,'djjdjjdkdkj2')
                        jsonData = cfc.dfjson(exceldata)
                        updatedCount=0
                        for i in jsonData:
                            cmo.updating("Expenses",{'_id':i['_id']},{'projectId':ObjectId(i['projectId'])},False)
                            cmo.updating("Approval",{'ExpenseUniqueId':i['_id']},{'projectId':ObjectId(i['projectId'])},False)
                            updatedCount=updatedCount+1
                        print('updatedCountupdatedCount',updatedCount)
                    
                    # if "Site Id" in exceldata:
                    #     del exceldata['Site Id']
                    # dict_data = exceldata.to_dict(orient="records")
                    # for i in dict_data:
                    #     updateBy = {
                    #         '_id':ObjectId(i['id'])
                    #     }
                    #     updateBy1 = {
                    #         'siteId':ObjectId(i['id'])
                    #     }
                    #     updateData = {
                    #         'systemId':i['systemId']
                    #     }
                    #     cmo.updating('SiteEngineer',updateBy,updateData,False)
                    #     cmo.updating_m('milestone',updateBy1,updateData,False)
                    
                    return respond({
                        'status':200,
                        "msg":'Data Updated Successfully',
                        "icon":'success'
                    })
                    
                if fileType == "ManageSalaryDB":
                    checkingFieldsArray=['customer', 'costCenter', 'year', 'month', 'cost']
                    errorlist=[]
                    checkingFieldsArray2=['customer', 'costCenter', 'year', 'month', 'cost']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    exceldata['IsValidMonth&Year'] = exceldata.apply(is_valid_month_year, axis=1)
                    invalidMonth_Year = (exceldata.index[~exceldata['IsValidMonth&Year']].tolist())
                    invalidMonth_Year = [idx + 2 for idx in invalidMonth_Year] 
                    if len(invalidMonth_Year):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows No do not have valid Month and Year Value  {invalidMonth_Year} Month should be like this January,Febuary... and Year 2025,2026,2027..",
                            }
                        )
                    exceldata['month_serial'] = exceldata['month'].map(monthMapping)
                    dbData = [
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
                                        }, {
                                            '$project': {
                                                'customerName': 1
                                            }
                                        }
                                    ], 
                                    'as': 'cusomerResult'
                                }
                            }, {
                                '$project': {
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$cusomerResult.customerName', 0
                                        ]
                                    }, 
                                    'costCenter': 1, 
                                    'customerId': {
                                        '$toString': '$customer'
                                    }, 
                                    'costCenterId': {
                                        '$toString': '$_id'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    empNameCodeData = cmo.finding_aggregate("costCenter", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])                                 
                        # print('empNameCodeDatadpempNameCodeDatadpempNameCodeDatadp',empNameCodeDatadp)
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["costCenter", "customer"], how="left")
                        # print(projectGroupmermerged,'projectGroupmermergedprojectGroupmermerged')
                        cresult2 = projectGroupmermerged[projectGroupmermerged["customerId"].isna()]
                        # print('dhbbjhjbjhjhyduddyh',cresult2)
                        unique_combinations = cresult2[["costCenter", "customer"]].drop_duplicates()
                        # print('jdhhyudek2kgyu74jh',unique_combinations)
                        unique_c = unique_combinations.values
                        # print('hbjrhuy4747374r87',unique_c)
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Cost Center and Customer not Found {unique_c}",
                                }
                            )
                        

                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    array=["cost"]
                    empty_rows = exceldata[array].apply(lambda x: x < 0 ).any(axis=1)
                    invalid_columns = exceldata.index[empty_rows].tolist()
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows Do not have a valid Salary{invalid_columns}",
                            }
                        )
                    arr=[
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
                                        }, {
                                            '$project': {
                                                'customerName': 1
                                            }
                                        }
                                    ], 
                                    'as': 'cusomerResult'
                                }
                            }, {
                                '$project': {
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$cusomerResult.customerName', 0
                                        ]
                                    }, 
                                    'costCenter': 1, 
                                    'customerId': {
                                        '$toString': '$customer'
                                    }, 
                                    'costCenterId': {
                                        '$toString': '$_id'
                                    }, 
                                    '_id': 0
                                }
                            }
                        ]
                    projectsdata=cmo.finding_aggregate("costCenter",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customer", "costCenter"], how="left")
                    cresult2 = projectmermerged[projectmermerged["costCenterId"].isna()]
                    unique_combinations = cresult2[["customer", "costCenter"]].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer and Project Group not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['customer']=projectmermerged['customerId']
                        exceldata['costCenter']=projectmermerged['costCenterId']
                    exceldata['month']=exceldata['month_serial']
                    jsonData = cfc.dfjson(exceldata)
                    print(jsonData,'jsonDatajsonDatajsonDatajsonData')
                    Response=None
                    
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['customer']):
                            i['customer']=ObjectId(i['customer'])
                        if is_valid_mongodb_objectid(i['costCenter']):
                            i['costCenter']=ObjectId(i['costCenter'])
                        Response=cmo.updating("SalaryDB",{'customer':ObjectId(i['customer']),'costCenter':ObjectId(i['costCenter']),'year':i['year'],'month':i['month']},i,True)
                    return respond(Response)
                    
                    
                if fileType == "OtherFixedCost":
                    checkingFieldsArray=['customer', 'costCenter', 'year', 'month', 'cost','costType']
                    errorlist=[]
                    checkingFieldsArray2=['customer', 'costCenter', 'year', 'month', 'cost','costType']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    exceldata['IsValidMonth&Year'] = exceldata.apply(is_valid_month_year, axis=1)
                    invalidMonth_Year = (exceldata.index[~exceldata['IsValidMonth&Year']].tolist())
                    invalidMonth_Year = [idx + 2 for idx in invalidMonth_Year] 
                    if len(invalidMonth_Year):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows No do not have valid Month and Year Value  {invalidMonth_Year} Month should be like this January,Febuary... and Year 2025,2026,2027..",
                            }
                        )
                    exceldata['month_serial'] = exceldata['month'].map(monthMapping)
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'zone', 
                                'localField': 'zone', 
                                'foreignField': '_id', 
                                'as': 'zoneResult'
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
                                'as': 'customerResults'
                            }
                        }, {
                            '$project': {
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResult.shortCode', 0
                                    ]
                                }, 
                                'zoneId': {
                                    '$toString': '$zone'
                                }, 
                                '_id': 0, 
                                'costCenter': 1, 
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customerResults.customerName', 0
                                    ]
                                }, 
                                'customerId': {
                                    '$toString': '$customer'
                                }, 
                                'costCenterId': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ]
                    empNameCodeData = cmo.finding_aggregate("costCenter", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])                                 
                        # print('empNameCodeDatadpempNameCodeDatadpempNameCodeDatadp',empNameCodeDatadp)
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["costCenter", "customer"], how="left")
                        # print(projectGroupmermerged,'projectGroupmermergedprojectGroupmermerged')
                        cresult2 = projectGroupmermerged[projectGroupmermerged["costCenterId"].isna()]
                        # print('dhbbjhjbjhjhyduddyh',cresult2)
                        unique_combinations = cresult2[["costCenter", "customer"]].drop_duplicates()
                        # print('jdhhyudek2kgyu74jh',unique_combinations)
                        unique_c = unique_combinations.values
                        # print('hbjrhuy4747374r87',unique_c)
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Cost Center and Customer not Found {unique_c}",
                                }
                            )
                        

                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    
                    array=["cost"]
                    empty_rows = exceldata[array].apply(lambda x: x < 0 ).any(axis=1)
                    invalid_columns = exceldata.index[empty_rows].tolist()
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows Do not have a valid Cost {invalid_columns}",
                            }
                        )
                    arr=[
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'zone', 
                                'localField': 'zone', 
                                'foreignField': '_id', 
                                'as': 'zoneResult'
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
                                'as': 'customerResults'
                            }
                        }, {
                            '$project': {
                                'zone': {
                                    '$arrayElemAt': [
                                        '$zoneResult.shortCode', 0
                                    ]
                                }, 
                                'zoneId': {
                                    '$toString': '$zone'
                                }, 
                                '_id': 0, 
                                'costCenter': 1, 
                                'customer': {
                                    '$arrayElemAt': [
                                        '$customerResults.customerName', 0
                                    ]
                                }, 
                                'customerId': {
                                    '$toString': '$customer'
                                }, 
                                'costCenterId': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ]
                    projectsdata=cmo.finding_aggregate("costCenter",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customer", "costCenter"], how="left")
                    cresult2 = projectmermerged[projectmermerged["costCenterId"].isna()]
                    unique_combinations = cresult2[["customer", "costCenter"]].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer and Cost Center not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['customer']=projectmermerged['customerId']
                        exceldata['costCenter']=projectmermerged['costCenterId']
                        
                        
                    arr=[
                            {
                                '$project': {
                                    'costTypeId': {
                                        '$toString': '$_id'
                                    }, 
                                    'costType': 1, 
                                    '_id': 0
                                }
                            }
                        ]
                    projectsdata=cmo.finding_aggregate("OtherCostTypes",arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["costType"], how="left")
                    cresult2 = projectmermerged[projectmermerged["costTypeId"].isna()]
                    unique_combinations = cresult2[["costType"]].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"This Cost Type Not Found in Database {unique_c}",
                            }
                        )
                    else:
                        exceldata['costType']=projectmermerged['costTypeId']
                        
                    exceldata['month']=exceldata['month_serial']
                    jsonData = cfc.dfjson(exceldata)
                    Response=None
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['customer']):
                            i['customer']=ObjectId(i['customer'])
                        if is_valid_mongodb_objectid(i['costCenter']):
                            i['costCenter']=ObjectId(i['costCenter'])
                        if is_valid_mongodb_objectid(i['costType']):
                            i['costType']=ObjectId(i['costType'])
                        Response=cmo.updating("OtherFixedCost",{'customer':ObjectId(i['customer']),'costCenter':ObjectId(i['costCenter']),'costType':ObjectId(i['costType']),'year':i['year'],'month':i['month']},i,True)
                    return respond(Response)
                    
                
                    
                if fileType == "SettlementAmount":
                    checkingFieldsArray=['empCode', 'email', 'claimType', 'projectId', 'Amount', 'remark']
                    errorlist=[]
                    checkingFieldsArray2=['empCode', 'email', 'Amount']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    dbData = [
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'email': 1, 
                                'empCode': 1
                            }
                        }
                    ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empCode", "email"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["_id"].isna()]
                        unique_combinations = cresult2[["empCode", "email"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Employee Code and Email not Found {unique_c}",
                                }
                            )
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    

                    if "empCode" in exceldata.columns:
                        dbData = [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    },
                                    'type': {
                                        '$ne': 'Partner'
                                    }
                                }
                            },
                            
                            {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    },
                                    'empCode': 1,
                                    'empID':{
                                        '$toString': '$_id'
                                    },
                                    
                                }
                            }
                        ]
                        try:
                            data = cmo.finding_aggregate("userRegister", dbData)
                            if len(data['data']) > 0:
                                empCodedp = pd.DataFrame.from_dict(data["data"])
                                filtered_exceldata = exceldata[(exceldata["empCode"].notna()) & (exceldata["empCode"] != "")]
                                empCodemerged = filtered_exceldata.merge(empCodedp, on=["empCode"], how="left")
                                cresult2 = empCodemerged[empCodemerged["_id"].isna()]
                                unique_c = cresult2["empCode"].unique()
                                if len(unique_c) > 0:
                                    missing_rows = []
                                    # for claimType_name in unique_c:
                                    #     missing_indices = cresult2[cresult2["empCode"] == claimType_name].index.tolist()
                                    #     missing_row_numbers = [idx + 2 for idx in missing_indices]
                                    #     missing_rows.extend(missing_row_numbers)
                                    # errorlist.append(
                                    #     f"Designation not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                    # )
                                else:
                                    
                                    exceldata.loc[~empCodemerged["empID"].isna(), "empID"] = empCodemerged["empID"]   
                            else:
                                errorlist.append("No empCode Found in ManageEmployee")
                        except Exception as e:  
                            errorlist.append(f"Error while processing data: {str(e)}")
                    else:
                        errorlist.append("Column 'empCode' not found in exceldata")
                    print('33333',current_time) 
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data1 {errorlist}",
                            }
                        )
                    
                    
                    
                    if 'approvalDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['approvalDate']) or row['approvalDate'] == "":
                                exceldata.at[index, 'approvalDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['approvalDate'] = pd.to_datetime(row['approvalDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Approval Date format is invaid in these rows{invalidRows}") 
                    if 'SettlementRequisitionDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['SettlementRequisitionDate']) or row['SettlementRequisitionDate'] == "":
                                exceldata.at[index, 'SettlementRequisitionDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['SettlementRequisitionDate'] = pd.to_datetime(row['SettlementRequisitionDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Settlement Requisition Date format is invaid in these rows{invalidRows}") 
                    if len(errorlist) > 0:
                        response = {
                            "status": 400,
                            "icon": "error",
                            "msg": f"These Rows do not have valid data: {errorlist}",
                        }
                    jsonData = cfc.dfjson(exceldata)
                    checkingarray2=['',None,'undefined']
                    exceldata['approvalDate'] = exceldata['approvalDate'].apply(
                        lambda x: make_timestamp(x) if x not in checkingarray2 else x)
                    exceldata['SettlementRequisitionDate'] = exceldata['SettlementRequisitionDate'].apply(
                        lambda x: make_timestamp(x) if x not in checkingarray2 else x)
                    dict_data = cfc.dfjson(exceldata)
                    thread = Thread(target=background_Settlement_Uploads, args=(dict_data,pathing))
                    thread.start()
                    return respond({
                            "status": 200,
                            "icon": "success",
                            "msg": f"Your file is in process when its completed we will notify",
                        })
                    return respond(Response)
                

                if fileType == "ManageClaims":
                    checkingFieldsArray=['empCode', 'email', 'claimType', 'projectId', 'Amount', 'remark']
                    errorlist=[]
                    checkingFieldsArray2=['empCode', 'email', 'claimType', 'projectId', 'Amount']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    dbData = [
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'email': 1, 
                                'empCode': 1
                            }
                        }
                    ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empCode", "email"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["_id"].isna()]
                        unique_combinations = cresult2[["empCode", "email"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Employee Code and Email not Found {unique_c}",
                                }
                            )
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                       
                    # if "empCode" in exceldata.columns:
                    #     dbData = [
                    #         {
                    #             '$match': {
                    #                 'deleteStatus': {'$ne': 1}, 
                    #                 'type': {'$ne': 'Partner'}
                    #             }
                    #         }, 
                    #         {
                    #             '$project': {
                    #                 '_id': {'$toString': '$_id'}, 
                    #                 'empCode': 1,
                    #                 'designation': 1
                    #             }
                    #         }
                    #     ]
                    #     empCode = cmo.finding_aggregate("userRegister", dbData)
                    #     if len(empCode['data']):
                    #         empCodedp = pd.DataFrame.from_dict(empCode["data"])
                    #         filtered_exceldata = exceldata[
                    #             (exceldata["empCode"].notna()) & (exceldata["empCode"] != "")
                    #         ]
                    #         empCodemerged = filtered_exceldata.merge(empCodedp, on=["empCode"], how="left")
                    #         cresult2 = empCodemerged[empCodemerged["_id"].isna()]
                    #         unique_c = cresult2["empCode"].unique()
                    #         missing_designation = empCodemerged[empCodemerged["designation"].isna()]
                    #         if not missing_designation.empty:
                    #             unique_c = missing_designation["empCode"].unique()
                    #             if len(unique_c) > 0:
                    #                 missing_rows = []
                    #                 for claimType_name in unique_c:
                    #                     missing_indices = missing_designation[missing_designation["empCode"] == claimType_name].index.tolist()
                    #                     missing_row_numbers = [idx + 2 for idx in missing_indices]
                    #                     missing_rows.extend(missing_row_numbers)
                                    
                    #                 errorlist.append(
                    #                 f"Designation not found for Row numbers: {', '.join(map(str, missing_rows))}"
                    #                 )

                    if "empCode" in exceldata.columns:
                        dbData = [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    },
                                    'type': {
                                        '$ne': 'Partner'
                                    }
                                }
                            },
                            
                            {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    },
                                    'empCode': 1,
                                    'designation': 1,
                                    
                                }
                            }
                        ]
                        try:
                            data = cmo.finding_aggregate("userRegister", dbData)
                            if len(data['data']) > 0:
                                empCodedp = pd.DataFrame.from_dict(data["data"])
                                filtered_exceldata = exceldata[(exceldata["empCode"].notna()) & (exceldata["empCode"] != "")]
                                empCodemerged = filtered_exceldata.merge(empCodedp, on=["empCode"], how="left")
                                cresult2 = empCodemerged[empCodemerged["_id"].isna()]
                                unique_c = cresult2["empCode"].unique()
                                if len(unique_c) > 0:
                                    missing_rows = []
                                    for claimType_name in unique_c:
                                        missing_indices = cresult2[cresult2["empCode"] == claimType_name].index.tolist()
                                        missing_row_numbers = [idx + 2 for idx in missing_indices]
                                        missing_rows.extend(missing_row_numbers)
                                    errorlist.append(
                                        f"Designation not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                    )
                                else:
                                    exceldata.loc[~empCodemerged["_id"].isna(), "addedFor"] = empCodemerged["_id"]
                                    exceldata.loc[~empCodemerged["designation"].isna(), "designation"] = empCodemerged["designation"]   
                            else:
                                errorlist.append("No empCode Found in ManageEmployee")
                        except Exception as e:  
                            errorlist.append(f"Error while processing data: {str(e)}")
                    else:
                        errorlist.append("Column 'empCode' not found in exceldata")
                    print('33333',current_time) 
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data1 {errorlist}",
                            }
                        )
                    print('44444',current_time) 
                    if "claimType" in exceldata.columns:
                        dbData = [
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
                                        'claimType': 1,
                                        'categoriesType':1,
                                        'type':"$categoriesType"
                                    }
                                }
                            ]
                        claimType = cmo.finding_aggregate("claimType", dbData)
                        if len(claimType['data']):
                            claimTypedp = pd.DataFrame.from_dict(claimType["data"])
                            filtered_exceldata = exceldata[(exceldata["claimType"].notna())& (exceldata["claimType"] != "")]
                            claimTypemerged = filtered_exceldata.merge(claimTypedp, on=["claimType"], how="left")
                            cresult2 = claimTypemerged[claimTypemerged["_id"].isna()]
                            unique_c = cresult2["claimType"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for claimType_name in unique_c:
                                    missing_indices = cresult2[cresult2["claimType"] == claimType_name].index.tolist()
                                    missing_row_numbers = [idx + 2 for idx in missing_indices]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(f"claimType not found: {', '.join(unique_c)}. Row numbers: {','.join(map(str, missing_rows))}")
                            else:
                                claimType_dict = claimTypemerged.set_index("claimType")["_id"].to_dict()
                                exceldata['type']=claimTypemerged["type"] 
                                exceldata["claimType"] = exceldata["claimType"].map(claimType_dict)         
                        else:
                            errorlist.append(f"No claimType Found in Claim Type")
                    print('5555',current_time)        
                    if "L1Approver" in exceldata.columns:
                        dbData = [
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
                            '$project': {
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'L1Approver':'$email'
                            }
                        }
                    ]
                        L1Approver = cmo.finding_aggregate("userRegister", dbData)
                        L1Approverdp = pd.DataFrame.from_dict(L1Approver["data"])
                        filtered_exceldata = exceldata[(exceldata["L1Approver"].notna())& (exceldata["L1Approver"] != "")]
                        L1Approvermerged = filtered_exceldata.merge(L1Approverdp, on=["L1Approver"], how="left")
                        cresult2 = L1Approvermerged[L1Approvermerged["_id"].isna()]
                        unique_c = cresult2["L1Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for L1Approver_name in unique_c:
                                missing_indices = cresult2[cresult2["L1Approver"] == L1Approver_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"L1-Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                        else:
                            circle_dict = L1Approvermerged.set_index("L1Approver")["_id"].to_dict()
                            exceldata["L1Approver"] = exceldata["L1Approver"].map(circle_dict)       
                    if "L2Approver" in exceldata.columns:
                        dbData = [
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
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    }, 
                                    'L2Approver':'$email'
                                }
                            }
                        ]
                        L2Approver = cmo.finding_aggregate("userRegister", dbData)
                        L2Approverdp = pd.DataFrame.from_dict(L2Approver["data"])
                        filtered_exceldata = exceldata[(exceldata["L2Approver"].notna())& (exceldata["L2Approver"] != "")]
                        L2Approvermerged = filtered_exceldata.merge(L2Approverdp, on=["L2Approver"], how="left")
                        cresult2 = L2Approvermerged[L2Approvermerged["_id"].isna()]
                        unique_c = cresult2["L2Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for L2Approver_name in unique_c:
                                missing_indices = cresult2[cresult2["L2Approver"] == L2Approver_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"L1-Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                        else:
                            circle_dict = L2Approvermerged.set_index("L2Approver")["_id"].to_dict()
                            exceldata["L2Approver"] = exceldata["L2Approver"].map(circle_dict)     
                    if "L3Approver" in exceldata.columns:
                        dbData = [
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
                                    '$project': {
                                        '_id': {
                                            '$toString': '$_id'
                                        }, 
                                        'L3Approver':'$email'
                                    }
                                }
                            ]
                        L3Approver = cmo.finding_aggregate("userRegister", dbData)
                        L3Approverdp = pd.DataFrame.from_dict(L3Approver["data"])
                        filtered_exceldata = exceldata[(exceldata["L3Approver"].notna())& (exceldata["L3Approver"] != "")]
                        L3Approvermerged = filtered_exceldata.merge(L3Approverdp, on=["L3Approver"], how="left")
                        cresult2 = L3Approvermerged[L3Approvermerged["_id"].isna()]
                        unique_c = cresult2["L3Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for L3Approver_name in unique_c:
                                missing_indices = cresult2[cresult2["L3Approver"] == L3Approver_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"L1-Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                        else:
                            circle_dict = L3Approvermerged.set_index("L3Approver")["_id"].to_dict()
                            exceldata["L3Approver"] = exceldata["L3Approver"].map(circle_dict)
                    print('666666',current_time) 
                    if "projectId" in exceldata.columns:
                        dbData = [
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
                                            'projectId': 1
                                        }
                                    }
                                ]
                        project = cmo.finding_aggregate("project", dbData)
                        if len(project['data']):
                            projectdp = pd.DataFrame.from_dict(project["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["projectId"].notna())
                                & (exceldata["projectId"] != "")
                            ]
                            projectmerged = filtered_exceldata.merge(
                                projectdp, on=["projectId"], how="left"
                            )
                            cresult2 = projectmerged[
                                projectmerged["_id"].isna()
                            ]
                            unique_c = cresult2["projectId"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for project_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["projectId"] == project_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"projectId not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                            else:
                                exceldata["projectId"] = projectmerged["_id"]
                        else:
                            errorlist.append(f"No project Found in Manage Project")
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data {errorlist}",
                            }
                        ) 
                    # valid_values = ['L1-Approved', 'L2-Approved', 'L3-Approved',
                    #                 'L1-Rejected', 'L2-Rejected', 'L3-Rejected', 'Submitted']                               
                    # for index, row in exceldata.iterrows():
                    #     has_valid_value = False
                    #     has_invalid_value = False
                    #     for col in ['L1Status', 'L2Status', 'L3Status']:
                    #         if row[col] in valid_values:
                    #             has_valid_value = True
                    #         else:
                    #             has_invalid_value = True
                        
                    #     if has_invalid_value and has_valid_value:
                    #         errorlist.append(f'Row Number: {index + 2}, contains invalid values in L1Status or  L2Status or L3Status. Valid values are: {valid_values}')
                    # if len(errorlist) >1:
                    #     return respond(
                    #         {
                    #             "status": 400,
                    #             "icon": "error",
                    #             "msg": f"These Rows does not have valid data{errorlist}",
                    #         }
                    #     )
                    
                    if 'submssionDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['submssionDate']) or row['submssionDate'] == "":
                                exceldata.at[index, 'submssionDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['submssionDate'] = pd.to_datetime(row['submssionDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"submssionDate format is invaid in these rows{invalidRows}") 
                    if 'expenseDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['expenseDate']) or row['expenseDate'] == "":
                                exceldata.at[index, 'expenseDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['expenseDate'] = pd.to_datetime(row['expenseDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"expenseDate format is invaid in these rows{invalidRows}") 
                    if 'actionAt' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['actionAt']) or row['actionAt'] == "":
                                exceldata.at[index, 'actionAt'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['actionAt'] = pd.to_datetime(row['actionAt'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Last Action Date format is invaid in these rows{invalidRows}") 
                    if len(errorlist) > 0:
                        response = {
                            "status": 400,
                            "icon": "error",
                            "msg": f"These Rows do not have valid data: {errorlist}",
                        }
                    valid_values = ['L1-Approved', 'L2-Approved', 'L3-Approved',
                                    'L1-Rejected', 'L2-Rejected', 'L3-Rejected', 'Submitted']
                    # exceldata = pd.DataFrame(data)
                    has_valid_value = exceldata[['L1Status', 'L2Status', 'L3Status']].isin(valid_values).any(axis=1)
                    has_invalid_value = ~exceldata[['L1Status', 'L2Status', 'L3Status']].isin(valid_values).all(axis=1)
                    error_rows = exceldata[has_valid_value & has_invalid_value].index + 2
                    errorlist = [f'Row Number: {row}, contains invalid values in L1Status or L2Status or L3Status. Valid values are: {valid_values}' for row in error_rows]
                    if len(errorlist) > 0:
                        return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": f"Plase Enter valid status for Approvers in these rows{errorlist}",
                                    }
                                )
                    
                    print('77777',current_time)
                    def convert_to_string(value):
                        
                        if isinstance(value, float):
                            if value.is_integer():
                                return str(int(value))
                            else:
                                return str(value)
                        return str(value)
                    if 'Site Id' in exceldata:
                        
                        exceldata['Site Id'] = exceldata['Site Id'].apply(convert_to_string)
                    if 'systemId' in exceldata:
                        exceldata['systemId'] = exceldata['systemId'].apply(convert_to_string)
                    if 'systemId' in exceldata.columns and 'Site Id' in exceldata.columns:
                        arr=[
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'Site Id': {
                                    '$toString': '$Site Id'
                                }, 
                                'systemId': 1, 
                                '_id': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ]
                        systemId = cmo.finding_aggregate("SiteEngineer", arr)
                        # if len(systemId['data']):
                        #     systemIddp = pd.DataFrame.from_dict(systemId["data"])
                        #     filtered_exceldata = exceldata[(exceldata["systemId"].notna())& (exceldata["systemId"] != "")]
                        #     systemIdpmerged = filtered_exceldata.merge(systemIddp, on=["systemId","Site Id"], how="left")
                        #     cresult2 = systemIdpmerged[systemIdpmerged["systemId"].isna()]
                        #     unique_combinations = cresult2[["systemId", "Site Id"]].drop_duplicates()
                        #     unique_c = unique_combinations.values
                        #     addedFor_not_found = systemIdpmerged.apply(lambda row: (isinstance(row['assignerId'], (list, str)) and row['addedFor'] not in row['assignerId']) if pd.notna(row['addedFor']) else False,axis=1)
                        #     if len(unique_c) > 0 or addedFor_not_found.any():
                        #         error_msg = "These Pairs of systemId and Site Id not Found"
                        #         if addedFor_not_found.any():
                        #             not_found_pairs = systemIdpmerged[addedFor_not_found][["systemId", "Site Id"]].drop_duplicates().values
                        #             error_msg += f" or addedFor ID not found in assignerId: {not_found_pairs}"
                        #         return respond(
                        #             {
                        #                 "status": 400,
                        #                 "icon": "error",
                        #                 "msg": f"{error_msg} {unique_c}",
                        #             }
                        #         )
                        #     else:
                        #         systemId_dict = systemIdpmerged.set_index("systemId")["_id"].to_dict()
                        #         exceldata["Site Id"] = exceldata["systemId"].map(systemId_dict)
                        # else:
                        #     errorlist.append('No Site assigned to any user')
                        if len(systemId['data']):
                            systemIddp = pd.DataFrame.from_dict(systemId["data"])
                            
                            filtered_exceldata = exceldata[(exceldata["systemId"].notna()) & (exceldata["systemId"] != "")]
                            systemIdpmerged = filtered_exceldata.merge(systemIddp, on=["systemId", "Site Id"], how="left")
                            cresult2 = systemIdpmerged[systemIdpmerged["_id"].isna()]
                            unique_combinations = cresult2[["systemId", "Site Id"]].drop_duplicates()
                            unique_c = unique_combinations.values
                            if len(unique_c) > 0:
                                error_msg = "These pairs of systemId and Site Id not found"
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": f"{error_msg} {unique_c}",
                                    }
                                )
                            else:
                                systemId_dict = systemIdpmerged.set_index("systemId")["_id"].to_dict()
                                # print(systemId_dict,'systemId_dictsystemId_dictsystemId_dict')
                                
                                exceldata["Site Id"] = exceldata["systemId"].map(systemId_dict)
                        else:
                            errorlist.append('No Site assigned to any user')
                    
                    # print('jkljhjkl',exceldata['Site Id'])
                    
                    # if 'systemId' in exceldata.columns and 'taskName' in exceldata.columns:
                    #     dbData = [
                    #         {
                    #             '$project': {
                    #                 'systemId': 1,
                    #                 '_id': { '$toString': '$_id' },
                    #                 'taskName': '$Name',
                    #                 'assignerId': {
                    #                     '$map': {
                    #                         'input': '$assignerId',
                    #                         'as': 'assignerId',
                    #                         'in': { '$toString': '$$assignerId' }
                    #                     }
                    #                 }
                    #             }
                    #         }
                    #     ]
                    #     milestoneData = cmo.finding_aggregate("milestone", dbData)
                    #     if len(milestoneData):
                    #         milestoneDatadp = pd.DataFrame.from_dict(milestoneData["data"])
                    #         milestoneDatamerged = exceldata.merge(milestoneDatadp, on=["systemId", "taskName"], how="left")
                    #         cresult2 = milestoneDatamerged[milestoneDatamerged["_id"].isna()]
                    #         unique_combinations = cresult2[["systemId", "taskName"]].drop_duplicates()
                    #         unique_c = unique_combinations.values
                    #         addedFor_not_found = milestoneDatamerged.apply(
                    #                 lambda row: (
                    #                     isinstance(row['assignerId'], (list, str)) and 
                    #                     row['addedFor'] not in row['assignerId']
                    #                 ) if pd.notna(row['addedFor']) else False,
                    #                 axis=1
                    #             )
                    #         if len(unique_c) > 0 and addedFor_not_found.any():
                    #             error_msg = "These Pairs of systemId and taskName not Found"
                    #             if addedFor_not_found.any():
                    #                 not_found_pairs = milestoneDatamerged[addedFor_not_found][["systemId", "taskName"]].drop_duplicates().values
                    #                 not_found_rows = milestoneDatamerged[addedFor_not_found].index + 2  
                    #                 error_msg += f" or addedFor ID not found in assignerId: {not_found_pairs}, Row numbers: {', '.join(map(str, not_found_rows))}"
                    #             return respond(
                    #                 {
                    #                     "status": 400,
                    #                     "icon": "error",
                    #                     "msg": f"{error_msg} {unique_c}",
                    #                 }
                    #             )
                    #         else:
                    #             claimType_dict = milestoneDatamerged.set_index("systemId")["_id"].to_dict()
                    #             exceldata["taskName"] = exceldata["systemId"].map(claimType_dict)
                    #     else:
                    #         errorlist.append("No Data Found In Database")   
                    print('99999',current_time)
                    # if 'expenseDate' in exceldata.columns:
                    #     invalidRows = []
                    #     for index, row in exceldata.iterrows():
                    #         if pd.isna(row['expenseDate']) or row['expenseDate'] == '':
                    #             exceldata.at[index, 'expenseDate'] = None
                    #         else:
                    #             try:
                    #                 # Define expected date format
                    #                 expected_format = '%d-%m-%Y'
                                    
                    #                 # Parse the date string
                    #                 row['expenseDate'] = pd.to_datetime(row['expenseDate'], format=expected_format, errors='raise')
                                    
                    #                 # Convert to timestamp string
                    #                 exceldata.at[index, 'expenseDate'] = convert_to_timestamp4(row['expenseDate'])
                    #             except Exception as e:
                    #                 # Append index of rows with invalid date formats to the list
                    #                 invalidRows.append(index + 2)  # +2 to account for 1-based indexing and header row

                    #     if len(invalidRows):
                    #         errorlist.append(f"Claim Date format is invalid in these rows: {invalidRows}")
                    
                    
                    
                    if 'systemId' in exceldata.columns and 'taskName' in exceldata.columns:
                        arr=[
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    }
                                }
                            }, {
                                '$project': {
                                    'taskName': '$Name', 
                                    'systemId': '$systemId', 
                                    '_id': {
                                        '$toString': '$_id'
                                    }
                                }
                            }
                        ]
                        systemId = cmo.finding_aggregate("milestone", arr)
                        if len(systemId['data']):
                            systemIddp = pd.DataFrame.from_dict(systemId["data"])
                            filtered_exceldata = exceldata[(exceldata["systemId"].notna()) & (exceldata["systemId"] != "")]
                            systemIdpmerged = filtered_exceldata.merge(systemIddp, on=["systemId", "taskName"], how="left")
                            cresult2 = systemIdpmerged[systemIdpmerged["_id"].isna()]
                            unique_combinations = cresult2[["systemId", "taskName"]].drop_duplicates()
                            unique_c = unique_combinations.values
                            if len(unique_c) > 0:
                                error_msg = "These pairs of System Id and Task not found"
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": f"{error_msg} {unique_c}",
                                    }
                                )
                            else:
                                systemId_dict = systemIdpmerged.set_index("systemId")["_id"].to_dict()
                                exceldata["taskName"] = exceldata["systemId"].map(systemId_dict)
                        else:
                            errorlist.append('No Site assigned to any user')
                                     
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data4 {errorlist}",
                            }
                        ) 
                    def ObjectIdd(id_str):
                        return id_str 
                     
                    jsonData = cfc.dfjson(exceldata)
                    # print('jsonDatajsonDatajsonData',jsonData)
                    # Predefined values
                    valid_values = ['L1-Approved', 'L2-Approved', 'L3-Approved',
                                    'L1-Rejected', 'L2-Rejected', 'L3-Rejected', 'Submitted']
                    checkingarray = [None, '', 'undefined']
                    checkingarray2 = [None, '', 'undefined']
                    claimTypecheckingArray = ['Expense', 'Daily Allowance']

                    # Step 1: Update Approver IDs
                    for col in ['L1', 'L2', 'L3']:
                        exceldata[f"{col}ApproverId"] = exceldata[f"{col}Approver"]
                        exceldata[f"{col}Approver"] = [[] for _ in range(len(exceldata))]
                    print('20201011',current_time)
                    # Step 2: Determine status and customStatus
                    exceldata['status'] = exceldata.apply(
                        lambda x: x['L3Status'] if x['L3Status'] not in checkingarray else
                        (x['L2Status'] if x['L2Status'] not in checkingarray else x['L1Status']), axis=1)
                    print('30201011',current_time)
                    custom_status_mapping = {
                        "L1-Approved": 2,
                        "L1-Rejected": 3,
                        "L2-Approved": 4,
                        "L2-Rejected": 5,
                        "L3-Approved": 6,
                        "L3-Rejected": 7,
                    }
                    
                    exceldata['customStatus'] = exceldata['status']
                    exceldata['customisedStatus'] = exceldata['customStatus'].map(custom_status_mapping).fillna(1)
                    def create_approver_action(row, col):
                        if row[f'{col}Status'] not in checkingarray:
                            return [{
                                "actionBy": ObjectIdd(row[f'{col}ApproverId']),
                                "actionAt": get_current_date_timestamp(),
                                "status": row[f'{col}Status'],
                                "customisedStatus": row['customisedStatus']
                            }]
                        return []
                    
                    for col in ['L1', 'L2', 'L3']:
                        exceldata[f"{col}Approver"] = exceldata.apply(lambda row, col=col: create_approver_action(row, col), axis=1)
                    exceldata['expenseDate'] = exceldata['expenseDate'].apply(
                        lambda x: make_timestamp(x) if x not in checkingarray2 else x)
                    exceldata['submssionDate'] = exceldata['submssionDate'].apply(
                        lambda x: make_timestamp(x) if x not in checkingarray2 else x)
                    exceldata['actionAt'] = exceldata['actionAt'].apply(
                        lambda x: make_timestamp(x) if x not in checkingarray2 else x)
                    
                    
                    
                    # exceldata['expenseDate'] = exceldata['expenseDate'].apply(
                    #     lambda x: check_and_convert_date(x) if x not in checkingarray2 else x)
                    # exceldata['actionAt'] = exceldata['actionAt'].apply(
                    #     lambda x: check_and_convert_date(x) if x not in checkingarray2 else x)
                    # exceldata['submissionDate'] = exceldata['submssionDate'].apply(
                    #     lambda x: check_and_convert_date(x) if x not in checkingarray2 else x)
                    
                    # exceldata['checkInDate'] = exceldata['checkInDate'].apply(
                    #     lambda x: convert_timestamp_to_string2(x) if x not in checkingarray2 else x)
                    
                    # exceldata['checkOutDate'] = exceldata['checkOutDate'].apply(
                    #     lambda x: convert_timestamp_to_string2(x) if x not in checkingarray2 else x)
                    
                    

                    # Apply the function to each row and create a new 'Number' column
                    
                    
                    # number_of_records=len(exceldata)
                    # counter = database.fileDetail.find_one_and_update({"id": "expenseIdCounter"},{"$inc": {"sequence_value": number_of_records}},return_document=True)
                    # starting_number = counter["sequence_value"] - number_of_records+1
                    # # exceldata['Number'] = ["SSID" + str(starting_number +i).zfill(8) for i in range(len(exceldata))]
                    # exceldata['Number'] = exceldata.apply(generate_number, axis=1)
                    
                    
                    
                    # print('40201011',current_time)
                    # exceldata['taskName'] = exceldata['taskName'].apply(
                    #     lambda x: ObjectId(x) if is_valid_mongodb_objectid2(x) else None)
                    # print('40301011',current_time)
                    # exceldata['Site Id'] = exceldata['Site Id'].apply(
                    #     lambda x: ObjectId(x) if is_valid_mongodb_objectid2(x) else None)
                    # print('40401011',current_time)
                    # print('ndhdhhnjejejexceldataexceldata',exceldata)
                    
                    # Step 6: Prepare data for insertion
                    # def prepare_data_for_insertion(row):
                    #     data = {
                    #         # "ExpenseNo": None,
                    #         "claimType": ObjectId(row['claimType']),
                    #         "Amount": int(row['Amount']),
                    #         "addedAt": row['submssionDate'],
                            # "CreatedAt": int(unique_timestampexpense()),
                    #         "addedFor": ObjectId(row["addedFor"]),
                    #         "designation": row["designation"],
                    #         "projectId": row['projectId'],
                    #         "taskName": row['taskName'],
                    #         'Site Id': row['Site Id'],
                    #         "status": row['status'],
                    #         "customStatus": row['status'],
                    #         "customisedStatus": row['customisedStatus'],
                    #         "type": row['type'],
                    #         'startKm': row['startKm'],
                    #         'endKm': row['endKm'],
                    #         "expenseDate": row['expenseDate'],
                    #         "startLocation": row['startLocation'],
                    #         "endLocation": row['endLocation'],
                    #         "Total_distance": row['Total_distance'],
                    #         "categories": row['categories'],
                    #         "billNumber": row['billNumber'],
                    #         "checkInDate": row['checkInDate'],
                    #         "checkOutDate": row['checkOutDate'],
                    #         'totaldays': row['totaldays'],
                    #         "addedBy": ObjectId(row["addedFor"]),
                    #         "L1Approver": row['L1Approver'],
                    #         "L2Approver": row['L2Approver'],
                    #         "L3Approver": row['L3Approver'],
                    #         "remark": row['remark'],
                    #         "ApprovedAmount": row["ApprovedAmount"],
                    #     }
                    #     return data
                    # insert_data = exceldata.apply(prepare_data_for_insertion, axis=1).tolist()
                    # print('40501011',current_time)
                    # print('insert_datainsert_datainsert_data',insert_data)
                    dict_data = cfc.dfjson(exceldata)
                    thread = Thread(target=background_Claims_upload, args=(dict_data,pathing))
                    thread.start()
                    return respond({
                            "status": 200,
                            "icon": "success",
                            "msg": f"Your file is in process when its completed we will notify",
                        })
                    # for i in jsonData:
                    #     claimTypecheckingArray = ['Expense', 'Daily Allowance']
                    #     checkingarray = [None, '', 'undefined']
                    #     checkingarray2 = [None, '', 'undefined']
                    #     i["L1ApproverId"] = i["L1Approver"]
                    #     i["L2ApproverId"] = i["L2Approver"]
                    #     i["L3ApproverId"] = i["L3Approver"]
                    #     i["L1Approver"] = []
                    #     i["L2Approver"] = []
                    #     i["L3Approver"] = []
                    #     if i['L3Status'] not in checkingarray:
                    #         i['status'] = i['L3Status']
                    #     elif i['L2Status'] not in checkingarray:
                    #         i['status'] = i['L2Status']
                    #     else:
                    #         i['status'] = i['L1Status']
                    #     if 'status' in i and i['status'] not in checkingarray:
                    #         i["customStatus"] = i["status"]
                    #         custom_status_mapping = {
                    #             "L1-Approved": 2,
                    #             "L1-Rejected": 3,
                    #             "L2-Approved": 4,
                    #             "L2-Rejected": 5,
                    #             "L3-Approved": 6,
                    #             "L3-Rejected": 7,
                    #         }
                    #         i["customisedStatus"] = custom_status_mapping.get(i["customStatus"], 1)
                    #     else:
                    #         i['customisedStatus'] = 1
                    #         i['status'] = 'Submitted'

                    #     if 'L1Status' in i and i['L1Status'] not in checkingarray:
                    #         i["L1Approver"].append({
                    #             "actionBy": ObjectId(i['L1ApproverId']),
                    #             "actionAt": get_current_date_timestamp(),
                    #             "status": i['L1Status'],
                    #             "customisedStatus": i['customisedStatus']
                    #         })
                    #     if 'L2Status' in i and i['L2Status'] not in checkingarray:
                    #         i["L2Approver"].append({
                    #             "actionBy": ObjectId(i['L2ApproverId']),
                    #             "actionAt": get_current_date_timestamp(),
                    #             "status": i['L2Status'],
                    #             "customisedStatus": i['customisedStatus']
                    #         })
                    #     if 'L3Status' in i and i['L3Status'] not in checkingarray:
                    #         i["L3Approver"].append({
                    #             "actionBy": ObjectId(i['L3ApproverId']),
                    #             "actionAt": get_current_date_timestamp(),
                    #             "status": i['L3Status'],
                    #             "customisedStatus": i['customisedStatus']
                    #         })

                    #     if "submssionDate" in i:
                    #         if i['submssionDate'] not in checkingarray2:
                    #             try:
                    #                 i['submssionDate'] = i['submssionDate']
                    #             except Exception:
                    #                 i['submssionDate'] = get_current_date_timestamp()

                    #     if i['type'] in claimTypecheckingArray:
                    #         arr = [
                    #             {"$match": {"deleteStatus": {"$ne": 1}, "_id": ObjectId(i['claimType'])}},
                    #             {"$addFields": {"claimTypeId": {"$toString": "$_id"}}},
                    #             {"$project": {"_id": 0}}
                    #         ]
                    #         shortcodeData = cmo.finding_aggregate("claimType", arr)["data"]

                    #         if 'ExpenseNo' not in i:
                    #             if claimType:
                    #                 ExpenseNo = "EXP"
                    #                 if len(shortcodeData) > 0:
                    #                     ExpenseNo = f"{ExpenseNo}/{currentFinancialMonth()}/"
                    #                     newArra = [{"$sort": {"_id": -1}}]
                    #                     responseData = cmo.finding_aggregate_with_deleteStatus("Expenses", newArra)["data"]
                    #                     if len(responseData) > 0 and "ExpenseNo" in responseData[0]:
                    #                         oldexpenseNo = responseData[0]["ExpenseNo"]
                    #                         ExpenseNo = generate_new_ExpenseNo(oldexpenseNo)
                    #                     else:
                    #                         ExpenseNo = ExpenseNo + "000001"

                    #         if is_valid_mongodb_objectid(i['projectId']):
                    #             i['projectId'] = ObjectId(i['projectId'])

                    #         if "expenseDate" in i:
                    #             if i['expenseDate'] not in checkingarray2:
                    #                 i['expenseDate'] = check_and_convert_date(i['expenseDate'])
                    #         if "checkInDate" in i:
                    #             if i['checkInDate'] not in checkingarray2:
                    #                 try:
                    #                     i['checkInDate'] = convert_timestamp_to_string2(i['checkInDate'])
                    #                 except Exception:
                    #                     i['checkInDate'] = i['checkInDate']
                    #         if "checkOutDate" in i:
                    #             if i['checkOutDate'] not in checkingarray2:
                    #                 try:
                    #                     i['checkOutDate'] = convert_timestamp_to_string2(i['checkOutDate'])
                    #                 except Exception:
                    #                     i['checkOutDate'] = i['checkOutDate']

                    #         if 'taskName' in i:
                    #             if is_valid_mongodb_objectid(i['taskName']):
                    #                 i['taskName'] = ObjectId(i['taskName'])
                    #             else:
                    #                 i['taskName'] = None

                    #         if 'Site Id' in i:
                    #             if is_valid_mongodb_objectid(i['Site Id']):
                    #                 i['Site Id'] = ObjectId(i['Site Id'])
                    #             else:
                    #                 i['Site Id'] = None

                    #         datatoinsert = {
                    #             "ExpenseNo": ExpenseNo,
                    #             "claimType": ObjectId(i['claimType']),
                    #             "Amount": int(i['Amount']),
                    #             "addedAt": i['submssionDate'],
                    #             "CreatedAt": int(unique_timestampexpense()),
                    #             "addedFor": ObjectId(i["addedFor"]),
                    #             "designation": i["designation"],
                    #             "projectId": i['projectId'],
                    #             "taskName": i['taskName'],
                    #             'Site Id': i['Site Id'],
                    #             "status": i['status'],
                    #             "customStatus": i['status'],
                    #             "customisedStatus": i['customisedStatus'],
                    #             "type": "Expense",
                    #             'startKm': i['startKm'],
                    #             'endKm': i['endKm'],
                    #             "expenseDate": i['expenseDate'],
                    #             "startLocation": i['startLocation'],
                    #             "endLocation": i['endLocation'],
                    #             "Total_distance": i['Total_distance'],
                    #             "categories": i['categories'],
                    #             "billNumber": i['billNumber'],
                    #             "checkInDate": i['checkInDate'],
                    #             "checkOutDate": i['checkOutDate'],
                    #             'totaldays': i['totaldays'],
                    #             "addedBy": ObjectId(i["addedFor"]),
                    #             "L1Approver": i['L1Approver'],
                    #             "L2Approver": i['L2Approver'],
                    #             "L3Approver": i['L3Approver'],
                    #             "remark": i['remark'],
                    #             "ApprovedAmount": i["ApprovedAmount"],
                    #         }

                    #         if i['type'] == 'Daily Allowance':
                    #             datatoinsertDailyAllowance = {
                    #                 "ExpenseNo": ExpenseNo,
                    #                 "claimType": ObjectId(i['claimType']),
                    #                 "Amount": int(i['Amount']),
                    #                 "addedAt": i['submssionDate'],
                    #                 "actionAt": get_current_date_timestamp(),
                    #                 "actionBy": ObjectId(i['L1ApproverId']),
                    #                 "CreatedAt": int(unique_timestampexpense()),
                    #                 "expenseDate": i['expenseDate'],
                    #                 "addedFor": ObjectId(i["addedFor"]),
                    #                 "designation": i["designation"],
                    #                 "addedBy": ObjectId(i['L1ApproverId']),
                    #                 "projectId": i['projectId'],
                    #                 "status": i['status'],
                    #                 "customStatus": i['status'],
                    #                 "customisedStatus": i['customisedStatus'],
                    #                 "type": "Expense",
                    #                 "fillBy": "L1-Approver",
                    #                 "remark": i['remark'],
                    #                 "action": "Approved",
                    #                 "L1Approver": i['L1Approver'],
                    #                 "L2Approver": i['L2Approver'],
                    #                 "L3Approver": i['L3Approver'],
                    #                 "ApprovedAmount": i["ApprovedAmount"],
                    #             }
                    #             Response = cmo.insertion("Expenses", datatoinsertDailyAllowance)
                    #             datatoinsert2 = {
                    #                 "ExpenseNo": ExpenseNo,
                    #                 "action": "Approved",
                    #                 "addedAt": i['submssionDate'],
                    #                 "actionAt": get_current_date_timestamp(),
                    #                 "actionBy": ObjectId(i['L1ApproverId']),
                    #                 "addedFor": ObjectId(i["addedFor"]),
                    #                 "status": i['status'],
                    #                 "customStatus": i['status'],
                    #                 "customisedStatus": i['customisedStatus'],
                    #                 "remark": i["remark"],
                    #                 "type": "Expense",
                    #                 "fillBy": "L1-Approver",
                    #                 "startLocation": i['startLocation'],
                    #                 "endLocation": i['endLocation'],
                    #                 "Total_distance": i['Total_distance'],
                    #                 "billNumber": i['billNumber'],
                    #                 "L1Approver": i['L1Approver'],
                    #                 "L2Approver": i['L2Approver'],
                    #                 "L3Approver": i['L3Approver'],
                    #                 'action': i['status'],
                    #                 "ApprovedAmount": i["ApprovedAmount"],
                    #                 "Amount": int(i['Amount']),
                    #             }
                    #             datatoinsert2["ExpenseUniqueId"] = ObjectId(Response["operation_id"])
                    #             Response = cmo.updating("Approval", datatoinsert2, {'ExpenseUniqueId': ObjectId(Response["operation_id"])}, True)

                    #         elif i['type'] == 'Expense':
                    #             Response = cmo.insertion("Expenses", datatoinsert)
                    #             newData = {
                    #                 "ExpenseNo": ExpenseNo,
                    #                 "ExpenseUniqueId": ObjectId(Response["operation_id"]),
                    #                 "action": i["status"],
                    #                 "addedAt": i['submssionDate'],
                    #                 "actionAt": get_current_date_timestamp(),
                    #                 "remark": i["remark"],
                    #                 "customisedStatus": i["customisedStatus"],
                    #                 "customStatus": i["status"],
                    #                 "status": i["status"],
                    #                 "startLocation": i['startLocation'],
                    #                 "endLocation": i['endLocation'],
                    #                 "Total_distance": i['Total_distance'],
                    #                 "categories": i['categories'],
                    #                 'startKm': i['startKm'],
                    #                 'endKm': i['endKm'],
                    #                 "type": "Expense",
                    #                 "addedFor": ObjectId(i["addedFor"]),
                    #                 "ApprovedAmount": i["ApprovedAmount"],
                    #                 "Amount": i["Amount"],
                    #                 "L1Approver": i['L1Approver'],
                    #                 "L2Approver": i['L2Approver'],
                    #                 "L3Approver": i['L3Approver'],
                    #                 'action': i['status']
                    #             }
                    #             Response = cmo.updating("Approval", {"ExpenseUniqueId": ObjectId(Response["operation_id"])}, newData, True)

                    #     elif i['type'] == 'Advance':
                    #         projectId = i['projectId']
                    #         advanceTypeId = i['claimType']
                    #         amount = i['Amount']
                    #         remark = i['remark']
                    #         chekingarray = ["", "undefined", None]

                    #         if amount not in chekingarray:
                    #             amount = int(amount)
                    #             if is_valid_mongodb_objectid(projectId):
                    #                 projectId = ObjectId(projectId)
                    #             data = {
                    #                 "projectId": projectId,
                    #                 "advanceTypeId": ObjectId(advanceTypeId),
                    #                 "Amount": int(amount),
                    #                 "remark": remark,
                    #                 "type": "Advance",
                    #                 "addedAt": i['submssionDate'],
                    #                 "status": i['status'],
                    #                 "customStatus": i['status'],
                    #                 "customisedStatus": i['customisedStatus'],
                    #                 "L1Approver": i['L1Approver'],
                    #                 'action': i['status'],
                    #                 "L2Approver": i['L2Approver'],
                    #                 "L3Approver": i['L3Approver'],
                    #                 "ApprovedAmount": i["ApprovedAmount"],
                    #                 "addedFor": ObjectId(i["addedFor"]),
                    #             }
                    #             data["addedFor"] = ObjectId(i['addedFor'])
                    #             data["designation"] = ObjectId(i['designation'])
                    #             data["addedAt"] = i['expenseDate']
                    #             data["CreatedAt"] = int(unique_timestampexpense())
                    #             AdvanceNo = "ADV"
                    #             AdvanceNo = f"{AdvanceNo}/{currentFinancialMonth()}/"
                    #             newArra = [{"$sort": {"_id": -1}}]
                    #             responseData = cmo.finding_aggregate_with_deleteStatus("Advance", newArra)["data"]
                    #             if len(responseData) > 0 and "AdvanceNo" in responseData[0]:
                    #                 oldadvanceNo = responseData[0]["AdvanceNo"]
                    #                 AdvanceNo = generate_new_AdvanceNo(oldadvanceNo)
                    #             else:
                    #                 AdvanceNo = AdvanceNo + "000001"
                    #             data["AdvanceNo"] = AdvanceNo
                    #             Response = cmo.insertion('Advance', data)
                    #             newData = {
                    #                 "AdvanceNo": data["AdvanceNo"],
                    #                 "AdvanceUniqueId": ObjectId(Response["operation_id"]),
                    #                 "advanceTypeId": ObjectId(advanceTypeId),
                    #                 "action": i["status"],
                    #                 "actionAt": get_current_date_timestamp(),
                    #                 "remark": i["remark"],
                    #                 "customisedStatus": i["customisedStatus"],
                    #                 "customStatus": i["status"],
                    #                 "status": i["status"],
                    #                 "type": "Advance",
                    #                 "addedFor": ObjectId(i["addedFor"]),
                    #                 "ApprovedAmount": i["ApprovedAmount"],
                    #                 "Amount": i["Amount"],
                    #                 'action': i['status']
                    #             }
                    #             Response = cmo.updating("Approval", {"AdvanceUniqueId": ObjectId(Response["operation_id"])}, newData, True)

                    return respond(Response)
             
                if fileType == "UploadAccuralRevenueMaster":
                    
                    array=["customer","project","subProject","projectType"]
                    empty_rows = exceldata[array].apply(lambda x: (x == "") | (x.isna()), axis=1).any(axis=1)
                    invalid_columns = exceldata.index[empty_rows].tolist()
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows are not valid Data{invalid_columns}",
                            }
                        )
                    array=["rate"]
                    empty_rows = exceldata[array].apply(lambda x: x < 0 ).any(axis=1)
                    invalid_columns = exceldata.index[empty_rows].tolist()
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows Do not have a valid Rate{invalid_columns}",
                            }
                        )
                    arr=[
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
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
                                        }
                                    ], 
                                    'as': 'result'
                                }
                            }, {
                                '$addFields': {
                                    'customer': {
                                        '$toObjectId': '$custId'
                                    }
                                }
                            }, {
                                '$lookup': {
                                    'from': 'customer', 
                                    'localField': 'customer', 
                                    'foreignField': '_id', 
                                    'as': 'customer'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$result', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$lookup': {
                                    'from': 'projectType', 
                                    'localField': 'result.projectType', 
                                    'foreignField': 'projectType', 
                                    'pipeline': [
                                        {
                                            '$match': {
                                                'deleteStatus': {
                                                    '$ne': 1
                                                }
                                            }
                                        }, {
                                            '$project': {
                                                '_id': 1, 
                                                'subProject': 1
                                            }
                                        }
                                    ], 
                                    'as': 'result2'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$result2', 
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$project': {
                                    'project': '$projectId', 
                                    'projectId': {
                                        '$toString': '$_id'
                                    }, 
                                    'projectTypeId': {
                                        '$toString': '$projectType'
                                    }, 
                                    'subProjectId': {
                                        '$toString': '$result2._id'
                                    }, 
                                    'subProject': '$result2.subProject', 
                                    'projectType': '$result.projectType', 
                                    '_id': {
                                        '$toString': '$_id'
                                    }, 
                                    'customer': {
                                        '$arrayElemAt': [
                                            '$customer.customerName', 0
                                        ]
                                    }, 
                                    'customerId': '$custId'
                                }
                            }
                        ]
                    projectsdata=cmo.finding_aggregate("project",arr)
                    projectsdata = cmo.finding_aggregate("project", arr)
                    projectsdataCodeDatadp = pd.DataFrame.from_dict(projectsdata["data"])
                    projectmermerged = exceldata.merge(projectsdataCodeDatadp, on=["customer","project", "subProject","projectType"], how="left")
                    cresult2 = projectmermerged[projectmermerged["_id"].isna()]
                    unique_combinations = cresult2[["customer","project", "subProject","projectType"]].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Customer, Project,Sub Project and projectType not Found {unique_c}",
                            }
                        )
                    else:
                        exceldata['project']=projectmermerged['projectId']
                        exceldata['subProject']=projectmermerged['subProjectId']
                        exceldata['projectType']=projectmermerged['projectTypeId']
                        exceldata['customer']=projectmermerged['customerId']
                    jsonData = cfc.dfjson(exceldata)
                    Response=None
                    for i in jsonData:
                        if is_valid_mongodb_objectid(i['project']):
                            i['project']=ObjectId(i['project'])
                        if is_valid_mongodb_objectid(i['subProject']):
                            i['subProject']=ObjectId(i['subProject'])
                        if is_valid_mongodb_objectid(i['projectType']):
                            i['projectType']=ObjectId(i['projectType'])
                        if is_valid_mongodb_objectid(i['customer']):
                            i['customer']=ObjectId(i['customer'])
                        Response=cmo.updating("AccuralRevenueMaster",{'project':ObjectId(i['project']),'subProject':ObjectId(i['subProject']),'rate':i['rate'],'activity':i['activity'],'band':i['band']},i,True)
                        print(Response,jsonData,"___Response")
                    return respond(Response)


                if fileType == "UploadCurrentBalance":
                    checkingFieldsArray=['empCode', 'email','Amount']
                    errorlist=[]
                    checkingFieldsArray2=['empCode', 'email','Amount']
                    #to check empty required columns 
                    empty_fields = exceldata[checkingFieldsArray2].isnull() | (exceldata[checkingFieldsArray2] == '')
                    rows_with_empty_fields = empty_fields.any(axis=1)
                    error_rows_indices = rows_with_empty_fields[rows_with_empty_fields].index.tolist()
                    if len(error_rows_indices):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The following required columns have  some value {checkingFieldsArray} value is missing on these rows{error_rows_indices}",
                            }
                        )
                    print('1111',current_time)
                    dbData = [
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'email': 1, 
                                'empCode': 1
                            }
                        }
                    ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empCode", "email"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["_id"].isna()]
                        unique_combinations = cresult2[["empCode", "email"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Employee Code and Email not Found {unique_c}",
                                }
                            )  
                        else:
                            print('hiiiii')
                            
                               
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data1 {errorlist}",
                            }
                        )
                    print('77777',current_time)
                    if "empCode" in exceldata.columns:
                        dbData = [
                            {
                                '$match': {
                                    'deleteStatus': {
                                        '$ne': 1
                                    },
                                    'type': {
                                        '$ne': 'Partner'
                                    }
                                }
                            },
                            
                            {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    },
                                    'empCode': 1,
                                    'designation': 1,
                                    
                                }
                            }
                        ]
                        
                        data = cmo.finding_aggregate("userRegister", dbData)
                        empDatadp = pd.DataFrame.from_dict(data["data"])
                        filtered_exceldata = exceldata[(exceldata["empCode"].notna())& (exceldata["empCode"] != "")]
                        empmerged = filtered_exceldata.merge(empDatadp, on=["empCode"], how="left")
                        cresult2 = empmerged[empmerged["_id"].isna()]
                        unique_c = cresult2["empCode"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for emp_Code in unique_c:
                                missing_indices = cresult2[cresult2["empCode"] == emp_Code].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"Employee Code not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                        else:
                            emp_dict = empmerged.set_index("empCode")["_id"].to_dict()
                            print('emp_dictemp_dictemp_dict',emp_dict)
                            exceldata["addedFor"] = exceldata["empCode"].map(emp_dict)
                    
                    
                    
                    def convert_to_string(value):
                        if isinstance(value, float):
                            if value.is_integer():
                                return str(int(value))
                            else:
                                return str(value)
                        return str(value)
                    exceldata['Amount'] = exceldata['Amount'].apply(convert_to_string)

                    jsonData = cfc.dfjson(exceldata)
                    Response=None
                    for i in jsonData:
                        i['employee']=i['addedFor']
                        if 'addedFor' in i:
                            if i['addedFor'] not in ['',None,'undefined']:
                                if is_valid_mongodb_objectid(i['addedFor']):
                                    print("i['addedFor']",i['addedFor'])
                                    i['addedFor']=ObjectId(i['addedFor'])
                        if 'Amount' in i:
                            if i['Amount'] not in ['',None,'undefined']:
                                i['Amount']=int(i['Amount'])
                        Response=cmo.updating('CurrentBalance',{'email':i['email']},i,True)
                    return respond(Response)
                
                if fileType == "UpgradeEmployee":
                    checkingFieldsArray=['title','resignDate','costCenter','ustJobCode','ustProjectId','allocationPercentage','businesssUnit','customer', 'empName', 'empCode', 'ustCode', 'fatherName', 'motherName', 'martialStatus', 'email', 'personalEmailId', 'dob', 'mobile', 'blood', 'country', 'state', 'city', 'pincode', 'address', 'pcountry', 'ppincode', 'pcity', 'pstate', 'paddress', 'panNumber', 'adharNumber', 'circle', 'salaryCurrency', 'experience', 'monthlySalary', 'grossCtc', 'joiningDate', 'lastWorkingDay', 'passport', 'passportNumber', 'bankName', 'accountNumber', 'ifscCode', 'benificiaryname', 'orgLevel', 'designation', 'role', 'userRole', 'band', 'department', 'reportingManager', 'L1Approver', 'L2Approver', 'financeApprover', 'reportingHrManager', 'assetManager', 'L1Vendor', 'L2Vendor', 'L1Compliance', 'L2Compliance', 'L1Commercial', 'L1Sales', 'L2Sales', 'status', 'password']
                    invalid_columns = [col for col in exceldata.columns if col not in checkingFieldsArray]
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Columns are not valid email{invalid_columns}",
                            }
                        )
                        
                    missingData = exceldata[exceldata.isna().any(axis=1) | (exceldata == "").any(axis=1)].index
                    missingDataRows = list(missingData + 2)
                    if len(missingDataRows):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data1 {missingDataRows}",
                            }
                        ) 
                        
                    missingEmployeCode = list(exceldata[exceldata["empCode"].isna() | (exceldata["empCode"] == "")].index)
                    if len(missingEmployeCode):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid EmployeeCode{missingEmployeCode}",
                            }
                        )

                    emailEmployee = exceldata[exceldata["email"].isna() | (exceldata["email"] == "")].index
                    emailEmployeelist = list(emailEmployee)
                    if len(emailEmployeelist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid email{emailEmployeelist}",
                            }
                        )
                        
                    
                        
                    jsonData = cfc.dfjson(exceldata)
                    errorlist = []
                    dbData = [
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'email': 1, 
                                'empCode': 1
                            }
                        }
                    ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empCode", "email"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["_id"].isna()]
                        unique_combinations = cresult2[["empCode", "email"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Pairs of Employee Code and Email not Found {unique_c}",
                                }
                            )
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                    # Department
                    if "department" in exceldata.columns:
                        dbData = [
                            {
                                "$project": {
                                    "departmentId": {"$toString": "$_id"},
                                    "department": 1,
                                    "_id": 0,
                                }
                            }
                        ]
                        department = cmo.finding_aggregate("department", dbData)
                        if len(department['data']):
                            departmentDatadp = pd.DataFrame.from_dict(department["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["department"].notna())
                                & (exceldata["department"] != "")
                            ]
                            departmentmerged = filtered_exceldata.merge(
                                departmentDatadp, on=["department"], how="left"
                            )
                            cresult2 = departmentmerged[
                                departmentmerged["departmentId"].isna()
                            ]
                            unique_c = cresult2["department"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["department"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"Departments not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                                # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                            else:
                                exceldata["department"] = departmentmerged["departmentId"]
                        else:
                            errorlist.append(f"No Department Found in Manage Department")
                    # User Role
                    if "userRole" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "userRoleId": {"$toString": "$_id"},
                                    "userRole": "$roleName",
                                }
                            },
                            {"$project": {"userRoleId": 1, "userRole": 1, "_id": 0}},
                        ]
                        
                        userRole = cmo.finding_aggregate("userRole", dbData)
                        if len(userRole['data']):
                            userRoleDatadp = pd.DataFrame.from_dict(userRole["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["userRole"].notna())
                                & (exceldata["userRole"] != "")
                            ]
                            userRolemerged = filtered_exceldata.merge(
                                userRoleDatadp, on=["userRole"], how="left"
                            )
                            cresult2 = userRolemerged[userRolemerged["userRoleId"].isna()]
                            unique_c = cresult2["userRole"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["userRole"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"userRole not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )

                            else:
                                exceldata["userRole"] = userRolemerged["userRoleId"]
                        else:
                            errorlist.append(f"User Role is not defined in ManageProfile")
                            
                    ### Customer        
                    if "customer" in exceldata.columns:
                        dbData = [
                                {
                                    '$project': {
                                        'customer': '$customerName', 
                                        '_id': {
                                            '$toString': '$_id'
                                        }, 
                                        
                                    }
                                }
                            ]
                        costCenter = cmo.finding_aggregate("customer", dbData)
                        costCenterDatadp = pd.DataFrame.from_dict(costCenter["data"])
                        filtered_exceldata = exceldata[(exceldata["customer"].notna())& (exceldata["customer"] != "")]
                        costCentermerged = filtered_exceldata.merge(costCenterDatadp, on=["customer"], how="left")
                        cresult2 = costCentermerged[costCentermerged["_id"].isna()]
                        unique_c = cresult2["_id"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for costCenter_name in unique_c:
                                missing_indices = cresult2[cresult2["customer"] == costCenter_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"Customer not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                            # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                        else:
                            costCenter_dict = costCentermerged.set_index("customer")["_id"].to_dict()
                            exceldata["customer"] = exceldata["customer"].map(costCenter_dict)    
                            

                    ##Cost center
                    if "costCenter" in exceldata.columns:
                        dbData = [
                                {
                                    '$project': {
                                        '_id': {
                                            '$toString': '$_id'
                                        }, 
                                        'costCenter': 1
                                    }
                                }
                            ]
                        costCenter = cmo.finding_aggregate("costCenter", dbData)
                        costCenterDatadp = pd.DataFrame.from_dict(costCenter["data"])
                        filtered_exceldata = exceldata[(exceldata["costCenter"].notna())& (exceldata["costCenter"] != "")]
                        costCentermerged = filtered_exceldata.merge(costCenterDatadp, on=["costCenter"], how="left")
                        cresult2 = costCentermerged[costCentermerged["_id"].isna()]
                        unique_c = cresult2["costCenter"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for costCenter_name in unique_c:
                                missing_indices = cresult2[cresult2["costCenter"] == costCenter_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"costCenter not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                            # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                        else:
                            costCenter_dict = costCentermerged.set_index("costCenter")["_id"].to_dict()
                            exceldata["costCenter"] = exceldata["costCenter"].map(costCenter_dict)
                    ###role
                    if "role" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "roleId": {"$toString": "$_id"},
                                    "role": "$roleName",
                                }
                            },
                            {"$project": {"roleId": 1, "role": 1, "_id": 0}},
                        ]
                        # print('dbDatadbData',dbData)
                        Role = cmo.finding_aggregate("userRole", dbData)
                        # print('RoleRole',Role)
                        if len(Role['data']):
                            RoleDatadp = pd.DataFrame.from_dict(Role["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["role"].notna()) & (exceldata["role"] != "")
                            ]
                            Rolemerged = filtered_exceldata.merge(
                                RoleDatadp, on=["role"], how="left"
                            )
                            cresult2 = Rolemerged[Rolemerged["roleId"].isna()]
                            unique_c = cresult2["role"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["role"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"Role not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )

                            else:
                                exceldata["role"] = Rolemerged["roleId"]
                        else:
                            errorlist.append(f"Role is not defined in Manage Profile")
                    # Designation
                    if "designation" in exceldata.columns:
                        dbData = [
                            {"$addFields": {"designationId": {"$toString": "$_id"}}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "designation": 1,
                                    "designationId": 1,
                                }
                            },
                        ]
                        designation = cmo.finding_aggregate("designation", dbData)
                        if len(designation['data']):
                            designationDatadp = pd.DataFrame.from_dict(designation["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["designation"].notna())
                                & (exceldata["designation"] != "")
                            ]
                            designationmerged = filtered_exceldata.merge(
                                designationDatadp, on=["designation"], how="left"
                            )
                            cresult2 = designationmerged[
                                designationmerged["designationId"].isna()
                            ]
                            unique_c = cresult2["designation"].unique()
                            # print("unique_cunique_cunique_c", unique_c)

                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["designation"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"designation not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                            else:
                                exceldata["designation"] = designationmerged[
                                    "designationId"
                                ]
                        else:
                            errorlist.append(f"Designation Not found in Manage Designation")


                    if "circle" in exceldata.columns:
                        dbData = [
                            {
                                '$project': {
                                    'circle': '$circleName', 
                                    '_id': {
                                        '$toString': '$_id'
                                    }, 
                                    
                                }
                            }
                        ]
                        circle = cmo.finding_aggregate("circle", dbData)
                        circleDatadp = pd.DataFrame.from_dict(circle["data"])
                        filtered_exceldata = exceldata[(exceldata["circle"].notna())& (exceldata["circle"] != "")]
                        circlemerged = filtered_exceldata.merge(circleDatadp, on=["circle"], how="left")
                        cresult2 = circlemerged[circlemerged["_id"].isna()]
                        unique_c = cresult2["circle"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for circle_name in unique_c:
                                missing_indices = cresult2[cresult2["circle"] == circle_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"Circle not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                            # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                        else:
                            circle_dict = circlemerged.set_index("circle")["_id"].to_dict()
                            exceldata["circle"] = exceldata["circle"].map(circle_dict)



                    # L1Approver
                    if "L1Approver" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L1ApproverId": {"$toString": "$_id"},
                                    "L1Approver": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L1Approver": 1,
                                    "L1ApproverId": 1,
                                }
                            },
                        ]
                        L1Approver = cmo.finding_aggregate("userRegister", dbData)
                        L1ApproverDatadp = pd.DataFrame.from_dict(L1Approver["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L1Approver"].notna())
                            & (exceldata["L1Approver"] != "")
                        ]
                        L1Approvermerged = filtered_exceldata.merge(
                            L1ApproverDatadp, on=["L1Approver"], how="left"
                        )
                        cresult2 = L1Approvermerged[
                            L1Approvermerged["L1ApproverId"].isna()
                        ]
                        unique_c = cresult2["L1Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Approver"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L1Approver"] = L1Approvermerged["L1ApproverId"]
                    #AssertManager
                    if "assetManager" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "assetManagerId": {"$toString": "$_id"},
                                    "assetManager": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "assetManager": 1,
                                    "assetManagerId": 1,
                                }
                            },
                        ]
                        assetManager = cmo.finding_aggregate("userRegister", dbData)
                        assetManagerDatadp = pd.DataFrame.from_dict(assetManager["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["assetManager"].notna())
                            & (exceldata["assetManager"] != "")
                        ]
                        assetManagermerged = filtered_exceldata.merge(
                            assetManagerDatadp, on=["assetManager"], how="left"
                        )
                        cresult2 = assetManagermerged[
                            assetManagermerged["assetManagerId"].isna()
                        ]
                        unique_c = cresult2["assetManager"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["assetManager"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"Asset Manager not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["assetManager"] = assetManagermerged["assetManagerId"]
                    # L1Commercial
                    if "L1Commercial" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L1CommercialId": {"$toString": "$_id"},
                                    "L1Commercial": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L1Commercial": 1,
                                    "L1CommercialId": 1,
                                }
                            },
                        ]
                        L1Commercial = cmo.finding_aggregate("userRegister", dbData)
                        L1CommercialDatadp = pd.DataFrame.from_dict(
                            L1Commercial["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["L1Commercial"].notna())
                            & (exceldata["L1Commercial"] != "")
                        ]
                        L1Commercialmerged = filtered_exceldata.merge(
                            L1CommercialDatadp, on=["L1Commercial"], how="left"
                        )
                        cresult2 = L1Commercialmerged[
                            L1Commercialmerged["L1CommercialId"].isna()
                        ]
                        unique_c = cresult2["L1Commercial"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Commercial"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Commercial not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )
                        else:
                            exceldata["L1Commercial"] = L1Commercialmerged[
                                "L1CommercialId"
                            ]

                    # L1Compliance
                    if "L1Compliance" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L1ComplianceId": {"$toString": "$_id"},
                                    "L1Compliance": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L1Compliance": 1,
                                    "L1ComplianceId": 1,
                                }
                            },
                        ]
                        L1Compliance = cmo.finding_aggregate("userRegister", dbData)
                        L1ComplianceDatadp = pd.DataFrame.from_dict(
                            L1Compliance["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["L1Compliance"].notna())
                            & (exceldata["L1Compliance"] != "")
                        ]
                        L1Compliancemerged = filtered_exceldata.merge(
                            L1ComplianceDatadp, on=["L1Compliance"], how="left"
                        )
                        cresult2 = L1Compliancemerged[
                            L1Compliancemerged["L1ComplianceId"].isna()
                        ]
                        unique_c = cresult2["L1Compliance"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Compliance"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Compliance not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L1Compliance"] = L1Compliancemerged[
                                "L1ComplianceId"
                            ]

                    # L1Sales
                    if "L1Sales" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L1SalesId": {"$toString": "$_id"},
                                    "L1Sales": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L1Sales": 1, "L1SalesId": 1}},
                        ]
                        L1Sales = cmo.finding_aggregate("userRegister", dbData)
                        L1SalesDatadp = pd.DataFrame.from_dict(L1Sales["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L1Sales"].notna())
                            & (exceldata["L1Sales"] != "")
                        ]
                        L1Salesmerged = filtered_exceldata.merge(
                            L1SalesDatadp, on=["L1Sales"], how="left"
                        )
                        cresult2 = L1Salesmerged[L1Salesmerged["L1SalesId"].isna()]
                        unique_c = cresult2["L1Sales"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Sales"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Sales not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L1Sales"] = L1Salesmerged["L1SalesId"]

                    # L1Vendor
                    if "L1Vendor" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L1VendorId": {"$toString": "$_id"},
                                    "L1Vendor": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L1Vendor": 1, "L1VendorId": 1}},
                        ]
                        L1Vendor = cmo.finding_aggregate("userRegister", dbData)
                        L1VendorDatadp = pd.DataFrame.from_dict(L1Vendor["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L1Vendor"].notna())
                            & (exceldata["L1Vendor"] != "")
                        ]
                        L1Vendormerged = filtered_exceldata.merge(
                            L1VendorDatadp, on=["L1Vendor"], how="left"
                        )
                        cresult2 = L1Vendormerged[L1Vendormerged["L1VendorId"].isna()]
                        unique_c = cresult2["L1Vendor"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Vendor"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Vendor not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L1Vendor"] = L1Vendormerged["L1VendorId"]

                    # L2Approver
                    if "L2Approver" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L2ApproverId": {"$toString": "$_id"},
                                    "L2Approver": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L2Approver": 1,
                                    "L2ApproverId": 1,
                                }
                            },
                        ]
                        L2Approver = cmo.finding_aggregate("userRegister", dbData)
                        L2ApproverDatadp = pd.DataFrame.from_dict(L2Approver["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L2Approver"].notna())
                            & (exceldata["L2Approver"] != "")
                        ]
                        L2Approvermerged = filtered_exceldata.merge(
                            L2ApproverDatadp, on=["L2Approver"], how="left"
                        )
                        cresult2 = L2Approvermerged[
                            L2Approvermerged["L2ApproverId"].isna()
                        ]
                        unique_c = cresult2["L2Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Approver"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            # print('exceldata["L2Approver"]',exceldata["L2Approver"],L2Approvermerged["L2ApproverId"])
                            exceldata["L2Approver"] = L2Approvermerged["L2ApproverId"]

                    # L2Compliance
                    if "L2Compliance" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L2ComplianceId": {"$toString": "$_id"},
                                    "L2Compliance": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L2Compliance": 1,
                                    "L2ComplianceId": 1,
                                }
                            },
                        ]
                        L2Compliance = cmo.finding_aggregate("userRegister", dbData)
                        L2ComplianceDatadp = pd.DataFrame.from_dict(
                            L2Compliance["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["L2Compliance"].notna())
                            & (exceldata["L2Compliance"] != "")
                        ]
                        L2Compliancemerged = filtered_exceldata.merge(
                            L2ComplianceDatadp, on=["L2Compliance"], how="left"
                        )
                        cresult2 = L2Compliancemerged[
                            L2Compliancemerged["L2ComplianceId"].isna()
                        ]
                        unique_c = cresult2["L2Compliance"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Compliance"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Compliance not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L2Compliance"] = L2Compliancemerged[
                                "L2ComplianceId"
                            ]

                    # L2Sales
                    if "L2Sales" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L2SalesId": {"$toString": "$_id"},
                                    "L2Sales": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L2Sales": 1, "L2SalesId": 1}},
                        ]
                        L2Sales = cmo.finding_aggregate("userRegister", dbData)
                        L2SalesDatadp = pd.DataFrame.from_dict(L2Sales["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L2Sales"].notna())
                            & (exceldata["L2Sales"] != "")
                        ]
                        L2Salesmerged = filtered_exceldata.merge(
                            L2SalesDatadp, on=["L2Sales"], how="left"
                        )
                        cresult2 = L2Salesmerged[L2Salesmerged["L2SalesId"].isna()]
                        unique_c = cresult2["L2Sales"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Sales"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Sales not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L2Sales"] = L2Salesmerged["L2SalesId"]

                    # L2Vendor
                    if "L2Vendor" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "L2VendorId": {"$toString": "$_id"},
                                    "L2Vendor": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L2Vendor": 1, "L2VendorId": 1}},
                        ]
                        L2Vendor = cmo.finding_aggregate("userRegister", dbData)
                        L2VendorDatadp = pd.DataFrame.from_dict(L2Vendor["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L2Vendor"].notna())
                            & (exceldata["L2Vendor"] != "")
                        ]
                        L2Vendormerged = filtered_exceldata.merge(
                            L2VendorDatadp, on=["L2Vendor"], how="left"
                        )
                        cresult2 = L2Vendormerged[L2Vendormerged["L2VendorId"].isna()]
                        unique_c = cresult2["L2Vendor"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Vendor"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Vendor not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["L2Vendor"] = L2Vendormerged["L2VendorId"]

                    # Finance Approver
                    if "financeApprover" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "financeApproverId": {"$toString": "$_id"},
                                    "financeApprover": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "financeApprover": 1,
                                    "financeApproverId": 1,
                                }
                            },
                        ]
                        financeApprover = cmo.finding_aggregate("userRegister", dbData)
                        financeApproverDatadp = pd.DataFrame.from_dict(
                            financeApprover["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["financeApprover"].notna())
                            & (exceldata["financeApprover"] != "")
                        ]
                        financeApprovermerged = filtered_exceldata.merge(
                            financeApproverDatadp, on=["financeApprover"], how="left"
                        )
                        cresult2 = financeApprovermerged[
                            financeApprovermerged["financeApproverId"].isna()
                        ]
                        unique_c = cresult2["financeApprover"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["financeApprover"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"financeApprover not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["financeApprover"] = financeApprovermerged[
                                "financeApproverId"
                            ]

                    # Reporting Manager
                    if "reportingManager" in exceldata.columns:
                        dbData = [
                            {
                                "$addFields": {
                                    "reportingManagerId": {"$toString": "$_id"},
                                    "reportingManager": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "reportingManager": 1,
                                    "reportingManagerId": 1,
                                }
                            },
                        ]
                        reportingManager = cmo.finding_aggregate("userRegister", dbData)
                        reportingManagerDatadp = pd.DataFrame.from_dict(
                            reportingManager["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["reportingManager"].notna())
                            & (exceldata["reportingManager"] != "")
                        ]
                        reportingManagermerged = filtered_exceldata.merge(
                            reportingManagerDatadp, on=["reportingManager"], how="left"
                        )
                        cresult2 = reportingManagermerged[
                            reportingManagermerged["reportingManagerId"].isna()
                        ]
                        unique_c = cresult2["reportingManager"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["reportingManager"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"reportingManager not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["reportingManager"] = reportingManagermerged[
                                "reportingManagerId"
                            ]

                    # Reporting HR Manager
                    if "reportingHrManager" in exceldata.columns:
                        
                        dbData = [
                            {"$match": {"roleName": "HR Head"}},
                            {
                                "$lookup": {
                                    "from": "userRegister",
                                    "localField": "_id",
                                    "foreignField": "userRole",
                                    "pipeline": [
                                        {"$match": {"deleteStatus": {"$ne": 1}}}
                                    ],
                                    "as": "users",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$users",
                                    "preserveNullAndEmptyArrays": True,
                                }
                            },
                            {
                                "$addFields": {
                                    "reportingHrManagerId": {"$toString": "$users._id"},
                                    "reportingHrManager": "$users.email",
                                }
                            },
                            {
                                "$project": {
                                    "reportingHrManagerId": 1,
                                    "_id": 0,
                                    "reportingHrManager": 1,
                                }
                            },
                        ]
                        reportingHrManager = cmo.finding_aggregate("userRole", dbData)
                        if len(reportingHrManager['data']) and "reportingHrManager" in reportingHrManager['data'][0]:
                            reportingHrManagerDatadp = pd.DataFrame.from_dict(reportingHrManager["data"])
                            filtered_exceldata = exceldata[(exceldata["reportingHrManager"].notna()) & (exceldata["reportingHrManager"] != "")]
                            reportingHrManagermerged = filtered_exceldata.merge(reportingHrManagerDatadp,on=["reportingHrManager"],how="left")
                            cresult2 = reportingHrManagermerged[reportingHrManagermerged["reportingHrManagerId"].isna()]
                            unique_c = cresult2["reportingHrManager"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["reportingHrManager"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(f"Reporting HR Manager not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")

                            else:
                                exceldata["reportingHrManager"] = reportingHrManagermerged["reportingHrManagerId"]
                        else:
                            errorlist.append('HR Head not found in manage HR')
                     
                    if 'joiningDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            if pd.isna(row['joiningDate']) or row['joiningDate'] == '':
                                exceldata.at[index, 'joiningDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['joiningDate'] = pd.to_datetime(row['joiningDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if len(invalidRows):  
                            errorlist.append(f"Joining date format is invaid in these rows{invalidRows}")   
                    
                    if 'resignDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            if pd.isna(row['resignDate']) or row['resignDate'] == "":
                                exceldata.at[index, 'resignDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['resignDate'] = pd.to_datetime(row['resignDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"resignDate format is invaid in these rows{invalidRows}") 
                     
                    if 'dob' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            if pd.isna(row['dob']) or row['dob'] == '':
                                exceldata.at[index, 'dob'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['dob'] = pd.to_datetime(row['dob'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if len(invalidRows):   
                            errorlist.append(f"Date Of Birth format is invalid in these rows{invalidRows}")      
                        
                    if 'lastWorkingDay' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            if pd.isna(row['lastWorkingDay']) or row['lastWorkingDay'] == '':
                                exceldata.at[index, 'lastWorkingDay'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['lastWorkingDay'] = pd.to_datetime(row['lastWorkingDay'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if len(invalidRows):    
                            errorlist.append(f"last Working Date  format is invaid in these rows{invalidRows}") 
                            

                    if len(errorlist) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows do not have valid values: {', '.join(errorlist)}",
                            }
                        )
                    data = cfc.dfjson(exceldata)
                    for i in data:
                        
                        if "userRole" in i:
                            if is_valid_mongodb_objectid(i["userRole"]):
                                i["userRole"] = ObjectId(i["userRole"])
                        if "password" in i:
                            if i["password"] != None and i["password"] != "undefined":
                                i["password"] = str(i["password"])
                                
                        if "joiningDate" in i:
                            i['joiningDate'] = check_and_convert_date3(i['joiningDate'])
                        
                        
                        if "resignDate" in i:
                            i['resignDate'] = check_and_convert_date3(i['resignDate'])
                        
                        if "dob" in i:
                            i['dob'] = check_and_convert_date3(i['dob'])
                            
                        if "lastWorkingDay" in i:
                            i['lastWorkingDay'] = check_and_convert_date3(i['lastWorkingDay'])

                        cmo.updating("userRegister", {"email": i["email"],"deleteStatus": {"$ne": 1}}, i, False)
                    return respond({
                        'status':200,
                        "msg":"Data Updated Successfully",
                        "icon":"success"
                    })
                if fileType == "UpgradeEmployeeWithEmpCode":
                    checkingFieldsArray=['empCode','email']
                    invalid_columns = [col for col in exceldata.columns if col not in checkingFieldsArray]
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Columns are not valid email{invalid_columns}",
                            }
                        )
                    missingData = exceldata[exceldata.isna().any(axis=1) | (exceldata == "").any(axis=1)].index
                    missingDataRows = list(missingData + 2)
                    if len(missingDataRows):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid data {missingDataRows}",
                            }
                        )
                    jsonData = cfc.dfjson(exceldata)
                    errorlist = []
                    dbData = [
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'empCode': 1
                            }
                        }
                    ]
                    empNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    if (len(empNameCodeData)):
                        empNameCodeDatadp = pd.DataFrame.from_dict(empNameCodeData["data"])
                        projectGroupmermerged = exceldata.merge(empNameCodeDatadp, on=["empCode"], how="left")
                        cresult2 = projectGroupmermerged[projectGroupmermerged["_id"].isna()]
                        unique_combinations = cresult2[["empCode"]].drop_duplicates()
                        unique_c = unique_combinations.values
                        if len(unique_c)>0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Employee Code not Found {unique_c}",
                                }
                            )
                    else:
                        errorlist.append(f"No Data Found In DataBase")
                       
                    
                    
                    arr = [
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
                                '_id': {
                                    '$toString': '$_id'
                                }, 
                                'email': 1
                            }
                        }
                    ]

                    data = cmo.finding_aggregate("userRegister", arr)["data"]
                    if len(data):
                        datas = pd.DataFrame(data)
                        datas["tmp"] = True
                        mergedData = pd.merge(exceldata, datas, "left", "email")
                        id = list(mergedData.index + 1)
                        mergedData.insert(0, "id", id)
                        newData = mergedData[mergedData["tmp"] == True]
                        newData = newData.groupby("email")
                        duplicatevendorCode = []
                        for employee_name, group in newData:
                            print(
                                duplicatevendorCode.append(group.index.tolist()[0] + 2)
                            )
                        if len(duplicatevendorCode):
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"The email-ID in {duplicatevendorCode} these rows is already present",
                                }
                            )
                    
                       
                          

                    if len(errorlist) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows do not have valid values: {', '.join(errorlist)}",
                            }
                        )
                    data = cfc.dfjson(exceldata)
                    for i in data:
                        cmo.updating("userRegister", {"empCode": i["empCode"],"deleteStatus": {"$ne": 1}}, i, False)
                    return respond({
                        'status':200,
                        "msg":"Data Updated Successfully",
                        "icon":"success"
                    })
                if fileType == "ManageEmployee":
                    checkingFieldsArray=['title', 'costCenter','ustJobCode','ustProjectId','allocationPercentage','businesssUnit','customer','resignDate','empName', 'empCode', 'ustCode', 'fatherName', 'motherName', 'martialStatus', 'email', 'personalEmailId', 'dob', 'mobile', 'blood', 'country', 'state', 'city', 'pincode', 'address', 'pcountry', 'ppincode', 'pcity', 'pstate', 'paddress', 'panNumber', 'adharNumber', 'circle', 'salaryCurrency', 'experience', 'monthlySalary', 'grossCtc', 'joiningDate', 'lastWorkingDay', 'passport', 'passportNumber', 'bankName', 'accountNumber', 'ifscCode', 'benificiaryname', 'orgLevel', 'designation', 'role', 'userRole', 'band', 'department', 'reportingManager', 'L1Approver', 'L2Approver', 'financeApprover', 'reportingHrManager', 'assetManager', 'L1Vendor', 'L2Vendor', 'L1Compliance', 'L2Compliance', 'L1Commercial', 'L1Sales', 'L2Sales', 'status', 'password']
                    invalid_columns = [col for col in exceldata.columns if col not in checkingFieldsArray]
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Columns are not valid email{invalid_columns}",
                            }
                        )
                    missingEmployeCode = list(exceldata[exceldata["empCode"].isna() | (exceldata["empCode"] == "")].index)
                    if len(missingEmployeCode):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid EmployeeCode{missingEmployeCode}",
                            }
                        )

                    emailEmployee = exceldata[exceldata["email"].isna() | (exceldata["email"] == "")].index
                    emailEmployeelist = list(emailEmployee)
                    if len(emailEmployeelist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid email{emailEmployeelist}",
                            }
                        )
                        
                    empName = exceldata[exceldata["empName"].isna() | (exceldata["empName"] == "")].index
                    empNamelist = list(empName)
                    if len(empNamelist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid Employee Name{empNamelist}",
                            }
                        )
                        
                    if "userRole" in exceldata.columns:
                        userRoleCode = exceldata[exceldata["userRole"].isna() | (exceldata["userRole"] == "")].index
                        userRoleCodelist = list(userRoleCode)
                        if len(userRoleCodelist):
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Rows does not have valid PMISROLE{userRoleCodelist}",
                                }
                            )

                    jsonData = cfc.dfjson(exceldata)
                    errorlist = []

                    arr = [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {
                            "$addFields": {
                                "_id": {"$toString": "$_id"},
                                "userRole": {"$toString": "$userRole"},
                            }
                        },
                    ]
                    data = cmo.finding_aggregate("userRegister", arr)["data"]
                    datas = pd.DataFrame(data)
                    datas["tmp"] = True
                    mergedData = pd.merge(exceldata, datas, "left", "email")
                    id = list(mergedData.index + 1)
                    mergedData.insert(0, "id", id)
                    newData = mergedData[mergedData["tmp"] == True]
                    newData = newData.groupby("email")
                    duplicateEmails = []
                    for employee_name, group in newData:
                        duplicateEmails.append(group.index.tolist()[0] + 2)
                    if len(duplicateEmails):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The email id in {duplicateEmails} these rows is already present",
                            }
                        )
                    mergedData = pd.merge(exceldata, datas, "left", "empCode")
                    id = list(mergedData.index + 1)
                    mergedData.insert(0, "id", id)
                    newData = mergedData[mergedData["tmp"] == True]
                    newData = newData.groupby("empCode")
                    duplicateEmpCodes = []
                    for employee_name, group in newData:
                        duplicateEmails.append(group.index.tolist()[0] + 2)
                    if len(duplicateEmails):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The Emp Code in {duplicateEmails} these rows is already present",
                            }
                        )
                    # Initialize the error list
                    errorlist = []
                    
                    ##Cost center
                    if "costCenter" in exceldata.columns:
                        dbData = [
                            {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    }, 
                                    'costCenter': 1
                                }
                            }
                        ]
                        costCenter = cmo.finding_aggregate("costCenter", dbData)
                        costCenterDatadp = pd.DataFrame.from_dict(costCenter["data"])
                        filtered_exceldata = exceldata[(exceldata["costCenter"].notna())& (exceldata["costCenter"] != "")]
                        costCentermerged = filtered_exceldata.merge(costCenterDatadp, on=["costCenter"], how="left")
                        cresult2 = costCentermerged[costCentermerged["_id"].isna()]
                        unique_c = cresult2["costCenter"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for costCenter_name in unique_c:
                                missing_indices = cresult2[cresult2["costCenter"] == costCenter_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"costCenter not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                            # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                        else:
                            costCenter_dict = costCentermerged.set_index("costCenter")["_id"].to_dict()
                            exceldata["costCenter"] = exceldata["costCenter"].map(costCenter_dict)
                    
                    
                    ### Customer        
                    if "customer" in exceldata.columns:
                        dbData = [
                                {
                                    '$project': {
                                        'customer': '$customerName', 
                                        '_id': {
                                            '$toString': '$_id'
                                        }, 
                                        
                                    }
                                }
                            ]
                        costCenter = cmo.finding_aggregate("customer", dbData)
                        costCenterDatadp = pd.DataFrame.from_dict(costCenter["data"])
                        filtered_exceldata = exceldata[(exceldata["customer"].notna())& (exceldata["customer"] != "")]
                        costCentermerged = filtered_exceldata.merge(costCenterDatadp, on=["customer"], how="left")
                        cresult2 = costCentermerged[costCentermerged["_id"].isna()]
                        unique_c = cresult2["_id"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for costCenter_name in unique_c:
                                missing_indices = cresult2[cresult2["customer"] == costCenter_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"Customer not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                            # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                        else:
                            costCenter_dict = costCentermerged.set_index("customer")["_id"].to_dict()
                            exceldata["customer"] = exceldata["customer"].map(costCenter_dict)    
                            


                    
                    # circle
                    if "circle" in exceldata.columns:
                        dbData = [
                            {
                                '$project': {
                                    'circle': '$circleName', 
                                    '_id': {
                                        '$toString': '$_id'
                                    }, 
                                    
                                }
                            }
                        ]
                        circle = cmo.finding_aggregate("circle", dbData)
                        circleDatadp = pd.DataFrame.from_dict(circle["data"])
                        filtered_exceldata = exceldata[(exceldata["circle"].notna())& (exceldata["circle"] != "")]
                        circlemerged = filtered_exceldata.merge(circleDatadp, on=["circle"], how="left")
                        cresult2 = circlemerged[circlemerged["_id"].isna()]
                        unique_c = cresult2["circle"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for circle_name in unique_c:
                                missing_indices = cresult2[cresult2["circle"] == circle_name].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(f"Circle not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}")
                            # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                        else:
                            circle_dict = circlemerged.set_index("circle")["_id"].to_dict()
                            exceldata["circle"] = exceldata["circle"].map(circle_dict)


                    # Department
                    if "department" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                }
                            },
                            {
                                "$project": {
                                    "departmentId": {"$toString": "$_id"},
                                    "department": 1,
                                    "_id": 0,
                                }
                            },
                        ]
                        department = cmo.finding_aggregate("department", dbData)
                        if len(department['data']):
                            departmentDatadp = pd.DataFrame.from_dict(department["data"])
                            
                            filtered_exceldata = exceldata[
                                (exceldata["department"].notna())
                                & (exceldata["department"] != "")
                            ]
                            departmentmerged = filtered_exceldata.merge(
                                departmentDatadp, on=["department"], how="left"
                            )
                            cresult2 = departmentmerged[
                                departmentmerged["departmentId"].isna()
                            ]
                            unique_c = cresult2["department"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["department"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"Departments not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                                # errorlist.append(f"this department not found: {', '.join(unique_c)}")
                            else:
                                department_dict = departmentmerged.set_index("department")["departmentId"].to_dict()
                                exceldata["department"] = exceldata["department"].map(department_dict)
                                
                        else:
                            errorlist.append(f"Depatment is not found in HR Module")

                    # User Role
                    if "userRole" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                }
                            },
                            {
                                "$addFields": {
                                    "userRoleId": {"$toString": "$_id"},
                                    "userRole": "$roleName",
                                }
                            },
                            {"$project": {"userRoleId": 1, "userRole": 1, "_id": 0}},
                        ]
                        userRole = cmo.finding_aggregate("userRole", dbData)
                        if (len(userRole['data'])):
                            userRoleDatadp = pd.DataFrame.from_dict(userRole["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["userRole"].notna())
                                & (exceldata["userRole"] != "")
                            ]
                            userRolemerged = filtered_exceldata.merge(
                                userRoleDatadp, on=["userRole"], how="left"
                            )
                            cresult2 = userRolemerged[userRolemerged["userRoleId"].isna()]
                            unique_c = cresult2["userRole"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["userRole"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"userRole not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )

                            else:
                                userRole_dict = userRolemerged.set_index("userRole")["userRoleId"].to_dict()
                                exceldata["userRole"] = exceldata["userRole"].map(userRole_dict)
                        else:
                            errorlist.append(f"User Role not found in HR Module")
                   
                    # role
                    if "role" in exceldata.columns:
                        dbData = [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {
                                "$addFields": {
                                    "roleId": {"$toString": "$_id"},
                                    "role": "$roleName",
                                }
                            },
                            {"$project": {"roleId": 1, "role": 1, "_id": 0}},
                        ]
                        Role = cmo.finding_aggregate("userRole", dbData)
                        if len(Role['data']):
                            RoleDatadp = pd.DataFrame.from_dict(Role["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["role"].notna()) & (exceldata["role"] != "")
                            ]
                            Rolemerged = filtered_exceldata.merge(
                                RoleDatadp, on=["role"], how="left"
                            )
                            cresult2 = Rolemerged[Rolemerged["roleId"].isna()]
                            unique_c = cresult2["role"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["role"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"Role not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                            else:
                                role_dict = Rolemerged.set_index("role")["roleId"].to_dict()
                                exceldata["role"] = exceldata["role"].map(role_dict)
                        else:
                            errorlist.append(f"Role Is not define HR Module")
                    
                    # Designation
                    if "designation" in exceldata.columns:
                        dbData = [
                            {"$match": {"deleteStatus": {"$ne": 1}}},
                            {"$addFields": {"designationId": {"$toString": "$_id"}}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "designation": 1,
                                    "designationId": 1,
                                }
                            },
                        ]
                        designation = cmo.finding_aggregate("designation", dbData)
                        if len(designation['data']):
                            
                            designationDatadp = pd.DataFrame.from_dict(designation["data"])
                            filtered_exceldata = exceldata[(exceldata["designation"].notna())& (exceldata["designation"] != "")]
                            designationmerged = filtered_exceldata.merge(designationDatadp, on=["designation"], how="left")
                            cresult2 = designationmerged[designationmerged["designationId"].isna()]
                            unique_c = cresult2["designation"].unique()

                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["designation"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"designation not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                            else:
                                designation_dict = designationmerged.set_index("designation")["designationId"].to_dict()
                                exceldata["designation"] = exceldata["designation"].map(designation_dict)
                        else:
                            errorlist.append(f"Designation is not found in HR module")
                    
                    # L1Approver
                    if "L1Approver" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L1ApproverId": {"$toString": "$_id"},
                                    "L1Approver": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L1Approver": 1,
                                    "L1ApproverId": 1,
                                }
                            },
                        ]
                        L1Approver = cmo.finding_aggregate("userRegister", dbData)
                        L1ApproverDatadp = pd.DataFrame.from_dict(L1Approver["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L1Approver"].notna())
                            & (exceldata["L1Approver"] != "")
                        ]
                        L1Approvermerged = filtered_exceldata.merge(
                            L1ApproverDatadp, on=["L1Approver"], how="left"
                        )
                        cresult2 = L1Approvermerged[
                            L1Approvermerged["L1ApproverId"].isna()
                        ]
                        unique_c = cresult2["L1Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Approver"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L1Approver_dict = L1Approvermerged.set_index("L1Approver")["L1ApproverId"].to_dict()
                            exceldata["L1Approver"] = exceldata["L1Approver"].map(L1Approver_dict)

                    #assetManager
                    if "assetManager" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "assetManagerId": {"$toString": "$_id"},
                                    "assetManager": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "assetManager": 1,
                                    "assetManagerId": 1,
                                }
                            },
                        ]
                        assetManager = cmo.finding_aggregate("userRegister", dbData)
                        assetManagerDatadp = pd.DataFrame.from_dict(assetManager["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["assetManager"].notna())
                            & (exceldata["assetManager"] != "")
                        ]
                        assetManagermerged = filtered_exceldata.merge(
                            assetManagerDatadp, on=["assetManager"], how="left"
                        )
                        cresult2 = assetManagermerged[
                            assetManagermerged["assetManagerId"].isna()
                        ]
                        unique_c = cresult2["assetManager"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["assetManager"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"Asset Manager not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            asset_dict = assetManagermerged.set_index("assetManager")["assetManagerId"].to_dict()
                            exceldata["assetManager"] = exceldata["assetManager"].map(asset_dict)
                            


                    # L1Commercial
                    if "L1Commercial" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L1CommercialId": {"$toString": "$_id"},
                                    "L1Commercial": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L1Commercial": 1,
                                    "L1CommercialId": 1,
                                }
                            },
                        ]
                        L1Commercial = cmo.finding_aggregate("userRegister", dbData)
                        L1CommercialDatadp = pd.DataFrame.from_dict(
                            L1Commercial["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["L1Commercial"].notna())
                            & (exceldata["L1Commercial"] != "")
                        ]
                        L1Commercialmerged = filtered_exceldata.merge(
                            L1CommercialDatadp, on=["L1Commercial"], how="left"
                        )
                        cresult2 = L1Commercialmerged[
                            L1Commercialmerged["L1CommercialId"].isna()
                        ]
                        unique_c = cresult2["L1Commercial"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Commercial"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Commercial not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )
                        else:
                            L1commericial_dict = L1Commercialmerged.set_index("L1Commercial")["L1CommercialId"].to_dict()
                            exceldata["L1Commercial"] = exceldata["L1Commercial"].map(L1commericial_dict)
                            
                    # L1Compliance
                    if "L1Compliance" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L1ComplianceId": {"$toString": "$_id"},
                                    "L1Compliance": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L1Compliance": 1,
                                    "L1ComplianceId": 1,
                                }
                            },
                        ]
                        L1Compliance = cmo.finding_aggregate("userRegister", dbData)
                        L1ComplianceDatadp = pd.DataFrame.from_dict(
                            L1Compliance["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["L1Compliance"].notna())
                            & (exceldata["L1Compliance"] != "")
                        ]
                        L1Compliancemerged = filtered_exceldata.merge(
                            L1ComplianceDatadp, on=["L1Compliance"], how="left"
                        )
                        cresult2 = L1Compliancemerged[
                            L1Compliancemerged["L1ComplianceId"].isna()
                        ]
                        unique_c = cresult2["L1Compliance"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Compliance"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Compliance not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L1Compliance_dict = L1Compliancemerged.set_index("L1Compliance")["L1ComplianceId"].to_dict()
                            exceldata["L1Compliance"] = exceldata["L1Compliance"].map(L1Compliance_dict)

                    # L1Sales
                    if "L1Sales" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L1SalesId": {"$toString": "$_id"},
                                    "L1Sales": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L1Sales": 1, "L1SalesId": 1}},
                        ]
                        L1Sales = cmo.finding_aggregate("userRegister", dbData)
                        L1SalesDatadp = pd.DataFrame.from_dict(L1Sales["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L1Sales"].notna())
                            & (exceldata["L1Sales"] != "")
                        ]
                        L1Salesmerged = filtered_exceldata.merge(
                            L1SalesDatadp, on=["L1Sales"], how="left"
                        )
                        cresult2 = L1Salesmerged[L1Salesmerged["L1SalesId"].isna()]
                        unique_c = cresult2["L1Sales"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Sales"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Sales not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L1Sales_dict = L1Salesmerged.set_index("L1Sales")["L1SalesId"].to_dict()
                            exceldata["L1Sales"] = exceldata["L1Sales"].map(L1Sales_dict)

                    # L1Vendor
                    if "L1Vendor" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L1VendorId": {"$toString": "$_id"},
                                    "L1Vendor": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L1Vendor": 1, "L1VendorId": 1}},
                        ]
                        L1Vendor = cmo.finding_aggregate("userRegister", dbData)
                        L1VendorDatadp = pd.DataFrame.from_dict(L1Vendor["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L1Vendor"].notna())
                            & (exceldata["L1Vendor"] != "")
                        ]
                        L1Vendormerged = filtered_exceldata.merge(
                            L1VendorDatadp, on=["L1Vendor"], how="left"
                        )
                        cresult2 = L1Vendormerged[L1Vendormerged["L1VendorId"].isna()]
                        unique_c = cresult2["L1Vendor"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L1Vendor"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L1Vendor not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L1Vendor_dict = L1Vendormerged.set_index("L1Vendor")["L1VendorId"].to_dict()
                            exceldata["L1Vendor"] = exceldata["L1Vendor"].map(L1Vendor_dict)
                            

                    # L2Approver
                    if "L2Approver" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L2ApproverId": {"$toString": "$_id"},
                                    "L2Approver": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L2Approver": 1,
                                    "L2ApproverId": 1,
                                }
                            },
                        ]
                        L2Approver = cmo.finding_aggregate("userRegister", dbData)
                        L2ApproverDatadp = pd.DataFrame.from_dict(L2Approver["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L2Approver"].notna())
                            & (exceldata["L2Approver"] != "")
                        ]
                        L2Approvermerged = filtered_exceldata.merge(
                            L2ApproverDatadp, on=["L2Approver"], how="left"
                        )
                        cresult2 = L2Approvermerged[
                            L2Approvermerged["L2ApproverId"].isna()
                        ]
                        unique_c = cresult2["L2Approver"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Approver"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Approver not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L2Approver_dict = L2Approvermerged.set_index("L2Approver")["L2ApproverId"].to_dict()
                            exceldata["L2Approver"] = exceldata["L2Approver"].map(L2Approver_dict)
                            
                    # L2Compliance
                    if "L2Compliance" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L2ComplianceId": {"$toString": "$_id"},
                                    "L2Compliance": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "L2Compliance": 1,
                                    "L2ComplianceId": 1,
                                }
                            },
                        ]
                        L2Compliance = cmo.finding_aggregate("userRegister", dbData)
                        L2ComplianceDatadp = pd.DataFrame.from_dict(
                            L2Compliance["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["L2Compliance"].notna())
                            & (exceldata["L2Compliance"] != "")
                        ]
                        L2Compliancemerged = filtered_exceldata.merge(
                            L2ComplianceDatadp, on=["L2Compliance"], how="left"
                        )
                        cresult2 = L2Compliancemerged[
                            L2Compliancemerged["L2ComplianceId"].isna()
                        ]
                        unique_c = cresult2["L2Compliance"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Compliance"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Compliance not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L2Compliance_dict = L2Compliancemerged.set_index("L2Compliance")["L2ComplianceId"].to_dict()
                            exceldata["L2Compliance"] = exceldata["L2Compliance"].map(L2Compliance_dict)
                            

                    # L2Sales
                    if "L2Sales" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L2SalesId": {"$toString": "$_id"},
                                    "L2Sales": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L2Sales": 1, "L2SalesId": 1}},
                        ]
                        L2Sales = cmo.finding_aggregate("userRegister", dbData)
                        L2SalesDatadp = pd.DataFrame.from_dict(L2Sales["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L2Sales"].notna())
                            & (exceldata["L2Sales"] != "")
                        ]
                        L2Salesmerged = filtered_exceldata.merge(
                            L2SalesDatadp, on=["L2Sales"], how="left"
                        )
                        cresult2 = L2Salesmerged[L2Salesmerged["L2SalesId"].isna()]
                        unique_c = cresult2["L2Sales"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Sales"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Sales not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L2Sales_dict = L2Salesmerged.set_index("L2Sales")["L2SalesId"].to_dict()
                            exceldata["L2Sales"] = exceldata["L2Sales"].map(L2Sales_dict)

                    # L2Vendor
                    if "L2Vendor" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "L2VendorId": {"$toString": "$_id"},
                                    "L2Vendor": "$email",
                                }
                            },
                            {"$project": {"_id": 0, "L2Vendor": 1, "L2VendorId": 1}},
                        ]
                        L2Vendor = cmo.finding_aggregate("userRegister", dbData)
                        L2VendorDatadp = pd.DataFrame.from_dict(L2Vendor["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["L2Vendor"].notna())
                            & (exceldata["L2Vendor"] != "")
                        ]
                        L2Vendormerged = filtered_exceldata.merge(
                            L2VendorDatadp, on=["L2Vendor"], how="left"
                        )
                        cresult2 = L2Vendormerged[L2Vendormerged["L2VendorId"].isna()]
                        unique_c = cresult2["L2Vendor"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["L2Vendor"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"L2Vendor not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            L2Vendor_dict = L2Vendormerged.set_index("L2Vendor")["L2VendorId"].to_dict()
                            exceldata["L2Vendor"] = exceldata["L2Vendor"].map(L2Vendor_dict)

                    # Finance Approver
                    if "financeApprover" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "financeApproverId": {"$toString": "$_id"},
                                    "financeApprover": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "financeApprover": 1,
                                    "financeApproverId": 1,
                                }
                            },
                        ]
                        financeApprover = cmo.finding_aggregate("userRegister", dbData)
                        financeApproverDatadp = pd.DataFrame.from_dict(
                            financeApprover["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["financeApprover"].notna())
                            & (exceldata["financeApprover"] != "")
                        ]
                        financeApprovermerged = filtered_exceldata.merge(
                            financeApproverDatadp, on=["financeApprover"], how="left"
                        )
                        cresult2 = financeApprovermerged[
                            financeApprovermerged["financeApproverId"].isna()
                        ]
                        unique_c = cresult2["financeApprover"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["financeApprover"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"financeApprover not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            financeApprover_dict = financeApprovermerged.set_index("financeApprover")["financeApproverId"].to_dict()
                            exceldata["financeApprover"] = exceldata["financeApprover"].map(financeApprover_dict)

                    # Reporting Manager
                    if "reportingManager" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                    "type": {"$ne": "Partner"},
                                }
                            },
                            {
                                "$addFields": {
                                    "reportingManagerId": {"$toString": "$_id"},
                                    "reportingManager": "$email",
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "reportingManager": 1,
                                    "reportingManagerId": 1,
                                }
                            },
                        ]
                        reportingManager = cmo.finding_aggregate("userRegister", dbData)
                        reportingManagerDatadp = pd.DataFrame.from_dict(
                            reportingManager["data"]
                        )
                        filtered_exceldata = exceldata[
                            (exceldata["reportingManager"].notna())
                            & (exceldata["reportingManager"] != "")
                        ]
                        reportingManagermerged = filtered_exceldata.merge(
                            reportingManagerDatadp, on=["reportingManager"], how="left"
                        )
                        cresult2 = reportingManagermerged[
                            reportingManagermerged["reportingManagerId"].isna()
                        ]
                        unique_c = cresult2["reportingManager"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["reportingManager"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [
                                    idx + 2 for idx in missing_indices
                                ]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"reportingManager not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            reportingManager_dict = reportingManagermerged.set_index("reportingManager")["reportingManagerId"].to_dict()
                            exceldata["reportingManager"] = exceldata["reportingManager"].map(reportingManager_dict)

                    # Reporting HR Manager
                    if "reportingHrManager" in exceldata.columns:
                        dbData = [
                            {"$match": {"roleName": "HR Head"}},
                            {
                                "$lookup": {
                                    "from": "userRegister",
                                    "localField": "_id",
                                    "foreignField": "userRole",
                                    "pipeline": [
                                        {"$match": {"deleteStatus": {"$ne": 1}}}
                                    ],
                                    "as": "users",
                                }
                            },
                            {
                                "$unwind": {
                                    "path": "$users",
                                    "preserveNullAndEmptyArrays": True,
                                }
                            },
                            {
                                "$addFields": {
                                    "reportingHrManagerId": {"$toString": "$users._id"},
                                    "reportingHrManager": "$users.email",
                                }
                            },
                            {
                                "$project": {
                                    "reportingHrManagerId": 1,
                                    "_id": 0,
                                    "reportingHrManager": 1,
                                }
                            },
                        ]
                        reportingHrManager = cmo.finding_aggregate("userRole", dbData)
                        if len(reportingHrManager['data']) and "reportingHrManager" in reportingHrManager['data'][0]:
                            reportingHrManagerDatadp = pd.DataFrame.from_dict(reportingHrManager["data"])
                            filtered_exceldata = exceldata[(exceldata["reportingHrManager"].notna()) & (exceldata["reportingHrManager"] != "")]
                            reportingHrManagermerged = filtered_exceldata.merge(
                                reportingHrManagerDatadp,
                                on=["reportingHrManager"],
                                how="left",
                            )
                            cresult2 = reportingHrManagermerged[
                                reportingHrManagermerged["reportingHrManagerId"].isna()
                            ]
                            unique_c = cresult2["reportingHrManager"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["reportingHrManager"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"reportingHrManager not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )

                            else:
                                reportingHrManager_dict = reportingHrManagermerged.set_index("reportingHrManager")["reportingHrManagerId"].to_dict()
                                exceldata["reportingHrManager"] = exceldata["reportingHrManager"].map(reportingHrManager_dict)
                        else:
                            errorlist.append(f"HR Head Role is not found in Manage Profile")
           
                    
                    # Date of Birth    
                    if 'dob' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['dob']) or row['dob'] == "":
                                exceldata.at[index, 'dob'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['dob'] = pd.to_datetime(row['dob'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"dob format is invaid in these rows{invalidRows}") 
                    if 'resignDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            if pd.isna(row['resignDate']) or row['resignDate'] == "":
                                exceldata.at[index, 'resignDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['resignDate'] = pd.to_datetime(row['resignDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"resignDate format is invaid in these rows{invalidRows}") 
                            
                    # Joinging Date       
                    if 'joiningDate' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['joiningDate']) or row['joiningDate'] == "":
                                exceldata.at[index, 'joiningDate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['joiningDate'] = pd.to_datetime(row['joiningDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"joining Date format is invaid in these rows{invalidRows}") 
                            
                    # Last Working Day       
                    if 'lastWorkingDay' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['lastWorkingDay']) or row['lastWorkingDay'] == "":
                                exceldata.at[index, 'lastWorkingDay'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['lastWorkingDay'] = pd.to_datetime(row['lastWorkingDay'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                                    
                        if (len(invalidRows)):   
                            errorlist.append(f"Last Working Day format is invaid in these rows{invalidRows}") 
                    
                            

                    if len(errorlist) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows do not have valid values: {', '.join(errorlist)}",
                            }
                        )

                    data = cfc.dfjson(exceldata)

                    for i in data:

                        
                        if "userRole" in i:
                            if is_valid_mongodb_objectid(i["userRole"]):
                                i["userRole"] = ObjectId(i["userRole"])
                                
                        if "password" in i:
                            if i["password"] != None and i["password"] != "undefined":
                                i["password"] = str(i["password"])
                                      
                        if "dob" in i:
                            i['dob'] = check_and_convert_date3(i['dob'])
                        if "resignDate" in i:
                            i['resignDate'] = check_and_convert_date3(i['resignDate'])
                                
                        if "joiningDate" in i:
                            i['joiningDate'] = check_and_convert_date3(i['joiningDate'])
                
                        if "lastWorkingDay" in i:
                            i['lastWorkingDay'] = check_and_convert_date3(i['lastWorkingDay'])
                            
                        Response = cmo.insertion("userRegister", i)
                    return respond(Response)
                
                
                
                
                
                if fileType == "UpgradeVendor":
                    checkingFieldsArray=['vendorCode', 'vendorName', 'ustCode', 'email', 'vendorCategory', 'vendorSubCategory', 'userRole', 'dateOfRegistration', 'validityUpto', 'ranking', 'paymentTerms', 'Circle', 'vendorRegistered', 'gstNumber', 'status', 'password', 'panNumber', 'tanNumber', 'esiNumber', 'epfNumber', 'stnNumber', 'accountNumber', 'bankName', 'ifscCode', 'bankAddress', 'financialTurnover', 'cbt', 'formToci', 'contactPerson', 'registeredAddress', 'contactDetails', 'SecContactDetails', 'companyType', 'teamCapacity', 'otherInfo']
                    invalid_columns = [col for col in exceldata.columns if col not in checkingFieldsArray]
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Columns ae not defined{invalid_columns}",
                            }
                        )
                    
                    
                    
                    missingData = exceldata[
                        exceldata.isna().any(axis=1) | (exceldata == "").any(axis=1)].index
                    missingDataRows = list(missingData + 2)

                    if len(missingDataRows):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not havehh valid data {missingDataRows}",
                            }
                        )
                    
                    dbData = [
                        {
                            "$match": {
                                "deleteStatus": {"$ne": 1},
                                "type": {"$eq": "Partner"},
                            }
                        },
                        {
                            "$project": {
                                "_id": {"$toString": "$_id"},
                                "vendorCode": 1,
                                "email": 1,
                            }
                        },
                    ]
                    vendorNameCodeData = cmo.finding_aggregate("userRegister", dbData)
                    vendorNameCodeDatadp = pd.DataFrame.from_dict(
                        vendorNameCodeData["data"]
                    )
                    vendormermerged = exceldata.merge(
                        vendorNameCodeDatadp, on=["vendorCode", "email"], how="left"
                    )
                    cresult2 = vendormermerged[vendormermerged["_id"].isna()]
                    unique_combinations = cresult2[
                        ["vendorCode", "email"]
                    ].drop_duplicates()
                    unique_c = unique_combinations.values
                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Pairs of Partner Code and Email not Found {unique_c}",
                            }
                        )
                    arr = [
                        {"$match": {"type": "Partner"}},
                        {"$project": {"_id": 0, "vendorCode": 1}},
                    ]
                    data = cmo.finding_aggregate("userRegister", arr)["data"]
                    # if len(data):
                    #     datas = pd.DataFrame(data)
                    #     datas["tmp"] = True
                    #     mergedData = pd.merge(exceldata, datas, "left", "vendorCode")
                    #     id = list(mergedData.index + 1)
                    #     mergedData.insert(0, "id", id)
                    #     newData = mergedData[mergedData["tmp"] == True]
                    #     newData = newData.groupby("vendorCode")
                    #     duplicatevendorCode = []
                    #     for employee_name, group in newData:
                    #         print(duplicatevendorCode.append(group.index.tolist()[0] + 2))
                    #     if len(duplicatevendorCode):
                    #         return respond(
                    #             {
                    #                 "status": 400,
                    #                 "icon": "error",
                    #                 "msg": f"The vendorCode id in {duplicatevendorCode} these rows is already present",
                    #             }
                    #         )
                    if "userRole" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                }
                            },
                            {
                                "$addFields": {
                                    "userRoleId": {"$toString": "$_id"},
                                    "userRole": "$roleName",
                                }
                            },
                            {"$project": {"userRoleId": 1, "userRole": 1, "_id": 0}},
                        ]
                        userRole = cmo.finding_aggregate("userRole", dbData)
                        if len(userRole):
                            userRoleDatadp = pd.DataFrame.from_dict(userRole["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["userRole"].notna())
                                & (exceldata["userRole"] != "")
                            ]
                            userRolemerged = filtered_exceldata.merge(
                                userRoleDatadp, on=["userRole"], how="left"
                            )
                            cresult2 = userRolemerged[userRolemerged["userRoleId"].isna()]
                            unique_c = cresult2["userRole"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["userRole"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                if len(missing_rows):
                                    return respond(
                                        {
                                            "status": 400,
                                            "icon": "error",
                                            "msg": f"The Role in these rows not found in our db{missing_rows} ",
                                        }
                                    )

                            else:
                                exceldata["userRole"] = userRolemerged["userRoleId"]
                        else:
                            errorlist.append('Role is not Found in Manage Roles')
                    errorlist = []
                    if "Circle" in exceldata.columns:
                        dbData = [
                            {
                                "$project": {
                                    "Circle": "$circleName",
                                    "band": 1,
                                    "_id": {"$toString": "$_id"},
                                }
                            }
                        ]
                        Circle = cmo.finding_aggregate("circle", dbData)
                        if len(Circle): 
                            CircleDatadp = pd.DataFrame.from_dict(Circle["data"])

                            filtered_exceldata = exceldata[
                                (exceldata["Circle"].notna()) & (exceldata["Circle"] != "")
                            ]
                            Circlemerged = filtered_exceldata.merge(
                                CircleDatadp, on=["Circle"], how="left"
                            )
                            cresult2 = Circlemerged[Circlemerged["_id"].isna()]
                            unique_c = cresult2["Circle"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["Circle"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                errorlist.append(
                                    f"Circle not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                                )
                            else:
                                exceldata["Circle"] = Circlemerged["_id"]
                        else:
                            errorlist.append('Circle not Found in Manage Circle')
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The Circle in {errorlist} these rows is not found in our DB",
                            }
                        )
                    if 'accountNumber' in exceldata.columns:
                        exceldata['accountNumber'] = exceldata['accountNumber'].astype(str)
                        
                        # exceldata['accountNumber'] = exceldata['accountNumber'].fillna('').astype(str)
                        # exceldata['accountNumber'] = exceldata['accountNumber'].str.strip()
                    print(exceldata,'dhhdhdh')
                    
                    jsonData = cfc.dfjson(exceldata)
                    

                    
                    
                    
                    if 'dateOfRegistration' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['dateOfRegistration']) or row['dateOfRegistration'] == "":
                                exceldata.at[index, 'dateOfRegistration'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['dateOfRegistration'] = pd.to_datetime(row['dateOfRegistration'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Date Of Registration format is invaid in these rows{invalidRows}")     
                    
                    if 'validityUpto' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['validityUpto']) or row['validityUpto'] == "":
                                exceldata.at[index, 'validityUpto'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['validityUpto'] = pd.to_datetime(row['validityUpto'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Validity Upto format is invalid in these rows{invalidRows}") 
                    print('jsonDatajsonDatajsonData',jsonData)
                    
                    for i in jsonData:
                        
                        i["type"] = "Partner"
                        if "userRole" in i:
                            i["userRole"] = ObjectId(i["userRole"])
                        if "password" in i:
                            checkingkey=['','undefined',None]
                            if 'password' not in checkingkey:
                                i["password"] = str(i["password"])
                        if "dateOfRegistration" in i:
                            i['dateOfRegistration'] = check_and_convert_date(i['dateOfRegistration'])
                        if "validityUpto" in i:
                            i['validityUpto'] = check_and_convert_date(i['validityUpto'])
                        if "userRole" in i:
                            i["userRole"] = ObjectId(i["userRole"])
                        if 'vendorName' in i:
                            if i['vendorName'] not in ['',None,'undefined']:
                                i['vendorName'] = str(i['vendorName'])
                                i['empName']=str(i['vendorName'])
                        if 'vendorCode' in i:
                            if i['vendorCode'] not in ['',None,'undefined']:
                                
                                i['empCode']=str(i['vendorCode'])
                        if 'accountNumber' in i:
                            if i['accountNumber'] not in ['',None,'undefined']:
                                i['accountNumber'] = str(i['accountNumber'])
                                
                        if 'ustCode' in i:
                            if i['ustCode'] not in ['',None,'undefined']:
                                i['ustCode'] = str(i['ustCode'])
                        Response = cmo.updating(
                            "userRegister", {"email": i["email"],"deleteStatus": {"$ne": 1}}, i, False
                        )
                    return respond(Response)
                if fileType == "ManageVendor":
                    checkingFieldsArray=['vendorCode', 'vendorName', 'ustCode', 'email', 'vendorCategory', 'vendorSubCategory', 'userRole', 'dateOfRegistration', 'validityUpto', 'ranking', 'paymentTerms', 'Circle', 'vendorRegistered', 'gstNumber', 'status', 'password', 'panNumber', 'tanNumber', 'esiNumber', 'epfNumber', 'stnNumber', 'accountNumber', 'bankName', 'ifscCode', 'bankAddress', 'financialTurnover', 'cbt', 'formToci', 'contactPerson', 'registeredAddress', 'contactDetails', 'SecContactDetails', 'companyType', 'teamCapacity', 'otherInfo']
                    invalid_columns = [col for col in exceldata.columns if col not in checkingFieldsArray]
                    if len(invalid_columns):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Columns ae not defined{invalid_columns}",
                            }
                        )
                    missingvendorCode = exceldata[
                        exceldata["vendorCode"].isna() | (exceldata["vendorCode"] == "")
                    ].index
                    missingvendorCodelist = list(missingvendorCode)
                    if len(missingvendorCodelist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid vendorCode{missingvendorCodelist}",
                            }
                        )

                    emailVendor = exceldata[
                        exceldata["email"].isna() | (exceldata["email"] == "")
                    ].index
                    emailVendorlist = list(emailVendor)
                    if len(emailVendorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid email{emailVendorlist}",
                            }
                        )
                    vendorName = exceldata[
                        exceldata["vendorName"].isna() | (exceldata["vendorName"] == "")
                    ].index
                    vendorNamelist = list(vendorName)
                    if len(vendorName):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"These Rows does not have valid vendor Name{vendorNamelist}",
                            }
                        )

                    if "userRole" in exceldata.columns:
                        userRoleCode = exceldata[
                            exceldata["userRole"].isna() | (exceldata["userRole"] == "")
                        ].index
                        userRoleCodelist = list(userRoleCode)
                        if len(userRoleCodelist):
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Rows does not have valid ROLE{userRoleCodelist}",
                                }
                            )
                    # if 'status' in exceldata.columns:
                    #     exceldata['status'] = exceldata['status'].replace([None, np.nan, ''], 'Active')
                        
                    arr = [
                        {"$match": {"type": "Partner"}},
                        {"$project": {"_id": 0, "vendorCode": 1}},
                    ]

                    data = cmo.finding_aggregate("userRegister", arr)["data"]
                    if len(data):
                        datas = pd.DataFrame(data)
                        datas["tmp"] = True
                        mergedData = pd.merge(exceldata, datas, "left", "vendorCode")
                        id = list(mergedData.index + 1)
                        mergedData.insert(0, "id", id)
                        newData = mergedData[mergedData["tmp"] == True]
                        newData = newData.groupby("vendorCode")
                        duplicatevendorCode = []
                        for employee_name, group in newData:
                            print(
                                duplicatevendorCode.append(group.index.tolist()[0] + 2)
                            )
                        if len(duplicatevendorCode):
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"The vendorCode id in {duplicatevendorCode} these rows is already present",
                                }
                            )

                    if "userRole" in exceldata.columns:
                        dbData = [
                            {
                                "$match": {
                                    "deleteStatus": {"$ne": 1},
                                }
                            },
                            {
                                "$addFields": {
                                    "userRoleId": {"$toString": "$_id"},
                                    "userRole": "$roleName",
                                }
                            },
                            {"$project": {"userRoleId": 1, "userRole": 1, "_id": 0}},
                        ]
                        userRole = cmo.finding_aggregate("userRole", dbData)
                        if len(userRole):
                            userRoleDatadp = pd.DataFrame.from_dict(userRole["data"])
                            filtered_exceldata = exceldata[
                                (exceldata["userRole"].notna())
                                & (exceldata["userRole"] != "")
                            ]
                            userRolemerged = filtered_exceldata.merge(
                                userRoleDatadp, on=["userRole"], how="left"
                            )
                            cresult2 = userRolemerged[userRolemerged["userRoleId"].isna()]
                            unique_c = cresult2["userRole"].unique()
                            if len(unique_c) > 0:
                                missing_rows = []
                                for department_name in unique_c:
                                    missing_indices = cresult2[
                                        cresult2["userRole"] == department_name
                                    ].index.tolist()
                                    missing_row_numbers = [
                                        idx + 2 for idx in missing_indices
                                    ]
                                    missing_rows.extend(missing_row_numbers)
                                if len(missing_rows):
                                    return respond(
                                        {
                                            "status": 400,
                                            "icon": "error",
                                            "msg": f"The Role in these rows not found in our db{missing_rows} ",
                                        }
                                    )

                            else:
                                exceldata["userRole"] = userRolemerged["userRoleId"]
                        else:
                            errorlist.append("userRole is not Found in DB")
                    arr = [
                        {"$match": {"deleteStatus": {"$ne": 1}}},
                        {"$project": {"_id": 0, "vendorCode": 1, "email": 1}},
                    ]

                    data = cmo.finding_aggregate("userRegister", arr)["data"]
                    if len(data):
                        datas = pd.DataFrame(data)
                        datas["tmp"] = True
                        mergedData = pd.merge(exceldata, datas, "left", "email")
                        id = list(mergedData.index + 1)
                        mergedData.insert(0, "id", id)
                        newData = mergedData[mergedData["tmp"] == True]
                        newData = newData.groupby("email")
                        duplicatevendorCode = []
                        for employee_name, group in newData:
                            print(
                                duplicatevendorCode.append(group.index.tolist()[0] + 2)
                            )
                        if len(duplicatevendorCode):
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"The Partner email in {duplicatevendorCode} these rows is already present",
                                }
                            )

                    errorlist = []
                    dbData = [
                        {
                            "$project": {
                                "Circle": "$circleName",
                                "band": 1,
                                "_id": {"$toString": "$_id"},
                            }
                        }
                    ]
                    Circle = cmo.finding_aggregate("circle", dbData)
                    if len(Circle):
                        CircleDatadp = pd.DataFrame.from_dict(Circle["data"])
                        filtered_exceldata = exceldata[
                            (exceldata["Circle"].notna()) & (exceldata["Circle"] != "")
                        ]
                        Circlemerged = filtered_exceldata.merge(
                            CircleDatadp, on=["Circle"], how="left"
                        )
                        cresult2 = Circlemerged[Circlemerged["_id"].isna()]
                        unique_c = cresult2["Circle"].unique()
                        if len(unique_c) > 0:
                            missing_rows = []
                            for department_name in unique_c:
                                missing_indices = cresult2[
                                    cresult2["Circle"] == department_name
                                ].index.tolist()
                                missing_row_numbers = [idx + 2 for idx in missing_indices]
                                missing_rows.extend(missing_row_numbers)
                            errorlist.append(
                                f"Circle not found: {', '.join(unique_c)}. Row numbers: {', '.join(map(str, missing_rows))}"
                            )

                        else:
                            exceldata["Circle"] = Circlemerged["_id"]
                    else:
                        errorlist.append('No circle Defined in DB')
                        
                        
                    if 'dateOfRegistration' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['dateOfRegistration']) or row['dateOfRegistration'] == "":
                                exceldata.at[index, 'dateOfRegistration'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['dateOfRegistration'] = pd.to_datetime(row['dateOfRegistration'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Date Of Registration format is invaid in these rows{invalidRows}")     
                    
                    if 'validityUpto' in exceldata.columns:
                        invalidRows=[]
                        for index, row in exceldata.iterrows():
                            
                            if pd.isna(row['validityUpto']) or row['validityUpto'] == "":
                                exceldata.at[index, 'validityUpto'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['validityUpto'] = pd.to_datetime(row['validityUpto'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidRows.append(index+2)
                        if (len(invalidRows)):   
                            errorlist.append(f"Validity Upto format is invalid in these rows{invalidRows}") 
                    
                        
                    
                    if len(errorlist):
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": f"The Circle in {errorlist} these rows is not found in our DB",
                            }
                        )
                    jsonData = cfc.dfjson(exceldata)
                    for i in jsonData:
                        i["type"] = "Partner"
                        checkingArray=['None','undefined','']
                        if 'status' in i:
                            if i['status']  in checkingArray:
                                i['status']='Active'
                        if 'status' not in i:
                            i['status']='Active'   
                        if "password" in i:
                            checkinCondition=['','undefined',None]
                            if i["password"] not in checkinCondition:
                                i["password"] = str(i["password"])
                        if "dateOfRegistration" in i:
                            i['dateOfRegistration'] = check_and_convert_date(i['dateOfRegistration'])
                        if "validityUpto" in i:
                            i['validityUpto'] = check_and_convert_date(i['validityUpto'])
                        if "userRole" in i:
                            i["userRole"] = ObjectId(i["userRole"])
                        if 'vendorName' in i:
                            if i['vendorName'] not in ['',None,'undefined']:
                                i['vendorName'] = str(i['vendorName'])
                                i['empName']=str(i['vendorName'])
                        if 'vendorCode' in i:
                            if i['vendorCode'] not in ['',None,'undefined']:
                                i['vendorCode'] = str(i['vendorCode'])
                                i['vendorCode']=str(i['vendorCode'])
                        if 'ustCode' in i:
                            if i['ustCode'] not in ['',None,'undefined']:
                                i['ustCode'] = str(i['ustCode'])
                        Response = cmo.insertion("userRegister", i)
                    return respond(Response)
                if fileType == "ManageCircle":
                    dbData = [
                        {
                            "$project": {
                                "customer": "$customerName",
                                "uniqueId": "$_id",
                            }
                        },
                        {"$project": {"_id": 0}},
                    ]
                    customer = cmo.finding_aggregate("customer", dbData)
                    customerDatadp = pd.DataFrame.from_dict(customer["data"])

                    customermerged = exceldata.merge(
                        customerDatadp, on=["customer"], how="left"
                    )

                    cresult2 = customermerged[customermerged["uniqueId"].isna()]
                    unique_c_with_index = cresult2[["customer"]].reset_index()
                    unique_c = cresult2["customer"].unique()

                    if len(unique_c) > 0:
                        unique_c_list = unique_c_with_index.apply(
                            lambda row: f"RowNo:-{row['index']+1}: {row['customer']}",
                            axis=1,
                        )
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "The following Customers were not found in the Database\n"
                                + "\n".join(unique_c_list),
                            }
                        )

                    else:
                        customermerged["customer"] = customermerged["uniqueId"]
                        new_df = customermerged[
                            ["customer", "circleName", "circleCode","band"]
                        ].copy()
                        new_df["band"] = new_df["band"].astype(str)
                        dict_data = new_df.to_dict(orient="records")
                        for i in dict_data:
                            data = {
                                "circleCode": i["circleCode"],
                                "customer": i["customer"],
                                "deleteStatus": {"$ne": 1},
                            }
                            response = cmo.updating("circle", data, i, True)
                        return respond(response)

                if fileType == "ManageZone":
                    dbData = [
                        {
                            "$project": {
                                "customer": "$customerName",
                                "uniqueId": "$_id",
                                "_id": 0,
                            }
                        }
                    ]
                    customer = cmo.finding_aggregate("customer", dbData)
                    customerDatadp = pd.DataFrame.from_dict(customer["data"])

                    customermerged = exceldata.merge(customerDatadp, on=["customer"], how="left")

                    cresult2 = customermerged[customermerged["uniqueId"].isna()]
                    unique_c = cresult2["customer"].unique()

                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "Customer Not Found " + ", ".join(unique_c),
                            }
                        )
                    else:
                        customermerged["customer"] = customermerged["uniqueId"]

                    dbData = [
                        {
                            "$project": {
                                "circle": "$circleName",
                                "uniqueId": "$_id",
                                "_id": 0,
                            }
                        }
                    ]
                    circle = cmo.finding_aggregate("circle", dbData)
                    circleDatadp = pd.DataFrame.from_dict(circle["data"])

                    result = customermerged.apply(my_function, args=(circleDatadp,), axis=1)

                    resulting = result.iloc[0]

                    if resulting["status"] != 200:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "Circle Not Found " + resulting["circle"],
                            }
                        )

                    else:
                        new_df = result[
                            ["customer", "zoneName", "shortCode", "circle"]
                        ].copy()
                        dict_data = new_df.to_dict(orient="records")
                        for i in dict_data:
                            data = {
                                "shortCode": i["shortCode"],
                                "customer": i["customer"],
                                "deleteStatus": {"$ne": 1},
                            }
                            response = cmo.updating("zone", data, i, True)
                            if response["status"] != 200:
                                return respond(response)
                        return respond(response)

                if fileType == "ManageCostCenter":
                    dbData = [
                        {
                            "$project": {
                                "customer": "$customerName",
                                "uniqueId": "$_id",
                            }
                        },
                        {"$project": {"_id": 0}},
                    ]
                    customer = cmo.finding_aggregate("customer", dbData)
                    customerDatadp = pd.DataFrame.from_dict(customer["data"])
                    customermerged = exceldata.merge(
                        customerDatadp, on=["customer"], how="left"
                    )
                    cresult2 = customermerged[customermerged["uniqueId"].isna()]
                    unique_c = cresult2["customer"].unique()

                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "Customer Not Found " + ", ".join(unique_c),
                            }
                        )

                    else:
                        customermerged["customer"] = customermerged["uniqueId"]

                    dbData = [
                        {
                            "$project": {
                                "zone": "$shortCode",
                                "uid": "$_id",
                                "_id": 0,
                            }
                        }
                    ]
                    zone = cmo.finding_aggregate("zone", dbData)
                    zoneDatadp = pd.DataFrame.from_dict(zone["data"])
                    zonemermerged = customermerged.merge(
                        zoneDatadp, on=["zone"], how="left"
                    )
                    cresult2 = zonemermerged[zonemermerged["uid"].isna()]
                    unique_c = cresult2["zone"].unique()

                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "Zone Not Found " + ", ".join(unique_c),
                            }
                        )
                    else:
                        zonemermerged["zone"] = zonemermerged["uid"]
                        new_df = zonemermerged[["customer", "zone", "costCenter","description","businessUnit"]].copy()
                        filtered_df = new_df[new_df['description'].str.strip() != '']
                        duplicated_descriptions = filtered_df[filtered_df['description'].duplicated(keep=False)].index.tolist()
                        if duplicated_descriptions:
                            duplicated_descriptions = [ x+2 for x in duplicated_descriptions]
                            return respond({
                                'status':400,
                                "icon":"error",
                                "msg":f"Rows where Description is found to be Duplicate:- {duplicated_descriptions}"
                            })
                        dict_data = new_df.to_dict(orient="records")
                        errorlist = []
                        for index,i in enumerate(dict_data):
                            if i['description']!="":
                                arra = [
                                    {
                                        '$match':{
                                            'description':i['description']
                                        }
                                    }
                                ]
                                length = len(cmo.finding_aggregate("costCenter",arra)['data'])
                                if length:
                                    errorlist.append(index)
                        if errorlist:
                            errorlist = [x+2 for x in errorlist]
                            return respond({
                                'msg':f"Thoes rows where description is already found in Database:-{errorlist}",
                                "status":400,
                                "icon":"error"
                            })
                        for i in dict_data:
                            data = {
                                "customer": i["customer"],
                                "zone": i["zone"],
                                "costCenter": i["costCenter"],
                                "deleteStatus": {"$ne": 1},
                            }
                            response = cmo.updating("costCenter", data, i, True)
                        return respond(response)

                if fileType == "PoInvoice":
                    
                    arra = [
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
                                'Customer': '$customer.customerName', 
                                'customeruid': {
                                    '$toString': '$customer._id'
                                }
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
                                'projectGroup': 1, 
                                'customer': '$Customer', 
                                'customeruid': 1, 
                                'projectGroupuid': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ]
                    Data = cmo.finding_aggregate("projectGroup",arra)
                    if len(Data):
                        DataDf = pd.DataFrame.from_dict(Data["data"])
                        Datamerged = exceldata.merge(DataDf, on=["customer","projectGroup"], how="left")
                        result = Datamerged[Datamerged["customeruid"].isna()]
                        unique_c = result[["customer","projectGroup"]].drop_duplicates()
                        if len(unique_c) > 0:
                            pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "The combination of Customer and Project Group was not found in the database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                }
                            )
                        else:
                            exceldata["customer"] = Datamerged["customeruid"]
                            exceldata["projectGroup"] = Datamerged["projectGroupuid"]
                
                        new_df = exceldata[["customer","projectGroup","gbpa","poNumber","poStartDate","poEndDate","itemCode","description","unitRate(INR)","initialPoQty"]].copy()
                        missingPONumber = list(new_df[new_df["poNumber"].isna() |  (new_df["poNumber"] == "") ].index)
                        if len(missingPONumber):
                            missingPONumber = [x + 2 for x in missingPONumber]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Rows do not have a PO Number:-{missingPONumber}",
                                }
                            )
                        invalidPoStartDate = []
                        invalidPoEndDate = []
                        expected_format = '%d-%m-%Y'
                        for index, row in new_df.iterrows():
                            if pd.isna(row['poStartDate']) or row['poStartDate'] == '':
                                row['poStartDate'] = None
                            else:
                                try:
                                    row['poStartDate'] = pd.to_datetime(row['poStartDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidPoStartDate.append(index+2)
                                   
                        if (len(invalidPoStartDate)):
                            return respond({
                                "status":400,
                                "icon":"error",
                                "msg":f"These rows have incorrect PO Start Date format or the PO Start Date may be empty:-{invalidPoStartDate}"
                            })
                        
                        for index, row in new_df.iterrows():
                            if pd.isna(row['poEndDate']) or row['poEndDate'] == '':
                                row['poEndDate'] = None
                            else:    
                                try:
                                    row['poEndDate'] = pd.to_datetime(row['poEndDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidPoEndDate.append(index+2)
                            
                        if (len(invalidPoEndDate)):
                            return respond({
                                "status":400,
                                "icon":"error",
                                "msg":f"These rows have incorrect PO End Date format or the PO End Date may be empty:-{invalidPoEndDate}"
                            })
                        
                        missingItemCode = list(new_df[new_df["itemCode"].isna() |  (new_df["itemCode"] == "") ].index)  
                        if len(missingItemCode):
                            missingItemCode = [x + 2 for x in missingItemCode]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These rows do not have an item code:-{missingItemCode}",
                                }
                            )
                        greaterDate = new_df[new_df['poStartDate'] > new_df['poEndDate']].index.tolist()
                        
                        if (greaterDate):
                            greaterDate = [x+2 for x in greaterDate]
                            return respond({
                                'status':400,
                                "icon":'error',
                                "msg":f"Rows where Po Start Date is greater than Po End Date:- {greaterDate}"
                            })

                        def is_numeric(value):
                            try:
                                numeric_value = pd.to_numeric(value)
                                if numeric_value > 0 and numeric_value == int(numeric_value):
                                    return True
                                else:
                                    return False
                            except ValueError:
                                return False
                            
                        is_numeric_column = new_df["unitRate(INR)"].apply(is_numeric)
                        is_numeric_column = is_numeric_column[is_numeric_column == False].index.tolist()
                        
                        if len(is_numeric_column) > 0:
                            is_numeric_column = [x + 2 for x in is_numeric_column]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"Rows where the Unit Rate is not a positive integer:-{is_numeric_column}",
                                }
                            )
                            
                        is_numeric_column = new_df["initialPoQty"].apply(is_numeric)
                        is_numeric_column = is_numeric_column[is_numeric_column == False].index.tolist()
                        
                        if len(is_numeric_column) > 0:
                            is_numeric_column = [x + 2 for x in is_numeric_column]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"Rows where the PO Initial Quantity is not a positive integer:-{is_numeric_column}",
                                }
                            )

                        new_df["gbpa"] = new_df["gbpa"].astype(str)
                        new_df["poNumber"] = new_df["poNumber"].astype(str)
                        new_df["itemCode"] = new_df["itemCode"].astype(str)
                        
                        duplicates = new_df[new_df.duplicated(['projectGroup', 'poNumber', 'itemCode'], keep=False)].index.tolist()
                        if (duplicates):
                            duplicates = [x + 2 for x in duplicates]
                            return respond({
                                'status':400,
                                "icon":"error",
                                "msg":f"These Rows have duplicate Project Group, poNumber and itemCode:- {duplicates}"
                            })
                            
                        
                        current_df = new_df[['projectGroup', 'poNumber', 'itemCode',"initialPoQty"]].copy()
                        arra = [
                            {
                                '$project': {
                                    'projectGroup': 1, 
                                    'poNumber': 1, 
                                    'itemCode': 1, 
                                    'invoicedQty': '$qty', 
                                    '_id': 0
                                }
                            }
                        ]
                        invoice  = cmo.finding_aggregate("invoice",arra)['data']
                        if len(invoice):
                            invoicedataframe = pd.DataFrame(invoice)
                            invoicedataframe_summed = invoicedataframe.groupby(['projectGroup', 'poNumber', 'itemCode'], as_index=False)['invoicedQty'].sum()
                            datamerged = current_df.merge(invoicedataframe_summed, on=['projectGroup', 'poNumber', 'itemCode'],how="left")
                            filtered_data = datamerged[datamerged['initialPoQty'] <= datamerged['invoicedQty']].index.tolist()
                            if (filtered_data):
                                filtered_data = [x+2 for x in filtered_data]
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Rows where the PO initial quantity is less than or equal to the invoiced quantity:-{filtered_data}"
                                })
                        new_df.replace({np.nan: None}, inplace=True)
                        dict_data = new_df.to_dict(orient="records")
                        dataArray = []
                        for i in dict_data:
                            i["itemCodeStatus"] = "Open"
                            i['poStatus'] = "Open"

                            data = {
                                "projectGroup": i["projectGroup"],
                                "itemCode": i["itemCode"],
                                "poNumber": i["poNumber"],
                                "deleteStatus": {"$ne": 1},
                            }
                            response = cmo.updating("PoInvoice", data, i, True)
                        return respond(response)
                    else:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":"There are no project groups found in the database."
                        })

                if fileType == "invoice":
                    duplicates = exceldata[exceldata.duplicated(['projectGroup', 'systemId',"poNumber","itemCode","invoiceNumber"], keep=False)].index.tolist()
                    if (duplicates):
                        duplicates = [x + 2 for x in duplicates]
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":f"These Rows have duplicate Project Group, SSID,PO Number,Item Code, Invoice Number:- {duplicates}"
                        })
                    
                    invalidStatus = exceldata[~exceldata['status'].isin(['Billed', 'Partially Billed'])].index.tolist()
                    if (len(invalidStatus)):
                        return respond({
                            "status":400,
                            "icon":"error",
                            "msg":'The "Status" column contains only the values "Billed" or "Partially Billed".'
                        })

                    PID = sample_projectId(current_user['roleName'],current_user['userUniqueId'])
                    PG = sample_pg()
                    
                    mergeData = PID.merge(PG,on='Project Group',how='left')
                    
                    
                    mergeData['customer'] = mergeData['Customer']
                    mergeData['projectGroup'] = mergeData['projectGroupName']
                    mergeData['projectId'] = mergeData['projectId']
                    mergeData['projectGroupuid'] = mergeData['Project Group']
                    mergeData['projectuid'] = mergeData['Project ID']
                    olddf = mergeData[['customer','projectGroup','projectId','customeruid','projectGroupuid','projectuid']]

                    
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': { 
                                'systemId': 1, 
                                'siteuid': {
                                    '$toString': '$_id'
                                }, 
                                'projectuid': '$projectuniqueId', 
                                '_id': 0
                            }
                        }
                    ]
                    allData = cmo.finding_aggregate("SiteEngineer",dbData)['data']
                    if len(allData):
                        allData = pd.DataFrame.from_dict(allData)
                        allDatadf = allData.merge(olddf,on='projectuid',how='left')
                        allDatamerged = exceldata.merge(allDatadf, on=["customer","projectGroup","projectId","systemId"], how='left')
                        result = allDatamerged[allDatamerged["customeruid"].isna()]
                        unique_c = result[["customer","projectGroup","projectId","systemId"]].drop_duplicates()
                        if len(unique_c) > 0:
                            pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "The combination of Customer, Project Group, Project ID,SSID was not found in the database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                }
                            )
                        else:
                            allDatamerged = allDatamerged.assign(
                                PG = allDatamerged['projectGroup'],
                                customer=allDatamerged["customeruid"],
                                projectGroup=allDatamerged["projectGroupuid"],
                                projectId=allDatamerged["projectuid"],
                                systemId = allDatamerged['siteuid'],
                                siteId = allDatamerged['siteuid'],
                            )
                        new_df = allDatamerged[["customer","projectGroup","projectId","systemId","siteId","wccNumber","wccSignOffdate","poNumber","itemCode","qty","invoiceNumber","invoiceDate","unitRate","status","PG"]].copy()
                        missingPONumber = list(new_df[new_df["poNumber"].isna() | (new_df["poNumber"] == "")].index)
                        if len(missingPONumber):
                            missingPONumber = [x + 2 for x in missingPONumber]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Rows does not have PO Number:-{missingPONumber}",
                                }
                            )
                            
                        missingItemCode = list(new_df[new_df["itemCode"].isna() | (new_df["itemCode"] == "")].index)
                        if len(missingItemCode):
                            missingItemCode = [x + 2 for x in missingItemCode]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Rows does not have Item Code:-{missingItemCode}",
                                }
                            )
                            
                        missinginvoiceNumber = list(new_df[new_df["invoiceNumber"].isna() | (new_df["invoiceNumber"] == "")].index)
                        if len(missinginvoiceNumber):
                            missinginvoiceNumber = [x + 2 for x in missinginvoiceNumber]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"These Rows does not have Invoice Number:-{missinginvoiceNumber}",
                                }
                            )
                        

                        def is_numeric(value):
                            try:
                                numeric_value = pd.to_numeric(value)
                                if numeric_value > 0 and numeric_value == int(numeric_value):
                                    return True
                                else:
                                    return False
                            except ValueError:
                                return False

                        is_numeric_column = new_df["qty"].apply(is_numeric)
                        is_numeric_column = is_numeric_column[is_numeric_column == False].index.tolist()
                        
                        if len(is_numeric_column) > 0:
                            is_numeric_column = [x + 2 for x in is_numeric_column]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"Rows where the Invoiced Quantity is not a positive integer number:-{is_numeric_column}",
                                }
                            )
                            
                        def is_numeric1(value):
                            try:
                                numeric_value = pd.to_numeric(value)
                                if numeric_value >= 0 and numeric_value == int(numeric_value):
                                    return True
                                else:
                                    return False
                            except ValueError:
                                return False
                        is_numeric_column = new_df["unitRate"].apply(is_numeric1)
                        is_numeric_column = is_numeric_column[is_numeric_column == False].index.tolist()

                        if len(is_numeric_column) > 0:
                            is_numeric_column = [x + 2 for x in is_numeric_column]
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": f"Rows where the Unit Rate is not a Whole Number:-{is_numeric_column}",
                                }
                            )
                            
                        invalidInvoiceDate=[]
                        expected_format = '%d-%m-%Y'
                        for index, row in new_df.iterrows():
                            if pd.isna(row['invoiceDate']) or row['invoiceDate'] == "":
                                invalidInvoiceDate.append(index+2)
                            else:
                                try:
                                    row['invoiceDate'] = pd.to_datetime(row['invoiceDate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidInvoiceDate.append(index+2)
                                    
                        if (len(invalidInvoiceDate)):   
                            return respond({
                                'msg':f"Invoice Date format is invaid in these rows:-{invalidInvoiceDate}",
                                "icon":"error",
                                "status":400
                                
                            })
                            
                        invalidWccDate=[]
                        for index, row in new_df.iterrows():
                            
                            if pd.isna(row['wccSignOffdate']) or row['wccSignOffdate'] == "":
                                new_df.at[index, 'wccSignOffdate'] = None
                            else:
                                try:
                                    expected_format = '%d-%m-%Y'
                                    row['wccSignOffdate'] = pd.to_datetime(row['wccSignOffdate'], format=expected_format, errors='raise')
                                except Exception as e:
                                    invalidWccDate.append(index+2)
                                    
                        if (len(invalidWccDate)):   
                            return respond({
                                'msg':f"Wcc Sign Off Date format is invaid in these rows:-{invalidWccDate}",
                                "icon":"error",
                                "status":400
                                
                            })
                        
                        new_df["itemCode"] = new_df["itemCode"].astype(str)
                        new_df["poNumber"] = new_df["poNumber"].astype(str)
                        new_df["invoiceNumber"] = new_df["invoiceNumber"].astype(str)
                        
                        current_df = new_df[['projectGroup','itemCode','poNumber']].copy()
                        arra = [
                            {
                                "$match":{
                                    'deleteStatus':{'$ne':1}
                                }
                            }, {
                                '$lookup': {
                                    'from': 'invoice', 
                                    'let': {
                                        'projectGroup': '$projectGroup', 
                                        'poNumber': '$poNumber', 
                                        'itemCode': '$itemCode'
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
                                                                '$projectGroup', '$$projectGroup'
                                                            ]
                                                        }, {
                                                            '$eq': [
                                                                '$poNumber', '$$poNumber'
                                                            ]
                                                        }, {
                                                            '$eq': [
                                                                '$itemCode', '$$itemCode'
                                                            ]
                                                        }
                                                    ]
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
                                                }
                                            }
                                        }, {
                                            '$group': {
                                                '_id': None, 
                                                'qty': {
                                                    '$sum': '$qty'
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
                                    'invoicedQty': '$result.qty'
                                }
                            }, {
                                '$addFields': {
                                    'invoicedQty': {
                                        '$ifNull': [
                                            '$invoicedQty', 0
                                        ]
                                    }
                                }
                            }, {
                                '$addFields': {
                                    'openQty': {
                                        '$subtract': [
                                            '$initialPoQty', '$invoicedQty'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'projectGroup': 1, 
                                    'poNumber': 1, 
                                    'itemCode': 1, 
                                    'initialPoQty': 1, 
                                    'invoicedQty': 1, 
                                    'openQty': 1, 
                                    "itemCodeStatus":1,
                                    "poStatus":1,
                                    '_id': 0
                                }
                            }
                        ]
                        POInvoiceData = cmo.finding_aggregate("PoInvoice",arra)['data']
                        if len(POInvoiceData):
                            POInvoiceDatadf = pd.DataFrame.from_dict(POInvoiceData)
                            POInvoiceDatamerged = current_df.merge(POInvoiceDatadf,on=['projectGroup','itemCode','poNumber'], how='left')
                            result1 = POInvoiceDatamerged[POInvoiceDatamerged["initialPoQty"].isna()].index.tolist()
                            if len(result1):
                                result1 = [x+2 for x in result1]
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Rows where the combination of Project Group,PO Number and Item Code is not found in the Database:- {result1}"
                                })
                            result2 = POInvoiceDatamerged[POInvoiceDatamerged['itemCodeStatus'] == 'Closed'].index.tolist()
                            if len(result2):
                                result2 = [x+2 for x in result2]
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":f"Rows where the Item Code Status is Already marked as Closed:- {result2}"
                                })
                            POInvoiceDatamerged = POInvoiceDatamerged[['projectGroup','itemCode','poNumber',"initialPoQty","poStatus","itemCodeStatus","invoicedQty","openQty"]].drop_duplicates()
                            
                            arra = [
                                {
                                    "$match":{
                                        'deleteStatus':{'$ne':1}
                                    }
                                }, {
                                    '$project':{
                                        'invoiceNumber':1,
                                        "poNumber":1,
                                        "itemCode":1,
                                        "projectGroup":1,
                                        "siteId":1,
                                        "systemId":1,
                                        "quantity":'$qty',
                                        '_id':0  
                                    }
                                }
                            ]
                            invoiceBill = cmo.finding_aggregate("invoice",arra)['data']
                            current_df = new_df[['projectGroup','poNumber','itemCode','systemId',"siteId","invoiceNumber"]].copy()
                            if len(invoiceBill):
                                invoiceBilldf = pd.DataFrame.from_dict(invoiceBill)
                                invoiceBilldfmerged = current_df.merge(invoiceBilldf,on=['projectGroup','poNumber','itemCode','systemId',"siteId","invoiceNumber"],how='left')
                                filtered_df = invoiceBilldfmerged.dropna(subset=['quantity'])
                                if not filtered_df.empty:
                                    temp_df = POInvoiceDatamerged.copy()
                                    POInvoiceDatamerged = POInvoiceDatamerged.merge(filtered_df, on=['projectGroup', 'poNumber', 'itemCode'], how='left', suffixes=('', '_filtered'))
                                    for column in temp_df.columns:
                                        if column not in ['projectGroup', 'poNumber', 'itemCode']:
                                            POInvoiceDatamerged[column] = POInvoiceDatamerged[column].combine_first(temp_df[column])
                                    POInvoiceDatamerged['quantity'] = POInvoiceDatamerged['quantity'].fillna(0)
                                    POInvoiceDatamerged['openQty'] = POInvoiceDatamerged['openQty'] + POInvoiceDatamerged['quantity']
            
                            invoicedataframe_summed = new_df.groupby(['projectGroup', 'poNumber', 'itemCode',"PG"], as_index=False)['qty'].sum()
                            merged_data = invoicedataframe_summed.merge(POInvoiceDatamerged,on=['projectGroup','itemCode','poNumber'], how='left')
                            missinglist = merged_data[merged_data['qty'] > merged_data['openQty']]
                            if len(missinglist):
                                missinglist = missinglist[['PG','poNumber','itemCode']]
                                pairs_not_found = list(missinglist.itertuples(index=False, name=None))
                                return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "The Overall Invoiced quantity exceeds the allowed limit for the combination of Project Group, PO Number, and item Code\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                }
                            )
                            new_df.replace({np.nan: None}, inplace=True)
                            dict_data = new_df.to_dict(orient="records")
                            # quantityDict = {}
                            # listError = []
                            # for index, i in enumerate(dict_data):
                            #     data = cagg.commonfuncforaddBulkUpload(i,index,quantityDict,listError)
                            # print(quantityDict,"quantityDict")
                            # if len(listError):
                            #     return respond({
                            #         'icon':"error",
                            #         "status":400,
                            #         'msg': "Some issues are found in this File:\n" + "\n".join(listError),
                            #     })
                            for i in dict_data:
                                cagg.commonfuncforaddBulkUploadwithnoError(i)
                            return respond ({
                                'msg':"Data Uploaded Successfully",
                                "status":200,
                                "icon":"success"
                            })
                        else:
                            return respond({
                                'status':400,
                                'ïcon':"error",
                                "msg":"There is No PO found in dataBase"
                            })
                          
                    else:
                        return respond({
                             'status':400,
                             "icon":"error",
                             "msg":"Site or project not found in database."
                        })   
                    #         if i["status"] == "Billed":
                    #             if i["invoiceDate"] == "":
                    #                 i["invoiceDate"] = None
                    #             updateSiteData = {
                    #                 "siteBillingStatus": "Billed",
                    #             }
                    #             updateSiteBy = {"_id": ObjectId(i["siteId"])}
                    #             updateMilestoneData = {
                    #                 "mileStoneStatus": "Close",
                    #                 "CC_Completion Date": df_current_date
                    #             }
                    #             updateMilestoneBy = {
                    #                 "Name": "Revenue Recognition",
                    #                 "siteId": ObjectId(i["siteId"]),
                    #             }
                    #             cmo.updating(
                    #                 "SiteEngineer",
                    #                 updateSiteBy,
                    #                 updateSiteData,
                    #                 False,
                    #             )
                    #             cmo.updating(
                    #                 "milestone",
                    #                 updateMilestoneBy,
                    #                 updateMilestoneData,
                    #                 False,
                    #             )
                    #         response = cmo.insertion("invoice", i)
                    #     else:
                    #         strData.append(data["msg"])

                    # if len(strData) > 0:
                    #     datams = {
                    #         "status": 400,
                    #         "icon": "error",
                    #         "msg": f"Total you have {str(len(dict_data))} row and error in {str(len(strData))} {', '.join(strData)}",
                    #         "data": [],
                    #     }
                    #     return respond(datams)
                    # else:
                    #     datams = {
                    #         "status": 200,
                    #         "icon": "info",
                    #         "msg": f"All row {str(len(dict_data))} submitted successfully",
                    #         "data": [],
                    #     }
                    #     return respond(datams)

                if fileType == "ItemCodeforWork":
                    
                    arra = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'projectuniqueId': {
                                    '$toString': '$_id'
                                }, 
                                'projectGroup': {
                                    '$toString': '$projectGroup'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("project",arra)['data']
                    projectdf = pd.DataFrame.from_dict(response)
                    
                    arra = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'Name': {
                                    '$in': [
                                        'MS1', 'MS2'
                                    ]
                                }, 
                                'CC_Completion Date': {
                                    '$ne': None
                                }
                            }
                        }, {
                            '$group': {
                                '_id': '$siteId', 
                                'systemId': {
                                    '$first': '$systemId'
                                }, 
                                'projectuniqueId': {
                                    '$first': '$projectuniqueId'
                                }, 
                                'data': {
                                    '$push': '$$ROOT'
                                }
                            }
                        }, {
                            '$sort': {
                                '_id': 1
                            }
                        }, {
                            '$addFields': {
                                'MS1': {
                                    '$arrayElemAt': [
                                        {
                                            '$filter': {
                                                'input': '$data', 
                                                'as': 'milestone', 
                                                'cond': {
                                                    '$eq': [
                                                        '$$milestone.Name', 'MS1'
                                                    ]
                                                }
                                            }
                                        }, 0
                                    ]
                                }, 
                                'MS2': {
                                    '$arrayElemAt': [
                                        {
                                            '$filter': {
                                                'input': '$data', 
                                                'as': 'milestone', 
                                                'cond': {
                                                    '$eq': [
                                                        '$$milestone.Name', 'MS2'
                                                    ]
                                                }
                                            }
                                        }, 0
                                    ]
                                }
                            }
                        }, {
                            '$addFields': {
                                'MS1': {
                                    '$toDate': '$MS1.CC_Completion Date'
                                }, 
                                'MS2': {
                                    '$toDate': '$MS2.CC_Completion Date'
                                }
                            }
                        }, {
                            '$addFields': {
                                'MS1': {
                                    '$dateToString': {
                                        'format': '%d-%m-%Y', 
                                        'timezone': 'Asia/Kolkata', 
                                        'date': '$MS1'
                                    }
                                }, 
                                'MS2': {
                                    '$dateToString': {
                                        'format': '%d-%m-%Y', 
                                        'timezone': 'Asia/Kolkata', 
                                        'date': '$MS2'
                                    }
                                }, 
                                'uid': {
                                    '$toString': '$_id'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0, 
                                'data': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("milestone",arra)['data']
                    milestonedf = pd.DataFrame.from_dict(response)
                    milestonedf = milestonedf.merge(projectdf, on="projectuniqueId",how='left')
                    mergedData = exceldata.merge(milestonedf, on="systemId",how='left')
                    cresult = mergedData[mergedData["uid"].isna()]
                    unique_c = cresult["systemId"].unique()

                    if len(unique_c) > 0:
                        return respond(
                            {
                                "status": 400,
                                "icon": "error",
                                "msg": "SSID Not Found  "
                                + ", ".join(unique_c)
                                + " . Kindly verify the SSID",
                            }
                        )
                    else:
                        exceldata['systemId'] = mergedData['uid']
                        exceldata['projectGroup'] = mergedData['projectGroup']
                        exceldata['MS1'] = mergedData['MS1']
                        exceldata['MS2'] = mergedData['MS2']
                        new_df = exceldata
                        dict_data = new_df.to_dict(orient="records")
                        for i in dict_data:
                            for k in range(7):
                                datewq = {
                                    
                                    "itemCode": (i["itemCode" + str(k + 1)] if i["itemCode" + str(k + 1)] else ""),
                                    "quantity": (i["quantity" + str(k + 1)] if i["quantity" + str(k + 1)] else None),
                                    "index": k + 1,
                                    "systemId_index": i["systemId"] + "_" + str(k + 1),
                                    "systemId": i["systemId"],
                                    "workdonebucket": i["workdonebucket"],
                                    "invoicebucket": i["invoicebucket"],
                                    "projectGroup": i["projectGroup"],
                                    "MS1": i["MS1"],
                                    "MS2": i["MS2"],
                                }
                                updateBy = {
                                    "systemId_index": i["systemId"] + "_" + str(k + 1)
                                }
                                cmo.updating("workDone", updateBy, datewq, True)
                        return respond(
                            {
                                "status": 200,
                                "msg": "Data Updated Successfully",
                                "icon": "success",
                            }
                        )
                        
                if fileType == "userProjectAllocation":
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'type': {
                                    '$ne': 'Partner'
                                },
                                "status":"Active"
                            }
                        }, {
                            '$project': {
                                'email': 1, 
                                'empId': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    empId = cmo.finding_aggregate("userRegister",dbData)
                    if len(empId['data']):
                        empIdDatadp = pd.DataFrame.from_dict(empId["data"])
                        empIdDatadpmerged = exceldata.merge(empIdDatadp, on=["email"], how="left")
                        cresult2 = empIdDatadpmerged[empIdDatadpmerged["empId"].isna()]
                        unique_c = cresult2["email"].unique()
                        if len(unique_c) > 0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "Emp Email Id Not Found in our Database or may be this user not Active:-  "
                                    + ", ".join(unique_c)
                                    + " . Kindly verify the Email Id",
                                }
                            )
                    else:
                        return respond({
                            "msg":"Emp not found in DataBase",
                            "icon":"error",
                            "status":400
                        }) 
                        
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'status': 'Active'
                            }
                        }, {
                            '$project': {
                                'projectId': 1, 
                                'projectuid': '$_id', 
                                '_id': 0
                            }
                        }
                    ]
                    projectid = cmo.finding_aggregate("project",dbData)
                    if len(projectid['data']):
                        projectidDatadp = pd.DataFrame.from_dict(projectid['data'])
                        result = empIdDatadpmerged.apply(my_function2,args =(projectidDatadp,), axis=1 )
                        listError = []
                        if isinstance(result, pd.DataFrame):
                            for index, row in result.iterrows():
                                if row['status'] != 200:
                                    listError.append(f"Rows Nu:-{index+2} {row['msg']}")
                            if len(listError):
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":"Project IDs not found in our Database:\n" + "\n".join(listError)
                                })
                        elif isinstance(result, pd.Series):
                            for index, row in enumerate(result):
                                if row['status'] != 200:
                                    listError.append(f"Rows Nu:-{index+2} {row['msg']}")
                            if len(listError):
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":"Project IDs not found in our Database:\n" + "\n".join(listError)
                                })
                            
                        new_df = result[['empId',"projectIds"]].copy()
                        dict_data = new_df.to_dict(orient="records")
                        for i in dict_data:
                            data = {
                                "projectIds":i['projectIds']
                            }
                            updateBy = {
                                'empId':i['empId']
                            }
                            response = cmo.updating("projectAllocation",updateBy,data,True)
                        return respond(response)
                    else:
                        return respond({
                            'status':400,
                            "msg":"Project Id Not found in Database",
                            "icon":"error"
                        })   
                        
                        
                if fileType == "partnerProjectAllocation":
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'type': "Partner",
                                "status":"Active"
                            }
                        }, {
                            '$project': {
                                'email': 1, 
                                'empId': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    empId = cmo.finding_aggregate("userRegister",dbData)
                    if len(empId['data']):
                        empIdDatadp = pd.DataFrame.from_dict(empId["data"])
                        empIdDatadpmerged = exceldata.merge(empIdDatadp, on=["email"], how="left")
                        cresult2 = empIdDatadpmerged[empIdDatadpmerged["empId"].isna()]
                        unique_c = cresult2["email"].unique()
                        if len(unique_c) > 0:
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "Partner Email Id Not Found in our Database or may be this user not active  :-  "
                                    + ", ".join(unique_c)
                                    + " . Kindly verify the Email Id",
                                }
                            )
                    else:
                        return respond({
                            "msg":"Partner not found in DataBase",
                            "icon":"error",
                            "status":400
                        }) 
                        
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'status': 'Active'
                            }
                        }, {
                            '$project': {
                                'projectId': 1, 
                                'projectuid': '$_id', 
                                '_id': 0
                            }
                        }
                    ]
                    projectid = cmo.finding_aggregate("project",dbData)
                    if len(projectid['data']):
                        projectidDatadp = pd.DataFrame.from_dict(projectid['data'])
                        result = empIdDatadpmerged.apply(my_function2,args =(projectidDatadp,), axis=1 )
                        listError = []
                        try:
                            for index, row in result.iterrows():
                                if row['status'] != 200:
                                    listError.append(f"Rows Nu:-{index} {row['msg']}")
                            if len(listError):
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg":"Project IDs not found in our Database:\n" + "\n".join(listError)
                                })
                        except:
                            return respond({
                                'status':400,
                                "msg":"Please check Project's In first Row",
                                "icon":"error"
                            })
                        else:
                            new_df = result[['empId',"projectIds"]].copy()
                            dict_data = new_df.to_dict(orient="records")
                            for i in dict_data:
                                i['type'] = "Partner"
                                updateBy = {
                                    'empId':i['empId']
                                }
                                response = cmo.updating("projectAllocation",updateBy,i,True)
                            return respond(response)
                    else:
                        return respond({
                            'status':400,
                            "msg":"Project Id Not found in Database",
                            "icon":"error"
                        })   
                
                if fileType == "EVMFinancial":
                    dbData = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
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
                                'as': 'Customer'
                            }
                        }, {
                            '$lookup': {
                                'from': 'projectGroup', 
                                'localField': '_id', 
                                'foreignField': 'costCenterId', 
                                'as': 'projectgroup'
                            }
                        }, {
                            '$addFields': {
                                'customer': {
                                    '$arrayElemAt': [
                                        '$Customer.customerName', 0
                                    ]
                                }, 
                                'projectgroupuid': {
                                    '$arrayElemAt': [
                                        '$projectgroup._id', 0
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'costCenter': 1, 
                                'customer': 1, 
                                'projectgroupuid': {
                                    '$toString': '$projectgroupuid'
                                }, 
                                'costCenteruid': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("costCenter",dbData)
                    df = pd.DataFrame.from_dict(response['data'])
                    mergedData = exceldata.merge(df, on=['customer',"costCenter"], how="left")
                    result = mergedData[mergedData['costCenteruid'].isna()]
                    unique_c = result[['customer',"costCenter"]].drop_duplicates()
                    if len(unique_c) > 0:
                            pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "The combination of Customer and Cost Center was not found in the database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                }
                            )
                    else:
                        exceldata['uniqueId'] = mergedData['costCenteruid']
                        exceldata['projectgroupuid'] = mergedData['projectgroupuid']
                        
                        if "customer" in exceldata:
                            del exceldata['customer']
                          
                        if "costCenter" in exceldata:
                            del exceldata['costCenter']
                                
                        invalid_years = exceldata.index[~exceldata['year'].apply(validate_year)]
                        if not  invalid_years.empty:
                            invalid_years.tolist()
                            invalid_years = [x+2 for x in invalid_years ]
                            return respond({
                                'status':400,
                                "icon":"warning",
                                "msg":f'These rows do not contain the correct year - {invalid_years}'
                            })
                        
                        numeric_columns = [
                            'pv-1', 'pv-2','pv-3',
                            'pv-4','pv-5','pv-6',
                            'pv-7','pv-8','pv-9',
                            'pv-10','pv-11','pv-12'
                        ]
                        invalid_indices = []
                        for column in numeric_columns:
                            invalid_entries = exceldata[~exceldata[column].apply(is_valid_number_or_empty)]
                            invalid_indices.extend(invalid_entries.index.tolist())
                        invalid_indices = list(set(invalid_indices))
                        if invalid_indices:
                            invalid_indices = [x+2 for x in invalid_indices ]
                            return respond({
                                'status':400,
                                "msg":f'These Rows Do not contain correct value of PV Target - {invalid_indices}',
                                "icon":"error",
                            })
                        exceldata.replace({"": None}, inplace=True)
                        dict_data = exceldata.to_dict(orient="records")
                        for i in dict_data:
                            updateBy = {
                                'year':int(i['year']),
                                "uniqueId":i['uniqueId']
                            }
                            cmo.updating("earnValue",updateBy,i,True)
                        return respond({
                            'status':200,
                            "msg":'Data Updated Successfully',
                            'icon':'success'
                        })
                            
                            
                if fileType == "EVMDelivery":
            
                    invalid_years = exceldata.index[~exceldata['year'].apply(validate_year)]
                    if not  invalid_years.empty:
                        invalid_years.tolist()
                        invalid_years = [x+2 for x in invalid_years ]
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":f'These rows do not contain the correct year - {invalid_years}'
                        })

                    invalid_months = exceldata.index[~exceldata['month'].apply(validate_month)]
                    if not  invalid_months.empty:
                        invalid_months.tolist()
                        invalid_months = [x+2 for x in invalid_months ]
                        return respond({
                            'status':400,
                            "icon":"warning",
                            "msg": f'These rows do not contain the correct Month Number - {", ".join(map(str, invalid_months))}'
                        })
                    
                    columns = [col for col in exceldata.columns if col not in ['customer', 'year', 'month', 'circle']]
                    subprojectArray = []
                    sub_project_mapping ={}
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
                                'as': 'result'
                            }
                        }, {
                            '$unwind': {
                                'path': '$result', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$project': {
                                'customerId': {
                                    '$toString': '$customer'
                                }, 
                                'subproject': {
                                    '$concat': [
                                        '$result.subProject', '(', '$projectType', ')'
                                    ]
                                }, 
                                'subProjectId': {
                                    '$toString': '$result._id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("masterSubProject",arra)['data']
                    if len(response):
                        for i in response:
                            subprojectArray.append(i['subproject'])
                            sub_project_mapping[i['subproject']] = i['subProjectId']
                        all_present = all(item in columns for item in subprojectArray)
                        if all_present:
                            extra_elements = list(set(columns) - set(subprojectArray))
                            if len(extra_elements):
                                return respond({
                                    'status':400,
                                    "icon":"error",
                                    "msg": f'These headers are found as extra in the template. Please remove them: {", ".join(extra_elements)}.'
                                })
                            numeric_columns = subprojectArray
                            invalid_indices = []
                            string_columns = ["Circle", "Customer"]

                            
                            for column in string_columns:
                                if column in exceldata.columns:
                                    exceldata[column] = exceldata[column].astype(str).str.strip()

                            
                            for column in exceldata.columns:
                                if column not in string_columns:  
                                    exceldata[column] = exceldata[column].apply(
                                        lambda x: int(str(x).replace(" ", "")) if isinstance(x, str) and str(x).strip().isdigit() else x
                                    )
                                
                            for column in exceldata.columns:
                                invalid_entries = exceldata[~exceldata[column].apply(is_valid_number)]
                                if not invalid_entries.empty:
                                    for index, row in invalid_entries.iterrows():
                                        print(f"Invalid entry in column '{column}' at row {index+2}")
                            for column in numeric_columns:
                                print(column,'columncolumncolumn')
                                invalid_entries = exceldata[~exceldata[column].apply(is_valid_number)]
                                print(invalid_entries,'invalid_entriesinvalid_entriesinvalid_entriesinvalid_entries')
                                invalid_indices.extend(invalid_entries.index.tolist())
                            invalid_indices = list(set(invalid_indices))
                            if invalid_indices:
                                invalid_indices = [x+2 for x in invalid_indices ]
                                return respond({
                                    'status':400,
                                    "msg":f'These Rows Do not contain correct value of Sub Project Target - {invalid_indices}',
                                    "icon":"error",
                                })
                            dbData = [
                                {
                                    '$match': {
                                        'customer': ObjectId(id)
                                    }
                                }, {
                                    '$lookup': {
                                        'from': 'customer', 
                                        'localField': 'customer', 
                                        'foreignField': '_id', 
                                        'pipeline':[
                                            {
                                                '$match':{
                                                    'deleteStatus':{"$ne":1}
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
                                        'customer': '$result.customerName', 
                                        'customerId': {
                                            '$toString': '$customer'
                                        }, 
                                        'circle': '$circleCode', 
                                        'circleId': {
                                            '$toString': '$_id'
                                        }, 
                                        '_id': 0
                                    }
                                }
                            ]
                            circleData = cmo.finding_aggregate("circle",dbData)
                            circleDf = pd.DataFrame.from_dict(circleData['data'])
                            mergedData = exceldata.merge(circleDf, on=['customer','circle'], how="left")
                            result = mergedData[mergedData['circleId'].isna()]
                            unique_c = result[['customer',"circle"]].drop_duplicates()
                            if len(unique_c) > 0:
                                pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": "The combination of Customer and Circle was not found in the database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                    }
                                )
                            else:
                                exceldata['customerId'] = mergedData['customerId']
                                exceldata['circleId'] = mergedData['circleId']
                                if "customer" in exceldata:
                                    del exceldata['customer']
                                if "circle" in exceldata:
                                    del exceldata['circle']
                                
                                def create_sub_projects(row):
                                    sub_projects = [
                                        {"subProjectId": sub_project_mapping[col], "subProjectName": col, "value": row[col]}
                                        for col in row.index if col not in ["year", "month","customerId","circleId"]
                                    ]
                                    return sub_projects

                                exceldata["subProjects"] = exceldata.apply(create_sub_projects, axis=1)
                                dict_data = exceldata.to_dict(orient="records")
                                for i in dict_data:
                                    updateBy = {
                                        'year':i['year'],
                                        "month":i['month'],
                                        "customerId":ObjectId(i['customerId']),
                                        "circleId":ObjectId(i['circleId']),
                                        "MSType":MSType
                                    }
                                    updateData = {
                                        'year':i['year'],
                                        "month":i['month'],
                                        "customerId":ObjectId(i['customerId']),
                                        "circleId":ObjectId(i['circleId']),
                                        "MSType":MSType,
                                        "subProjects":i['subProjects']
                                    }
                                    cmo.updating("deliveryPVA",updateBy,updateData,True)
                                return respond({
                                    'status':200,
                                    "icon":"success",
                                    "msg":"Data Updated Successfully"
                                })
                                    
                        else:
                            return respond({
                                'status':400,
                                "icon":"error",
                                "msg": "Please check the template. Some sub-projects selected in the Sub project Master Table are not found in the header."
                            })
                    else:
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":"Please select the Sub Project list In Sub project Master Table"
                        })
                    
                            
                if fileType == "profitloss":
                    
                    invalid_years = exceldata.index[~exceldata['year'].apply(validate_year)]
                    if not  invalid_years.empty:
                        invalid_years.tolist()
                        invalid_years = [x+2 for x in invalid_years ]
                        return respond({
                            'status':400,
                            "icon":"warning",
                            "msg":f'These rows do not contain the correct Year - {invalid_years}'
                        })
                        
                    invalid_months = exceldata.index[~exceldata['month'].apply(validate_month)]
                    if not  invalid_months.empty:
                        invalid_months.tolist()
                        invalid_months = [x+2 for x in invalid_months ]
                        return respond({
                            'status':400,
                            "icon":"warning",
                            "msg":f'These rows do not contain the correct Month - {invalid_months},Please Write month name like this January, February,March ...'
                        })
                            
                    duplicates = exceldata[exceldata.duplicated(['year', 'month', 'costCenter'], keep=False)].index.tolist()
                    if (duplicates):
                        duplicates = [x + 2 for x in duplicates]
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":f"These Rows have duplicate Year, Month, Cost Center:- {duplicates}"
                        })
                    
                    arra = [
                        {
                            '$project': {
                                'costCenter': 1, 
                                'uid': {
                                    '$toString': '$_id'
                                }, 
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("costCenter",arra)['data']
                    costCenterdf = pd.DataFrame.from_dict(response)
                    mergedData = exceldata.merge(costCenterdf, on=['costCenter'],how='left')
                    result = mergedData[mergedData['uid'].isna()]
                    unique_c = result[['costCenter']].drop_duplicates()
                    if len(unique_c) > 0:
                            pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                            return respond(
                                {
                                    "status": 400,
                                    "icon": "error",
                                    "msg": "The Cost center was not found in the Database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                }
                            )
                    else:
                        exceldata['costCenter'] = mergedData['uid']
                        projectedRevenue = []
                        actualRevenue = []
                        projectedCost = []
                        actualCost = []
                        sgna = []
                        for index,row in exceldata.iterrows():
                            if pd.isna(row['projectedRevenue']) or row['projectedRevenue'] == "":
                                exceldata.at[index, 'projectedRevenue'] = None
                            else:
                                try:
                                    numeric_value = pd.to_numeric(row['projectedRevenue'])
                                    if numeric_value > 0 :
                                        pass 
                                    else:
                                        projectedRevenue.append(index+2)
                                except ValueError:
                                    projectedRevenue.append(index+2)
                        if (len(projectedRevenue)>0):
                            return respond({
                                'msg':f"These Rows do not have Positive Numbers for 'Projected Revenue' :-{projectedRevenue}",
                                "icon":"error",
                                "status":400
                                
                            })
                            
                        for index,row in exceldata.iterrows():
                            if pd.isna(row['actualRevenue']) or row['actualRevenue'] == "":
                                exceldata.at[index, 'actualRevenue'] = None
                            else:
                                try:
                                    numeric_value = pd.to_numeric(row['actualRevenue'])
                                    if numeric_value > 0:
                                        pass 
                                    else:
                                        actualRevenue.append(index+2)
                                except ValueError:
                                    actualRevenue.append(index+2)
                        if (len(actualRevenue)>0):
                            return respond({
                                'msg':f"These Rows do not have Positive Numbers for 'Actual Revenue' :-{actualRevenue}",
                                "icon":"error",
                                "status":400
                                
                            })
                            
                        for index,row in exceldata.iterrows():
                            if pd.isna(row['projectedCost']) or row['projectedCost'] == "":
                                exceldata.at[index, 'projectedCost'] = None
                            else:
                                try:
                                    numeric_value = pd.to_numeric(row['projectedCost'])
                                    if numeric_value > 0:
                                        pass 
                                    else:
                                        projectedCost.append(index+2)
                                except ValueError:
                                    projectedCost.append(index+2)
                        if (len(projectedCost)>0):
                            return respond({
                                'msg':f"These Rows do not have Positive Numbers for 'Projected Cost' :-{projectedCost}",
                                "icon":"error",
                                "status":400
                                
                            })
                        for index,row in exceldata.iterrows():
                            if pd.isna(row['actualCost']) or row['actualCost'] == "":
                                exceldata.at[index, 'actualCost'] = None
                            else:
                                try:
                                    numeric_value = pd.to_numeric(row['actualCost'])
                                    if numeric_value > 0:
                                        pass 
                                    else:
                                        actualCost.append(index+2)
                                except ValueError:
                                    actualCost.append(index+2)
                        if (len(actualCost)>0):
                            return respond({
                                'msg':f"These Rows do not have Positive Numbers for 'Actual Cost' :-{actualCost}",
                                "icon":"error",
                                "status":400
                                
                            })
                            
                        for index,row in exceldata.iterrows():
                            if pd.isna(row['sgna']) or row['sgna'] == "":
                                exceldata.at[index, 'sgna'] = None
                            else:
                                try:
                                    numeric_value = pd.to_numeric(row['sgna'])
                                    if numeric_value > 0:
                                        pass 
                                    else:
                                        sgna.append(index+2)
                                except ValueError:
                                    sgna.append(index+2)
                        if (len(sgna)>0):
                            return respond({
                                'msg':f"These Rows do not have Positive Numbers  for 'SGNA COST' :-{sgna}",
                                "icon":"error",
                                "status":400
                                
                            })
                    dict_data = exceldata.to_dict(orient="records")
                    for i in dict_data:
                        i['costCenter'] = ObjectId(i['costCenter'])
                        updateBy = {
                            'year':i['year'],
                            "month":i['month'],
                            "costCenter":i['costCenter']
                        }
                        response = cmo.updating("profitloss",updateBy,i,True)
                    return respond({
                        'status':200,
                        "icon":'success',
                        "msg":"Data updated Successfully"
                    })
                    

                if fileType == "PoUpgrade":  
                    duplicates = exceldata[exceldata.duplicated(['projectGroup', 'poNumber',"itemCode"], keep=False)].index.tolist()
                    if (duplicates):
                        duplicates = [x + 2 for x in duplicates]
                        return respond({
                            'status':400,
                            "icon":"error",
                            "msg":f"These Rows have duplicate Project Group,PO Number and Item Code:- {duplicates}"
                        })
                    
                    invalidStatus = exceldata[~exceldata['poStatus'].isin(['Closed', 'Short Closed'])].index.tolist()
                    if (len(invalidStatus)):
                        return respond({
                            "status":400,
                            "icon":"error",
                            "msg":'The "PO Status" column contains only the values "Closed" or "Short Closed".'
                        })
                    
                    exceldata['poNumber'] = exceldata['poNumber'].astype(str)
                    exceldata['itemCode'] = exceldata['itemCode'].astype(str)
                    
                    projectGroupdf = projectGroup()
                    
                    
                    
                    if "Customer" in projectGroupdf.columns:
                        del projectGroupdf['Customer']
                        
                    projectGroupdf = projectGroupdf.rename(columns={
                        'projectGroupName': 'projectGroup',
                        'projectGroup': 'projectGroupUid'
                    })
                    arra = [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$project': {
                                'projectGroupUid': '$projectGroup', 
                                'poNumber': 1, 
                                'itemCode': 1, 
                                'uid':{
                                    '$toString':'$_id'
                                },
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("PoInvoice",arra)['data']
                    df = pd.DataFrame.from_dict(response)
                    if not df.empty:
                        df = df.merge(projectGroupdf,on='projectGroupUid',how='left')
                        mergedData = exceldata.merge(df,on=['projectGroup','poNumber','itemCode'],how='left')
                        result = mergedData[mergedData['uid'].isna()]
                        unique_c = result[['projectGroup','poNumber','itemCode']].drop_duplicates()
                        if len(unique_c) > 0:
                                pairs_not_found = list(unique_c.itertuples(index=False, name=None))
                                return respond(
                                    {
                                        "status": 400,
                                        "icon": "error",
                                        "msg": "The pair of Project Group, PO Number And Item Code was not found in the Database.\n" + "\n".join(", ".join(map(str, row)) for row in pairs_not_found),
                                    }
                                )
                        else:
                            exceldata['uid'] = mergedData['uid']
                            dict_data = exceldata.to_dict(orient="records")
                            for i in dict_data:
                                updateBy = {
                                    '_id':ObjectId(i['uid'])
                                }
                                updateData = {
                                    'poStatus':i['poStatus'],
                                    'itemCodeStatus':'Closed'
                                }
                                cmo.updating("PoInvoice",updateBy,updateData,False)
                            return respond({
                                'status':200,
                                "msg":"Data Updated Successfully",
                                "icon":"success"
                            })
                    return respond({
                        'icon':'error',
                        "msg":"Active PO not Found in Database",
                        "status":400
                    })
                    
                    
                
              
                    
            else:
                return respond(data)


######################################################################################################################


@common_blueprint.route("/templateUploadFile/<id>", methods=["GET", "POST"])
@token_required
def templateupload(current_user,id=None):
    if request.method == "POST":
        upload = request.files.get("uploadedFile[]")
        fileType = request.form.get("fileType")
        allData = {}
        supportFile = ["xlsx", "xls"]
        pathing = cform.singleFileSaver(upload, "", supportFile)
        if pathing["status"] == 200:
            allData["pathing"] = pathing["msg"]
        elif pathing["status"] == 422:
            return respond(pathing)
        fileCheck = {
            "Site Engg": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Tracking": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Issues": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Financials": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "MileStone": {
                "validate": cdcm.validate_Upload_Milestone,
                "rename": cdcm.rename_Upload_Milestone,
            },
            "Commercial": {
                "validate": cdcm.validate_Upload_Commercial,
                "rename": cdcm.rename_Upload_Commercial,
            },
            "Template": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Planning Details": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Site Details": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Checklist": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Snap": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
            "Acceptance Log": {
                "validate": cdcm.validate_Upload_Template,
                "rename": cdcm.rename_Upload_Template,
            },
        }
        if fileType in fileCheck:
            excel_file_path = os.path.join(os.getcwd(), allData["pathing"])
            validate = fileCheck[fileType]["validate"]
            rename = fileCheck[fileType]["rename"]
            if fileType in ["Site Engg", "Financials", "Issues", "Tracking","Template","Planning Details","Site Details","RAN AT Checklist","Snap","Acceptance Log"]:
                valGames = [["Mandatory(Y/N)", ["Yes", "No"]],["InputType",["Text","Number","Decimal","Date","Dropdown","Auto Created"]],["Status", ["Active", "Inactive"]]]
            else:
                valGames = []
            data = cfc.exceltodf(excel_file_path, rename, validate, valGames)
            if data["status"] == 200:
                data["data"] = cfc.dfjson(data["data"])
                updateBy = {"_id": ObjectId(id),"deleteStatus": {"$ne": 1}}
                if fileType in ["Site Engg", "Financials", "Issues", "Tracking","Template","Planning Details","Site Details","Checklist","Snap","Acceptance Log"]:
                    for index,datew in enumerate(data["data"]):
                        if (datew["dataType"] == "Dropdown" or datew["dataType"] == "Auto Created"):
                            datew["childView"] = True
                            datew['index'] = index+1
                        else:
                            datew["childView"] = False
                            datew['index'] = index+1
                    
                    
                    setAllMsg=[]
                    def checkData(data):
                        if isinstance(data, list):
                            return [checkData(item) for item in data]

                        elif isinstance(data, dict):
                            if data.get("dataType","") not in  ["Dropdown","Auto Created"] and "dropdownValue" in data:
                                del data['dropdownValue']
                            for key, value in data.items():
                                if data.get("dataType","") in ["Dropdown","Auto Created"] and key == "dropdownValue" and isinstance(value, str):
                                    value= [(i+"*#@"+key+"*#@"+f'{data.get("index","")}') for i in value.split(",")]
                                    value = checkData(value)
                                    data[key] = ",".join(value)
                                else:
                                    if isinstance(value,str):
                                        value=value+"*#@"+key+"*#@"+f'{data.get("index","")}'
                                    data[key] = checkData(value)
                            return data

                        elif isinstance(data, str):
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
                    
                    data["data"] = checkData(data["data"])
                    if len(setAllMsg):
                        return respond({"status": 400, "msg": ", ".join(list(set(setAllMsg))), "icon": "error"})
                    
                    
                    if fileType == "Site Engg":
                        siteId = False
                        prAll_rfi = False
                        for datew in data["data"]:
                            if datew["fieldName"] == "Site Id":
                                siteId = True
                            if (datew["fieldName"] == "RFAI Date" or datew["fieldName"] == "Allocation Date"):
                                prAll_rfi = True
                        if siteId and prAll_rfi:
                            tempData = {"t_sengg": data["data"]}
                            response = cmo.updating("projectType", updateBy, tempData, False)
                            return respond(response)
                        else:
                            finData = {"status": 422, "msg": ""}
                            if siteId == False:
                                finData["msg"] = (
                                    'The "Site Id" field does not exist. Please create it.'
                                )
                            elif prAll_rfi == False:
                                finData["msg"] = (
                                    'The fields for "Allocation Date" or "RFAI Date" do not exist. Please create at least one of these fields.'
                                )
                            return respond(finData)
                        
                    elif fileType == "Tracking":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"t_tracking": data["data"]}
                        response = cmo.updating("projectType", updateBy, tempData, False)
                        return respond(response)
                        
                    elif fileType == "Issues":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"t_issues": data["data"]}
                        response = cmo.updating("projectType", updateBy, tempData, False)
                        return respond(response)
                        
                    elif fileType == "Financials":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"t_sFinancials": data["data"]}
                        response = cmo.updating("projectType", updateBy, tempData, False)
                        return respond(response)
                    
                    elif fileType == "Template":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"Template": data["data"]}
                        response = cmo.updating("complianceForm", updateBy, tempData, False)
                        return respond(response)
                    
                    elif fileType == "Planning Details":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"planDetails": data["data"]}
                        response = cmo.updating("complianceForm", updateBy, tempData, False)
                        return respond(response)
                    
                    elif fileType == "Site Details":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"siteDetails": data["data"]}
                        response = cmo.updating("complianceForm", updateBy, tempData, False)
                        return respond(response)
                    
                    elif fileType == "Checklist":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"ranChecklist": data["data"]}
                        response = cmo.updating("complianceForm", updateBy, tempData, False)
                        return respond(response)
                    
                    elif fileType == "Snap":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"snap": data["data"]}
                        response = cmo.updating("complianceForm", updateBy, tempData, False)
                        return respond(response)
                    
                    elif fileType == "Acceptance Log":
                        for i in range(len(data["data"])):
                            data["data"][i]["index"] = i + 1
                        tempData = {"acceptanceLog": data["data"]}
                        response = cmo.updating("complianceForm", updateBy, tempData, False)
                        return respond(response)
                        
                    
                
                elif fileType == "MileStone":
                    for i in range(len(data["data"])):
                        data["data"][i]["index"] = i + 1
                    tempData = {"MileStone": data["data"]}
                    response = cmo.updating("projectType", updateBy, tempData, False)
                    
                elif fileType == "Commercial":
                    for i in range(len(data["data"])):
                        data["data"][i]["index"] = i + 1
                    tempData = {"Commercial": data["data"]}
                    response = cmo.updating("projectType", updateBy, tempData, False)
                    
                
                return respond(response)
            else:
                response = {"status": 400, "icon": "error", "msg": data["msg"]}
                return respond(response)
        else:
            response = {"status": 400, "icon": "error", "msg": "File Type not found"}
            return respond(response)