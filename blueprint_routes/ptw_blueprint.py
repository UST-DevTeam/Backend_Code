from base import *
from common.config import get_current_date_timestamp as get_current_date_timestamp
import common.excel_write as excelWriteFunc
from common.config import changedThings3 as changedThings3
from common.config import unique_ptwtimestamp, datestamp,unique_8hoursPlusTimestamp
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from common.exportExcelAndPdfWithDynamicKeys import export_to_pdf,export_to_excel
from common.commonDateFilter import dateFilter
ptw_blueprint = Blueprint('ptw_blueprint', __name__)



def generatePtwNumber(ptwResponse, last_doc):
    circle = ptwResponse["circle"]
    siteId = ptwResponse["siteId"]
    date = datestamp()

    if last_doc is None:
        new_serial = "00001"
    else:
        last_ptw_number = last_doc.get("ptwNumber")
        last_serial = int(last_ptw_number.split("/")[-1])
        new_serial = str(last_serial + 1).zfill(5)
    ptwNumber = f"{circle}/{siteId}/{date}/{new_serial}"
    return ptwNumber

def current_time():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time

def current_time_plus_8hrs():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    future_time = current_time + timedelta(hours=8)
    formatted_time = future_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


def ptwAdminsLogs(fileType,subform, formId, userUniqueId, method):
    if method=="PATCH":
        method="UPDATED"
    elif method=="POST":
        method="Added"
    Added =fileType +" "+ subform + " Form "+ method
    cmo.insertion("ptwAdminLogs", {'type': method, 'module': 'PTW Project Type', 'formId':formId,'actionAt': unique_ptwtimestamp(), 'actionBy': ObjectId(userUniqueId), 'action': Added})

def ptwRaiseTicketLogs(fileType=None,subform=None, mileStoneId=None, userUniqueId=None, method=None, allData=None):
    if method=="PATCH":
        method="UPDATED"
    elif method=="POST":
        method="Added"
    Added=""
    
    if subform == 'Closed' or subform == 'Auto Closed':
        Added =fileType +" "+ " PTW "+ subform

    elif fileType=="Rejection":
        Added =subform + " "+ fileType
    elif subform=="L1-Approver" :
        Added =subform + " "+ "Selected" 
    elif subform == "L2-Approver":
        Added =subform + " "+ "Selected" +" and "+ "L1-approved" 
    elif subform==None:
        Added ="L2-Approved"
    else:
        Added =fileType +" "+ subform + " Form "+ method
    cmo.insertion("ptwRaiseTicketLogs", {'actionType': method, 'module': 'PTW Raise Ticket', 'mileStoneId':mileStoneId,'actionAt': unique_ptwtimestamp(), 'actionBy': ObjectId(userUniqueId), 'action': Added, "payload":allData})

def updateProjectLog(fileType=None,subform=None, mileStoneId=None, userUniqueId=None, method=None, allData=None, siteId=None,projectId=None):
    if method=="PATCH":
        method="UPDATED"
    elif method=="POST":
        method="Added"
    Added=""
    if subform == 'Closed' or subform == 'Auto Closed':
        Added =fileType +" "+ " PTW "+ subform
    if fileType=="Rejection":
        Added =subform + " "+ fileType
    elif subform=="L1-Approver" :
        Added =subform + " "+ "Selected"
    elif subform == "L2-Approver":
        Added =subform + " "+ "Selected" +" and "+ "L1-approved" 
    elif subform==None:
        Added ="L2-Approved"
    else:
        Added =fileType +" "+ subform + " Form "+ method
    cmo.insertion("projectEventlog", {'actionType': method, 'module': 'PTW Raise Ticket', 'milestoneuid':mileStoneId,'siteuid':siteId,'projectuid':projectId,'UpdatedAt': current_time(), 'UpdatedBy': userUniqueId, 'updatedData': Added, "payload":allData})

def validate_fields(allData):
    names = set()

    for item in allData:
        field_name = str(item.get("fieldName", "")).strip()
        field_name_lower = field_name.lower()

        if field_name_lower in names:
            return f"Duplicate fieldName found: {field_name}"
        names.add(field_name_lower)

        data_type = str(item.get("dataType", "")).strip().lower()
        if data_type == "dropdown":
            raw_values = item.get("dropdownValue", "")
            
            if isinstance(raw_values, str):
                dropdown_values = [val.strip() for val in raw_values.split(",") if val.strip()]
            elif isinstance(raw_values, list):
                dropdown_values = [str(val).strip() for val in raw_values if str(val).strip()]
            else:
                dropdown_values = []

            if not dropdown_values:
                return f"Dropdown values missing for: {field_name}"

            seen_values = set()
            for val in dropdown_values:
                val_lower = val.lower()
                if val_lower in seen_values:
                    return f"Duplicate dropdown value '{val}' in: {field_name}"
                seen_values.add(val_lower)

    return True

def importfile(file):
            if True:    
                if not file:
                    return respond({
                            'show':True,
                            'status': 400,
                            'text': "No file uploaded",
                            'icon': 'error'
                        })
                    
                supportFile = ["xlsx", "xls"]
                allData = {}
                pathing = cform.singleFileSaver(file, "", supportFile)
                if pathing["status"] == 200:
                    allData["pathing"] = pathing["msg"]
                elif pathing["status"] == 422:
                    return respond(pathing)
                fileType = 'ptw'
                fileCheck =  {
                    "ptw": {
                        "validate": cdcm.validate_Upload_Template,
                        "rename": cdcm.rename_Upload_Template,
                    },
                }
                if fileType in fileCheck:
                    excel_file_path = os.path.join(os.getcwd(), allData["pathing"])
                    validate = fileCheck[fileType]["validate"]
                    rename = fileCheck[fileType]["rename"]
                    if fileType in ['ptw']:
                        valGames = [["Mandatory(Y/N)", ["Yes", "No"]],["InputType",["Text","Number","Decimal","Date","Dropdown","Auto Created","AutoFill","Selfie","DateTime","img"]],["Status", ["Active", "Inactive"]]]
                    else:
                        valGames = []
                    data = cfc.exceltodf(excel_file_path, rename, validate, valGames)
                    if data["status"] == 200:
                        data["data"] = cfc.dfjson(data["data"])
                        if fileType in ['ptw']:
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
                                            # value= [(i+"#@"+key+"#@"+f'{data.get("index","")}') for i in value.split(",")]
                                            value= [(i) for i in value.split(",")]
                                            value = checkData(value)
                                            data[key] = ",".join(value)
                                            
                                        else:
                                            if isinstance(value,str):
                                                # value=value+"#@"+key+"#@"+f'{data.get("index","")}'
                                                value=value  
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
                            
                            data = checkData(data["data"])
                            
                            if len(setAllMsg):
                                return {"status": 400, "msg": ", ".join(list(set(setAllMsg))), "icon": "error"}
                            
                    else:
                        response = {"status": 400, "icon": "error", "msg": data["msg"]}
                        return response
                else:
                    response = {"status": 400, "icon": "error", "msg": "File Type not found"}
                    return response

            return data
     
@ptw_blueprint.route("/admin/ptw/<fileType>", methods=['GET']) 
@ptw_blueprint.route("/admin/ptw/<fileType>/<subform>", methods=['GET', "POST"]) 
@ptw_blueprint.route("/admin/ptw/<fileType>/<subform>/<rowId>", methods=['GET', "POST", "PATCH", "DELETE"])
@token_required
def manageRtwsProject(current_user,fileType=None, subform=None, rowId = None):
    
    if(current_user["roleName"]=="Admin"):
        if request.method == "GET":

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
                        }
                    ]
            elif (fileType !=None ):
                arra = [
                        {
                            '$match': {
                                'fileType':fileType
                            }
                        },
                        {
                            '$addFields': {
                                '_id': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    ]

            response = cmo.finding_aggregate("ptwProjectType", arra)
            
            return respond(response)
        
        elif request.method == "POST":
            allData = None
            if request.args.get("file") == "true":
                upload = request.files.get("uploadedFile[]")
                allData = importfile(upload)
                
                validation = validate_fields(allData)
                if validation != True:
                    return respond({
                            'show':True,
                            'status': 400,
                            'text': validation,
                            'icon': 'error'
                        })
                createdAt = int(unique_ptwtimestamp())
            
                if rowId is None:
                    arra = [
                        {
                            '$match': {
                                'fileType': fileType
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("ptwProjectType", arra)
                    if len(response["data"])!=0:

                        newData = response["data"][0].get(subform, [])
                        if not isinstance(newData, list):
                            newData = []

                        length = len(newData)
                        for data in allData:
                            data["index"]=int(length)+1
                        existing_fields = set(item['fieldName'].lower() for item in newData)

                        new_Data = [item for item in allData if item['fieldName'].lower() not in existing_fields]

                        if len(new_Data)!=0:
                            newData.extend(new_Data)

                        allData = {
                            subform:newData
                        }
                       
                        updateBy = {
                            'fileType': fileType
                        }
                        updateresponse = cmo.updating("ptwProjectType", updateBy, allData, False)
                        ptwAdminsLogs(fileType,subform,response["data"][0]["_id"], current_user['userUniqueId'], request.method)
                        return respond(updateresponse)
                    else:
                
                        filteredData = {}

                        filteredData.update({
                            "fileType": fileType,
                            subform:allData,
                            "creatorId": current_user['userUniqueId'],
                            "createdAt": createdAt  
                        })
                        
                        response = cmo.insertion("ptwProjectType", filteredData)

                        ptwAdminsLogs(fileType,subform, response["operation_id"], current_user['userUniqueId'], request.method)
                        return respond(response)
            else:
                allData = request.get_json()   
            
                validation = validate_fields(allData)
                if validation != True:
                    return respond({
                            'show':True,
                            'status': 400,
                            'text': validation,
                            'icon': 'error'
                        })
                createdAt = int(unique_ptwtimestamp())
                
                if rowId is None:
                    arra = [
                        {
                            '$match': {
                                'fileType': fileType
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("ptwProjectType", arra)
                    if len(response['data']):
                        return {
                            "status": 400,
                            "msg": "This Form is Already Exist",
                            "icon": "error",
                        }, 400
                
                    filteredData = {}

                    filteredData.update({
                        "fileType": fileType,
                        subform:allData,
                        "creatorId": current_user['userUniqueId'],
                        "createdAt": createdAt  
                    })
                    
                    response = cmo.insertion("ptwProjectType", filteredData)

                    ptwAdminsLogs(fileType,subform, response["operation_id"], current_user['userUniqueId'], request.method)
                    return respond(response)

                else:
                    return respond({
                            'show':True,
                            'status': 400,
                            'text': 'Something Went Wrong',
                            'icon': 'error'
                        })
        
        elif request.method == "PATCH":
           
            if rowId != None:
                allData = request.get_json()
                updateBy = {
                    'fileType': fileType
                }
           
                if ("checklist"==subform):

                    validation = validate_fields(allData)
                    if validation != True:
                        return respond({
                            'show':True,
                            'status': 400,
                            'text': validation,
                            'icon': 'error'
                        })
                    auto_created = True
                    drop_down = True
                    # for datew in allData:
                    #     for key, value in datew.items():
                    #         if key in ["fieldName", "dropdownValue"]:
                               
                    #             datew[key] = value.strip()

                    #     try:
                    #         if (datew["dataType"] == "Auto Created" and datew["dropdownValue"] == ''):
                    #             auto_created = False
                    #     except:
                    #         auto_created = False
                    #     try:
                    #         if (datew["dataType"] == "Dropdown" and datew["dropdownValue"] == ''):
                    #             drop_down = False
                    #     except:
                    #         drop_down = False
                    for datew in allData:
                        
                        for key, value in list(datew.items()):
                            if isinstance(value, str):
                                datew[key] = value.strip()

                        if datew.get("dataType") == "Auto Created" and not datew.get("dropdownValue", ""):
                            auto_created = False

                        if datew.get("dataType") == "Dropdown" and not datew.get("dropdownValue", ""):
                            drop_down = False

                        if datew.get("dataType") not in ["Dropdown", "Auto Created"]:
                            datew.pop("dropdownValue", None)
                    if(auto_created and drop_down):
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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
                    
                elif("photo" ==subform):

                    validation = validate_fields(allData)
                    if validation != True:
                        return respond({
                            'show':True,
                            'status': 400,
                            'text': validation,
                            'icon': 'error'
                        })
                    for datew in allData:
                        
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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
                    
                elif subform == "roadsafetychecklist4wheeler" and fileType == "drivetestactivity":
                    auto_created = True
                    drop_down = True
                    for datew in allData:
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
                        
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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

                elif subform == "ptwphoto4wheeler" and fileType == "drivetestactivity":
                    auto_created = True
                    drop_down = True
                    for datew in allData:
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
                        
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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
                                
                elif subform == "roadsafetychecklist2wheeler" and fileType == "drivetestactivity":
                    auto_created = True
                    drop_down = True
                    for datew in allData:
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
                        
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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

                elif subform == "ptwphoto2wheeler" and fileType == "drivetestactivity":
                    auto_created = True
                    drop_down = True
                    for datew in allData:
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
                        
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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
                      
                elif("riskassessment" ==subform):
                    auto_created = True
                    drop_down = True
                    for datew in allData:
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

                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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

                elif("rejectionreason" ==subform):
                    auto_created = True
                    drop_down = True
                    for datew in allData:
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
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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

                elif("ptwphoto"== subform):

                    auto_created = True
                    drop_down = True
                    for datew in allData:
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
                        
                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                    
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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

                elif("teamdetails"==subform):
                    validation = validate_fields(allData)
                    if validation != True:
                        return {
                            'status': 400,
                            "icon": "error",
                            "msg": validation
                        },400
                    
                    for datew in allData:

                        allData = {
                            subform:allData
                        }
                        response = cmo.updating("ptwProjectType", updateBy, allData, False)
                        ptwAdminsLogs(fileType,subform, rowId, current_user['userUniqueId'], request.method)
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
            else:
                return respond({
                            'show':True,
                            'status': 400,
                            'text': 'Something Went Wrong',
                            'icon': 'error'
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
                            '_id': 0
                        }
                    }
                ]
                Responsert = cmo.finding_aggregate("ptwProjectType", arrttss)['data']
                response = cmo.deleting("ptwProjectType", rowId, current_user['userUniqueId'] )

                ptwAdminsLogs(Responsert[0].get('fileType'),subform, rowId, current_user['userUniqueId'], request.method)
                return respond(response)
            else:
                return respond({
                            'show':True,
                            'status': 400,
                            'text': 'Please Provide a Valid UniqueID',
                            'icon': 'error'
                        })
    else:
        return respond({
                'show':True,
                'status': 400,
                'text': 'You Do Not Have Permission',
                'icon': 'error'
            })

@ptw_blueprint.route("/show/ptw/<fileType>",methods=['GET'])
@token_required
def getPtwForm(current_user,fileType):
    if (fileType !=None ):
        arra = [
            {
                '$match': {
                    'fileType':fileType
                }
            },
            {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            },{
            '$project': {
                '_id': 0, 
                'createdAt': 0, 
                'creatorId': 0, 
                'fileType': 0
            }
        }
    ]
    response = cmo.finding_aggregate("ptwProjectType", arra)
    return respond(response)


@ptw_blueprint.route("/submit/ptw/<fileType>/<subform>", methods=['GET', 'POST' ])
@ptw_blueprint.route("/submit/ptw/<fileType>/<subform>/<mileStoneId>", methods=['GET', 'POST','PATCH','DELETE'])
@token_required
def submitPtwform(current_user,fileType=None,subform=None,mileStoneId=None):
    if(request.method=="GET"):
        arra = [
            {
                '$match': {
                    'mileStoneId': mileStoneId,
                    '_id': request.args.get('operation_id')
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("ptwRaiseTicket", arra)
        return respond(response)
    
    elif(request.method=="POST"):
        allData = request.get_json()
        if not allData:
            return respond({
                    'show':True,
                    'status': 400,
                    'text': 'No JSON data provided',
                    'icon': 'error'
                })

        required_fields = {
            "siteUid": "Site UID is missing",
            "projectuniqueId": "Project Unique ID is missing",
            "mileStoneId": "Milestone ID is missing"
        }

        for key, error_message in required_fields.items():
            if key not in allData or not allData[key]:
                return respond({
                    'show':True,
                    'status': 400,
                    'text': error_message,
                    'icon': 'error'
                })
                
        arra = [
            {
                '$match': { 
                    'mileStoneId': allData["mileStoneId"],
                     '$or': [
                            {
                                'status': 'Closed'
                            }, {
                                'status': 'Auto Closed'
                            }
                        ],
                        'ptwDeleteStatus':{
                            '$ne':1
                        }
                    }
            }
        ]

        responseGet = cmo.finding_aggregate('ptwRaiseTicket', arra)['data']

        if responseGet : 
                allData[subform]['PTW Request date']=current_time()
                projectGroupId = allData["projectuniqueId"]
                siteUid = allData['siteUid']
                allData={
                    "formType":fileType,
                    "projectID":allData["projectID"],
                    "mileStoneId":allData["mileStoneId"],
                    "siteId":allData["siteId"],
                    "circle":allData["circle"],
                    "circleId":allData.get("circleId"),
                    "Milestone":allData["Milestone"],
                    "customerName":allData["customerName"],
                    "subProject":allData["subProject"],
                    "createdBy":current_user["userUniqueId"],
                    subform:allData[subform],
                    "projectuniqueId":projectGroupId,
                    "createdAt":int(unique_ptwtimestamp()),
                    "siteUid":siteUid
                }
                updateBy = {
                            'mileStoneId': allData['mileStoneId'],
                            '$or': [
                                    {
                                        'status': 'Closed'
                                    }, {
                                        'status': 'Auto Closed'
                                    }
                                ],
                            'ptwDeleteStatus':{
                                '$ne':1 
                            }
                            
                            }

                updateData = cmo.updating('ptwRaiseTicket',updateBy,{'ptwDeleteStatus':1},False)

                response = cmo.insertion("ptwRaiseTicket", allData)
                updateProjectLog(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                ptwRaiseTicketLogs(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData)

                return respond(response)
        elif not responseGet:
                allData[subform]['PTW Request date']=current_time()
                projectGroupId = allData["projectuniqueId"]
                siteUid = allData['siteUid']
                allData={
                    "formType":fileType,
                    "projectID":allData["projectID"],
                    "mileStoneId":allData["mileStoneId"],
                    "siteId":allData["siteId"],
                    "circle":allData["circle"],
                    "circleId":allData.get("circleId"),
                    "Milestone":allData["Milestone"],
                    "customerName":allData["customerName"],
                    "subProject":allData["subProject"],
                    "createdBy":current_user["userUniqueId"],
                    subform:allData[subform],
                    "projectuniqueId":projectGroupId,
                    "createdAt":int(unique_ptwtimestamp()),
                    "siteUid":siteUid
                }
                response = cmo.insertion("ptwRaiseTicket", allData)
                updateProjectLog(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                ptwRaiseTicketLogs(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData)
                return respond(response)
        else:
                allData[subform]['PTW Request date']=current_time()
                projectGroupId = allData["projectuniqueId"]
                siteUid = allData['siteUid']
                updateBy={
                    "mileStoneId":allData["mileStoneId"]
                }
                allData={
                    "formType":fileType,
                    "projectID":allData["projectID"],
                    "mileStoneId":allData["mileStoneId"],
                    "siteId":allData["siteId"],
                    "circle":allData["circle"],
                    "circleId":allData.get("circleId"),
                    "Milestone":allData["Milestone"],
                    "customerName":allData["customerName"],
                    "subProject":allData["subProject"],
                    "createdBy":current_user["userUniqueId"],
                    subform:allData[subform],
                    "projectuniqueId":projectGroupId,
                    "createdAt":int(unique_ptwtimestamp()),
                    "siteUid":siteUid
                }
                unset_data = {
                        "rejectionReason": "",
                        "ptwphoto2wheeler": "",
                        "roadsafetychecklist4wheeler": "",
                        "ptwphoto4wheeler": "",
                        "roadsafetychecklist2wheeler": "",
                        "ptwphoto": "",
                        "teamdetails": "",
                        "riskassessment": "",
                        "photo": "",
                    } if subform == "checklist" else {}
                response = cmo.updating("ptwRaiseTicket", updateBy, allData, False, unset=unset_data)
                updateProjectLog(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                ptwRaiseTicketLogs(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData)
                return respond(response)
        
    elif(request.method=="PATCH"):
            skip_keys = {"mileStoneId", "siteId", "Milestone","circle","projectID","customerName","subProject","projectuniqueId","siteUid","circleId"}
            format = ['png','jpeg','jpg']
            base_url = request.host_url.rstrip("/").replace("http://", "https://")
            operationId = request.args.get('operation_id')

            if operationId :
                updateBy={}
                if operationId:
                    updateBy = {
                                'mileStoneId': mileStoneId,
                                "_id":ObjectId(operationId)
                            }
                def validate_required_fields(data, is_json=True):
                    required_fields = {
                        "siteUid": "Site Unique ID is missing",
                        "projectuniqueId": "Project Unique ID is missing",
                        "mileStoneId": "Milestone Unique ID is missing"
                    }
                    for key, msg in required_fields.items():
                        value = data.get(key) if is_json else request.form.get(key)
                        if not value:
                            return {
                                'show': True,
                                'status': 400,
                                'text': msg,
                                'icon': 'error'
                            }
                    return None

                def parse_form_and_files(subform, fileType):
                    data = {}
                    for key, file in request.files.items():
                        if file:
                            pathing = cform.singleFileSaverPtw(file, "", "", subform, fileType)
                            if pathing["status"] == 422:
                                return None, pathing
                            elif pathing["status"] == 200:
                                data[key] = pathing["msg"]
                    
                    for key in request.form:
                        val = request.form.get(key)
                        if val and val != "undefined" and key not in skip_keys:
                            data[key] = val.replace(base_url, "", 1) if val.startswith(base_url) else val

                    return {subform: data}, None

                def perform_update_and_logging(fileType, subform, mileStoneId, user_id, method, allData, siteUid, projectGroupId, unset_data=None):
                    updateProjectLog(fileType, subform, mileStoneId, user_id, method, allData, siteUid, projectGroupId)
                    ptwRaiseTicketLogs(fileType, subform, mileStoneId, user_id, method, allData)
                    return cmo.updating("ptwRaiseTicket", updateBy, allData, False if not unset_data else True, unset=unset_data or {})

                # Actual route logic:
                if subform in ["checklist", "riskassessment", "roadsafetychecklist2wheeler", "roadsafetychecklist4wheeler"]:
                    allData = request.get_json()
                    if not allData:
                        return respond({
                            'show': True,
                            'status': 400,
                            'text': 'No JSON data provided',
                            'icon': 'error'
                        })
                    validation_error = validate_required_fields(allData)
                    if validation_error:
                        return respond(validation_error)

                    projectGroupId = allData["projectuniqueId"]
                    siteUid = allData["siteUid"]
                    if subform == "checklist":
                        allData = {
                            "formType": fileType,
                            "projectID": allData["projectID"],
                            "mileStoneId": mileStoneId,
                            "siteId": allData["siteId"],
                            "circle": allData["circle"],
                            "circleId": allData.get("circleId"),
                            "Milestone": allData["Milestone"],
                            "customerName": allData["customerName"],
                            "subProject": allData["subProject"],
                            "createdBy": current_user["userUniqueId"],
                            subform: allData[subform],
                            "projectuniqueId": projectGroupId,
                            "createdAt": int(unique_ptwtimestamp()),
                            "siteUid": siteUid
                        }

                        data = cmo.finding_aggregate('ptwRaiseTicket', [
                            {'$match': {'mileStoneId': mileStoneId, '_id': ObjectId(operationId)}}
                        ])['data']
                        unset_data = {} if data[0].get('status') in ['Submitted', 'L1-Approved'] else {
                            "rejectionReason": "",
                            "ptwphoto2wheeler": "",
                            "roadsafetychecklist4wheeler": "",
                            "ptwphoto4wheeler": "",
                            "roadsafetychecklist2wheeler": "",
                            "ptwphoto": "",
                            "teamdetails": "",
                            "riskassessment": "",
                            "photo": "",
                        }
                        response = perform_update_and_logging(fileType, subform, mileStoneId, current_user['userUniqueId'], request.method, allData, siteUid, projectGroupId, unset_data)

                    else:
                        allData = {subform: allData[subform]}
                        response = perform_update_and_logging(fileType, subform, mileStoneId, current_user['userUniqueId'], request.method, allData, siteUid, projectGroupId)

                    return respond(response)

                elif subform in ["photo", "ptwphoto", "ptwphoto2wheeler", "ptwphoto4wheeler", "teamdetails"]:
                    # validation_error = validate_required_fields(request.form, is_json=False)
                    # if validation_error:
                    #     return respond(validation_error)

                    parsed_data, error = parse_form_and_files(subform, fileType)
                    if error:
                        return respond(error)

                    projectGroupId = request.form.get("projectuniqueId")
                    siteUid = request.form.get("siteUid")

                    response = perform_update_and_logging(fileType, subform, mileStoneId, current_user['userUniqueId'], request.method, parsed_data, siteUid, projectGroupId)
                    return respond(response)

                # if(subform=="checklist"):
                #         allData = request.get_json()
                        
                #         if not allData:
                #             return respond({
                #                 'show':True,
                #                 'status': 400,
                #                 'text': 'No JSON data provided',
                #                 'icon': 'error'
                #             })
                #         required_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }

                #         for key, error_message in required_fields.items():
                #             if key not in allData or not allData[key]:
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })

                #         projectGroupId = allData["projectuniqueId"]
                #         siteUid = allData['siteUid']
                #         allData={
                #             "formType":fileType,
                #             "projectID":allData["projectID"],
                #             "mileStoneId":mileStoneId,
                #             "siteId":allData["siteId"],
                #             "circle":allData["circle"],
                #             "circleId":allData.get("circleId"),
                #             "Milestone":allData["Milestone"],
                #             "customerName":allData["customerName"],
                #             "subProject":allData["subProject"],
                #             "createdBy":current_user["userUniqueId"],
                #             subform:allData[subform],
                #             "projectuniqueId":projectGroupId,
                #             "createdAt":int(unique_ptwtimestamp()),
                #             "siteUid":siteUid
                #         }
                #         newaggr = [
                #             {
                #                 '$match': {
                #                     'mileStoneId': mileStoneId, 
                #                     '_id': ObjectId(operationId)
                #                 }
                #             }
                #         ]
                #         data = cmo.finding_aggregate('ptwRaiseTicket',newaggr)['data']
                #         unset_data={}
                #         if data[0].get('status')=='Submitted'or data[0].get('status')=='L1-Approved':
                #             unset_data={}
                #         else:
                #             unset_data = {
                #                 "rejectionReason": "",
                #                 "ptwphoto2wheeler": "",
                #                 "roadsafetychecklist4wheeler": "",
                #                 "ptwphoto4wheeler": "",
                #                 "roadsafetychecklist2wheeler": "",
                #                 "ptwphoto": "",
                #                 "teamdetails": "",
                #                 "riskassessment": "",
                #                 "photo": "",
                #             } if subform == "checklist" else {}

                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, True,unset=unset_data)

                #         return respond(response)
                
                # elif (subform == "photo"):
                #         allData = {}

                #         required_form_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }

                #         for field, error_message in required_form_fields.items():
                #             if not request.form.get(field):
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })
                #         projectGroupId = request.form.get("projectuniqueId")
                #         siteUid =request.form.get("siteUid")

                #         for key in request.files:
                #             file = request.files.get(key)
                #             if file:
                #                 pathing = cform.singleFileSaverPtw(file, "", "", subform,fileType)
                #                 if pathing["status"] == 422:
                #                     return respond(pathing)
                #                 elif pathing["status"] == 200:
                #                     allData[key] = pathing["msg"]
                #         for key in request.form:
                #             value = request.form.get(key)
                #             if value and value != "undefined" and key not in skip_keys:
                #                 if value.startswith(base_url):
                #                     value = value.replace(base_url, "", 1)
                #                 allData[key] = value
                        
                #         allData={
                #             subform:allData,
                #         }

                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)

                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)
                
                # elif(subform == "riskassessment"):
                #         allData = request.get_json()
                #         if not allData:
                #             return respond({
                #                 'show':True,
                #                 'status': 400,
                #                 'text': 'No JSON data provided',
                #                 'icon': 'error'
                #             })

                #         required_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }

                #         for key, error_message in required_fields.items():
                #             if key not in allData or not allData[key]:
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })
                #         projectGroupId = allData["projectuniqueId"]
                #         siteUid = allData['siteUid']
                #         allData= {
                #             subform:allData[subform],
                #         }
                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)
                    
                # elif(subform == "ptwphoto"):
                #         allData = {}

                #         required_form_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }
                #         for field, error_message in required_form_fields.items():
                #             if not request.form.get(field):
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })
                #         projectGroupId = request.form.get("projectuniqueId")
                #         siteUid =request.form.get("siteUid")
                       
                #         for key in request.files:
                #             file = request.files.get(key)
                #             if file:
                #                 pathing = cform.singleFileSaverPtw(file, "", "", subform,fileType)
                #                 if pathing["status"] == 422:
                #                     return respond(pathing)
                #                 elif pathing["status"] == 200:
                #                     allData[key] = pathing["msg"]
                #         for key in request.form:
                #             value = request.form.get(key)
                #             if value and value != "undefined" and key not in skip_keys:
                #                 if value.startswith(base_url):
                #                     value = value.replace(base_url, "", 1)
                #                 allData[key] = value
                                    
                #         allData = {
                #             subform: allData,
                #         }

                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)
                
                # elif(subform=="teamdetails"):
                #         allData = {}

                #         projectGroupId = request.form.get("projectuniqueId")
                #         siteUid =request.form.get("siteUid")
                        
                #         for key in request.files:
                #             file = request.files.get(key)
                #             if file:
                #                 pathing = cform.singleFileSaverPtw(file, "", "", subform,fileType)
                #                 if pathing["status"] == 422:
                #                     return respond(pathing)
                #                 elif pathing["status"] == 200:
                #                     allData[f"{key}"] = pathing["msg"]
                #         for key in request.form:
                #             value = request.form.get(key)
                #             if value and value != "undefined" and key not in skip_keys:
                #                 if value.startswith(base_url):
                #                     value = value.replace(base_url, "", 1)
                #                 allData[key] = value
                        
                #         allData = {
                #             subform: allData,   
                #         }

                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)
                    
                # elif(subform == "roadsafetychecklist2wheeler"):
                        
                #         allData = request.get_json()
                #         if not allData:
                #             return respond({
                #                 'show':True,
                #                 'status': 400,
                #                 'text': 'No JSON data provided',
                #                 'icon': 'error'
                #             })

                #         required_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }

                #         for key, error_message in required_fields.items():
                #             if key not in allData or not allData[key]:
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })
                #         projectGroupId = allData["projectuniqueId"]
                #         siteUid = allData['siteUid']
                #         allData= {
                #             subform:allData[subform],
                #         }
                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)

                # elif(subform == "ptwphoto2wheeler"):
                #         allData = {}

                #         required_form_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }

                #         for field, error_message in required_form_fields.items():
                #             if not request.form.get(field):
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })
                #         projectGroupId = request.form.get("projectuniqueId")
                #         siteUid =request.form.get("siteUid")
                       
                #         for key in request.files:
                #             file = request.files.get(key)
                #             if file:
                #                 pathing = cform.singleFileSaverPtw(file, "", "", subform,fileType)
                #                 if pathing["status"] == 422:
                #                     return respond(pathing)
                #                 elif pathing["status"] == 200:
                #                     allData[key] = pathing["msg"]
                #         for key in request.form:
                #             value = request.form.get(key)
                #             if value and value != "undefined" and key not in skip_keys:
                #                 if value.startswith(base_url):
                #                     value = value.replace(base_url, "", 1)
                #                 allData[key] = value
                        
                #         allData = {
                #             subform: allData,
                #         }

                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)
                    
                # elif(subform == "roadsafetychecklist4wheeler"):
                    
                #         allData = request.get_json()
                #         if not allData:
                #             return respond({
                #                 'show':True,
                #                 'status': 400,
                #                 'text': 'No JSON data provided',
                #                 'icon': 'error'
                #             })

                #         required_fields = {
                #             "siteUid": "Site Unique ID is missing",
                #             "projectuniqueId": "Project Unique ID is missing",
                #             "mileStoneId": "Milestone Unique ID is missing"
                #         }

                #         for key, error_message in required_fields.items():
                #             if key not in allData or not allData[key]:
                #                 return respond({
                #                     'show':True,
                #                     'status': 400,
                #                     'text': error_message,
                #                     'icon': 'error'
                #                 })

                #         projectGroupId = allData["projectuniqueId"]
                #         siteUid = allData['siteUid']
                #         allData= {
                #             subform:allData[subform],
                #         }
                #         response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #         updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #         ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #         return respond(response)
                    
                # elif(subform == "ptwphoto4wheeler"):
                #     allData = {}

                #     required_form_fields = {
                #         "siteUid": "Site Unique ID is missing",
                #         "projectuniqueId": "Project Unique ID is missing",
                #         "mileStoneId": "Milestone Unique ID is missing"
                #     }

                #     for field, error_message in required_form_fields.items():
                #         if not request.form.get(field):
                #             return respond({
                #                 'show':True,
                #                 'status': 400,
                #                 'text': error_message,
                #                 'icon': 'error'
                #             })
                #     projectGroupId = request.form.get("projectuniqueId")
                #     siteUid =request.form.get("siteUid")
                    
                #     for key in request.files:
                #         file = request.files.get(key)
                #         if file:
                #             pathing = cform.singleFileSaverPtw(file, "", "", subform,fileType)
                #             if pathing["status"] == 422:
                #                 return respond(pathing)
                #             elif pathing["status"] == 200:
                #                 allData[key] = pathing["msg"]
                #     for key in request.form:
                #         value = request.form.get(key)
                #         if value and value != "undefined" and key not in skip_keys:
                #             if value.startswith(base_url):
                #                 value = value.replace(base_url, "", 1)
                #             allData[key] = value

                #     allData = {
                #         subform: allData,
                #     }
                #     response = cmo.updating("ptwRaiseTicket", updateBy, allData, False)
                #     updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                #     ptwRaiseTicketLogs(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData)
                #     return respond(response)
            
            else:

                allData = request.get_json()
                projectGroupId = allData["projectuniqueId"]
                siteUid = allData['siteUid']
                allData={
                        "formType":fileType,
                        "projectID":allData["projectID"],
                        "mileStoneId":allData["mileStoneId"],
                        "siteId":allData["siteId"],
                        "circle":allData["circle"],
                        "circleId":allData.get("circleId"),
                        "Milestone":allData["Milestone"],
                        "customerName":allData["customerName"],
                        "subProject":allData["subProject"],
                        "createdBy":current_user["userUniqueId"],
                        subform:allData[subform],
                        "projectuniqueId":projectGroupId,
                        "createdAt":int(unique_ptwtimestamp()),
                        "siteUid":siteUid
                    }
                allData[subform]['PTW Request date']=current_time()

                updateBy = {
                                'mileStoneId': allData['mileStoneId'],
                                '$or': [
                                        {
                                            'status': 'Closed'
                                        }, {
                                            'status': 'Auto Closed'
                                        }
                                    ], 
                                'ptwDeleteStatus':{
                                    '$ne':1 
                                }
                                }
                response = cmo.insertion("ptwRaiseTicket", allData)
                updateData = cmo.updating('ptwRaiseTicket',updateBy,{'ptwDeleteStatus':1},False)
                
                updateProjectLog(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData,siteUid,projectGroupId)
                ptwRaiseTicketLogs(fileType,subform,allData["mileStoneId"], current_user['userUniqueId'], request.method, allData)
               
                return respond(response)

@ptw_blueprint.route("/template/<filePath>/ptw", methods=["GET"])
@token_required
def template_download(current_user, filePath):
    if filePath in ["checklist","photo","riskassessment","ptwphoto","teamdetails","rejectionreason","roadsafetychecklist4wheeler","roadsafetychecklist2wheeler",'ptwphoto2wheeler','ptwphoto4wheeler']:
        filePath = "Template.xlsx"
    return send_file(os.path.join(os.getcwd(), "templateList", filePath))



# @ptw_blueprint.route("/")
@ptw_blueprint.route("/Ptw/customers", methods=["GET"])
@token_required
def ptwCustomers(current_user):
    if request.method == 'GET':
        arr=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }
                    }
                }, {
                    '$project': {
                        'customerName': 1, 
                        'shortName': 1, 
                        'customer': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
        Response=cmo.finding_aggregate("customer",arr)
        return respond(Response)
    
@ptw_blueprint.route("/Ptw/employee", methods=["GET"])
@token_required
def ptwEmployee(current_user):
    if request.method == 'GET':
        arr=[
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
                        'empName': {
                            '$concat': [
                                '$empName', '(', '$empCode', ')'
                            ]
                        }, 
                        'userRole': {
                            '$arrayElemAt': [
                                '$userResults.roleName', 0
                            ]
                        }, 
                        'empCode': 1, 
                        'employeeId': {
                            '$toString': '$_id'
                        }, 
                        '_id': 0
                    }
                }
            ]
        Response=cmo.finding_aggregate("userRegister",arr)
        return respond(Response)



@ptw_blueprint.route("/Ptw/projectType", methods=["GET"])
@ptw_blueprint.route("/Ptw/projectType/<id>", methods=["GET"])
@token_required
def ptwprojectType(current_user,id=None):
    if request.method == 'GET':
        if id != None:
            arr=[
                    {
                        '$match': {
                            'deleteStatus': {
                                '$ne': 1
                            }, 
                            'status': 'Active', 
                            'custId': id
                        }
                    }, {
                        '$group': {
                            '_id': '$projectType', 
                            'projectTypeName': {
                                '$first': '$projectType'
                            }, 
                            'custId': {
                                '$first': '$custId'
                            }, 
                            'projectType': {
                                '$first': {
                                    '$toString': '$_id'
                                }
                            }
                        }
                    }
                ]

            Response=cmo.finding_aggregate("projectType",arr)
            return respond(Response)
        else:
            return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Invalid Customer',
                    'icon': 'error'
                })

@ptw_blueprint.route("/Ptw/projectGroup", methods=["GET"])
@ptw_blueprint.route("/Ptw/projectGroup/<id>", methods=["GET"])
@token_required
def ptwprojectGroup(current_user,id=None):
    if request.method == 'GET':
        if id != None:
            arr=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        }, 
                        'customerId': ObjectId(id)
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
            Response=cmo.finding_aggregate("projectGroup",arr)
            return respond(Response)
        else:
            return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Invalid Customer',
                    'icon': 'error'
                })


@ptw_blueprint.route("/Ptw/circleList", methods=["GET"])
@ptw_blueprint.route("/Ptw/circleList/<id>/<projectType>", methods=["GET"])
@token_required
def ptwmilestoneList(current_user,id=None,projectType=None):
    if request.method == 'GET':
        if id != None and projectType != None:
            arr=[
                {
                    '$match': {
                        '_id': ObjectId(projectType), 
                        'customerId': ObjectId(id)
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
                        '_id': 0
                    }
                }
            ]
            Response=cmo.finding_aggregate("projectGroup",arr)
            
            return respond(Response)
        else:
            return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Invalid Customer or Project Type!',
                    'icon': 'error'
                })
            
            
@ptw_blueprint.route("/Ptw/MDBApprover",methods=['GET','POST','DELETE'])
@ptw_blueprint.route("/Ptw/MDBApprover/<id>",methods=['GET','POST','DELETE'])
@token_required
def ptwMDBApprover(current_user,id=None):
    if request.method == 'GET':
        ApproverType=request.args.get('ApproverType')
        arrr=[
                {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        "ApproverType":ApproverType
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'employee', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$lookup': {
                                    'from': 'userRole', 
                                    'localField': 'userRole', 
                                    'foreignField': '_id', 
                                    'as': 'RoleResults'
                                }
                            }, {
                                '$addFields': {
                                    'userRole': {
                                        '$arrayElemAt': [
                                            '$RoleResults.roleName', 0
                                        ]
                                    }
                                }
                            }
                        ], 
                        'as': 'employee'
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
                        'as': 'projectGroupResults'
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
                        'empName': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$employee.empName', 0
                                ]
                            }
                        }, 
                        'employeeId': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$employee._id', 0
                                ]
                            }
                        }, 
                        'profile': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$employee.userRole', 0
                                ]
                            }
                        }, 
                        'projectTypeName': 1, 
                        'projectType': {
                            '$toString': '$projectType'
                        }, 
                        'Milestone': '$milestone', 
                        'projectGroupName': {
                            '$arrayElemAt': [
                                '$projectGroupResults.projectGroupName', 0
                            ]
                        }, 
                        'customerName': {
                            '$arrayElemAt': [
                                '$projectGroupResults.Customer', 0
                            ]
                        }, 
                        'customer': {
                            '$toString': '$customer'
                        }, 
                        'projectGroup': {
                            '$toString': '$projectGroup'
                        }, 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'circleName': {
                            '$arrayElemAt': [
                                '$circleResults.circleName', 0
                            ]
                        }, 
                        '_id': 0, 
                        'circle': {
                            '$toString': '$circle'
                        },
                        'circleName': 1
                    }
                }
            
            ]
        print(arrr,'guyegduyguygq')
        arrr+=apireq.commonarra
        if request.args.get('page') and request.args.get('limit'):
            arrr = arrr+apireq.args_pagination(request.args)
        # print(arrr,"kjhgfdsyuioiuytdfghjkjhgfdcvghjk")
        Response=cmo.finding_aggregate("ptwMdb",arrr)
        return respond(Response)
    if request.method == 'POST':
        if id != None:
            
            data=request.get_json()
            checkingList=['customer','employee','projectGroup','projectType','circle','ApproverType']
            invalid_Values=[]
            for keys in checkingList:
                if keys not in data or data[keys] in ['',None,'undefined']:
                    invalid_Values.append(keys)
            if len(invalid_Values):
                return respond({
                    'show':True,
                    'status': 400,
                    'text': f'Please fill these{invalid_Values}',
                    'icon': 'error'
                })
            insertionData={
                'customer':ObjectId(data['customer']),
                'employee':ObjectId(data['employee']),
                'projectGroup':ObjectId(data['projectGroup']),
                'projectType':ObjectId(data['projectType']),
                'circle':ObjectId(data['circle']),
                'addedAt':get_current_date_timestamp(),
                'addedBy':ObjectId(current_user["userUniqueId"]),
                'projectTypeName':data['projectTypeName'],
                'ApproverType':data['ApproverType']
            }
            arrt=[
                    {
                        '$match': {
                            'customer': insertionData['customer'], 
                            'employee': insertionData['employee'], 
                            'projectGroup': insertionData['projectGroup'], 
                            'projectTypeName': insertionData['projectTypeName'], 
                            'circle':insertionData['circle'], 
                            'ApproverType':data['ApproverType'],
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ]
            Rest=cmo.finding_aggregate("ptwMdb",arrt)['data']
            if len(Rest):
                return respond({
                    'show':True,
                    'status': 400,
                    'text': f'Already defined for this Customer,Project Type,Project Group and Circle',
                    'icon': 'error'
                })
            Response=cmo.updating("ptwMdb",{'_id':ObjectId(id)},insertionData,False)
            return respond(Response)
        else:
            data=request.get_json()
            checkingList=['customer','employee','projectGroup','projectType','ApproverType','circle']
            invalid_Values=[]
            for keys in checkingList:
                if keys not in data or data[keys] in ['',None,'undefined']:
                    invalid_Values.append(keys)
            if len(invalid_Values):
                return respond({
                    'show':True,
                    'status': 400,
                    'text': f'Please fill these{invalid_Values}',
                    'icon': 'error'
                })
            insertionData={
                'customer':ObjectId(data['customer']),
                'employee':ObjectId(data['employee']),
                'projectGroup':ObjectId(data['projectGroup']),
                'projectType':ObjectId(data['projectType']),
                'circle':ObjectId(data['circle']),
                'addedAt':get_current_date_timestamp(),
                'addedBy':ObjectId(current_user["userUniqueId"]),
                'projectTypeName':data['projectTypeName'],
                'ApproverType':data['ApproverType']
            }
            arrt=[
                    {
                        '$match': {
                            'customer': insertionData['customer'], 
                            'employee': insertionData['employee'], 
                            'projectGroup': insertionData['projectGroup'], 
                            'projectTypeName': insertionData['projectTypeName'], 
                            'circle':insertionData['circle'],
                            'ApproverType':data['ApproverType'],
                            'deleteStatus': {
                                '$ne': 1
                            }
                        }
                    }
                ]
            Rest=cmo.finding_aggregate("ptwMdb",arrt)['data']
            if len(Rest):
                return respond({
                    'show':True,
                    'status': 400,
                    'text': f'Already defined for this Customer,Project Type,Project Group ',
                    'icon': 'error'
                })
                
            Response=cmo.insertion("ptwMdb",insertionData)
            return respond(Response)
    if request.method == 'DELETE':
        if id != None:
            Response=cmo.deleting("ptwMdb",id)
            return respond(Response)          

@ptw_blueprint.route("/Export/ptwMDB",methods=['GET','POST'])  
@ptw_blueprint.route("/Export/ptwMDB/<id>",methods=['GET','POST'])  
@token_required
def ExportptwMdb(current_user,id=None):
    data=request.get_json()
    ApproverType=data['ApproverType']
    arr=[
             {
                    '$match': {
                        'deleteStatus': {
                            '$ne': 1
                        },
                        "ApproverType":ApproverType
                    }
                }, {
                    '$lookup': {
                        'from': 'userRegister', 
                        'localField': 'employee', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$lookup': {
                                    'from': 'userRole', 
                                    'localField': 'userRole', 
                                    'foreignField': '_id', 
                                    'as': 'RoleResults'
                                }
                            }, {
                                '$addFields': {
                                    'userRole': {
                                        '$arrayElemAt': [
                                            '$RoleResults.roleName', 0
                                        ]
                                    }
                                }
                            }
                        ], 
                        'as': 'employee'
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
                        'as': 'projectGroupResults'
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
                        'empName': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$employee.empName', 0
                                ]
                            }
                        }, 
                        'employeeId': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$employee._id', 0
                                ]
                            }
                        }, 
                        'profile': {
                            '$toString': {
                                '$arrayElemAt': [
                                    '$employee.userRole', 0
                                ]
                            }
                        }, 
                        'projectTypeName': 1, 
                        'projectType': {
                            '$toString': '$projectType'
                        }, 
                        'Milestone': '$milestone', 
                        'projectGroupName': {
                            '$arrayElemAt': [
                                '$projectGroupResults.projectGroupName', 0
                            ]
                        }, 
                        'customerName': {
                            '$arrayElemAt': [
                                '$projectGroupResults.Customer', 0
                            ]
                        }, 
                        'customer': {
                            '$toString': '$customer'
                        }, 
                        'projectGroup': {
                            '$toString': '$projectGroup'
                        }, 
                        'uniqueId': {
                            '$toString': '$_id'
                        }, 
                        'circleName': {
                            '$arrayElemAt': [
                                '$circleResults.circleName', 0
                            ]
                        }, 
                        '_id': 0, 
                        'circle': {
                            '$toString': '$circle'
                        }
                    }
                }
            , {
                '$project': {
                    'Employee Name': '$empName', 
                    'Profile': '$profile', 
                    'Customer': '$customerName', 
                     'Project Group': '$projectGroupName', 
                    'Project Type': '$projectTypeName', 
                    'Circle Name': '$circleName'
                }
            }
        ]
    response=cmo.finding_aggregate("ptwMdb",arr)
    response = response["data"]
    dataframe = pd.DataFrame(response)
    name="Export_"+ApproverType
    fullPath = excelWriteFunc.excelFileWriter(
        dataframe, name, name
    )

    return send_file(fullPath)
    
#---------------For getting L1 & L2 Approver Data on Selection and below for Assign--------------->   
@ptw_blueprint.route("/getPtwApprover", methods=['GET', 'POST','PUT','PATCH'])  
@ptw_blueprint.route("/getPtwApprover/<projectType>", methods=['GET', 'POST','PUT','PATCH'])  
@ptw_blueprint.route("/getPtwApprover/<projectType>/<circleId>", methods=['GET', 'POST','PUT','PATCH'])  
@token_required
def getPtwApprover(current_user, projectType=None,circleId=None):

    if request.args.get('projectType'):
        projectType = request.args.get('projectType')

    match_stage = {
        'projectTypeName': projectType
    }

    if circleId:
        match_stage['circle'] = ObjectId(circleId)
    
    if request.method == 'GET':
        arra = [
            {
                '$match':match_stage
            },{
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'employee', 
                'foreignField': '_id', 
                'as': 'employeeData'
            }
        }, {
            '$addFields': {
                'empName': {
                    '$concat': [
                        {
                            '$ifNull': [
                                {
                                    '$arrayElemAt': [
                                        '$employeeData.empName', 0
                                    ]
                                }, ''
                            ]
                        }, '(', {
                            '$ifNull': [
                                {
                                    '$arrayElemAt': [
                                        '$employeeData.empCode', 0
                                    ]
                                }, ''
                            ]
                        },')'
                    ]
                }, 
                'empId': {
                    '$arrayElemAt': [
                        '$employeeData._id', 0
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'projectTypeName': 1, 
                'ApproverType': 1, 
                'empName': 1, 
                'empId': {
                    '$toString': '$empId'
                }
            }
        }
        ]
        print('this is aggr',arra,'qwerty')
        response = cmo.finding_aggregate('ptwMdb', arra)

        return respond(response)
    
    if request.method == 'PATCH':
        allData = request.get_json()

        empId = allData.get('empId')
        ApproverType = allData.get('ApproverType')                                                                                          
        ptwNumber = request.args.get('ptwNumber')
        operationId = request.args.get('operation_id')

        match_query = {
            'mileStoneId': projectType
        }

        if operationId:
            match_query['_id'] = ObjectId(operationId)

        if ptwNumber:
            match_query['ptwNumber'] = ptwNumber

        aggr = [
            { '$match': match_query }
        ]

        if allData.get("status") == "L2-Approved":
            aggr.append({
                '$addFields': {
                    'approverId': {
                        '$toObjectId': current_user['userUniqueId']
                    },
                    'creatorId': {
                        '$toObjectId': '$createdBy'
                    }
                }
            })
        else:

            if not empId:
                return respond({
                    'show':True,
                    'status': 400,
                    'text': 'empId is Required',
                    'icon': 'error'
                })
            aggr.append({
                '$addFields': {
                    'approverId': {
                        '$toObjectId': empId 
                    },
                    'creatorId': {
                        '$toObjectId': '$createdBy'
                    }
                }
            })

        aggr.extend([
            {
                '$lookup': {
                    'from': 'userRegister',
                    'localField': 'approverId',
                    'foreignField': '_id',
                    'as': 'approverData'
                }
            },
            {
                '$addFields': {
                    'approverEmail': {'$arrayElemAt': ['$approverData.email', 0]},
                    'approverName': {'$arrayElemAt': ['$approverData.empName', 0]}
                }
            },
            {
                '$lookup': {
                    'from': 'userRegister',
                    'localField': 'creatorId',
                    'foreignField': '_id',
                    'as': 'userData'
                }
            },
            {
                '$addFields': {
                    'userEmail': {'$arrayElemAt': ['$userData.email', 0]},
                    'userName': {'$arrayElemAt': ['$userData.empName', 0]}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'circle': 1,
                    'siteId': 1,
                    'fileType': '$formType',
                    'siteuid': {'$ifNull': ['$siteUid', None]},
                    'projectuid': {'$ifNull': ['$projectuniqueId', None]},
                    'approverName': 1,
                    'approverEmail': 1,
                    'userName': 1,
                    'userEmail': 1,
                    "ptwNumber":1,
                    'checklist':1,
                    'raiseCounter':1
                }
            }
        ])

        ptwResponse = cmo.finding_aggregate('ptwRaiseTicket', aggr) 

        ptwResponse = ptwResponse["data"][0] if ptwResponse["data"] else {}

        existPtwNumber = allData.get('ptwNumber') or ptwResponse.get('ptwNumber')
        raiseCounter = 0
        if existPtwNumber:
            newPtwNumber = existPtwNumber
            raiseCounter = ptwResponse.get("raiseCounter")
            raiseCounter = raiseCounter + 1 if raiseCounter is not None else 1
        else:
            # newarr = [
            #     ('ptwRaiseAt', -1)
            # ]
            query = {
                'ptwNumber': { '$exists': True }
            }

            newarr = [
                ('createdAt', -1)
            ]

            # last_doc_list = cmo.find_one('ptwRaiseTicket',newarr)
            last_doc_list = cmo.find_one('ptwRaiseTicket',newarr,query)
            # print(last_doc_list,"kdf")
            last_doc = last_doc_list if last_doc_list and "ptwNumber" in last_doc_list else None

            newPtwNumber = generatePtwNumber(ptwResponse, last_doc)
        updateData= {}
        level=""
        if ApproverType=="L1-Approver":
            if not empId:
                    return respond({
                    'show':True,
                    'status': 400,
                    'text': 'empId is Required',
                    'icon': 'error'
                })
            level="L1-Approval"

            updateData = {
                ApproverType: {
                    "approverId": empId,
                },
                'checklist':{
                    **ptwResponse.get("checklist", {}),
                    'PTW Valid from time':current_time(),
                    'PTW Valid upto time':current_time_plus_8hrs(),
                    'PTW Issuer':ptwResponse['approverName']
                },
                "isL1Approved":False,

                "raiseCounter":raiseCounter,
                "isL2Approved":False,
                "isL1Rejected":False,
                "isL2Rejected":False,
                "ptwRaiseAt":int(unique_ptwtimestamp()),
                "ptwUpdatedAt":int(unique_ptwtimestamp()),
                "status":"Submitted", #allData.get("status")
                "expireAt":int(unique_8hoursPlusTimestamp()),
                "ptwNumber": newPtwNumber
            }
        elif  ApproverType=="L2-Approver" :
            if not empId:
                return respond({
                    'show':True,
                    'status': 400,
                    'text': 'empId is Required',
                    'icon': 'error'
                })
            level="L2-Approval"
            updateData = {
                ApproverType: {
                    "approverId": empId,
                },
                'checklist':{
                    **ptwResponse.get("checklist", {}),
                    'PTW Issuer':ptwResponse['approverName']
                },
                "L1-Approver": {
                    "approverId": current_user["userUniqueId"],
                    "actionAt":int(unique_ptwtimestamp()),
                }, 
                "isL1Approved":allData.get("approved"),

                "isAutoClose":False,
                "ptwUpdatedAt":int(unique_ptwtimestamp()),
                "expireAt":int(unique_8hoursPlusTimestamp()),
                "status":allData.get("status")
            }
            
        elif  allData.get("status")=="L2-Approved" :
            if allData['approved'] is False:
                return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Approved is True Required',
                    'icon': 'error'
                })
            if allData['status'] is None:
                return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Status is Required',
                    'icon': 'error'
                })
            updateData = {
                "L2-Approver": {
                    "approverId": current_user["userUniqueId"],
                    "actionAt":int(unique_ptwtimestamp()),
                },
                "isL2Approved":allData.get("approved"),

                "ptwUpdatedAt":int(unique_ptwtimestamp()),
                "status":allData.get("status"),
                "expireAt":int(unique_8hoursPlusTimestamp())
            }
        updateBy={}
        if operationId:
            updateBy = {
                'mileStoneId': projectType,
                '_id': ObjectId(operationId)
            }
        else:
            updateBy = {
                'mileStoneId': projectType,
                'ptwNumber': ptwNumber
            }
            
        response = cmo.updating("ptwRaiseTicket", updateBy, updateData, True)

        updateProjectLog(ptwResponse["fileType"],ApproverType,projectType, current_user['userUniqueId'], request.method,updateData,ptwResponse['siteuid'],ptwResponse['projectuid'])
        ptwRaiseTicketLogs(ptwResponse["fileType"],ApproverType,projectType, current_user['userUniqueId'], request.method,updateData)
        if response:
            ptwData = {
                "status":allData.get("status",'Submitted') ,
                "userName":current_user['empName'],
                "ptwNumber":newPtwNumber
            }
            ptwAppData = {
                "status":allData.get("status",'Submitted') ,
                "userName":ptwResponse['approverName'],
                "ptwNumber":newPtwNumber,
            }

            if ApproverType=='L1-Approver':
                try:  
                    cmailer.ptw_sendmail(to=[ptwResponse['approverEmail']],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwApproverMailTemplate(ptwAppData,level))
                    cmailer.ptw_sendmail(to=[current_user["email"]],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwMailTempplate(ptwData))
                except Exception as e:
                    print("Email sending failed:", str(e))
            elif ApproverType == 'L2-Approver':
                try:
                    ptwData['userName'] = ptwResponse['userName']
                    cmailer.ptw_sendmail(to=[ptwResponse['approverEmail']],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwApproverMailTemplate(ptwAppData,level))
                    cmailer.ptw_sendmail(to=[ptwResponse['userEmail']],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwMailTempplate(ptwData))
                except Exception as e:
                    print("Email sending failed:", str(e))    
            elif allData.get("status")=="L2-Approved":
                try:
                    ptwData['userName'] = ptwResponse['userName']
                    cmailer.ptw_sendmail(to=[ptwResponse['userEmail']],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwMailTempplate(ptwData))
                except Exception as e:
                    print("Email sending failed:", str(e))
            return respond(response)
        else :
            return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Unable to Send Mail',
                    'icon': 'error'
                })

@ptw_blueprint.route("/ptw/close",methods=["PATCH"])
@token_required
def close_ptw(current_user):
    ptwNumber =  request.args.get("ptwNumber")
    allData = request.get_json()

    if  not ptwNumber:
        return respond({
                'show':True,
                'status': 400,
                'text': 'Ptw Number is Required',
                'icon': 'error'
             })

    updateBy ={
        "ptwNumber":ptwNumber
    }
    aggr = [
        {
            "$match": { 
                "ptwNumber": ptwNumber
                }
        }
    ]
    getData = cmo.finding_aggregate("ptwRaiseTicket", aggr)["data"][0]

    is_l1_approved = getData.get("isL1Approved")
    is_l2_approved = getData.get("isL2Approved")
    if not is_l1_approved:
        return respond({
                'show':True,
                'status': 400,
                'text': "L1 and L2 approval is required to close PTW",
                'icon': 'error'
            })
    if not is_l2_approved:
        return respond({
            'show':True,
            'status': 400,
            'text': "L2 approval is required to close PTW",
            'icon': 'error'
        })
    status_val = allData.get("status", "").strip()
    if status_val.lower() == "closed":
        status_val = "Closed"
    updateData = {
        "isAutoClose":False,
        "status":status_val,
        "closedAt":int(unique_ptwtimestamp())
    }

    response = cmo.updating("ptwRaiseTicket", updateBy,updateData, True)
    ptwData = {
        "status":allData.get("status",'Submitted') ,#submitted
        "userName":current_user['empName'],# username
        "ptwNumber":ptwNumber#ptwNumber
    }
   
    cmailer.ptw_sendmail(to=[current_user['email']],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwMailTempplate(ptwData))
    updateProjectLog(getData['formType'],'Closed',getData["mileStoneId"], current_user['userUniqueId'], request.method, allData,getData['siteUid'],getData['projectuniqueId'])
    ptwRaiseTicketLogs(getData['formType'],'Closed',getData["mileStoneId"], current_user['userUniqueId'], request.method, allData)
    return respond(response)


#------------For getting in Table L1-Approver and L2-Approver Data----------------
@ptw_blueprint.route("/approverData" , methods = ['GET','POST'])
@token_required
def getApproverData(current_user):
    userId = current_user['userUniqueId']
    type = request.args.get("ApproverType")
    startDate = request.args.get("startDate")
    endDate = request.args.get("endDate")
    
    aggr = [{ 
            '$match': {
                'L1-Approver.approverId': userId
            }
            },{
                '$addFields': {
                    'actionAt': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$isL1Rejected', True
                                ]
                            }, 
                            'then': '$L1-Approver.actionAt', 
                            'else': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$isL2Rejected', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt', 
                                    'else': {
                                        '$cond': {
                                            'if': {
                                                '$eq': [
                                                    '$isL2Approved', True
                                                ]
                                            }, 
                                            'then': '$L2-Approver.actionAt', 
                                            'else': {
                                                '$cond': {
                                                    'if': {
                                                        '$eq': [
                                                            '$isL1Approved', True
                                                        ]
                                                    }, 
                                                    'then': '$L1-Approver.actionAt', 
                                                    'else': ''
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }, 
                    
                    'l1ActionDAte': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$isL1Rejected', True
                                        ]
                                    }, 
                                    'then': '$L1-Approver.actionAt'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$isL1Approved', True
                                        ]
                                    }, 
                                    'then': '$L1-Approver.actionAt'
                                }
                            ], 
                            'default': ''
                        }
                    }, 
                    'l2ActionDAte': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$isL2Rejected', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$isL2Approved', True
                                        ]
                                    }, 
                                    'then': '$L2-Approver.actionAt'
                                }
                            ], 
                            'default': ''
                        }
                    }, 
                    'l1Ageing': {
                        '$dateDiff': {
                            'startDate': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'endDate':  {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$eq': [
                                                    '$isL1Approved', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L1-Approver.actionAt'
                                            }
                                        }, {
                                            'case': {
                                                '$eq': [
                                                    '$isL1Rejected', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L1-Approver.actionAt'
                                            }
                                        }
                                    ], 
                                    'default': {
                                        '$toDate': get_current_date_timestamp()
                                    }
                                }
                            },
                            'unit': 'day'
                        }
                    }, 
                    'l2Ageing': {
                        '$dateDiff': {
                            'startDate': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'endDate': {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$eq': [
                                                    '$isL2Approved', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L2-Approver.actionAt'
                                            }
                                        }, {
                                            'case': {
                                                '$eq': [
                                                    '$isL2Rejected', True
                                                ]
                                            }, 
                                            'then': {
                                                '$toDate': '$L2-Approver.actionAt'
                                            }
                                        }
                                    ], 
                                    'default': {
                                        '$toDate': get_current_date_timestamp()
                                    }
                                }
                            }, 
                            'unit': 'day'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'createdBy': {
                        '$toObjectId': '$createdBy'
                    }, 
                    'ssId': '$checklist.SSID', 
                    'uniqueId': '$checklist.Unique ID', 
                    'srNumber': '', 
                    'projectType': '$checklist.Project type', 
                    'mileStoneId': {
                        '$toObjectId': '$mileStoneId'
                    }, 
                    'activity': '$checklist.Activity', 
                    'submissionDate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y %H:%M', 
                            'date': {
                                '$toDate': '$ptwRaiseAt'
                            }, 
                            'timezone': 'Asia/Kolkata'
                        }
                    }, 
                    'actionDate': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$actionAt', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$actionAt'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'ptwCreationDate': {
                        '$cond': {
                            'if': {
                                '$in': [
                                    '$status', [
                                        'Closed', 'Auto Closed'
                                    ]
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$closedAt'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    },
                    'l1ActionDAte': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$l1ActionDAte', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$l1ActionDAte'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }, 
                    'l2ActionDAte': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$l2ActionDAte', ''
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'format': '%d-%m-%Y %H:%M', 
                                    'date': {
                                        '$toDate': '$l2ActionDAte'
                                    }, 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }, 
                            'else': ''
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectID', 
                    'foreignField': 'projectId', 
                    'pipeline': [
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
                                            'projectGroupName': {
                                                '$concat': [
                                                    '$customer.customerName', '-', '$zone.shortCode', '-', '$costCenter.costCenter'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'projectGroupName': 1, 
                                            '_id': 0
                                        }
                                    }
                                ], 
                                'as': 'projectGroupData'
                            }
                        }, {
                            '$unwind': '$projectGroupData'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$projectGroupData'
                            }
                        }
                    ], 
                    'as': 'projectGroupName'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'createdBy', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$addFields': {
                                'ptwRequester': {
                                    '$concat': [
                                        '$empName', '(', '$empCode', ')'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'empName': 1, 
                                'ptwRequester': 1
                            }
                        }
                    ], 
                    'as': 'createdByData'
                }
            }, {
                '$addFields': {
                    'projectGroupName': {
                        '$arrayElemAt': [
                            '$projectGroupName.projectGroupName', 0
                        ]
                    }, 
                    'createdBy': {
                        '$arrayElemAt': [
                            '$createdByData.ptwRequester', 0
                        ]
                    }, 
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'mileStoneId': {
                        '$toString': '$mileStoneId'
                    },
                    #         'createdTime': {
                    #     '$dateToString': {
                    #         'format': '%Y-%m-%d', 
                    #         'date': {
                    #             '$toDate': '$createdAt'
                    #         }
                    #     }
                    # }
                }
            }, {
                '$project': {
                    'createdByData': 0
                }
            }
        ]
    if request.method == 'GET':
        if not type:
            return respond({
                    'show':True,
                    'status': 400,
                    'text': 'Approver Type is required',
                    'icon': 'error'
                })
        if(type == 'l1Approver'):
            aggr[0]['$match'] = {
                'L1-Approver.approverId': userId,
                'status': 'Submitted'
            }
        else:
            aggr[0]['$match'] =  {
                'L2-Approver.approverId': userId,
                'status': 'L1-Approved'
            }
        finalAgg=aggr
        if startDate != None and endDate  != None:
            finalAgg=dateFilter(startDate,endDate,aggr)
        finalAgg+=apireq.commonarra
        if request.args.get('page') and request.args.get('limit'):
            finalAgg += apireq.args_pagination(request.args)
        data = cmo.finding_aggregate("ptwRaiseTicket" , finalAgg)
        return respond(data)
    
    if request.method == "POST":
        status = request.json.get("status")
        type = request.json.get("ApproverType")
        allData = request.get_json()
        if len(status):
            aggr = aggr + [
                {
                    '$match': {
                        'status': {
                            "$in": status
                        }
                    }
                }
        ]
        if(type == 'l1Approver'):
            aggr[0]['$match'] = {
                'L1-Approver.approverId': userId,
            }
        else:
            aggr[0]['$match'] =  {
                'L2-Approver.approverId': userId,
            }
        # aggr+=apireq.commonarra
        # if request.args.get('page') and request.args.get('limit'):
        #     aggr += apireq.args_pagination(request.args)
        data = cmo.finding_aggregate("ptwRaiseTicket" , aggr)
        return respond(data)


@ptw_blueprint.route("/submit/rejection/<mileStoneId>",methods=["PATCH"])
@token_required
def ptwreject(current_user, mileStoneId):
    allData = request.get_json()
    ApproverType = allData.get("ApproverType")
    ptwNumber = request.args.get('ptwNumber')
    updateBy = {
        "mileStoneId":mileStoneId,
        'ptwNumber':ptwNumber
    }
    arra=[
        {
            '$match': {
                'mileStoneId': mileStoneId,
                'ptwNumber':ptwNumber
            }
        }, {
            '$addFields': {
                'createdByObjId': {
                    '$toObjectId': '$createdBy'
                }
            }
        }, {
            '$lookup': {
                'from': 'userRegister', 
                'localField': 'createdByObjId', 
                'foreignField': '_id', 
                'as': 'userData'
            }
        }, {
            '$addFields': {
                'userEmail': {
                    '$arrayElemAt': [
                        '$userData.email', 0
                    ]
                }, 
                'userEmpName': {
                    '$arrayElemAt': [
                        '$userData.empName', 0
                    ]
                }
            }
        }, {
            '$project': {
                'createdByObjId': 0, 
                'userData': 0
            }
        }
    ]
    responseGet = cmo.finding_aggregate("ptwRaiseTicket", arra)['data']
    updateData = {}
    if ApproverType == "L1-Approver":   
        updateData = {
            "isL1Rejected":True,  
            "status":"L1-Rejected", # allData['status']
            ApproverType: {
                        "approverId": current_user["userUniqueId"],
                        "actionAt":int(unique_ptwtimestamp()),
                    },
            "ptwUpdatedAt":int(unique_ptwtimestamp()),  
            "rejectionReason":allData['rejectionReason']    
        }
    elif ApproverType=="L2-Approver":   
        updateData = {  
                      "isL2Rejected":True,
                      "status":"L2-Rejected",# allData['status']
                      ApproverType: {
                          "approverId": current_user["userUniqueId"],
                          "actionAt":int(unique_ptwtimestamp()),
                          },
                      "ptwUpdatedAt":int(unique_ptwtimestamp()),
                      "rejectionReason":allData['rejectionReason']
                    }
    response = cmo.updating("ptwRaiseTicket", updateBy,updateData, False)
    ptwRaiseTicketLogs("Rejection", ApproverType, mileStoneId, current_user['userUniqueId'], request.method,updateData)
    if response:
            ptwData = {
                "status":allData.get("status",'Submitted') ,#submitted
                "userName":responseGet[0]['userEmpName'],# username
                "ptwNumber":responseGet[0]['ptwNumber']
            }
            cmailer.ptw_sendmail(to=[responseGet[0]['userEmail']],cc=[],subject="PMIS-PTW STATUS",message=cmt.ptwMailTempplate(ptwData))
            return respond(response)
    else :
        return respond({
            'show':True,
            'status': 400,
            'text': 'unable to send mail',
            'icon': 'error'
        })


@ptw_blueprint.route("/rtwsGetFormsData",methods=['GET'])
# @token_required
def rtwsGetFormsData():
    ptwNumber = request.args.get("ptwNumber")
    if not ptwNumber:
        return respond({
                'show':True,
                'status': 400,
                'text': 'PTW Number is Required',
                'icon': 'error'
            })
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'ptwNumber': ptwNumber
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'checklist': 1, 
                    'photo': 1, 
                    'riskassessment': 1, 
                    'teamdetails': 1, 
                    'ptwphoto': 1, 
                    'roadsafetychecklist2wheeler': 1, 
                    'ptwphoto4wheeler': 1, 
                    'roadsafetychecklist4wheeler': 1, 
                    'ptwphoto2wheeler': 1
                }
            }
        ]
        response = cmo.finding_aggregate("ptwRaiseTicket",arra)
        return respond(response)


# @ptw_blueprint.route("/regeneratePtw/<fileType>/<subform>",methods=["GET","POST","PATCH"])
@ptw_blueprint.route("/regeneratePtw/<fileType>/<subform>/<mileStoneId>", methods=["PATCH"])
@token_required
def regeneratePtw(current_user, fileType=None, subform=None, mileStoneId=None):
    if not mileStoneId:
        return respond({
                'show':True,
                'status': 400,
                'text': 'mileStoneId Unique Id is required',
                'icon': 'error'
            })

    aggr = [{'$match': {'mileStoneId': mileStoneId,'ptwDeleteStatus':{'$ne':1}}}]
    responseGet = cmo.finding_aggregate('ptwRaiseTicket', aggr)['data']

    if not responseGet:
        return respond({
                'show':True,
                'status': 400,
                'text': 'No Data Found',
                'icon': 'error'
            })

    updateBy = {'mileStoneId': mileStoneId,'ptwDeleteStatus':{'$ne':1}}
    allData = {}

    form_subforms = {"checklist", "riskassessment", "roadsafetychecklist2wheeler", "roadsafetychecklist4wheeler"}
    file_subforms = {"photo", "ptwphoto", "ptwphoto2wheeler", "ptwphoto4wheeler", "teamdetails"}

    base_url = request.host_url.rstrip("/").replace("http://", "https://")

    if subform in form_subforms:
        
        req_data = request.get_json()

        if not req_data or subform not in req_data:
            return respond({
                'show':True,
                'status': 400,
                'text': f"No JSON data or '{subform}' key missing",
                'icon': 'error'
            })
        allData = {subform: req_data[subform]}


    elif subform in file_subforms:
        
        skip_keys = {"mileStoneId", "siteId", "Milestone", "circle", "projectID", "customerName", "subProject", "projectuniqueId", "siteUid",'circleId'}
        for key in request.files:
            file = request.files.get(key)
            if file:
                saved = cform.singleFileSaverPtw(file, "", "", subform, fileType)
                if saved["status"] != 200:
                    return respond(saved)
                allData[key] = saved["msg"]
        for key in request.form:
            value = request.form.get(key)
            if value and value != "undefined" and key not in skip_keys:
                if value.startswith(base_url):
                    value = value.replace(base_url, "", 1)
                allData[key] = value

        allData = {subform: allData}


    else:
        return respond({
                'show':True,
                'status': 400,
                'text': f"Unsupported subform type '{subform}'",
                'icon': 'error'
            })

    ptwRaiseTicketLogs(fileType, subform, mileStoneId, current_user['userUniqueId'], request.method, allData)
    unset_data = {
            "rejectionReason": "",
            "ptwphoto2wheeler": "",
            "roadsafetychecklist4wheeler": "",
            "ptwphoto4wheeler": "",
            "roadsafetychecklist2wheeler": "",
            "ptwphoto": "",
            "teamdetails": "",
            "riskassessment": "",
            "photo": "",
            "L2-Approver":"",
            "L1-Approver":"",
        } if subform == "checklist" else {}

    updateProjectLog(fileType,subform,mileStoneId, current_user['userUniqueId'], request.method, allData,responseGet[0].get('siteUid'),responseGet[0].get('projectuniqueId'))
    response = cmo.updating("ptwRaiseTicket", updateBy, allData, False, unset=unset_data)
    response['operation_id'] = str(responseGet[0]['_id'])

    return respond(response)


# =====================export excel and pdf
@ptw_blueprint.route("/ptw_export", methods=["POST"])
@token_required
def PTWExport(current_user):
    try:
        ptwNumberArg = request.args.get("ptwNumber")
        ptwNumberArg = request.args.get("ptwNumber")
        exportType = request.args.get("exportType", "excel").lower()
        tableData = request.get_json()

        if not ptwNumberArg:
            return respond({
                'show':True,
                'status': 400,
                'text':  "Missing 'ptwNumber' parameter",
                'icon': 'error'
            })
            # return jsonify({"error": "Missing 'ptwNumber' parameter"}), 400
        
        if ptwNumberArg=='undefined':
            ptwNumberAgg = [{'$match': {'_id': ObjectId(tableData['rowData'].get('uniqueId'))}}]
        else:
            ptwNumberAgg = [{'$match': {'ptwNumber': ptwNumberArg}}]
        result = cmo.finding_aggregate("ptwRaiseTicket", ptwNumberAgg)
        
        if not result or not result.get("data"):
            return respond({
                'show':True,
                'status': 400,
                'text':  "No record found for given PTW number",
                'icon': 'error'
            })
        
        document = result["data"][0]
        keys = ['rowData','checklist', 'photo', 'riskassessment', 'teamdetails', 'ptwphoto','roadsafetychecklist2wheeler','ptwphoto4wheeler','roadsafetychecklist4wheeler','ptwphoto2wheeler','rejectionReason']
        
        if "rowData" in tableData and isinstance(tableData["rowData"], dict):
            for key in keys:
                tableData["rowData"].pop(key, None)  
        
        columns = []
        for item in tableData["columns"]:

            val = item.get('value')
            if val not in ['edit', 'delete']:
                columns.append(val)

        tableData["rowData"] = { k: v for k, v in tableData["rowData"].items() if k in columns }

        result["data"][0]["rowData"] =tableData["rowData"]

        if exportType == "pdf":
            return export_to_pdf(document, keys, ptwNumberArg)
        else:
            return export_to_excel(document, keys, ptwNumberArg)

    except Exception as e:
        traceback.print_exc()
        return respond({
                'show':True,
                'status': 500,
                'text':  "Something went wrong", "details": str(e),
                'icon': 'error'
            })


@ptw_blueprint.route("/ptw/backupLog",methods=["GET"])
@token_required
def backupLog(current_user):
    if(request.method=="GET"):
        arra = [
            
        #  {
        #     '$match': {
        #         'ptwNumber': {
        #             '$exists': True,
        #         }
        #     }
        #  },
            
            {
                '$addFields': {
                    'ssid': '$checklist.SSID', 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'currentStatus': '$status', 
                    'user': {
                        '$toObjectId': '$createdBy'
                    }, 
                    'l1Id': {
                        '$toObjectId': '$L1-Approver.approverId'
                    }, 
                    'milestoneUniqueId': {
                        '$toObjectId': '$mileStoneId'
                    }, 
                    'projectType': '$checklist.Project type', 
                    'activity': '$checklist.Activity', 
                    'ptwSubmissionDate': {
                        '$dateToString': {
                            'format': '%m/%d/%Y %H:%M', 
                            'date': {
                                '$toDate': '$ptwRaiseAt'
                            }
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'ptwMdb', 
                    'let': {
                        'projectType': '$checklist.Project type', 
                        'l1Id': '$l1Id'
                    }, 
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$projectTypeName', '$$projectType'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$employee', '$$l1Id'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'projectGroupResult'
                }
            }, {
                '$addFields': {
                    'projectGroup': {
                        '$arrayElemAt': [
                            '$projectGroupResults.projectGroup', 0
                        ]
                    }
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
                    'as': 'projectGroupResults'
                }
            }, {
                '$lookup': {
                    'from': 'userRegister', 
                    'localField': 'user', 
                    'foreignField': '_id', 
                    'as': 'userResult'
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'milestoneUniqueId', 
                    'foreignField': '_id', 
                    'as': 'milestoneResult'
                }
            }, {
                '$addFields': {
                    'ptwRequester': {
                        '$arrayElemAt': [
                            '$userResult.empName', 0
                        ]
                    }, 
                    'milestoneName': {
                        '$arrayElemAt': [
                            '$milestoneResult.Name', 0
                        ]
                    }, 
                    'projectGroupName': {
                        '$arrayElemAt': [
                            '$projectGroupResult.projectTypeName', 0
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'actionAtFinal': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$eq': [
                                            {
                                                '$ifNull': [
                                                    '$L2-Approver.actionAt', None
                                                ]
                                            }, None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$ifNull': [
                                                    '$L1-Approver.actionAt', None
                                                ]
                                            }, None
                                        ]
                                    }
                                ]
                            }, None, {
                                '$cond': [
                                    {
                                        '$ne': [
                                            {
                                                '$ifNull': [
                                                    '$L2-Approver.actionAt', None
                                                ]
                                            }, None
                                        ]
                                    }, '$L2-Approver.actionAt', {
                                        '$cond': [
                                            {
                                                '$ne': [
                                                    {
                                                        '$ifNull': [
                                                            '$L1-Approver.actionAt', None
                                                        ]
                                                    }, None
                                                ]
                                            }, '$L1-Approver.actionAt', None
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'approvedOrRejectionDate': {
                        '$dateToString': {
                            'format': '%m/%d/%Y %H:%M', 
                            'date': {
                                '$toDate': '$actionAtFinal'
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'ptwNumber': 1, 
                    'ptwRequester': 1, 
                    'milestoneName': 1, 
                    'siteId': 1, 
                    'ssid': 1, 
                    'uniqueId': 1, 
                    'projectGroupName': 1, 
                    'projectID': 1, 
                    'projectType': 1, 
                    'subProject': 1, 
                    'activity': 1, 
                    'ptwSubmissionDate': 1, 
                    'approvedOrRejectionDate': 1, 
                    'currentStatus': 1, 
                    '_id': 0
                }
            }
        ]
        
        arra+=apireq.commonarra
        print("thisis aggr",arra)
        if request.args.get('page') and request.args.get('limit'):
            arra = arra+apireq.args_pagination(request.args)
            
        response = cmo.finding_aggregate("ptwRaiseTicket",arra)
        return respond(response)


@ptw_blueprint.route('/ptwApproverLog/<id>',methods=["POST"])
def ptw_approver_log(id):
    if request.method == "POST":
        approverId = id
        aggregateData=[
            {
                '$match': {
                    'L1-Approver.approverId': approverId, 
                    'ptwNumber': {
                        '$exists': True
                    }
                }
            }, {
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
        response  =  cmo.finding_aggregate("ptwRaiseTicket",aggregateData)
        return respond(response)


@ptw_blueprint.route("/ptwFormData",methods=["POST","GET"])
# @token_required
def ptw_formData():
    argsData= request.args.get("ptwNumber")
    aggregateData=[
        {
            '$match': {
                'ptwNumber': argsData
            }
        }, {
            '$lookup': {
                'from': 'ptwProjectType', 
                'localField': 'formType', 
                'foreignField': 'fileType', 
                'pipeline': [
                                {
                                    '$project': {
                                        '_id': 0,
                                        'createdAt': 0,
                                    }
                                }
                            ], 
                'as': 'formQuestionResult'
            }
        }, {
            '$addFields': {
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }
    ]
    response = cmo.finding_aggregate("ptwRaiseTicket",aggregateData)
    if not response['data']:
         result = {'state':4, "status":200 ,'msg': 'No Data Found', 'data': []}
         return respond(result)
    data = response['data'][0]   
    

    form_question_result = data.get('formQuestionResult', [{}])[0] 
    root_keys = set(data.keys())
    form_keys = set(form_question_result.keys())

   
    common_keys = root_keys.intersection(form_keys)
    flowKeys = []
    for key in common_keys:
        flowKeys.append(key)

    flowKeys.sort()  
    final_output = {
        "formType": {key: form_question_result[key] for key in common_keys},
        "formData": {key: data[key] for key in common_keys},
        "flow":flowKeys,
        "formType":data.get('formType')
    }
    response['data']=final_output
    return respond(response)

# GET rejection Reason for mobile
@ptw_blueprint.route("/rejection",methods=['GET'])
@token_required
def ptw_rejctionReason(current_user):
    ptwNumber= request.args.get("ptwNumber")
    if not ptwNumber:
        return respond({
                'show':True,
                'status': 400,
                'text': 'Ptw Number is Required',
                'icon': 'error'
             })
    aggr = [
            {
                '$match': {
                    'ptwNumber': ptwNumber
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'rejectionReason': {
                        '$arrayToObject': {
                            '$filter': {
                                'input': {
                                    '$objectToArray': '$rejectionReason'
                                }, 
                                'as': 'reason', 
                                'cond': {
                                    '$and': [
                                        {
                                            '$ne': [
                                                '$$reason.v', None
                                            ]
                                        }, {
                                            '$ne': [
                                                '$$reason.v', ''
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        ]
    response = cmo.finding_aggregate('ptwRaiseTicket',aggr)
    return respond(response)

#web Rejection Reason
@ptw_blueprint.route("/web/rejection",methods=['GET'])
@token_required
def web_ptw_rejctionReason(current_user):
    ptwNumber= request.args.get("ptwNumber")
    if not ptwNumber:
        return respond({
                'show':True,
                'status': 400,
                'text': 'Ptw Number is Required',
                'icon': 'error'
             })
    aggr = [
        {
            '$match': {
                'ptwNumber': ptwNumber
            }
        }, {
            '$project': {
                '_id': 0, 
                'rejectionReason': {
                    '$map': {
                        'input': {
                            '$filter': {
                                'input': {
                                    '$objectToArray': '$rejectionReason'
                                }, 
                                'as': 'item', 
                                'cond': {
                                    '$and': [
                                        {
                                            '$ne': [
                                                '$$item.v', None
                                            ]
                                        }, {
                                            '$ne': [
                                                '$$item.v', ''
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, 
                        'as': 'filtered', 
                        'in': '$$filtered.v'
                    }
                }
            }
        }
    ]
    # print(aggr,"lksjgdhgfjkasljjhgdf")
    response = cmo.finding_aggregate('ptwRaiseTicket',aggr)
    return respond(response)
