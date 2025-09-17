from base import *
from common.config import *
import common.excel_write as excelWriteFunc
from datetime import datetime
import pytz
import pprint
airtel_blueprint = Blueprint('airtel_blueprint', __name__)

def new_current_time():
    current_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%d-%m-%Y")
    return current_time




def currentuser_circleId(empId):
    aggregation = [
        {
            '$match': {
                'empId': empId,
                "type":{
                    '$ne':"Partner"
                }
            }
        }, {
            '$unwind': {
                'path': '$projectIds'
            }
        }, {
            '$lookup': {
                'from': 'project', 
                'localField': 'projectIds', 
                'foreignField': '_id', 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'circleId': {
                    '$arrayElemAt': [
                        '$result.circle', 0
                    ]
                }
            }
        }, {
            '$lookup': {
                'from': 'airtelMappedCircle', 
                'localField': 'circleId', 
                'foreignField': 'mcomCircle', 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result'
            }
        }, {
            '$addFields': {
                'mcomCircleId': '$result.mcomCircle'
            }
        }, {
            '$group': {
                '_id': '$mcomCircleId'
            }
        },{
            '$match': {
                '_id': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                'mcomCircleId': '$_id', 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectAllocation",aggregation)['data']
    circleId = []
    if response:
        for i in response:
            circleId.append(i['mcomCircleId'])
    return circleId


def currentuser_circle(empId,customerId):
    aggregation = [
        {
            '$match': {
                'empId': empId
            }
        }, {
            '$unwind': {
                'path': '$projectIds'
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
            '$addFields': {
                'circleId': {
                    '$arrayElemAt': [
                        '$result.circle', 0
                    ]
                }, 
                'customerId': {
                    '$arrayElemAt': [
                        '$result.custId', 0
                    ]
                }
            }
        }, {
            '$match': {
                'customerId': customerId
            }
        }, {
            '$group': {
                '_id': '$circleId', 
                'circleId': {
                    '$first': '$circleId'
                }
            }
        }, {
            '$match': {
                'circleId': {
                    '$ne': None
                }
            }
        }, {
            '$addFields': {
                'circleId': {
                    '$toObjectId': '$circleId'
                }
            }
        }, {
            '$lookup': {
                'from': 'circle', 
                'localField': 'circleId', 
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
                'CircleName': {
                    '$arrayElemAt': [
                        '$result.circleCode', 0
                    ]
                }, 
                'uniqueId': '$_id', 
                '_id': 0
            }
        }, {
            '$sort':{
                'CircleName':1
            }
        }
    ]
    response = cmo.finding_aggregate("projectAllocation",aggregation)
    return response

    

def exceltodf(file_path,validate,rename):

    # na_values = [""]
    na_values = ["NA", "NaN", "null", None]

    newData = pd.read_excel(file_path,na_values=na_values,keep_default_na=False)
    newData.fillna('', inplace=True)
    newData = newData.replace('NaN',None)
    newData = newData.replace('',None)
    dataf = newData.map(lambda x: x.strip() if isinstance(x, str) else x)
    dataf.columns = dataf.columns.str.strip()



    listOfCol=dataf.columns

  

    miss_col=[]
    for i in validate:
        if(i not in listOfCol):
            miss_col.append(i)

    if(len(miss_col)>0):
        return {
            "status":400,
            "icon":"error",
            "msg":"The Columns is must required. The column names are..\n{}".format("\n".join(miss_col))
        }
    
        
    dataf.rename(columns=rename,inplace=True)

    return {
        "status":200,
        "data":dataf
    }




def  validate_and_fetch(data):
    paths = [
        ["Data"],
        ["Data", 0, "InputData"],
        ["Data", 0, "InputData", "Input_JSON"],
        ["Data", 0, "InputData", "Input_JSON", "Request_Ref_No"],
        ["Data", 0, "InputData", "Input_JSON", "Parent_SR_Number"],
        ["Data", 0, "InputData", "Input_JSON", "SiteDetails", "Circle"],
        ["Data", 0, "InputData", "Input_JSON", "SiteDetails", "Site ID"],
    ]
    """
    data  : dict or list
    paths : list of key/index paths (each path is a list of keys/indices)

    Returns: dict {path_string: {"exists": True/False, "value": actual_value_or_None}}
    """
    results = {}

    for path in paths:
        current = data
        ok = True

        for key in path:
            if isinstance(current, dict):
                if key in current:
                    current = current[key]
                else:
                    ok = False
                    current = None
                    break
            elif isinstance(current, list) and isinstance(key, int):
                if 0 <= key < len(current):
                    current = current[key]
                else:
                    ok = False
                    current = None
                    break
            else:
                ok = False
                current = None
                break

        results[".".join(map(str, path))] = {"exists": ok, "value": current if ok else None}

    return results


msgSet = {

    "Data":"Data Array is not found in Json Data(Root)",
    "Data.0.InputData":"InputData Object is not found in Data Array",
    "Data.0.InputData.Input_JSON":"Input_JSON Object is not found in InputData Object",
    "Data.0.InputData.Input_JSON.Request_Ref_No":"SR Number (Request_Ref_No) is either missing in Input_JSON Object or has no value.",
    "Data.0.InputData.Input_JSON.Parent_SR_Number":"Parent_SR_Number is either missing in Input_JSON Object or has no value",
    "Data.0.InputData.Input_JSON.SiteDetails.Circle":"Circle is either missing in SiteDeatils Object or has no value",
    "Data.0.InputData.Input_JSON.SiteDetails.Site ID":"Site ID is either missing in SiteDeatils Object or has no value"

}


def validate_fields(data):
    required_fields = [
        ("Data.0.InputData.Input_JSON.Request_Ref_No", data["Data"][0]["InputData"]["Input_JSON"].get("Request_Ref_No")),
        ("Data.0.InputData.Input_JSON.Parent_SR_Number", data["Data"][0]["InputData"]["Input_JSON"].get("Parent_SR_Number")),
        ("Data.0.InputData.Input_JSON.SiteDetails.Circle", data["Data"][0]["InputData"]["Input_JSON"]["SiteDetails"].get("Circle")),
        ("Data.0.InputData.Input_JSON.SiteDetails.Site ID", data["Data"][0]["InputData"]["Input_JSON"]["SiteDetails"].get("Site ID")),
    ]
    
    results = {}
    for field_path, value in required_fields:
        results[field_path] = {
            "exists": value is not None,
            "valid": bool(value) if value is not None else False,
            "value": value
        }
    
    return results

@airtel_blueprint.route("/airtel/commonUploadFile", methods=["POST"])
@airtel_blueprint.route("/airtel/commonUploadFile/<id>", methods=["POST"])
def airtel_common_upload_file():
    if request.method == "POST":
        uploadedFile = request.files.get("uploadedFile[]")
        fileType = request.form.get("fileType")
        allData = {}
        supportFile = ["xls","xlsx"]
        pathing = cform.singleFileSaver(uploadedFile, "", supportFile)
        if pathing["status"] == 200:
            allData["filePath"] = pathing["msg"]
            
        elif pathing["status"] == 422:
            return respond(pathing)
        
        fileTeamCheck = {
            "RFAI_offered":{
                "validate":{
                    "Circle",
                    "Site ID",
                    "TOCO Site ID",
                    "TOCO Name",
                    "SR Type",
                    "Product Name",
                    "Upgrade Type",
                    "SR No",
                    "SR Current Status",
                    "SR Submission Date",
                    "SR Current Status Date",
                    "SR Created by",
                    "SR Entry Type",
                    "SP DATE",
                    "SO DATE",
                    "RFAI Date",
                    "RFAI Status",
                    "RFAI Acceptance Date",
                    "RFAI Rejection Date",
                    "RFAI History",
                    "RFAI Survey Status",
                    "RFAI Survey Date",
                }
            },
            "RFAISurveyChecklist":{
                "validate":{
                    "Field Name",
                    "Field Type",
                    "Mandatory",
                    "Data Type",
                    "DropDown Values"
                },
                "rename":{
                    "Field Name":"fieldName",
                    "Field Type":"fieldType",
                    "Mandatory":"required",
                    "Data Type":"dataType",
                    "DropDown Values":"dropdownValues" 
                }
            }
        }

        if fileType in fileTeamCheck:
            file_path = os.path.join(os.getcwd(), allData["filePath"])
            validate = fileTeamCheck[fileType]["validate"]
            rename = fileTeamCheck[fileType]["rename"]
            data = exceltodf(file_path,validate,rename)
            if data['status'] != 400:
                excelData = data["data"]
                if excelData.empty:
                    return respond(
                        {
                            "status": 400,
                            "icon": "error",
                            "msg": "The uploaded file is empty. Please check the file and try again.",
                        }
                    )
                if fileType == "RFAI_offered":
                    # print(excelData[['SR Submission Date',"SR Current Status Date","SP DATE","SO DATE","RFAI Date","RFAI Acceptance Date","RFAI Rejection Date","RFAI Survey Date"]])
                    aggregation = [
                        {
                            '$project': {
                                'Circle': '$airtelCircle', 
                                'circleId': '$mcomCircle', 
                                '_id': 0
                            }
                        }
                    ]
                    response = cmo.finding_aggregate("airtelMappedCircle",aggregation)['data']
                    if response:
                        df = pd.DataFrame(response)
                        excelData = excelData.merge(df,on="Circle",how="left")
                        excelData = excelData.replace(np.nan, "", regex=True)
                    dictData = excelData.to_dict(orient="records")
                    for i in dictData:
                        response = cmo.insertion("RFAIOffered",i)
                    return respond({
                        'status':200,
                        "icon":"success",
                        "msg":"Data Uploaded Successfully"
                    })
                
                elif fileType == "RFAISurveyChecklist":
                    dictData = excelData.to_dict(orient="records")
                    for i in dictData:
                        updateBy = {
                            'fieldName':i['fieldName'],
                            "fieldType":i['fieldType']
                        }
                        cmo.updating("surveyChecklist",updateBy,i,True)
                    return respond({
                        'status':200,
                        "icon":"success",
                        "msg":"Data updated successfully"
                    })

            else:
                return respond(data)



@airtel_blueprint.route('/airtel/mappedCircle', methods=['GET', "POST"])
@airtel_blueprint.route('/airtel/mappedCircle/<id>', methods=["DELETE"])
@token_required
def aurtel_mapped_circle(current_user, id=None):

    if request.method == "GET":
        aggregation = [
            {
                '$addFields': {
                    'mcomCircle': {
                        '$toObjectId': '$mcomCircle'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'circle', 
                    'localField': 'mcomCircle', 
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
                    'mcomCircle': {
                        '$arrayElemAt': [
                            '$result.circleCode', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'airtelCircle': 1, 
                    'mcomCircle': 1, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    '_id': 0
                }
            }, {
                '$sort':{
                    'airtelCircle':1
                }
            }
        ]
        aggregation = aggregation + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("airtelMappedCircle", aggregation)
        return respond(response)

    elif request.method == "POST":
        if id == None:
            allData = request.get_json()
            aggregation = [
                {
                    '$match': {
                        '$expr': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$airtelCircle', allData['airtelCircle']
                                    ]
                                }, {
                                    '$eq': [
                                        '$mcomCircle', allData['mcomCircle']
                                    ]
                                }
                            ]
                        }
                    }
                }
            ]
            response = cmo.finding_aggregate("airtelMappedCircle", aggregation)
            if len(response['data']):
                return {
                    "status": 400,
                    "msg": "Airtel Circle or MCOM Circle already exists in the database. Please choose another.",
                    "icon": "error",
                }, 400

            response = cmo.insertion("airtelMappedCircle", allData)
            return respond(response)

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("airtelMappedCircle", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({'msg': 'Please provide valid Unique Id'})
        
@airtel_blueprint.route('/airtel/CircleList', methods=['GET'])
def airtle_circle_list():
    aggregation = [
        {
            '$match':{
                'customer':ObjectId("667d593927f39f1ac03d7863")
            }
        }, {
            '$project':{
                'circleCode':1,
                "uniqueId":{
                    '$toString':"$_id"
                },
                "_id":0
            }
        }
    ]
    response = cmo.finding_aggregate("circle",aggregation)
    return respond(response)


@airtel_blueprint.route('/export/airtelMappedCircle', methods=['GET'])
def export_airtel_mapped_circle():
    aggregation = [
        {
            '$addFields': {
                'mcomCircle': {
                    '$toObjectId': '$mcomCircle'
                }
            }
        }, {
            '$lookup': {
                'from': 'circle', 
                'localField': 'mcomCircle', 
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
                'mcomCircle': {
                    '$arrayElemAt': [
                        '$result.circleCode', 0
                    ]
                }
            }
        }, {
            '$project': {
                "Airtel Circle":'$airtelCircle', 
               "MCOM Circle":'$mcomCircle',  
                '_id': 0
            }
        }, {
            '$sort':{
                'airtelCircle':1
            }
        }
    ]
    response = cmo.finding_aggregate("airtelMappedCircle",aggregation)['data']
    if response:
        df = pd.DataFrame(response)
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Airtel_MCOM_Circle", "Airtel_MCOM_Circle")
        return send_file(fullPath)
    else:
        df = pd.DataFrame()
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_Airtel_MCOM_Circle", "Airtel_MCOM_Circle")
        return send_file(fullPath)




@airtel_blueprint.route('/airtel/RFAIOffered', methods=['GET',"PATCH"])
@token_required
def airtel_rfai_offered(current_user):
    if request.method == "GET":
        aggregation = []

        if current_user['roleName']!= "Admin":
            aggregation = [
                {
                    '$match':{
                        'circleId':{
                            '$in': currentuser_circleId(current_user['userUniqueId'])
                        }
                    }
                }
            ]
        
        if (request.args.get('searvhView') != None and request.args.get("searvhView") != 'undefined'):
            searchView = request.args.get('searvhView').strip()
            aggregation = aggregation + [
                {
                    '$match': {
                        '$or': [
                            {
                                'Sr Number': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'Circle': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'Site ID': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'TOCO Site ID': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'TOCO Name': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'SR Type': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }, {
                                'status': {
                                    '$regex': searchView,
                                    '$options': 'i'
                                }
                            }
                        ]
                    }
                }
            ]
        aggregation = aggregation + [
            {
                '$lookup': {
                    'from': 'RFAISurveyAllocation', 
                    'localField': '_id', 
                    'foreignField': 'parentId',  
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$_id'
                    },
                    'assignDate':{
                        '$arrayElemAt':['$result.assignDate',0]
                    },
                    'assignerId':{
                        '$arrayElemAt':['$result.assignerId',0]
                    },
                    'typeAssigned':{
                        '$arrayElemAt':['$result.typeAssigned',0]
                    },
                    "SiteDetails":{
                        "$ifNull":['$SiteDetails',{}]
                    },
                    "BTS":{
                        "$ifNull":['$BTS',{}]
                    },
                    "Antenna":{
                        "$ifNull":['$Antenna',{}]
                    },
                    "BBU Model":{
                        "$ifNull":['$BBU Model',{}]
                    },
                    "Media Type":{
                        "$ifNull":['$Media Type',{}]
                    },
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
                    '_id': 0,
                    "assignerId":0,
                    "result":0,
                    "fullData":0
                }
            }
        ]
        aggregation = aggregation + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("RFAIOffered",aggregation)
        return respond(response)
    
    if request.method == "PATCH":

        allData = request.get_json()
        allData['assignerId'] = allData['assignerId'].split(",")

        assignerId = []
        for i in allData['assignerId']:
            assignerId.append(ObjectId(i))


        finalData = {
            "assignerId":assignerId,
            "typeAssigned":allData['assigningTo'],
            "assignDate":new_current_time()
        }
        updateBy = {
            'parentId':ObjectId(allData['uid'])
        }
        response = cmo.updating("RFAISurveyAllocation",updateBy,finalData,True)
        cmo.updating("RFAIOffered",{'_id':ObjectId(allData['uid'])},{"status":"Allocated"},False)
        return respond(response)


@airtel_blueprint.route('/export/airtelPlannedData', methods=['GET'])
@airtel_blueprint.route('/export/airtelPlannedData/<id>', methods=['GET'])
@token_required
def export_airtel_planned_Data(current_user,id=None):
    if request.method == "GET":
        # aggregation = [
        #     {
        #         '$match': {
        #             '_id': ObjectId(id)
        #         }
        #     }, {
        #         '$lookup': {
        #             'from': 'RFAISurveyAllocation', 
        #             'localField': '_id', 
        #             'foreignField': 'parentId', 
        #             'pipeline': [
        #                 {
        #                     '$lookup': {
        #                         'from': 'userRegister', 
        #                         'localField': 'assignerId', 
        #                         'foreignField': '_id', 
        #                         'pipeline': [
        #                             {
        #                                 '$match': {
        #                                     'deleteStatus': {
        #                                         '$ne': 1
        #                                     }
        #                                 }
        #                             }, {
        #                                 '$addFields': {
        #                                     'empCode': {
        #                                         '$ifNull': [
        #                                             '$empCode', '$vendorCode'
        #                                         ]
        #                                     }, 
        #                                     'empName': {
        #                                         '$ifNull': [
        #                                             '$empName', '$vendorName'
        #                                         ]
        #                                     }
        #                                 }
        #                             }, {
        #                                 '$addFields': {
        #                                     'empCode': {
        #                                         '$toString': '$empCode'
        #                                     }, 
        #                                     'empName': {
        #                                         '$toString': '$empName'
        #                                     }
        #                                 }
        #                             }, {
        #                                 '$addFields': {
        #                                     'empName': {
        #                                         '$concat': [
        #                                             '$empName', '(', '$empCode', ')'
        #                                         ]
        #                                     }
        #                                 }
        #                             }
        #                         ], 
        #                         'as': 'result'
        #                     }
        #                 }, {
        #                     '$addFields': {
        #                         'Task Owner': '$result.empName'
        #                     }
        #                 }, {
        #                     '$addFields': {
        #                         'Assign to': {
        #                             '$reduce': {
        #                                 'input': '$Task Owner', 
        #                                 'initialValue': '', 
        #                                 'in': {
        #                                     '$concat': [
        #                                         '$$value', {
        #                                             '$cond': [
        #                                                 {
        #                                                     '$eq': [
        #                                                         '$$value', ''
        #                                                     ]
        #                                                 }, '', ','
        #                                             ]
        #                                         }, '$$this'
        #                                     ]
        #                                 }
        #                             }
        #                         }
        #                     }
        #                 }
        #             ], 
        #             'as': 'result'
        #         }
        #     }, {
        #         '$project': {
        #             'Sr Number': 1, 
        #             'Request_Ref_No': 1, 
        #             'Circle': 1, 
        #             'Site ID': 1, 
        #             'TOCO Site ID': 1, 
        #             'TOCO Name': 1, 
        #             'SR Type': 1, 
        #             'Assign Date': {
        #                 '$arrayElemAt': [
        #                     '$result.assignDate', 0
        #                 ]
        #             }, 
        #             'Assign to': {
        #                 '$arrayElemAt': [
        #                     '$result.Assign to', 0
        #                 ]
        #             }, 
        #             'Assign Type': {
        #                 '$arrayElemAt': [
        #                     '$result.typeAssigned', 0
        #                 ]
        #             }, 
        #             'Acceptance Date': '', 
        #             'Rejection Date': '', 
        #             'Status': '$status', 
        #             'History': '', 
        #             '_id': 0
        #         }
        #     }
        # ]
        aggregation = []
        if id!=None:
            aggregation = aggregation + [
                {
                    '$match': {
                        '_id': ObjectId(id)
                    }
                }
            ]
        aggregation = aggregation + [
            {
                '$lookup': {
                    'from': 'RFAISurveyAllocation', 
                    'localField': '_id', 
                    'foreignField': 'parentId', 
                    'pipeline': [
                        {
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
                                        '$addFields': {
                                            'empCode': {
                                                '$ifNull': [
                                                    '$empCode', '$vendorCode'
                                                ]
                                            }, 
                                            'empName': {
                                                '$ifNull': [
                                                    '$empName', '$vendorName'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$addFields': {
                                            'empCode': {
                                                '$toString': '$empCode'
                                            }, 
                                            'empName': {
                                                '$toString': '$empName'
                                            }
                                        }
                                    }, {
                                        '$addFields': {
                                            'empName': {
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
                                'Task Owner': '$result.empName'
                            }
                        }, {
                            '$addFields': {
                                'Assign to': {
                                    '$reduce': {
                                        'input': '$Task Owner', 
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
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'basicDetails': {
                        'Sr Number': '$Sr Number', 
                        'Request_Ref_No': '$Request_Ref_No', 
                        'Circle': '$Circle', 
                        'Site ID': '$Site ID', 
                        'TOCO Site ID': '$TOCO Site ID', 
                        'TOCO Name': '$TOCO Name', 
                        'SR Type': '$SR Type', 
                        'Assign Date': {
                            '$arrayElemAt': [
                                '$result.assignDate', 0
                            ]
                        }, 
                        'Assign to': {
                            '$arrayElemAt': [
                                '$result.Assign to', 0
                            ]
                        }, 
                        'Assign Type': {
                            '$arrayElemAt': [
                                '$result.typeAssigned', 0
                            ]
                        }, 
                        'Acceptance Date': '', 
                        'Rejection Date': '', 
                        'Status': '$status', 
                        'History': ''
                    }
                }
            }, 
            {
                '$project': {
                    'data1': '$basicDetails', 
                    'data2': '$SiteDetails', 
                    'data3': '$BTS', 
                    'data4': '$Antenna', 
                    'data5': '$BBU Model', 
                    'data6': '$Media Type', 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("RFAIOffered",aggregation)['data']
        df = pd.DataFrame()
        if response:
            rows = []
            for item in response:
                row = {}
                row.update(item.get("data1", {}))
                for k, v in item.get("data2", {}).items():
                    if k == "Media Type":
                        row["SiteDetails.Media Type"] = v
                    else:
                        row[k] = v
                row.update(item.get("data3", {}))
                row.update(item.get("data4", {}))
                row.update(item.get("data5", {}))
                row.update(item.get("data6", {}))
                rows.append(row)
            df = pd.DataFrame(rows)
        fullPath = excelWriteFunc.excelFileWriter(df, "Export_PlannedData", "PlannedData")
        return send_file(fullPath)




@airtel_blueprint.route('/airtel/projectAllocationList/<circleId>', methods=['GET'])
@token_required
def airtel_project_allocation_list(current_user,circleId=None):
    if request.method == "GET":
        aggregation = [
            {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectIds', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            '$match': {
                                'circle': circleId,
                                "deleteStatus":{'$ne':1},
                                "status":"Active"
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
                                'deleteStatus': {
                                    '$ne': 1
                                }, 
                                'status': 'Active'
                            }
                        }, {
                            '$addFields': {
                                'empName': {
                                    '$ifNull': [
                                        '$empName', '$vendorName'
                                    ]
                                }, 
                                'empCode': {
                                    '$ifNull': [
                                        '$empCode', '$vendorCode'
                                    ]
                                }
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
                                'emp': {
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
                '$match': {
                    'userResult': {
                        '$ne': []
                    }
                }
            }, {
                '$project': {
                    'id': {
                        '$toString': '$empId'
                    }, 
                    'name': {
                        '$arrayElemAt': [
                            '$userResult.emp', 0
                        ]
                    }, 
                    'type': 1, 
                    '_id': 0
                }
            }, {
                '$facet': {
                    'empDeatils': [
                        {
                            '$match': {
                                'type': {
                                    '$ne': 'Partner'
                                }
                            }
                        }
                    ], 
                    'vendorDetails': [
                        {
                            '$match': {
                                'type': {
                                    '$eq': 'Partner'
                                }
                            }
                        }, {
                            '$project': {
                                'type': 0
                            }
                        }
                    ]
                }
            }
        ]
        response = cmo.finding_aggregate("projectAllocation",aggregation)
        return respond(response)




@airtel_blueprint.route('/airtel/myRFAISurveyTask', methods=['GET'])
@token_required
def airtel_my_RFAI_Survey_Task(current_user):

    if request.method == "GET":

        # aggregation = [
        #     {
        #         '$match':{
        #             'assignerId':ObjectId(current_user['userUniqueId'])
        #         }
        #     }, {
        #         '$addFields':{
        #             'uniqueId':{
        #                 '$toString':"$_id"
        #             }
        #         }

        #     }, {
        #         '$project':{
        #             'assignerId':0,
        #             "_id":0
        #         }
        #     }
        # ]
        aggregation = [
            {
                '$match': {
                    'assignerId':ObjectId(current_user['userUniqueId'])
                }
            }, {
                '$lookup': {
                    'from': 'RFAIOffered', 
                    'let': { 'pid': '$parentId' },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': { '$eq': ['$_id', '$$pid'] }
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
                '$addFields': {
                    'result.allocationuid': {
                        '$toString': '$_id'
                    }, 
                    'result.assignDate': '$assignDate', 
                    'result.uniqueId': {
                        '$toString': '$result._id'
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$result'
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        if (request.args.get('searvhView') != None and request.args.get("searvhView") != 'undefined'):
            searchView = request.args.get('searvhView').strip()
            search_stage   =  {
                '$match': {
                    '$or': [
                        {
                            'Sr Number': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }, {
                            'Circle': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }, {
                            'Site ID': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }, {
                            'TOCO Site ID': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }, {
                            'TOCO Name': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }, {
                            'SR Type': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }, {
                            'status': {
                                '$regex': searchView,
                                '$options': 'i'
                            }
                        }
                    ]
                }
            }
            aggregation[1]['$lookup']['pipeline'].append(search_stage)
        aggregation = aggregation + apireq.commonarra + apireq.args_pagination(request.args)
        # response = cmo.finding_aggregate("RFAIOffered",aggregation)
        response = cmo.finding_aggregate("RFAISurveyAllocation",aggregation)
        return respond(response)
    

@airtel_blueprint.route('/airtel/approverList', methods=['GET'])
@airtel_blueprint.route('/airtel/approverList/<id>', methods=['POST'])
@token_required
def airtel_RFAI_Survey_Approver_List(current_user,id=None):

    if request.method == "GET":
        aggregation = [
            {
                '$match': {
                    'type': {
                        '$ne': 'Partner'
                    }, 
                    'status': 'Active',
                    'deleteStatus':{'$ne':1}
                }
            }, {
                '$project': {
                    'empName': {
                        '$toString': '$empName'
                    }, 
                    'empCode': {
                        '$toString': '$empCode'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'userRole': 1
                }
            }, {
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
                    'roleName': {
                        '$ne': 'Field Resource'
                    }
                }
            }, {
                '$project': {
                    'emp': {
                        '$concat': [
                            '$empName', '(', '$empCode', ')'
                        ]
                    }, 
                    'uniqueId': 1, 
                    'roleName': 1, 
                    '_id': 0
                }
            }, {
                '$lookup': {
                    'from': 'RFAISurveyApprover', 
                    'localField': 'uniqueId', 
                    'foreignField': 'empId', 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'circle': {
                        '$arrayElemAt': [
                            '$result.circleId', 0
                        ]
                    }, 
                    'customer': {
                        '$arrayElemAt': [
                            '$result.customerId', 0
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'circle': {
                        '$toObjectId': '$circle'
                    }, 
                    'customer': {
                        '$toObjectId': '$customer'
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
                    'as': 'cresult'
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
                    'as': 'cusresult'
                }
            }, {
                '$addFields': {
                    'circleName': {
                        '$arrayElemAt': [
                            '$cresult.circleCode', 0
                        ]
                    }, 
                    'customerName': {
                        '$arrayElemAt': [
                            '$cusresult.customerName', 0
                        ]
                    }, 
                    'circle': {
                        '$toString': '$circle'
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }
                }
            }, {
                '$project': {
                    'result': 0, 
                    'cresult': 0, 
                    'cusresult': 0
                }
            }
        ]
        aggregation = aggregation + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("userRegister", aggregation)
        return respond(response)
    
    if request.method == "POST":
        allData = request.get_json()
        if not all(key in allData for key in ["empId", "customer", "circle"]):
            return respond({
                'status':400,
                "icon":"error",
                "msg":"Something went wrong please reload the page and Try Again"
            })
        updateData = {
            "empId":allData['empId'],
            "customerId":allData['customer'],
            "circleId":allData['circle']
        }
        updateBy = {
            'empId':allData['empId']
        }
        response = cmo.updating("RFAISurveyApprover",updateBy,updateData,True)
        return respond(response)


@airtel_blueprint.route('/airtel/currentUserCircleList/<customerId>/<empId>', methods=['GET'])
@token_required
def airtel_currentUser_circleList(current_user,customerId=None,empId=None):
    if request.method == "GET":
        return respond(currentuser_circle(empId,customerId))



@airtel_blueprint.route('/airtel/getOnePlanningData/<id>',methods=['GET'])
@token_required
def airte_get_one_planningdata(current_user,id=None):
    if request.method == "GET":
        aggregation = [
            {
                '$match': {
                    '_id': ObjectId(id)
                }
            }, {
                '$project': {
                    'SiteDetails': {
                        '$ifNull': [
                            '$SiteDetails', {}
                        ]
                    }, 
                    'BTS': {
                        '$ifNull': [
                            '$BTS', {}
                        ]
                    }, 
                    'Antenna': {
                        '$ifNull': [
                            '$Antenna', {}
                        ]
                    }, 
                    'BBU Model': {
                        '$ifNull': [
                            '$BBU Model', {}
                        ]
                    }, 
                    'Media Type': {
                        '$ifNull': [
                            '$Media Type', {}
                        ]
                    }, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("RFAIOffered",aggregation)
        return respond(response)



@airtel_blueprint.route('/airtel/surveyChecklist', methods=['GET', "POST"])
@airtel_blueprint.route('/airtel/surveyChecklist/<id>', methods=["DELETE"])
@token_required
def airtel_survey_checklist(current_user, id=None):

    if request.method == "GET":
        aggregation = [
            {
                '$addFields':{
                    'uniqueId':{
                        '$toString':"$_id"
                    }
                }
            }, {
                '$project':{
                    '_id':0
                }
            }
        ]
        aggregation = aggregation + apireq.commonarra + apireq.args_pagination(request.args)
        response = cmo.finding_aggregate("surveyChecklist", aggregation)
        return respond(response)

    elif request.method == "POST":
        allData = request.get_json()
        allData['fieldName'] = allData['fieldName'].strip()
        # aggregation = [
        #     {
        #         '$match': {
        #             "fieldName":allData['fieldName'],
        #             "fieldType":allData['fieldType']
        #         }
        #     }
        # ]
        # response = cmo.finding_aggregate("surveyChecklist", aggregation)
        # if len(response['data']):
        #     return respond({
        #         "status": 400,
        #         "msg": "Field Name or Field Type already exists in the database.",
        #         "icon": "error",
        #     })
        updateBy = {
            "fieldName":allData['fieldName'],
            "fieldType":allData['fieldType']
        }
        response = cmo.updating("surveyChecklist",updateBy,allData,True)
        return respond(response)

    elif request.method == "DELETE":
        if id != None:
            response = cmo.deleting("surveyChecklist", id, current_user['userUniqueId'])
            return respond(response)
        else:
            return jsonify({'msg': 'Please provide valid Unique Id'})




@airtel_blueprint.route('/workflow')
def airtel_workFlow():
    allData = {
        "Data": [
            {
                "Skipped": False,
                "InputData": {
                    "Input_JSON": {
                        "Request_Ref_No": "AIR66891_MobileComm_1",
                        "Parent_SR_Number":"AIR66891",
                        "SiteDetails": {
                            "Circle": "KK",
                            "Site ID": "CUST-004",
                            "Site Name": "CUST-004",
                            "Technology": "4G",
                            "Town/City Name": "Delhi",
                            "Cluster": "bsb",
                            "Site Address": "Delhi",
                            "IP Name": "Indus",
                            "IP ID": "IN-123459",
                            "Latitude": "26.2312",
                            "Longitude": "76.5121",
                            "Type of Site": "Sharing",
                            "Type of Sharing": "Sharing",
                            "Site Type": "Pole",
                            "BTS Cabinate Type": "Outdoor",
                            "Building Ht": "10",
                            "Tower Ht": "20",
                            "OEM": "Nokia",
                            "Media Type": "Other"
                        },
                        "BTS": {
                            "Existing enodeB": "NA",
                            "No of Existing BTS/enodeB": "NA",
                            "Technologies Running on Existing BTS/enodeB": "NA"
                        },
                        "Antenna": {
                            "No of MW mount available": 3,
                            "Height of MW Antenna mount": {
                                "Planned": [2, 12, 30]
                            },
                            "Azimuth of MW Antenna mount": {
                                "Planned": [0, 30, 60]
                            },
                            "New GSM mount height": {
                                "Planned": [12, 45, 40]
                            },
                            "New GSM mount azimuth": {
                                "Planned": [0, 60, 120]
                            },
                            "No of RRU": {
                                "Planned": [2, 3, 2,]
                            },
                            "RAN Type": {
                                "Planned": ["Antenna", "AAU", "NA"]
                            }
                        },
                        "BBU Model": ["RBS 1130", "Small cell Flexi Zone (4G_1C Stand Alone @ 1800 Mhz)"],
                        "Media Type": ["MW", "Fiber"]
                    }
                },
                # "DigimopOperationId": "id__103__958__2392__Trigger-iDeploy-NDPd-Update__Activity_03d0qwh"
            }
        ]
    }

    results = validate_and_fetch(allData)

    first_missing = None
    for path, info in results.items():
        if not info["exists"]:
            first_missing = path
            break
    if first_missing:
        # return jsonify({
        #     'Status':False,
        #     "Message":msgSet[first_missing]
        # }),400
        print("")

    results = validate_fields(allData)
    notFound = []
    for path, info in results.items():
        if not info["valid"]:
            notFound.append(path)

    if notFound:
        messages = [msgSet.get(field, field) for field in notFound]
        print(", ".join(messages),"================")
        # return jsonify({
        #     'Status':False,
        #     "Message":", ".join(messages)
        # }),400
 
    updateData = {
        "Sr Number": allData['Data'][0]['InputData']['Input_JSON']['Parent_SR_Number'].strip(),
        "Request_Ref_No": allData['Data'][0]['InputData']['Input_JSON']['Request_Ref_No'].strip(),
        "Circle": allData['Data'][0]['InputData']['Input_JSON']['SiteDetails'].get("Circle").strip(),
        "Site ID": allData['Data'][0]['InputData']['Input_JSON']['SiteDetails'].get("Site ID"),
        "TOCO Site ID": allData['Data'][0]['InputData']['Input_JSON']['SiteDetails'].get("IP ID"),
        "TOCO Name": allData['Data'][0]['InputData']['Input_JSON']['SiteDetails'].get("IP Name"),
        "SR Type": allData['Data'][0]['InputData']['Input_JSON']['SiteDetails'].get("Type of Site"),
        "circleId":None,
        "status":"Open",
        "SiteDetails":allData['Data'][0]['InputData']['Input_JSON'].get('SiteDetails'),
        "BTS":allData['Data'][0]['InputData']['Input_JSON'].get('BTS'),
        "Antenna":{
            "No of MW mount available":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['No of MW mount available'],
            "Height of MW Antenna mount":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['Height of MW Antenna mount']['Planned'],
            "Azimuth of MW Antenna mount":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['Azimuth of MW Antenna mount']['Planned'],
            "New GSM mount height":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['New GSM mount height']['Planned'],
            "New GSM mount azimuth":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['New GSM mount azimuth']['Planned'],
            "No of RRU":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['No of RRU']['Planned'],
            "RAN Type":allData['Data'][0]['InputData']['Input_JSON'].get('Antenna')['RAN Type']['Planned'],
        },
        "BBU Model":{
            "BBU Model":allData['Data'][0]['InputData']['Input_JSON'].get('BBU Model')},
        "Media Type":{
            "Media Type":allData['Data'][0]['InputData']['Input_JSON'].get('Media Type')},
        "fullData":allData,
    }
    aggreagtion = [
        {
            '$match': {
                'airtelCircle': updateData['Circle']
            }
        }, {
            '$project': {
                'circleId': '$mcomCircle', 
                '_id': 0
            }
        }
    ]
    result = cmo.finding_aggregate("airtelMappedCircle",aggreagtion)['data']
    if result:
        updateData['circleId'] = result[0]['circleId']

    cmo.insertion("RFAIOffered",updateData)
    print("complete")

# airtel_workFlow()




@airtel_blueprint.route("/saurabhApi",methods=['GET',"POST"])
def sample_Function():
    if request.method == "GET":
        # file_path = os.path.join(os.getcwd(),"C:/Users/DELL/Downloads/DataFile_202533830- International Services study_Excel")
        # uploadedFile = request.files.get("file")
        # headerName = request.form.get("name")

        # headerName = headerName.split(",")

        # supportFile = ["xlsx"]
        # allData = {}
        # pathing = cform.singleFileSaver(uploadedFile, "", supportFile)
        # if pathing["status"] == 200:
        #     allData["filePath"] = pathing["msg"]
            
        # elif pathing["status"] == 422:
        #     return respond(pathing)


        headerName = request.args.get("name")

        print(headerName,"444")

        headerName = headerName.split(",")

        type = request.args.get("type")

        print(headerName,"445")

        allData = {}
        allData['filePath'] = "C:/Users/DELL/Desktop/US-Canada-Project/Backend_code/250824.xlsx"
        
        na_values = ["NA", "NaN", "null", None]
        newData = pd.read_excel(allData["filePath"],na_values=na_values,keep_default_na=False)
        newData.fillna('', inplace=True)
        newData = newData.replace('NaN',"")
        newData = newData.replace('',"")
        originalData = newData.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        originalData.columns = originalData.columns.str.strip()


        data1 = originalData[["record"]+headerName]


        print(data1,"data1")


        # data1["duplicate"] = (data1["Q1"].astype(str) + "_" + data1["Q2"].astype(str) + "_" + data1["Q3"].astype(str))
        if type != "uid":
            data1["duplicate"] = originalData[headerName].astype(str).agg("_".join, axis=1)
        else:
            data1["duplicate"] = originalData[headerName].bfill(axis=1).iloc[:,0]

        print(data1,"472")

        duplicate1 = data1[data1["duplicate"].duplicated(keep=False)]



        duplicate1 = duplicate1.sort_values(by="duplicate")


        print(duplicate1,"duplicate1")


        duplicate1["duplicate_record"] = (duplicate1.groupby("duplicate")["record"].transform(lambda x: ",".join(x.astype(str))))

        duplicate1["dulpicate_record_count"] = (
            duplicate1.groupby("duplicate_record")["record"]
                .transform("count")
        )

        print(duplicate1,"489")

        unique_df1 = duplicate1.drop_duplicates(subset=["duplicate_record", "dulpicate_record_count"])

        unique_df1 = unique_df1[["duplicate_record", "dulpicate_record_count"]]

        unique_df1.insert(0, "sr_no", range(1, len(unique_df1) + 1))


        print(unique_df1,"unique_df1")

        dataframes = [duplicate1,unique_df1]

        sheet_names = ["sheet1","sheet2"]

        tab_colors = [
                "#92d050", 
                "#00B0F0",
        ]

        fullPath = os.path.join(os.getcwd(), "downloadFile", "sample.xlsx")
        newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
        workbook = newExcelWriter.book

        header_format = workbook.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vtop',
                'bg_color': '#24292d', 
                'border':1,
                'border_color':'black',
                'color':'white'
            })
            
        cell_format = workbook.add_format({
            'align':'center',
            'valign':'vtop',
        })

        for df, sheet_name, color in zip(dataframes, sheet_names, tab_colors):
            df.to_excel(newExcelWriter, index=False, sheet_name=sheet_name)
            worksheet = newExcelWriter.sheets[sheet_name]
            worksheet.set_tab_color(color)
            worksheet.set_column(0, len(df.columns) - 1, 20, cell_format)

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

        newExcelWriter.close()
        return send_file(fullPath)



# sample_Function()
