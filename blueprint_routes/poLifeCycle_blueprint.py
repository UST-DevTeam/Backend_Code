from base import *
poLifeCycle_blueprint = Blueprint('poLifeCycle_blueprint', __name__)
from common.config import *
from blueprint_routes.currentuser_blueprint import projectId_str,projectGroup_str
from blueprint_routes.sample_blueprint import *
import datetime
import pytz
import calendar

def get_month_dates(month, year):
    _, last_day = calendar.monthrange(year, month)
    start_date = f"01/{month:02d}/{year}"
    last_date = f"{last_day:02d}/{month:02d}/{year}"
    
    return start_date, last_date



def current_date():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone('Asia/Kolkata') 
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_date = asia_now.strftime("%d-%m-%Y")
    return current_date

def current_month_year():
    utc_now = datetime.datetime.utcnow()
    asia_timezone = pytz.timezone('Asia/Kolkata') 
    asia_now = utc_now.replace(tzinfo=pytz.utc).astimezone(asia_timezone)
    current_month = asia_now.month
    current_year = asia_now.year
    return current_month,current_year

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

def projectGroup():
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
                'Customer':'$customer.customerName'
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
                'Customer':1,
                'costCenter':1
               
            }
        }
    ]
    response = cmo.finding_aggregate("projectGroup",arra)
    df = pd.DataFrame(response['data'])
    return df
            
def projectid():
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'projectId': 1, 
                'projectGroup': {
                    '$toString': '$projectGroup'
                }, 
                'projectuniqueId': {
                    '$toString': '$_id'
                },
                '_id':0
            }
        }
    ]
    response = cmo.finding_aggregate("project",arra) 
    df = pd.DataFrame(response['data'])
    return df

def subproject():
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'SubProjectId': {
                    '$toString': '$_id'
                }, 
                '_id': 0, 
                'projectType': '$projectType', 
                'subProject': '$subProject'
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)
    df = pd.DataFrame(response['data'])
    return df

def masterunitRate():
    arra = [
        {
            '$addFields': {
                'projectuniqueId': {
                    '$toString': '$project'
                }, 
                'SubProjectId': {
                    '$toString': '$subProject'
                }, 
                'band': {
                    '$toString': '$band'
                }, 
                'activity': {
                    '$toString': '$activity'
                }, 
                'itemCode01': {
                    '$toString': '$itemCode01'
                }, 
                'itemCode02': {
                    '$toString': '$itemCode02'
                }, 
                'itemCode03': {
                    '$toString': '$itemCode03'
                }, 
                'itemCode04': {
                    '$toString': '$itemCode04'
                }, 
                'itemCode05': {
                    '$toString': '$itemCode05'
                }, 
                'itemCode06': {
                    '$toString': '$itemCode06'
                }, 
                'itemCode07': {
                    '$toString': '$itemCode07'
                }
            }
        }, {
            '$addFields': {
                'BAND': {
                    '$ifNull': [
                        '$band', ''
                    ]
                }, 
                'ACTIVITY': {
                    '$ifNull': [
                        '$activity', ''
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'projectType': 0, 
                'project': 0, 
                'subProject': 0, 
                'band': 0, 
                'activity': 0, 
                'uniqueId': 0, 
                'projectGroup': 0, 
                'overall_table_count': 0, 
                'Action': 0,
                "customer":0
            }
        }
    ]
    response = cmo.finding_aggregate("AccuralRevenueMaster",arra)
    df = pd.DataFrame(response['data'])
    return df


@poLifeCycle_blueprint.route('/finance/poInvoiceBased', methods=["GET","POST","PUT","PATCH","DELETE"])
@poLifeCycle_blueprint.route('/finance/poInvoiceBased/<id>', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def poInvoiceBased(current_user,id=None):
    if request.method=="GET":
        poStatus = "Open"
        if (request.args.get("poStatus")!=None and request.args.get("poStatus")!='undefined'):
            poStatus= request.args.get("poStatus")
        arra = []
        if  id not in ['','undefined',None]:
            arra=arra+[
                {
                '$match': {
                    'deleteStatus': {'$ne': 1}, 
                    
                    'customer':id
                }
            },
            ]       
        arra = arra+[
            {
                '$match': {
                    'deleteStatus': {'$ne': 1}, 
                    "poStatus":poStatus,
                    'projectGroup': {'$in': projectGroup_str(current_user['userUniqueId'])},
                    
                }
            }, {
                '$sort':{
                    '_id':-1
                }
            }
        ]
        if (request.args.get("itemCodeStatus")!=None and request.args.get("itemCodeStatus")!='undefined'):
            arra = arra + [
                {
                    '$match':{
                        'itemCodeStatus': request.args.get("itemCodeStatus")
                    }
                }
            ]
        if request.args.get("searvhView")!=None and request.args.get("searvhView")!='undefined':
            searvhView = request.args.get("searvhView").strip()
            arra = arra + [
                {
                    '$match': {
                        '$or': [
                            {
                                'itemCode': {
                                    '$regex': searvhView, 
                                    '$options': 'i'
                                }
                            }, {
                                'poNumber': {
                                    '$regex': searvhView, 
                                    '$options': 'i'
                                }
                            }, {
                                'gbpa': {
                                    '$regex': searvhView, 
                                    '$options': 'i'
                                }
                            }, {
                                'description': {
                                    '$regex': searvhView, 
                                    '$options': 'i'
                                }
                            }, {
                                'itemCodeStatus': {
                                    '$regex': searvhView, 
                                    '$options': 'i'
                                }
                            }, {
                                'poStatus': {
                                    '$regex': searvhView, 
                                    '$options': 'i'
                                }
                            }
                        ]
                    }
                }
            ]
        arra = arra + apireq.countarra("PoInvoice",arra) + apireq.args_pagination(request.args)
        arra = arra + [
            {
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
                    'customer': {
                        '$toObjectId': '$customer'
                    },
                    'projectGroup': {
                        '$toObjectId': '$projectGroup'
                    },  
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'invoicedQty': '$result.qty'
                }
            }, {
                '$lookup': {
                    'from': 'customer', 
                    'localField': 'customer', 
                    'foreignField': '_id', 
                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                    'as': 'customerResult'
                }
            }, {
                '$lookup': {
                    'from': 'projectGroup', 
                    'localField': 'projectGroup', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            "$match":{
                                'deleteStatus':{'$ne':1}
                            }
                        }, {
                            '$lookup': {
                                'from': 'customer', 
                                'localField': 'customerId', 
                                'foreignField': '_id', 
                                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                                }
                            }
                        }, {
                            '$project': {
                                'projectGroupId': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'projectGroupIdResult'
                }
            }, {
                '$unwind': {
                    'path': '$customerResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$projectGroupIdResult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'customerName': '$customerResult.customerName',  
                    'projectGroupId': '$projectGroupIdResult.projectGroupId', 
                    'invoicedQty': {
                        '$ifNull': [
                            '$invoicedQty', 0
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'projectGroup': {
                        '$toString': '$projectGroup'
                    }, 
                    'customer': {
                        '$toString': '$customer'
                    }, 
                    'poStartDate': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$poStartDate', ''
                                ]
                            }, '', {
                                '$dateToString': {
                                    'date': {
                                        '$toDate': '$poStartDate'
                                    }, 
                                    'format': '%d-%m-%Y', 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }
                        ]
                    }, 
                    'poEndDate': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$poEndDate', ''
                                ]
                            }, '', {
                                '$dateToString': {
                                    'date': {
                                        '$toDate': '$poEndDate'
                                    }, 
                                    'format': '%d-%m-%Y', 
                                    'timezone': 'Asia/Kolkata'
                                }
                            }
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'povalidity': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$poEndDate', ''
                                ]
                            }, '', {
                                '$divide': [
                                    {
                                        '$subtract': [
                                            {
                                                '$toDate': '$poEndDate'
                                            }, {
                                                '$toDate': current_date()
                                            }
                                        ]
                                    }, 86400000
                                ]
                            }
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
                '$addFields': {
                    'OpenPoValue': {
                        '$multiply': [
                            '$unitRate(INR)', '$openQty'
                        ]
                    }
                }
            }, {
                '$project': {
                    'customerResult': 0,
                    'projectGroupIdResult': 0, 
                    'result': 0
                }
            }
        ]
        response = cmo.finding_aggregate("PoInvoice",arra)
        return respond(response)

    elif request.method == "POST":
        
        if id==None:
            allData = request.get_json()
            if allData['poStatus']  in ['Closed',"Short Closed"]:
                return respond ({
                    'status':400,
                    "icon":"error",
                    "msg":"Please change the PO status. The PO status cannot be closed or short-closed."
                })
                

            arra = [
                {
                    '$match':{
                            'projectGroup':allData['projectGroup'],
                            'itemCode':allData['itemCode'],
                            'poNumber':allData['poNumber'],
                    }
                }
            ]
            reponse = cmo.finding_aggregate("PoInvoice",arra)
            if (len(reponse['data'])):
                return {
                    'status':400,
                    "msg":"This PO , Item Code And Project Group Combination Is Already Exist In DataBase.",
                    "icon":"error",
                },400
            
            allData['unitRate(INR)'] = int(allData['unitRate(INR)'])
            allData['initialPoQty'] = int(allData['initialPoQty'])
            allData['addedAt'] = unique_timestamp()
            allData['itemCodeStatus'] = "Open"
            updateBy = {
                'poNumber':allData['poNumber']
            }
            updateData = {
                'poStatus':"Open"
            }
            cmo.updating_m("PoInvoice",updateBy,updateData,False)
            response=cmo.insertion("PoInvoice",allData)
            return respond(response)
        
        elif id!=None:
            allData=request.get_json()

            if "projectGroupId" in allData:
                del allData['projectGroupId']
            if "customerName" in allData:
                del allData['customerName']
                
            allData['unitRate(INR)'] = int(allData['unitRate(INR)'])
            allData['initialPoQty'] = int(allData['initialPoQty'])


            arra = [
                {
                    '$match':{
                        '_id':ObjectId(id)
                    }
                }
            ]
            response = cmo.finding_aggregate("PoInvoice",arra)
            PoStatus = response['data'][0]['poStatus']
            projectGroup1 = response['data'][0]['projectGroup']
            
            if allData['poStatus'] in ['Closed','Short Closed'] and PoStatus in ['Closed','Short Closed']:
                return respond({
                    'status':400,
                    "msg":"This PO Number is already closed. First, open it and then edit it.",
                    "icon":"error"
                })

            if allData['poStatus'] == "Open" and PoStatus in ['Closed','Short Closed']:
                # match_Stage = {
                #     '_id':ObjectId(id)
                # }
                # response = cagg.poEdit(match_Stage)
                # initialPoQty = allData['initialPoQty']
                # invoicedQty = response['data'][0]['invoicedQty']
                # openQty = initialPoQty - invoicedQty
                # if openQty == 0:
                #     return respond({
                #         'status':400,
                #         "msg":"Please increase the Initial PO Qty",
                #         "icon":"error"
                #     })
                # if  openQty < 0:
                #     return respond({
                #         'status':400,
                #         "msg":"The Initial PO Quantity is not less than the Invoiced Quantity, so please increase the Initial PO Qty accordingly.",
                #         "icon":"error"
                #     })
                    
                match_Stage = {
                    'poNumber':allData['poNumber']
                }
                response = cagg.poEdit(match_Stage)
                Data = response['data']

                for i in Data:
                    PONumber = []
                    if i['openQty']!=0:
                        PONumber.append(i['poNumber'])

                        updateData = {
                            'itemCodeStatus':"Open",
                        }
                        updateBy = {
                            '_id':i['_id']
                        }
                        cmo.updating("PoInvoice",updateBy,updateData,False)
                        
                    if i['openQty'] == 0:
                        PONumber.append(i['poNumber'])
                        
                        
                    for i in PONumber:
                        updateData = {
                            "poStatus":"Open"
                        }
                        updateBy = {
                            'poNumber': i
                        }
                        cmo.updating_m("PoInvoice",updateBy,updateData,False)
                        
                updateBy = {
                    "_id":ObjectId(id)
                }
                        
                updateData = {
                    'itemCodeStatus':"Open"
                }
                response = cmo.updating("PoInvoice",updateBy,updateData,False)
                return respond(response)

            if allData['poStatus'] in ['Closed','Short Closed']:
                data = {
                    'itemCodeStatus':"Closed"
                }
                data['poStatus'] = allData['poStatus']

                updateBy = {
                    'poNumber':allData['poNumber']
                }
                response = cmo.updating_m("PoInvoice",updateBy,data,False)
                return respond(response)
            
            else:
                match_stage = {
                            'poNumber': allData['poNumber'],
                            'itemCode':allData['itemCode'],
                            'projectGroup':projectGroup1
                        }
                    
                response = cagg.poEdit(match_stage)
                initialPoQty = allData['initialPoQty']
                invoicedQty = response['data'][0]['invoicedQty']
                openQty = initialPoQty - invoicedQty
                if  openQty < 0:
                    return respond({
                        'status':400,
                        "msg":"The Initial PO Quantity is not less than the Invoiced Quantity, so please increase the Initial PO Qty accordingly.",
                        "icon":"error"
                    })
                if (openQty) > 0:
                    allData['itemCodeStatus'] = "Open"
                    updateby = {
                        'poNumber':allData['poNumber']
                    }
                    updateData = {
                        'poStatus':'Open'
                    }
                    cmo.updating_m("PoInvoice",updateby,updateData,False)
                updateBy={
                    "_id":ObjectId(id)
                }
                cmo.updating("PoInvoice",updateBy,allData,False)
                    
                if (openQty) == 0:
                    allData['itemCodeStatus'] = "Closed"
                    match_stage = {
                        'poNumber':allData['poNumber']
                    }
                    match_stage2 = {
                        'openQty':{
                            '$ne':0
                        }
                    }
                    response = cagg.poEdit(match_stage,match_stage2)
                    response = response['data']
                    if (len(response) == 0):
                        allData['poStatus'] = "Closed"
                        updateby = {
                            'poNumber':allData['poNumber']
                        }
                        updateData = {
                            'poStatus':'Closed'
                        }
                        cmo.updating_m("PoInvoice",updateby,updateData,False)
                updateBy={
                    "_id":ObjectId(id)
                }
                response=cmo.updating("PoInvoice",updateBy,allData,False)
                return respond(response)
        
    elif request.method=="DELETE":
        allData = request.json.get("ids")
        if (len(allData)>0):
            itemCodeArray = []
            for i in allData:
                arra = [
                    {
                        '$match': {
                            '_id':ObjectId(i)
                        }
                    }, {
                        '$addFields': {
                            'initialPoQty': {
                                '$cond': {
                                    'if': {
                                        '$eq': [
                                            '$initialPoQty', ''
                                        ]
                                    }, 
                                    'then': 0, 
                                    'else': '$initialPoQty'
                                }
                            }
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
                            'invoicedQty':{
                                '$ifNull':['$result.qty',0]
                            }
                        }
                    }
                ]
                response = cmo.finding_aggregate("PoInvoice",arra)
                invoicedQty = response['data'][0]['invoicedQty']
                itemCode = response['data'][0]['itemCode']
                if invoicedQty == 0:
                    cmo.deleting("PoInvoice",i,current_user['userUniqueId'])
                else:
                    itemCodeArray.append(itemCode)
            if (len(itemCodeArray)>0):
                if (len(itemCodeArray) == 1):
                    datams={
                        "status":400,
                        "icon":"error",
                        "msg":f"This Line Item Code ({', '.join(itemCodeArray)}) contains Invoiced Quantity",
                        "data":[]
                    }
                    return respond(datams)
                else:
                    datams={
                        "status":400,
                        "icon":"error",
                        "msg":f"These Line Item Code ({', '.join(itemCodeArray)}) contains Invoiced Quantity",
                        "data":[]
                    }
                    return respond(datams)
            else:
                datams={
                    "status":200,
                    "icon":"info",
                    "msg":f"Data deleted successfully",
                    "data":[]
                }
                return respond(datams)    
        else:
            return jsonify({'msg':"Id not found"})
        






@poLifeCycle_blueprint.route('/finance/invoice', methods=["GET","POST","PUT","PATCH","DELETE"])
@poLifeCycle_blueprint.route('/finance/invoice/<id>', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def invoice(current_user,id=None):
    if request.method=="GET":
        arra = [
            {
                '$match': {
                    'deleteStatus': {
                        '$ne': 1
                    }, 
                    'projectGroup': {
                        '$in': projectGroup_str(current_user['userUniqueId'])
                    },
                    'customer':id
                }
            }
        ]
        if (request.args.get("wccNumber")!=None and request.args.get("wccNumber")!='undefined'):
            arra = arra + [
                {
                    '$match':{
                        'wccNumber':{
                            '$regex':re.escape(request.args.get("wccNumber").strip()),
                            '$options':'i'
                        }
                    }
                }
            ]
        if (request.args.get("invoiceNumber")!=None and request.args.get("invoiceNumber")!='undefined'):
            arra = arra + [
                {
                    '$match':{
                        'invoiceNumber':{
                            '$regex':re.escape(request.args.get("invoiceNumber").strip()),
                            '$options':'i'
                        }
                    }
                }
            ]
        
        arra = arra + [
            {
                '$sort':{
                    '_id':-1
                }
            }, {
                '$addFields':{
                    'uniqueId':{
                        '$toString':'$_id'
                    },
                    'invoiceDate': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$invoiceDate', ''
                                ]
                            }, None, {
                                '$toDate': '$invoiceDate'
                            }
                        ]
                    }, 
                    'wccSignOffdate': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$wccSignOffdate', ''
                                ]
                            }, None, {
                                '$toDate': '$wccSignOffdate'
                            }
                        ]
                    },
                }
            }, {
                '$addFields': {
                    'invoiceDate': {
                        '$dateAdd': {
                            'startDate': '$invoiceDate', 
                            'unit': 'minute', 
                            'amount': 330
                        }
                    }, 
                    'wccSignOffdate': {
                        '$dateAdd': {
                            'startDate': '$wccSignOffdate', 
                            'unit': 'minute', 
                            'amount': 330
                        }
                    }
                }
            }, {
                '$addFields': {
                    'invoiceDate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata', 
                            'date': '$invoiceDate'
                        }
                    }, 
                    'wccSignOffdate': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'timezone': 'Asia/Kolkata', 
                            'date': '$wccSignOffdate'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'date': '$invoiceDate'
                }
            }, {
                '$addFields': {
                    'date': {
                        '$dateFromString': {
                            'dateString': '$date', 
                            'format': '%d-%m-%Y',
                            'timezone':'Asia/Kolkata'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'year': {
                        '$year': '$date'
                    }, 
                    'month':{
                        '$month':'$date'
                    }
                }  
            }, {
                '$addFields': {
                    'month': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$month', 1
                                        ]
                                    }, 
                                    'then': 'Jan'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 2
                                        ]
                                    }, 
                                    'then': 'Feb'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 3
                                        ]
                                    }, 
                                    'then': 'Mar'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 4
                                        ]
                                    }, 
                                    'then': 'Apr'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 5
                                        ]
                                    }, 
                                    'then': 'May'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 6
                                        ]
                                    }, 
                                    'then': 'Jun'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 7
                                        ]
                                    }, 
                                    'then': 'Jul'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 8
                                        ]
                                    }, 
                                    'then': 'Aug'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 9
                                        ]
                                    }, 
                                    'then': 'Sep'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 10
                                        ]
                                    }, 
                                    'then': 'Oct'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 11
                                        ]
                                    }, 
                                    'then': 'Nov'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$month', 12
                                        ]
                                    }, 
                                    'then': 'Dec'
                                }
                            ], 
                            'default': None
                        }
                    }
                }
            }, {
                '$project':{
                    '_id':0,
                    'date':0,
                }
            }
        ]
        if (request.args.get("year")!=None and request.args.get("year")!='undefined'):
            arra = arra + [
                {
                    '$match':{
                        'year':int(request.args.get("year"))
                    }
                }
            ]
        if (request.args.get("month")!=None and request.args.get("month")!='undefined'):
            arra = arra + [
                {
                    '$match':{
                        'month':request.args.get("month")
                    }
                }
            ]
        response = cmo.finding_aggregate("invoice",arra)
        invoicedf = pd.DataFrame.from_dict(response['data'])
        if not invoicedf.empty:
        
            projectIddf = projectid()
            projectIddf.rename(columns={
                'projectId': 'projectIdName',
                'projectuniqueId': 'projectId'
            }, inplace=True)
            projectIddf.drop(columns=['projectGroup'],inplace=True)
            
            projectGroupdf = projectGroup()
            projectGroupdf.rename(columns={
                'Customer':'customerName',
                'projectGroupName':'projectGroupId'
            }, inplace=True)
        
            
            arra = [
                {
                    '$project': {
                        'siteId': {'$toString': '$_id'}, 
                        'siteIdName': '$Site Id',
                        'ssidName': '$systemId',
                        '_id': 0
                    }
                }
            ]
            
            sitedf = pd.DataFrame.from_dict(cmo.finding_aggregate("SiteEngineer",arra)['data'])
            mergedSite = invoicedf.merge(sitedf,on='siteId',how='left')
            mergedPG = invoicedf.merge(projectGroupdf,on='projectGroup',how='left')
            mergedPD = invoicedf.merge(projectIddf,on='projectId',how='left')
            
            invoicedf['siteIdName'] = mergedSite['siteIdName']
            invoicedf['ssidName'] = mergedSite['ssidName']
            invoicedf['projectIdName'] = mergedPD['projectIdName']
            invoicedf['projectGroupId'] = mergedPG['projectGroupId']
            invoicedf['customerName'] = mergedPG['customerName']
            invoicedf['amount'] = invoicedf['qty']*invoicedf['unitRate']
            
            
            if (request.args.get("siteId")!=None and request.args.get("siteId")!='undefined'):
                siteFilter = request.args.get("siteId").strip()
                invoicedf = invoicedf[invoicedf['siteIdName'].str.contains(siteFilter, regex=True, case=False, na=False)]
                
            if (request.args.get("projectGroup")!=None and request.args.get("ProjectGroup")!='undefined'):
                pgFilter = request.args.get("projectGroup").strip()
                invoicedf = invoicedf[invoicedf['projectGroupId'].str.contains(pgFilter, regex=True, case=False, na=False)]
            
            if (request.args.get("projectId")!=None and request.args.get("projectId")!='undefined'):
                pdFilter = request.args.get("projectId").strip()
                invoicedf = invoicedf[invoicedf['projectIdName'].str.contains(pdFilter, regex=True, case=False, na=False)]  
            
            invoicedf['overall_table_count'] = len(invoicedf)
            
            try:
                page = int(request.args.get("page"))
                if not isinstance(page, int):
                    page = 1
                
                limit = int(request.args.get("limit"))
                if not isinstance(limit, int):
                    limit = 50
                
                page = (page - 1) * limit
                limit = limit + page
                
                invoicedf = invoicedf.iloc[page:limit]
            except:
                invoicedf = invoicedf.iloc[0:50]
                
            response['data'] = json.loads(invoicedf.to_json(orient='records'))
            return respond(response)
        else:
            return respond({
                'status':200,
                "data":[],
                "msg":"Data Get Successfully"
            })

    elif request.method == "POST":

        if id == None:

            allData = request.get_json()
            allData['qty'] = int(allData['qty'])
            allData['unitRate'] = int(allData['unitRate'])
            allData['systemId'] = allData['siteId']
            if allData['wccSignOffdate'] == "":
                allData['wccSignOffdate'] = None
            
            arra = [
                {
                    '$match':{
                        'projectGroup':allData['projectGroup'],
                        'itemCode':allData['itemCode'],
                        'siteId':allData['siteId'],
                        "systemId":allData['systemId'],
                        "poNumber":allData['poNumber'],
                        "invoiceNumber":allData['invoiceNumber']
                    }
                }
            ]
            response = cmo.finding_aggregate("invoice",arra)['data']
            if (len(response)):
                return respond({
                    'status':400,
                    "icon":"error",
                    "msg":"This combination of Project Group, Item Code, System Id, Site Id, PO Number and Invoice number is already exist in Database"
                })


            data = cagg.commonfuncforadd(allData)
            if data['status']!=400:

                if allData['status'] == "Billed":
                    updateSiteData = {
                        'siteBillingStatus':'Billed',
                    }
                    updateSiteBy = {
                        '_id':ObjectId(allData['siteId'])
                    }
                    updateMilestoneData = {
                        'mileStoneStatus':'Closed',
                        'CC_Completion Date':allData['invoiceDate']
                    }
                    updateMilestoneBy = {
                        'Name':'Revenue Recognition',
                        'siteId':ObjectId(allData['siteId'])
                    }
                    arra = [
                        {
                            "$match":{
                                'deleteStatus':{'$ne':1}
                            }
                        }, {
                            '$match':{
                                'siteId':ObjectId(allData['siteId']),
                                "mileStoneStatus":"Open"
                            }
                        },
                    ]
                    MilestoneData = cmo.finding_aggregate("milestone",arra)['data']
                    OpenMilestone = len(MilestoneData)
                    if OpenMilestone == 1 and MilestoneData[0]['Name'] == "Revenue Recognition":
                        updateSiteData['siteStatus'] = "Close"
                        updateSiteData['Site_Completion Date'] = allData['invoiceDate']
                    if OpenMilestone == 0:
                        updateSiteData['siteStatus'] = "Close"
                        updateSiteData['Site_Completion Date'] = allData['invoiceDate']
                    cmo.updating("SiteEngineer",updateSiteBy,updateSiteData,False)
                    cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
                    
                elif allData['status'] == "Partially Billed":
                    updateSiteData = {
                        'siteBillingStatus':'Unbilled',
                        "siteStatus":"Open",
                        "Site_Completion Date":None
                    }
                    updateSiteBy = {
                        '_id':ObjectId(allData['siteId'])
                    }
                    updateMilestoneData = {
                        'mileStoneStatus':'Open',
                        'CC_Completion Date':None
                    }
                    updateMilestoneBy = {
                        'Name':'Revenue Recognition',
                        'siteId':ObjectId(allData['siteId'])
                    }
                    cmo.updating("SiteEngineer",updateSiteBy,updateSiteData,False)
                    cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
                allData['addedAt'] = unique_timestamp()
                response=cmo.insertion("invoice",allData)
                return respond(response)
            else:
                return respond(data)
    
        elif id!=None:

            allData=request.get_json()
            allData['qty'] = int(allData['qty'])
            allData['unitRate'] = int(allData['unitRate'])
            allData['systemId'] = allData['siteId']
            if allData['wccSignOffdate'] == "":
                allData['wccSignOffdate'] = None

            if "customerName" in allData:
                del allData['customerName']

            data = cagg.commonfuncforedit(allData,id)

            if data['status']!=400:
                arra = [
                    {
                        '$match':{
                            '_id':ObjectId(id)
                        }
                    }
                ]
                response = cmo.finding_aggregate("invoice",arra)
                billedStatus = response['data'][0]['status']

                if allData['status'] == "Billed":
                        
                    updateSiteData = {
                        'siteBillingStatus':'Billed',
                    }
                    updateSiteBy = {
                        '_id':ObjectId(allData['siteId']),
                        "deleteStatus":{"$ne":1},
                    }
                    updateMilestoneData = {
                        'mileStoneStatus':'Closed',
                        'CC_Completion Date':allData['invoiceDate']
                    }
                    updateMilestoneBy = {
                        'Name':'Revenue Recognition',
                        'siteId':ObjectId(allData['siteId']),
                        "deleteStatus":{"$ne":1},
                    }
                    arra = [
                        {
                            '$match':{
                                'siteId':ObjectId(allData['siteId']),
                                "mileStoneStatus":"Open"
                            }
                        },
                    ]
                    MilestoneData = cmo.finding_aggregate("milestone",arra)['data']
                    OpenMilestone = len(MilestoneData)
                    if OpenMilestone == 1 and MilestoneData[0]['Name'] == "Revenue Recognition":
                        updateSiteData['siteStatus'] = "Close"
                        updateSiteData['Site_Completion Date'] = allData['invoiceDate']
                    if OpenMilestone == 0:
                        updateSiteData['siteStatus'] = "Close"
                        updateSiteData['Site_Completion Date'] = allData['invoiceDate']
                    cmo.updating("SiteEngineer",updateSiteBy,updateSiteData,False)
                    cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
                
                if allData['status'] != "Billed":
                    updateSiteData = {
                        'siteBillingStatus':'Unbilled',
                        'siteStatus':'Open',
                        "Site_Completion Date":None
                    }
                    updateSiteBy = {
                        '_id':ObjectId(allData['siteId'])
                    }
                    updateMilestoneData = {
                        'mileStoneStatus':'Open',
                        'CC_Completion Date':None
                    }
                    updateMilestoneBy = {
                        'Name':'Revenue Recognition',
                        'siteId':ObjectId(allData['siteId'])
                    }
                    cmo.updating("SiteEngineer",updateSiteBy,updateSiteData,False)
                    cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)

                updateBy={"_id":ObjectId(id)}
                response=cmo.updating("invoice",updateBy,allData,False)
                return respond(response)
            else:
                return respond(data)
        
    elif request.method == "DELETE":
        allData = request.json.get("ids")

        if (len(allData)>0):
            for i in allData:
                arra = [
                    {
                        '$match':{
                            '_id':ObjectId(i)
                        }
                    }
                ]
                response = cmo.finding_aggregate("invoice",arra)['data']
                siteId = response[0]['siteId']
                systemId = response[0]['systemId']
                itemCode = response[0]['itemCode']
                poNumber = response[0]['poNumber']
                invoiceNumber = response[0]['invoiceNumber']
                id = response[0]['_id']
                updateBy = {
                    
                    'itemCode':response[0]['itemCode'],
                    'poNumber':response[0]['poNumber'],
                    'projectGroup':response[0]['projectGroup']
                }
                updatedData = {
                    'itemCodeStatus':"Open"
                }
                cmo.updating("PoInvoice",updateBy,updatedData,False)
                PoupdateBy = {
                    'poNumber':response[0]['poNumber']
                }
                poupdatedData = {
                    'poStatus':'Open'
                }
                cmo.updating_m("PoInvoice",PoupdateBy,poupdatedData,False)
                
                arra = [
                    {
                        '$match':{
                            'siteId':siteId
                        }
                    }, {
                        '$match':{
                            '_id':{
                                '$ne':id
                            }
                        }
                    }, {
                       '$sort':{
                           '_id':-1
                       } 
                    }
                ]
                siteResponse = cmo.finding_aggregate("invoice",arra)['data']
                length = len(siteResponse)
                if len(siteResponse):
                    sitestatus = siteResponse[0]['status']
                    siteinvoiceDate = siteResponse[0]['invoiceDate']
                if length == 0:
                    
                    siteStatusUpdate = {
                        '_id':ObjectId(siteId)
                    }
                    siteStatusUpdateData = {
                        'siteBillingStatus':'Unbilled',
                        "siteStatus":'Open',
                        "Site_Completion Date":None
                    }
                    cmo.updating("SiteEngineer",siteStatusUpdate,siteStatusUpdateData,False)
                    updateMilestoneData = {
                        'mileStoneStatus':'Open',
                        'CC_Completion Date':None
                    }
                    updateMilestoneBy = {
                        'Name':'Revenue Recognition',
                        'siteId':ObjectId(siteId)
                    }
                    cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
                    cmo.deleting("invoice",i,current_user['userUniqueId'])

                if length >= 1:
                    if sitestatus == "Partially Billed":
                        siteStatusUpdate = {
                            '_id':ObjectId(siteId)
                        }
                        siteStatusUpdateData = {
                            'siteBillingStatus':'Unbilled',
                            "siteStatus":'Open',
                            "Site_Completion Date":None
                        }
                        cmo.updating("SiteEngineer",siteStatusUpdate,siteStatusUpdateData,False)
                        updateMilestoneData = {
                            'mileStoneStatus':'Open',
                            'CC_Completion Date':None
                        }
                        updateMilestoneBy = {
                            'Name':'Revenue Recognition',
                            'siteId':ObjectId(siteId)
                        }
                        cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
                        cmo.deleting("invoice",i,current_user['userUniqueId'])
                        
                    if sitestatus == "Billed":
                        siteStatusUpdate = {
                            '_id':ObjectId(siteId)
                        }
                        siteStatusUpdateData = {
                            'siteBillingStatus':'Billed',
                        }
                        arra = [
                            {
                                '$match':{
                                    '_id':ObjectId(siteId)
                                }
                            }
                        ]
                        findStatusofsite = cmo.finding_aggregate("SiteEngineer",arra)['data'][0]['siteStatus']
                        if findStatusofsite == "Close":
                            siteStatusUpdateData['Site_Completion Date'] = siteinvoiceDate
                        cmo.updating("SiteEngineer",siteStatusUpdate,siteStatusUpdateData,False)
                        updateMilestoneData = {
                            'mileStoneStatus':'Closed',
                            'CC_Completion Date':siteinvoiceDate
                        }
                        updateMilestoneBy = {
                            'Name':'Revenue Recognition',
                            'siteId':ObjectId(siteId)
                        }
                        cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
                        cmo.deleting("invoice",i,current_user['userUniqueId'])
                        
            return respond({
                'status':200,
                "msg":"Data Deleted Successfully",
                "icon":"success"
            })
        else:
            return respond({
                'status':400,
                "msg":"No unique ID Found",
                "icon":"error"
            })
        
@poLifeCycle_blueprint.route('/finance/poTrackingWorkdone/<id>',methods=['GET'])
@token_required
def finance_po_workdone_dashboard(current_user,id=None):

    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }, 
                'projectGroup': {
                        '$in': projectGroup_str(current_user['userUniqueId'])
                },
                "customer":id
            }
        }, {
            '$addFields': {
                'unitRate(INR)': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$unitRate(INR)', ''
                            ]
                        }, 
                        'then': 0, 
                        'else': '$unitRate(INR)'
                    }
                }, 
                'initialPoQty': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$initialPoQty', ''
                            ]
                        }, 
                        'then': 0, 
                        'else': '$initialPoQty'
                    }
                }
            }
        }, {
            '$group': {
                '_id': {
                    'projectGroup': '$projectGroup', 
                    'itemCode': '$itemCode'
                }, 
                'Data': {
                    '$first': '$$ROOT'
                }, 
                'initialPoQty': {
                    '$sum': '$initialPoQty'
                }
            }
        }, {
            '$set': {
                'Data.initialPoQty': '$initialPoQty'
            }
        }, {
            '$replaceRoot': {
                'newRoot': '$Data'
            }
        }, {
            '$lookup': {
                'from': 'invoice', 
                'let': {
                    'projectGroup': '$projectGroup', 
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
                'invoicedQty': {
                    '$ifNull': [
                        '$result.qty', 0
                    ]
                }
            }
        }, {
            '$lookup': {
                'from': 'workDone', 
                'let': {
                    'projectGroup': '$projectGroup', 
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
                                            '$quantity', ''
                                        ]
                                    }, 
                                    'then': 0, 
                                    'else': '$quantity'
                                }
                            }
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'quantity': {
                                '$sum': '$quantity'
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
                'workDoneQty': {
                    '$ifNull': [
                        '$result.quantity', 0
                    ]
                }
            }
        }, {
            '$addFields': {
                'totalQty': {
                    '$sum': [
                        '$invoicedQty', '$workDoneQty'
                    ]
                }
            }
        }, {
            '$addFields': {
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
                }, 
                'projectGroup': {
                    '$cond': [
                        {
                            '$eq': [
                                '$projectGroupd', ''
                            ]
                        }, '', {
                            '$toObjectId': '$projectGroup'
                        }
                    ]
                }, 
                'projectType': {
                    '$cond': [
                        {
                            '$eq': [
                                '$projectType', ''
                            ]
                        }, '', {
                            '$toObjectId': '$projectType'
                        }
                    ]
                }, 
                '_id': {
                    '$toString': '$_id'
                }, 
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$lookup': {
                'from': 'customer', 
                'localField': 'customer', 
                'foreignField': '_id',
                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}], 
                'as': 'customerResult'
            }
        }, {
            '$lookup': {
                'from': 'project', 
                'localField': 'projectId', 
                'foreignField': '_id',
                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}], 
                'as': 'projectResult'
            }
        }, {
            '$lookup': {
                'from': 'projectType', 
                'localField': 'projectType', 
                'foreignField': '_id',
                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}], 
                'as': 'projectTypeResult'
            }
        }, {
            '$lookup': {
                'from': 'projectGroup', 
                'localField': 'projectGroup', 
                'foreignField': '_id', 
                'pipeline': [
                    {
                        "$match":{
                            'deleteStatus':{'$ne':1}
                        }
                    }, {
                        '$lookup': {
                            'from': 'customer', 
                            'localField': 'customerId', 
                            'foreignField': '_id', 
                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
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
                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}], 
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
                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}], 
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
                            }
                        }
                    }, {
                        '$project': {
                            'projectGroupId': 1, 
                            '_id': 0
                        }
                    }
                ], 
                'as': 'projectGroupIdResult'
            }
        }, {
            '$unwind': {
                'path': '$customerResult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$unwind': {
                'path': '$projectResult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$unwind': {
                'path': '$projectTypeResult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$unwind': {
                'path': '$projectGroupIdResult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'customer': '$customerResult.customerName', 
                'projectId': '$projectResult.projectId', 
                'projectType': '$projectTypeResult.projectType', 
                'projectGroup': '$projectGroupIdResult.projectGroupId'
            }
        }, {
            '$addFields': {
                'openQty': {
                    '$subtract': [
                        '$initialPoQty', '$totalQty'
                    ]
                }
            }
        }, {
            '$project': {
                'customer': 1, 
                'projectGroup': 1, 
                'projectType': 1, 
                'projectId': 1, 
                'gbpa': 1, 
                'itemCode': 1, 
                'description': 1, 
                'unitRate(INR)': 1, 
                'initialPoQty': 1, 
                'invoicedQty': 1, 
                'workDoneQty': 1, 
                'openQty': 1, 
                '_id': 0,
                'uniqueId':'1',
            }
        }
    ]
    
    if request.args.get("customer")!=None and request.args.get("customer")!='undefined':
        arra = arra + [
            {
                '$match':{
                    'customer':request.args.get("customer")
                }
            }
        ]
        
    if request.args.get("projectGroup")!=None and request.args.get("projectGroup")!='undefined':
        arra = arra + [
            {
                '$match':{
                    'projectGroup':request.args.get("projectGroup")
                }
            }
        ]
        
    if request.args.get("projectId")!=None and request.args.get("projectId")!='undefined':
        arra = arra + [
            {
                '$match':{
                    'projectId':request.args.get("projectId")
                }
            }
        ]
        
    if request.args.get("itemCode")!=None and request.args.get("itemCode")!='undefined':
        arra = arra + [
            {
                '$match':{
                    'itemCode':request.args.get("itemCode")
                }
            }
        ]
    arra = arra + apireq.commonarra + apireq.args_pagination(request.args)
    response = cmo.finding_aggregate("PoInvoice",arra)
    return respond(response)



@poLifeCycle_blueprint.route('/finance/poProjectID', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def financepoProjectID(current_user):
    arra = [
        {
            '$match': {
                'projectGroup': ObjectId(request.args.get("projectGroup"))
            }
        }, {
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
    response = cmo.finding_aggregate("project",arra)
    return respond(response)

@poLifeCycle_blueprint.route('/finance/subProject', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def financesubproject(current_user,id=None):
    arra = [
        {
            '$match': {
                '_id': ObjectId(request.args.get("projectType"))
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)
    projectType = response['data'][0]['projectType']
    arra = [
        {
            '$match': {
                'projectType': projectType
            }
        }, {
            '$addFields': {
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$project': {
                'subProject': 1, 
                'uniqueId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)
    print(response['data'])
    return respond(response)



@poLifeCycle_blueprint.route('/finance/projectType', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def finance_projectType(current_user):
    arra = []
    if request.args.get("customer")!="undefind" and request.args.get("customer")!=None:
        arra = arra + [
            {
                '$match':{
                    'custId':request.args.get("customer")
                }
            }
        ]
    arra =arra+ [
        {
            '$addFields': {
                'projectType': {
                    '$concat': [
                        '$projectType', ' (', '$subProject', ')'
                    ]
                }, 
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$project': {
                'projectType': 1, 
                'uniqueId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("projectType",arra)
    return respond(response)


@poLifeCycle_blueprint.route('/finance/siteId', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def finance_siteid(current_user):
    arra = [
        {
            '$match': {
                'projectuniqueId': request.args.get('projectId'),
            }
        }, {
            '$addFields': {
                'siteId': {
                    '$concat': [
                        '$Site Id', '(', '$systemId', ')'
                    ]
                }, 
                'uniqueId': {
                    '$toString': '$_id'
                }
            }
        }, {
            '$project': {
                'siteId': 1, 
                'uniqueId': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("SiteEngineer",arra)
    return respond(response)


@poLifeCycle_blueprint.route('/finance/commercial', methods=["GET","POST","PUT","PATCH","DELETE"])
@token_required
def finance_itemcode(current_user):
    arra = [
        {
            '$match': {
                'deleteStatus': {
                    '$ne': 1
                }
            }
        }, {
            '$project': {
                'itemCode01': 1, 
                'itemCode02': 1, 
                'itemCode03': 1, 
                'itemCode04': 1, 
                'itemCode05': 1, 
                'itemCode06': 1, 
                'itemCode07': 1, 
                '_id': 0
            }
        }
    ]
    response = cmo.finding_aggregate("AccuralRevenueMaster",arra)
    final_df = pd.DataFrame.from_dict(response['data'])
    item_code_columns = ['itemCode01', 'itemCode02', 'itemCode03', 'itemCode04', 'itemCode05', 'itemCode06', 'itemCode07']
    df_list = []
    for item_code in item_code_columns:
        df = final_df[[item_code]].copy()
        df.rename(columns={item_code: 'itemCode'}, inplace=True)
        df_list.append(df)
    df = pd.concat(df_list, ignore_index=True,)
    df = df[df['itemCode'] != '']
    df_unique = df.drop_duplicates(subset='itemCode')
    df_sorted = df_unique.sort_values(by='itemCode')
    dict_data = df_sorted.to_dict(orient='records')
    response['data'] = dict_data
    return respond(response)
    
    
@poLifeCycle_blueprint.route("/finance/accrualRevenue",methods=["GET","POST"])
@token_required
def getAccural(current_user):
    
    if request.method == "POST":
        
        allData = request.get_json()
        year = allData["year"]
        viewBy = allData["viewBy"].split(",")
        
        viwq = []

        for i in viewBy:
            viwq.append(int(i))
        
            
            
        arra = [
            {
                "$match":{'deleteStatus':{'$ne':1},'projectuniqueId':'66a8eef5980b0aa6ba73b7f9'}
            }, {
                '$addFields': {
                    'SubProjectId': {
                        '$toObjectId': '$SubProjectId'
                    }, 
                    'projectuniqueId': {
                        '$toObjectId': '$projectuniqueId'
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
                                'as': 'customerResult'
                            }
                        }, {
                            '$unwind': {
                                'path': '$customerResult', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$addFields': {
                                'customerName': '$customerResult.customerName'
                            }
                        }, {
                            '$project': {
                                'projectType': 1, 
                                'customerName': 1, 
                                '_id': 0
                            }
                        }
                    ], 
                    'as': 'ProjectTypeArray'
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
                                            'projectGroupUid': {
                                                '$toString': '$_id'
                                            }, 
                                            '_id': 0, 
                                            'projectGroupId': 1
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
                                'projectGroup': '$result.projectGroupId', 
                                'projectGroupUid': '$result.projectGroupUid'
                            }
                        }, {
                            '$project': {
                                'projectGroup': 1, 
                                'projectId': 1, 
                                '_id': 0, 
                                'projectGroupUid': 1
                            }
                        }
                    ], 
                    'as': 'projectArray'
                }
            }, {
                '$unwind': {
                    'path': '$ProjectTypeArray', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$projectArray', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'customer': '$ProjectTypeArray.customerName', 
                    'projectType': '$ProjectTypeArray.projectType', 
                    'subProject': '$ProjectTypeArray.subProject', 
                    'projectId': '$projectArray.projectId', 
                    'projectGroup': '$projectArray.projectGroup', 
                    'projectGroupUid': '$projectArray.projectGroupUid', 
                    '_id': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$project': {
                    'customer': 1, 
                    'projectGroup': 1, 
                    'projectType': 1, 
                    'projectId': 1, 
                    'systemId': 1, 
                    '_id': {
                        '$toObjectId': '$_id'
                    }, 
                    'siteUid': {
                        '$toObjectId': '$_id'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': 'siteUid', 
                    'foreignField': 'siteId', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {'$ne': 1}, 
                                'Name': {'$in': ['MS1', 'MS2']}
                            }
                        }, {
                            '$addFields': {
                                'CC_Completion Date': {
                                    '$ifNull': [
                                        '$CC_Completion Date', None
                                    ]
                                }
                            }
                        }, {
                            '$addFields': {
                                'CC_Completion Date': {
                                    '$toDate':'$CC_Completion Date'
                                }
                            }
                        }, 
                        {
                            '$addFields': {
                                'documentMonth': {
                                    '$month': '$CC_Completion Date'
                                }, 
                                'documentYear': {
                                    '$year': '$CC_Completion Date'
                                }
                            }
                        }, 
                        {
                           '$match':{
                               'documentMonth':{'$in':viwq},
                               'documentYear':{'$eq':int(year)}
                           } 
                        }
                    ], 
                    'as': 'Milestone_1'
                }
            }, {
                '$addFields': {
                    'length1': {
                        '$size': '$Milestone_1'
                    }
                }
            }, {
                '$match': {
                    '$expr': {
                        '$or': [
                            {
                                '$gte': [
                                    '$length1', 1
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'uid': {
                        '$toString': '$_id'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'workDone', 
                    'localField': 'uid', 
                    'foreignField': 'systemId', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {
                                    '$ne': 1
                                }
                            }
                        }, {
                            '$match': {
                                'itemCode': {
                                    '$ne': ''
                                }
                            }
                        }
                    ], 
                    'as': 'workDoneresult'
                }
            }, {
                '$addFields': {
                    'workDoneresultIndex': {
                        '$size': '$workDoneresult'
                    }
                }
            }, {
                '$match': {
                    'workDoneresultIndex': {
                        '$gte': 1
                    }
                }
            }, {
                '$unwind': {
                    'path': '$workDoneresult', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$Milestone_1', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'itemCode': '$workDoneresult.itemCode', 
                    'documentMonth': '$Milestone_1.documentMonth', 
                    'documentYear': '$Milestone_1.documentYear', 
                    'mName': '$Milestone_1.Name', 
                    'qty': {
                        '$ifNull': [
                            '$workDoneresult.quantity', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'Milestone_1': 0, 
                    'length': 0, 
                    'workDoneresultIndex': 0, 
                    'workDoneresult': 0, 
                    'length1': 0, 
                    'length2': 0
                }
            }, {
                '$group': {
                    '_id': {
                        'documentMonth': '$documentMonth', 
                        'documentYear': '$documentYear', 
                        'itemCode': '$itemCode', 
                        'mName': '$mName', 
                        'projectGroup': '$projectGroup', 
                        'projectId': '$projectId'
                    }, 
                    'qty': {
                        '$sum': '$qty'
                    }, 
                    'documentMonth': {
                        '$first': '$documentMonth'
                    }, 
                    'documentYear': {
                        '$first': '$documentYear'
                    }, 
                    'itemCode': {
                        '$first': '$itemCode'
                    }, 
                    'mName': {
                        '$first': '$mName'
                    }, 
                    'projectGroup': {
                        '$first': '$projectGroup'
                    }, 
                    'projectId': {
                        '$first': '$projectId'
                    }, 
                    'data': {
                        '$push': '$$ROOT'
                    }
                }
            }, {
                '$unwind': {
                    'path': '$data', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'data.documentMonth': '$documentMonth', 
                    'data.documentYear': '$documentYear', 
                    'data.qty': '$qty', 
                    'data.itemCode': '$itemCode', 
                    'data.mName': '$mName'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$data'
                }
            }, {
                '$project': {
                    'siteUid': 0, 
                    'systemId': 0, 
                    'uid': 0
                }
            }, {
                '$lookup': {
                    'from': 'projectType', 
                    'let': {
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
                                'Commercial': {
                                    '$exists': {
                                        '$ne': None
                                    }
                                }
                            }
                        }, {
                            '$unwind': {
                                'path': '$Commercial', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$addFields': {
                                'UnitRate': '$Commercial.UnitRate', 
                                'ItemCode': '$Commercial.ItemCode'
                            }
                        }, {
                            '$project': {
                                'UnitRate': 1, 
                                'ItemCode': 1, 
                                '_id': 0
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$ItemCode', '$$itemCode'
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'result'
                }
            }, {
                '$addFields': {
                    'unitRate': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$result.UnitRate', 0
                                ]
                            }, 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'result': 0
                }
            }, 
        ]
        dataAggr=cmo.finding_aggregate("SiteEngineer",arra)
        
        if (len(dataAggr['data'])>0):
            
            respDf = pd.DataFrame(dataAggr['data'])    
            pivot_df=respDf.pivot_table(index=["itemCode","projectId","projectGroup","customer","projectType","unitRate"],columns=['documentMonth',"mName"],values="qty",aggfunc="sum",fill_value=0)
            all_months = viwq
            all_mnames = ['MS1', 'MS2']
            all_combinations = pd.MultiIndex.from_product([all_months, all_mnames], names=['documentMonth', 'mName'])
            pivot_df = pivot_df.reindex(columns=all_combinations, fill_value=0)
            
            amounts = {}

            for month in viwq:
                
                ms1_column = (month, 'MS1')
                ms2_column = (month, 'MS2')
                amounts["amount"+str(month)] = (pivot_df[ms1_column] * pivot_df.index.get_level_values('unitRate') * 0.65 + pivot_df[ms2_column] * pivot_df.index.get_level_values('unitRate') * 0.35)

            amounts_df = pd.DataFrame(amounts)
            
            result_df = pd.concat([pivot_df, amounts_df], axis=1)
            
            listrespDf=respDf[["itemCode","projectId","projectGroup","customer","projectType","unitRate"]]
            
            listrespDf=listrespDf.drop_duplicates()
            
            final_data=listrespDf.merge(result_df,on=["itemCode","projectId","projectGroup","customer","projectType","unitRate"],how="right")
            
            
            jsonD=final_data.to_json(orient="records")
            dataAggr["data"]=json.loads(jsonD)
            return respond(dataAggr)
        else:
            return respond({
                "status":200,
                "data":[],
                "msg":"Data Get Successfully"
            })
            
              
@poLifeCycle_blueprint.route('/finance/poWorkdoneBased/<id>', methods=["GET"])
@token_required
def sample_site_function(current_user,id=None):
    if request.method == "GET":
        subProjectArray = (sub_project(current_user['userUniqueId'],id))[0]['uid']
        if request.args.get("projectType")!=None and request.args.get("projectType")!="undefined":
            subProjectArray = request.args.get("projectType").split(',')
        arra = [
            {
                '$match':{
                    'deleteStatus': {'$ne': 1},
                    'projectuniqueId':{
                        '$in':projectId_str(current_user['userUniqueId'])
                    },
                    "customerId":id
                }
            }
        ]
        if request.args.get("siteId")!=None and request.args.get("siteId")!='undefined':
            arra = arra + [
                {
                    '$match': {
                        'Site Id':{
                            '$regex':request.args.get("siteId").strip(),
                            '$options': 'i'
                        }
                    }
                }
            ]
            subProjectArray = []
            for i in (sub_project(current_user['userUniqueId'],id)):
                for k in i['uid']:
                    subProjectArray.append(k)
        if request.args.get("siteBillingStatus")!=None and request.args.get("siteBillingStatus")!="undefined":
            siteStatus = request.args.get("siteBillingStatus")
            arra = arra + [
                {
                    '$match':{
                        'siteBillingStatus':siteStatus,
                        
                    }
                }
            ]
        arra = arra +[
            {
                '$match':{
                    'SubProjectId':{'$in':subProjectArray}
                }
            }, 
            {
                '$lookup': {
                    'from': 'milestone', 
                    'localField': '_id', 
                    'foreignField': 'siteId', 
                    'pipeline': [
                        {
                            '$match': {
                                'deleteStatus': {'$ne': 1}, 
                                'Name': {'$in': ['MS1', 'MS2']}, 
                                'mileStoneStatus': 'Closed'
                            }
                        }, {
                            '$addFields': {
                                'CC_Completion Date': {
                                    '$toDate': '$CC_Completion Date'
                                }
                            }
                        }, {
                            '$addFields': {
                                'CC_Completion Date': {
                                    '$dateAdd': {
                                        'startDate': '$CC_Completion Date', 
                                        'unit': 'minute', 
                                        'amount': 330
                                    }
                                }
                            }
                        }, {
                            '$sort': {
                                'Name': 1
                            }
                        }
                    ], 
                    'as': 'Milestone'
                }
            }, {
                '$match': {
                    'Milestone': {
                        '$ne': []
                    }
                }
            }, {
                '$addFields': {
                    'MS1': {
                        '$arrayElemAt': [
                            {
                                '$filter': {
                                    'input': '$Milestone', 
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
                                    'input': '$Milestone', 
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
                    'MS1': '$MS1.CC_Completion Date',
                    'MS2': '$MS2.CC_Completion Date',
                }
            }, {
                '$addFields':{
                    'MS1':{
                        '$dateToString': {
                            'date': '$MS1', 
                            'timezone': 'Asia/Kolkata', 
                            'format': '%d/%m/%Y'
                        }
                    },
                    'MS2':{
                        '$dateToString': {
                            'date': '$MS2', 
                            'timezone': 'Asia/Kolkata', 
                            'format': '%d/%m/%Y'
                        }
                    }
                }
            }, {
                '$project':{
                    'projectuniqueId': 1, 
                    'SubProjectId': 1, 
                    'siteBillingStatus': 1, 
                    'BAND':{
                        '$ifNull':['$BAND',""]
                    }, 
                    'ACTIVITY':{
                        '$ifNull':['$ACTIVITY',""]
                    },
                    'siteId': {
                        '$toString': '$_id'
                    }, 
                    'siteuid':{
                        '$toString':'$_id'
                    },
                    'Site Id':1,
                    "MS1":1,
                    "MS2":1,
                    "systemId":1,
                    '_id':0,
                }
            }
        ]
        arra = arra + apireq.countarra("SiteEngineer",arra) + apireq.args_pagination(request.args)
        
        response = cmo.finding_aggregate("SiteEngineer",arra)
        if len(response['data']):
            pivotedData = pd.DataFrame(response['data'])
            pivotedData['BAND'] = pivotedData['BAND'].apply(lambda x: str(0) if x == "" else str(len(str(x).split('-'))))
            projectGroupdf = projectGroup()
            projectIddf = projectid()
            subProjectdf = subproject()
            masterunitRatedf = masterunitRate()
            mergedData = projectIddf.merge(projectGroupdf,on='projectGroup',how='left')
            projectIddf['projectGroup'] = mergedData['projectGroupName']
            projectIddf['customer'] = mergedData['Customer']
            mergedData = pivotedData.merge(projectIddf,on="projectuniqueId",how="left")
            pivotedData['projectId'] = mergedData['projectId']
            pivotedData['projectGroup'] = mergedData['projectGroup']
            pivotedData['customer'] = mergedData['customer']
            mergedData = pivotedData.merge(subProjectdf,on='SubProjectId',how='left')
            pivotedData['subProject'] = mergedData['subProject']
            pivotedData['projectType'] = mergedData['projectType']
            mergedData = masterunitRatedf.merge(projectIddf,on="projectuniqueId",how="left")
            masterunitRatedf['projectId'] = mergedData['projectId']
            mergedData = masterunitRatedf.merge(subProjectdf,on='SubProjectId',how='left')
            masterunitRatedf['subProject'] = mergedData['subProject']
            masterunitRatedf['projectType'] = mergedData['projectType']
            pivotedData.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
            masterunitRatedf.drop(['projectuniqueId', 'SubProjectId'], axis=1, inplace=True)
            df_macro = pivotedData[pivotedData['projectType'] == 'MACRO']
            if "BAND" in df_macro:
                del df_macro['BAND']
            # df_ibs_operation = pivotedData[pivotedData['projectType'].isin(['IBS', 'OPERATION'])]
            df_dismantle_relocation = pivotedData[(pivotedData['projectType'] == 'RELOCATION') | (pivotedData['subProject'] == 'RELOCATION DISMANTLE')]
            if "ACTIVITY" in df_dismantle_relocation:
                del df_dismantle_relocation['ACTIVITY']
            # df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO', 'IBS', 'OPERATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
            df_other = pivotedData[~pivotedData['projectType'].isin(['MACRO','RELOCATION']) & (pivotedData['subProject'] != 'RELOCATION DISMANTLE')]
            if 'BAND' in df_other.columns:
                del df_other['BAND']
            if 'ACTIVITY' in df_other.columns:
                del df_other['ACTIVITY']
            df_macro_merged = pd.merge(df_macro, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'ACTIVITY'], how='left')
            df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
            # df_dismantle_relocation_merged = pd.merge(df_dismantle_relocation, masterunitRatedf, on=['projectId', 'subProject', 'projectType', 'BAND'], how='left')
            df_other_merged = pd.merge(df_other, masterunitRatedf, on=['projectId', 'subProject', 'projectType'], how='left')
            final_df = pd.concat([df_macro_merged, df_dismantle_relocation_merged, df_other_merged], ignore_index=True)
            def calculate_amounts(row):
                amount1 = row['rate'] * 0.65 if pd.notna(row['MS1']) else 0
                amount2 = row['rate'] * 0.35 if pd.notna(row['MS2']) else 0
                return pd.Series([amount1, amount2])
            final_df[['amount1', 'amount2']] = final_df.apply(calculate_amounts, axis=1)
            final_df['final_amount'] = final_df['amount1'] + final_df['amount2']
            final_df['uniqueId'] = "1"
            json_Data = json.loads(final_df.to_json(orient='records'))
            
            return respond({
                'status':200,
                "msg":'Data Get Successfully',
                "data":json_Data
            })
            
        else:
            return respond({
                'status':200,
                "msg":'No Data Found',
                "data":[]
            })