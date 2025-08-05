from base import *
df_current_date = pd.to_datetime(datetime.now(tz=pytz.timezone("Asia/Kolkata")))
current_date = df_current_date.replace(hour=0, minute=0, second=0, microsecond=0)
df_current_date = pd.to_datetime(current_date)

def commonfuncforadd(allData):

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
        
    
    if (len(reponse['data'])>0):

        if( reponse['data'][0]['itemCodeStatus'] == "Closed"):
            return {
                "status":400,
                "msg":"The Combination of  PO Number Item Code and Project Group are already marked as closed",
                "icon":"error"
            }
        
        if allData['qty']==0:
            return {
                'status':400,
                "msg":"You Can't fill Zero Quantity.",
                "icon":"error",
            }
        
        if allData['qty']<0:
            return {
                'status':400,
                "msg":"You Can't fill Negative Quantity.",
                "icon":"error",
            }
        if allData['qty']>0:
            arra = [
                {
                    '$match':{
                            'projectGroup':allData['projectGroup'],
                            'itemCode':allData['itemCode'],
                            'poNumber':allData['poNumber'],
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
                            '$ifNull': ['$invoicedQty', 0]
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
                }
            ]
            response = cmo.finding_aggregate("PoInvoice",arra)
            openqty = response['data'][0]['openQty']
                

            if allData['qty'] > openqty:
                return {
                    'status':400,
                    "msg":"The invoiced quantity exceeds the open quantity for this PO"+ allData['poNumber'] + ", Item Code:-" + allData['itemCode']+ " and Project Group:-" +ProjectGroup,
                    "icon":"error",
                }
            
            remainingQty = openqty - allData['qty']


            if remainingQty == 0:
                updateData = {
                    'itemCodeStatus':"Closed"
                }
                updateBy = {

                    'projectGroup':allData['projectGroup'],
                    'itemCode':allData['itemCode'],
                    'poNumber':allData['poNumber'],
                    "deleteStatus":{'$ne':1}
                }
                cmo.updating("PoInvoice",updateBy,updateData,False)
            arra = [
                {
                    '$match': {
                        'poNumber': allData['poNumber']
                    }
                }, {
                    '$group': {
                        '_id': '$poNumber', 
                        'initialPoQty': {
                            '$push': '$initialPoQty'
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$initialPoQty', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$group': {
                        '_id': None, 
                        'poNumber': {
                            '$first': '$_id'
                        }, 
                        'totalPoQty': {
                            '$sum': '$initialPoQty'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'invoice', 
                        'let': {
                            'poNumber': '$poNumber'
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
                                        '$eq': [
                                            '$poNumber', '$$poNumber'
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
                                '$totalPoQty', '$invoicedQty'
                            ]
                        }
                    }
                }
            ]
            response = cmo.finding_aggregate("PoInvoice",arra)
            poOpenQty = response['data'][0]['openQty']

            poOpenQty = poOpenQty-allData['qty']

            if poOpenQty == 0:
                updateData = {
                    "itemCodeStatus":"Closed",
                    "poStatus":"Closed"
                }
                updateBy = {
                    'poNumber':allData['poNumber'],
                    "deleteStatus":{"$ne":1}
                }
                response = cmo.updating_m("PoInvoice",updateBy,updateData,False)
                return response
            else:
                return {
                    'status':200,
                    "msg":"successfully"
                }
    else:
        return {
            'status':400,
            "msg":"The combination of  PO Number,item Code and Project Group is not found in Database.Please correct this.",
            "icon":"error",
        }


        
def commonfuncforaddBulkUpload(allData,index,quantityDict = {},listError=[]):
    
    index = index+2
    
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
        
    
    if (len(reponse['data'])>0):
        
        if( reponse['data'][0]['itemCodeStatus'] == "Closed"):
            listError.append(f"Row No-{index} The PO Number, Item Code, and Project Group have already been marked as closed.")
            return

        arra = [
            {
                '$match':{
                        'projectGroup':allData['projectGroup'],
                        'itemCode':allData['itemCode'],
                        'poNumber':allData['poNumber'],
                        "deleteStatus":{'$ne':1}
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
                        '$ifNull': ['$invoicedQty', 0]
                    },
                    'openQty': {
                        '$subtract': [
                            '$initialPoQty', '$invoicedQty'
                        ]
                    },
                    '_id':{
                        '$toString':'$_id'
                    }
                }
            }
        ]
        response = cmo.finding_aggregate("PoInvoice",arra)['data'][0]
        openqty = response['openQty']
        arra = [
            {
                '$match':{
                    'invoiceNumber':allData['invoiceNumber'],
                    "poNumber":allData['poNumber'],
                    "itemCode":allData['itemCode'],
                    "projectGroup":allData['projectGroup'],
                    "siteId":allData['siteId'],
                    "systemId":allData['systemId'],
                    "deleteStatus":{'$ne':1},
                }
            }
        ]
        response2 = cmo.finding_aggregate("invoice",arra)['data']
        if len(response2):
            quantity = response2[0]['qty']
            openqty = openqty + quantity
        
        if(response['_id'] in quantityDict):
            if(quantityDict[response['_id']]["final"]-allData['qty']>=0):
                quantityDict[response['_id']]["final"]=quantityDict[response['_id']]["final"]-allData['qty']
            else:
                # listError.append(f"{allData['poNumber']} in this invoice only {quantityDict[response['_id']]['final']} is left but need {allData['qty']}")
                listError.append(f"Row No-{index} The invoiced quantity exceeds the open quantity")
                return
            
            
        else:
            quantityDict[response['_id']]={
                "initial":openqty,
                "final":openqty-allData['qty']
            }  

        if allData['qty'] > openqty:
            listError.append(f"Row No-{index} The invoiced quantity exceeds the open quantity")
            return 
        else:
            return  
    else:
        listError.append(f"Row No-{index} The combination of PO Number, Item Code, and Project Group was not found in the database. Please correct this.")


def commonfuncforaddBulkUploadwithnoError(allData):
    arra = [
        {
            '$match':{
                    'projectGroup':allData['projectGroup'],
                    'itemCode':allData['itemCode'],
                    'poNumber':allData['poNumber'],
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
                    '$ifNull': ['$invoicedQty', 0]
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
        }
    ]
    response = cmo.finding_aggregate("PoInvoice",arra)
    openqty = response['data'][0]['openQty']
    
    
    arra = [
        {
            '$match':{
                'invoiceNumber':allData['invoiceNumber'],
                "poNumber":allData['poNumber'],
                "itemCode":allData['itemCode'],
                "projectGroup":allData['projectGroup'],
                "siteId":allData['siteId'],
                "systemId":allData['systemId'],
                    
            }
        }
    ]
    response = cmo.finding_aggregate("invoice",arra)['data']
    if len(response):
        quantity = response[0]['qty']
        openqty = openqty + quantity
    
    remainingQty = openqty - allData['qty']
    
    if remainingQty == 0:
        updateData = {
            'itemCodeStatus':"Closed"
        }
        updateBy = {

            'projectGroup':allData['projectGroup'],
            'itemCode':allData['itemCode'],
            'poNumber':allData['poNumber'],
            "deleteStatus":{"$ne":1},
        }
        cmo.updating("PoInvoice",updateBy,updateData,False)
    arra = [
        {
            '$match': {
                'poNumber': allData['poNumber']
            }
        }, {
            '$group': {
                '_id': '$poNumber', 
                'initialPoQty': {
                    '$push': '$initialPoQty'
                }
            }
        }, {
            '$unwind': {
                'path': '$initialPoQty', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$group': {
                '_id': None, 
                'poNumber': {
                    '$first': '$_id'
                }, 
                'totalPoQty': {
                    '$sum': '$initialPoQty'
                }
            }
        }, {
            '$lookup': {
                'from': 'invoice', 
                'let': {
                    'poNumber': '$poNumber'
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
                                '$eq': [
                                    '$poNumber', '$$poNumber'
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
                        '$totalPoQty', '$invoicedQty'
                    ]
                }
            }
        }
    ]
    response = cmo.finding_aggregate("PoInvoice",arra)
    poOpenQty = response['data'][0]['openQty']
    arra = [
        {
            '$match':{
                'invoiceNumber':allData['invoiceNumber'],
                "poNumber":allData['poNumber'],
                "itemCode":allData['itemCode'],
                "projectGroup":allData['projectGroup'],
                "siteId":allData['siteId'],
                "systemId":allData['systemId'],
                    
            }
        }
    ]
    response = cmo.finding_aggregate("invoice",arra)['data']
    if len(response):
        quantity = response[0]['qty']
        poOpenQty = poOpenQty + quantity

    poOpenQty = poOpenQty-allData['qty']

    if poOpenQty == 0:
        updateData = {
            "itemCodeStatus":"Closed",
            "poStatus":"Closed"
        }
        updateBy = {
            'poNumber':allData['poNumber'],
             "deleteStatus":{"$ne":1},
        }
        cmo.updating_m("PoInvoice",updateBy,updateData,False)
        
    if allData["status"] == "Billed":
        updateSiteData = {
            "siteBillingStatus": "Billed",
        }
        updateSiteBy = {"_id": ObjectId(allData["siteId"]), "deleteStatus":{"$ne":1}}
        updateMilestoneData = {
            "mileStoneStatus": "Closed",
            "CC_Completion Date": allData['invoiceDate']
        }
        updateMilestoneBy = {
            "Name": "Revenue Recognition",
            "siteId": ObjectId(allData["siteId"]),
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
        if OpenMilestone == 1:
            updateSiteData['siteStatus'] = "Close"
            updateSiteData['Site_Completion Date'] = allData['invoiceDate']
        if OpenMilestone == 0:
            updateSiteData['siteStatus'] = "Close"
            updateSiteData['Site_Completion Date'] = allData['invoiceDate']
        cmo.updating("SiteEngineer",updateSiteBy,updateSiteData,False)
        cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
        
    if allData["status"] == "Partially Billed":
        updateSiteData = {
            "siteBillingStatus": "Unbilled",
        }
        updateSiteBy = {"_id": ObjectId(allData["siteId"]), "deleteStatus":{"$ne":1}}
        updateMilestoneData = {
            "mileStoneStatus": "Open",
            "CC_Completion Date": None
        }
        updateMilestoneBy = {
            "Name": "Revenue Recognition",
            "siteId": ObjectId(allData["siteId"]),
             "deleteStatus":{"$ne":1},
        }
        cmo.updating("SiteEngineer",updateSiteBy,updateSiteData,False)
        cmo.updating("milestone",updateMilestoneBy,updateMilestoneData,False)
        
    updateData = {   
        'invoiceNumber':allData['invoiceNumber'],
        "poNumber":allData['poNumber'],
        "itemCode":allData['itemCode'],
        "projectGroup":allData['projectGroup'],
        "siteId":allData['siteId'],
        "systemId":allData['systemId'],
        "deleteStatus":{'$ne':1},
    }
    cmo.updating("invoice",updateData,allData,True)
    return
    
    
    

def commonfuncforedit(allData,id):


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
        
    
    if (len(reponse['data'])>0): 


        if( reponse['data'][0]['initialPoQty'] == 0):
            return {
                'status':400,
                "msg":"Intial Quantity does not Exist For this PO Number, Item Code and Project Group",
                "icon":"error",
            }
        
        if( reponse['data'][0]['itemCodeStatus'] == "Closed"):
            return {
                "status":400,
                "msg":"This PO Number, Item Code, and Project Group are already marked as closed",
                "icon":"error"
            }
        
        if allData['qty']==0:
            return {
                'status':400,
                "msg":"You Can't fill Zero Quantity.",
                "icon":"error",
            }
            
        if allData['qty']<0:
            return {
                'status':400,
                "msg":"You Can't fill Negative Quantity.",
                "icon":"error",
            }
        
        if allData['qty']!=0:

            arra = [
                {
                    '$match':{
                            'projectGroup':allData['projectGroup'],
                            'itemCode':allData['itemCode'],
                            'poNumber':allData['poNumber'],
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
                            '$ifNull': ['$invoicedQty', 0]
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
                }
            ]
            response = cmo.finding_aggregate("PoInvoice",arra)
            openqty = response['data'][0]['openQty']

            arra = [
                {
                    '$match':{
                        '_id':ObjectId(id)
                    }
                }
            ]
            response = cmo.finding_aggregate("invoice",arra)
            quantity = response['data'][0]['qty']


            openqty = openqty + quantity


            if allData['qty'] > openqty:
                return {
                    'status':400,
                    "msg":"The invoiced quantity exceeds the open quantity for this PO Number Item Code and ProjectGroup",
                    "icon":"error",
                }
            
            remainingQty = openqty - allData['qty']


            if remainingQty == 0:
                updateData = {
                    'itemCodeStatus':"Closed"
                }
                updateBy = {

                    'projectGroup':allData['projectGroup'],
                    'itemCode':allData['itemCode'],
                    'poNumber':allData['poNumber'],
                     "deleteStatus":{"$ne":1},
                }
                cmo.updating("PoInvoice",updateBy,updateData,False)
            # if remainingQty != 0:
            #     updateData = {
            #         'itemCodeStatus':"Open"
            #     }
            #     updateBy = {

            #         'projectGroup':allData['projectGroup'],
            #         'itemCode':allData['itemCode'],
            #         'poNumber':allData['poNumber'],
            #     }
            #     cmo.updating("PoInvoice",updateBy,updateData,False)
            arra = [
                {
                    '$match': {
                        'poNumber': allData['poNumber']
                    }
                }, {
                    '$group': {
                        '_id': '$poNumber', 
                        'initialPoQty': {
                            '$push': '$initialPoQty'
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$initialPoQty', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$group': {
                        '_id': None, 
                        'poNumber': {
                            '$first': '$_id'
                        }, 
                        'totalPoQty': {
                            '$sum': '$initialPoQty'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'invoice', 
                        'let': {
                            'poNumber': '$poNumber'
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
                                        '$eq': [
                                            '$poNumber', '$$poNumber'
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
                                '$totalPoQty', '$invoicedQty'
                            ]
                        }
                    }
                }
            ]
            response = cmo.finding_aggregate("PoInvoice",arra)
            poOpenQty = response['data'][0]['openQty']

            poOpenQty = poOpenQty+quantity-allData['qty']

            if poOpenQty == 0:
                updateData = {
                    "itemCodeStatus":"Closed",
                    "poStatus":"Closed"
                }
                updateBy = {
                    'poNumber':allData['poNumber'],
                     "deleteStatus":{"$ne":1},
                }
                response = cmo.updating_m("PoInvoice",updateBy,updateData,False)
                return response
            else:
                return {
                    'status':200,
                    "msg":"successfully"
                }
    else:
        return {
            'status':400,
            "msg":"The combination of this PO Number, Item Code  and Project Group is not found in Database.Please correct this.",
            "icon":"error",
        }


def poEdit(matchStage,match_stage2 = ""):
    arra = [
        {
            '$match': matchStage
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
                'invoicedQty': {
                    '$ifNull':['$result.qty',0]
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
        }
    ]
    if match_stage2!="":
        arra = arra + [
            {
                '$match':match_stage2
            }
        ]
    response = cmo.finding_aggregate("PoInvoice",arra)
    return response
            

            







