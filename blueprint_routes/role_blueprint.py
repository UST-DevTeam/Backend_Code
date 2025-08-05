from base import *

role_blueprint = Blueprint('role_blueprint', __name__)


@role_blueprint.route("/vendorSiteId",methods=['GET'])
@token_required
def usersiteid(current_user):
    print(current_user)
    if request.method == "GET":
        arra = [
            {
                '$match': {
                    'assignerId': ObjectId(current_user['userUniqueId'])
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    }, 
                    'uniqueId': {
                        '$toString': '$_id'
                    }, 
                    'mileStoneStartDate': {
                        '$dateToString': {
                            'date': '$mileStoneStartDate', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'mileStoneEndDate': {
                        '$dateToString': {
                            'date': '$mileStoneEndDate', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'CC_Completion Date': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    {
                                        '$type': '$CC_Completion Date'
                                    }, 'date'
                                ]
                            }, 
                            'then': {
                                '$dateToString': {
                                    'date': '$CC_Completion Date', 
                                    'format': '%d-%m-%Y', 
                                    'timezone': 'UTC'
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
                                        '$type': '$CC_Completion Date'
                                    }, 'date'
                                ]
                            }, 
                            'then': {
                                '$round': {
                                    '$divide': [
                                        {
                                            '$subtract': [
                                                '$mileStoneEndDate', '$CC_Completion Date'
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
                                                '$mileStoneEndDate', '$$NOW'
                                            ]
                                        }, 86400000
                                    ]
                                }
                            }
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
                            "$match":{
                                'deleteStatus':{'$ne':1}
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
                    'let': {
                        'projectuniqueId': '$projectuniqueId'
                    }, 
                    'localField': 'siteId', 
                    'foreignField': '_id', 
                    'pipeline': [
                        {
                            "$match":{
                                'deleteStatus':{'$ne':1}
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
                                'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                                'as': 'SubProjectId'
                            }
                        }, {
                            '$unwind': {
                                'path': '$SubProjectId', 
                                'preserveNullAndEmptyArrays': True
                            }
                        }, {
                            '$addFields': {
                                'SubProject': '$SubProjectId.subProject', 
                                'projectuniqueId': {
                                    '$toObjectId': '$projectuniqueId'
                                }
                            }
                        }, {
                            '$project': {
                                'SubProjectId': 0
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
                                            'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}], 
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
                '$addFields': {
                    'siteIdArrayLength': {
                        '$size': '$siteResult'
                    }
                }
            }, {
                '$match': {
                    'siteIdArrayLength': {
                        '$ne': 0
                    }
                }
            }, {
                '$unwind': '$siteResult'
            }, {
                '$addFields': {
                    'projectuniqueId': '$siteResult.projectuniqueId'
                }
            }, {
                '$lookup': {
                    'from': 'project', 
                    'localField': 'projectuniqueId', 
                    'foreignField': '_id', 
                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                    'as': 'project'
                }
            }, {
                '$unwind': {
                    'path': '$project', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'projectId': '$project.projectId'
                }
            }, {
                '$project': {
                    'siteResult._id': 0, 
                    '_id': 0, 
                    'projectuniqueId': 0, 
                    'project': 0
                }
            }, {
                '$addFields': {
                    'uniqueId': {
                        '$toString': '$siteId'
                    }, 
                    'siteResult.projectuniqueId': {
                        '$toString': '$siteResult.projectuniqueId'
                    }, 
                    'siteStartDate': {
                        '$dateToString': {
                            'date': '$siteResult.siteStartDate', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'siteEndDate': {
                        '$dateToString': {
                            'date': '$siteResult.siteEndDate', 
                            'format': '%d-%m-%Y'
                        }
                    }, 
                    'PMName': '$siteResult.projectArray.PMName', 
                    'siteId': '$siteResult.siteid', 
                    'subProject': '$siteResult.SubProject', 
                    'siteName': '$siteResult.sitename', 
                    'milestoneArray': {
                        '$map': {
                            'input': '$milestoneArray', 
                            'as': 'milestone', 
                            'in': {
                                '$mergeObjects': [
                                    '$$milestone', {
                                        'siteId': {
                                            '$toString': '$$milestone.siteId'
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'result': 0, 
                    'siteIdArrayLength': 0
                }
            }
        ]
        response = cmo.finding_aggregate("milestone",arra)
        return respond(response)