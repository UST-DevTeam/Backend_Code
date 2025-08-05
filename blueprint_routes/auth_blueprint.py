from base import *
import time
auth_blueprint = Blueprint('auth_blueprint', __name__)

profileStatus={
        
    1:"User Registered",
    2:"Password Setup Successfully",
    3:"KYC Submitted",
    11:"User Rejected due to email expire"
}
  
@auth_blueprint.route('/login',methods=['POST'])
def login():

    body = request.get_json()
    loginAggr=[{
            '$addFields': {
                'userRoleId': {
                    '$toString': '$userRole'
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
            '$match': {
                'email': body['email'],
                "status":"Active",
            }
        }, 
        {
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
            'as': 'CurrentBalance'
        }
    }, {
        '$unwind': {
            'path': '$CurrentBalance', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'CurrentBalance': '$CurrentBalance.Amount'
        }
    },
        
        {
            '$lookup': {
                'from': 'uamManagement', 
                'localField': 'userRoleId', 
                'foreignField': 'roleId', 
                'pipeline':[
                    {
                        "$match":{
                            'deleteStatus':{'$ne':1}
                        }
                    }, {
                    '$match':{
                        "accessType":{
                            '$ne':False
                        }
                    }
                }],
                'as': 'roleresult'
            }
        }, {
            '$addFields': {
                'roleName': '$userRole.roleName', 
                'permission': '$userRole.permission', 
                'roleId': {
                    '$toString': '$userRole._id'
                }, 
                'id': {
                    '$toString': '$_id'
                },
                'uniqueId': {
                    '$toString': '$_id'
                },
            }
        }, {
            '$project': {
                'userRole': 0, 
                '_id': 0
            }
        }, {
            '$unwind': {
                'path': '$roleresult', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$project': {
                'roleresult._id': 0
            }
        }, {
            '$group': {
                '_id': '$roleresult.typeVal', 
                'dew': {
                    '$first': '$roleresult.typeVal'
                }, 
                'data': {
                    '$first': '$$ROOT'
                }, 
                'roleresult': {
                    '$push': '$roleresult'
                }, 
                'data': {
                    '$first': '$$ROOT'
                }
            }
        }, {
            '$group': {
                '_id': None, 
                'data': {
                    '$first': '$data'
                }, 
                'roless': {
                    '$push': '$roleresult'
                }, 
                'rols': {
                    '$push': '$dew'
                }
            }
        }, {
            '$addFields': {
                'data.roleresult': '$roless', 
                'data.rols': '$rols'
            }
        }, {
            '$replaceRoot': {
                'newRoot': '$data'
            }
        }
    ]
    userData=cmo.finding_aggregate("userRegister",loginAggr)

    if(len(userData["data"])>=1 and userData["data"][0]["password"]==body['password']):

        userData["data"]=userData["data"][0]
        newData={}

        for i in range(len(userData["data"]["rols"])):
            newData[userData["data"]["rols"][i]]=userData["data"]["roleresult"][i]
        uniqueid=userData["data"]["id"]
        
        userData["data"]["permissions"]=newData
        userData["data"]["expiresIn"]="36000" #10 Hour session expire 
        expire=ctm.u_timestamp(timedelta(days=7))
        userData["data"]["expiresTimeStamp"]=expire 
        roleName=userData["data"]["roleName"]

        expTime=(int(time.time())+60*60*18)
        # expTime=(int(time.time())+20)
        token = jwt.encode({'uniqueid': uniqueid, 'exp' : expTime},key=os.environ.get("SECRET_KEY"),algorithm="HS512")
        
        
        if(type(token)!=type("")):
            token=token.decode('utf-8')
        userData["data"]["idToken"]=token
        response = make_response()
        response.headers['Authorization'] = "Bearer "+token
        response.data = json.dumps(userData["data"])
        userData["status"]=200
        return respond(userData)

    else:
        userData["status"]=400
        userData["msg"]="Please Use Valid Credentials"
        userData['data'] = []
        return respond(userData)
    
    
    
@auth_blueprint.route('/forgetPassword',methods=['POST'])
def forget_password():
    allData = request.get_json()
    email = allData['email'].strip()
    arra = [
        {
            '$match':{
                'email':email
            }
        }
    ]
    response = cmo.finding_aggregate("userRegister",arra)['data']
    if response:
        password = response[0]['password']
        email = response[0]['email']
        empName = response[0]['empName']
        empCode = response[0]['empCode']
        cmailer.formatted_sendmail(to=[email],cc=[],subject="PMIS-Forget Password",message=cmt.forgetPassword_mail(empName,empCode,password))
        return respond({
            'status':200,
            "icon":"success",
            "msg":'Success! Your Existing password has been sent to your email. Please check your inbox.'
        })
    else:
        return respond({
            'status':400,
            "icon":"error",
            "msg":"This Email address not found. Please verify and try again."
        })
        


@auth_blueprint.route('/setupPassword',methods=['POST'])   
def reset_password():
    allData = request.get_json()
    arra = [
        {
            '$match':{
                'email':allData['email'].strip(),
                'password':allData['currentpassword'].strip()
            }
        }
    ]
    response = cmo.finding_aggregate("userRegister",arra)['data']
    if (response):
        updateBy = {
            'email':allData['email'].strip(),
            'deleteStatus':{'$ne':1}
        }
        updateData = {
            'password':allData['confirmPassword']
        }
        cmo.updating("userRegister",updateBy,updateData,False)
        return respond({
            'icon':'success',
            "status":200,
            "msg":'Your Password has been changed successfully.'
        })
    else:
        return respond({
            'status':400,
            'icon':'error',
            "msg":'Either Email Id or password is not found.please verify and try again'
        })


















# Amansas_api

@auth_blueprint.route('/register', methods=['GET',"POST","PUT","PATCH","DELETE"])
def register():
    if(request.method == "POST"):
        dataAll=request.get_json()
        data=cmo.finding("userRole",{"roleName":dataAll['roleName']})["data"][0]
        dataAll["userRole"]=ObjectId(data["_id"]["$oid"])
        dataAll["profileStatus"]=1
        dataAll['kycStatus']=0
        dataAll["enabled"]=0
        dataAll["mailLinkExpireAt"]=ctm.date_u_timestamp(ctm.curr_add_form(minute=15))
        userData=cmo.insertion("userRegister",dataAll)
        userData["msg"]="Your Verification Link is sent to email address"
        cmailer.formatted_sendmail(to=[dataAll["email"]],cc=[],subject="Verify Your Account - Action Required",message=cmt.Verification_mail(dataAll["firstName"]+" "+dataAll["surname"],userData["operation_id"]))
        return respond(userData)

@auth_blueprint.route('/setupRegistration', methods=['GET',"POST","PUT","PATCH","DELETE"])
@auth_blueprint.route('/setupRegistration/<id>', methods=['GET',"POST","PUT","PATCH","DELETE"])
def setupregistration(id=None):

    if request.method == "POST":
        position = request.form.get("position")
        uid = request.form.get("uid")
        mandatoryBusiness = request.form.get("mandatoryBusiness")
        specifyPosition = request.form.get("specifyPosition")
        mandateLetter = request.files.get("mandateLetter")

        data = {}

        if mandateLetter:
            pathing=cform.singleFileSaver(mandateLetter,"","")
            if(pathing["status"]==422):
                return respond(pathing)
            if(pathing["status"]==200):
                data['mandateLetter'] = pathing['msg']
        
        if position:
            data['position'] = position

        if mandatoryBusiness:
            data['mandatoryBusiness'] = mandatoryBusiness

        if specifyPosition:
            data['specifyPosition'] = specifyPosition

        updateBy = {
            '_id':ObjectId(uid)
        }
        response = cmo.updating("userRegister",updateBy,data,False)
        arra =[
            {
                '$match':{
                    '_id':ObjectId(uid)
                }
            }, {
                "$project":{
                    '_id':0,
                    'roleName':1
                }
            }
        ]
        data = cmo.finding_aggregate("userRegister",arra)
        response['role'] = data['data'][0]['roleName']
        return respond(response)
    

@auth_blueprint.route('/businessRegister', methods=['GET',"POST","PUT","PATCH","DELETE"])
@auth_blueprint.route('/businessRegister/<id>', methods=['GET',"POST","PUT","PATCH","DELETE"])
def businessregister():
    if request.method == "POST":
        allData = request.get_json()
        uid = allData['uid']
        updateBy = {
            '_id':ObjectId(uid)
        }
        response = cmo.updating("userRegister",updateBy,allData,False)
        arra =[
            {
                '$match':{
                    '_id':ObjectId(uid)
                }
            }, {
                "$project":{
                    '_id':0,
                    'roleName':1
                }
            }
        ]
        data = cmo.finding_aggregate("userRegister",arra)
        response['role'] = data['data'][0]['roleName']
        return respond(response)
    

@auth_blueprint.route('/setuppassword/stepOne',methods=['GET',"POST","PUT","PATCH","DELETE"])
def setuppassword_stepOne():

    if request.method == "POST":
        dataAll=request.get_json()
        looker = [
            {
                '$match':{
                    '_id':ObjectId(dataAll["userid"])
                }
            }, {
                '$lookup': {
                    'from': 'userRole', 
                    'localField': 'userRole', 
                    'foreignField': '_id', 
                    'pipeline':[{'$match':{'deleteStatus':{'$ne':1}}}],
                    'as': 'result'
                }
            }, {
                '$unwind': {
                    'path': '$result', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'roleName': '$result.roleName'
                }
            }, {
                '$project':{
                    'result':0
                }
            }
        ]
        olddata=cmo.finding_aggregate("userRegister",looker)["data"][0]
        # del dataAll["userid"]
        userData={

            "status":400,
            "msg":"Something Went Wrong",
            "data":[],
            "icon":"error",
        }
        # if(olddata["mailLinkExpireAt"]>ctm.u_timestamp()):
        if 1==1:
            looker={
                "_id":ObjectId(dataAll["userid"])
            }  
            userData=cmo.updating("userRegister",looker,dataAll,False)
            if(userData["status"]==200):
                data = {
                   'profileStatus':2 
                }
                userData=cmo.updating("userRegister",looker,data,False)
                userData["msg"]="Password Setup Successfully"
                userData['role'] = olddata['roleName']
            else:
                userData["msg"]="Something Went Wrong"
        else:
            userData["msg"]="Link is expired"

        return respond(userData)