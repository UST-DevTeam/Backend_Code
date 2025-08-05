from base import *

location_blueprint = Blueprint("location_blueprint", __name__)


@location_blueprint.route("/country",methods=['GET'])
def getCountry():
    if request.method == "GET":
        arra =[
            {
                '$project': {
                    'name': 1, 
                    'iso3': 1, 
                    'iso2': 1, 
                    '_id': 0
                }
            }
        ]
        response = cmo.finding_aggregate("country",arra)
        return respond(response)


@location_blueprint.route("/state",methods=['GET'])
def getState():
    # countryCode = request.args.get("countryCode")
    countryCode = "IN"
    if countryCode:
        aggr = [
            {"$match": {"country_code": countryCode}},
            {"$project": {"_id": 0, "name": 1, "state_code": 1}},
        ]
        response=cmo.finding_aggregate("state", aggr)
        return respond(response)
    else:
        return jsonify({"msg":"Please Select Country"}),404



@location_blueprint.route("/city",methods=['GET'])
def getCities():
    if request.method == "GET":
        stateCode = request.args.get("stateCode")
        countryCode = "IN"
        if stateCode:
            aggr = [{"$match": {"state_code": stateCode,"country_code":countryCode}}, {"$project": {"_id": 0, "city": "$name"}}]
            cities = cmo.finding_aggregate("city", aggr)
            return respond(cities)
        else:
            response = {
                "status": 200,
                "data": []
            }
            return respond(response)
        

