from base import *

download_blueprint = Blueprint('download_blueprint', __name__)


@download_blueprint.route('/output/<fileloc>/<filePath>',methods=['GET'])
def output_queryResult(fileloc,filePath):
    return send_file(os.path.join(os.getcwd(),"output",fileloc,filePath))



@download_blueprint.route('/uploads/<filePath>',methods=['GET'])
def showuploads(filePath):
    return send_file(os.path.join(os.getcwd(),"uploads",filePath))

@download_blueprint.route('/uploads/<subFol>/<filePath>',methods=['GET'])
def subFilesGet(subFol,filePath):
    return send_file(os.path.join(os.getcwd(),"uploads",subFol,filePath))


@download_blueprint.route('/testing',methods=['GET'])
@token_required
def testing(current_user):
    return current_user
