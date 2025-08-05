from base import *
app = Flask(__name__)

from blueprint_routes.admin_blueprint import admin_blueprint as admin_blueprint
from blueprint_routes.auth_blueprint import auth_blueprint as auth_blueprint
from blueprint_routes.hr_blueprint import hr_blueprint as hr_blueprint
from blueprint_routes.download_blueprint import download_blueprint as download_blueprint
from blueprint_routes.location_blueprint import location_blueprint as location_blueprint
from blueprint_routes.myHome_blueprint import myHome_blueprint as myHome_blueprint
from blueprint_routes.common_blueprint import common_blueprint as common_blueprint
from blueprint_routes.export_blueprint import export_blueprint as export_blueprint
from blueprint_routes.project_blueprint import project_blueprint as project_blueprint
from blueprint_routes.vendor_management_blueprint import vendor_management_blueprint as vendor_management_blueprint
from blueprint_routes.role_blueprint import role_blueprint as role_blueprint
from blueprint_routes.poLifeCycle_blueprint import poLifeCycle_blueprint as poLifeCycle_blueprint
from blueprint_routes.upgrade_blueprint import upgrade_blueprint as upgrade_blueprint
from blueprint_routes.forms_blueprint import forms_blueprint as forms_blueprint
from blueprint_routes.filter_blueprint import filter_blueprint as filter_blueprint
from blueprint_routes.expenses_blueprint import expenses_blueprint as expenses_blueprint
from blueprint_routes.approval_blueprint import approval_blueprint as approval_blueprint
from blueprint_routes.graph_blueprint import graph_blueprint as graph_blueprint
from blueprint_routes.employee_blueprint import employee_blueprint as employee_blueprint
from blueprint_routes.mobile_blueprint import mobile_blueprint as mobile_blueprint
from blueprint_routes.currentuser_blueprint import currentuser_blueprint as currentuser_blueprint
from blueprint_routes.sample_blueprint import sample_blueprint as sample_blueprint
from blueprint_routes.gpTracking import gpTracking_blueprint as gpTracking_blueprint
from blueprint_routes.gpTrackingScheduler import gpTrackingScheduler_blueprint as gpTrackingScheduler_blueprint
from blueprint_routes.gpTrackingScheduler import get_gpTracking as get_gpTracking
from blueprint_routes.gpTrackingScheduler import getcccdc as getcccdc
from blueprint_routes.ptw_blueprint import ptw_blueprint as ptw_blueprint



app.config["SECRET_KEY"]=os.environ.get('SECRET_KEY')
app.register_blueprint(auth_blueprint)
app.register_blueprint(admin_blueprint)
app.register_blueprint(location_blueprint)
app.register_blueprint(download_blueprint)
app.register_blueprint(hr_blueprint)
app.register_blueprint(myHome_blueprint)
app.register_blueprint(common_blueprint)
app.register_blueprint(export_blueprint)
app.register_blueprint(project_blueprint)
app.register_blueprint(vendor_management_blueprint)
app.register_blueprint(role_blueprint)
app.register_blueprint(poLifeCycle_blueprint)
app.register_blueprint(upgrade_blueprint)
app.register_blueprint(forms_blueprint)
app.register_blueprint(filter_blueprint)
app.register_blueprint(expenses_blueprint)
app.register_blueprint(approval_blueprint)
app.register_blueprint(graph_blueprint)
app.register_blueprint(employee_blueprint)
app.register_blueprint(mobile_blueprint)
app.register_blueprint(currentuser_blueprint)
app.register_blueprint(sample_blueprint)
app.register_blueprint(gpTracking_blueprint)
app.register_blueprint(gpTrackingScheduler_blueprint)
app.register_blueprint(ptw_blueprint)


@app.route('/',methods=['GET'])
def home():
    
    return "<h1>Welcome to Backend Server...</h1>"

def my_function():
    get_gpTracking()


schedule.every(30).minutes.do(my_function)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(10)  


scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()




@app.route('/version',methods=['GET'])
def version():
    return "ver0.0.0" 
CORS(app)


cfsocket.socketio.init_app(app)
cfsocket.socketio.run(app,port=7585,host="0.0.0.0", debug=True,allow_unsafe_werkzeug=True)