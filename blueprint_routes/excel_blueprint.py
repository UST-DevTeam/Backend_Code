from base import *

excel_blueprint = Blueprint('excel_blueprint', __name__)


def excelFileWriter(dataframe,fileName):
    fullPath = os.path.join(os.getcwd(), "downloadFile", "Export_Circle.xlsx")
    newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
    dataframe.to_excel(newExcelWriter, index=False, sheet_name="Circle")
    workbook = newExcelWriter.book
    worksheet = newExcelWriter.sheets["Circle"]
    worksheet.set_tab_color('#FFFF00')
    header_format = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vtop',
        'bg_color': '#92d050', 
        'border':1,
        'border_color':'black'
    })
    for col_num, value in enumerate(dataframe.columns.values):
        worksheet.write(0, col_num, value, header_format)
    
    cell_format = workbook.add_format({
        'align':'center',
        'valign':'vtop',
    })

    cols = len(dataframe.axes[1])
    start_column = 0  # Starting column index
    end_column = cols    # Ending column index
    width = 20 
    for col in range(start_column, end_column):
        worksheet.set_column(col, col, width, cell_format)
    newExcelWriter.close()
    return send_file(os.path.join(os.getcwd(),"downloadFile","Export_Circle.xlsx"))


def dataframefunc(exceldata):
    dbData = [
        {
            '$project': {
                'customer': "$customerName", 
                'uniqueId': '$_id'
            }
        },{
            '$project':{
                '_id':0
            }
        }
    ]
    customer = cmo.finding_aggregate("customer",dbData)