from base import *

def excelFileWriter(dataframe,fileName,sheetName):

    fullPath = os.path.join(os.getcwd(), "downloadFile",fileName+".xlsx")
    newExcelWriter = pd.ExcelWriter(fullPath, engine="xlsxwriter")
    dataframe.to_excel(newExcelWriter, index=False, sheet_name=sheetName)
    
    

    workbook = newExcelWriter.book
    worksheet = newExcelWriter.sheets[sheetName]
    worksheet.set_tab_color('#FFFF00')

    header_format = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vtop',
        'bg_color': '#24292d', 
        'border':1,
        'border_color':'black',
        'color':'white'
    })
    for col_num, value in enumerate(dataframe.columns.values):
        worksheet.write(0, col_num, value, header_format)
    
    cell_format = workbook.add_format({
        'align':'center',
        'valign':'vtop',
    })

    worksheet.set_column(0, len(dataframe.columns) - 1, 20, cell_format)
    newExcelWriter.close()
    return fullPath