from base import *

def dftocsv(dataf,excel_file_path):
    print(type(dataf),excel_file_path)
    dataf.to_csv(excel_file_path, index=False)
    return excel_file_path

def dftoexcel(dataf,excel_file_path):

    dataf.to_excel(excel_file_path, index=False, engine='xlsxwriter')
    return excel_file_path

def dflen(dataf):

    
    return len(dataf)


def dfjson(dataf):
    data=dataf.to_json(orient='records')
    return json.loads(data)


def jsondf(dataf):
    data=dataf.to_json(orient='records')
    return json.loads(data)


def jsoncsv(openpath):
    df=pd.read_csv(openpath)
    return df


def exceltodf(excel_file_path,rename,validate,valGames=[]):

    na_values = [""]

    newData = pd.read_excel(excel_file_path, engine="openpyxl",na_values=na_values,keep_default_na=False)
    newData.fillna('', inplace=True)
    newData = newData.replace('NaN',None)
    dataf = newData.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    dataf.columns = dataf.columns.str.strip()

    # dataf=pd.read_excel(excel_file_path)

    missing_col=set(validate)-set(dataf.columns.tolist())

    # extra_col=set(dataf.columns.tolist())-set(validate)


    if(len(missing_col)>0):
        return {
            "status":400,
            "icon":"error",
            "msg":"Some columns are missing. The column names are..\n{}".format("\n".join(missing_col))
        }
    
    dataf=dataf[list(validate)]
    
    
    for oneval in valGames:
        checker=cdv.get_invalid_value(dataf,oneval[0],oneval[1])
        if(checker["status"]!=200):
            return checker
        
    dataf.rename(columns=rename,inplace=True)

    # print(dataf,"dataf")
   

    return {
        "status":200,
        "data":dataf
    }





def exceltodfnoval(excel_file_path,rename,validate,valGames=[]):

    na_values = [""]

    newData = pd.read_excel(excel_file_path, engine="openpyxl",na_values=na_values,keep_default_na=False)
    newData.fillna('', inplace=True)
    newData = newData.replace('NaN',None)
    newData = newData.replace('',None)
    dataf = newData.applymap(lambda x: x.strip() if isinstance(x, str) else x)
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


def exceltodfnoval2(excel_file_path,rename,validate,valGames=[]):

    na_values = [""]

    newData = pd.read_excel(excel_file_path, engine="openpyxl",na_values=na_values,keep_default_na=False)
    newData.fillna('', inplace=True)
    
    dataf = newData.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    dataf.columns = dataf.columns.str.strip()



    listOfCol=dataf.columns
    print(listOfCol,'fgchlkfghjkl')
    print(validate,'djhklojfhgjkl')

    miss_col=[]
    for i in validate:
        if(i not in listOfCol):
            miss_col.append(i)
    print('ghjlkkjhgfghgjkll',miss_col)
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


def exceltodfnoval3(excel_file_path,rename,validate,valGames=[]):
    na_values = [""]
    dtype = {'Bank Account Number': str}
    newData = pd.read_excel(excel_file_path, engine="openpyxl",dtype=dtype,na_values=na_values,keep_default_na=False)
    # newData = pd.read_excel(excel_file_path, engine="openpyxl", dtype=dtype, keep_default_na=False)
    if 'Bank Account Number' in newData.columns:
        newData['Bank Account Number'] = newData['Bank Account Number'].astype(str).str.strip()
    newData.fillna('', inplace=True)
    dataf = newData.applymap(lambda x: x.strip() if isinstance(x, str) else x)
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

