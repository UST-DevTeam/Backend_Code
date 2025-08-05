
def get_invalid_value(data,colname,validdata):
    
    filtered_rows = data[~data[colname].isin(validdata)]

    if(len(filtered_rows)>0):
        return {
            "status":400,
            "icon":"error",
            "msg":"You Have Invalid Data in "+colname+". This Column have invalid value is "+",".join(filtered_rows[colname].to_list()),
            "data":filtered_rows[colname].to_list()
        }
    else:
        return {
            "status":200,
            "msg":"Good Passed",
            "data":[]
        }