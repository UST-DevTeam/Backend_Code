# from base import *
# from common.config import unique_ptwtimestamp
# from blueprint_routes.ptw_blueprint import ptwRaiseTicketLogs,updateProjectLog

# app = Flask(__name__)
# scheduler = BackgroundScheduler()
# scheduler.start()








# def data_insert():

#     openpath=os.path.join("input","Query_alarms_output.csv")

#     data=cfc.jsoncsv(openpath)

#     for i in data.to_dict(orient="records"):
#         del i["Unnamed: 0"]

#         cmo.insertion("temp_data",total=i,columns=list(i.keys()),values=tuple(i.values()))



# def update_ptw_status():
#     aggr = [
#             {
#                 '$match': {
#                     'isL2Approved': True, 
#                     'status': {
#                         '$eq': 'L2-Approved'
#                     }, 
#                     'expireAt': {
#                         '$lte': int(unique_ptwtimestamp())
#                     }
#                 }
#             }, {
#                 '$project': {
#                     '_id': {
#                         '$toString': '$_id'
#                     }, 
#                     'formType': '$formType', 
#                     'mileStoneId': '$mileStoneId', 
#                     'createdBy': '$createdBy', 
#                     'siteUid': '$siteUid', 
#                     'projectuniqueId': '$projectuniqueId', 
#                     'status': '$status', 
#                     'closedAt': '$closedAt'
#                 }
#             }
#         ]
#     allData = cmo.finding_aggregate("ptwRaiseTicket" , aggr)['data']
#     print(allData,"cronJob all data get")
#     # if len(allData):
#     postData = {"status" : "Auto Closed",'isAutoClose' : True , 'closedAt' : int(unique_ptwtimestamp()) }
#     #     cmo.updating_m("ptwRaiseTicket" , {"_id" : { "$in" : [ObjectId(data['_id']) for data in allData] }} , postData)
#         # if allData[0]['status'] != 'Auto Closed':
#         #     updateProjectLog(allData[0]['formType'],'AutoClosed',allData[0]["mileStoneId"], allData[0]["createdBy"], 'PATCH', postData,allData[0]['siteUid'],allData[0]['projectuniqueId'])
#         #     ptwRaiseTicketLogs(allData[0]['formType'],'AutoClosed',allData[0]["mileStoneId"], allData[0]["createdBy"], 'PATCH', postData)
#     ptws_to_update = [data for data in allData if data['status'] != 'Auto Closed']

#     if ptws_to_update:
#         print("this is detals 68",ptws_to_update)
#         cmo.updating_m("ptwRaiseTicket",{"_id": {"$in": [ObjectId(data['_id']) for data in ptws_to_update]}},postData)
#     print("this is detals 70",ptws_to_update)
#     for data in ptws_to_update:
#         print("this is detals 72",data)
#         updateProjectLog(data['formType'],'AutoClosed',data["mileStoneId"],data["createdBy"],'PATCH',postData,data['siteUid'],data['projectuniqueId'])
#         print("this is detals 74",data)
#         ptwRaiseTicketLogs(data['formType'],'AutoClosed',data["mileStoneId"],data["createdBy"],'PATCH',postData)



# def sendalertmails(tkk):
#     tkk=tkk

#     # print(,end)


#     time=ctm.mdy_timestamp()
#     start=time+":00"
#     end=time+":59"
#     try:
            
#         # sqlQuery=f"""SELECT * FROM alertConfigInstant WHERE deleteStatus=0 AND enabled=1;"""

#         lastSendingAt=f"{ctm.curr_add_form(form='%Y-%m-%d %H:%M:00')}"


#         sqlQuery=f"""WITH TimeGapCTE AS (SELECT alertConfigInstant.*,(DATEPART(MINUTE, CONVERT(DATETIME, '{lastSendingAt}', 121) - lastSendAt)+(DATEPART(HOUR, CONVERT(DATETIME, '{lastSendingAt}', 121) - lastSendAt))*60) AS timeGap FROM alertConfigInstant)SELECT * FROM TimeGapCTE WHERE startAt <= '{start}' AND endAt >= '{end}' AND enabled=1 AND deleteStatus=0 AND timeGap >= frequency AND blockage=0;"""

        
        
#         tabledata=cmo.finding(sqlQuery)["data"]
        
#         if(len(tabledata)>0):
#             tkk=tkk+1
#             i=tabledata[0]

#             # cmo.updating("alertConfigInstant",{"id":i["id"]},{"enabled":0})



#             print(i)
#             sqlQuery=f"SELECT * FROM dbConfig where id='{i['dbServer']}' AND deleteStatus=0;"
        
#             savedServer=cmo.finding(sqlQuery)["data"][0]

#             mail_body_final=i["mailBody"]


#             mail_body_data=cmo.findFromDifferentServer(savedServer,i['mailQueryBody'])["data"]
#             print(len(mail_body_data),mail_body_data,"mail_body_datamail_body_datamail_body_data")
#             if(len(mail_body_data)>0):
                    
#                 print(mail_body_data.columns[0],"mail_body_datamail_body_data")
#                 mail_query_data=cmo.findFromDifferentServer(savedServer,i['mailQuery'])["data"]
#                 graph_query_data=cmo.findFromDifferentServer(savedServer,i['graphQuery'])["data"]


#                 # saaasadsa

#                 mail_body_data[mail_body_data.columns[0]]=mail_body_data[mail_body_data.columns[0]].astype(str)
#                 mail_body_final=mail_body_final+"<br>".join(mail_body_data[mail_body_data.columns[0]].to_list())

#                 zip_path=""
#                 if(len(mail_query_data)>0):
                        

#                     filen=ctm.fileame_mdy_timestamp()
#                     filename=os.path.join("output","graphResult",filen)
#                     destfilename=filen
#                     finalfilen=""
#                     if(i["mailOutput"]=="excel"):
#                         finalfilen=filen+".xlsx"
#                         cfc.dftoexcel(mail_query_data,filename+".xlsx")
#                     else:
#                         finalfilen=filen+".csv"
#                         cfc.dftocsv(mail_query_data,filename+".csv")
#                     dest_dir=os.path.join("output","graphResultZip")
#                     move_to=czc.fileMover(dest_dir=dest_dir,destfilename=destfilename,source_dir=os.path.join("output","graphResult"),filenames=[finalfilen])
#                     zip_path=czc.zipCreate(move_to,os.path.join(dest_dir,destfilename))


#                 imageList=[]
#                 if(len(graph_query_data)>0):
                        
#                     colsList=graph_query_data.columns.to_list()
#                     graph_query_data[colsList[0]] = pd.to_datetime(graph_query_data[colsList[0]], format='%Y-%m-%d %H:%M:%S')
#                     graph_query_data[colsList[2]] = pd.to_numeric(graph_query_data[colsList[2]])

#                     path=os.path.join("output","graphResult",filen+".jpg")

#                     savinngPath=os.path.join(os.getcwd(),path)
                    
#                     cgraph.create_line_graph(x=graph_query_data[colsList[0]],y=graph_query_data[colsList[2]],data=graph_query_data,path=path,hue=graph_query_data[colsList[1]])
#                     print(zip_path,"zip_pathzip_path")


#                     imageList.append(savinngPath)

#                 # if(zip_path!="" and len(imageList)>0):
#                 cmailer.sendmail_any_attachment(i["id"],"alertConfigInstant",i['mailQuery'],imageList,i["mailRecipients"].split(","),[],i["mailSubject"],mail_body_final,zip_path+".zip" if zip_path!="" else "","zip")


#             cmo.updating("alertConfigInstant",{"id":i["id"]},{
#                 "nextSendAt":f"cnvrtCONVERT(DATETIME, '{ctm.curr_add_form(form='%Y-%m-%d %H:%M:00',minute=int(i['frequency']))}', 121)cnvrt",
#                 "lastSendAt":f"cnvrtCONVERT(DATETIME, '{ctm.curr_add_form(form='%Y-%m-%d %H:%M:00')}', 121)cnvrt"
#             })
            
#             if(len(tabledata)>1):

#                 return sendalertmails(tkk)
#             else:
#                 return tkk
#         else:
#             return tkk
        
#     except Exception as e:
#         print(traceback.print_exc())
#         exception_type = type(e).__name__
#         exception_message = str(e)

#         # Get the line number where the exception occurred
#         exc_type, exc_value, exc_traceback = sys.exc_info()
#         line_number = exc_traceback.tb_lineno

#         # Print the error information
#         print(f"Error: {exception_type} - {exception_message} at line {line_number}")

#         print(e,i,"error error errorerrorerror112")


#         cmo.updating("alertConfigInstant",{"id":i["id"]},{
#             "blockage":f"cnvrtCONVERT(DATETIME, '{ctm.curr_add_form(form='%Y-%m-%d %H:%M:00',minute=int(i['frequency']))}', 121)cnvrt",
#             "lastSendAt":f"cnvrtCONVERT(DATETIME, '{ctm.curr_add_form(form='%Y-%m-%d %H:%M:00')}', 121)cnvrt",
#             "blockage":1
#         })



#         final={
#             "content":str(i["id"])+str(exception_message),
#             "fromTable":"sendalertmails"
#         }


#         cmo.insertion("corn_exception",total=final,columns=list(final.keys()),values=tuple(final.values()))





        
#         # return sendalertmails(tkk)

# def query_send_mail(start,end):

#     print(ctm.ymd_timestamp()+":00")

#     time=start.split(" ")[1]
#     hr=time.split(":")[0]
#     min=time.split(":")[1]
#     # print(,end)

#     sqlQuery=f"""SELECT * FROM alertConfig WHERE startAt <= '{start}' AND endAt >= '{end}' AND enabled=1 AND deleteStatus=0 AND ((DATEPART(HOUR, timeall)='{hr}' AND DATEPART(MINUTE, timeall)='{min}' AND frequency='Daily') or (DATEPART(MINUTE, timeall)='{min}' AND frequency='Hourly'));"""

#     # sqlQuery="""SELECT * FROM alertConfig WHERE startAt < '2023-12-27 11:34:00' AND endAt > '2023-12-27 12:32:00';"""
#     print(sqlQuery,"sqlQuery")


#     # dwsasadsadasdsadas


#     # sqlQuery=f"SELECT dbConfig.dbName,alertConfig.* from alertConfig JOIN dbConfig ON dbConfig.id=alertConfig.dbServer WHERE alertConfig.deleteStatus=0;"

#     tabledata=cmo.finding(sqlQuery)["data"]


#     for i in tabledata:
#         print(i)
#         sqlQuery=f"SELECT * FROM dbConfig where id='{i['dbServer']}' AND deleteStatus=0;"
    
#         savedServer=cmo.finding(sqlQuery)["data"][0]


#         mail_query_data=cmo.findFromDifferentServer(savedServer,i['mailQuery'])["data"]
#         graph_query_data=cmo.findFromDifferentServer(savedServer,i['graphQuery'])["data"]
#         zip_path=""
#         if(len(mail_query_data)>0):
                

#             filen=ctm.fileame_mdy_timestamp()
#             filename=os.path.join("output","graphResult",filen)


#             # openpath=os.path.join("input","Query_alarms_output.csv")

#             # data=cfc.jsoncsv(openpath)

#             destfilename=filen
#             # print("filenamefilenamefilename",filename+".xlsx",filename+".csv")
#             finalfilen=""
#             if(i["mailOutput"]=="excel"):
#                 finalfilen=filen+".xlsx"
#                 cfc.dftoexcel(mail_query_data,filename+".xlsx")
#             else:
#                 finalfilen=filen+".csv"
#                 cfc.dftocsv(mail_query_data,filename+".csv")

#             dest_dir=os.path.join("output","graphResultZip")
#             move_to=czc.fileMover(dest_dir=dest_dir,destfilename=destfilename,source_dir=os.path.join("output","graphResult"),filenames=[finalfilen])
#             zip_path=czc.zipCreate(move_to,os.path.join(dest_dir,destfilename))


#         imageList=[]
#         if(len(graph_query_data)>0):
                
#             colsList=graph_query_data.columns.to_list()
#             graph_query_data[colsList[0]] = pd.to_datetime(graph_query_data[colsList[0]], format='%Y-%m-%d %H:%M:%S')
#             graph_query_data[colsList[2]] = pd.to_numeric(graph_query_data[colsList[2]])

#         # print(graph_query_data['starttime'])

        

        

        
#             path=os.path.join("output","graphResult",filen+".jpg")

#             savinngPath=os.path.join(os.getcwd(),path)
#         # x = data["starttime"].to_list()
#         # y = data["Total_Drops"].to_list()

#             print() 


        
#         # print(graph_query_data[0].keys())

        

#         # dsadsadsadas
            
#             cgraph.create_line_graph(x=graph_query_data[colsList[0]],y=graph_query_data[colsList[2]],data=graph_query_data,path=path,hue=graph_query_data[colsList[1]])
#             print(zip_path,"zip_pathzip_path")


#             imageList.append(savinngPath)



    
#         if(zip_path!="" and len(imageList)>0):
#             cmailer.sendmail_any_attachment(i["id"],"",i['mailQuery'],imageList,i["mailRecipients"].split(","),[],i["mailSubject"],i["mailBody"],zip_path+".zip" if zip_path!="" else "","zip")


#     return len(tabledata)





# def every_one_five_min():
#     print("hello every_one_five_min",ctm.mdy_timestamp()+":00",ctm.add_minute_in_date(ctm.mdy_timestamp(),15)+":00")

#     start=ctm.mdy_timestamp()+":00"
#     end=ctm.add_minute_in_date(ctm.mdy_timestamp(),14)+":59"
#     val=query_send_mail(start,end)
#     final={
#         "calltype":"every_one_five_min",
#         "startTime":start,
#         "endTime":end,
#         "lenOfFile":val
#     }


#     cmo.insertion("corncaller",total=final,columns=list(final.keys()),values=tuple(final.values()))




# def every_one_min_test():
#     print("start")
#     time.sleep(10)
#     print("ennd")

# def every_one_min():

    
#     try:
            
#         print("hello every_one_min",ctm.mdy_timestamp()+":00",ctm.add_minute_in_date(ctm.mdy_timestamp(),15)+":00")
        
#         val=sendalertmails(0)
#         time=ctm.mdy_timestamp()
#         start=time+":00"
#         end=time+":59"
#         final={
#             "calltype":"every_one_min",
#             "startTime":start,
#             "endTime":end,
#             "lenOfFile":val
#         }


#         cmo.insertion("corncaller",total=final,columns=list(final.keys()),values=tuple(final.values()))

#         print("hello every_one_min")
    
#     except Exception as e:
        
#         print(e,"errorororororooror 260")

#         exception_message = str(e)
#         final={
#             "content":str(exception_message),
#             "fromTable":"every_one_min"
#         }


#         cmo.insertion("corn_exception",total=final,columns=list(final.keys()),values=tuple(final.values()))



# scheduler.add_job(every_one_min, 'cron', minute="*/1")

# scheduler.add_job(every_one_five_min, 'cron', minute="0,15,30,45")
# scheduler.add_job(update_ptw_status, 'cron', minute="*/1")


# if __name__ == '__main__':
#     app.run(debug=True,port=8092,use_reloader=False)
    
