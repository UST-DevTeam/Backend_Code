from base import *
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from flask_session import Session
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
import os
from email import encoders
import openpyxl
from xlsx2html.core import worksheet_to_data, render_table
from pandas import *
from xlsx2html import *
import io
import xlsxwriter
from xlsx2html import xlsx2html



def sendmail(to=[],subject="",message=""):
    msg = MIMEMultipart()
    bcc=[]
    bcc.append("amit@fourbrick.com")
    newTo=[]
    for i in to:
        if(i!=None and i!='None'):
           newTo.append(i)     
    mail_login = False if os.environ.get("MAIL_LOGIN")=="False" else True
    if(mail_login):
        print(True)
    else:
        print(False)
    password = os.environ.get("MAIL_PASSCODE")
    msg['From'] = os.environ.get("MAIL_NAME")+' <' + os.environ.get("MAIL_ID") + '>' 
    msg['To'] = ", ".join(newTo)
    msg['bcc'] = ", ".join(bcc)
    msg['Subject'] = subject

    # all_email=msg['bcc']+","+msg['To']
    all_email=newTo+bcc 
    
    # add in the message body
    msg.attach(MIMEText(message, 'plain'))
    
    #create server

    # 'smtp.gmail.com: 587'
    server = smtplib.SMTP(os.environ.get("SMTP_DETAILS"))
    
    server.starttls()
    
    # Login Credentials for sending the mail

    print("server",os.environ.get("SMTP_DETAILS"),"mail id ==>",msg['From'],"password ==>",password)

    if(mail_login):
        server.login(os.environ.get("MAIL_ID"), password)

    
    
    # send the message via the server.
    server.sendmail(os.environ.get("MAIL_ID"), all_email, msg.as_string())
    
    server.quit()
    # mailLogger={
    #     "fromtable":dbname,
    #     "uid":uid,
    #     "query":query,
    #     "fromemail":"".join(msg['From']),
    #     "toemail":"".join(msg['To']),
    #     "CC":"".join(msg['CC']),
    #     "bcc":"".join(msg['bcc']),
    #     "Subject":msg['Subject'],
    #     "message":message,
    #     "all_email":",".join(all_email),
    #     "timestamp":ctm.ymd_timestamp(),
    #     "msgas_string":msg.as_string(),
    #     "passmail":"successfully sent email from %s to %s:" % (msg['From'],",".join(all_email))
    # }
    # cmo.insertion("mailSendLogMaintainer",mailLogger)
    # print("successfully sent email from %s to %s:" % (msg['From'],msg['To']))

def sendmail_any_attachment(uid="",dbname="",query="",imageList=[],to=[],cc=[],subject="",message="",filePath="",fileType=""):
# create message object instance
    msg = MIMEMultipart()
    
    
    bcc=[]
    
    bcc.append("amit@fourbrick.com")

    newTo=[]
    for i in to:
        if(i!=None and i!='None'):
           newTo.append(i) 

    
    newcc=[]
    for i in cc:
        if(i!=None and i!='None'):
           newcc.append(i) 
    # setup the parameters of the message
           
    mail_login = False if os.environ.get("MAIL_LOGIN")=="False" else True
    password = os.environ.get("MAIL_PASSCODE")
    msg['From'] = os.environ.get("MAIL_ID")
    msg['To'] = ", ".join(newTo)
    msg['CC'] = ", ".join(newcc)
    msg['bcc'] = ", ".join(bcc)
    msg['Subject'] = subject

    # all_email=msg['bcc']+","+msg['To']+","+msg['CC'] 
    all_email=newcc+newTo+bcc
    # add in the message body

    finneimage=[]
    for img in range(len(imageList)):
        fp = open(imageList[img], 'rb')
        image = MIMEImage(fp.read())
        image.add_header('Content-ID', '<image'+str(img)+'>')
        fp.close()
        msg.attach(image)

        finneimage.append('<img width="100%" src="cid:image'+str(img)+'" alt="img" />')


    message=message+"<br>"+"<br>".join(finneimage)
    msg.attach(MIMEText(message, 'html'))
    
    #create server

    # 'smtp.gmail.com: 587'
    server = smtplib.SMTP(os.environ.get("SMTP_DETAILS"))
    
    server.starttls()

    print(filePath.split("/")[-1])

    # bdasdadas

    if(filePath!=""): 
        with open(filePath,'rb') as file:
            msg.attach(MIMEApplication(file.read(),Name=filePath.split("/")[-1]))

    # part = MIMEBase('application','vnd.ms-excel')
    # part.add_header('Content-Disposition', 'attachment', filename=fileNewName+".xlsx")
    
    # Login Credentials for sending the mail
            
    if(mail_login):
        server.login(msg['From'], password)

    
    
    # send the message via the server.
    server.sendmail(msg['From'], all_email, msg.as_string())
    
    server.quit()
    mailLogger={
        "fromtable":dbname,
        "uid":uid,
        "query":query,
        "fromemail":"".join(msg['From']),
        "toemail":"".join(msg['To']),
        "CC":"".join(msg['CC']),
        "imageList":",".join(imageList),
        "bcc":"".join(msg['bcc']),
        "Subject":msg['Subject'],
        "filePath":filePath,
        "message":message,
        "all_email":",".join(all_email),
        "timestamp":ctm.ymd_timestamp(),
        "msgas_string":msg.as_string(),
        "passmail":"successfully sent email from %s to %s:" % (msg['From'],",".join(all_email))
    }


    
    cmo.insertion("maillogger",total=mailLogger,columns=list(mailLogger.keys()),values=tuple(mailLogger.values()))
    # cmo.insertion("mailSendLogMaintainer",mailLogger)
    print("successfully sent email from %s to %s:" % (msg['From'],msg['To']))


def formatted_sendmail(to,cc,subject,message,type=None):
# create message object instance
    msg = MIMEMultipart()
    
    bcc=[]

    newTo=[]
    for i in to:
        if(i!=None and i!='None'):
           newTo.append(i) 

    newcc=[]
    for i in cc:
        if(i!=None and i!='None'):
           newcc.append(i) 
    
    
    # setup the parameters of the message
           
    mail_login = False if os.environ.get("MAIL_LOGIN")=="False" else True

    password = os.environ.get("MAIL_PASSCODE")
    msg['From'] = os.environ.get("MAIL_ID")


    if type == "user":
        password = "vsyj qwgr qbdg eakw"
        msg.replace_header("From", "ust.expenseuser@gmail.com")

    elif type == "L1":
        password = "zzkd xoch hiqp nobb"
        msg.replace_header("From", "ustexpanseuser1@gmail.com")
    
    



    msg['To'] = ", ".join(newTo)
    msg['Cc'] = ", ".join(newcc)
    msg['Bcc'] = ", ".join(bcc)
    msg['Subject'] = subject
    

    
    # add in the message body
    msg.attach(MIMEText(message, 'html'))
    
    all_email=newcc+newTo+bcc
    
    #create server

    server = smtplib.SMTP(os.environ.get("SMTP_DETAILS"))
    
    # print(server,"server")

    server.starttls()
    # Login Credentials for sending the mail
    
    # print(msg['From'],password)
    
    if(mail_login):
        server.login(msg['From'], password)
    
    # send the message via the server.
    # print(msg.as_string())
    server.sendmail(msg['From'], all_email, msg.as_string())
    
    server.quit()

    mailLogger={
        "fromtable":msg['From'],
        "uid":msg['From'],
        "fromtable":msg['From'],
        "to":msg['To'],
        "CC":msg['Cc'],
        "bcc":msg['Bcc'],
        "Subject":msg['Subject'],
        "message":message,
        # "timestamp":ctm.ymd_timestamp,
        "all_email":all_email,
        "msgas_string":msg.as_string(),
        "passmail":"successfully sent email from %s to %s:" % (msg['From'],all_email)
    }
    cmo.insertion("maillogger",mailLogger)

def ptw_sendmail(to,cc,subject,message):
    msg = MIMEMultipart()
    bcc=[]

    # bcc.append("shashank.verma@fourbrick.com")
    # bcc += ["aman.singh@fourbrick.com"]
    newTo=[]
    for i in to:
        if(i!=None and i!='None'):
           newTo.append(i) 

    newcc=[]
    for i in cc:
        if(i!=None and i!='None'):
           newcc.append(i)  
    # print(",sajbdfjkaskdjfj",os.environ.get("MAIL_ID"))
    # mail_login = False if os.environ.get("MAIL_LOGIN")=="False" else True
    mail_login=True#os.environ.get("MAIL_LOGIN")
    password ="qdmu nmlq taiq xcqn"# os.environ.get("MAIL_PASSCODE")

    msg['From'] = "usttelco@gmail.com"#os.environ.get("MAIL_ID")
    msg['To'] = ", ".join(newTo)
    msg['Cc'] = ", ".join(newcc)
    msg['Bcc'] = ", ".join(bcc)
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'html'))

    all_email=newcc+newTo+bcc
    
    # server = smtplib.SMTP(os.environ.get("SMTP_DETAILS"))
    server = smtplib.SMTP("smtp.gmail.com:587")

    print(msg,"msggggggggggggggggggggggggggggggggg")
    server.starttls()
        
    if(mail_login):
        server.login(msg['From'], password)

    server.sendmail(msg['From'], all_email, msg.as_string())
    
    server.quit()

    mailLogger={
        "fromtable":msg['From'],
        "uid":msg['From'],
        "fromtable":msg['From'],
        "to":msg['To'],
        "CC":msg['Cc'],
        "bcc":msg['Bcc'],
        "Subject":msg['Subject'],
        "message":message,
        # "timestamp":ctm.ymd_timestamp,
        "all_email":all_email,
        "msgas_string":msg.as_string(),
        "passmail":"successfully sent email from %s to %s:" % (msg['From'],all_email)
    }

    cmo.insertion("maillogger",mailLogger)
