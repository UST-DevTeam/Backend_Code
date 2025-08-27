import os
from datetime import datetime,timezone,timedelta
from PIL import Image as Image2
import io
import pytz
import time
import re
from pdf2image import convert_from_path
from bson import ObjectId
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter, PdfFileReader
import pandas as pd
import PyPDF2
from dateutil import parser
from flask import *
# from base import *
# from mongo_operations import finding_aggregate_with_deleteStatus as finding_aggregate_with_deleteStatus


# import fitz

time_zone="Asia/Kolkata"
# time_zone="America/Mexico_City"
# time_zone="America/North_Dakota/Center"
def unique_timestamp():
    now = datetime.now()
    return str(datetime.timestamp(now)).split(".")[0]+str(datetime.timestamp(now)).split(".")[1]

def u_timestamp(addtime=None):
    now = datetime.now()
    if(addtime):
        now=now+addtime
    return int(str(datetime.timestamp(now)).split(".")[0])


def strtodate(date_string):
    return datetime.fromisoformat(date_string)



def date_u_timestamp(date1):
    
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')

    return int(str(datetime.timestamp(startdate)).split(".")[0])

def datetimeObj():
    now = datetime.now()
    return now

# def updatedatetimeObj():
#     now = datetime.now()
#     india_timezone = pytz.timezone('Asia/Kolkata')
#     now_india = now.replace(tzinfo=pytz.utc).astimezone(india_timezone)
#     formatted_date = now_india.strftime("%Y-%m-%d"+"T00:00:00"+"+05:30")
#     return formatted_date

def new_updatedatetimeObj():
    current_time = datetime.now().strftime("%m-%d-%Y")
    return current_time


def timestamp():
    return '{:%m/%d/%Y-%H:%M}'.format(datetime.now())


def mdy_timestamp():
    IST = pytz.timezone(time_zone)
    
    return '{:%m/%d/%Y %H:%M}'.format(datetime.now(IST))


def ymd_timestamp():
    IST = pytz.timezone(time_zone)
    
    return '{:%Y/%m/%d %H:%M}'.format(datetime.now(IST))




def convert_datetime_to_custom_format(date_str, time_zone="Asia/Kolkata"):
    """
    Check if a string is in ISO 8601 format with a timezone. If it is, convert it to the specified
    timezone and format it to 'DD-MMM-YYYY'. Otherwise, return the original string.

    Parameters:
    - date_str (str): The string to check and convert.
    - time_zone (str): The time zone to convert the datetime to.

    Returns:
    - str: The formatted datetime string if the input matches the ISO 8601 format with a timezone,
           otherwise the original string.
    """
    iso8601_regex = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)$'
    
    if re.match(iso8601_regex, date_str):
        try:
            # Parse the ISO 8601 formatted datetime string
            date_obj = datetime.fromisoformat(date_str)

            
            target_timezone = pytz.timezone(time_zone)

            
            date_obj_tz = date_obj.astimezone(target_timezone)

            
            date_obj_naive = date_obj_tz.replace(tzinfo=None)

            
            formatted_date = date_obj_naive.strftime('%d-%b-%Y').lower()

            return formatted_date
        except Exception as e:
            
            return date_str
    else:
        
        return date_str


def fileame_mdy_timestamp():
    IST = pytz.timezone(time_zone)
    
    return '{:%m%d%Y%H%M}'.format(datetime.now(IST))


def date_mdy_timestamp():
    IST = pytz.timezone(time_zone)
    
    return '{:%m/%d/%Y}'.format(datetime.now(IST))

def date_mdy_CST():
    # IST = pytz.timezone(time_zone)
    CST = pytz.timezone('US/Central')
    return '{:%m/%d/%Y}'.format(datetime.now(CST))


def mdy_timestampwof(merge):
    IST = pytz.timezone(time_zone)
    
    return '{:%m_%d_%Y_%H_%M}'.format(datetime.now(IST))
    #hbxhjdsbxhjndvxhn

def dash_mdy_timestamp(date1):
    IST = pytz.timezone(time_zone)
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    
    return '{:%Y-%d-m%}'.format(datetime.now(startdate))

def curr_add_form(form='%m/%d/%Y %H:%M',minute=0,second=0,hour=0,day=0,month=0,year=0):

    IST = pytz.timezone(time_zone)
    final=datetime.now(IST)+timedelta(days=day, hours=hour, minutes=minute, seconds=second)
    formatted_date = final.strftime(form)
    return formatted_date


def mdy_timestamp_week():
    IST = pytz.timezone(time_zone)
    
    return '{:%U}'.format(datetime.now(IST))


def strtomdy_timestamp(date):
    IST = pytz.timezone(time_zone)
    
    return '{:%m/%d/%Y %H:%M}'.format(datetime.now(IST))


def agebtwtwodate_timestamp(date1,date2,returntype):
    IST = pytz.timezone(time_zone)
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    enddate=datetime.strptime(date2, '%m/%d/%Y %H:%M')

    cmopared=enddate-startdate
    cmopared.total_seconds()

    return cmopared.total_seconds()
    
    return '{:%m/%d/%Y %H:%M:%S}'.format(datetime.now(IST))





def agetwodate_timestamp(date1,date2,returntype):
    IST = pytz.timezone(time_zone)
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    enddate=datetime.strptime(date2, '%m/%d/%Y %H:%M')
    # print(enddate,"enddate",startdate,"startdate")
    cmopared=enddate-startdate
    # cmopared.total_seconds()
    # print(dasdsa)
    return cmopared
    
    return '{:%m/%d/%Y %H:%M:%S}'.format(datetime.now(IST))


def agetwodate_w_timestamp(date1,date2,returntype):
    IST = pytz.timezone(time_zone)
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    enddate=datetime.strptime(date2, '%m/%d/%Y %H:%M')
    # print(enddate,"enddate",startdate,"startdate")
    cmopared=enddate-startdate
    # print(cmopared)
    days, seconds = cmopared.days, cmopared.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    # cmopared.total_seconds()

    # print("0"+str(hours)+":00" if len(str(hours))==1 else str(hours)+":00",minutes)
    # return "0"+str(hours)+":00" if len(str(hours))==1 else str(hours)+":00"


def difference_of_two(date1,date2):
    IST = pytz.timezone(time_zone)
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    enddate=date2.split(":")[0]
    # print(enddate,"enddate",startdate,"startdate")
    cmopared=startdate+timedelta(hours=int(enddate))
    # cmopared.total_seconds()
    
    # print(cmopared,"cmopared")

    
    return '{:%m/%d/%Y %H:%M}'.format(cmopared)


def difference_of_twousinghour(date1,date2):
    IST = pytz.timezone(time_zone)
    startdate=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    cmopared=startdate+timedelta(hours=int(date2))
    # cmopared.total_seconds()
    
    # print(cmopared,"cmopared")

    
    return '{:%m/%d/%Y %H:%M}'.format(cmopared)



def mobiledateFormat(type,date):
    if(type=="Full"):
        startdate=datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        return '{:%m/%d/%Y %H:%M}'.format(startdate) 
    
    if(type=="custom"):
        startdate=datetime.strptime(date, '%H:%M')
        return '{:%H:%M}'.format(startdate) 
    





def add_day_in_date(dayaddOn):
    IST = pytz.timezone(time_zone)

    nextDate=datetime.now(IST)+timedelta(days=dayaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)

def add_hour_in_udate(date1,dayaddOn):
    IST = pytz.timezone(time_zone)
    initial_date=date1
    nextDate=initial_date+timedelta(days=dayaddOn) 
    return nextDate

def add_hour_in_udate2(date1,dayaddOn):
    IST = pytz.timezone(time_zone)
    date1=strtodate(date1)
    initial_date=date1
    # print('jdjkjkjkjkkjkje',type(initial_date),initial_date)
    # print('djjjfjfjf',timedelta,type(timedelta))
    nextDate=initial_date+timedelta(days=dayaddOn)
    
    return nextDate


def subtract_day_in_date(dayaddOn):
    IST = pytz.timezone(time_zone)

    nextDate=datetime.now(IST)-timedelta(days=dayaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)


def add_hour_in_date(date1,hoursaddOn):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')

    nextDate=initial_date+timedelta(hours=hoursaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)


def add_minute_in_date(date1,minuteaddOn):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')

    nextDate=initial_date+timedelta(minutes=minuteaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)


def add_minute_sec_in_date(date1,minuteaddOn,secondaddOn):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')

    nextDate=initial_date+timedelta(minutes=minuteaddOn,seconds=secondaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)


def sub_hour_in_date(date1,hoursaddOn):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')

    nextDate=initial_date-timedelta(hours=hoursaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)

def sub__in_date(date1,hoursaddOn):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')

    nextDate=initial_date-timedelta(hours=hoursaddOn)
    
    return '{:%m/%d/%Y %H:%M}'.format(nextDate)


def time_from_date(date1):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    
    return '{:%H:%M}'.format(initial_date)


def time_from_date_no_ss(date1):
    IST = pytz.timezone(time_zone)
    initial_date=datetime.strptime(date1, '%m/%d/%Y %H:%M')
    
    return '{:%H:%M}'.format(initial_date)



def stringdatetodate(date1,form='%m/%d/%Y %H:%M'):
    UTC = pytz.timezone('UTC')
    initial_date=datetime.strptime(date1, form)
    
    # print(initial_date.year,initial_date.month,initial_date.day,initial_date,"initial_date")
    return initial_date

def convert_to_timestamp4(date_obj):
    """Convert a datetime object to a timestamp string."""
    return date_obj.strftime('%Y-%m-%dT%H:%M:%S')

def check_and_convert_date(timestamp):
    try:
        timestamp_sec = timestamp / 1000
        date_obj = datetime.fromtimestamp(timestamp_sec)
        formatted_date = date_obj.strftime('%Y-%m-%d'+'T00:00:00')
        return formatted_date
    except Exception as e:
        return timestamp
    
def check_and_convert_date3(timestamp):
    
    try:
        timestamp_sec = timestamp / 1000
        date_obj = datetime.fromtimestamp(timestamp_sec)
        formatted_date = date_obj.strftime('%Y-%m-%dT%H:%M:%S')
        return formatted_date
    except Exception as e:
        # print(f"Error: {e}")
        return timestamp
def check_and_convert_date2(timestamp):
    try:
        timestamp_sec = timestamp / 1000
        date_obj = datetime.fromtimestamp(timestamp_sec)
        formatted_date = date_obj.strftime('%Y-%m-%dT%H:%M:%S')
        return formatted_date
    except Exception as e:
        try:
            timestamp=get_current_date_timestamp()
            timestamp_sec = timestamp / 1000
            date_obj = datetime.fromtimestamp(timestamp_sec)
            formatted_date = date_obj.strftime('%Y-%m-%dT%H:%M:%S')
            return formatted_date
        except Exception as e:
            
            # print(f"Error: {e}")
            return None
        
        # print(f"Error: {e}")
    
    
def convert_to_iso_with_timezone2(date_str):
    # print("ggggtttt",date_str)
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        ist = pytz.timezone('Asia/Kolkata')
        date_obj_ist = ist.localize(date_obj)
        formatted_date = date_obj_ist.isoformat()
        return formatted_date
    except Exception as e:
        return date_str


## Sandeep Function

def get_client_ip():
    client_ip = request.remote_addr
    return f"Client IP: {client_ip}", 200

def get_financial_year(date):
    year = date.year
    if date.month >= 4: 
        return f'{year}-{year + 1}'
    else:
        return f'{year - 1}-{year}'

def checkngStringtimeStamp(strh):
    if type(strh)== str:
        if len(strh) == 13:
            return True
        else:
            return False
        
        
def changedThings(allData, allData2):
    allData3 = []

    for key in allData:
        if key in allData2 and allData[key] != allData2[key]:
            if is_valid_mongodb_objectid(allData[key]) and is_valid_mongodb_objectid(allData2[key]):
                change_info = {
                    "changedKey": key,
                    "Oldvalue": ObjectId(allData[key]),
                    "Newvalue": ObjectId(allData2[key]),
                    'type':'OBJECTID'
                }
                allData3.append(change_info)
            else:
                change_info = {
                    "changedKey": key,
                    "Oldvalue": allData[key],
                    "Newvalue": allData2[key],
                    'type':'STRING'
                }
                allData3.append(change_info)
                
    return allData3      

        
def changedThings2(allData, allData2):
    allData3 = ""

    for key in allData:
        if key in allData2 and allData[key] != allData2[key]:
            change_info = {
                "changedKey": key,
                "Oldvalue": allData[key],
                "Newvalue": allData2[key],
                'type':'STRING'
            } 
            allData3=allData3+change_info['changedKey']+","
    return allData3      

def changedThings5(allData, allData2):
    allData3 = []

    for key in allData:
        if key in allData2 and allData[key] != allData2[key]:
            change_info = {
                "changedKey": key,
                "Oldvalue": allData[key],
                "Newvalue": allData2[key],
                'type':'STRING'
            } 
            allData3.append(change_info['changedKey'])
    return allData3 

def changedThings3(data1, data2):
    changes = []
    fields_to_check = ['dataType', 'required', 'dropdownValue', 'childView', 'Status']
    for key in data1:
        if key in data2:
            
            for item1, item2 in zip(data1[key], data2[key]):
                
                if item1['fieldName'] != item2['fieldName']:
                    change_info = {
                        "changedKey": 'fieldName',
                        "Oldvalue": item1['fieldName'],
                        "Newvalue": item2['fieldName'],
                        'type': 'STR',
                        'field': 'fieldName'
                    }
                    changes.append(change_info)

                if item1['fieldName'] == item2['fieldName']:
                    for field in fields_to_check:
                        if item1[field] != item2[field]:
                            change_info = {
                                "changedKey": item1['fieldName'],
                                "Oldvalue": item1[field],
                                "Newvalue": item2[field],
                                'type': type(item1[field]).__name__.upper(),
                                'field': field
                            }
                            changes.append(change_info)

    return changes

def changedThings4(data1, data2):
    changes = []
    fields_to_check = ["Completion Criteria", "Estimated Time (Days)", "Predecessor", "Status", "WCC Sign off", "fieldName", "index"]
    for key in data1:
        if key in data2:
            
            for item1, item2 in zip(data1[key], data2[key]):
                
                if item1['fieldName'] != item2['fieldName']:
                    change_info = {
                        "changedKey": 'fieldName',
                        "Oldvalue": item1['fieldName'],
                        "Newvalue": item2['fieldName'],
                        'type': 'STR',
                        'field': 'fieldName'
                    }
                    changes.append(change_info)

                if item1['fieldName'] == item2['fieldName']:
                    for field in fields_to_check:
                        if item1[field] != item2[field]:
                            change_info = {
                                "changedKey": item1['fieldName'],
                                "Oldvalue": item1[field],
                                "Newvalue": item2[field],
                                'type': type(item1[field]).__name__.upper(),
                                'field': field
                            }
                            changes.append(change_info)

    return changes

def tempfunction():
    current_time = datetime.now().strftime('%H:%M:%S')
    return current_time
    
def resize_image(image_path):
    try:
        temp_path = image_path + '.temp'
        with open(image_path, 'rb') as f:
            img = Image2.open(f)
            original_size = os.path.getsize(image_path)
            # print(f"Original size: {original_size} bytes")

            if original_size > 30720:  # Resize if greater than 30 KB
                quality = 95
                buffer = io.BytesIO()
                while original_size > 30720 and quality > 0:
                    buffer.seek(0)
                    img.save(buffer, format='JPEG', quality=quality)
                    original_size = buffer.tell()
                    quality -= 5
                with open(temp_path, 'wb') as out_f:
                    out_f.write(buffer.getvalue())
                # print(f"Resized image saved at temporary path: {temp_path}, Size: {original_size} bytes")
            else:
                with open(temp_path, 'wb') as out_f:
                    out_f.write(f.read())
            os.replace(temp_path, image_path)
            # print(f"Replaced original image with resized image: {image_path}")
            return image_path
    except Exception as e:
        # print(f"An error occurred while resizing the image: {e}")
        return None

def resize_pdf(pdf_path):
    try:
        temp_path = pdf_path + '.temp.pdf'
        reader = PdfReader(pdf_path)
        writer = PdfWriter()

        for page in reader.pages:
            writer.add_page(page)

        with open(temp_path, 'wb') as f_out:
            writer.write(f_out)
        
        original_size = os.path.getsize(pdf_path)
        new_size = os.path.getsize(temp_path)
        # print(f"Original PDF size: {original_size} bytes, Resized PDF size: {new_size} bytes")

        os.replace(temp_path, pdf_path)
        # print(f"Replaced original PDF with resized PDF: {pdf_path}")
        return pdf_path
    except Exception as e:
        # print(f"An error occurred while resizing the PDF: {e}")
        return None

def process_file(file_path):
    file_name, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()
    if file_extension in ('.png', '.jpg', '.jpeg', '.gif', '.bmp'):
        return resize_image(file_path)
    elif file_extension == '.pdf':
        return resize_pdf(file_path)
    else:
        return None

def unique_timestampexpense():
    gmt_plus_10 = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(gmt_plus_10)
    future_time = current_time + timedelta(hours=5,minutes=30)
    return str(future_time.timestamp() * 1000).split(".")[0]


def unique_ptwtimestamp():
    india_tz = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(india_tz)
    return str(int(current_time.timestamp() * 1000))


def unique_8hoursPlusTimestamp():
    india_tz = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(india_tz)
    future_time = current_time + timedelta(hours=8)
    return str(int(future_time.timestamp() * 1000))


def datestamp():
    formatted_date = datetime.fromtimestamp(int(unique_timestampexpense()) / 1000).strftime("%d%m%Y")
    return formatted_date

def make_timestamp(date_str, timezone='Asia/Kolkata'):
    
    try:
        naive_date_obj = parser.parse(str(date_str))
        tz = pytz.timezone(timezone)
        localized_date_obj = tz.localize(naive_date_obj)
        timestamp = int(time.mktime(localized_date_obj.timetuple()))
        timestamp=timestamp * 1000
        return timestamp
    except Exception as e:
        return date_str

datetime_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}')
datetime_pattern1 = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\-\d{2}:\d{2}')
datetime_pattern2 = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{2}:\d{2}')
datetime_pattern3 = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}:\d{2}:\d{2}')
datetime_pattern4 = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
datetime_with_timezone_pattern = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}')
custom_datetime_pattern = re.compile(r'\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}')
custom_datetime_pattern2 = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
custom_datetime_pattern3 = re.compile(r'\d{2}-\d{2}-\d{4}')
custom_datetime_pattern4 = re.compile(r'\d{4}-\d{2}-\d{2}')
unix_timestamp_pattern = re.compile(r'^\d{10,13}$')
float_timestamp_pattern = re.compile(r'^\d+\.\d+$')
date_pattern = re.compile(r'\d{2}-[A-Za-z]{3}-\d{4}')

def convertToDateBulkExport(date_string):
    if datetime_pattern.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
        
    elif datetime_pattern1.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
    
    elif datetime_pattern2.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string  
    elif datetime_with_timezone_pattern.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%d-%m-%YT%H:%M:%S+HH:MM', errors='coerce')
            if not pd.isnull(dt):
                return dt.date()
            else:
                return date_string
        except ValueError:
            return date_string 
    
    elif datetime_pattern3.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%d-%m-%YT%H:%M:%S', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
    elif datetime_pattern4.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%dT%H:%M:%S', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
        
      
         
    elif custom_datetime_pattern.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%d-%m-%Y %H:%M:%S', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
    
    elif custom_datetime_pattern2.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%d %H:%M:%S', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
    
    elif custom_datetime_pattern3.match(str(date_string)):
        try:
            dt = pd.to_datetime(date_string, format='%d-%m-%Y', errors='coerce')
            
            if not pd.isnull(dt):
                date_only = dt.date()
                
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
        
    elif custom_datetime_pattern4.match(str(date_string)): 
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%d', errors='coerce')
            if not pd.isnull(dt):
                date_only = dt.date()
                return date_only
            else:
                return date_string
        except ValueError:
            return date_string
        
    elif unix_timestamp_pattern.match(str(date_string)):
        try:
            timestamp_sec = int(date_string)
            if len(str(date_string)) > 10:
                timestamp_sec = timestamp_sec / 1000
            dt = pd.to_datetime(timestamp_sec, unit='s', errors='coerce')
            if not pd.isnull(dt):
                dt = dt.tz_localize('UTC').tz_convert('Asia/Kolkata')
                return dt.date()
            else:
                return date_string
        except ValueError:
            return date_string  
        
    elif float_timestamp_pattern.match(str(date_string)):
        try:
            timestamp_sec = float(date_string)
            timestamp_sec = int(timestamp_sec) / 1000 
            dt = pd.to_datetime(timestamp_sec, unit='s', errors='coerce')
            if not pd.isnull(dt):
                dt = dt.tz_localize('UTC').tz_convert('Asia/Kolkata')
                return dt.date()
            else:
                return date_string
        except ValueError:
            return date_string
    
      
    elif date_pattern.match(str(date_string)):
        try:
            date_obj = datetime.strptime(date_string, "%d-%b-%Y")
            ist = pytz.timezone('Asia/Kolkata')
            date_obj_ist = ist.localize(date_obj)
            return date_obj_ist.date()
        except ValueError:
            return date_string        
    else:
        return date_string
    
    

 

    
    

def convertToDateBulkExport2(datetime_str):
    try:
        pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}'
        if re.fullmatch(pattern, datetime_str):
            dt = datetime.fromisoformat(datetime_str)
            kolkata_tz = pytz.timezone('Asia/Kolkata')
            dt = dt.astimezone(kolkata_tz)
            date_str = dt.strftime('%Y-%m-%d')
            return date_str
        else:
            return datetime_str
    except (ValueError, pytz.UnknownTimeZoneError, TypeError) as e:
        return datetime_str



def convert_date(date_str):
    try:
        if pd.notnull(date_str) and date_str != "" and isinstance(date_str, pd.Timestamp):
            date_obj = date_str.to_pydatetime()
            date_obj = time_zone.localize(date_obj)
            return date_obj.isoformat()
        else:
            return date_str
    except Exception as e:
        
        return None



def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def generate_new_ExpenseNo(last_id):
    print('last_id')
    pattern = r"^(EXP/\d{2}-\d{2}/)(\d{6})$"
    match = re.match(pattern, last_id)
    if match:
        base_part = match.group(1)
        numerical_part = match.group(2)
        new_numerical_part = int(numerical_part) + 1
        new_numerical_part_str = f"{new_numerical_part:06d}"  # Ensure it remains 6 digits long
        new_id = f"{base_part}{new_numerical_part_str}"
        return new_id
    # else:
    #     raise ValueError("Invalid ID format")


def generate_new_AdvanceNo(last_id):
    pattern = r"^(ADV/\d{2}-\d{2}/)(\d{6})$"
    match = re.match(pattern, last_id)
    if match:
        base_part = match.group(1)
        numerical_part = match.group(2)
        new_numerical_part = int(numerical_part) + 1
        new_numerical_part_str = f"{new_numerical_part:06d}"  # Ensure it remains 6 digits long
        new_id = f"{base_part}{new_numerical_part_str}"
        return new_id
    # else:
    #     raise ValueError("Invalid ID format")
    
def generate_new_SettlementID(last_id):
    pattern = r"^(SET/\d{2}-\d{2}/)(\d{6})$"
    match = re.match(pattern, last_id)
    if match:
        base_part = match.group(1)
        numerical_part = match.group(2)
        new_numerical_part = int(numerical_part) + 1
        new_numerical_part_str = f"{new_numerical_part:06d}"  # Ensure it remains 6 digits long
        new_id = f"{base_part}{new_numerical_part_str}"
        return new_id



def getCurrentDate():
    gmt_plus_10 = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(gmt_plus_10)
    # print("cbhvws xmvwcvguecugecer======", current_time)
    timeStamp = "{:%Y-%m-%d %H:%M}".format(current_time)
    return str(timeStamp)


def date_to_unique_timestamp(date, T=False, hours=0,days=0):
    if T and days:
        timestamp_format = "%Y-%m-%dT%H:%M"
        timestamp = datetime.strptime(date, timestamp_format)
        # add time
        new_timestamp = timestamp + timedelta(days=days)
        return int(new_timestamp.timestamp() * 1000)
    if T and hours:
        timestamp_format = "%Y-%m-%dT%H:%M"
        timestamp = datetime.strptime(date, timestamp_format)
        new_timestamp = timestamp + timedelta(hours=float(hours))
        return int(new_timestamp.timestamp() * 1000)
    if T:
        dating = datetime.strptime(date, "%Y-%m-%dT%H:%M")
        new_timestamp = dating + timedelta(hours=5,minutes=30)
        return int(new_timestamp.timestamp() * 1000)
        
        # return str(datetime.strptime(date, "%Y-%m-%dT%H:%M").timestamp() * 1000).split(
        #     "."
        # )[0]

    gmt_plus_10 = pytz.timezone("Asia/Kolkata")
    dating = datetime.strptime(date, "%Y-%m-%d %H:%M")
    current_time = gmt_plus_10.localize(dating) 
    timeStamp = int(current_time.timestamp() * 1000)
    return str(timeStamp)

def get_current_date_timestamp():
    gmt_plus_10 = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(gmt_plus_10)
    # future_time = current_time + timedelta(hours=5,minutes=30)
    formatted_date = current_time.strftime("%Y-%m-%d")
    
    return int(date_to_unique_timestamp(f"{formatted_date}T00:00", T=True))

def get_current_date_timestamp2():
    gmt_plus_10 = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(gmt_plus_10)
    future_time = current_time + timedelta(hours=5,minutes=30)
    formatted_date = current_time.strftime("%Y-%m-%d")
    
    return int(date_to_unique_timestamp(f"{formatted_date}T00:00", T=True))

def convert_timestamp_to_string(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    offset = timedelta(hours=5, minutes=30)
    dt = dt.astimezone(timezone(offset))
    formatted_date = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return formatted_date

def convert_timestamp_to_string2(timestamp):
    try:
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        offset = timedelta(hours=5, minutes=30)
        dt = dt.astimezone(timezone(offset))
        formatted_date = dt.strftime("%d-%m-%Y")
        return formatted_date
    except Exception as e:
        pass
        # print (e,'ddhdh')

# dateFormtting(dateToFormat)

def is_valid_mongodb_objectid(oid):
    try:
        
        ObjectId(oid)
        return True
    except Exception as e:
        return False
def is_valid_mongodb_objectid2(oid):
    try:
        ObjectId(oid)
        return ObjectId(oid)
    except Exception as e:
        
        return False




def generate_pdf_from_dict(data_list, output_path, title=None):
    doc = SimpleDocTemplate(output_path, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'title',
        parent=styles['Title'],
        fontSize=18,
        alignment=1,
        textColor=colors.black
    )
    
    key_style = ParagraphStyle(
        'key',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.black,
        spaceAfter=3,  # Reduced space after key
        alignment=0  # Left align
    )
    
    value_style = ParagraphStyle(
        'value',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.green,
        spaceAfter=3,  # Reduced space after value
        alignment=2  # Right align
    )
    
    for idx, data in enumerate(data_list):
        title = "Expense/Advance Report"
        elements.append(Paragraph(title, title_style))
        elements.append(Spacer(1, 20))
        
        table_data = []
        for key, value in data.items():
            if key not in ['Attachment', 'Signature', 'L1-Approver', 'L2-Approver', 'L3-Approver', 'L1-Approver Date', 'L2-Approver Date', 'L3-Approver Date', 'L1ApproverStatus', 'L2ApproverStatus', 'L3ApproverStatus']:
                table_data.append([Paragraph(f"<b>{key}</b>", styles['Normal']), Paragraph(str(value), styles['Normal'])])
        
        col_widths = [150, 350]
        row_height = 25
        
        from reportlab.platypus import Table, TableStyle

        table = Table(table_data, colWidths=col_widths, rowHeights=row_height)
        table_style = [
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]
        table.setStyle(TableStyle(table_style))
        elements.append(table)
        
        elements.append(Spacer(1, 20))
        
        attachment_path = data.get('Attachment', '')
        if attachment_path:
            try:
                img_path = os.path.join(os.getcwd(), attachment_path)
                from reportlab.platypus import Image
                if img_path.lower().endswith('.pdf'):
                    from PyPDF2 import PdfFileReader

                    pdf_reader = PdfFileReader(open(img_path, 'rb'))
                    if pdf_reader.numPages > 0:
                        from reportlab.pdfgen import canvas

                        first_page = pdf_reader.getPage(0)
                        pdf_writer = open(os.path.join(os.getcwd(), 'temp_img.png'), 'wb')
                        pdf_writer.write(first_page)
                        pdf_writer.close()

                        image_path = os.path.join(os.getcwd(), 'temp_img.png')
                        img = Image(image_path, width=200, height=200)
                        elements.append(img)
                else:
                    img = Image(img_path, width=200, height=200)
                    elements.append(img)
            except Exception as e:
                elements.append(Paragraph(f"Error loading image: {str(e)}", styles['Normal']))
        
        if idx < len(data_list) - 1:
            elements.append(PageBreak())
    
    # Create the signature page with the desired layout
    elements.append(PageBreak())
    elements.append(Paragraph("Signatures", title_style))
    elements.append(Spacer(1, 20))
    
    # Adding User Signature
    elements.append(Paragraph("User Signature: ", key_style))
    elements.append(Paragraph(data_list[0].get('Signature', ''), value_style))
    elements.append(Spacer(1, 3))  # Reduced space between User Signature and next item
    
    # Adding approver details
    approver_keys = [
        ('L1-Approver', 'L1ApproverStatus', 'L1-Approver Date'),
        ('L2-Approver', 'L2ApproverStatus', 'L2-Approver Date'),
        ('L3-Approver', 'L3ApproverStatus', 'L3-Approver Date')
    ]
    
    for approver_key, status_key, date_key in approver_keys:
        approver_value = data_list[0].get(approver_key, None)
        status_value = data_list[0].get(status_key, None)
        date_value = data_list[0].get(date_key, None)
        if approver_value and status_value and date_value:
            elements.append(Paragraph(f"{approver_key.replace('-', ' ')}: ", key_style))
            elements.append(Paragraph(f"{status_value}", value_style))
            elements.append(Paragraph(f"{approver_value}", value_style))
            elements.append(Paragraph(f"{date_value}", value_style))
            elements.append(Spacer(3, 3))  # Reduced space between approver details
    
    doc.build(elements)



def read_file_from_path(file_path):
    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()
            return file_bytes
    except IOError:
        return None