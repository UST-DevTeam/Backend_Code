from base import *
import os
import datetime

def singleFileSaver(file,path,accept):
    


    if file==None:
        return {
                "status":422,
                "msg":'No selected file',
                "icon":'error',
                'data':[]
            }
    if file.filename == '':
        return {
            "status":422,
            "msg":'No selected file',
            "icon":'error',
            'data':[]
        }

    
    fileExt=file.filename.split(".")[-1]

    if(fileExt in accept or len(accept)==0):
        filePath=""
        if(path==""):
            filePath=os.path.join("uploads",file.filename)
            file.save(os.path.join(os.getcwd(),filePath))
        else:
            filePath=os.path.join(path,file.filename)
            file.save(os.path.join(os.getcwd(),filePath))

        return {
            "status":200,
            "msg":filePath,
            "fnamemsg":file.filename,
            "data":[]
        }
    else:
        return {
            "status":422,
            "msg":'File type not allowed. Valid file types are...\n{}'.format(" , ".join(accept)),
            "icon":'error',
            "data":[]
        }
    
def singleFileSaverPtw(file, path, accept, subform, fileType):
    if file is None or file.filename == '':
        return {
            "status": 422,
            "msg": 'No selected file',
            "icon": 'error',
            'data': []
        }

    fileExt = file.filename.split(".")[-1].lower()

    if fileExt in accept or len(accept) == 0:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        newFileName = f"{fileType}_{subform}_{timestamp}.{fileExt}"

        save_dir = path if path else "uploads/ptw"
        os.makedirs(save_dir, exist_ok=True) 

        filePath = os.path.join(save_dir, newFileName)

        file.save(os.path.join(os.getcwd(), filePath))

        return {
            "status": 200,
            "msg": filePath,
            "fnamemsg": newFileName,
            "data": []
        }

    else:
        return {
            "status": 422,
            "msg": 'File type not allowed. Valid file types are: {}'.format(", ".join(accept)),
            "icon": 'error',
            "data": []
        }