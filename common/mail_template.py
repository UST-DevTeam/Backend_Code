from base import *

def Verification_mail(name,id):
    return f"""Dear {name},
<br>
Thank you for signing up with FOURBRICK INVESTMENT COMPANY. To complete the registration process and ensure the security of your account, we kindly ask you to verify your email address.
<br>
Please click on the following link to verify your email:
<br>
<a href="{os.environ.get("FRONTEND_URL")+"/setupPassword/"+str(id)}">Click Here</a>
<br>
If the link above is not clickable, you can copy and paste it into your browser's address bar.
<br>
If you did not create an account with FOURBRICK INVESTMENT COMPANY, please disregard this email. Your information remains secure, and no further action is required.
<br>
Thank you for choosing FOURBRICK INVESTMENT COMPANY. If you encounter any issues or have questions, please contact our support team at [Support Email Address].
<br>
Best regards,
<br>
FOURBRICK INVESTMENT COMPANY"""



def TaskAllocation_mail(name,TaskName,SiteID,ProjectID):
    return f"""Dear {name},

<br>
<br>
The following task has been assigned to User-
<br>
Task - {TaskName}
<br>
Site Id - {SiteID}
<br>
Project ID - {ProjectID}
<br>
<br>
<br>
<br>
<br>
Regards,
<br>
PMO Team

<br>
<br>
<br>
<br>
<br>

*This is a computer-generated acknowledgement email by PMO. Please do not reply on this email."""


def ClaimType_mail(userName,userCode,Id,totalAmount,type,approver,approveAmount):
    return f"""Dear {userName},

<br>
<br>
The {type} of {totalAmount} amount is {approver}  with following details :
<br>
Name - {userName}
<br>
Code - {userCode}
<br>
{type} ID - {Id}
<br>
Approved Amount - {approveAmount}
<br>
<br>
Please login to the portal for further details.
<br>
<br>
Regards,
<br>
PMO Team

<br>
<br>
<br>
<br>
<br>

*This is a computer-generated acknowledgement email by PMO. Please do not reply on this email."""


def forgetPassword_mail(empName,empCode,password):
    return f"""Dear {empName},

<br>
<br>
Emp Code:- {empCode}
<br>
<br>
Your login password is - "{password}"
<br>
<br>
---If you did not request this change or if you encounter any issues, please contact PMO team.
<br>
<br>
<br>
Regards,
<br>
PMO-Telco Engineering Services India
<br>
<br>
<br>

*This is a computer-generated acknowledgement email by PMO. Please do not reply on this email.""" 



def ptwMailTempplate(ptwData):
    return f"""Dear {ptwData['userName']},

<br>
<br>
Your PTW Number Is  - "{ptwData['ptwNumber']}"
<br>
<br>
PTW Status Is  {ptwData['status']}
<br>
<br>
<br>
<br>
---If you did not request this change or if you encounter any issues, please contact PMO team.
<br>
<br>
<br>
Regards,
<br>
PMO-Telco Engineering Services India
<br>
<br>
<br>

*This is a computer-generated acknowledgement email by PMO. Please do not reply on this email.""" 


def ptwApproverMailTemplate(ptwData,level):
    return f"""
    <p>Dear <strong>{ptwData['userName']}</strong>,</p>

    <p>A new Permit to Work (PTW) has been assigned to you for <strong>{level} Approval</strong>.</p>

    <p><strong>PTW Number:</strong> {ptwData['ptwNumber']}</p>

    <p>Please review and take necessary action on the PTW at your earliest convenience.</p>

    <hr>

    <p style="font-size: 12px; color: #555;">
        If you did not expect this email or encounter any issues, please contact the PMO team.
    </p>

    <br>

    <p>Regards,<br>
    PMO – Telco Engineering Services India</p>

    <p style="font-size: 11px; color: gray;">
        *This is an automated email from the PMO system. Please do not reply directly to this email.*
    </p>
    """