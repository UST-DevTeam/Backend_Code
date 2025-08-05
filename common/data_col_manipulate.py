validate_Upload_ManageCircle = {
    "Customer",
    "Circle Name",
    "Circle Code",
    "Band"
}

rename_Upload_ManageCircle = {
    "Customer": "customer",
    "Circle Name": "circleName",
    "Circle Code": "circleCode",
    "Band":"band"
}


validate_Upload_ManageZone = {"Customer", "Zone Name", "Zone Code", "Circle"}


rename_Upload_ManageZone = {
    "Customer": "customer",
    "Zone Name": "zoneName",
    "Zone Code": "shortCode",
    "Circle": "circle",
}

validate_Upload_ManageCostCenter = {
    "Customer",
    "Zone Code",
    "Cost Center",
    "Description",
    "Business Unit"
}

rename_Upload_ManageCostCenter = {
    "Customer": "customer",
    "Zone Code": "zone",
    "Cost Center": "costCenter",
    "Description":'description',
    "Business Unit":"businessUnit"
}
validateUpgradeVendor = {
    "Partner Code",
    "e-mail Address",
}
renameUpgradeVendor = {
    "Partner Code": "vendorCode",
    "Partner Name": "vendorName",
    "UST Partner Code": "ustCode",
    "e-mail Address": "email",
    "Partner Category": "vendorCategory",
    "Partner Subcategory": "vendorSubCategory",
    "Role": "userRole",
    "Date of Registration": "dateOfRegistration",
    "Validity Upto": "validityUpto",
    "Partner Ranking": "ranking",
    "Payment Terms (Days)": "paymentTerms",
    "Working Circle": "Circle",
    "Partner Registered with GST (Y/N)": "vendorRegistered",
    "GST Number": "gstNumber",
    "Current Status": "status",
    "Password": "password",
    "PAN Number": "panNumber",
    "TAN Number": "tanNumber",
    "ESI Number": "esiNumber",
    "EPF Number": "epfNumber",
    "STN Number": "stnNumber",
    "Bank Account Number": "accountNumber",
    "Bank Name": "bankName",
    "IFSC Code": "ifscCode",
    "Branch Address": "bankAddress",
    "Financial Turnover": "financialTurnover",
    "CBTHR Certified (Y/N)": "cbt",
    "Form Toclii": "formToci",
    "Contact Person": "contactPerson",
    "Registered Address": "registeredAddress",
    "Contact Details": "contactDetails",
    "Secondary Contact details": "SecContactDetails",
    "Company Type": "companyType",
    "Team Capacity": "teamCapacity",
    "Other Information": "otherInfo",
}
validate_upload_vendor = {
    "Partner Name",
    "Partner Code",
    "e-mail Address",
    "Role",
}
rename_upload_vendor = {
    "Partner Code": "vendorCode",
    "Partner Name": "vendorName",
    "UST Partner Code": "ustCode",
    "e-mail Address": "email",
    "Partner Category": "vendorCategory",
    "Partner Subcategory": "vendorSubCategory",
    "Role": "userRole",
    "Date of Registration": "dateOfRegistration",
    "Validity Upto": "validityUpto",
    "Partner Ranking": "ranking",
    "Payment Terms (Days)": "paymentTerms",
    "Working Circle": "Circle",
    "Partner Registered with GST (Y/N)": "vendorRegistered",
    "GST Number": "gstNumber",
    "Current Status": "status",
    "Password": "password",
    "PAN Number": "panNumber",
    "TAN Number": "tanNumber",
    "ESI Number": "esiNumber",
    "EPF Number": "epfNumber",
    "STN Number": "stnNumber",
    "Bank Account Number": "accountNumber",
    "Bank Name": "bankName",
    "IFSC Code": "ifscCode",
    "Branch Address": "bankAddress",
    "Financial Turnover": "financialTurnover",
    "CBTHR Certified (Y/N)": "cbt",
    "Form Toclii": "formToci",
    "Contact Person": "contactPerson",
    "Registered Address": "registeredAddress",
    "Contact Details": "contactDetails",
    "Secondary Contact details": "SecContactDetails",
    "Company Type": "companyType",
    "Team Capacity": "teamCapacity",
    "Other Information": "otherInfo",
}
validate_Upload_AccuralRevenueMaster={
   "Customer", "Project Type","Project ID","Sub Project","Band","ACTIVITY","Rate","Item Code-01","Item Code-02","Item Code-03","Item Code-04","Item Code-05","Item Code-06","Item Code-07"
}
rename_upload_AccuralRevenueMaster={
    "Customer":"customer",
    "Project Type":"projectType",
                            "Project ID":"project",
                            "Sub Project":"subProject",
                            "Band":"band",
                            "ACTIVITY":"activity",
                            "Rate":"rate",
                            "Item Code-01":"itemCode01",
                            "Item Code-02":"itemCode02",
                            "Item Code-03":"itemCode03",
                            "Item Code-04":"itemCode04",
                            "Item Code-05":"itemCode05",
                            "Item Code-06":"itemCode06",
                            "Item Code-07":"itemCode07"}
validate_Upload_ManageEmployee = {
    "Employee Code",
    "Employee Name",
    "Official Email-ID",
    "PMIS Role",
}

rename_Upload_ManageEmployee = {
    "Title": "title","Employee Name": "empName","Employee Code": "empCode","UST Project ID":"ustProjectId","UST Job Code":"ustJobCode",
    "UST Emp Code": "ustCode","Father's Name": "fatherName","Mother's Name": "motherName","Marital Status": "martialStatus","Official Email-ID": "email","Personal Email-ID": "personalEmailId",
    "Date Of Birth(as Per doc)": "dob","Contact Number": "mobile","Blood Group": "blood","Country": "country","State": "state","City": "city","PinCode": "pincode",
    "Address": "address","Permanent Country": "pcountry","Permanent PinCode": "ppincode","Permanent City": "pcity","Permanent State": "pstate","Permanent Address": "paddress","PAN Number": "panNumber","Aadhar Number": "adharNumber","Circle": "circle",
    "Salary Currency": "salaryCurrency","Experience": "experience","Salary": "monthlySalary","Gross CTC": "grossCtc",
    "Joining Date": "joiningDate","Last Working Day": "lastWorkingDay","Resign Date":"resignDate","Passport (Y/N)": "passport","Passport Number": "passportNumber","Bank Name": "bankName","Bank Account Number": "accountNumber","IFSC Code": "ifscCode","Beneficiary Name": "benificiaryname",
    "Organization Level": "orgLevel",
    "Allocation Percentage":"allocationPercentage",
    "Business Unit":"businesssUnit",
    "Customer Name":"customer",
    "Grade": "designation","Role": "role","PMIS Role": "userRole","Cost Center":"costCenter","Designation": "band","Department": "department","Reporting Manager": "reportingManager","L1Approver": "L1Approver","L2Approver": "L2Approver","Finance Approver": "financeApprover",
    "Reporting HR Manager": "reportingHrManager","Asset Manager": "assetManager","L1Vendor": "L1Vendor","L2Vendor": "L2Vendor","L1Compliance": "L1Compliance","L2Compliance": "L2Compliance","L1Commercial": "L1Commercial","L1Sales": "L1Sales","L2Sales": "L2Sales","Status": "status","Password": "password",
}
renameUpgradeBulkEmployee = {
    "Title": "title","Employee Name": "empName","Employee Code": "empCode","UST Emp Code": "ustCode","UST Project ID":"ustProjectId","UST Job Code":"ustJobCode","Father's Name": "fatherName","Mother's Name": "motherName","Marital Status": "martialStatus","Official Email-ID": "email","Personal Email-ID": "personalEmailId",
    "Date Of Birth(as Per doc)": "dob","Contact Number": "mobile","Blood Group": "blood","Country": "country","State": "state","City": "city","PinCode": "pincode",
    "Address": "address","Permanent Country": "pcountry","Permanent PinCode": "ppincode","Permanent City": "pcity","Permanent State": "pstate","Permanent Address": "paddress","PAN Number": "panNumber","Aadhar Number": "adharNumber","Circle": "circle",
    "Salary Currency": "salaryCurrency","Experience": "experience","Salary": "monthlySalary","Gross CTC": "grossCtc",
    "Joining Date": "joiningDate","Last Working Day": "lastWorkingDay","Resign Date":"resignDate","Passport (Y/N)": "passport","Passport Number": "passportNumber","Bank Name": "bankName","Bank Account Number": "accountNumber","IFSC Code": "ifscCode","Beneficiary Name": "benificiaryname",
    "Organization Level": "orgLevel",
    "Allocation Percentage":"allocationPercentage",
    "Business Unit":"businesssUnit",
    "Customer Name":"customer",
    "Organization Level": "orgLevel","Grade": "designation","Role": "role","PMIS Role": "userRole","Cost Center":"costCenter","Designation": "band","Department": "department","Reporting Manager": "reportingManager","L1Approver": "L1Approver","L2Approver": "L2Approver","Finance Approver": "financeApprover",
    "Reporting HR Manager": "reportingHrManager","Asset Manager": "assetManager","L1Vendor": "L1Vendor","L2Vendor": "L2Vendor","L1Compliance": "L1Compliance","L2Compliance": "L2Compliance","L1Commercial": "L1Commercial","L1Sales": "L1Sales","L2Sales": "L2Sales","Status": "status","Password": "password",
}

validateUpgradeBulkEmployee = {
    "Employee Code",
    "Official Email-ID",
}
validateBulkUploadExpense={
    "Employee Code",
    "Official Email-ID",
    "Claim Type",
    "Amount",
    "L1-Approver Email",
    "L2-Approver Email",
    "L3-Approver Email",
    "L1 Status",
    "Last Action Date",
    "L2 Status",
    "L3 Status"
    }


validateBulkUploadSettlementAmount={"Employee Code","Official Email-ID","Amount"}
renameBulkUploadSettlementAmount={
    "Employee Code":"empCode","Official Email-ID":"email",
    "Settlement Requisition Date":"SettlementRequisitionDate",
    "Approval Date":"approvalDate","Amount":"Amount",
    "Remarks":"remarks"
}
renameBulkUploadExpense = {
    "Employee Code": "empCode",
    "Official Email-ID":"email",
    "Project ID": "projectId",
    "System ID":"systemId",
    "Site ID":"Site Id",
    "Task Name":"taskName",
    "Claim Date":"expenseDate",
    "Claim Type":"claimType",
    "Category":"categories",
    "Check-IN Date":"checkInDate", ###format "18-03-2024"
    "Check-OUT Date":"checkOutDate", ###format "18-03-2024"
    "Total Days":"totaldays", ###format "2"
    "Amount": "Amount",
    "Submission Date":"submssionDate", ###format timestamp
    "Start KM":"startKm", ###format "2"
    "End KM":"endKm",###format "2"
     "Total KM":"Total_distance",###format "2"
     "Start Location":"startLocation",
    "End Location":"endLocation",
    "Approved Amount":"ApprovedAmount",
    "Bill No":"billNumber",
    "L1 Status":"L1Status",
    "L2 Status":"L2Status",
    "L3 Status":"L3Status",
    "Last Action Date":"actionAt",
    "L1-Approver Email":"L1Approver",
    "L2-Approver Email":"L2Approver",
    "L3-Approver Email":"L3Approver",
    "Remarks":"remark",
    "Approved Date":"actionAt",
}

validateBulkUploadCurrentBalance={
    "Employee Code",
    "Official Email-ID",
    "Balance"
}
renameBulkUploadCurrentBalance={
    "Employee Code":"empCode",
    "Official Email-ID":"email",
    "Balance":"Amount",
}
# validate_Upload_Template = {
#     "Status",
#     "dataType",
#     "fieldName",
#     "index",
#     "required",
# }

# rename_Upload_Template={
#     "circle":"circle1",
#     "short":"short1",
# }

validate_Upload_Template = {
    "FieldName",
    "Mandatory(Y/N)",
    "InputType",
    "Dropdown Values",
    "Status",
}

rename_Upload_Template = {
    "FieldName": "fieldName",
    "Mandatory(Y/N)": "required",
    "InputType": "dataType",
    "Dropdown Values": "dropdownValue",
    "Status": "Status",
}
validate_Upload_Milestone = {
    "Milestone",
    "WCC Sign off",
    "Estimated Time (Days)",
    "Completion Criteria",
    "Predecessor",
    "Status",
}

rename_Upload_Milestone = {
    "Milestone": "fieldName",
    "WCC Sign off": "WCC Sign off",
    "Estimated Time (Days)": "Estimated Time (Days)",
    "Completion Criteria": "Completion Criteria",
    "Predecessor": "Predecessor",
    "Status": "Status",
}
validate_Upload_Commercial = {
    "GBPA",
    "ItemCode",
    "UnitRate",
    "Description",
}

rename_Upload_Commercial = {
    "GBPA": "GBPA",
    "ItemCode": "ItemCode",
    "UnitRate": "UnitRate",
    "Description": "Description",
}

validate_Upload_invoice = {
    "Customer",
    "Project Group",
    "Project ID",
    "SSID",
    "WCC No",
    "WCC SignOff Date",
    "PO Number",
    "Item Code",
    "Invoiced Quantity",
    "Invoice Number",
    "Invoice Date",
    "Unit Rate",
    "Status",
}
rename_Upload_invoice = {
    "Customer": "customer",
    "Project Group": "projectGroup",
    "Project ID": "projectId",
    "SSID": "systemId",
    "WCC No": "wccNumber",
    "WCC SignOff Date": "wccSignOffdate",
    "PO Number": "poNumber",
    "Item Code": "itemCode",
    "Invoiced Quantity": "qty",
    "Invoice Number": "invoiceNumber",
    "Invoice Date": "invoiceDate",
    "Unit Rate": "unitRate",
    "Status": "status",
}


validate_Upload_PoInvoice = {
    "Customer",
    "Project Group",
    "GBPA",
    "PO Number",
    "PO Start Date",
    "PO End Date",
    "Item Code",
    "Description",
    "Unit Rate(INR)",
    "Initial PO Qty"
}

rename_Upload_PoInvoice = {
    "Customer": "customer",
    "Project Group": "projectGroup",
    "GBPA": "gbpa",
    "PO Number": "poNumber",
    "PO Start Date": "poStartDate",
    "PO End Date": "poEndDate",
    "Item Code": "itemCode",
    "Description": "description",
    "Unit Rate(INR)": "unitRate(INR)",
    "Initial PO Qty": "initialPoQty"
}
validate_Upgrade_PoInvoice = {
    "Project Group",
    "PO Number",
    "Item Code",
    "PO Status"
}
rename_Upgrade_PoInvoice = {
    "Project Group": "projectGroup",
    "PO Number": "poNumber",
    "Item Code": "itemCode",
    "PO Status":"poStatus"
}

validate_Upload_ItemCode = {
    "SSID",
    "Work Done Bucket",
    "Invoice Bucket",
    "Item Code1",
    "Quantity1",
    "Item Code2",
    "Quantity2",
    "Item Code3",
    "Quantity3",
    "Item Code4",
    "Quantity4",
    "Item Code5",
    "Quantity5",
    "Item Code6",
    "Quantity6",
    "Item Code7",
    "Quantity7",
}

rename_Upload_ItemCode = {
    "SSID": "systemId",
    "Work Done Bucket": "workdonebucket",
    "Invoice Bucket": "invoicebucket",
    "Item Code1": "itemCode1",
    "Quantity1": "quantity1",
    "Item Code2": "itemCode2",
    "Quantity2": "quantity2",
    "Item Code3": "itemCode3",
    "Quantity3": "quantity3",
    "Item Code4": "itemCode4",
    "Quantity4": "quantity4",
    "Item Code5": "itemCode5",
    "Quantity5": "quantity5",
    "Item Code6": "itemCode6",
    "Quantity6": "quantity6",
    "Item Code7": "itemCode7",
    "Quantity7": "quantity7",
}

validate_Upload_SiteIdBulk = {
    "Customer",
    "Project Group",
    "Project ID",
    "Project Type",
    "Sub Project",
    "Circle",
    "Site ID",
    "Unique ID",
    "RFAI Date",
    "Allocation Date",
}

rename_Upload_SiteIdBulk = {
    "Customer": "customerName",
    "Project Type": "projectType",
    "Site ID": "Site Id",
    "Project ID": "projectuniqueId",
    "Sub Project": "SubProjectId",
    "Allocation Date": "ALLOCATION DATE",
    "RFAI Date": "RFAI Date",
    "Project Group": "projectGroup",
    "Unique ID": "uniqueid",
    "Circle": "circle",
}

validate_site_upload = {
    "System ID",
}

rename_site_upload = {"System ID": "systemId", "Site Status": "siteStatus","Site ID":"Site Id"}

validate_task_upload = {
    "System ID",
    "Task Name",
    "Task Owner",
}


rename_task_upload = {
    "System ID": "systemId",
    "Task Name": "Name",
    "Task Owner": "assignerId",
    "MS Completion Date": "CC_Completion Date",
    "Custom Status": "mileStoneStatus",
}

validate_old_task = {
    "System ID",
    "Task Name",
}
rename_old_task = {
    "System ID": "systemId",
    "Task Name": "Name",
    "MS Completion Criteria": "Completion Criteria",
    "MS Predecessor": "Predecessor",
}

validateuserProjectAllocation = {
    "Emp Email Id",
    "Project"
}

renameuserProjectAllocation = {
    "Emp Email Id":"email",
    "Project":"projectId"
}

validatepartnerProjectAllocation = {
    "Partner Email Id",
    "Project"
}

renamepartnerProjectAllocation = {
    "Partner Email Id":"email",
    "Project":"projectId"
}

validateEVMFinancial = {
    "Customer",
    'Cost Center',
    'Year',
    "PV Target(Jan)", "PV Target(Feb)", "PV Target(Mar)", "PV Target(Apr)", "PV Target(May)", "PV Target(Jun)","PV Target(Jul)", "PV Target(Aug)","PV Target(Sep)", "PV Target(Oct)", "PV Target(Nov)", "PV Target(Dec)"

}

renameEVMFinancial = {
    "Customer":'customer',
    'Cost Center':'costCenter',
    'Year':'year',
    "PV Target(Jan)":'pv-1',
    "PV Target(Feb)": 'pv-2',
    "PV Target(Mar)": 'pv-3',
    "PV Target(Apr)": 'pv-4',
    "PV Target(May)": 'pv-5',
    "PV Target(Jun)": 'pv-6',
    "PV Target(Jul)": 'pv-7',
    "PV Target(Aug)": 'pv-8',
    "PV Target(Sep)": 'pv-9',
    "PV Target(Oct)": 'pv-10',
    "PV Target(Nov)": 'pv-11',
    "PV Target(Dec)": 'pv-12'
}

# validateEVMDelivery = {
#     "Project Type","Project ID","Year","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","WK#01","WK#02","WK#03","WK#04","WK#05","WK#06","WK#07","WK#08","WK#09","WK#10","WK#11","WK#12","WK#13","WK#14","WK#15","WK#16","WK#17","WK#18","WK#19","WK#20","WK#21","WK#22","WK#23","WK#24","WK#25","WK#26","WK#27","WK#28","WK#29","WK#30","WK#31","WK#32","WK#33","WK#34","WK#35","WK#36","WK#37","WK#38","WK#39","WK#40","WK#41","WK#42","WK#43","WK#44","WK#45","WK#46","WK#47","WK#48","WK#49","WK#50","WK#51","WK#52"

# }

validateEVMDelivery = {
    "Customer","Year","Month","Circle"

}

renameEVMDelivery = {
    "Customer":"customer",
    "Year":'year',
    "Month":'month',
    "Circle":'circle',
}

# renameEVMDelivery = {
#     "Project Type":'projectType',
#     'Project ID':'projectId',
#     "Year":'year',
#     "Jan": "M-1",
#     "Feb": "M-2",
#     "Mar": "M-3",
#     "Apr": "M-4",
#     "May": "M-5",
#     "Jun": "M-6",
#     "Jul": "M-7",
#     "Aug": "M-8",
#     "Sep": "M-9",
#     "Oct": "M-10",
#     "Nov": "M-11",
#     "Dec": "M-12",
#     "WK#01": "WK#01",
#     "WK#02": "WK#02",
#     "WK#03": "WK#03",
#     "WK#04": "WK#04",
#     "WK#05": "WK#05",
#     "WK#06": "WK#06",
#     "WK#07": "WK#07",
#     "WK#08": "WK#08",
#     "WK#09": "WK#09",
#     "WK#10": "WK#10",
#     "WK#11": "WK#11",
#     "WK#12": "WK#12",
#     "WK#13": "WK#13",
#     "WK#14": "WK#14",
#     "WK#15": "WK#15",
#     "WK#16": "WK#16",
#     "WK#17": "WK#17",
#     "WK#18": "WK#18",
#     "WK#19": "WK#19",
#     "WK#20": "WK#20",
#     "WK#21": "WK#21",
#     "WK#22": "WK#22",
#     "WK#23": "WK#23",
#     "WK#24": "WK#24",
#     "WK#25": "WK#25",
#     "WK#26": "WK#26",
#     "WK#27": "WK#27",
#     "WK#28": "WK#28",
#     "WK#29": "WK#29",
#     "WK#30": "WK#30",
#     "WK#31": "WK#31",
#     "WK#32": "WK#32",
#     "WK#33": "WK#33",
#     "WK#34": "WK#34",
#     "WK#35": "WK#35",
#     "WK#36": "WK#36",
#     "WK#37": "WK#37",
#     "WK#38": "WK#38",
#     "WK#39": "WK#39",
#     "WK#40": "WK#40",
#     "WK#41": "WK#41",
#     "WK#42": "WK#42",
#     "WK#43": "WK#43",
#     "WK#44": "WK#44",
#     "WK#45": "WK#45",
#     "WK#46": "WK#46",
#     "WK#47": "WK#47",
#     "WK#48": "WK#48",
#     "WK#49": "WK#49",
#     "WK#50": "WK#50",
#     "WK#51": "WK#51",
#     "WK#52": "WK#52",   
# }

validate_profit_loss = {
    "Year",
    "Month",
    "Cost Center",
    "Projected Revenue",
    "Actual Revenue",
    "Projected Cost",
    "Actual Cost",
    "SGNA Cost",
}

rename_profit_loss = {
    "Year":"year",
    "Month":'month',
    "Cost Center":'costCenter',
    "Projected Revenue":'projectedRevenue',
    "Actual Revenue":"actualRevenue",
    "Projected Cost":'projectedCost',
    "Actual Cost":'actualCost',
    "SGNA Cost":'sgna'
}

validate_test = {
    'uid',
    "systemId"
}
rename_test = {
    'uid':'id',
    "systemId":"systemId"
}
validate_test2 = {
    '_id',
    "projectId"
}
rename_test2 = {
    'projectId':'projectId',
    "_id":"_id"
}
validate_SalaryDB={
    "Customer",
    "Cost Center",
    "Year",
    "Month",
    "Cost"
}
rename_SalaryDB={
    "Customer":"customer",
    "Cost Center":"costCenter",
    
    "Year":"year",
    "Month":"month",
    "Cost":"cost"
}
validate_OtherFixedCost={
    "Customer",
    "Cost Center",
    "Zone",
    "Year",
    "Month",
    "Cost",
    "Cost Type"
}


rename_OtherFixedCost={
    "Customer":"customer",
    "Cost Center":"costCenter",
    "Zone":"zone",
    "Year":"year",
    "Month":"month",
    "Cost Type":"costType",
    "Cost":"cost"
}

validate_AOPDB={
    "Month",
    "Year",
    "Customer",
    "UST Project ID",
    "Cost Center",
    # "Zone",
    "Planned Revenue",
    "Planned COGS",
    "Planned SGNA",
    "Actual Revenue",
    # "Actual COGS",
    "Actual Salary",
    "Actual SGNA",
    "Actual Vendor Cost",
    "Actual Revenue",
    "Other Fixed Cost",
    "Employee Expanse",
    "Miscellaneous Expenses",
    "Miscellaneous Expenses Second",
    'Business Unit'
}

validate_vendorCost = [
    "Customer",
    "Project Group",
    "Project Type",
    "Sub Project",
    "Activity Name",
    "Customer Item Code",
    
    "Vendor Item Code",
    "Item Code Description",
    "GBPA",
    "Milestone",
    "Vendor Email",
    "Vendor Code",
    "Rate",
]

rename_Upload_vendorCost = {
    "Customer":"customer",
    "Project Group" : "projectGroup",
    "Project Type": "projectType",
    "Sub Project":"subProject",
    "Activity Name":"activityName",
    "Customer Item Code":"customerItemCode",
    "Vendor Item Code": "itemCode",
    "Item Code Description": "itemCodeDescription",
    "GBPA": "GBPA",
    "Milestone":"milestone",
    "Vendor Code":"vendorId",
    "Vendor Email": "vendorEmail",
    "Rate":"rate"
}
validate_AOPDB={
    "Month",
    "Year",
    "Customer",
    "UST Project ID",
    "Cost Center",
    # "Zone",
    "Planned Revenue",
    "Planned COGS",
    "Planned SGNA",
    "Actual Revenue",
    # "Actual COGS",
    "Actual Salary",
    "Actual SGNA",
    "Actual Vendor Cost",
    "Actual Revenue",
    "Other Fixed Cost",
    "Employee Expanse",
    "Miscellaneous Expenses",
    "Miscellaneous Expenses Second",
    'Business Unit'


}

rename_AOPDB={
    "Month":"month",
    "Year":"year",
    "Customer": "customer",
    "UST Project ID":"ustProjectID",
    "Cost Center": "costCenter",
    # "Zone": "zone",
    "Planned Revenue": "planRevenue",
    "Planned COGS": "COGS",
    "Planned SGNA": "SGNA",
    "Actual Revenue": "actualRevenue",
    # "Actual COGS": "actualCogs",
    "Actual Salary":"actualSalary",
    "Actual SGNA": "actualSGNA",
    "Actual Vendor Cost": "actualVendorCost",
    "Other Fixed Cost": "otherFixedCost",
    "Employee Expanse": "employeeExpanse",
    "Business Unit":"businessUnit",
    "Miscellaneous Expenses": "miscellaneousExpenses",
    "Miscellaneous Expenses Second": "miscellaneousExpensesSecond"
}




validate_L1Approver_MDB={
    "Employee Name",
    "Email",
    "Customer",
    "Project Group",
    "Project Type",
    "Circle Name"
}
validate_L2Approver_MDB={
    "Employee Name",
    "Email",
    "Customer",
    "Project Group",
    "Project Type",
    "Circle Name"
}

rename_L1Approver_MDB={
    "Employee Name":"empName",
    "Email":"email",
    "Customer":"customerName",
    "Project Group":"projectGroupName",
    "Project Type":"projectTypeName",
    "Circle Name":"circleName",
}
rename_L2Approver_MDB={
    "Employee Name":"empName",
    "Email":"email",
    "Customer":"customerName",
    "Project Group":"projectGroupName",
    "Project Type":"projectTypeName",
    "Circle Name":"circleName",
}
