import io
import os
import re
import pandas as pd
from PIL import ImageFile
from flask import send_file
from openpyxl.drawing.image import Image as ExcelImage 

from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph,
    PageBreak, Spacer, Image as PlatypusImage
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
ImageFile.LOAD_TRUNCATED_IMAGES = True



UPLOAD_DIR = "uploads"  
def export_to_excel(document, keys, ptwNumberArg):
    excel_buffer = io.BytesIO()
    update_keys = ['Row Data','Checklist', 'Photo', 'Risk Assessment', 'Team Details', 'Ptw Photo','Road Safety Checklist 2 Wheeler','Ptw Photo 4 Wheeler','Road Safety Checklist 4 Wheeler','Ptw Photo 2 Wheeler','Rejection Reason']
    
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        # for key in keys:
        for i, key in enumerate(keys):
            value = document.get(key)

            if not value:
                continue

            if isinstance(value, dict):
                # df = pd.DataFrame(value.items(), columns=['Field', 'Value'])
                df = pd.DataFrame(value.items(), columns=['FIELD', 'VALUE'])
                
            elif isinstance(value, list) and value:
                df = pd.DataFrame(value)
            else:
                continue

            # sheet_name = key[:31]
            # df.columns = [col.upper() for col in df.columns]
            sheet_name = update_keys[i][:31] if i < len(update_keys) else key[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            ws.column_dimensions['B'].width = 60

            for idx, row in enumerate(df.itertuples(index=False), start=2):
                val = str(row.VALUE).strip()
                if not val or val.lower() == "undefined":
                    continue

                if val.lower().endswith(('.png', '.jpg', '.jpeg')):
                    local_image_path = os.path.join(UPLOAD_DIR, os.path.relpath(val, "uploads"))
                    if os.path.isfile(local_image_path):
                        try:
                            img = ExcelImage(local_image_path)
                            img.width = 120
                            img.height = 120
                            img.anchor = f'B{idx}'
                            ws.add_image(img)
                            ws[f'B{idx}'].value = None
                            ws.row_dimensions[idx].height = 100
                        except Exception as e:
                            print(f"[Image Embed Error] {local_image_path} — {e}")
                            continue

    excel_buffer.seek(0)
    filename = f"PTW_Export_{ptwNumberArg.replace('/', '-')}.xlsx"
    return send_file(
        io.BytesIO(excel_buffer.read()),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )

def export_to_pdf(document, keys, ptwNumberArg):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=30,
        rightMargin=30,
        topMargin=30,
        bottomMargin=30
    )

    elements = []
    styles = getSampleStyleSheet()
    normal_style = styles["Normal"]
    header_style = styles["Heading2"]
    
    def build_table(data_dict):
        # rows = [["Field", "Value"]]  
        rows = [["FIELD", "VALUE"]]

        for field, val in data_dict.items():
            if val is None or str(val).strip().lower() == "undefined":
                continue

            val_str = str(val).strip()
            image_path = None

           
            image_match = re.search(r"(uploads/[^\s]+?\.(?:jpg|jpeg|png))", val_str, re.IGNORECASE)
            if image_match:
                rel_path = os.path.relpath(image_match.group(1), "uploads")
                local_path = os.path.join(UPLOAD_DIR, rel_path)
                if os.path.isfile(local_path):
                    image_path = local_path

    
            wrapped_field = Paragraph(field, normal_style)

            if image_path:
                try:
                    img = PlatypusImage(image_path, width=100, height=100)
                    rows.append([wrapped_field, img])
                except Exception as e:
                    print(f"[Image Embed Error] {field}: {e}")
                    rows.append([wrapped_field, Paragraph("Image could not be displayed", normal_style)])
            else:
                wrapped_value = Paragraph(val_str, normal_style)
                rows.append([wrapped_field, wrapped_value])

        table = Table(rows, colWidths=[160, 320]) 
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        return table

    update_keys = ['Row Data','Checklist', 'Photo', 'Risk Assessment', 'Team Details', 'Ptw Photo','Road Safety Checklist 2 Wheeler','Ptw Photo 4 Wheeler','Road Safety Checklist 4 Wheeler','Ptw Photo 2 Wheeler','Rejection Reason']
    
    # for section in keys:
    for i, section in enumerate(keys):
        section_data = document.get(section)
        if not section_data:
            continue
        
        section_title = update_keys[i] if i < len(update_keys) else section
        
        
        # elements.append(Paragraph(f"{section.upper()} SECTION", header_style))
        elements.append(Paragraph(f"{section_title} Section", header_style))
        elements.append(Spacer(1, 6))

        if isinstance(section_data, dict):
            elements.append(build_table(section_data))
        elif isinstance(section_data, list):
            for item in section_data:
                if isinstance(item, dict):
                    elements.append(build_table(item))
                    elements.append(Spacer(1, 10))
                else:
                    # elements.append(build_table({f"{section}": item}))
                    elements.append(build_table({f"{section_title}": item}))

        elements.append(PageBreak())


    doc.build(elements)
    buffer.seek(0)

    return send_file(
        buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"PTW_Export_{ptwNumberArg.replace('/', '-')}.pdf"
    )