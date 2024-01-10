
"""Below is the code for zipping attachment before sending email."""
# import zipfile
# if attachment:
#     MAX_ATTACHMENT_SIZE = 15 * 1024 * 1024
#     with open(attachment, 'rb') as fp:
#         bytes = fp.read()
#     attachment_name = basename(attachment)
#     if len(bytes) > MAX_ATTACHMENT_SIZE:
#         from io import BytesIO
#
#         buffer = BytesIO()
#         with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
#             zip_file.writestr(attachment_name, bytes)
#         # New attachment name and new data:
#         attachment_name += '.zip'
#         bytes = buffer.getvalue()
#     att = MIMEApplication(bytes, Name=attachment_name)
#     att['Content-Disposition'] = f'attachment; filename="{attachment_name}"'
#     msg.attach(att)