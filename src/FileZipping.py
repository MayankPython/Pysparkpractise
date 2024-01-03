
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

import zipfile
import gzip
import io
from io import BytesIO
import pandas as pd

# comp_file_path = "C:/Users/Mayank_Pandey/Downloads/archive/board_games.zip"
# comp_file = zipfile.ZipFile(comp_file_path, "w")
# comp_file.write("C:/Users/Mayank_Pandey/Downloads/archive/board_games.csv",
#                 "board_games.xlsx", compress_type=zipfile.ZIP_DEFLATED)
# comp_file.close()

df = pd.read_excel("C:/Users/Mayank_Pandey/Downloads/archive/board_games.xlsx")
print(len(df))
buffer = io.BytesIO()
df.to_excel(buffer)
buffer.seek(0)
with gzip.open("C:/Users/Mayank_Pandey/Downloads/archive/output.xlsx.gz", "wb") as f:
    f.write(buffer.read())
    