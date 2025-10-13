import os, json, base64, gspread
from google.oauth2.service_account import Credentials
from .log import log

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

_gs = None

def get_gspread_client():
    global _gs
    if _gs is not None:
        return _gs
    b64 = os.getenv("GCP_SA_JSON_BASE64", "")
    inline = os.getenv("GOOGLE_SVCKEY_JSON", "")
    if b64:
        info = json.loads(base64.b64decode(b64))
    elif inline:
        info = json.loads(inline)
    else:
        raise RuntimeError("Missing credentials: set GCP_SA_JSON_BASE64 or GOOGLE_SVCKEY_JSON")
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _gs = gspread.authorize(creds)
    try:
        client_email = info.get("client_email")
        if client_email:
            print(f"[GSHEET] service account email = {client_email}")
    except Exception:
        pass
    return _gs
