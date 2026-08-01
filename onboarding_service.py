"""
Onboarding Microservice - FastAPI
Port: 8100

Consolidates 4 former AWS Lambda functions:
  - Chakorahub-Upload-Resume       -> POST   /upload-resume
  - ChakoraHub-List-Applications   -> GET    /applications
  - ChakoraHub-Application-Status  -> GET    /application-status/{application_id}
  - ChakoraHub-Update-Status       -> PUT    /update-status
                                       DELETE /delete-application
                                       GET    /download-resume/{application_id}

DB: Oracle (oracledb) — same connection pattern as billing_service.py
Run: python onboarding_service.py
"""
import base64
import os
import re
import urllib.parse as _up
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
import oracledb
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr, Field

load_dotenv()

app = FastAPI(title="Onboarding Service", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# Oracle Configuration (matches billing_service.py exactly)
# ==========================================
ORACLE_HOST         = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT         = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER         = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD     = os.getenv("ORACLE_PASSWORD", "Welcome123")

DictCursor = object()   # sentinel — same as billing_service.py


class _OracleCursorCompat:
    """Copied verbatim from billing_service.py — rewrites %s → :1/:2 for Oracle."""
    def __init__(self, raw_cursor, dict_mode=False):
        self._cursor  = raw_cursor
        self._dict_mode = dict_mode

    @staticmethod
    def _rewrite_sql(sql, params):
        if params is None:
            return sql
        if isinstance(params, dict):
            return re.sub(r"%\((\w+)\)s", r":\1", sql)
        if "%s" in sql:
            parts   = sql.split("%s")
            rebuilt = parts[0]
            for idx, tail in enumerate(parts[1:], start=1):
                rebuilt += f":{idx}{tail}"
            return rebuilt
        return sql

    def execute(self, sql, params=None):
        rewritten = self._rewrite_sql(sql, params)
        if params is None:
            self._cursor.execute(rewritten)
        else:
            self._cursor.execute(rewritten, params)
        if self._dict_mode and self._cursor.description:
            columns = [d[0] for d in self._cursor.description]
            self._cursor.rowfactory = lambda *vals, cols=columns: dict(zip(cols, vals))
        return self

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _OracleConnectionCompat:
    """Copied verbatim from billing_service.py."""
    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self, *args, **kwargs):
        dict_mode = bool(args)
        return _OracleCursorCompat(self._conn.cursor(), dict_mode=dict_mode)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_db_connection():
    """Copied verbatim from billing_service.py."""
    try:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
        )
        raw_conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
        )

        cur = raw_conn.cursor()
        cur.execute("ALTER SESSION SET CURRENT_SCHEMA = CHAKORA")
        cur.close()

        conn = _OracleConnectionCompat(raw_conn)
        print("✅ Onboarding Service: Connected to Oracle")
        return conn
    except Exception as e:
        print(f"❌ Oracle connection error: {e}")
        return None


# ==========================================
# AWS Configuration (S3 + SES)
# ==========================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET_RAW  = os.getenv("S3_BUCKET", "chakora-resumes-2025-s3")
ADMIN_EMAIL    = os.getenv("ADMIN_EMAIL", "admin@chakorahub.com")
SES_SENDER     = os.getenv("SES_SENDER_EMAIL", ADMIN_EMAIL)
SEND_EMAILS    = os.getenv("SEND_EMAILS", "true").lower() == "true"


def _normalize_s3_bucket_name(bucket_value: str) -> str:
    value = (bucket_value or "").strip()
    if value.startswith("arn:aws:s3:::"):
        return value.split("arn:aws:s3:::", 1)[1]
    return value


S3_BUCKET = _normalize_s3_bucket_name(S3_BUCKET_RAW)

s3  = None
ses = None


def _init_aws_clients():
    # 1) Try explicit credentials only if both are provided.
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        try:
            explicit_session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY,
                region_name=AWS_REGION,
            )
            # Validate credentials at startup so bad keys fail fast.
            explicit_session.client("sts").get_caller_identity()
            print("✅ AWS clients initialized (explicit credentials)")
            return (
                explicit_session.client("s3"),
                explicit_session.client("ses"),
            )
        except Exception as e:
            print(f"⚠️ Explicit AWS credentials are invalid/unusable: {e}")

    # 2) Fallback: default provider chain (IAM role / env / aws profile).
    try:
        default_session = boto3.Session(region_name=AWS_REGION)
        default_session.client("sts").get_caller_identity()
        print("✅ AWS clients initialized (default credential chain)")
        return (
            default_session.client("s3"),
            default_session.client("ses"),
        )
    except Exception as e:
        print(f"❌ AWS client init failed: {e}")
        return (None, None)


s3, ses = _init_aws_clients()


# ==========================================
# Helpers
# ==========================================
def safe_str(v):
    return "" if v is None else str(v)


def parse_dt_to_iso(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        try:
            dt = datetime.fromisoformat(str(v).replace(" ", "T", 1))
        except Exception:
            return str(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def derive_bucket_key(resume_path, default_bucket=None):
    default_bucket = default_bucket or S3_BUCKET
    if not resume_path:
        return (None, None)
    path = resume_path.strip()
    if path.startswith("s3://"):
        no_prefix = path.replace("s3://", "", 1)
        parts = no_prefix.split("/", 1)
        return (parts[0], parts[1] if len(parts) > 1 else "")
    if "amazonaws.com/" in path:
        after = path.split("amazonaws.com/", 1)[1]
        parts = after.split("/", 1)
        if len(parts) == 1:
            return (default_bucket, parts[0])
        first, rest = parts[0], parts[1]
        if first == default_bucket or default_bucket in first:
            return (default_bucket, rest)
        if ".s3." in first:
            return (default_bucket, after)
        return (first, rest)
    if "/" in path:
        return (default_bucket, path.lstrip("/"))
    return (default_bucket, path)


STATUS_MAPPING = {
    "resume uploaded": "Resume Uploaded", "resumeuploaded": "Resume Uploaded",
    "uploaded": "Resume Uploaded",
    "pending": "Pending for Review", "pending review": "Pending for Review",
    "pending for review": "Pending for Review", "under review": "Pending for Review",
    "review": "Pending for Review",
    "first round": "1st Round Interview", "first interview": "1st Round Interview",
    "interview 1": "1st Round Interview", "1st interview": "1st Round Interview",
    "round 1": "1st Round Interview",
    "second round": "2nd Round Interview", "second interview": "2nd Round Interview",
    "interview 2": "2nd Round Interview", "2nd interview": "2nd Round Interview",
    "round 2": "2nd Round Interview",
    "selected": "Selected", "hired": "Selected", "approved": "Selected",
    "rejected": "Rejected", "declined": "Rejected", "not selected": "Rejected",
    "evaluation": "Evaluation in Progress", "evaluating": "Evaluation in Progress",
    "in progress": "Evaluation in Progress",
    "consent": "Consent from Applicant", "applicant consent": "Consent from Applicant",
    "offer letter": "Offer Letter Sent", "offer sent": "Offer Letter Sent", "offer": "Offer Letter Sent",
    "on hold": "On Hold - Criteria Not Met", "hold": "On Hold - Criteria Not Met",
    "criteria not met": "On Hold - Criteria Not Met",
    "priority": "Priority Review", "priority review": "Priority Review",
    "docs": "DOCS_UPLOADED", "docs uploaded": "DOCS_UPLOADED",
    "documents": "DOCS_UPLOADED", "documents uploaded": "DOCS_UPLOADED",
}


# ==========================================
# Email helpers
# ==========================================
def _send_email(subject, to_addresses, html_content, text_content=""):
    if not SEND_EMAILS or not ses:
        print(f"⚠️ Email skipped -> {subject}")
        return False
    try:
        ses.send_email(
            Source=SES_SENDER,
            Destination={"ToAddresses": to_addresses},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": html_content, "Charset": "UTF-8"},
                    **({"Text": {"Data": text_content, "Charset": "UTF-8"}} if text_content else {}),
                },
            },
        )
        print(f"✅ Email sent to {to_addresses}")
        return True
    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False


def send_status_changed_email(recipient_email, app_id, message_title, applicant_name):
    display_name = applicant_name or "Applicant"
    html = f"""<html><body>
        <h3>Dear {display_name},</h3>
        <p>{message_title}</p>
        <p><b>Application ID:</b> {app_id}</p>
        <br><p>Regards,<br>Chakora Hub Team</p>
    </body></html>"""
    to_list = [recipient_email, ADMIN_EMAIL] if recipient_email else [ADMIN_EMAIL]
    return _send_email(message_title, to_list, html)


def send_application_received_email(email, name, app_id, message_type, throttle_message=""):
    if message_type == "throttled_criteria_not_met":
        subject, accent, status_label = "Application Received - Chakora Hub", "#667eea", "On Hold - Reviewing"
        box_bg, box_border, box_text = "#fff3cd", "#ffc107", "#856404"
        box_title, box_msg = "⚠️ Application Status", throttle_message
        intro = "Thank you for applying to Chakora Hub! Your application has been received."
    elif message_type == "priority_review":
        subject, accent, status_label = "🌟 Priority Review - Chakora Hub", "#28a745", "Priority Review"
        box_bg, box_border, box_text = "#d4edda", "#28a745", "#155724"
        box_title = "🌟 Priority Review Status"
        box_msg = "Your profile meets our requirements and has been marked for <strong>priority review</strong>."
        intro = "Thank you for applying to Chakora Hub! We're impressed with your qualifications."
    else:
        subject, accent, status_label = "Application Received - Chakora Hub", "#667eea", "Under Review"
        box_bg, box_border, box_text = "#d1ecf1", "#17a2b8", "#0c5460"
        box_title, box_msg = "✅ Application Received", "Your resume has been received and is currently under review."
        intro = "Thank you for submitting your application to Chakora Hub!"

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0;">
<div style="max-width:600px;margin:20px auto;background:#fff;border-radius:10px;overflow:hidden;">
<div style="background:linear-gradient(135deg,{accent} 0%,#20c997 100%);color:#fff;padding:30px;text-align:center;">
<h1 style="margin:0;">Chakora Hub</h1></div>
<div style="padding:30px;">
<p style="font-size:18px;">Dear <strong style="color:{accent};">{name}</strong>,</p>
<p>{intro}</p>
<div style="background:#f8f9ff;border:2px solid {accent};border-radius:10px;padding:20px;margin:20px 0;text-align:center;">
<div style="font-size:14px;color:#666;">APPLICATION ID</div>
<div style="font-size:24px;font-weight:bold;color:{accent};font-family:monospace;">{app_id}</div>
</div>
<div style="background:{box_bg};border:2px solid {box_border};border-radius:10px;padding:20px;margin:20px 0;">
<div style="color:{box_text};font-size:18px;font-weight:bold;margin-bottom:10px;">{box_title}</div>
<div style="color:{box_text};">{box_msg}</div>
</div>
<p>Status: <strong>{status_label}</strong></p>
<p style="text-align:center;">
<a href="https://www.chakorahub.com/track-application?id={app_id}"
   style="display:inline-block;background:{accent};color:#fff;padding:12px 30px;
          text-decoration:none;border-radius:5px;font-weight:bold;">
Track Your Application</a></p>
</div>
<div style="text-align:center;padding:20px;color:#666;font-size:12px;border-top:1px solid #e0e0e0;">
<p style="margin:0;font-weight:bold;">Chakora Hub Recruitment Team</p>
</div></div></body></html>"""
    return _send_email(subject, [email], html)


def send_admin_new_application_email(name, email, phone, app_id, source, message_type,
                                      experience=0, qualification="", is_throttled=False, meets_criteria=True):
    if message_type == "throttled_criteria_not_met":
        emoji, label, color = "🔴", "THROTTLED - Criteria Not Met", "#dc3545"
        action = "⚠️ APPLICATION ON HOLD - Does not meet current hiring criteria"
    elif message_type == "priority_review":
        emoji, label, color = "🟢", "PRIORITY REVIEW", "#28a745"
        action = "🌟 PRIORITY APPLICATION - REVIEW ASAP!"
    else:
        emoji, label, color = "📋", "NORMAL APPLICATION", "#17a2b8"
        action = "✅ Normal application - Review when available"

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0;">
<div style="max-width:600px;margin:20px auto;background:#fff;border-radius:10px;overflow:hidden;">
<div style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:30px;text-align:center;">
<h1 style="margin:0;">{emoji} New Application Received</h1></div>
<div style="padding:30px;">
<p><strong style="color:{color};">{label}</strong> — Application ID: {app_id}</p>
<table style="width:100%;border-collapse:collapse;margin:15px 0;border:1px solid #e0e0e0;">
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;width:40%;">Name</td><td style="padding:8px;">{name}</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Email</td><td style="padding:8px;">{email}</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Phone</td><td style="padding:8px;">{phone or 'Not provided'}</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Source</td><td style="padding:8px;">{source}</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Experience</td><td style="padding:8px;">{experience} years</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Qualification</td><td style="padding:8px;">{qualification or 'Not specified'}</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Throttling</td><td style="padding:8px;">{'ACTIVE' if is_throttled else 'INACTIVE'}</td></tr>
<tr><td style="padding:8px;background:#f5f5f5;font-weight:bold;">Meets Criteria</td><td style="padding:8px;">{'YES' if meets_criteria else 'NO'}</td></tr>
</table>
<div style="background:#f8f9fa;border-left:5px solid {color};padding:15px;margin:20px 0;">{action}</div>
</div></div></body></html>"""
    return _send_email(f"{emoji} {label} - {app_id}", [ADMIN_EMAIL], html)


# ==========================================
# Pydantic models
# ==========================================
class ApplicationSubmission(BaseModel):
    name: str
    email: EmailStr
    phone: Optional[str] = ""
    source: Optional[str] = "Website"
    resume: str = Field(..., description="Base64-encoded resume file content")
    filename: str
    experience_years: Optional[float] = 0
    qualification: Optional[str] = ""


class StatusUpdateRequest(BaseModel):
    application_id: str
    status: str
    notes: Optional[str] = "Status updated by admin."
    admin_username: Optional[str] = "ADMIN"


class DeleteRequest(BaseModel):
    action: Optional[str] = "delete"
    application_id: str


# ==========================================
# 1) POST /upload-resume
# ==========================================
@app.post("/upload-resume")
def upload_resume(body: ApplicationSubmission):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail={"message": "Database connection failed"})
    cursor = conn.cursor(DictCursor)
    try:
        # Check open positions / throttling
        cursor.execute("""
            SELECT ID, ACCEPTING_APPLICATIONS, MIN_EXPERIENCE_YEARS,
                   MIN_QUALIFICATION, THROTTLE_MESSAGE, TITLE
            FROM NRM_POSITIONS WHERE STATUS = 'open'
            ORDER BY CREATED_DATE DESC FETCH FIRST 1 ROWS ONLY
        """)
        position = cursor.fetchone()

        is_throttled    = True
        min_experience  = 0.0
        min_qualification = ""
        throttle_message  = (
            "Thank you for applying to Chakora Hub. Currently, we do not have open "
            "positions. However, we will prioritize your profile & get back to you at the earliest."
        )

        if position:
            is_throttled      = position["ACCEPTING_APPLICATIONS"] != "Y"
            min_exp           = position["MIN_EXPERIENCE_YEARS"]
            min_qual          = position["MIN_QUALIFICATION"]
            throttle_msg      = position["THROTTLE_MESSAGE"]
            if min_exp  is not None: min_experience   = float(min_exp)
            if min_qual and min_qual.strip(): min_qualification = min_qual.strip()
            if throttle_msg and throttle_msg.strip(): throttle_message = throttle_msg.strip()

        # Criteria check (only when throttled)
        meets_criteria = True
        reasons = []
        if is_throttled:
            if min_experience > 0 and body.experience_years < min_experience:
                meets_criteria = False
                reasons.append(f"Min {min_experience} years required (has {body.experience_years})")
            if min_qualification:
                required = [q.strip().lower() for q in min_qualification.split(",")]
                aq = (body.qualification or "").lower().strip()
                if not any(r in aq or aq in r for r in required):
                    meets_criteria = False
                    reasons.append(f"Required qualification: {min_qualification}")

        application_id = f"APP_{datetime.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:8].upper()}"

        # Decode + upload resume to S3
        try:
            resume_bytes = base64.b64decode(body.resume)
        except Exception:
            raise HTTPException(status_code=400, detail={"message": "resume must be valid base64"})
        if not s3:
            raise HTTPException(status_code=500, detail={"message": "S3 not configured"})

        s3_key = f"resumes/{datetime.now().year}/{datetime.now().month}/{application_id}_{body.filename}"
        s3.put_object(
            Bucket=S3_BUCKET, Key=s3_key, Body=resume_bytes,
            ContentType="application/pdf" if body.filename.endswith(".pdf") else "application/msword",
            Metadata={"applicant-name": body.name, "email": body.email, "application-id": application_id},
        )
        s3_path = f"s3://{S3_BUCKET}/{s3_key}"

        # Determine status
        if is_throttled and not meets_criteria:
            status_description, message_type = "On Hold - Criteria Not Met", "throttled_criteria_not_met"
            notes = f"On hold. {'; '.join(reasons)}. Exp: {body.experience_years}y, Qual: {body.qualification}"
        elif is_throttled and meets_criteria:
            status_description, message_type = "Priority Review", "priority_review"
            notes = f"Priority - criteria met during throttling. Exp: {body.experience_years}y, Qual: {body.qualification}"
        else:
            status_description, message_type = "Resume Uploaded", "normal"
            notes = f"Initial submission. Exp: {body.experience_years}y, Qual: {body.qualification}"

        # Get status_id — Oracle uses FETCH FIRST instead of LIMIT
        cursor.execute(
            "SELECT ID FROM NRM_APPLICATION_STATUSES WHERE STATUS_NAME = :1",
            (status_description,)
        )
        status_row = cursor.fetchone()
        if status_row:
            status_id = status_row["ID"]
        else:
            # Insert new status and get its ID via RETURNING
            cursor.execute(
                """INSERT INTO NRM_APPLICATION_STATUSES
                   (STATUS_NAME, STATUS_ORDER, STATUS_DESCRIPTION, IS_ACTIVE)
                   VALUES (:1, 99, :2, 'Y')""",
                (status_description, f"Application status: {status_description}")
            )
            conn.commit()
            cursor.execute(
                "SELECT ID FROM NRM_APPLICATION_STATUSES WHERE STATUS_NAME = :1",
                (status_description,)
            )
            status_id = cursor.fetchone()["ID"]

        # Insert application record
        cursor.execute("""
            INSERT INTO NRM_APPLICATIONS
            (APPLICATION_ID, APPLICANT_NAME, EMAIL, PHONE, SOURCE,
             RESUME_S3_PATH, RESUME_FILENAME, CURRENT_STATUS_ID, NOTES)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
        """, (application_id, body.name, body.email, body.phone, body.source,
              s3_path, body.filename, status_id, notes))

        # Insert status history
        cursor.execute("""
            INSERT INTO NRM_APPLICATION_STATUS_HISTORY
            (APPLICATION_ID, NEW_STATUS_ID, CHANGED_BY, NOTES)
            VALUES (:1, :2, 'SYSTEM', :3)
        """, (application_id, status_id, notes))

        conn.commit()

        # Send emails
        send_application_received_email(body.email, body.name, application_id, message_type, throttle_message)
        send_admin_new_application_email(
            body.name, body.email, body.phone, application_id, body.source,
            message_type, body.experience_years, body.qualification, is_throttled, meets_criteria,
        )

        user_message = (
            throttle_message if message_type == "throttled_criteria_not_met"
            else "🌟 Your application has been marked for PRIORITY REVIEW!" if message_type == "priority_review"
            else "Your resume has been received and is under review."
        )
        html_message_type = {
            "throttled_criteria_not_met": "warning",
            "priority_review": "success"
        }.get(message_type, "info")

        return {
            "message": "Resume uploaded successfully",
            "application_id": application_id,
            "status": message_type,
            "user_message": user_message,
            "html_message_type": html_message_type,
            "is_throttled": is_throttled,
            "meets_criteria": meets_criteria,
            "throttle_message": throttle_message,
            "status_description": status_description,
            "served_by": "onboarding_service:8100",
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ upload_resume error: {e}")
        raise HTTPException(status_code=500, detail={"message": f"Error: {str(e)}"})
    finally:
        try: cursor.close(); conn.close()
        except Exception: pass


# ==========================================
# 2) GET /applications
# ==========================================
@app.get("/applications")
def list_applications():
    conn = get_db_connection()
    if not conn:
        return {"applications": [], "total_count": 0}
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute("""
            SELECT a.APPLICATION_ID, a.APPLICANT_NAME, a.EMAIL, a.PHONE,
                   s.STATUS_NAME, a.APPLIED_DATE, a.LAST_UPDATED
            FROM NRM_APPLICATIONS a
            LEFT JOIN NRM_APPLICATION_STATUSES s ON a.CURRENT_STATUS_ID = s.ID
            ORDER BY a.APPLIED_DATE DESC
        """)
        rows = cursor.fetchall()
        return {
            "applications": [
                {
                    "application_id": r["APPLICATION_ID"],
                    "name":           r["APPLICANT_NAME"],
                    "email":          r["EMAIL"],
                    "phone":          r["PHONE"],
                    "status":         r["STATUS_NAME"],
                    "applied_date":   parse_dt_to_iso(r["APPLIED_DATE"]),
                    "last_updated":   parse_dt_to_iso(r["LAST_UPDATED"]),
                }
                for r in rows
            ],
            "total_count": len(rows),
            "served_by": "onboarding_service:8100",
        }
    except Exception as e:
        print(f"❌ list_applications error: {e}")
        return {"applications": [], "total_count": 0}
    finally:
        try: cursor.close(); conn.close()
        except Exception: pass


# ==========================================
# 3) GET /application-status/{application_id}
#    track-application.html expects {"applications": [...]}
# ==========================================
@app.get("/application-status/{application_id}")
def get_application_status(application_id: str):
    conn = get_db_connection()
    if not conn:
        return {"success": False, "applications": [], "total_applications": 0}
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute("""
            SELECT a.APPLICATION_ID, a.APPLICANT_NAME, a.EMAIL, a.PHONE,
                   COALESCE(s.STATUS_NAME, 'Unknown') AS STATUS_NAME,
                   s.ID AS STATUS_ID,
                   a.APPLIED_DATE, a.LAST_UPDATED
            FROM NRM_APPLICATIONS a
            LEFT JOIN NRM_APPLICATION_STATUSES s ON a.CURRENT_STATUS_ID = s.ID
            WHERE a.APPLICATION_ID = :1
        """, (application_id,))
        rows = cursor.fetchall()
        applications = [
            {
                "application_id": safe_str(r["APPLICATION_ID"]),
                "applicant_name": safe_str(r["APPLICANT_NAME"]),
                "email":          safe_str(r["EMAIL"]),
                "phone":          safe_str(r["PHONE"]),
                "current_status": {"name": safe_str(r["STATUS_NAME"]), "order": r["STATUS_ID"]},
                "applied_date":   parse_dt_to_iso(r["APPLIED_DATE"]),
                "last_updated":   parse_dt_to_iso(r["LAST_UPDATED"]),
            }
            for r in rows
        ]
        return {
            "success": True,
            "applications": applications,
            "total_applications": len(applications),
            "timestamp": datetime.utcnow().isoformat(),
            "served_by": "onboarding_service:8100",
        }
    except Exception as e:
        print(f"❌ get_application_status error: {e}")
        return {"success": False, "applications": [], "total_applications": 0}
    finally:
        try: cursor.close(); conn.close()
        except Exception: pass


# ==========================================
# 4) PUT /update-status
# ==========================================
@app.put("/update-status")
def update_status(body: StatusUpdateRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail={"message": "Database connection failed"})
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT EMAIL, APPLICANT_NAME FROM NRM_APPLICATIONS WHERE APPLICATION_ID = :1",
            (body.application_id,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail={"message": "Application not found"})
        applicant_email, applicant_name = row["EMAIL"], row["APPLICANT_NAME"]

        new_status = body.status.strip()
        mapped = STATUS_MAPPING.get(new_status.lower())
        if mapped:
            new_status = mapped

        cursor.execute(
            "SELECT CURRENT_STATUS_ID FROM NRM_APPLICATIONS WHERE APPLICATION_ID = :1",
            (body.application_id,)
        )
        old_status_id = cursor.fetchone()["CURRENT_STATUS_ID"]

        cursor.execute("SELECT ID, STATUS_NAME FROM NRM_APPLICATION_STATUSES ORDER BY ID")
        available = cursor.fetchall()
        status_lookup = {r["STATUS_NAME"].upper(): (r["ID"], r["STATUS_NAME"]) for r in available}

        new_status_id, matched_name = None, None
        # Exact → case-insensitive → partial
        for r in available:
            if r["STATUS_NAME"] == new_status:
                new_status_id, matched_name = r["ID"], r["STATUS_NAME"]
                break
        if not new_status_id and new_status.upper() in status_lookup:
            new_status_id, matched_name = status_lookup[new_status.upper()]
        if not new_status_id:
            for r in available:
                if new_status.upper() in r["STATUS_NAME"].upper() or r["STATUS_NAME"].upper() in new_status.upper():
                    new_status_id, matched_name = r["ID"], r["STATUS_NAME"]
                    break

        if not new_status_id:
            raise HTTPException(status_code=400, detail={
                "message": f"Invalid status: '{new_status}'",
                "valid_statuses": [r["STATUS_NAME"] for r in available],
            })

        cursor.execute("""
            UPDATE NRM_APPLICATIONS
            SET CURRENT_STATUS_ID = :1, LAST_UPDATED = CURRENT_TIMESTAMP, NOTES = :2
            WHERE APPLICATION_ID = :3
        """, (new_status_id, body.notes, body.application_id))

        cursor.execute("""
            INSERT INTO NRM_APPLICATION_STATUS_HISTORY
            (APPLICATION_ID, OLD_STATUS_ID, NEW_STATUS_ID, CHANGED_BY, NOTES)
            VALUES (:1, :2, :3, :4, :5)
        """, (body.application_id, old_status_id, new_status_id, body.admin_username, body.notes))

        conn.commit()
        send_status_changed_email(
            applicant_email, body.application_id,
            f"Your application status has been updated to {matched_name}.", applicant_name,
        )

        return {
            "message": "Status updated",
            "application_id": body.application_id,
            "new_status": matched_name,
            "new_status_id": new_status_id,
            "served_by": "onboarding_service:8100",
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ update_status error: {e}")
        raise HTTPException(status_code=500, detail={"message": "Internal server error", "error": str(e)})
    finally:
        try: cursor.close(); conn.close()
        except Exception: pass


# ==========================================
# 5) DELETE /delete-application
# ==========================================
@app.delete("/delete-application")
def delete_application(body: DeleteRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail={"message": "Database connection failed"})
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT EMAIL, APPLICANT_NAME, RESUME_S3_PATH FROM NRM_APPLICATIONS WHERE APPLICATION_ID = :1",
            (body.application_id,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail={"message": "Application not found"})
        applicant_email = row["EMAIL"]
        applicant_name  = row["APPLICANT_NAME"]
        resume_path     = row["RESUME_S3_PATH"]

        s3_delete_error = None
        if resume_path and s3:
            bucket, key = derive_bucket_key(resume_path)
            if bucket and key:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                except Exception as e:
                    s3_delete_error = str(e)

        cursor.execute(
            "DELETE FROM NRM_APPLICATION_STATUS_HISTORY WHERE APPLICATION_ID = :1",
            (body.application_id,)
        )
        cursor.execute(
            "DELETE FROM NRM_APPLICATIONS WHERE APPLICATION_ID = :1",
            (body.application_id,)
        )
        conn.commit()

        send_status_changed_email(
            applicant_email, body.application_id,
            "Your application has been deleted.", applicant_name
        )

        result = {"message": "Application deleted", "application_id": body.application_id,
                  "served_by": "onboarding_service:8100"}
        if s3_delete_error:
            result["s3_delete_error"] = s3_delete_error
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ delete_application error: {e}")
        raise HTTPException(status_code=500, detail={"message": "Internal server error", "error": str(e)})
    finally:
        try: cursor.close(); conn.close()
        except Exception: pass


# ==========================================
# 6) GET /download-resume/{application_id}
# ==========================================
@app.get("/download-resume/{application_id}")
def download_resume(application_id: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail={"message": "Database connection failed"})
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT RESUME_S3_PATH FROM NRM_APPLICATIONS WHERE APPLICATION_ID = :1",
            (application_id,)
        )
        row = cursor.fetchone()
        if not row or not row["RESUME_S3_PATH"]:
            raise HTTPException(status_code=404, detail={"message": "No resume found"})

        bucket, key = derive_bucket_key(row["RESUME_S3_PATH"])
        if not bucket or not key or not s3:
            raise HTTPException(status_code=400, detail={"message": "Could not resolve resume file"})

        file_name = key.split("/")[-1]
        url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": bucket, "Key": key,
                "ResponseContentDisposition": f'attachment; filename="{_up.quote(file_name)}"',
            },
            ExpiresIn=900,
        )
        return RedirectResponse(url)
    except HTTPException:
        raise
    finally:
        try: cursor.close(); conn.close()
        except Exception: pass


@app.get("/health")
def health():
    return {"status": "ok", "service": "onboarding-service", "port": 8100}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8100)
