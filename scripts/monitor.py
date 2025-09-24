import logging
import smtplib
from email.mime.text import MIMEText
import os

# Ensure logs directory exists
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_DIR}/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_event(message: str, level="info"):
    getattr(logging, level)(message)

'''def send_alert(subject: str, body: str, to_email: str):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = "etl@system.com"
    msg["To"] = to_email

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        # server.login(USER, PASSWORD)  # credenciales seguras en .env
        server.send_message(msg)'''

def send_alert(subject, body, to_email):
    logging.error(f"ALERT: {subject} -> {body} (not sent, demo mode)")
