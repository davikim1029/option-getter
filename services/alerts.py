# alerts.py
import os
import smtplib
import time
from email.mime.text import MIMEText
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from services.logging.logger_singleton import getLogger

logger = getLogger()


def send_alert(message: str, debug: bool = False):
    """
    Send an alert via SMS (email-to-text gateway). If the message exceeds
    a single text length, it will be split into multiple chunks.

    Falls back gracefully and logs all issues.
    """
    load_dotenv()

    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASS = load_encrypted_password()
    EMAIL_HOST = os.getenv("EMAIL_HOST")
    EMAIL_PORT = int(os.getenv("EMAIL_PORT"))
    SMS_TO = os.getenv("SMS_TO")

    if not all([EMAIL_FROM, EMAIL_PASS, EMAIL_HOST, EMAIL_PORT, SMS_TO]):
        logger.logMessage("[ALERT] Missing required email/SMS environment variables.")
        return

    MAX_SMS_LENGTH = 100

    try:
        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
            server.login(EMAIL_FROM, EMAIL_PASS)

            # Split the message into chunks if needed
            chunks = _split_message(message, MAX_SMS_LENGTH)

            for idx, chunk in enumerate(chunks, 1):
                sms_msg = MIMEText(chunk)
                sms_msg["From"] = EMAIL_FROM
                sms_msg["To"] = SMS_TO
                
                if len(chunks) > 1:
                    sms_msg["Subject"] = f"Part {idx}/{len(chunks)}"
                else:
                    sms_msg["Subject"] = ""

                server.send_message(sms_msg)
                if debug:
                    logger.logMessage(f"Text part {idx}/{len(chunks)} sent to {SMS_TO}")

                # Avoid tripping carrier limits or rate filters
                time.sleep(2)

    except Exception as e:
        logger.logMessage(f"[ALERT ERROR] {e}")


def send_alert_alternate(message: str):
    """
    Alternate fallback alert sender using STARTTLS.
    """
    load_dotenv()
    to_number = os.getenv("SMS_TO")
    smtp_server = os.getenv("EMAIL_HOST")
    smtp_port = int(os.getenv("EMAIL_PORT"))
    smtp_user = os.getenv("EMAIL_FROM")
    smtp_pass = os.getenv("SMTP_PASSWORD")

    if not all([to_number, smtp_server, smtp_user, smtp_pass]):
        logger.logMessage(f"[ALERT] {message}")
        return

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, to_number, message)
        server.quit()
        logger.logMessage(f"[Alert sent] {message}")
    except Exception as e:
        logger.logMessage(f"[Alert error] {e}")


def load_encrypted_password() -> str:
    """
    Load and decrypt the email password from local encryption files.
    """
    with open("encryption/secret.key", "rb") as key_file:
        key = key_file.read()

    with open("encryption/email_password.enc", "rb") as enc_file:
        encrypted = enc_file.read()

    fernet = Fernet(key)
    return fernet.decrypt(encrypted).decode()


def _split_message(message: str, max_length: int):
    """
    Splits the message into chunks that fit within max_length.
    Attempts to split on line breaks or word boundaries where possible.
    """
    if len(message) <= max_length:
        return [message]

    chunks = []
    while message:
        if len(message) <= max_length:
            chunks.append(message)
            break

        # Try to split at the last newline or space within the limit
        split_point = max(
            message.rfind("\n", 0, max_length),
            message.rfind(" ", 0, max_length),
        )

        if split_point == -1:
            split_point = max_length

        chunks.append(message[:split_point].strip())
        message = message[split_point:].strip()

    return chunks
