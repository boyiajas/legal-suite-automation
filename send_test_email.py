#!/usr/bin/env python3
import argparse
import os
import smtplib
import sys
from email.message import EmailMessage

from env_config import load_env_file

load_env_file()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a simple SMTP test email using .env settings.")
    parser.add_argument(
        "--to",
        action="append",
        help="Recipient email address. Can be repeated. Defaults to MAIL_FROM_ADDRESS if omitted.",
    )
    parser.add_argument(
        "--cc",
        action="append",
        help="CC email address. Can be repeated.",
    )
    parser.add_argument(
        "--subject",
        default="SMTP Test Message",
        help="Email subject.",
    )
    parser.add_argument(
        "--body",
        default="This is a test email sent by send_test_email.py.",
        help="Email body.",
    )
    return parser.parse_args()


def get_mail_settings() -> dict[str, object]:
    host = os.getenv("MAIL_HOST", os.getenv("SMTP_HOST", "")).strip()
    port = int(os.getenv("MAIL_PORT", os.getenv("SMTP_PORT", "587")).strip() or "587")
    username = os.getenv("MAIL_USERNAME", os.getenv("SMTP_USER", "")).strip()
    password = os.getenv("MAIL_PASSWORD", os.getenv("SMTP_PASS", "")).strip()
    from_address = os.getenv("MAIL_FROM_ADDRESS", os.getenv("SMTP_FROM", username)).strip()
    encryption = os.getenv("MAIL_ENCRYPTION", "").strip().lower()
    use_tls = (
        encryption not in {"", "null", "none", "false", "0", "no"}
        if "MAIL_ENCRYPTION" in os.environ
        else os.getenv("SMTP_USE_TLS", "true").strip().lower() not in {"0", "false", "no"}
    )
    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "from_address": from_address,
        "use_tls": use_tls,
    }


def build_message(
    from_address: str,
    to_recipients: list[str],
    cc_recipients: list[str],
    subject: str,
    body: str,
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = from_address
    message["To"] = ", ".join(to_recipients)
    if cc_recipients:
        message["Cc"] = ", ".join(cc_recipients)
    message["Subject"] = subject
    message.set_content(body)
    return message


def main() -> int:
    args = parse_args()
    settings = get_mail_settings()

    missing = [name for name in ("host", "from_address") if not settings[name]]
    if missing:
        print(
            "Missing mail settings: "
            + ", ".join("MAIL_HOST" if name == "host" else "MAIL_FROM_ADDRESS" for name in missing),
            file=sys.stderr,
        )
        return 1

    to_recipients = args.to or [str(settings["from_address"])]
    cc_recipients = args.cc or []

    message = build_message(
        from_address=str(settings["from_address"]),
        to_recipients=to_recipients,
        cc_recipients=cc_recipients,
        subject=args.subject,
        body=args.body,
    )

    try:
        with smtplib.SMTP(str(settings["host"]), int(settings["port"]), timeout=60) as server:
            if bool(settings["use_tls"]):
                server.starttls()
            if settings["username"]:
                server.login(str(settings["username"]), str(settings["password"]))
            server.send_message(
                message,
                from_addr=str(settings["from_address"]),
                to_addrs=to_recipients + cc_recipients,
            )
    except Exception as exc:
        print(f"Email send failed: {exc}", file=sys.stderr)
        return 1

    print("Email sent successfully.")
    print(f"From: {settings['from_address']}")
    print(f"To: {', '.join(to_recipients)}")
    if cc_recipients:
        print(f"Cc: {', '.join(cc_recipients)}")
    print(f"Subject: {args.subject}")
    if "mailtrap" in str(settings["host"]).lower():
        print("Mailtrap sandbox detected: check the Mailtrap inbox, not the recipient mailbox.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
