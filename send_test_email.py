#!/usr/bin/env python3
import argparse
import mimetypes
import os
import smtplib
import sys
from email.message import EmailMessage

from env_config import load_env_file

load_env_file()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a simple SMTP test email using .env settings.")
    parser.add_argument(
        "--profile",
        choices=["test", "production"],
        default="test",
        help="Mail profile to use from .env (default: test).",
    )
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
    parser.add_argument(
        "--smtp-debug",
        action="store_true",
        help="Print SMTP client/server conversation for debugging.",
    )
    parser.add_argument(
        "--attach",
        action="append",
        help="Path to a file to attach. Can be repeated.",
    )
    return parser.parse_args()


def get_mail_settings(profile: str) -> dict[str, object]:
    if profile == "production":
        prefixes = [
            "MAIL_PROD_",
            "SMTP_PROD_",
        ]
    else:
        prefixes = [
            "MAIL_TEST_",
            "SMTP_TEST_",
        ]

    def get_value(name: str, fallback: str = "") -> str:
        candidates: list[str] = []
        for prefix in prefixes:
            candidates.append(f"{prefix}{name}")
        if profile == "production":
            candidates.extend(
                [
                    f"MAIL_{name}",
                    f"SMTP_{name}",
                ]
            )
        for key in candidates:
            value = os.getenv(key)
            if value is not None and value.strip():
                return value.strip()
        return fallback

    host = get_value("HOST")
    port = int(get_value("PORT", "587") or "587")
    username = get_value("USERNAME", get_value("USER"))
    password = get_value("PASSWORD", get_value("PASS"))
    from_address = get_value("FROM_ADDRESS", get_value("FROM", username))
    auth_mode = get_value("AUTH_MODE", "login").lower()
    use_auth = auth_mode not in {"none", "noauth", "false", "0", "no"}
    encryption = get_value("ENCRYPTION").lower()
    use_tls_key_present = any(os.getenv(f"{prefix}ENCRYPTION") is not None for prefix in prefixes)
    if profile == "production":
        use_tls_key_present = use_tls_key_present or "MAIL_ENCRYPTION" in os.environ
    use_tls = (
        encryption not in {"", "null", "none", "false", "0", "no"}
        if use_tls_key_present
        else get_value("USE_TLS", "true").lower() not in {"0", "false", "no"}
    )
    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "from_address": from_address,
        "use_tls": use_tls,
        "use_auth": use_auth,
        "profile": profile,
    }


def build_message(
    from_address: str,
    to_recipients: list[str],
    cc_recipients: list[str],
    subject: str,
    body: str,
    attachments: list[str],
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = from_address
    message["To"] = ", ".join(to_recipients)
    if cc_recipients:
        message["Cc"] = ", ".join(cc_recipients)
    message["Subject"] = subject
    message.set_content(body)
    for attachment_path in attachments:
        mime_type, _ = mimetypes.guess_type(attachment_path)
        if mime_type:
            maintype, subtype = mime_type.split("/", 1)
        else:
            maintype, subtype = "application", "octet-stream"
        with open(attachment_path, "rb") as handle:
            message.add_attachment(
                handle.read(),
                maintype=maintype,
                subtype=subtype,
                filename=os.path.basename(attachment_path),
            )
    return message


def main() -> int:
    args = parse_args()
    settings = get_mail_settings(args.profile)

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
    attachments = args.attach or []

    missing_attachments = [path for path in attachments if not os.path.isfile(path)]
    if missing_attachments:
        print(
            "Missing attachment file(s): " + ", ".join(missing_attachments),
            file=sys.stderr,
        )
        return 1

    message = build_message(
        from_address=str(settings["from_address"]),
        to_recipients=to_recipients,
        cc_recipients=cc_recipients,
        subject=args.subject,
        body=args.body,
        attachments=attachments,
    )

    try:
        with smtplib.SMTP(str(settings["host"]), int(settings["port"]), timeout=60) as server:
            if args.smtp_debug:
                server.set_debuglevel(1)
            if bool(settings["use_tls"]):
                server.starttls()
            if bool(settings["use_auth"]) and settings["username"]:
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
    print(f"Profile: {settings['profile']}")
    print(f"From: {settings['from_address']}")
    print(f"To: {', '.join(to_recipients)}")
    if cc_recipients:
        print(f"Cc: {', '.join(cc_recipients)}")
    print(f"Subject: {args.subject}")
    if attachments:
        print(f"Attachments: {', '.join(attachments)}")
    if "mailtrap" in str(settings["host"]).lower():
        print("Mailtrap sandbox detected: check the Mailtrap inbox, not the recipient mailbox.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
