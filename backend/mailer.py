"""Async transactional email delivery through Resend, SendGrid, or Amazon SES."""
from __future__ import annotations

import asyncio
import os

import boto3
import httpx


async def send_email(recipient: str, subject: str, body: str) -> None:
    sender = os.environ.get("ALERT_FROM_EMAIL")
    if not sender:
        raise RuntimeError("ALERT_FROM_EMAIL não configurado")
    if os.getenv("RESEND_API_KEY"):
        async with httpx.AsyncClient(timeout=4) as client:
            response = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {os.environ['RESEND_API_KEY']}", "Content-Type": "application/json"},
                json={"from": sender, "to": [recipient], "subject": subject, "text": body},
            )
            response.raise_for_status()
    elif os.getenv("SENDGRID_API_KEY"):
        async with httpx.AsyncClient(timeout=4) as client:
            response = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {os.environ['SENDGRID_API_KEY']}", "Content-Type": "application/json"},
                json={
                    "personalizations": [{"to": [{"email": recipient}]}],
                    "from": {"email": sender},
                    "subject": subject,
                    "content": [{"type": "text/plain", "value": body}],
                },
            )
            response.raise_for_status()
    elif os.getenv("AWS_SES_REGION"):
        client = boto3.client("ses", region_name=os.environ["AWS_SES_REGION"])
        await asyncio.wait_for(
            asyncio.to_thread(
                client.send_email,
                Source=sender,
                Destination={"ToAddresses": [recipient]},
                Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
            ),
            timeout=4,
        )
    else:
        raise RuntimeError("Configure RESEND_API_KEY, SENDGRID_API_KEY ou AWS_SES_REGION para habilitar e-mail")
