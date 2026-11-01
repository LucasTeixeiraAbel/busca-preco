import asyncio

import pytest

from backend import mailer


def test_resend_sends_text_email(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    monkeypatch.delenv("SENDGRID_API_KEY", raising=False)
    monkeypatch.delenv("AWS_SES_REGION", raising=False)
    monkeypatch.setenv("ALERT_FROM_EMAIL", "Radar <alerts@example.com>")
    sent = {}

    class Response:
        def raise_for_status(self):
            return None

    class Client:
        def __init__(self, timeout):
            assert timeout == 4

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, *, headers, json):
            sent.update(url=url, headers=headers, json=json)
            return Response()

    monkeypatch.setattr(mailer.httpx, "AsyncClient", Client)
    asyncio.run(mailer.send_email("user@example.com", "Oferta", "Preço caiu"))

    assert sent["url"] == "https://api.resend.com/emails"
    assert sent["headers"]["Authorization"] == "Bearer re_test_key"
    assert sent["json"] == {
        "from": "Radar <alerts@example.com>",
        "to": ["user@example.com"],
        "subject": "Oferta",
        "text": "Preço caiu",
    }


def test_mailer_fails_closed_without_provider(monkeypatch):
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    monkeypatch.delenv("SENDGRID_API_KEY", raising=False)
    monkeypatch.delenv("AWS_SES_REGION", raising=False)
    monkeypatch.setenv("ALERT_FROM_EMAIL", "alerts@example.com")
    with pytest.raises(RuntimeError, match="Configure RESEND_API_KEY"):
        asyncio.run(mailer.send_email("user@example.com", "Oferta", "Preço caiu"))
