import pytest
from pydantic import ValidationError

from app.config import Settings
from app.schemas import PaymentCreate


@pytest.mark.parametrize("amount", ["0", "-1", "0.001", "10000000000000000", "NaN", "Infinity"])
def test_invalid_amounts(payment_request, amount):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**payment_request.model_dump(), "amount": amount})


@pytest.mark.parametrize("url", ["ftp://example.com/file", "not-a-url", "http://user:pass@host/a"])
def test_invalid_webhook(payment_request, url):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**payment_request.model_dump(), "webhook_url": url})


def test_fingerprint_normalizes_amount_and_metadata_order(payment_request):
    first = PaymentCreate.model_validate(
        {**payment_request.model_dump(), "amount": "100.0", "metadata": {"a": 1, "b": 2}}
    )
    second = PaymentCreate.model_validate(
        {**payment_request.model_dump(), "amount": "100.00", "metadata": {"b": 2, "a": 1}}
    )
    assert first.fingerprint() == second.fingerprint()
    assert first.fingerprint() != payment_request.fingerprint()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_metadata_rejects_nonfinite_numbers(payment_request, value):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(
            {**payment_request.model_dump(), "metadata": {"nested": [value]}}
        )


@pytest.mark.parametrize(
    "values",
    [
        {"api_key": ""},
        {"gateway_delay_min": 6},
        {"gateway_success_rate": 1.1},
        {"outbox_poll_interval": 0},
        {"webhook_timeout": -1},
    ],
)
def test_invalid_settings(values):
    with pytest.raises(ValidationError):
        Settings(**values)
