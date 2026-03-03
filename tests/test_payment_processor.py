"""Tests for the payment processing module."""
import pytest
from unittest.mock import patch
from payment_processor import process_payment, get_transaction, refund_payment


def test_successful_payment():
    result = process_payment(
        customer_id="C-001",
        amount=100.00,
        currency="USD",
        merchant_id="M-001",
        description="Test payment"
    )
    assert result["status"] == "completed"
    assert result["amount"] == 100.00


def test_invalid_currency():
    with pytest.raises(ValueError, match="Unsupported currency"):
        process_payment(
            customer_id="C-001",
            amount=100.00,
            currency="BTC",
            merchant_id="M-001"
        )


def test_negative_amount():
    with pytest.raises(ValueError, match="Amount must be positive"):
        process_payment(
            customer_id="C-001",
            amount=-50.00,
            currency="USD",
            merchant_id="M-001"
        )


def test_missing_customer_id():
    with pytest.raises(ValueError, match="customer_id and merchant_id are required"):
        process_payment(
            customer_id="",
            amount=100.00,
            currency="USD",
            merchant_id="M-001"
        )


def test_get_transaction():
    result = process_payment(
        customer_id="C-002",
        amount=250.00,
        currency="USD",
        merchant_id="M-002"
    )
    txn = get_transaction(result["transaction_id"])
    assert txn is not None
    assert txn["amount"] == 250.00
    assert txn["customer_id"] == "C-002"


def test_refund():
    result = process_payment(
        customer_id="C-003",
        amount=75.00,
        currency="USD",
        merchant_id="M-003"
    )
    refund = refund_payment(result["transaction_id"], reason="Customer request")
    assert refund["status"] == "refunded"


def test_refund_nonexistent():
    with pytest.raises(ValueError, match="not found"):
        refund_payment("nonexistent-id")
