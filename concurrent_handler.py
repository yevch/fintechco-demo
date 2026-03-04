"""
Concurrent Payment Handler

Processes batch payments using thread pool for high-throughput scenarios
like merchant settlement batches and payroll disbursements.
"""
import uuid
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from models import SessionLocal, Transaction, AuditLog, PaymentStatus
from config import SUPPORTED_CURRENCIES, FRAUD_THRESHOLD

logger = logging.getLogger(__name__)

MAX_WORKERS = 8
BATCH_TIMEOUT = 60


def process_batch_payments(payments):
    """Process a list of payments concurrently.

    Each payment is submitted in its own thread for maximum throughput.
    Used for merchant settlement batches and bulk disbursements.

    Args:
        payments: list of dicts with customer_id, amount, currency, merchant_id

    Returns:
        list of result dicts with transaction_id, status, amount
    """
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_payment = {}
        for payment in payments:
            future = executor.submit(_process_single, payment)
            future_to_payment[future] = payment

        for future in as_completed(future_to_payment, timeout=BATCH_TIMEOUT):
            payment = future_to_payment[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Payment failed for customer {payment.get('customer_id')}: {e}")
                results.append({
                    "customer_id": payment.get("customer_id"),
                    "status": "error",
                    "error": "Payment processing failed"
                })

    return results


def _process_single(payment_data):
    """Process a single payment within the batch.

    Creates a new database session per thread and handles the full
    payment lifecycle: validation, creation, gateway submission, status update.
    """
    session = SessionLocal()

    customer_id = payment_data.get("customer_id")
    amount = float(payment_data.get("amount", 0))
    currency = payment_data.get("currency", "USD")
    merchant_id = payment_data.get("merchant_id")
    idempotency_key = payment_data.get("idempotency_key")

    try:
        if not customer_id or not merchant_id:
            raise ValueError("customer_id and merchant_id required")
        if amount <= 0:
            raise ValueError("Amount must be positive")
        if currency not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency: {currency}")

        if idempotency_key:
            existing = session.query(Transaction).filter(
                Transaction.idempotency_key == idempotency_key
            ).first()
            if existing:
                return {
                    "transaction_id": existing.id,
                    "status": existing.status.value,
                    "amount": existing.amount,
                    "currency": existing.currency,
                    "duplicate": True
                }

        transaction_id = str(uuid.uuid4())

        txn = Transaction(
            id=transaction_id,
            customer_id=customer_id,
            amount=amount,
            currency=currency,
            merchant_id=merchant_id,
            description=payment_data.get("description"),
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key
        )
        session.add(txn)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = session.query(Transaction).filter(
                Transaction.idempotency_key == idempotency_key
            ).first()
            return {
                "transaction_id": existing.id,
                "status": existing.status.value,
                "amount": existing.amount,
                "currency": existing.currency,
                "duplicate": True,
            }

        _log_audit(session, transaction_id, "CREATED", f"Batch payment: {amount} {currency}")

        # Simulate gateway latency
        time.sleep(0.05)
        gateway_ok = amount <= 50000

        if gateway_ok:
            txn.status = PaymentStatus.COMPLETED
            _log_audit(session, transaction_id, "COMPLETED", "Gateway confirmed (batch)")
        else:
            txn.status = PaymentStatus.FAILED
            _log_audit(session, transaction_id, "FAILED", "Gateway rejected (batch)")

        txn.updated_at = datetime.utcnow()
        session.commit()

        return {
            "transaction_id": transaction_id,
            "status": txn.status.value,
            "amount": amount,
            "currency": currency
        }

    except Exception as e:
        session.rollback()
        logger.error(f"Error processing payment: {e}")
        raise
    finally:
        session.close()


def _log_audit(session, transaction_id, action, details):
    entry = AuditLog(
        transaction_id=transaction_id,
        action=action,
        details=details
    )
    session.add(entry)
