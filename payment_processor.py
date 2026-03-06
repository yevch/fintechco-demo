"""
Payment Processing Module for FinTechCo

Handles payment creation, processing, and settlement for digital payment services.
Supports multiple currencies, retry logic, and basic fraud screening.
"""
import uuid
import time
import logging
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from models import SessionLocal, Transaction, AuditLog, PaymentStatus
from config import MAX_RETRY_ATTEMPTS, SUPPORTED_CURRENCIES, FRAUD_THRESHOLD, PAYMENT_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


def process_payment(customer_id, amount, currency, merchant_id, description=None, idempotency_key=None):
    """Process a payment transaction end-to-end.

    This function handles the full lifecycle: validation, fraud check,
    gateway submission, and status updates. Supports idempotency keys
    for safe retries.
    """
    session = SessionLocal()

    try:
        if not customer_id or not merchant_id:
            raise ValueError("customer_id and merchant_id are required")

        if amount <= 0:
            raise ValueError("Amount must be positive")

        if currency not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency: {currency}. Supported: {SUPPORTED_CURRENCIES}")

        fraud_result = check_fraud(customer_id, amount, currency, session)
        if fraud_result["flagged"]:
            logger.warning(f"Fraud flag for customer {customer_id}: {fraud_result['reason']}")
            return {"status": "rejected", "reason": fraud_result["reason"]}

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
                    "duplicate": True,
                }

        transaction_id = str(uuid.uuid4())

        txn = Transaction(
            id=transaction_id,
            customer_id=customer_id,
            amount=amount,
            currency=currency,
            merchant_id=merchant_id,
            description=description,
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

        log_audit(session, transaction_id, "CREATED", f"Payment of {amount} {currency} initiated")

        gateway_response = submit_to_gateway(transaction_id, amount, currency, merchant_id)

        if gateway_response["success"]:
            txn.status = PaymentStatus.COMPLETED
            log_audit(session, transaction_id, "COMPLETED", "Gateway confirmed")
        else:
            if txn.retry_count < MAX_RETRY_ATTEMPTS:
                txn.retry_count += 1
                txn.status = PaymentStatus.PROCESSING
                log_audit(session, transaction_id, "RETRY", f"Attempt {txn.retry_count}")
                time.sleep(0.1)
                return retry_payment(session, txn)
            else:
                txn.status = PaymentStatus.FAILED
                log_audit(session, transaction_id, "FAILED", f"Max retries exceeded: {gateway_response.get('error')}")

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
        logger.error(f"Payment processing error: {str(e)}")
        raise
    finally:
        session.close()


def retry_payment(session, txn):
    """Retry a failed payment by resubmitting to the gateway."""
    gateway_response = submit_to_gateway(txn.id, txn.amount, txn.currency, txn.merchant_id)

    if gateway_response["success"]:
        txn.status = PaymentStatus.COMPLETED
        log_audit(session, txn.id, "COMPLETED", f"Succeeded on retry {txn.retry_count}")
    else:
        if txn.retry_count < MAX_RETRY_ATTEMPTS:
            txn.retry_count += 1
            time.sleep(0.1)
            return retry_payment(session, txn)
        else:
            txn.status = PaymentStatus.FAILED
            log_audit(session, txn.id, "FAILED", "Max retries after retry loop")

    txn.updated_at = datetime.utcnow()
    session.commit()
    return {
        "transaction_id": txn.id,
        "status": txn.status.value,
        "amount": txn.amount,
        "currency": txn.currency
    }


def submit_to_gateway(transaction_id, amount, currency, merchant_id):
    """Submit payment to the external payment gateway.

    In production, this calls the actual gateway API. For now,
    simulates with basic logic.
    """
    time.sleep(0.05)

    if amount > 50000:
        return {"success": False, "error": "Amount exceeds gateway limit"}

    return {"success": True, "gateway_ref": f"GW-{transaction_id[:8]}"}


def check_fraud(customer_id, amount, currency, session):
    """Basic fraud screening based on transaction patterns."""
    if amount > FRAUD_THRESHOLD:
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        recent_txns = session.query(Transaction).filter(
            Transaction.customer_id == customer_id,
            Transaction.created_at >= recent_cutoff,
            Transaction.status == PaymentStatus.COMPLETED
        ).all()

        total_recent = sum(t.amount for t in recent_txns)

        if total_recent + amount > FRAUD_THRESHOLD * 3:
            return {"flagged": True, "reason": "Velocity limit exceeded: 24h total exceeds threshold"}

    return {"flagged": False, "reason": None}


def get_transaction(transaction_id):
    """Retrieve a transaction by ID."""
    session = SessionLocal()
    try:
        txn = session.query(Transaction).filter(Transaction.id == transaction_id).first()
        if not txn:
            return None
        return {
            "transaction_id": txn.id,
            "customer_id": txn.customer_id,
            "amount": txn.amount,
            "currency": txn.currency,
            "status": txn.status.value,
            "merchant_id": txn.merchant_id,
            "created_at": txn.created_at.isoformat(),
            "retry_count": txn.retry_count
        }
    finally:
        session.close()


def get_customer_transactions(customer_id, limit=50, offset=0):
    """Get paginated transaction history for a customer."""
    session = SessionLocal()
    try:
        txns = session.query(Transaction).filter(
            Transaction.customer_id == customer_id
        ).order_by(Transaction.created_at.desc()).limit(limit).offset(offset).all()

        total = session.query(Transaction).filter(
            Transaction.customer_id == customer_id
        ).count()

        results = []
        for txn in txns:
            results.append({
                "transaction_id": txn.id,
                "amount": txn.amount,
                "currency": txn.currency,
                "status": txn.status.value,
                "merchant_id": txn.merchant_id,
                "created_at": txn.created_at.isoformat()
            })

        return {
            "transactions": results,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total
        }
    finally:
        session.close()


def refund_payment(transaction_id, reason=None):
    """Process a refund for a completed transaction."""
    session = SessionLocal()
    try:
        txn = session.query(Transaction).filter(Transaction.id == transaction_id).first()
        if not txn:
            raise ValueError(f"Transaction {transaction_id} not found")
        if txn.status != PaymentStatus.COMPLETED:
            raise ValueError(f"Cannot refund transaction in {txn.status.value} status")

        txn.status = PaymentStatus.REFUNDED
        txn.updated_at = datetime.utcnow()
        log_audit(session, transaction_id, "REFUNDED", reason or "Customer requested refund")
        session.commit()

        return {"transaction_id": txn.id, "status": "refunded", "amount": txn.amount}
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()


def get_daily_settlement_report(date=None):
    """Generate a daily settlement report for all completed transactions."""
    if date is None:
        date = datetime.utcnow().date()

    session = SessionLocal()
    try:
        start = datetime.combine(date, datetime.min.time())
        end = start + timedelta(days=1)

        completed = session.query(Transaction).filter(
            Transaction.status == PaymentStatus.COMPLETED,
            Transaction.created_at >= start,
            Transaction.created_at < end
        ).all()

        refunded = session.query(Transaction).filter(
            Transaction.status == PaymentStatus.REFUNDED,
            Transaction.updated_at >= start,
            Transaction.updated_at < end
        ).all()

        total_volume = sum(t.amount for t in completed)
        total_refunds = sum(t.amount for t in refunded)
        net_settlement = total_volume - total_refunds

        merchants = {}
        for txn in completed:
            if txn.merchant_id not in merchants:
                merchants[txn.merchant_id] = {"volume": 0, "count": 0}
            merchants[txn.merchant_id]["volume"] += txn.amount
            merchants[txn.merchant_id]["count"] += 1

        return {
            "date": date.isoformat(),
            "total_transactions": len(completed),
            "total_volume": round(total_volume, 2),
            "total_refunds": round(total_refunds, 2),
            "net_settlement": round(net_settlement, 2),
            "refund_count": len(refunded),
            "merchant_breakdown": merchants
        }
    finally:
        session.close()


def search_transactions(query_text):
    """Search across transactions for the ops dashboard."""
    from sqlalchemy import or_
    session = SessionLocal()
    try:
        pattern = f"%{query_text}%"
        txns = session.query(Transaction).filter(
            or_(
                Transaction.customer_id.ilike(pattern),
                Transaction.description.ilike(pattern)
            )
        ).order_by(Transaction.created_at.desc()).limit(100).all()
        return [
            {"id": t.id, "customer_id": t.customer_id, "amount": t.amount, "status": t.status}
            for t in txns
        ]
    finally:
        session.close()


def log_audit(session, transaction_id, action, details):
    """Write an entry to the audit log."""
    entry = AuditLog(
        transaction_id=transaction_id,
        action=action,
        details=details
    )
    session.add(entry)
