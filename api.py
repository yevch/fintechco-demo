"""
FinTechCo Payment API

REST API for the payment processing system. Handles payment creation,
retrieval, refunds, and settlement reports.
"""
from flask import Flask, request, jsonify
from payment_processor import (
    process_payment,
    get_transaction,
    get_customer_transactions,
    refund_payment,
    get_daily_settlement_report,
)
from concurrent_handler import process_batch_payments
from config import RATE_LIMIT_PER_MINUTE
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route("/api/v1/payments", methods=["POST"])
def create_payment():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body required"}), 400

    required = ["customer_id", "amount", "merchant_id"]
    missing = [f for f in required if f not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    try:
        result = process_payment(
            customer_id=data["customer_id"],
            amount=float(data["amount"]),
            currency=data.get("currency", "USD"),
            merchant_id=data["merchant_id"],
            description=data.get("description"),
            idempotency_key=data.get("idempotency_key"),
        )
        return jsonify(result), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.error(f"Payment failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/api/v1/payments/batch", methods=["POST"])
def create_batch_payments():
    """Process multiple payments concurrently for high-throughput scenarios."""
    data = request.get_json()
    if not data or "payments" not in data:
        return jsonify({"error": "payments array required"}), 400

    try:
        results = process_batch_payments(data["payments"])
        return jsonify({"results": results}), 201
    except Exception as e:
        logging.error(f"Batch payment failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/api/v1/payments/<transaction_id>", methods=["GET"])
def get_payment(transaction_id):
    txn = get_transaction(transaction_id)
    if not txn:
        return jsonify({"error": "Transaction not found"}), 404
    return jsonify(txn)


@app.route("/api/v1/customers/<customer_id>/transactions", methods=["GET"])
def list_customer_transactions(customer_id):
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    result = get_customer_transactions(customer_id, limit=limit, offset=offset)
    return jsonify(result)


@app.route("/api/v1/payments/<transaction_id>/refund", methods=["POST"])
def refund(transaction_id):
    data = request.get_json() or {}
    try:
        result = refund_payment(transaction_id, reason=data.get("reason"))
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/v1/reports/settlement", methods=["GET"])
def settlement_report():
    from datetime import datetime

    date_str = request.args.get("date")
    if date_str:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    else:
        date = None

    report = get_daily_settlement_report(date)
    return jsonify(report)


@app.route("/api/v1/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "service": "fintechco-payments"})


if __name__ == "__main__":
    app.run(debug=True, port=8080)
