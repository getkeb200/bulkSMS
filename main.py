import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import sql

app = Flask(__name__)

# DB connection from env (Render sets DATABASE_URL)
db_url = os.environ.get('DATABASE_URL')
conn = psycopg2.connect(db_url)
conn.autocommit = True  # For simplicity; use with caution in prod

# Init tables if not exist (run on startup)
def init_db():
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                token VARCHAR PRIMARY KEY,
                is_paid BOOLEAN DEFAULT FALSE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sms_queue (
                id SERIAL PRIMARY KEY,
                receiver VARCHAR NOT NULL,
                msgdata TEXT NOT NULL,
                status VARCHAR DEFAULT 'queued',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

init_db()

# Endpoint: Receive send request
@app.route('/send-sms', methods=['POST'])
def send_sms():
    data = request.json
    token = data.get('api_token')
    to = data.get('to')
    message = data.get('message')
    
    if not all([token, to, message]):
        return jsonify({'error': 'Missing parameters'}), 400
    
    with conn.cursor() as cur:
        cur.execute("SELECT is_paid FROM users WHERE token = %s", (token,))
        user = cur.fetchone()
    
    if not user or not user[0]:
        return jsonify({'error': 'Invalid token or not paid. Buy package at example.com/buy'}), 403
    
    # Insert to queue
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sms_queue (receiver, msgdata) VALUES (%s, %s)",
            (to, message)
        )
    
    return jsonify({'success': 'Message queued'}), 200

# Endpoint: Get next queued message (atomic)
@app.route('/get-next-message', methods=['GET'])
def get_next():
    phone_key = request.headers.get('Phone-Key')  # Simple auth
    if phone_key != 'getero$@508747':  # Change to a strong secret
        return jsonify({'error': 'Unauthorized'}), 401
    
    with conn.cursor() as cur:
        # Atomic: Update and return
        cur.execute("""
            UPDATE sms_queue
            SET status = 'processing'
            WHERE id = (
                SELECT id FROM sms_queue
                WHERE status = 'queued'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, receiver, msgdata;
        """)
        msg = cur.fetchone()
    
    if msg:
        return jsonify({'id': msg[0], 'to': msg[1], 'message': msg[2]})
    return jsonify({'message': 'No queued messages'}), 204

# Endpoint: Update status
@app.route('/update-status', methods=['POST'])
def update_status():
    data = request.json
    msg_id = data.get('id')
    status = data.get('status')  # 'sent' or 'failed'
    
    if not msg_id or status not in ['sent', 'failed']:
        return jsonify({'error': 'Invalid parameters'}), 400
    
    with conn.cursor() as cur:
        if status == 'sent':
            cur.execute(
                "UPDATE sms_queue SET status = 'sent' WHERE id = %s",
                (msg_id,)
            )
        else:
            # Reset for retry (or delete if no retries)
            cur.execute(
                "UPDATE sms_queue SET status = 'queued' WHERE id = %s",
                (msg_id,)
            )
    
    return jsonify({'success': True}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

