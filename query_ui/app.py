import os
from flask import Flask, render_template, request, jsonify
from trino.dbapi import connect
from trino.auth import BasicAuthentication

app = Flask(__name__)

def get_trino_conn():
    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "admin")
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("SCHEMA", "default")
    
    return connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="http"
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute', methods=['POST'])
def execute():
    query = request.json.get('query')
    if not query:
        return jsonify({"error": "No query provided"}), 400
    
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute(query)
        
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        
        return jsonify({
            "columns": columns,
            "rows": rows
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
