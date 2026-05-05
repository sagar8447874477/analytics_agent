"""
server.py  —  Flask REST API wrapper for the Analytics Agent (Gemini edition)

Endpoints:
  POST /ask          Body: { "question": "What was DAU last week?" }
  GET  /health       Returns: { "status": "ok" }
  GET  /cache/stats  Returns: hit rate, size, etc.
"""

import sys, os

# Make sure Python can find agent.py which lives in ../agent/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

from flask import Flask, request, jsonify
from flask_cors import CORS
from agent import ask, cache

app = Flask(__name__)
CORS(app)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "model": "gemini-1.5-flash"})


@app.route("/ask", methods=["POST"])
def ask_endpoint():
    body     = request.get_json(force=True, silent=True) or {}
    question = (body.get("question") or "").strip()

    if not question:
        return jsonify({"error": "Request body must contain a non-empty 'question' field."}), 400

    result = ask(question)
    return jsonify(result)


@app.route("/cache/stats", methods=["GET"])
def cache_stats():
    return jsonify(cache.stats())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
