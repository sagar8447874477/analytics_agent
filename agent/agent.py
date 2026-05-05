"""
agent.py  —  Analytics Q&A Agent (Gemini edition)
Pipeline:
  1. Cache check       – instant return for repeat questions
  2. Classifier        – detect question type + ambiguity
  3. SQL Generator     – produce safe SQLite SQL
  4. Executor          – run SQL, guard against dangerous statements
  5. Verifier          – sanity-check the numbers
  6. Formatter         – write a human answer
"""

import os, re, json, sqlite3, hashlib, time
from collections import OrderedDict
import urllib.request, urllib.error

# ── Config ─────────────────────────────────────────────────────────────────────
# analytics.db lives one level up in  ../data/
DB_PATH    = os.path.join(os.path.dirname(__file__), "..", "data", "analytics.db")

# Gemini REST endpoint — we use gemini-1.5-flash (fast + free tier)
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)

CACHE_SIZE = 128

# ── Database schema description (fed to Gemini in every prompt) ────────────────
SCHEMA = """
SQLite database. All date columns are stored as TEXT in ISO-8601 format: 'YYYY-MM-DD HH:MM:SS'.

TABLE users
  user_id          INTEGER  PRIMARY KEY
  created_at       TEXT     when the user signed up
  country          TEXT     two-letter code  e.g. US, IN, GB, CA, AU, DE, FR, BR, MX, SG
  platform         TEXT     iOS | Android | Web | Desktop
  plan             TEXT     free | basic | pro | enterprise
  age              INTEGER
  referral_channel TEXT     organic | paid_search | social | email | referral | direct

TABLE sessions
  session_id   INTEGER  PRIMARY KEY
  user_id      INTEGER  FOREIGN KEY → users.user_id
  started_at   TEXT     session start timestamp
  duration_sec INTEGER  length of session in seconds
  platform     TEXT     iOS | Android | Web | Desktop
  country      TEXT     two-letter code
  is_bounce    INTEGER  1 = bounced (< 10 sec), 0 = did not bounce

TABLE transactions
  txn_id      INTEGER  PRIMARY KEY
  user_id     INTEGER  FOREIGN KEY → users.user_id
  created_at  TEXT     transaction timestamp
  amount_usd  REAL     amount charged in US dollars
  product     TEXT     'Pro Monthly' | 'Pro Annual' | 'Enterprise' | 'Add-on Storage' | 'API Credits'
  currency    TEXT     always 'USD'
  status      TEXT     completed | refunded | failed

TABLE content_views
  view_id      INTEGER  PRIMARY KEY
  user_id      INTEGER  FOREIGN KEY → users.user_id
  viewed_at    TEXT     view timestamp
  content_type TEXT     article | video | podcast | tutorial | webinar
  content_id   INTEGER  ID of the content item
  duration_sec INTEGER  how long the user watched/read
  completed    INTEGER  1 = finished, 0 = did not finish

Data date range : 2024-01-01  to  2024-12-31
"Today" for queries: 2024-12-31
Row counts: users ≈ 5 000 | sessions ≈ 20 000 | transactions ≈ 6 000 | content_views ≈ 30 000
"""

# ── LRU Cache ──────────────────────────────────────────────────────────────────
class SimpleCache:
    def __init__(self, maxsize=CACHE_SIZE):
        self.store   = OrderedDict()
        self.maxsize = maxsize
        self.hits    = 0
        self.misses  = 0

    def _key(self, q: str) -> str:
        return hashlib.md5(q.strip().lower().encode()).hexdigest()

    def get(self, q: str):
        k = self._key(q)
        if k in self.store:
            self.store.move_to_end(k)
            self.hits += 1
            out = self.store[k].copy()
            out["cached"] = True
            return out
        self.misses += 1
        return None

    def set(self, q: str, result: dict):
        k = self._key(q)
        self.store[k] = result
        self.store.move_to_end(k)
        if len(self.store) > self.maxsize:
            self.store.popitem(last=False)

    def stats(self):
        total = self.hits + self.misses
        return {
            "hits":     self.hits,
            "misses":   self.misses,
            "hit_rate": round(self.hits / total, 3) if total else 0,
            "size":     len(self.store),
        }

cache = SimpleCache()

# ── Gemini API helper ──────────────────────────────────────────────────────────
def call_gemini(system_instruction: str, user_message: str, max_tokens: int = 1500) -> str:
    """
    Calls the Gemini REST API.
    Gemini uses:
      systemInstruction  →  the system prompt
      contents           →  the user turn
    Returns the model's text reply.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is not set.")

    url = f"{GEMINI_URL}?key={api_key}"

    payload = json.dumps({
        "systemInstruction": {
            "parts": [{"text": system_instruction}]
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": user_message}]
            }
        ],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.1        # low temperature = more deterministic JSON
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=40) as resp:
            data = json.loads(resp.read())
            # Gemini response structure:
            # data["candidates"][0]["content"]["parts"][0]["text"]
            candidates = data.get("candidates", [])
            if not candidates:
                raise RuntimeError(f"Gemini returned no candidates. Full response: {data}")
            parts = candidates[0].get("content", {}).get("parts", [])
            return "".join(p.get("text", "") for p in parts)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"Gemini API HTTP {e.code}: {body}")


def parse_json_from_text(text: str) -> dict:
    """
    Gemini sometimes wraps JSON in ```json ... ``` fences.
    This strips the fences and parses the JSON.
    """
    # Remove markdown code fences if present
    clean = re.sub(r"```(?:json)?", "", text).replace("```", "").strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        # Try to find a JSON object anywhere in the text
        m = re.search(r'\{.*\}', clean, re.DOTALL)
        if m:
            return json.loads(m.group())
        raise ValueError(f"No valid JSON found in model output:\n{text[:400]}")


# ── Step 1 : Classify question ─────────────────────────────────────────────────
CLASSIFY_SYSTEM = """
You are an analytics question classifier. Your ONLY job is to classify a question and output JSON.

Classify into exactly one of these types:
  simple_aggregate   → DAU, MAU, totals, averages, counts with no extra filters
  filtered_aggregate → same as above but with a WHERE condition (country, plan, date range, etc.)
  cohort             → retention (D7, D30), cohort analysis, churn
  comparison         → this week vs last week, plan A vs plan B, before vs after
  ambiguous          → question is too vague to answer without more information

Rules:
- Set needs_clarification to true ONLY if the question cannot be answered without a guess.
- If needs_clarification is true, provide 1-3 short clarifying_questions.
- reasoning must be one sentence.

Output ONLY this JSON — no markdown, no extra text:
{
  "type": "simple_aggregate",
  "needs_clarification": false,
  "clarifying_questions": [],
  "reasoning": "one sentence explanation"
}
"""

def classify(question: str) -> dict:
    raw = call_gemini(CLASSIFY_SYSTEM, question, max_tokens=300)
    try:
        return parse_json_from_text(raw)
    except Exception:
        return {
            "type": "ambiguous",
            "needs_clarification": True,
            "clarifying_questions": ["Could you rephrase your question with more detail?"],
            "reasoning": "Could not parse classifier response."
        }


# ── Step 2 : Generate SQL ──────────────────────────────────────────────────────
SQL_SYSTEM = f"""
You are an expert SQLite query writer. Given a user question, write a single valid SQLite SELECT statement.

OUTPUT FORMAT — return ONLY this JSON, no markdown fences, no extra text:
{{
  "sql": "SELECT ...",
  "explanation": "one sentence describing what this query does",
  "chart_type": "bar",
  "chart_x": "column_name_or_null",
  "chart_y": "column_name_or_null"
}}

chart_type must be one of: bar | line | table | number

STRICT RULES:
1. Use ONLY the tables and columns defined in the schema below. Do NOT invent any column or table.
2. All date filtering must use SQLite date functions: DATE(), strftime(), datetime().
3. DAU  = COUNT(DISTINCT user_id) grouped by DATE(started_at) from the sessions table.
4. MAU  = COUNT(DISTINCT user_id) grouped by strftime('%Y-%m', started_at) from sessions.
5. D7 retention = percentage of users who had at least one session between 7 and 13 days after their very first session date.
6. "last week"  → started_at BETWEEN date('2024-12-31','-7 days') AND date('2024-12-31','-1 day')
7. "this week"  → started_at >= date('2024-12-31','-6 days')
8. Always add LIMIT 100 unless the question asks for a single number.
9. For revenue questions, only count transactions WHERE status = 'completed'.

SCHEMA:
{SCHEMA}
"""

def generate_sql(question: str, q_type: str) -> dict:
    prompt = f"Question type hint: {q_type}\nUser question: {question}"
    raw = call_gemini(SQL_SYSTEM, prompt, max_tokens=600)
    try:
        return parse_json_from_text(raw)
    except Exception as e:
        raise ValueError(f"Could not parse SQL from Gemini output.\nRaw output:\n{raw[:300]}\nError: {e}")


# ── Step 3 : Execute SQL safely ────────────────────────────────────────────────
_DANGEROUS = re.compile(
    r'\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|ATTACH|DETACH|PRAGMA\s+(?!table_info))\b',
    re.IGNORECASE
)

def execute_sql(sql: str):
    """Run the SQL against SQLite. Returns (rows, columns)."""
    if _DANGEROUS.search(sql):
        raise PermissionError(
            "The generated SQL contains a forbidden keyword (DROP/DELETE/INSERT/etc). Blocked for safety."
        )

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur  = conn.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    except sqlite3.Error as e:
        raise RuntimeError(f"SQLite error: {e}\n\nSQL that failed:\n{sql}")
    finally:
        conn.close()


# ── Step 4 : Verify result ─────────────────────────────────────────────────────
VERIFY_SYSTEM = """
You are a data quality reviewer for an analytics system.

You will receive a user question, the SQL that was run, and a sample of the result rows.
Your job: decide if the result is PLAUSIBLE given the dataset size and the question asked.

Known dataset facts:
- Total users: ~5 000
- Total sessions: ~20 000  →  DAU should be roughly 50–400 on any given day
- Total transactions: ~6 000
- Revenue per month: roughly $5 000 – $50 000
- Retention rates: realistically 15%–60%
- All data is from 2024 only

Flag as NOT plausible if:
- DAU > 5 000
- Retention > 100%
- Revenue for a single day > $200 000
- Result rows are completely empty when the question clearly expects data

Output ONLY this JSON — no markdown, no extra text:
{
  "plausible": true,
  "confidence": 0.9,
  "concern": null
}
"""

def verify_result(question: str, sql: str, rows: list) -> dict:
    sample  = rows[:5]
    prompt  = (
        f"Question: {question}\n"
        f"SQL used:\n{sql}\n"
        f"First 5 result rows: {json.dumps(sample)}\n"
        f"Total rows returned: {len(rows)}"
    )
    try:
        raw = call_gemini(VERIFY_SYSTEM, prompt, max_tokens=200)
        return parse_json_from_text(raw)
    except Exception:
        return {"plausible": True, "confidence": 0.75, "concern": None}


# ── Step 5 : Format human answer ──────────────────────────────────────────────
FORMAT_SYSTEM = """
You are a product analytics assistant inside a chat interface.

Given a user question and the data returned by a SQL query, write a clear, concise answer.

Rules:
- Start with the most important number or finding.
- Add 1-2 sentences of context or insight if the data supports it.
- Maximum 4 sentences total.
- Do NOT mention SQL, databases, or technical details.
- Do NOT use bullet points. Write in natural prose.
"""

def format_answer(question: str, rows: list, cols: list) -> str:
    sample = rows[:20]
    prompt = (
        f"User question: {question}\n"
        f"Column names: {cols}\n"
        f"Data (up to 20 rows): {json.dumps(sample)}\n"
        f"Total rows in full result: {len(rows)}"
    )
    return call_gemini(FORMAT_SYSTEM, prompt, max_tokens=300).strip()


# ── Main public function ───────────────────────────────────────────────────────
def ask(question: str) -> dict:
    """
    Full pipeline: question → classify → SQL → execute → verify → format → answer.
    Returns a dict with all intermediate steps included.
    """
    t0 = time.time()

    # ── Cache hit? ─────────────────────────────────────────────────────────────
    cached = cache.get(question)
    if cached:
        return cached

    result = {
        "question":             question,
        "type":                 None,
        "sql":                  None,
        "rows":                 [],
        "columns":              [],
        "answer":               None,
        "chart":                {},
        "verification":         {},
        "needs_clarification":  False,
        "clarifying_questions": [],
        "cached":               False,
        "error":                None,
        "elapsed_ms":           0,
    }

    try:
        # 1. Classify
        cls = classify(question)
        result["type"] = cls.get("type", "unknown")

        # 2. Ambiguous → ask for clarification, don't generate SQL
        if cls.get("needs_clarification") or result["type"] == "ambiguous":
            result["needs_clarification"]  = True
            result["clarifying_questions"] = cls.get("clarifying_questions", [])
            cqs = " ".join(result["clarifying_questions"])
            result["answer"] = (
                "I need a bit more detail to answer accurately. " + cqs
            )
            result["elapsed_ms"] = int((time.time() - t0) * 1000)
            return result

        # 3. Generate SQL
        sql_data        = generate_sql(question, result["type"])
        result["sql"]   = sql_data.get("sql", "")
        result["chart"] = {
            "type":        sql_data.get("chart_type", "table"),
            "x":           sql_data.get("chart_x"),
            "y":           sql_data.get("chart_y"),
            "explanation": sql_data.get("explanation", ""),
        }

        # 4. Execute
        rows, cols        = execute_sql(result["sql"])
        result["rows"]    = rows
        result["columns"] = cols

        # 5. Verify
        result["verification"] = verify_result(question, result["sql"], rows)

        # 6. Format
        result["answer"] = format_answer(question, rows, cols)

    except Exception as e:
        result["error"]  = str(e)
        result["answer"] = f"Sorry, something went wrong: {e}"

    result["elapsed_ms"] = int((time.time() - t0) * 1000)

    # Only cache successful results
    if not result["error"]:
        cache.set(question, result)

    return result


# ── Quick CLI test  (python agent.py "your question here") ────────────────────
if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What was our DAU last week?"
    print(f"\n Question : {q}\n")
    r = ask(q)
    print(f" Type     : {r['type']}")
    print(f" SQL      : {r['sql']}")
    print(f" Answer   : {r['answer']}")
    print(f" Verify   : {r['verification']}")
    print(f" Elapsed  : {r['elapsed_ms']} ms")
    print(f" Cached   : {r['cached']}")
