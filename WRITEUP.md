# Analytics Q&A Agent — Architecture & Write-up

## What was built

A natural-language analytics assistant that takes questions like  
*"What was our DAU last week?"* and returns a direct answer backed by  
real SQL execution against a synthetic SQLite database.

---

## 1. Architecture

### High-level flow

```
User question
     │
     ▼
┌──────────────┐
│  Cache Check │  ── hit ──► return cached result (instant)
└──────┬───────┘
       │ miss
       ▼
┌──────────────┐
│  Classifier  │  Claude call #1 → question type + ambiguity check
└──────┬───────┘
       │ ambiguous? → return clarifying questions (no SQL)
       │
       ▼
┌──────────────┐
│ SQL Generator│  Claude call #2 → SQL + chart hints
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Executor   │  Run SQL against SQLite (with injection guard)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Verifier   │  Claude call #3 → plausibility check on result
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Formatter  │  Claude call #4 → natural language answer
└──────┬───────┘
       │
       ▼
  Structured response
  { answer, sql, rows, chart, verification, type, cached, elapsed_ms }
```

### Why this architecture?

**Separation of concerns.** Each step has a single job and a single  
Claude call with a tightly scoped system prompt. This makes debugging  
easy — if SQL is wrong, you look at step 2 only.

**Classifier first.** Before generating SQL I classify the question type  
(simple_aggregate, filtered_aggregate, cohort, comparison, ambiguous).  
Two benefits: (a) the SQL generation prompt can be specialised per type,  
(b) ambiguous questions are caught before any SQL is generated, avoiding  
hallucinated guesses.

**Verifier step.** A separate Claude call reviews the result for  
plausibility (e.g. retention > 100%, DAU > total users). This catches  
correct SQL that returns nonsensical numbers due to data issues.

**Structured JSON contracts between steps.** Every Claude call returns  
JSON with a fixed schema. This makes parsing deterministic and allows  
retries on parse failure.

---

## 2. Handling wrong / hallucinated SQL

Several defensive layers:

### Layer 1 — Schema injection
The full schema (column names, types, valid enum values, date format) is  
embedded in every SQL generation prompt. Claude cannot invent columns  
that don't exist because the prompt makes clear what exists.

### Layer 2 — SQL safety guard
A regex rejects any statement containing DROP, DELETE, INSERT, UPDATE,  
ALTER, CREATE, ATTACH, or dangerous PRAGMA before execution. Read-only  
SQLite flag is also set at the connection level.

### Layer 3 — SQLite error handling
If the generated SQL is syntactically valid but semantically wrong  
(references a non-existent column or table), SQLite raises an error.  
This is caught, and the error message is returned to the user rather  
than silently failing.

### Layer 4 — Verifier
After execution, a separate Claude call checks whether the numbers are  
plausible given what we know about the dataset. Examples of what it  
flags:
- DAU of 50,000 when total users = 5,000
- D7 retention of 120%
- Revenue of $0 when status filter was not applied

If the verifier flags an issue, the response includes a warning badge.

### Layer 5 — SQL transparency
The generated SQL is always returned in the response and shown to the  
user (via a "show SQL" toggle in the UI). Power users can audit it.

### Layer 6 — Retry logic (production extension)
In the server implementation, a failed parse or SQL error can trigger  
a retry with the error appended to the prompt: "The previous SQL failed  
with: [error]. Fix it." This self-corrects most generation mistakes.

---

## 3. How I'd evaluate this

### Offline evaluation (golden set)

Build a test set of ~50–100 question → SQL → expected_result triples:

```
{
  "question": "What was DAU on 2024-03-15?",
  "expected_sql_keywords": ["COUNT(DISTINCT", "user_id", "started_at"],
  "expected_answer_contains": ["daily active users", "March 15"],
  "expected_row_count": 1,
  "question_type": "simple_aggregate"
}
```

Metrics:
- **SQL execution success rate** — did the SQL run without error?
- **Result correctness** — does the answer match a hand-verified ground truth?
- **Type classification accuracy** — did the classifier pick the right type?
- **Ambiguity precision** — did it ask for clarification only when needed?
- **Latency** — p50/p95 end-to-end response time

### Online evaluation (once deployed)

- **Thumbs up/down** on each response
- **Clarification rate** — what % of questions triggered clarification?  
  (target: <10% on well-scoped questions)
- **Cache hit rate** — useful signal for question diversity
- **Error rate** by question type

### Regression suite

Every merged change runs the golden set. A drop in SQL success rate  
> 5% blocks the deploy.

---

## 4. Scaling to a real warehouse with 100+ tables

### The core problem

At 100+ tables the schema is too large to fit in a prompt (and  
overwhelming even if it did fit). You need semantic retrieval to find  
the right tables.

### Proposed architecture at scale

```
User question
     │
     ▼
┌──────────────────────┐
│  Semantic table      │  Embed question → cosine search over table
│  retrieval           │  + column embeddings stored in a vector DB
└──────────┬───────────┘
           │ top-k tables (e.g. k=5)
           ▼
┌──────────────────────┐
│  Schema hydration    │  Fetch DDL + sample rows for retrieved tables
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  SQL generation      │  Same as current, but with minimal schema
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Query optimizer     │  Push to warehouse (BigQuery / Snowflake /
│                      │  Redshift) with cost guards (row limit,
│                      │  scan byte limit, time limit)
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Result cache        │  Redis with TTL keyed by (question_hash,
│                      │  table_versions). Invalidated on ETL run.
└──────────────────────┘
```

### Key additions for scale

**1. Table + column embeddings**  
Embed each table's description, column names, sample values. On a new  
question, embed the question and retrieve top-k relevant tables. Only  
the retrieved DDL goes into the prompt.

**2. Metadata layer**  
A catalog (e.g. dbt docs, Datahub) provides human-written descriptions  
for every table and column. These descriptions dramatically improve  
retrieval accuracy over raw column names.

**3. Cost guards on warehouse queries**  
Always preview query cost (EXPLAIN / dry run) before execution. Reject  
queries that would scan > N GB or return > M rows.

**4. Query result caching in Redis**  
Cache results by (question_embedding_cluster, date_partition). Invalidate  
on new ETL loads. Repeat questions (e.g. daily DAU) are served instantly.

**5. Feedback loop**  
Every thumbs-down is reviewed. Correct SQL is added to the golden set  
and used as few-shot examples in the prompt for similar question types.

**6. Access control**  
Map user roles to table allowlists. The SQL generator only receives  
tables the user is allowed to query.

---

## 5. Dataset

Synthetic SQLite DB generated by `data/generate_data.py`.

| Table          | Rows   | Key columns                                      |
|----------------|--------|--------------------------------------------------|
| users          | 5,000  | user_id, created_at, country, plan, platform     |
| sessions       | 20,000 | user_id, started_at, duration_sec, is_bounce     |
| transactions   | 6,000  | user_id, created_at, amount_usd, status, product |
| content_views  | 30,000 | user_id, viewed_at, content_type, completed      |

Data covers 2024-01-01 to 2024-12-31. Country distribution is US-heavy  
(30%) with India (18%), GB (10%) and 7 others. Plan distribution is  
free-heavy (55%) with paying tiers making up 45%.

---

## 6. Question types handled

| Type                | Example                                              |
|---------------------|------------------------------------------------------|
| simple_aggregate    | "What was our DAU last week?"                       |
| filtered_aggregate  | "How much revenue from US users in November?"       |
| cohort              | "Which cohort has the best D7 retention?"           |
| comparison          | "Compare sessions this week vs last week"           |
| ambiguous           | "Who are the top users?" → asks clarifying question |

---

## 7. Bonus features implemented

- **Caching layer** — LRU cache (128 entries) with MD5 key on lowercased  
  question. Cache hits return instantly with a "cached" badge.
- **Result verification** — separate Claude call checks plausibility of  
  numbers before returning to the user.
- **Chart rendering** — bar and line charts via Chart.js rendered inline  
  in the chat UI based on chart_type/chart_x/chart_y hints from the  
  SQL generation step.
- **SQL transparency** — collapsible "show SQL" toggle on every response.

---

## 8. Files delivered

```
analytics-agent/
├── data/
│   ├── generate_data.py      # synthetic DB generation script
│   └── analytics.db          # generated SQLite database
├── agent/
│   └── agent.py              # core agent (classify → SQL → exec → verify → format)
└── api/
    └── server.py             # Flask REST wrapper (POST /ask, GET /cache/stats)
```

The interactive chat UI runs entirely in the browser and calls the  
Anthropic API directly, making it zero-infrastructure to demo.
