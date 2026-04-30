

<h1 align="center">Viscacha</h1>

<p align="center">
Background jobs for Python. Built for background tasks and AI pipelines. Every job is crash safe, traceable, and retriable.
</p>


```python
from viscacha import Client, Worker

client = Client()
worker = Worker(client)

@worker.job("greet")
def greet(name: str) -> dict:
    return {"message": f"Hello, {name}!"}

worker.run(blocking=False)

handle = client.enqueue("greet", name="Alice")
result = handle.wait()
print(result.result)  # {'message': 'Hello, Alice!'}
```

No broker, Redis, or Docker. Just Python, and simpler than Celery/SQS!

---

## Install

```bash
pip install viscacha 
git clone https://github.com/SkylarM-B/Viscacha/
```

Requires Python 3.10+.

---

## How it works

1. Submit a job
2. A worker function runs it
3. Get the result or inspect what happened

```python
handle = client.enqueue("send_email", to="alice@example.com")

result = handle.wait(timeout=30)  # raises TimeoutError if it doesn't finish
print(result.status)   # 'done' | 'failed' | 'cancelled'
print(result.result)   # return value of the job function
print(result.error)    # set if failed, else None

handle.cancel()        # cancel a pending job

client.jobs()               # list all jobs
client.jobs(status="done")  # filter by status
client.get(handle.id)       # get one by ID
```

---

## Guarantees

- **No lost jobs** — a job stays in the queue until a worker completes it
- **Safe retries** — transient failures retry automatically
- **Full traceability** — every job logged with type, args, result, retries, error
- **Crash-safe** — if a worker dies mid-job, the lease expires and the job returns to the queue
- **Crash safe execution** - Workers acquire jobs through atomic leases. If a worker does die mid-job, the lease expires and the job returns to the queue.
-  **Durable persistence** - The system uses an append only log with periodic snapshotting and compaction to keep state small and recovery fast.

---

## AI pipelines

Each Claude call is a job. Workers run in parallel and failures retry automatically.

```python
import anthropic
from viscacha import Client, Worker

client = Client()
worker = Worker(client)
ai = anthropic.Anthropic()

@worker.job("classify_ticket", max_retries=2)
def classify_ticket(title: str, body: str) -> dict:
    response = ai.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        messages=[{"role": "user", "content": f"Classify: {title}\n{body}"}],
    )
    return {"category": "bug", "priority": "high"}

worker.run(blocking=False)

handles = [client.enqueue("classify_ticket", title=t, body=b) for t, b in tickets]
results = [h.wait(timeout=30) for h in handles]
```

```bash
ANTHROPIC_API_KEY=sk-... python demos/demo_ai_jobs.py
```

---

## Any function works

Email, HTTP calls, reports, transforms, ect. A worker is just a function.

```python
@worker.job("send_email")
def send_email(to: str, subject: str, html: str) -> dict:
    return {"to": to, "sent": True}

client.enqueue("send_email", to="bob@example.com", subject="Order confirmed", html="...")
```

```bash
python demos/demo_email_jobs.py  # dry-run, no SMTP needed
```

---

## Retries and crash recovery

```python
@worker.job("call_api", max_retries=5, lease_ttl=60.0)
def call_api(endpoint: str) -> dict:
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()
    return response.json()
```

`max_retries` — retries on any exception (default 3)  
`lease_ttl` — seconds before a stalled job is reclaimed (default 30)

---

## Persistence

```python
client = Client(log_path="jobs.jsonl")
```

Append-only log. 
Jobs survive restarts.

---

## HTTP API

Expose jobs over HTTP so workers can run anywhere:

```python
from viscacha import Client
from viscacha.server import create_app
import uvicorn

app = create_app(Client(log_path="jobs.jsonl"))
uvicorn.run(app, host="0.0.0.0", port=8000)
```

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_type": "greet", "args": {"name": "Alice"}}'

curl http://localhost:8000/jobs?status=done
```

---

## Under the hood
Jobs are represented as tuples in an append-only tuple space.
Workers claim jobs using atomic leases, ensuring that only one worker can process a job at a time.
If a worker crashes, the lease expires and the job becomes available again.

The log is periodically snapshotted and compacted.
Snapshots capture the current state so the system does not need to replay the entire log on startup.
Compaction removes obsolete events and keeps the log small.

Viscacha is a thin API over these mechanisms.

---

---
## Tuple space?
A Tuple Space is a form of associative memory used for parallel/distributed computing.
It's not a line; it's a shared space. Workers don't just 'take' jobs; they 'lease' them. If the worker crashes, the lease expires, and the job reappears automatically.

## Roadmap

- [ ] Priority queues
- [ ] Job chaining / workflows
- [ ] Web dashboard
- [ ] Scheduled / cron jobs
- [ ] Distributed workers (multi-process, multi-host)
