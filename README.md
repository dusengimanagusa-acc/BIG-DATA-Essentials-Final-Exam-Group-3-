# BIG-DATA-Essentials-Final-Exam-Group-3-
This is the final Exam Work of Group 3
# ☕ Urban Brew — Big Data Pipeline

**Big Data Essentials — Group Final Exam Project**

Cross-platform build: same commands, same file layout, on **Windows**
and **Linux/macOS** — every step below shows both.

If you just want a **single ordered checklist to type through from a
blank machine to a working demo**, use **`STEP_BY_STEP.md`** instead —
it's every command in this file, in strict order, including starting
Kafka/Zookeeper/HDFS/MySQL themselves. This file explains the *why*
behind the architecture; `STEP_BY_STEP.md` is the *what to type*.

If you're looking for what to actually *say* to examiners for each rubric
line item, see **`PRESENTATION_TALKING_POINTS.md`**.

**One-sentence project summary:**
> Our Kafka stream represents live orders from Urban Brew's 3 café
> branches; our MLlib model predicts which orders are likely to need
> staff review (large, late-night, cash orders) and recommends a
> concrete next action for each one; our secured dashboard supports
> faster end-of-day reconciliation by showing managers exactly which
> orders to double-check, and what to do about them.

---

## 1. Why this case study (and why it changed from an earlier draft)

An earlier version of this project forecast each branch's *daily*
revenue. That worked, but it needed several distinct **calendar days**
of history per branch before the forecast meant anything — awkward for
a one-sitting demo. This version keeps the same familiar shape (a
multi-branch retail chain, orders, items) but predicts something at the
**order level** instead: is this order likely to get flagged for staff
review? That needs no waiting — a few hundred simulated orders from a
single run of the autoscript is already enough to train, evaluate, and
predict, which is the whole point of the change.

---

## 2. Architecture (what we actually built)

```
                                    ┌──► consumer/mysql_writer.py ──► MySQL (api_order)
                                    │    "custom Python consumer,
                                    │     named group, manual offsets"
AutoScript ──► Django REST API ──► Kafka topic
(rate-tunable,   (validates,        "urbanbrew_orders"
 simulates a     publishes,         (3 partitions,     │
 full day of     keyed by branch)    keyed by branch)  ├──► consumer/hdfs_writer.py ──► HDFS (dt=-partitioned)
 order times)                                          │    "custom Python consumer,
                                                        │     named group, manual offsets"
                                                        │                                             │
                                                        │                                             ▼
                                                        │                              spark_jobs/analytics_job.py ──► MySQL (branch_summary, item_summary,
                                                        │                              (PySpark: 3 insights + MLlib    hourly_summary, model_metrics,
                                                        │                               risk classifier + accuracy     order_risk_predictions)
                                                        │                               metrics + prediction + a
                                                        │                               derived recommendation)
                                                        │                                             │
                                                        │                                             │
                                                        └──► consumer/notifier.py                     │
                                                             "custom Python consumer,                 │
                                                              named group, manual                     │
                                                              offsets, rebalance-                     │
                                                              able" → business-rule                   │
                                                              alerts (LiveAlert)                      │
                                                                  │                                   │
                                                                  ▼                                   ▼
                                                             Django Dashboard (/dashboard/, login required)
                                                             — operational view + Live Alerts
                                                               + Spark/MLlib analytics + Risk Watch
                                                               (prediction + recommended action)
```

**Three independent, named, rebalance-capable custom Python Kafka
consumers read the same topic with zero coordination between them** —
this is the project's default pipeline (guidelines Section 3.2 + rubric
Component 2, "a custom Python consumer application that is part of a
named consumer group"):

- **`consumer/mysql_writer.py`** — inserts each order into MySQL's
  `api_order` table via the Django ORM. Named group
  `urbanbrew-mysql-writer`, manual at-least-once offset commits.
- **`consumer/hdfs_writer.py`** — writes one small JSON file per order
  into a `dt=YYYY-MM-DD/` folder in HDFS via WebHDFS. Named group
  `urbanbrew-hdfs-writer`, same delivery guarantee.
- **`consumer/notifier.py`** — applies our own simple business rule per
  order and pushes the result straight to the dashboard the instant it
  fires (the diagram's "Custom business logic/Notifications" box) — the
  one consumer that does something a generic sink tool structurally
  can't. Named group `urbanbrew-notifier`, demoable with multiple
  instances for a live rebalance.

All three share the same connection/rebalance-listener code
(`consumer/kafka_utils.py`), so the pattern is written once and reused,
not copy-pasted three times.

**A real Kafka Connect setup — a JDBC Sink connector into MySQL and an
HDFS3 Sink connector into HDFS — is ALSO fully built and documented in
`kafka_connect/`, as an optional, already-working alternative to
`mysql_writer.py`/`hdfs_writer.py`.** Section 3.2 names Kafka Connect
explicitly ("Kafka Connect configured to sink data automatically into
downstream storage"), so it's worth having working and being able to
talk about confidently — but it is not what the default demo runs, since
the two Python consumers above do the identical job (automatic,
code-driven, zero manual copy steps) with fewer moving parts to keep
reliable on exam day. See `kafka_connect/README.md` for the full setup
and how to demo it instead if you'd rather. Full reasoning, with exact
rubric line mapping, is in `PRESENTATION_TALKING_POINTS.md`.

---

## 3. Project structure

```
urbanbrew-pipeline/
├── README.md                        ← you are here (the "why")
├── STEP_BY_STEP.md                  ← the "what to type", in strict order
├── PRESENTATION_TALKING_POINTS.md   ← rubric-mapped demo cheat-sheet
├── requirements.txt
├── .env.example                     ← copy to .env and fill in your values
├── .gitignore
└── urbanbrew/                       ← run every command from inside HERE
    ├── manage.py
    ├── config.py                    ← shared .env loader used by every script
    ├── urbanbrew/                   ← Django project settings
    ├── api/                         ← REST endpoint + Kafka producer + Order model
    ├── dashboard/                   ← analytics dashboard (reads Spark output)
    │   ├── static/dashboard/         ← self-hosted assets (no CDN dependency):
    │   │   ├── chart.umd.min.js          Chart.js
    │   │   └── vendor/
    │   │       ├── fontawesome/          Font Awesome (icons)
    │   │       └── poppins/              Poppins (font)
    │   └── templates/
    │       ├── dashboard/dashboard.html  ← the dashboard itself (login required)
    │       └── registration/login.html   ← sign-in page (see "Authentication")
    ├── autoscript/simulate.py       ← rapid data generator (rate-tunable, token-authenticated)
    ├── kafka_connect/                ← OPTIONAL alternative to mysql_writer.py/hdfs_writer.py - see kafka_connect/README.md
    │   ├── connect-standalone.properties
    │   ├── mysql-sink.properties     ← JDBC Sink: Kafka → MySQL (api_order)
    │   ├── hdfs-sink.properties      ← HDFS3 Sink: Kafka → HDFS (dt=-partitioned)
    │   └── plugins/                  ← downloaded connector JARs go here (not in Git)
    ├── consumer/kafka_utils.py      ← shared consumer setup
    ├── consumer/notifier.py         ← custom Kafka consumer → Live Alerts (dashboard)
    ├── consumer/mysql_writer.py     ← custom Kafka consumer (PRIMARY MySQL sink) → api_order
    ├── consumer/hdfs_writer.py      ← custom Kafka consumer (PRIMARY HDFS sink) → HDFS
    ├── spark_jobs/analytics_job.py  ← PySpark + MLlib risk classifier (prediction + recommendation)
    ├── mapreduce/                    ← BONUS: Hadoop Streaming job + written comparison
    │   ├── order_count_mapper.py
    │   ├── order_count_reducer.py
    │   └── COMPARISON.md             ← required write-up for the bonus (Section 3.6)
    └── scripts/                     ← small helper scripts (topic creation, optional Kafka Connect, etc.)
```

---

## 4. Prerequisites

Install these once per machine (group members can split this up).

| Tool | Windows | Linux/macOS |
|---|---|---|
| Python 3.10+ | python.org installer | `apt install python3` / already on macOS |
| Java 11 or 17 (needed by Kafka, Hadoop, Spark) | Adoptium MSI installer | `apt install openjdk-17-jdk` |
| Apache Kafka | download the `.tgz`, unzip anywhere, e.g. `C:\bigdata\kafka` | unzip to e.g. `/opt/bigdata/kafka` |
| Apache Hadoop (HDFS) | needs `winutils.exe` — see Troubleshooting | unzip to e.g. `/opt/bigdata/hadoop` |
| Apache Spark | unzip to e.g. `C:\bigdata\spark` | unzip to e.g. `/opt/bigdata/spark` |
| MySQL Server 8.x | MySQL installer | `apt install mysql-server` |

No Kafka Connect / connector JARs to download for the default pipeline —
every consumer it runs is plain Python (`pip install -r requirements.txt`
is enough). Kafka Connect is fully built too, but it's an optional
alternative path (`kafka_connect/`), not required to get running.

Kafka, Hadoop and Spark all ship **both** a `.sh` launcher (Linux/macOS)
and a `.bat`/`.cmd` launcher (Windows) inside the same download — every
command below shows both.

---

## 5. One-time setup

### 5.1 Python virtual environment + dependencies

```bash
# from the urbanbrew-pipeline/ folder (the one with requirements.txt)

# Windows
py -m venv .venv
.venv\Scripts\activate
py -m pip install -r requirements.txt

# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Activate this same virtual environment in **every** new terminal you
open for this project.

### 5.2 Configure your secrets/paths

```bash
# Windows
copy .env.example urbanbrew\.env

# Linux/macOS
cp .env.example urbanbrew/.env
```

Open `urbanbrew/.env` and fill in your real MySQL password.

### 5.3 Create the MySQL database + user

```sql
-- Linux/macOS: open with `sudo mysql` (socket auth, no password over TCP)
-- Windows: open with `mysql -u root -p` and your root password

CREATE DATABASE urbanbrew_db;
CREATE USER 'urbanbrew_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON urbanbrew_db.* TO 'urbanbrew_user'@'localhost';
FLUSH PRIVILEGES;
```

Use the SAME password in `urbanbrew/.env`'s `MYSQL_PASSWORD`.

### 5.4 Create the Django tables

```bash
cd urbanbrew

# Windows
py manage.py makemigrations
py manage.py migrate
py manage.py createsuperuser

# Linux/macOS
python3 manage.py makemigrations
python3 manage.py migrate
python3 manage.py createsuperuser
```

No further SQL tweak is needed — `consumer/mysql_writer.py` supplies
every column (including `created_at`) itself on every insert, via the
Django ORM, so there's nothing extra for MySQL to default.

The `createsuperuser` you just ran above is **also your dashboard
login** now (see 5.4.1 right below) — same username/password, one
account, nothing extra to create for that part.

### 5.4.1 Authentication (dashboard login + API token)

Two separate mechanisms protect two separate things — see the comment
block above `LOGIN_URL` in `urbanbrew/settings.py` for the "why":

- **The dashboard** (`/dashboard/...`) now requires a normal Django
  session login. Visiting it while logged out redirects you to a
  sign-in page; the superuser you created in 5.4 is your login.
- **The order-ingestion API** (`POST /api/orders/`, used by
  `autoscript/simulate.py`) now requires a DRF auth **token** sent as
  an `Authorization: Token <...>` header — a script has no browser
  session, so it authenticates with one fixed secret instead.

Create a separate account for the autoscript (don't reuse your own
superuser for this — keeping a machine's identity separate from a
person's is good practice and makes it obvious in the admin who/what
is doing what), then generate its token:

```bash
# Windows
py manage.py createsuperuser --username autoscript_service
py manage.py drf_create_token autoscript_service

# Linux/macOS
python3 manage.py createsuperuser --username autoscript_service
python3 manage.py drf_create_token autoscript_service
```

(`createsuperuser` here is just the easiest built-in way to create
*any* user from the command line — the autoscript service account
doesn't need admin rights, it just needs to exist so it can hold a
token. Feel free to answer the email/password prompts with anything;
this account never logs into the dashboard.)

The second command prints a line like:

```
Generated token 9f2c...a1b4 for user autoscript_service
```

Copy that token into `urbanbrew/.env`:

```
AUTOSCRIPT_API_TOKEN=9f2c...a1b4
```

`autoscript/simulate.py` reads it from there automatically and sends
it on every request — nothing else to configure. If you forget this
step, `simulate.py` fails fast at startup with a clear
"Missing required setting 'AUTOSCRIPT_API_TOKEN'" error instead of a
confusing wall of 401s once the loop is already running.

### 5.5 Create the Kafka topic

```bash
# Linux/macOS
KAFKA_HOME=/opt/bigdata/kafka ./scripts/create_kafka_topic.sh

# Windows
set KAFKA_HOME=C:\bigdata\kafka
scripts\create_kafka_topic.bat
```

### 5.6 Create the HDFS folder

```bash
# Linux/macOS
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /urbanbrew/orders
$HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /urbanbrew/orders
# Windows
%HADOOP_HOME%\bin\hdfs dfs -mkdir -p /urbanbrew/orders
%HADOOP_HOME%\bin\hdfs dfs -chmod -R 777 /urbanbrew/orders
```

That `chmod` matters: `-mkdir` creates the folder owned by whatever OS
user you're logged in as (Hadoop's unsecured "simple" auth just trusts
your username), but `consumer/hdfs_writer.py` (and, if you use it
instead, Kafka Connect's HDFS3 sink) writes as `HDFS_USER` from `.env`
(default `hadoop`) — a **different** identity as far as HDFS is
concerned, which gets denied write access unless the folder is
world-writable. Opening it up with `chmod 777` sidesteps this entirely;
it costs nothing in a single-user practice cluster with no real security
enabled anyway.

### 5.7 (Optional, bonus) Set up Kafka Connect

**Not required to run the default pipeline** — skip straight to Section
6 if you're on a time budget; `consumer/mysql_writer.py` and
`consumer/hdfs_writer.py` (plain Python, nothing extra to install) are
what the default demo runs. If your group has time and wants the extra
Q&A safety margin of literally satisfying Section 3.2's "Kafka Connect
configured to sink data automatically into downstream storage" line
(rubric Component 4) as well, see `kafka_connect/README.md` for the
complete guide: which connector JARs to download, exactly where to put
them, filling in your MySQL password, starting the worker, and
troubleshooting.

---

## 6. Running the full pipeline (live demo)

Open **6 terminals**, all with the virtual environment activated, all
`cd`'d into `urbanbrew/`. Start them roughly in this order so all three
consumers are already listening before orders start flowing:

| # | Terminal | Windows | Linux/macOS |
|---|---|---|---|
| 1 | Django API | `py manage.py runserver` | `python3 manage.py runserver` |
| 2 | MySQL writer | `py consumer\mysql_writer.py` | `python3 consumer/mysql_writer.py` |
| 3 | HDFS writer | `py consumer\hdfs_writer.py` | `python3 consumer/hdfs_writer.py` |
| 4 | Notifier, instance A | `py consumer\notifier.py --instance-name A` | `python3 consumer/notifier.py --instance-name A` |
| 5 | Notifier, instance B *(rebalance demo)* | `py consumer\notifier.py --instance-name B` | `python3 consumer/notifier.py --instance-name B` |
| 6 | AutoScript | `py autoscript\simulate.py --rate 5` | `python3 autoscript/simulate.py --rate 5` |

`--rate 5` means ~5 orders/second — enough to reach the ~40-order
minimum the ML step needs within under a minute. Turn it up/down live to
show the "velocity" dimension is genuinely tunable.

> Terminals 2, 3, and 4/5 are all **independent** — Kafka gives each
> named consumer group its own full copy of every message, so none of
> them knows the others exist. If you'd rather demo the optional Kafka
> Connect path instead of terminals 2 and 3, see
> `kafka_connect/README.md` — but don't run both at once, or every order
> gets written twice.

### Verify data is flowing

- **MySQL (operational)**: `http://127.0.0.1:8000/api/orders/list/`
  (sign in first — see "Authentication" above), or
  `SELECT COUNT(*) FROM api_order;` in MySQL — written automatically by
  `consumer/mysql_writer.py`, no manual step.
- **HDFS (historical)**: `hdfs dfs -ls -R /urbanbrew/orders` (you should
  see one small JSON file per order under `dt=YYYY-MM-DD/` — click into
  that folder in the NameNode's web UI at
  `http://localhost:9870/explorer.html` if you're checking visually,
  since a folder row always shows "0 B" no matter how much is inside
  it) — written automatically by `consumer/hdfs_writer.py`, also no
  manual step.
- **Live Alerts (business logic)**: `http://127.0.0.1:8000/dashboard/`
  → "🔔 Live Alerts" panel, or `http://127.0.0.1:8000/admin/` →
  `LiveAlert` — written by our own `consumer/notifier.py`.
- **Django Admin**: `http://127.0.0.1:8000/admin/`

### Demonstrate the consumer-group rebalance (rubric item 2)

With terminals 4 and 5 both running, note the "Partitions assigned" line
each prints. Press **Ctrl+C in terminal 5**. Watch terminal 4 print
"Partitions revoked" then "Partitions assigned" with ALL partitions —
that's the rebalance. Restart terminal 5 and watch it happen again in
reverse. While this happens, run in a 7th terminal:

```bash
# Linux/macOS
KAFKA_HOME=/opt/bigdata/kafka ./scripts/check_consumer_group.sh
# Windows
set KAFKA_HOME=C:\bigdata\kafka
scripts\check_consumer_group.bat
```

This inspects the `urbanbrew-notifier` group's offsets/lag. The exact
same rebalance demo works identically with `consumer/mysql_writer.py` or
`consumer/hdfs_writer.py` too (run two instances of either one, and pass
its group name — `urbanbrew-mysql-writer` or `urbanbrew-hdfs-writer` —
as the script's argument) — every custom consumer in this project shares
the same rebalance-listener code in `consumer/kafka_utils.py`.

---

## 7. Running the Spark analytics + MLlib job

Run this any time after ~40+ orders have flowed through (re-run it
whenever you want the dashboard refreshed — right before each demo is a
good habit):

```bash
# Linux/macOS
$SPARK_HOME/bin/spark-submit \
  --packages com.mysql:mysql-connector-j:8.3.0 \
  spark_jobs/analytics_job.py

# Windows (Command Prompt)
%SPARK_HOME%\bin\spark-submit.cmd ^
  --packages com.mysql:mysql-connector-j:8.3.0 ^
  spark_jobs\analytics_job.py
```

Then visit **`http://127.0.0.1:8000/dashboard/`** to see the refreshed
charts, model accuracy metrics, and the Risk Watch panel (each
prediction now also shows a **recommended action** — see
`spark_jobs/analytics_job.py`'s `recommend_action()`).

> Unlike a day-by-day forecast, this model needs no multi-day wait — a
> single autoscript run of a few hundred orders is enough for a
> meaningful train/test split and real accuracy numbers.

---

## 7b. (Bonus, optional) Running the Hadoop MapReduce comparison job

Worth up to 2 bonus marks (guidelines Section 3.6) — **not required**,
and doesn't replace anything above. Computes the exact same "orders +
revenue per branch" insight as `spark_jobs/analytics_job.py`'s
`compute_branch_summary()`, the low-level way, so the two can be fairly
compared:

```bash
# Linux/macOS
HADOOP_HOME=/opt/bigdata/hadoop ./scripts/run_mapreduce.sh

# Windows
set HADOOP_HOME=C:\bigdata\hadoop
scripts\run_mapreduce.bat
```

See `mapreduce/order_count_mapper.py` / `order_count_reducer.py` for how
it works (both are short, heavily-commented Hadoop Streaming scripts —
no Java required), and `mapreduce/COMPARISON.md` for the required
written comparison (code complexity, development time, runtime — fill in
your own timing numbers after running both jobs on your machine).

---

## 7.5. Making it fully automated (no manual re-runs, dashboard auto-refreshes)

By default, the dashboard only shows fresh branch/item/hourly summaries
and ML predictions after you manually re-run the command above — and
the page itself only reloads if you press refresh. Two small additions
turn this into a genuinely hands-off, always-live pipeline, matching
how a real production setup would behave, **without** rewriting
`analytics_job.py` into true Spark Structured Streaming (a much bigger,
riskier change — see the trade-off note below):

1. **The dashboard page auto-refreshes itself.** `dashboard.html`'s
   JavaScript now calls the `dashboard-data` JSON endpoint every 5
   seconds on a loop (`setInterval`), instead of once on page load.
   Nothing to run for this — it's already live in the template. The
   **Live Alerts** panel in particular updates in genuinely real time
   this way, since `consumer/notifier.py` writes each alert to MySQL
   the instant it sees a matching order — no Spark involved.

2. **The Spark analytics job re-runs itself on a schedule**, via
   `scripts/run_spark_loop.sh` (Linux/macOS) or
   `scripts\run_spark_loop.bat` (Windows) — a small wrapper that calls
   `spark-submit` in a loop, waiting a configurable number of seconds
   (`INTERVAL_SECONDS`, default 180) between runs:
   ```bash
   # Linux/macOS — run from the urbanbrew/ project directory
   INTERVAL_SECONDS=60 ./scripts/run_spark_loop.sh

   # Windows (Command Prompt)
   set INTERVAL_SECONDS=60
   scripts\run_spark_loop.bat
   ```
   Leave this running in its own terminal alongside the autoscript, the
   MySQL/HDFS writers, and the notifier, and `branch_summary` / `item_summary` /
   `hourly_summary` / `model_metrics` / `order_risk_predictions` all
   refresh themselves automatically — combined with the dashboard's own
   auto-refresh, you never have to touch a terminal again once
   everything is started.

**Why a repeating batch loop instead of true Spark Structured
Streaming (worth saying plainly if asked):** Structured Streaming
would make the analytics genuinely continuous with no re-run at all,
but it's a real architectural rewrite — a different read API
(`readStream`), a different write path since JDBC isn't a native
streaming sink (needs `foreachBatch`), and checkpointing to track
progress safely across restarts. That's meaningfully more that can go
subtly wrong, and it's not what this project's own design already
describes: `PRESENTATION_TALKING_POINTS.md` explicitly frames the
**instant, rule-based path** (`notifier.py` → Live Alerts) and the
**periodic batch/ML path** (Spark → Risk Watch) as two deliberately
different techniques worth contrasting — a trap-question answer this
project already has (see PRESENTATION_TALKING_POINTS.md Component 9).
An auto-repeating batch loop keeps that same, deliberate two-path
story intact while still making the whole dashboard feel continuously
live — the best trade-off of "correct," "simple," and "matches what
you've already told the story to be."

---

## 8. Troubleshooting

**`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`** — you
have the old `kafka-python==2.0.2` installed on **Python 3.12 or
newer** (very common on a fresh Mac/Homebrew or Windows install today).
kafka-python 2.x vendors an old copy of the `six` library whose import
trick breaks on Python 3.12+; `requirements.txt` now pins
`kafka-python==3.0.11`, which dropped that dependency and officially
supports Python 3.8–3.14. Fix it in your existing virtual environment
with:
```bash
# Windows
py -m pip install --upgrade kafka-python
# Linux/macOS
python3 -m pip install --upgrade kafka-python
```
No code changes needed — every Kafka API this project uses
(`KafkaProducer`, `KafkaConsumer`, serializers, `ConsumerRebalanceListener`,
`.subscribe()`/`.commit()`) is unchanged between 2.x and 3.x.

**mysqlclient / MySQLdb import errors** — this project intentionally uses
`PyMySQL` instead (see `urbanbrew/urbanbrew/__init__.py`), because
`mysqlclient` needs a C compiler on Windows.

**`sudo mysql` doesn't work on Windows** — Linux-specific (socket auth).
On Windows use `mysql -u root -p` with your root password.

**`python`, `python3`, or `py`?** — Windows installs the `py` launcher by
default; Linux/macOS almost always use `python3`. Every command above
shows both.

**Windows: HDFS/Hadoop needs `winutils.exe`** — download a `winutils.exe`
+ `hadoop.dll` matching your Hadoop version, place them in
`%HADOOP_HOME%\bin`, and set `HADOOP_HOME` before starting HDFS.

**`--packages` in spark-submit can't reach Maven** — download
`mysql-connector-j-8.3.0.jar` manually from
https://dev.mysql.com/downloads/connector/j/ and run spark-submit with
`--jars /path/to/mysql-connector-j-8.3.0.jar` instead.

**`consumer/hdfs_writer.py` can't connect / "Connection refused"** — it
talks to HDFS over **WebHDFS (HTTP)**, not the `hdfs://` RPC port Spark
uses. Confirm `HDFS_WEBHDFS_URL` in `.env` matches your NameNode's web
UI port (default `9870` on Hadoop 3.x; older Hadoop 2.x installs use
`50070`), and that the NameNode is actually running
(`http://localhost:9870` should load a page in your browser).

**`consumer/hdfs_writer.py` crashes with `HdfsError: Permission denied:
user=hadoop, access=WRITE, inode="/urbanbrew/orders":...`** — you
created `/urbanbrew/orders` (Section 5.6) without the `chmod -R 777`
step. `hdfs dfs -mkdir` made the folder owned by your OS username
(Hadoop's unsecured mode just trusts whoever's logged in), but
`hdfs_writer.py` connects claiming to be `HDFS_USER` from `.env`
(default `hadoop`) — a different identity, denied write access on a
folder that only its owner can write to. Fix it without recreating
anything:
```bash
# Linux/macOS
$HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /urbanbrew/orders
# Windows
%HADOOP_HOME%\bin\hdfs dfs -chmod -R 777 /urbanbrew/orders
```
Then re-run `consumer/hdfs_writer.py` — no restart of anything else needed.

**`spark-submit` crashes immediately with `java.lang.
UnsupportedOperationException: getSubject is not supported`** (thrown
from deep inside `org.apache.hadoop.security.UserGroupInformation`,
before your script even starts) — your machine's default Java is **too
new** for the Hadoop libraries Spark 3.5.x bundles. This shows up on
Java 21+ (especially Java 23/24), which removed/blocked the old
security APIs (`Subject.getSubject`) that Hadoop 3.3.4's client still
calls. It's a Spark/Hadoop-vs-JDK version mismatch, nothing to do with
your code or `.env`. Fix it by pointing **just this terminal session**
at Java 17 (an LTS version Spark 3.5 fully supports), without touching
your system's default Java. (A quick way to tell Linux from macOS if
you're not sure which you're on: your home folder is `/home/<user>/...`
on Linux, `/Users/<user>/...` on macOS — a zsh theme with a
`mac`-looking username/hostname doesn't necessarily mean macOS.)
```bash
# Linux — see what's already installed:
ls /usr/lib/jvm/
# If a java-17-... folder is listed:
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # match the exact folder name

# If no 17 is listed and you use conda (no sudo needed):
conda install -c conda-forge openjdk=17 -y
export JAVA_HOME=$CONDA_PREFIX

# ...or with apt + sudo instead:
sudo apt install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# macOS — list every JDK actually installed:
/usr/libexec/java_home -V
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
# if none listed: brew install openjdk@17, then the export above
```
Then re-run the exact same `spark-submit` command in that same
terminal — `JAVA_HOME` only affects the shell you set it in, so nothing
else on your machine changes. You'll need to `export JAVA_HOME=...`
again in any new terminal you use for Spark.

**`consumer/mysql_writer.py`, `hdfs_writer.py`, or `notifier.py` crash
with `kafka.errors.KafkaTimeoutError: KafkaTimeoutError: None`, usually
right after a "Partitions revoked" line** — the consumer processed a
message and then `consumer.commit()` didn't hear back from the group
coordinator in time. This is almost always a **volume** problem, not a
code bug: if you've left `autoscript/simulate.py` running for a long
time (tens of thousands of orders, offsets past 10,000+), the sustained
message rate can make Kafka's group coordinator slow to respond,
especially during a rebalance (e.g. when a second `notifier.py`
instance starts/stops for the rebalance demo). **Stop the autoscript**
first — you only need `MIN_ORDERS_TO_TRAIN` (40) orders to train the
model, so tens of thousands is far more than the demo needs and just
adds load to every part of the pipeline (Kafka, HDFS's NameNode, and
these consumers). Then just re-run the consumer script that crashed -
at-least-once delivery means the one message whose commit didn't land
gets safely reprocessed, never lost.
That said, a plain restart *used to* crash `hdfs_writer.py` a second
time, with `HdfsError: .../order_pX_oY.json ... already exists` - if
the HDFS write itself had actually succeeded just before the failed
commit, the reprocessed message tried to create the exact same file
again, and HDFS correctly refused. `hdfs_writer.py` now catches
exactly this case (`err.exception == 'FileAlreadyExistsException'`),
logs that the order was already safely stored, and moves on to commit
instead of crashing - so a restart after this kind of timeout is safe
to do repeatedly.

**`consumer/mysql_writer.py` / `hdfs_writer.py` exit immediately with an
import error** — make sure you're running them from inside `urbanbrew/`
(not from `consumer/`) with the virtual environment active, so
`config.py` and `urbanbrew.settings` can be found (see each script's
"PROJECT_ROOT bootstrap" comment).

**Model metrics/Risk Watch panel are empty** — you need 40+ orders in
HDFS before the Spark job will train a model (see
`MIN_ORDERS_TO_TRAIN` in `spark_jobs/analytics_job.py`). Let the
autoscript run a little longer, then re-run the Spark job.

**Live Alerts panel is empty** — the notifier only creates an alert when
an order's total is >= `LARGE_ORDER_ALERT_THRESHOLD` in `.env` (default
1500). Either wait for autoscript to generate a big enough order, or
temporarily lower the threshold and restart `consumer/notifier.py`.

**Dashboard shows `TypeError: Object of type bytes is not JSON
serializable` in the Django terminal, and `/dashboard/` shows "Could
not load dashboard data" even though Spark finished successfully** —
`dashboard/models.py`'s `OrderRiskPrediction.actual_flagged` /
`.predicted_flagged` are declared as `BooleanField`, but that table is
created by **Spark's JDBC writer** (`mode="overwrite"` in
`spark_jobs/analytics_job.py`), not by a Django migration — and Spark
maps `BooleanType` columns to MySQL's `BIT(1)` type, not `TINYINT(1)`.
Django's MySQL backend only knows how to auto-convert `TINYINT(1)`
values into real Python `bool`s; a `BIT(1)` column comes back from
PyMySQL as a raw `bytes` object (`b'\x00'`/`b'\x01'`), which
`json.dumps()` can't serialize. `dashboard/views.py` now has a
`_to_bool()` helper that normalizes bytes/int/bool into a plain `bool`
before the response is built — already fixed, nothing to do here
unless you're comparing against an older copy of `views.py`.

**Dashboard shows real KPI numbers at the top, but every chart is
blank and the "Could not load dashboard data" banner still shows** —
this happened when Chart.js was loaded from a CDN
(`cdnjs.cloudflare.com`) and the network couldn't reach it (school
Wi-Fi, a firewall, or just no internet at that moment): the KPI row is
plain text and doesn't need Chart.js, but every `new Chart(...)` call
threw `Chart is not defined`, which aborted the rest of the page's
render function (the metrics boxes, Risk Watch table, and Live Alerts
table never ran either). `dashboard.html` now loads Chart.js from
**our own static file**
(`dashboard/static/dashboard/chart.umd.min.js`) instead of a CDN, so
the whole dashboard works even fully offline. If you ever see this
again, open the browser's dev tools Console tab — the exact error line
tells you which script failed to load.

**Dashboard redesign (AdminLTE-style admin layout)** — `dashboard.html`
now uses a sidebar + topbar admin layout, Font Awesome icons, and the
Poppins font, all self-hosted the same way Chart.js is (see the two
entries above) — nothing loads from Google Fonts or a Font Awesome
CDN either, so the whole page still works fully offline. It also
splits what used to be one dual-axis "Orders & Flagged-Order Rate by
Hour" chart into two separate single-axis charts ("Orders by Hour" and
"Flagged-Order Rate by Hour") — a dual-axis chart (two independent
y-scales sharing one plot) makes it look like the two lines are
correlated purely because of how the axes happen to be scaled, which
isn't a real relationship in the data. Branch and menu-item colors are
now fixed by name (Kigali is always the same blue everywhere on the
page, Ruhengeri always orange, Rubavu always aqua; same idea for
Tea/Coffee/Juice) instead of whatever order MySQL happens to return
rows in, so a color always means the same branch/item no matter when
you refresh.

**Dashboard "auto-refreshes" but it's distracting / feels like
flickering every 5 seconds** — `dashboard.html` now keeps the last
successfully rendered JSON payload and skips ALL chart/table redraws
whenever a refresh comes back identical to what's already on screen
(this is the common case — most 5-second ticks land between new
orders). When the data genuinely changes, charts are updated in place
with `chart.update('none')` instead of being destroyed and recreated,
so there's no blank-canvas flash, and the error banner is a
permanently-reserved line (not inserted/removed) so it can never shift
the layout. If you still find 5s too frequent for your machine, raise
`REFRESH_MS` near the top of `dashboard.html`'s `<script>` block.

**Visiting `/dashboard/` redirects you to a sign-in page you weren't
expecting** — not a bug: the dashboard now requires a login (see 5.4.1
"Authentication"). Sign in with the superuser account you created in
step 5.4 (`python manage.py createsuperuser`). If you never ran that
command, run it now, then sign in.

**`autoscript/simulate.py` crashes immediately with `RuntimeError:
Missing required setting 'AUTOSCRIPT_API_TOKEN'`** — the
order-ingestion API now requires a token on every request (see 5.4.1).
Follow 5.4.1 to create the `autoscript_service` user, run
`python manage.py drf_create_token autoscript_service`, and paste the
printed token into `AUTOSCRIPT_API_TOKEN=` in your `.env`.

**AutoScript prints `❌ API Error 401 — not authenticated` (or `403`)
for every order** — `AUTOSCRIPT_API_TOKEN` in `.env` is missing, wrong,
or belongs to a user that no longer has a token. Re-run
`python manage.py drf_create_token autoscript_service` (it prints the
*existing* token if one already exists, or use
`drf_create_token -r autoscript_service` to force a fresh one), and
make sure the value in `.env` matches exactly — no `Token ` prefix, no
quotes, no trailing space.

**Testing the API by hand with curl/Postman gets 401** — add the
header the browsable API also needs:
`-H "Authorization: Token <your AUTOSCRIPT_API_TOKEN value>"`. If
you're signed into the dashboard in the same browser, you can also
just open `http://127.0.0.1:8000/api/orders/` directly — DRF's
browsable UI reuses your existing session login and lets you POST a
test order from a plain HTML form, no token needed for that one path.

**(Only if you're trying the optional Kafka Connect path) Kafka Connect
won't start, or MySQL/HDFS stay empty** — this has its own dedicated,
detailed troubleshooting section: see `kafka_connect/README.md`. The
short version: check the connector JARs are actually unpacked under
`kafka_connect/plugins/<connector-name>/` (not loose files, and not
skipped), and that `kafka_connect/mysql-sink.properties`'s
`connection.password` was actually edited to your real password (that
file is NOT read from `.env`). This isn't the default pipeline, though —
if it's not coming together in time, just run `consumer/mysql_writer.py`
/ `consumer/hdfs_writer.py` instead (Section 6), which is what the demo
uses by default anyway.

**Risk Watch panel is missing the "Recommended action" column, or shows
old data with no action** — you're looking at predictions from BEFORE
this project added `recommended_action` (Round 4 below). Since
`order_risk_predictions` is written fresh by Spark every run
(`mode="overwrite"` — see `analytics_job.py`), just re-run the Spark job
once and the new column appears automatically; no migration needed
(this table is `managed = False` in `dashboard/models.py` — Django never
creates or alters it, Spark's own JDBC writer does).

---

## 9. What changed, and why (four rounds of revision)

### Round 1 — from the tea-shop revenue-forecast version

- **New case study**: Urban Brew, a 3-branch café chain — same familiar
  shape (branches, items, orders) as the earlier tea shop draft, so
  everything you already understood about the pipeline still applies.
- **New ML task**: order-level risk classification (will this order need
  staff review?) instead of branch-level daily revenue forecasting —
  trains and predicts from day one, no multi-day wait.
- **New fields**: `payment_method` and `is_flagged` (the simulated
  ground-truth label) added to every order.
- **New evaluation**: accuracy, precision, recall, F1, and AUC on a
  held-out test split (a genuinely richer, more standard classification
  report than the old RMSE/MAE-only regression evaluation).
- **New dashboard panel**: "Risk Watch" — a real prediction-explanation
  panel per rubric item 7, listing specific orders and why the model
  thinks they're risky, replacing the old anomaly-vs-forecast panel.

### Round 2 — aligned to the original reference architecture diagram

The original diagram shows **one** "Kafka Connect → Storage" path
covering both the operational and historical stores, and a **separate**
"Custom Application (Python consumer) → business logic/notifications"
path straight to the dashboard. Earlier drafts of this project had the
custom Python consumer writing to HDFS, which doesn't match that split.
This version fixes that:

- **Kafka Connect now owns *both* sinks**: added
  `kafka_connect/hdfs-sink.properties` alongside the existing
  `mysql-sink.properties`, run together in one Connect worker — no
  custom code writes to storage anymore.
- **The custom consumer is now genuinely "business logic," not another
  storage writer**: `consumer/hdfs_consumer.py` → `consumer/notifier.py`,
  which checks a simple, explainable rule (large order total) and writes
  to a new `LiveAlert` table, shown live on the dashboard — this is the
  "Custom business logic/Notifications" box from the diagram.
  It still keeps every rubric-required consumer behaviour (named group,
  manual offset commits, multi-instance rebalance demo) — only *what it
  writes* changed, not *how it consumes*.
- **New dashboard panel**: "🔔 Live Alerts", fed by the notifier, sitting
  alongside the Spark/MLlib-driven "Risk Watch" panel — two clearly
  different techniques (an instant rule vs. a trained model) solving two
  clearly different problems, which is a good talking point on its own.

### Round 3 — dropped Kafka Connect for two more custom Python consumers

Kafka Connect (Round 2's JDBC Sink + HDFS Sink) worked, but needed a
JDBC connector JAR, a MySQL driver JAR, a Hadoop-version-matched HDFS
connector JAR, and a correctly configured `plugin.path` before it would
even start — too many extra, fragile moving parts for a cross-platform
beginner project, and none of it was code the group could read or
explain. This round replaces it with two more plain Python consumers,
so the whole pipeline — producer AND all storage/business-logic
consumers — is 100% Python we wrote ourselves:

- **`consumer/mysql_writer.py`** (new) replaces the JDBC Sink connector
  — inserts each order into MySQL's `api_order` table via the Django
  ORM. Bonus: it can supply the order's real `created_at` on every
  insert, so `sql/create_tables.sql`'s one-time MySQL default tweak
  (needed only because Connect's insert skipped that column) is gone
  too — one less setup step.
- **`consumer/hdfs_writer.py`** (new, recreated from an earlier draft)
  replaces the HDFS Sink connector — writes one small JSON file per
  order into a `dt=YYYY-MM-DD/` folder via WebHDFS (the `hdfs` pip
  package), which `spark_jobs/analytics_job.py` reads back exactly as
  before.
- **`consumer/kafka_utils.py`** (new) factors out the consumer-building
  and rebalance-listener code all three consumers share, so it's
  written and explained once instead of three times.
- **The `urbanbrew/kafka_connect/` folder is gone** — there is no
  Kafka Connect worker to start at all anymore. The pipeline now has
  **three** independent, named, rebalance-capable custom consumers
  (`mysql_writer`, `hdfs_writer`, `notifier`) instead of one plus a
  Connect worker — see Section 2's architecture diagram.
- **A defensible trade-off, worth saying plainly if asked**: earlier
  project guidance explicitly mentioned Kafka Connect as one way to
  satisfy "automatic sink, no manual copy steps." We evaluated it,
  found the setup fragile across different group members' machines,
  and implemented the *exact same behavior* — automatic, code-driven,
  zero manual copying — with plain Python instead. Examiners generally
  care that you understand *why* a component exists and can defend an
  alternative, not that you used one specific named tool; say so
  directly rather than pretending Kafka Connect is still in the build.

### Round 4 — read the ACTUAL official guidelines closely, closed 3 real gaps

Rounds 1–3 above were built against an early, informal sketch of the
requirements. Reading the actual, official
`Group_Final_Exam_Project_Guidelines.pdf` closely (Sections 3–6)
surfaced three genuine gaps against the literal, graded rubric — this
round closes all three, without touching anything that was already
correct:

- **Kafka Connect is back, for real** (rubric Component 4, 3 marks):
  Section 3.2 explicitly requires *"Kafka Connect configured to sink
  data automatically into downstream storage,"* and it's a separately
  graded line, not just "any automatic sink." Round 3's reasoning above
  (fragile setup, more moving parts) was a legitimate engineering
  argument, but against a literal, separately-graded rubric line, a
  strict examiner could reasonably award Component 4 zero. We
  reintroduced real Kafka Connect (`kafka_connect/` — a JDBC Sink into
  MySQL, an HDFS3 Sink into HDFS), fully built and documented, satisfying
  the requirement exactly as written if run. (Round 5 below changed
  which of Kafka Connect vs. the Python writers is the *default* demo
  path — both are still fully built either way.) `consumer/notifier.py`
  remains the project's one custom Python consumer, satisfying Section
  3.2's *separate* "custom Python consumer application... named consumer
  group" requirement (Component 2) — Kafka Connect and a custom business-
  logic consumer were never actually alternatives to each other in the
  guidelines; they're two different, both-mandatory things.
- **Authentication, satisfying the dashboard's "at least one
  enhancement" requirement** (rubric Component 7): the dashboard now
  requires a session login, and the order-ingestion API requires a DRF
  token — see "Authentication" above. The rubric's own Component 7
  criteria literally lists "auth" as an example enhancement, so this
  both secures the demo and satisfies that requirement explicitly.
- **A recommendation, alongside the prediction** (Section 4's example
  case studies mention "prediction/recommendation" for e-commerce; only
  ONE MLlib model is actually required - Section 3.4 - so rather than
  build a second real model, `spark_jobs/analytics_job.py`'s
  `recommend_action()` turns the existing risk model's probability into
  a concrete recommended next action, shown as a new column on the Risk
  Watch panel. One trained PREDICTION, one derived RECOMMENDATION —
  simple, explainable, and doesn't risk the already-working classifier.
- **Bonus: a Hadoop MapReduce comparison** (Section 3.6, up to 2 bonus
  marks, optional): `mapreduce/order_count_mapper.py` +
  `order_count_reducer.py` (Hadoop Streaming, no Java) compute the same
  branch/revenue insight as `analytics_job.py`'s
  `compute_branch_summary()`, with the required written comparison in
  `mapreduce/COMPARISON.md`.

### Round 5 — reverted the default sink back to the Python consumers; Kafka Connect stays as a documented, optional path

Round 4 made Kafka Connect the default demo path for the MySQL/HDFS
sinks, purely to satisfy Section 3.2's literal wording as safely as
possible. After weighing it again, the group's call was that
`consumer/mysql_writer.py` / `consumer/hdfs_writer.py` are the better
choice to actually run on exam day — Kafka Connect's extra JAR
downloads, exact-version matching, and `plugin.path` setup are real,
documented friction across different machines, and the two Python
writers do the *identical* job (automatic sink, zero manual copy steps,
one small file/row per order) with code every group member wrote and can
read line-by-line if asked. This is a deliberate, informed trade-off,
not an oversight — see `kafka_connect/README.md`'s opening section for
the fuller reasoning:

- **`consumer/mysql_writer.py` and `consumer/hdfs_writer.py` are the
  pipeline's default again** — Section 6's terminal table runs them, and
  Section 2's architecture diagram shows them as the primary MySQL/HDFS
  sinks.
- **Kafka Connect is unchanged and still fully built and documented** in
  `kafka_connect/` — nothing was deleted or degraded. It's simply framed
  as an optional, already-working alternative path rather than the
  default, so the group can still demo it, or discuss it confidently in
  Q&A, without needing it to work on the actual exam machine.
- **Dashboard real-time and ML explainability improvements** added
  alongside this revert — see "Dashboard enhancements" below.

### Dashboard enhancements (real-time notifications + a clearer prediction/recommendation story)

Beyond the required panels, two additions specifically target the two
kinds of question examiners tend to ask live: *"is this actually
real-time?"* and *"what exactly is the model predicting, and where's the
recommendation?"*

- **New-alert / new-high-risk-order highlighting**: the dashboard already
  polled for fresh data every 5 seconds; it now visibly calls out what's
  NEW between polls — freshly arrived Live Alerts and newly-flagged
  high-risk predictions are highlighted the moment they appear, plus a
  toast/banner announcing the count, instead of just silently updating
  numbers a viewer has to notice on their own. This makes the
  Kafka→dashboard latency visibly demonstrable, not just claimed.
- **An always-visible prediction/recommendation explainer**: the Risk
  Watch panel now states plainly, right above the table, what's being
  predicted (probability an order needs staff review) and what the
  recommendation is derived from (`recommend_action()`'s probability
  thresholds) — so "what are you predicting, where are the
  recommendations" has an immediate, on-screen answer during Q&A, not
  just one buried in code or documentation.
