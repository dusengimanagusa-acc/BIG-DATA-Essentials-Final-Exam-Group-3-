# ☕ Urban Brew — Step-by-Step: Zero to Working Demo

*Big Data Essentials — Group Final Exam Project*

This is the **single, linear checklist** — do the steps in this exact
order and you will end with the whole pipeline running and the
dashboard live. Every command shows **Windows** and **Linux/macOS**
side by side. Tick each box as your group finishes it.

- `README.md` explains *why* things are built this way (architecture,
  design decisions).
- `PRESENTATION_TALKING_POINTS.md` is what to *say* to examiners.
- **This file** is only "what to type, in what order."

> 💡 Split the work: one person can do Phase A (installs) on every
> machine while another does Phase C (project setup) on the machine
> that will run the live demo.

---

## Phase A — Install the software (once per machine)

- [ ] **Python 3.10+**
  Windows: install from python.org (tick "Add to PATH").
  Linux/macOS: `sudo apt install python3 python3-venv` (or already on macOS).

- [ ] **Java 11 or 17** (Kafka, Hadoop, and Spark all need it)
  Windows: install the Adoptium Temurin MSI.
  Linux: `sudo apt install openjdk-17-jdk`.
  Check it worked: `java -version` in a new terminal.

- [ ] **Apache Kafka** — this project runs Kafka in **KRaft mode** (Kafka's
  own built-in consensus, no separate Zookeeper process to install,
  configure, or keep running). Nothing extra to download for this -
  modern Kafka releases (3.x+) ship KRaft support already.
  Download the `.tgz` from kafka.apache.org, then unzip it to:
  Windows: `C:\bigdata\kafka`
  Linux/macOS: `/opt/bigdata/kafka`

- [ ] **Apache Hadoop** (for HDFS)
  Unzip to:
  Windows: `C:\bigdata\hadoop` — **also** download `winutils.exe` +
  `hadoop.dll` matching your Hadoop version and place them in
  `C:\bigdata\hadoop\bin` (search "winutils <your-hadoop-version>").
  Linux/macOS: `/opt/bigdata/hadoop`

- [ ] **Apache Spark**
  Unzip to:
  Windows: `C:\bigdata\spark`
  Linux/macOS: `/opt/bigdata/spark`

- [ ] **MySQL Server 8.x**
  Windows: run the MySQL Installer, keep the default "MySQL80" service.
  Linux: `sudo apt install mysql-server`

- [ ] **Set environment variables** so every command below can find
  these tools (put this in your shell profile / Windows environment
  variables so it's permanent, not just this one terminal):

  ```bash
  # Linux/macOS (add to ~/.bashrc or ~/.zshrc)
  export KAFKA_HOME=/opt/bigdata/kafka
  export HADOOP_HOME=/opt/bigdata/hadoop
  export SPARK_HOME=/opt/bigdata/spark

  # Windows (System Properties -> Environment Variables, or per-terminal):
  set KAFKA_HOME=C:\bigdata\kafka
  set HADOOP_HOME=C:\bigdata\hadoop
  set SPARK_HOME=C:\bigdata\spark
  ```

---

## Phase B — Start the big-data services

These are the underlying servers everything else talks to. Start them
**in this order**, each in its **own terminal that you leave open**.

- [ ] **B1. MySQL**
  Linux: `sudo systemctl start mysql`
  Windows: usually already running as a service; if not, `net start MySQL80`
  (service name may differ — check Windows Services app).

- [ ] **B2. Format Kafka's storage — KRaft mode (ONLY the very first time
  ever, per machine)**
  ⚠️ Skip this on every run after the first — it wipes Kafka's own
  metadata log (not your data, but re-running it on an already-running
  broker's directory throws a "already formatted" error, which is your
  sign you already did this step). No Zookeeper involved at all — a
  Kafka cluster ID + a local metadata log replace what Zookeeper used to
  track.
  ```bash
  # Linux/macOS - from inside $KAFKA_HOME
  cd $KAFKA_HOME
  KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
  bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties

  # Windows - from inside %KAFKA_HOME%, in one Command Prompt session
  cd %KAFKA_HOME%
  for /f %i in ('bin\windows\kafka-storage.bat random-uuid') do set KAFKA_CLUSTER_ID=%i
  bin\windows\kafka-storage.bat format -t %KAFKA_CLUSTER_ID% -c config\server.properties
  ```

- [ ] **B3. Kafka broker** (every session — no Zookeeper to wait on first,
  this is the only Kafka process you start)
  ```bash
  # Linux/macOS - Terminal 1
  $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

  # Windows - Terminal 1
  %KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties
  ```
  Check it worked: `$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
  (Windows: `%KAFKA_HOME%\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092`)
  should return without error (an empty list is fine — the topic itself
  is created in step C5).

- [ ] **B4. HDFS — format the NameNode (ONLY the very first time ever)**
  ⚠️ Skip this step on every run after the first — it erases HDFS data.
  ```bash
  # Linux/macOS
  $HADOOP_HOME/bin/hdfs namenode -format
  # Windows
  %HADOOP_HOME%\bin\hdfs namenode -format
  ```

- [ ] **B5. HDFS — start NameNode + DataNode** (every session)
  ```bash
  # Linux/macOS - one command starts both
  $HADOOP_HOME/sbin/start-dfs.sh

  # Windows - if start-dfs.cmd exists in %HADOOP_HOME%\sbin, use it:
  %HADOOP_HOME%\sbin\start-dfs.cmd
  # Otherwise (no start-dfs.cmd in your Hadoop build) start both by hand,
  # each in its own terminal:
  #   Terminal 3:  %HADOOP_HOME%\bin\hdfs.cmd namenode
  #   Terminal 4:  %HADOOP_HOME%\bin\hdfs.cmd datanode
  ```
  Check it worked: `hdfs dfsadmin -report` (or the NameNode UI at
  `http://localhost:9870`). That same URL/port is also what
  `consumer/hdfs_writer.py` talks to later (WebHDFS, HDFS's built-in
  HTTP API) — it's on by default, nothing extra to enable.

You now have MySQL, Kafka (KRaft mode — no separate Zookeeper process),
and HDFS all running. **Leave these terminals open** — everything below
assumes they're up.

---

## Phase C — One-time project setup (do this once per machine)

- [ ] **C1. Get the project and create a virtual environment**
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
  open for this project from now on.

- [ ] **C2. Configure your `.env`**
  ```bash
  # Windows
  copy .env.example urbanbrew\.env
  # Linux/macOS
  cp .env.example urbanbrew/.env
  ```
  Open `urbanbrew/.env` and set your real MySQL password (you'll create
  that password in the next step).

- [ ] **C3. Create the MySQL database + user**
  ```sql
  -- Linux/macOS: open with `sudo mysql`
  -- Windows: open with `mysql -u root -p` and your root password

  CREATE DATABASE urbanbrew_db;
  CREATE USER 'urbanbrew_user'@'localhost' IDENTIFIED BY 'group3rocks';
  GRANT ALL PRIVILEGES ON urbanbrew_db.* TO 'urbanbrew_user'@'localhost';
  FLUSH PRIVILEGES;
  ```
  Use the **same** password in `urbanbrew/.env`'s `MYSQL_PASSWORD`.

- [ ] **C4. Create the Django tables**
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

- [ ] **C4.5. Set up authentication (dashboard login + API token)**
  The superuser you just created in C4 is also your **dashboard
  login** now — nothing extra needed for that part. The
  order-ingestion API is separately protected by a token, for the
  autoscript (a script, not a person in a browser) to use:
  ```bash
  # Windows
  py manage.py createsuperuser --username autoscript_service
  py manage.py drf_create_token autoscript_service

  # Linux/macOS
  python3 manage.py createsuperuser --username autoscript_service
  python3 manage.py drf_create_token autoscript_service
  ```
  Copy the printed token into `urbanbrew/.env`:
  ```
  AUTOSCRIPT_API_TOKEN=<the token it printed>
  ```
  (Full explanation: README.md section 5.4.1 "Authentication.")

- [ ] **C5. Create the Kafka topic**
  ```bash
  # Linux/macOS
  KAFKA_HOME=/opt/bigdata/kafka ./scripts/create_kafka_topic.sh
  # Windows
  scripts\create_kafka_topic.bat
  ```
  This script just wraps the two commands below — if you'd rather run
  them directly (or the script isn't cooperating), from inside
  `$KAFKA_HOME`:
  ```bash
  bin/kafka-topics.sh --list --bootstrap-server localhost:9092
  # (empty the first time - confirms the broker is reachable)

  bin/kafka-topics.sh --create \
    --topic urbanbrew_orders \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
  ```
  (Windows: same flags, via `bin\windows\kafka-topics.bat`.)

- [ ] **C6. Create the HDFS folder**
  ```bash
  # Linux/macOS
  $HADOOP_HOME/bin/hdfs dfs -mkdir -p /urbanbrew/orders
  $HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /urbanbrew/orders
  # Windows
  %HADOOP_HOME%\bin\hdfs dfs -mkdir -p /urbanbrew/orders
  %HADOOP_HOME%\bin\hdfs dfs -chmod -R 777 /urbanbrew/orders
  ```
  Don't skip the `chmod` — without it, `consumer/hdfs_writer.py` (step D
  below) gets a `Permission denied` error the first time it tries to
  write (see "If something breaks" below for why).

  If HDFS is still in safe mode right after formatting/starting (writes
  fail with `Name node is in safe mode`), take it out manually:
  ```bash
  # Linux/macOS
  $HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
  # Windows
  %HADOOP_HOME%\bin\hdfs dfsadmin -safemode leave
  ```

- [ ] **C7. (Optional, bonus) Set up Kafka Connect** — not required for
  the default pipeline; `consumer/mysql_writer.py` and
  `consumer/hdfs_writer.py` (plain Python, nothing extra to install) are
  what Phase D runs by default. Skip straight to Phase D if you're on a
  time budget. If your group wants the extra Q&A safety margin of also
  literally satisfying Section 3.2's "Kafka Connect configured to sink
  data automatically into downstream storage" line (rubric Component 4),
  **budget real time for it** — full walkthrough (exact JAR downloads,
  where to unpack them, filling in your MySQL password) is in
  `kafka_connect/README.md`; the short version:
  1. Download the JDBC Sink connector + MySQL Connector/J, and the
     HDFS3 Sink connector (links in that guide).
  2. Unpack each into its own folder under `kafka_connect/plugins/`.
  3. Edit `kafka_connect/mysql-sink.properties` — set
     `connection.password` to your real MySQL password.
  4. Start it: `KAFKA_HOME=/opt/bigdata/kafka ./scripts/start_kafka_connect.sh`
     (Windows: `scripts\start_kafka_connect.bat`) — instead of, never
     alongside, the two Python writers below.

**Phase C is done once.** From here on, only Phases B and D repeat
each time you demo.

---

## Phase D — Run the full pipeline (every demo)

Make sure Phase B's services are all still running. Open the
following terminals, all with the virtual environment activated
(`.venv` from step C1), all `cd`'d into `urbanbrew/`. Start them in
this order so all three consumers are listening before orders start
flowing:

| # | Terminal | Windows | Linux/macOS |
|---|---|---|---|
| 1 | Django API | `py manage.py runserver` | `python3 manage.py runserver` |
| 2 | MySQL writer | `py consumer\mysql_writer.py` | `python3 consumer/mysql_writer.py` |
| 3 | HDFS writer | `py consumer\hdfs_writer.py` | `python3 consumer/hdfs_writer.py` |
| 4 | Notifier, instance A | `py consumer\notifier.py --instance-name A` | `python3 consumer/notifier.py --instance-name A` |
| 5 | Notifier, instance B *(for the rebalance demo)* | `py consumer\notifier.py --instance-name B` | `python3 consumer/notifier.py --instance-name B` |
| 6 | AutoScript (generates orders) | `py autoscript\simulate.py --rate 5` | `python3 autoscript/simulate.py --rate 5` |

`--rate 5` ≈ 5 orders/second — enough to pass the ~40-order minimum the
ML step needs in under a minute. Change it live to show the rate is
genuinely tunable. Terminals 2, 3, and 4/5 are independent consumers —
start them in any order. (Demoing the optional Kafka Connect path
instead? Skip terminals 2 and 3 and run
`scripts\start_kafka_connect.bat` / `./scripts/start_kafka_connect.sh`
in one terminal instead — never alongside the Python writers.)

- [ ] **D1. Verify data is flowing**
  - MySQL writer (terminal 2): should be printing one line per order
    (`→ wrote ... to MySQL`-style output) with no tracebacks.
  - HDFS writer (terminal 3): same, one line per order written to HDFS.
  - MySQL: `http://127.0.0.1:8000/api/orders/list/` (log in at
    `/accounts/login/` first, or via `/admin/` in the same browser —
    this endpoint requires authentication now, see step C4.5), or
    `SELECT COUNT(*) FROM api_order;` — these rows are written by
    `consumer/mysql_writer.py`, not by the Django view directly.
  - HDFS: `hdfs dfs -ls -R /urbanbrew/orders` (files land directly under
    `dt=YYYY-MM-DD/` — no extra topic-name subfolder, since
    `consumer/hdfs_writer.py` writes there directly; that's why
    `analytics_job.py` reads `.../orders/dt=*/*.json`)
  - Live Alerts: `http://127.0.0.1:8000/dashboard/` → "🔔 Live Alerts"
    (also requires login) — this one comes from `consumer/notifier.py`
  - Django Admin: `http://127.0.0.1:8000/admin/`

- [ ] **D2. Demonstrate the consumer-group rebalance**
  With terminals 4 and 5 both running, press **Ctrl+C in terminal 5**.
  Watch terminal 4 print "Partitions revoked" then "Partitions
  assigned" with ALL partitions — that's the rebalance. Restart
  terminal 5 to see it happen again in reverse. Optionally watch it
  live in a 7th terminal:
  ```bash
  # Linux/macOS
  ./scripts/check_consumer_group.sh
  # Windows
  scripts\check_consumer_group.bat
  ```
  (The same demo works with `mysql_writer.py` or `hdfs_writer.py`
  instead — pass their group name, e.g.
  `./scripts/check_consumer_group.sh urbanbrew-mysql-writer`.)

- [ ] **D3. Run the Spark analytics + MLlib job** (after ~40+ orders
  have flowed through; re-run any time to refresh the dashboard)
  ```bash
  # Linux/macOS
  $SPARK_HOME/bin/spark-submit \
    --packages com.mysql:mysql-connector-j:8.3.0 \
    spark_jobs/analytics_job.py

  # Windows
  %SPARK_HOME%\bin\spark-submit.cmd ^
    --packages com.mysql:mysql-connector-j:8.3.0 ^
    spark_jobs\analytics_job.py
  ```
  This also computes the `recommended_action` column — a simple
  rule-based suggestion derived from each order's risk probability, see
  `recommend_action()` in `spark_jobs/analytics_job.py` — alongside the
  risk prediction itself.

  Rather than re-run this by hand before every look at the dashboard,
  `scripts/run_spark_loop.sh` re-runs it automatically on a timer (see
  README.md §7.5). If your default Java is too new for Spark's bundled
  Hadoop libraries (see "If something breaks" below), point just that
  terminal at an older JDK first, e.g.:
  ```bash
  # Linux/macOS
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  export SPARK_HOME=/opt/bigdata/spark
  ./scripts/run_spark_loop.sh
  ```

- [ ] **D4. Open the dashboard**
  `http://127.0.0.1:8000/dashboard/` — sign in with the superuser
  account from step C4 if prompted, and you should then see
  operational summaries, Live Alerts, model accuracy metrics, and the
  Risk Watch panel (now with a "Recommended action" column), all
  populated.

- [ ] **D5. (Bonus, optional) Run the Hadoop MapReduce comparison job**
  Only if you're going for the 2 bonus marks in guidelines Section 3.6 —
  not required for the core 25 marks. Full explanation in README.md's
  "7b." section; short version:
  ```bash
  # Linux/macOS
  HADOOP_HOME=/opt/bigdata/hadoop ./scripts/run_mapreduce.sh
  # Windows
  set HADOOP_HOME=C:\bigdata\hadoop
  scripts\run_mapreduce.bat
  ```
  Compare its printed branch totals against MySQL's `branch_summary`
  table (written by the PySpark job) — they should match exactly, both
  read the same HDFS data. Fill in the runtime numbers in
  `mapreduce/COMPARISON.md` and be ready to explain the difference in
  words too (short version already written there: MapReduce pays a fixed
  per-job startup cost that dominates at this small a scale, while Spark
  keeps data in memory between steps).

---

## Phase E — Shutting everything down (end of session)

Order doesn't matter much here, but roughly reverse of how you started:
Ctrl+C the AutoScript, all four consumer terminals (mysql_writer,
hdfs_writer, and both notifier instances), and the Django server; then
Ctrl+C the Kafka broker terminal (no separate Zookeeper process in KRaft
mode — this is the only Kafka terminal you have);
`$HADOOP_HOME/sbin/stop-dfs.sh` (Linux/macOS) or Ctrl+C the HDFS
terminals (Windows). Nothing needs to be reformatted — Phase B2 (Kafka
storage format) and B4 (HDFS format) are both one-time-ever steps, not
per-session ones.

---

## If something breaks

Full troubleshooting tables live in `README.md` §8 — the most common
early blockers are:

- **Phase B never got started** — almost every later error ("connection
  refused", "no brokers available") traces back to Kafka, HDFS, or MySQL
  not actually being up. Re-check Phase B first.
- **Kafka broker won't start: `kafka.common.InconsistentClusterIdException`
  or complaints about `meta.properties`** — you re-ran step B2's
  `kafka-storage.sh format` against a data directory that was already
  formatted (or formatted with a different cluster ID than what's
  currently on disk). B2 is a one-time-ever step per machine — skip it
  on every run after the first. If you genuinely need a clean slate,
  stop the broker and delete the log directory named by `log.dirs` in
  `config/server.properties` (default `/tmp/kraft-combined-logs`), then
  redo B2 once.
- **`kafka-storage.sh: command not found` or no `bin/windows/kafka-storage.bat`**
  — you're on an old Kafka build without KRaft support. Download a
  current Kafka release (3.x or newer) from kafka.apache.org; this
  project doesn't use Zookeeper at all, so an old Zookeeper-only build
  won't work here.
- **`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`** —
  Python 3.12+ (common on a new install) breaks the old
  `kafka-python==2.0.2`. Run
  `python3 -m pip install --upgrade kafka-python` (or `py -m pip
  install --upgrade kafka-python` on Windows) inside your activated
  `.venv` to pick up the fixed `3.0.11` pin — see README.md §8 for why.
- **`winutils.exe` missing (Windows)** — HDFS commands fail cryptically
  without it. See Phase A's Hadoop install step.
- **`consumer/hdfs_writer.py` can't connect** — it uses WebHDFS (HTTP,
  default port `9870`), not the `hdfs://` RPC port. Check
  `HDFS_WEBHDFS_URL` in `.env` and that the NameNode's web UI loads in
  a browser.
- **`consumer/hdfs_writer.py` fails with `Permission denied: user=hadoop,
  access=WRITE`** — you skipped the `chmod -R 777` in step C6. Run it
  now: `hdfs dfs -chmod -R 777 /urbanbrew/orders` (Windows:
  `%HADOOP_HOME%\bin\hdfs dfs -chmod -R 777 /urbanbrew/orders`) — no
  need to redo anything else.
- **Forgot to activate the virtual environment in a new terminal** —
  every Python command in Phases C/D needs it (`.venv\Scripts\activate`
  / `source .venv/bin/activate`) first.
- **`/dashboard/` redirects to a sign-in page** — not a bug, the
  dashboard now requires login (step C4.5). Sign in with the
  superuser account from step C4.
- **AutoScript exits immediately with `Missing required setting
  'AUTOSCRIPT_API_TOKEN'`, or prints `API Error 401`/`403` for every
  order** — step C4.5 wasn't done (or the token in `.env` doesn't
  match): run `python manage.py drf_create_token autoscript_service`
  again and copy the token it prints into
  `AUTOSCRIPT_API_TOKEN=` in `.env`.
- **`spark-submit` dies instantly with `UnsupportedOperationException:
  getSubject is not supported`** — your default Java is too new for
  Spark 3.5's bundled Hadoop libraries (seen on Java 21+, especially
  23/24). Point just that terminal at a Java 17 install: on Linux,
  `ls /usr/lib/jvm/` to see what's there, then
  `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (path varies —
  or `conda install -c conda-forge openjdk=17 -y && export
  JAVA_HOME=$CONDA_PREFIX` if you'd rather not use `sudo`); on macOS,
  `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` (install with
  `brew install openjdk@17` first if none is listed) — then re-run the
  same `spark-submit` command. See README.md §8 for the full
  explanation.
- **A consumer (`mysql_writer.py` / `hdfs_writer.py` / `notifier.py`)
  crashes with `KafkaTimeoutError` after a "Partitions revoked" line**
  — almost always means you've left the autoscript running way past
  what the demo needs (tens of thousands of orders puts real load on
  Kafka/HDFS). **Stop the autoscript** — 40 orders is the real minimum
  — then just re-run the consumer that crashed; at-least-once delivery
  means nothing is lost, and `hdfs_writer.py` now safely tolerates
  being restarted this way. See README.md §8 for the full explanation.