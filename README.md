# learn-airflow-dags

## 🚀 Apache Airflow DAGs: Learn to Orchestrate Your Data Workflows

Welcome to the `learn-airflow-dags` repository! This repository is your essential companion for mastering Apache Airflow DAGs (Directed Acyclic Graphs), from fundamental concepts to advanced patterns. It hosts all the code examples, practical demonstrations, and starter templates used in our comprehensive YouTube video series and detailed documentation website.

Whether you're new to data orchestration or looking to deepen your Airflow knowledge, this repository provides hands-on code to accelerate your learning journey.

---

### ✨ What You'll Find Here

This repository holds all Airflow **code** — DAGs and supporting plugins — in one place:

```
.
├── dags/                 # workflow definitions
│   ├── 01_basic/
│   └── google_fit_ingestion/
└── plugins/              # shared Python modules imported by DAGs
    └── google_fit/
```

Tutorial and project sections under `dags/`:

* **`dags/01_basic/`**: Get started with core Airflow concepts. This section contains:
    * Your very first "Hello World" DAG.
    * Examples demonstrating various operators (e.g., `BashOperator`, `PythonOperator`).
    * How to define task dependencies and control workflow flow.
    * ...and more foundational examples as the series progresses!
* **`dags/google_fit_ingestion/`**: Google Fit → Postgres pipeline example.
* **`dags/02_advanced`**: (Coming Soon!) Dive into more complex Airflow features, including XComs, branching, sensors, and dynamic DAG generation.
* **`dags/03_cicd`**: (Coming Soon!) Explore best practices for continuous integration and deployment of your Airflow DAGs.

---

### 🐍 Local Python environment

For running tests, `get_token.py`, or editing plugins outside Docker:

```bash
./scripts/setup_venv.sh
source .venv/bin/activate
pytest tests/ -v
```

**Test live Google Fit credentials** (after running `get_token.py`):

```bash
python scripts/test_fetch_steps.py          # last 7 days
python scripts/test_fetch_steps.py --days 30
```

Requires `scripts/token.json` from `get_token.py`.

Requires **Python 3.10+** (Airflow 3.x compatible). Dependencies live in `requirements.txt` (runtime) and `requirements-dev.txt` (tests).

---

### 📺 Watch the Tutorials

This code comes alive with our step-by-step video tutorials. Subscribe to our YouTube channel to follow along and see these DAGs in action!

➡️ **[Visit our YouTube Channel](https://youtube.com/@shantanukhond)**

---

### 📚 Read the Documentation

For detailed explanations, theory, troubleshooting tips, and written versions of our tutorials, explore our official documentation website. All code snippets are available there for easy copy-pasting.

➡️ **[Explore the Documentation Website](https://airflow.atwish.org)**

---

### 🛠️ Getting Started with the Code

1.  **Clone this repo into your Airflow project's `airflow-code/` folder** (see [learn-airflow](https://github.com/shantanukhond/learn-airflow)):
    ```bash
    git clone https://github.com/shantanukhond/learn-airflow-dags.git airflow-code
    ```
2.  **Ensure Airflow is Running:** Use the [learn-airflow](https://github.com/shantanukhond/learn-airflow) Docker Compose setup, which mounts `airflow-code/dags` and `airflow-code/plugins` from this repository.
3.  **Explore!** Open your Airflow UI and watch your DAGs appear.

---

### 🤝 Contributing

Contributions, feedback, and suggestions are welcome! If you find any issues, have ideas for new topics, or want to improve existing code/documentation, please open an issue or submit a pull request.

---

### 📄 License

This project is licensed under the MIT License.

---

Happy Orchestrating!