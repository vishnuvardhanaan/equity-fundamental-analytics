# 📊 Equity Fundamental Analytics

An end-to-end, explainable equity analytics platform for NSE-listed companies built using **SQL, Python, and Power BI**.

This project implements a production-style analytics engineering pipeline that standardizes financial data, computes advanced metrics, applies rule-based scoring, and delivers fully explainable insights through an interactive dashboard.

---

## 🎯 Problem Statement

Fundamental analysis at scale suffers from:

- Inconsistent metric definitions  
- Lack of scoring transparency  
- Fragmented data sources  
- Black-box evaluation logic  

Analysts require a structured, transparent, and automated framework to evaluate company financial health consistently.

This project builds a modular, auditable, and scalable analytics system following modern data engineering principles.

---

## 🧠 System Overview

The platform:

- Ingests, cleans, and standardizes financial statement data  
- Computes base and derived financial metrics  
- Applies multi-layer rule-based scoring engines  
- Supports macro-phase-aware evaluation  
- Produces explainable score outputs  
- Delivers insights via an interactive Power BI dashboard  

Design philosophy: Traceable. Modular. Explainable. Production-ready.

---

## 🏗️ Architecture

```
Raw Financial Data
        ↓
     Silver Layer
 (Clean & Standardize)
        ↓
      Gold Layer
 (Metrics + Scoring Engines)
        ↓
 Explainability Views
        ↓
 Power BI Dashboard
```

Architecture follows a Bronze / Silver / Gold medallion design pattern for scalability and auditability.

---

## ✨ Key Features

### 🔹 Data Engineering

- Structured Silver cleaning pipeline  
- Modular Python orchestration  
- SQLite analytical warehouse  
- SQL-first metric modeling  

### 🔹 Financial Intelligence

- Base metrics engine  
- Derived metrics engine  
- Multi-year trend computation  
- Sector-aware relative scoring  

### 🔹 Scoring Framework

- Absolute rules engine  
- Relative rules engine  
- Trend rules engine  
- Macro-phase configurable weights  
- Fully explainable score outputs  

### 🔹 Visualization

- Multi-page Power BI dashboard  
- Score transparency views  
- Historical trend analysis  
- Risk and stability breakdown  

---

## 🛠️ Tech Stack

### Languages & Processing
- Python  
- Pandas  
- SQL (SQLite)  

### Analytics Engineering
- SQL-first metric modeling  
- Vectorized rule evaluation  

### Architecture
- Medallion (Bronze / Silver / Gold) design  
- Modular orchestration  

### Visualization
- Power BI  

---

## 📂 Repository Structure

```
├── data/
│   ├── gold/        # Gold warehouse + dashboard assets
│   └── silver/      # Silver cleaned database
│
├── logs/            # Pipeline execution logs
│
├── notebooks/
│   ├── exploratory_gold/
│   └── exploratory_silver/
│
├── sandbox/         # Experimental work and prototypes
│
└── src/
    ├── common/      # Shared utilities and configs
    ├── silver/      # Data cleaning pipeline
    └── gold/        # Metrics, scoring, explainability
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository

```bash
git clone https://github.com/vishnuvardhanaan/equity-fundamental-analytics.git
cd equity-fundamental-analytics
```

### 2️⃣ Create Virtual Environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Verify Database Paths

Check and update if required:

```
src/common/paths.py
```

---

## ▶️ Running the Pipeline

### Run Silver Layer

```bash
python -m src.silver.app
```

### Run Gold Layer

```bash
python -m src.gold.app
```

---

## 📊 Power BI Dashboard

The dashboard includes:

- Overview  
- Company Summary  
- Financial Health  
- Profitability & Growth  
- Cash Flow & Capital Allocation  
- Stability & Risk  
- Score Explainability  
- Historic Trends  
- Remarks  

Dashboard file:

```
data/gold/nse_equity_universe_dashboard.pbix
```

---

## 🔍 Explainability Philosophy

Every score in this system is:

- Rule-driven  
- Transparent  
- Auditable  
- Reproducible  

The objective is to eliminate black-box fundamental scoring and make investment analysis defensible and traceable.

---

## 🚧 Future Enhancements

Planned improvements:

- Automated macro-regime detection  
- Sector-adaptive weighting  
- Performance optimization for large universes  
- Cloud warehouse migration  
- API layer for external consumption  
- ML-based anomaly detection  

---

## 💼 Portfolio Positioning

This project demonstrates expertise in:

- Analytics Engineering  
- Financial Data Modeling  
- SQL-First Metric Design  
- Explainable Scoring Systems  
- End-to-End Data Pipelines  
- Power BI Dashboarding  

---

## 📜 License

MIT License  

Copyright (c) 2026 Vishnu Vardhanaan S  

Permission is hereby granted, free of charge, to any person obtaining a copy  
of this software and associated documentation files (the "Software"), to deal  
in the Software without restriction, including without limitation the rights  
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell  
copies of the Software, and to permit persons to whom the Software is  
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all  
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR  
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE  
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER  
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,  
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE  
SOFTWARE.
