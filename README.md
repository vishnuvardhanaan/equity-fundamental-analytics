EQUITY FUNDAMENTAL ANALYTICS

An end-to-end, explainable equity analytics platform for NSE-listed companies built using SQL, Python, and Power BI.
This project implements a production-style analytics engineering pipeline that standardizes financial data, computes advanced metrics, applies rule-based scoring, and delivers fully explainable insights through an interactive dashboard.
________________________________________

🎯 Problem Statement

Fundamental analysis at scale suffers from:
•	Inconsistent metric definitions
•	Lack of scoring transparency
•	Fragmented across multiple data sources
•	Lacking explainability behind scores
Analysts need a structured, transparent, and automated framework to evaluate company financial health consistently. This project solves the problem by building a modular, auditable, and scalable analytics framework following modern data engineering principles.
________________________________________

🧠 System Overview

This project builds an end-to-end equity analytics pipeline that:
•	Ingests, cleans and standardizes raw financial statements
•	Computes base and derived financial metrics
•	Applies rule-based scoring engines
•	Supports macro-phase aware evaluation
•	Produces explainable score outputs
•	Visualizes insights through an interactive Power BI dashboard
________________________________________

🏗️ Architecture

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

Design philosophy: traceable, modular, and analytics-engineer friendly.
________________________________________

✨ Key Features

🔹 Data Engineering
•	Structured Silver cleaning pipeline
•	Modular Python orchestration
•	SQLite analytical warehouse
•	SQL-first metric modelling

🔹 Financial Intelligence
•	Base metrics engine
•	Derived metrics engine
•	Multi-year trend computation
•	Sector-aware relative scoring

🔹 Scoring Framework
•	Absolute rules engine
•	Relative rules engine
•	Trend rules engine
•	Macro-phase configurable weights
•	Fully explainable scoring outputs

🔹 Visualization
•	Multi-page Power BI dashboard
•	Score transparency views
•	Historical trend analysis
•	Risk and stability breakdown
________________________________________

🛠️ Tech Stack

Languages & Processing
•	Python
•	Pandas
•	SQL (SQLite)

Analytics Engineering
•	SQL (extensive metric modelling)
•	Vectorized rule evaluation

Architecture
•	Bronze / Silver / Gold medallion design
•	Modular orchestration

Visualization
•	Power BI
________________________________________

📂 Repository Structure
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
________________________________________

⚙️ Setup Instructions

1️⃣ Clone the repository

git clone https://github.com/vishnuvardhanaan/equity-fundamental-analytics.git
cd equity-fundamental-analytics
________________________________________

2️⃣ Create virtual environment

python -m venv .venv
.venv\Scripts\activate
________________________________________

3️⃣ Install dependencies
pip install -r requirements.txt
________________________________________

4️⃣ Verify database paths
Check and update if required:
src/common/paths.py
________________________________________

▶️ How to Run the Pipeline

Run Silver pipeline
python -m src.silver.app

Run Gold pipeline
python -m src.gold.app
________________________________________

📊 Power BI Dashboard

The dashboard includes:
•	Overview
•	Company Summary
•	Financial Health
•	Profitability & Growth
•	Cash Flow & Capital Allocation
•	Stability & Risk
•	Score Explainability
•	Historic Trends
•	Remarks

Dashboard file:
data/gold/nse_equity_universe_dashboard.pbix
________________________________________

🔍 Explainability Philosophy

Every score in this system is:
•	rule-driven
•	transparent
•	auditable
•	reproducible
The goal is to eliminate black-box fundamental scoring and make investment analysis defensible and traceable.
________________________________________

🚧 Future Enhancements

Planned improvements:
•	Automated macro-regime detection
•	Sector-adaptive weighting
•	Performance optimization for large universes
•	Cloud warehouse migration
•	API layer for external consumption
•	ML-based anomaly detection
________________________________________

💼 Portfolio Positioning

This project demonstrates capabilities in:
•	Analytics Engineering
•	Financial Data Modelling
•	SQL-First Metric Design
•	Explainable Scoring Systems
•	End-to-End Data Pipelines
•	Power BI Dashboarding
________________________________________

📜 License

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


