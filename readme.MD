# ShopZada Data Warehouse Project

## Overview

ShopZada is a fast-growing global e-commerce platform managing over half a million customer orders and more than two million product items across diverse categories. As the company expands into international markets, operational complexity has increased, with multiple departments—Business, Customer Management, Enterprise, Marketing, and Operations—managing data independently. Each department uses different formats, structures, and storage methods, resulting in fragmented data, inconsistent reporting, and slower decision-making.

To address these challenges, the ShopZada Data Warehouse project establishes a unified, scalable, and reliable data management platform. This system consolidates departmental data into a centralized warehouse, supporting standardized reporting, interactive analytics, and informed strategic decision-making.

---

## Assumptions

This project is developed under the following assumptions:

- All departmental datasets, regardless of format or structure, are accessible and can be integrated into a centralized data warehouse.
- Necessary permissions, credentials, and access rights for data extraction and transfer are in place across all departments.
- Source data quality is sufficient for transformation, with minor inconsistencies handled during ETL through standardization, enrichment, and validation.
- The workflow orchestration platform can reliably manage task scheduling, execution monitoring, and error handling.
- The data warehouse provides a unified and consistent foundation for reporting, analytics, and decision-making to support organizational growth.

---

## Data Architectural Design

The ShopZada data warehouse architecture follows the **Kimball dimensional modeling approach**, organizing data around key business processes. Departmental data marts are integrated through conformed dimensions, enabling a cohesive enterprise view.

Key design principles include:

- **Centralized and Scalable Warehouse**  
  Consolidates fragmented departmental data into a modular and extensible repository.

- **Dimensional Modeling**  
  Business processes are represented through fact tables and shared dimension tables.

- **Efficient ETL Pipelines**  
  Supports streamlined extraction, transformation, and loading of heterogeneous data sources.

- **Unified Reporting**  
  Ensures consistent and reliable cross-departmental analytics.

---

## Technology Stack

The project uses a modern, fully containerized technology stack to ensure scalability, reproducibility, and maintainability.

### A. Source Systems

- **Departments:** Business, Customer Management, Enterprise, Marketing, Operations  
- **Formats:** CSV, XLSX, JSON, Pickle, Parquet, HTML  
- **Data Types:** Transactional data, product data, campaign data, merchant records, and customer information

---

### B. ETL Layer

- **Workflow Orchestration:** Kestra  
- **Transformation Engine:** Python with Pandas (in-memory processing)
- **ETL Characteristics:**
  - No intermediate staging tables
  - Direct loading into fact and dimension tables
  - Embedded data quality checks (null handling, format standardization, validation)

Kestra manages task dependencies, scheduling, logging, monitoring, and error handling to ensure reliable pipeline execution.

---

### C. Data Storage Layer

- **Database:** PostgreSQL  
- **Schema Design:** Kimball star schema  
- **Optimizations:**
  - Indexing
  - Constraints
  - Optimized data types for performance and integrity

PostgreSQL serves as the centralized data warehouse supporting analytical workloads.

---

### D. Presentation Layer

Due to limited native Power BI support on macOS, a web-based analytics solution was adopted.

- **Frontend:** HTML, CSS, JavaScript  
- **Visualization Library:** Chart.js  
- **Backend API:** Flask  
- **Data Access:** Optimized SQL queries and predefined aggregations returning JSON responses

The dashboard enables stakeholders to monitor KPIs, analyze campaign effectiveness, and explore customer behavior interactively.

---

## Dockerized Environment Overview

The project environment is fully containerized to ensure reproducibility and ease of deployment.

### Core Services

- **PostgreSQL**
  - Stores warehouse data, workflow metadata, and task results
  - Uses persistent volumes to maintain data integrity

- **Kestra**
  - Orchestrates ETL workflows
  - Executes task-specific containers
  - Mounts local directories for scripts, datasets, workspace files, and storage

### Infrastructure Features

- Docker networks for seamless inter-service communication  
- Health checks to ensure Kestra initializes only after PostgreSQL is ready  
- Persistent volumes for data durability

This setup provides a stable and maintainable operational foundation for data processing and workflow execution.

---

## Workflow Process and Orchestration

The end-to-end workflow ensures consistent and reliable data movement:

1. **Data Extraction**  
   Departmental datasets are accessed from heterogeneous file formats.

2. **Data Transformation**  
   Python and Pandas perform in-memory cleaning, standardization, column mapping, date key generation, and enrichment.

3. **Workflow Orchestration**  
   Kestra manages sequential execution, logging, monitoring, and error handling across ETL tasks.

4. **Data Loading**  
   Transformed data is loaded directly into PostgreSQL fact and dimension tables aligned with the Kimball model.

5. **Analytics and Reporting**  
   The Flask API retrieves data via optimized SQL queries and serves it to the web-based dashboard for visualization.

This orchestration framework guarantees reproducibility, accuracy, and timely availability of analytical insights.

---

## Project Benefits

- Centralized and standardized cross-departmental data
- Faster and more reliable reporting
- Improved analytical consistency and decision-making
- Scalable architecture supporting future growth
- Platform-independent, web-based analytics accessible across teams

---
