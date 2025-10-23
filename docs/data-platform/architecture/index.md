---
title: Data Platform Architecture
---

![architecture](../../assets/architecture.svg)

## **Overview**

American Beauty Institute's data platform runs entirely on Google Cloud Platform and powers all organizational data apps and services.

## **Components**

### **Data Sources**

The pipeline currently ingests data from the following sources:
- **Google Ads**
- **Google Analytics**
- **Google Sheets**
- **Facebook Ads**
- **PayPal**

### **Extract**

Data are extracted programmatically using custom [Extractor](../api/extractor.md) Python modules. These extractors leverage the official Python SDKs and APIs of each data source to retrieve raw data in standardized PyArrow tables.

### **Load**

Extracted data are loaded using custom [Loader](../api/loader.md) Python modules to Google Cloud Storage which serve as both the landing zone and the data lake. Files are organized by source and timestamp, converted to Parquet format, then stored.

### **Data Warehouse**

**BigQuery** serves as the platform's data warehouse. Tables are sourced directly from Google Cloud Storage, organized into raw, staging, and mart datasets.

### **Transform**

Data transformations are performed in BigQuery using **dbt**. The resulting data marts are the basis for every data app and services served on the platform.

### **Analytics**

Users and analyze data and access insights through:

- **Looker Studio**
- **Streamlit**

### **Orchestration**

**Prefect** orchestrates the entire end-to-end data pipeline daily. It is self-hosted and is managed internally by the engineering team.

### **Containerization & Deployment**

**Docker** containerizes all self-hosted components (e.g., Prefect, Streamlit) and deployed to Google Cloud Run.

**GitHub Actions** is used for the CI/CD orchestration, automating the build and deployment process of the platform.
