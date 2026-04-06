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
- **Stripe**

### **Extract**

Data are extracted programmatically using custom [Extractor](../api/extractor.md) Python modules. These extractors leverage the official Python SDKs and APIs of each data source to retrieve raw data in standardized PyArrow tables.

### **Load**

Extracted data are loaded using custom [Loader](../api/loader.md) Python modules to Google Cloud Storage which serve as both the landing zone and the data lake. Files are organized by source and date partition, converted to Parquet format, then stored.

### **Data Warehouse**

**BigQuery** serves as the platform's data warehouse. Tables are organized into three datasets: `raw`, `staging`, and `marts`.

### **Transform**

Data transformations are performed in BigQuery using **SQLMesh**. Staging models clean and type-cast raw data. Mart models answer business questions around enrollment, inventory, ad attribution, and revenue.

### **Analytics**

Users analyze data and access insights through:

- **Looker Studio**
- **Streamlit**

### **Orchestration**

**Dagster** orchestrates the entire end-to-end data pipeline daily. It is self-hosted on a GCP `e2-micro` VM and managed via systemd. The Dagster daemon runs without a webserver — all interactions are via CLI.

### **Deployment**

**Terraform** provisions all GCP infrastructure including the VM, GCS buckets, IAM bindings, and IAP firewall rules.

**GitHub Actions** runs CI on every push and pull request, executing the full test suite across Python 3.11 and 3.12 on Ubuntu and macOS.
