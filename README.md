# Ingestion Module

**Overview**

The ingestion module is the first step in the sim broker data pipeline, responsible for loading raw source files into a standardized, query-ready format. It prepares both event data (e.g., orders, trades) and reference data (e.g., dimensions) for downstream reporting and analytics.

**Core Responsibilities**

- **File Reading:** Supports reading JSON Lines (JSONL) and CSV formats from a configurable input location.
- **Data Parsing:** Transforms raw files into Spark DataFrames, applying basic validation and schema inference.
- **Storage Writes:** Outputs data to a structured directory layout—using Parquet by default for development and Delta Lake for production streaming scenarios.
- **Logging & Monitoring:** Tracks row counts and logs progress at each stage to simplify debugging and auditing.

**Key Dependencies & Choices for MVP**

- **Apache Spark (3.5.x):** Provides a scalable engine for batch and future streaming workloads.
- **Delta Lake (v3.1.0):** Enables ACID transactions and efficient streaming reads; writes can be toggled via configuration.
- **Parquet Fallback:** During development, we default to Parquet files to avoid complex Delta catalog setup; switching to Delta is controlled by flags.
- **Hadoop & WinUtils:** On Windows, compatible Hadoop binaries (winutils.exe) and disabling native IO are required to prevent filesystem errors.
- **Configuration-Driven:** All paths and feature flags (e.g., storage format, extension loading) are managed via a single YAML file to maximize flexibility.
- **Testing with Pytest:** Sample files and cleanup routines ensure each ingestion function behaves as expected in isolation.

**MVP Design Choices**

1. **Parquet by Default:** Simplifies local development and CI environments without Delta catalog configurations.
2. **Placeholder Dimensions:** Automates pipeline continuity by creating minimal placeholder tables when reference files are missing.
3. **Lightweight Logging:** Focuses on row-level metrics and error conditions to keep overhead low in the MVP.

**Future Enhancements**

- Structured streaming support for real-time data ingestion.
- Advanced schema evolution and merge strategies in Delta Lake.
- Integration with a centralized metadata/catalog service.

This document outlines the high-level architecture and choices for the ingestion module MVP, minimizing maintenance overhead and preparing for future scaling.

