# AIS Data Pipeline & Visualization 🚢📊

## Overview
This project demonstrates a real-time AIS (Automatic Identification System) data pipeline:
- Ingest AIS messages via Kafka
- Process and normalize with FastAPI
- Store structured data in a database
- Visualize vessel positions and detections with Dash/Plotly

## Architecture Diagram
```mermaid
flowchart LR
    A[AIS Data Source] --> B[Kafka Producer]
    B --> C[Kafka Broker]
    C --> D[Kafka Consumer]
    D --> E[FastAPI]
    E --> F[Dash/Matplotlib Visualization]
    E --> G[Database]
    G --> H[NextJS + Leaflet]