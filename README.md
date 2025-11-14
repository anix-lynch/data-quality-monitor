# Data Quality & Monitoring Dashboard

A comprehensive data quality monitoring system for ML training data that validates schema compliance, detects anomalies, and provides real-time monitoring through a FastAPI dashboard.

## Purpose

**Data Quality Monitoring for ML Training Data**

This system ensures the quality and reliability of training datasets by:
- Validating data against predefined schemas
- Detecting distribution shifts and anomalies
- Monitoring data freshness and completeness
- Providing automated quality reports
- Offering a REST API for integration with ML pipelines

## Features

### 🔍 Schema Validation
- Required column presence checks
- Data type validation (string, int, float, datetime)
- Constraint enforcement (ranges, allowed values, non-null requirements)

### 📊 Anomaly Detection
- Distribution shift detection using KS-test
- Missing value spike identification
- Outlier detection using z-score and IQR methods
- Cardinality change monitoring

### 📈 Quality Metrics
- Overall quality scoring (0-100)
- Data completeness percentages
- Anomaly severity levels (LOW/MEDIUM/HIGH)
- Historical trend analysis

### 🌐 REST API Dashboard
- Real-time quality reports generation
- Data freshness monitoring
- Drift summary endpoints
- Interactive API documentation

## Project Structure

```
/Users/anixlynch/dev/Takehome3_data_quality/
├── src/
│   ├── __init__.py
│   ├── data_quality_monitor.py    # Main monitoring class
│   ├── dashboard_api.py           # FastAPI application
│   └── utils/
│       ├── __init__.py
│       ├── validation_utils.py    # Data validation helpers
│       ├── schema_utils.py        # Schema validation logic
│       ├── drift_utils.py         # Anomaly detection
│       └── logging_utils.py       # Logging utilities
├── data/
│   ├── messy_input.csv           # Test dataset with quality issues
│   ├── schema.json               # Data schema definition
│   └── historical_reference.csv  # Clean reference dataset
├── tests/
│   └── test_dq_monitor.py        # Unit tests
├── reports/
│   ├── sample_report.json        # Generated quality reports
│   └── sample_report.html        # HTML report samples
├── docker/
│   └── Dockerfile                # Docker configuration
├── scripts/
│   ├── run_checks.sh            # Run quality checks script
│   ├── run_dashboard.sh         # Start dashboard script
│   └── generate_sample_data.py  # Generate test data
├── docker-compose.yml           # Docker Compose setup
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Quick Start

### 1. Generate Sample Data

```bash
# Generate synthetic datasets for testing
python scripts/generate_sample_data.py
```

This creates:
- `schema.json`: Field definitions and constraints
- `historical_reference.csv`: Clean reference dataset (1000 rows)
- `messy_input.csv`: Dataset with intentional quality issues (800 rows)

### 2. Run Quality Checks

```bash
# Run comprehensive quality validation
./scripts/run_checks.sh
```

This will:
- Validate the messy input data against the schema
- Compare against historical reference data for anomalies
- Generate JSON and HTML quality reports in `reports/`

### 3. Start the Dashboard API

```bash
# Start the FastAPI monitoring dashboard
./scripts/run_dashboard.sh
```

The API will be available at `http://localhost:8010`

## API Endpoints

### Health Check
```http
GET /health
```
Returns service health status and timestamp.

### Quality Report
```http
GET /quality_report?dataset_path=data/messy_input.csv
```
Generates a fresh comprehensive quality report. Returns JSON with validation results, anomaly detection, and quality metrics.

**Response Example:**
```json
{
  "report_metadata": {
    "generated_at": "2024-01-15T10:30:00",
    "dataset_rows": 800,
    "dataset_columns": 5
  },
  "summary": {
    "overall_quality_score": 67.5,
    "critical_issues": 2,
    "warnings": 3,
    "anomaly_score": 4,
    "completeness_rate": 92.5
  }
}
```

### Data Freshness
```http
GET /freshness
```
Returns freshness metrics based on file modification timestamps.

### Drift Summary
```http
GET /drift_summary
```
Returns data drift indicators and severity assessments.

### Interactive Documentation
```http
GET /docs
```
Interactive Swagger UI for testing all endpoints.

## Quality Metrics Implemented

### Schema Validation
- **Required Columns**: Ensures mandatory fields are present
- **Data Types**: Validates int, float, string, datetime types
- **Range Constraints**: Enforces min/max values for numeric fields
- **Non-null Requirements**: Checks required fields have no null values

### Anomaly Detection
- **Distribution Shifts**: Kolmogorov-Smirnov test for statistical differences
- **Missing Value Spikes**: Detects unusual increases in null values
- **Outliers**: Z-score (>3σ) and IQR-based outlier detection
- **Cardinality Changes**: Monitors unique value count changes

### Data Completeness
- **Column-level Completeness**: Percentage of non-null values per column
- **Overall Completeness**: Average completeness across all columns
- **Fully Complete Columns**: Count of columns with 100% completeness

## Sample Data Schema

```json
{
  "fields": {
    "user_id": {
      "dtype": "string",
      "required": true
    },
    "age": {
      "dtype": "int",
      "min": 0,
      "max": 120,
      "required": true
    },
    "country": {
      "dtype": "string",
      "required": false
    },
    "signup_date": {
      "dtype": "datetime",
      "required": true
    },
    "spend": {
      "dtype": "float",
      "min": 0,
      "required": false
    }
  }
}
```

## Docker Deployment

### Build and Run with Docker Compose

```bash
# Build and start the service
docker-compose up --build

# Run in background
docker-compose up -d --build
```

The API will be available at `http://localhost:8010`

### Docker Commands

```bash
# View logs
docker-compose logs dq_api

# Stop services
docker-compose down

# Rebuild after code changes
docker-compose up --build --force-recreate
```

## Development

### Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Run Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### Code Quality

The system includes comprehensive test coverage for:
- Schema validation edge cases
- Anomaly detection algorithms
- Report generation functionality
- API endpoint responses

## Limitations & Realistic Extensions

### Current Limitations
- Single dataset monitoring (no multi-dataset support)
- Basic statistical methods (could use more sophisticated ML-based approaches)
- No alerting system (email, Slack notifications)
- No historical trend storage (only current vs reference comparison)

### Production Extensions

#### 🔄 Advanced Monitoring
- **Airflow Integration**: Schedule automated quality checks in ML pipelines
- **Multi-dataset Support**: Monitor multiple training datasets simultaneously
- **Real-time Streaming**: Monitor data quality for streaming data sources

#### 🚨 Alerting & Notification
- **Email/Slack Alerts**: Notify stakeholders of quality degradation
- **Severity-based Escalation**: Different alert levels for different issue types
- **Trend Analysis**: Monitor quality trends over time

#### 📊 Enhanced Analytics
- **Time-series Quality Metrics**: Track quality changes over time
- **Automated Remediation**: Suggest fixes for common data quality issues
- **ML-based Anomaly Detection**: Use unsupervised ML for better anomaly detection

#### 🏗️ Enterprise Features
- **Role-based Access Control**: Different permissions for viewers vs administrators
- **Audit Logging**: Track all quality checks and user actions
- **Data Lineage Integration**: Connect quality metrics to data pipeline stages

#### 🔧 Technical Improvements
- **Database Storage**: Persist quality metrics and historical reports
- **Caching Layer**: Cache expensive computations for faster API responses
- **Batch Processing**: Handle large datasets efficiently
- **Configurable Rules**: Allow custom validation rules via configuration

## Technology Stack

- **Python 3.11**: Core language
- **FastAPI**: High-performance REST API framework
- **Pandas**: Data manipulation and analysis
- **SciPy**: Statistical testing for anomaly detection
- **Jinja2**: HTML report templating
- **Docker**: Containerized deployment
- **Pytest**: Testing framework

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for new functionality
4. Ensure all tests pass (`pytest tests/`)
5. Commit changes (`git commit -m 'Add amazing feature'`)
6. Push to branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is open source and available under the MIT License.
