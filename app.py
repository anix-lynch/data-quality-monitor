#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Streamlit Web App

🎯 PURPOSE: Interactive web interface for real-time data quality monitoring with comprehensive visualization and automated reporting
📊 FEATURES: Live quality metrics dashboard, interactive data exploration, automated report generation, health monitoring, drift analysis
🏗️ ARCHITECTURE: Streamlit frontend → DataQualityMonitor engine → Pandas data processing → Automated HTML/JSON reports
⚡ STATUS: Production web application with responsive UI, real-time updates, comprehensive error handling, Docker-ready deployment
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

from src.data_quality_monitor import DataQualityMonitor

# Page configuration
st.set_page_config(
    page_title="Data Quality Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin: 10px 0;
    }
    .success-metric { border-left-color: #28a745; }
    .warning-metric { border-left-color: #ffc107; }
    .error-metric { border-left-color: #dc3545; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f8f9fa;
        border-radius: 4px 4px 0 0;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1f77b4;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

def load_data():
    """Load sample datasets."""
    data_dir = Path("data")

    # Load datasets
    try:
        messy_df = pd.read_csv(data_dir / "messy_input.csv")
        historical_df = pd.read_csv(data_dir / "historical_reference.csv")

        # Load schema
        with open(data_dir / "schema.json", 'r') as f:
            schema = json.load(f)

        return messy_df, historical_df, schema
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

def create_metric_card(title, value, subtitle="", status="neutral"):
    """Create a styled metric card."""
    status_class = {
        "success": "success-metric",
        "warning": "warning-metric",
        "error": "error-metric",
        "neutral": ""
    }.get(status, "")

    st.markdown(f"""
    <div class="metric-card {status_class}">
        <h3 style="margin: 0; color: #1f77b4;">{title}</h3>
        <h2 style="margin: 5px 0; color: #333;">{value}</h2>
        <p style="margin: 0; color: #666; font-size: 0.9em;">{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)

def main():
    """Main Streamlit application."""
    st.title("📊 Data Quality & Monitoring Dashboard")
    st.markdown("**Real-time monitoring for ML training datasets**")

    # Load data
    messy_df, historical_df, schema = load_data()

    if messy_df is None:
        st.error("Failed to load data. Please ensure data files are present.")
        return

    # Initialize monitor
    try:
        monitor = DataQualityMonitor(
            schema_path="data/schema.json",
            reference_data_path="data/historical_reference.csv",
            reports_dir="reports"
        )
    except Exception as e:
        st.error(f"Failed to initialize monitor: {e}")
        return

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Controls")

        if st.button("🔄 Generate Fresh Report", type="primary"):
            with st.spinner("Generating quality report..."):
                report = monitor.generate_report(messy_df)
                st.success("Report generated successfully!")
                st.rerun()

        st.divider()
        st.subheader("📈 Dataset Info")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Rows", f"{len(messy_df):,}")
        with col2:
            st.metric("Columns", len(messy_df.columns))

        st.subheader("🎯 Schema Fields")
        for field_name, field_config in schema['fields'].items():
            required = "✅" if field_config.get('required', False) else "❌"
            st.write(f"{required} {field_name}")

    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Data Explorer", "📋 Quality Report", "📁 Reports"])

    with tab1:
        # Overview Dashboard
        st.header("Dashboard Overview")

        # Generate report for metrics
        report = monitor.generate_report(messy_df)
        summary = report['summary']

        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            score = summary['overall_quality_score']
            status = "success" if score >= 80 else "warning" if score >= 60 else "error"
            create_metric_card(
                "Quality Score",
                ".1f",
                "Overall assessment",
                status
            )

        with col2:
            create_metric_card(
                "Critical Issues",
                summary['critical_issues'],
                "Errors found",
                "error" if summary['critical_issues'] > 0 else "success"
            )

        with col3:
            create_metric_card(
                "Warnings",
                summary['warnings'],
                "Potential issues",
                "warning" if summary['warnings'] > 0 else "success"
            )

        with col4:
            create_metric_card(
                "Completeness",
                ".1f",
                "Data completeness rate",
                "success" if summary['completeness_rate'] >= 95 else "warning"
            )

        # Charts row
        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 Data Completeness by Column")

            # Completeness chart
            completeness_data = report['data_completeness']['column_completeness']
            completeness_df = pd.DataFrame([
                {
                    'Column': col,
                    'Completeness': stats['completeness_rate'],
                    'Null Count': stats['null_count']
                }
                for col, stats in completeness_data.items()
            ])

            fig = px.bar(
                completeness_df,
                x='Column',
                y='Completeness',
                title="Column Completeness (%)",
                color='Completeness',
                color_continuous_scale=['red', 'yellow', 'green']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🚨 Issues Summary")

            # Issues breakdown
            issues_data = {
                'Type': ['Schema Errors', 'Schema Warnings', 'Anomaly Score'],
                'Count': [
                    summary['critical_issues'],
                    summary['warnings'],
                    summary.get('anomaly_score', 0)
                ],
                'Color': ['red', 'orange', 'blue']
            }

            if any(issues_data['Count']):
                fig = px.pie(
                    values=issues_data['Count'],
                    names=issues_data['Type'],
                    title="Issues Distribution",
                    color_discrete_sequence=['#dc3545', '#ffc107', '#1f77b4']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("🎉 No issues detected!")

    with tab2:
        # Data Explorer
        st.header("Data Explorer")

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("📋 Dataset Preview")
            st.dataframe(messy_df.head(50), use_container_width=True)

        with col2:
            st.subheader("📊 Column Statistics")

            selected_column = st.selectbox(
                "Select column to analyze:",
                messy_df.columns.tolist()
            )

            if selected_column:
                col_data = messy_df[selected_column]

                st.write(f"**Data Type:** {col_data.dtype}")
                st.write(f"**Non-null Count:** {col_data.count()}/{len(col_data)}")

                if col_data.dtype in ['int64', 'float64']:
                    st.write(".2f")
                    st.write(".2f")
                    st.write(".2f")

                    # Distribution plot
                    fig = px.histogram(
                        col_data.dropna(),
                        title=f"Distribution of {selected_column}",
                        marginal="box"
                    )
                    st.plotly_chart(fig, use_container_width=True)

                elif col_data.dtype == 'object':
                    # Value counts for categorical
                    value_counts = col_data.value_counts().head(10)
                    fig = px.bar(
                        x=value_counts.index,
                        y=value_counts.values,
                        title=f"Top values in {selected_column}"
                    )
                    st.plotly_chart(fig, use_container_width=True)

    with tab3:
        # Quality Report
        st.header("Quality Report Details")

        # Schema validation results
        st.subheader("🔍 Schema Validation")

        validation = report['schema_validation']

        if validation['errors']:
            st.error("**Critical Issues Found:**")
            for error in validation['errors']:
                st.write(f"❌ {error}")

        if validation['warnings']:
            st.warning("**Warnings:**")
            for warning in validation['warnings']:
                st.write(f"⚠️ {warning}")

        if not validation['errors'] and not validation['warnings']:
            st.success("✅ Schema validation passed!")

        # Anomaly detection
        st.subheader("📈 Anomaly Detection")

        anomalies = report['anomaly_detection']

        col1, col2, col3 = st.columns(3)

        with col1:
            shift_count = anomalies['distribution_shifts']['shift_count']
            status = "error" if shift_count > 2 else "warning" if shift_count > 0 else "success"
            st.metric("Distribution Shifts", shift_count)

        with col2:
            spike_count = anomalies['missing_value_spikes']['spike_count']
            status = "error" if spike_count > 1 else "warning" if spike_count > 0 else "success"
            st.metric("Missing Value Spikes", spike_count)

        with col3:
            outlier_cols = anomalies['outliers']['columns_with_outliers']
            status = "error" if outlier_cols > 2 else "warning" if outlier_cols > 0 else "success"
            st.metric("Columns with Outliers", outlier_cols)

        # Detailed anomaly information
        with st.expander("View Detailed Anomaly Report"):
            st.json(anomalies)

    with tab4:
        # Reports Management
        st.header("Reports Management")

        reports_dir = Path("reports")
        if reports_dir.exists():
            report_files = list(reports_dir.glob("report_*.json"))

            if report_files:
                st.subheader("📄 Available Reports")

                # Sort by modification time (newest first)
                report_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

                for report_file in report_files[:5]:  # Show latest 5
                    mod_time = datetime.fromtimestamp(report_file.stat().st_mtime)

                    col1, col2, col3 = st.columns([3, 2, 1])

                    with col1:
                        st.write(f"**{report_file.name}**")
                        st.caption(f"Generated: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

                    with col2:
                        file_size = report_file.stat().st_size / 1024
                        st.caption(".1f")

                    with col3:
                        if st.button("📥 Download", key=f"download_{report_file.name}"):
                            with open(report_file, 'r') as f:
                                report_content = f.read()
                            st.download_button(
                                label="Download JSON",
                                data=report_content,
                                file_name=report_file.name,
                                mime="application/json",
                                key=f"dl_{report_file.name}"
                            )

                # HTML reports
                html_files = list(reports_dir.glob("report_*.html"))
                if html_files:
                    st.subheader("🌐 HTML Reports")
                    html_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

                    for html_file in html_files[:3]:
                        mod_time = datetime.fromtimestamp(html_file.stat().st_mtime)
                        st.write(f"📄 {html_file.name} - {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

                        if st.button(f"🔗 Open {html_file.name}", key=f"open_{html_file.name}"):
                            with open(html_file, 'r') as f:
                                html_content = f.read()
                            st.components.v1.html(html_content, height=600, scrolling=True)
            else:
                st.info("No reports found. Click 'Generate Fresh Report' to create one.")
        else:
            st.error("Reports directory not found.")

if __name__ == "__main__":
    main()
