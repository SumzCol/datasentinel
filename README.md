# Data Sentinel

[![Python version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://pypi.org/project/datasentinel/)
[![PyPI version](https://badge.fury.io/py/datasentinel.svg)](https://pypi.org/project/datasentinel/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/SumzCol/datasentinel/blob/main/LICENSE)

## What is Data Sentinel?

Data Sentinel is a powerful framework for data quality validation and monitoring in production data pipelines. It provides a comprehensive suite of tools to ensure data accuracy, completeness, consistency, and integrity with native support for PySpark and Pandas dataframes.

Data Sentinel is designed with software engineering best practices to help you create robust, maintainable, and scalable data quality monitoring solutions.

## How do I install Data Sentinel?

To install Data Sentinel from the Python Package Index (PyPI) run:

```bash
pip install datasentinel
```

For specific use cases, you can install with optional dependencies:

```bash
# For PySpark focused data validation
pip install datasentinel[pyspark-checks]

# For Pandas focused data validation
pip install datasentinel[pandas-checks]

# For complete installation with all features
pip install datasentinel[all]
```

## What are the main features of Data Sentinel?

| Feature                       | What is this?                                                                                                                            |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| **Data Quality Validations**  | Execute comprehensive checks to ensure data accuracy, completeness, consistency, and integrity using industry-standard validation rules. |
| **Multi-DataFrame Support**   | Native support for PySpark and Pandas dataframes with consistent APIs.                                                                   |
| **Audit Stores**              | Comprehensive audit trail logging to multiple destinations including databases and Delta tables.                                         |
| **Notifications**             | Configurable notification system that alerts stakeholders when data quality issues are detected.                                         |
| **Validation Results Stores** | Store data quality validation results in various formats and destinations for reporting, analysis, and historical tracking.              |


## Why does Data Sentinel exist?

Data quality is critical for successful data-driven organizations, but implementing comprehensive data quality monitoring can be complex and time-consuming. Data Sentinel addresses this by providing:

- **Standardized approach** to data quality validation across different technologies.
- **Extensible architecture** that adapts to your specific requirements.
- **Best practices** built-in for audit logging, notifications, and result management.

## Can I contribute?

We welcome contributions to Data Sentinel! Whether you're fixing bugs, adding features, improving documentation, or sharing feedback, your contributions help make Data Sentinel better for everyone.

Check out our [contribution guidelines](CONTRIBUTING.md) to get started.

## Where can I learn more?

- **Documentation**: [Coming Soon] - Comprehensive guides and API reference
- **GitHub Repository**: [https://github.com/SumzCol/datasentinel](https://github.com/SumzCol/datasentinel)
- **Issue Tracker**: [https://github.com/SumzCol/datasentinel/issues](https://github.com/SumzCol/datasentinel/issues)

## License

Data Sentinel is licensed under the [Apache Software License (Apache 2.0)](LICENSE).
