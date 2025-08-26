# 0.1.1
## Bug fixes and other changes
### Lazy loading
- Added lazy module loading for checks, notifiers, renderers, audit stores, and result stores modules, allowing users to import specific components without requiring all optional dependencies to be installed.

# 0.1.0
## Major features and improvements

### Core Framework
- **DataSentinelSession**: Central entry point for accessing all DataSentinel functionalities with session management and thread-safe operations
- **Validation Workflows**: Workflow runner system for orchestrating complex data validation processes including notification and results storage with `SimpleWorkflowRunner`
- **Data Asset Management**: Abstract data asset framework supporting multiple data backends with in-memory support.
- **Multi-Engine Support**: Native support for both Pandas and PySpark data processing engines
- **Thread-Safe Operations**: Concurrent session management with proper locking mechanisms
- **Modern Python Support**: Full compatibility with Python 3.10+ and Pydantic integration for type safety

### Audit Data Logging
- **Audit Store Manager**: Centralized audit logging system with pluggable storage backends
- **Database Audit Store**: SQLAlchemy-based audit logging for relational databases
- **Delta Table Audit Store**: Spark-based audit logging using Delta Lake format for big data scenarios

### Data Quality Validations
- **Validation Engine**: Complete validation framework with `DataValidation` class supporting multiple data quality checks
- **Abstract Check Framework**: Extensible check system supporting custom validation rules and strategies
- **Cuallee Integration**: Built-in support for Cuallee data quality checks for both Pandas and PySpark environments
- **Row-Level Result Checks**: Granular validation with dedicated strategies for Pandas and PySpark data processing
- **Failed Rows Dataset**: Specialized tracking and storage for validation failures across different data engines
- **Validation Results**: Comprehensive result tracking with timing metrics and detailed check outcomes

### Data Validation Result Stores
- **Result Store Manager**: Pluggable result storage system with multiple backend support
- **Delta Table Result Store**: Spark-based result storage using Delta Lake format for scalable data quality metrics
- **Flexible Storage Architecture**: Extensible system allowing custom result store implementations

### Notifications
- **Notification Manager**: Centralized notification orchestration and delivery system
- **Multi-Channel Notifiers**: Support for Email (SMTP) and Slack notifications with extensible notifier architecture
- **Template-Based Email Rendering**: Jinja2-powered email message rendering with HTML template support
- **Slack Message Rendering**: Dedicated Slack message formatting and rendering capabilities

## Bug fixes and other changes
