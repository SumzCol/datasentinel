import threading
from unittest.mock import Mock, patch

import pytest

from datasentinel.notification.notifier.core import AbstractNotifierManager
from datasentinel.notification.notifier.manager import NotifierManager
from datasentinel.session.core import SessionAlreadyExistsError, SessionNotSpecifiedError
from datasentinel.session.data_sentinel_session import DataSentinelSession
from datasentinel.store.audit.core import AbstractAuditStoreManager
from datasentinel.store.audit.manager import AuditStoreManager
from datasentinel.store.result.core import AbstractResultStoreManager
from datasentinel.store.result.manager import ResultStoreManager
from datasentinel.validation.runner.core import AbstractWorkflowRunner
from datasentinel.validation.runner.simple_workflow_runner import SimpleWorkflowRunner
from datasentinel.validation.workflow import ValidationWorkflow


@pytest.mark.unit
@pytest.mark.session
class TestDataGuardSessionUnit:
    def teardown_method(self):
        # Code to run after each test method
        DataSentinelSession._active_sessions.clear()

    def test_create_session(self):
        session = DataSentinelSession(name="test_session")
        assert session.name == "test_session"
        assert session in DataSentinelSession._active_sessions.values()

    def test_create_duplicate_session(self):
        DataSentinelSession(name="test_session")
        with pytest.raises(SessionAlreadyExistsError):
            DataSentinelSession(name="test_session")

    def test_get_or_create_session(self):
        session = DataSentinelSession.get_or_create(name="test_session")
        assert session.name == "test_session"
        assert session in DataSentinelSession._active_sessions.values()

    def test_get_or_create_session_multithreaded(self):
        def get_or_create_session(name):
            DataSentinelSession.get_or_create(name=name)

        expected_active_session = 10
        threads = []
        for i in range(expected_active_session):
            thread = threading.Thread(target=get_or_create_session, args=(f"session_{i}",))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(DataSentinelSession._active_sessions) == expected_active_session

    def test_get_or_create_single_session_multithreaded(self):
        def get_or_create_session():
            DataSentinelSession.get_or_create(name="single_session")

        threads = []
        for _ in range(10):
            thread = threading.Thread(target=get_or_create_session)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(DataSentinelSession._active_sessions) == 1

    def test_session_availability_across_threads(self):
        session_name = "shared_session"

        def create_session():
            DataSentinelSession(name=session_name)

        def access_session():
            session = DataSentinelSession.get_or_create(name=session_name)
            assert session.name == session_name

        create_session()

        threads = []
        for _ in range(10):
            thread = threading.Thread(target=access_session)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify that the session is available and only one instance exists
        assert len(DataSentinelSession._active_sessions) == 1
        assert session_name in DataSentinelSession._active_sessions

    def test_get_or_create_without_name(self):
        session1 = DataSentinelSession.get_or_create()
        session2 = DataSentinelSession.get_or_create()
        assert session1 == session2

    def test_get_or_create_multiple_sessions_without_name(self):
        DataSentinelSession(name="session1")
        DataSentinelSession(name="session2")
        with pytest.raises(SessionNotSpecifiedError):
            DataSentinelSession.get_or_create()

    def test_managers_initialization(self):
        session = DataSentinelSession(name="test_session")
        assert isinstance(session.notifier_manager, NotifierManager)
        assert isinstance(session.result_store_manager, ResultStoreManager)
        assert isinstance(session.audit_store_manager, AuditStoreManager)

    def test_create_session_with_custom_managers(self):
        # Create mock instances of the managers
        custom_notifier_manager = Mock(spec=AbstractNotifierManager)
        custom_audit_store_manager = Mock(spec=AbstractAuditStoreManager)
        custom_result_store_manager = Mock(spec=AbstractResultStoreManager)

        # Create a session with the custom managers
        session = DataSentinelSession(
            name="custom_session",
            notifier_manager=custom_notifier_manager,
            audit_store_manager=custom_audit_store_manager,
            result_store_manager=custom_result_store_manager,
        )

        # Verify that the session uses the provided instances
        assert session.notifier_manager is custom_notifier_manager
        assert session.audit_store_manager is custom_audit_store_manager
        assert session.result_store_manager is custom_result_store_manager

        # Verify that the session is correctly registered
        assert session.name == "custom_session"
        assert session in DataSentinelSession._active_sessions.values()

    def test_get_or_create_session_with_custom_managers(self):
        # Create mock instances of the managers
        custom_notifier_manager = Mock(spec=AbstractNotifierManager)
        custom_audit_store_manager = Mock(spec=AbstractAuditStoreManager)
        custom_result_store_manager = Mock(spec=AbstractResultStoreManager)

        # Use get_or_create to create a session with the custom managers
        session = DataSentinelSession.get_or_create(
            name="custom_session",
            notifier_manager=custom_notifier_manager,
            audit_store_manager=custom_audit_store_manager,
            result_store_manager=custom_result_store_manager,
        )

        # Verify that the session uses the provided instances
        assert session.notifier_manager is custom_notifier_manager
        assert session.audit_store_manager is custom_audit_store_manager
        assert session.result_store_manager is custom_result_store_manager

        # Verify that the session is correctly registered
        assert session.name == "custom_session"
        assert session in DataSentinelSession._active_sessions.values()

    def test_run_data_validation_with_custom_runner_and_data_asset(self):
        session = DataSentinelSession(name="test_session")
        validation_workflow = Mock(spec=ValidationWorkflow)
        runner = Mock(spec=AbstractWorkflowRunner)
        session.run_validation_workflow(validation_workflow=validation_workflow, runner=runner)

        runner.run.assert_called_once_with(
            validation_workflow=validation_workflow,
            notifier_manager=session.notifier_manager,
            result_store_manager=session.result_store_manager,
        )

    @patch.object(SimpleWorkflowRunner, "run")
    def test_run_data_validation_without_custom_runner(self, mock_run):
        session = DataSentinelSession(name="test_session")
        validation_workflow = Mock(spec=ValidationWorkflow)
        session.run_validation_workflow(validation_workflow=validation_workflow)

        mock_run.assert_called_once_with(
            validation_workflow=validation_workflow,
            notifier_manager=session.notifier_manager,
            result_store_manager=session.result_store_manager,
        )
