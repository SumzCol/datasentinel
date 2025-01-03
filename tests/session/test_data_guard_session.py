import threading

import pytest

from dataguard.notification.notifier.core import AbstractNotifierManager
from dataguard.session.data_guard_session import DataGuardSession
from dataguard.session.core import SessionAlreadyExistsError, SessionNotSpecifiedError
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.store.audit.core import AbstractAuditStoreManager
from dataguard.store.result.core import AbstractResultStoreManager
from dataguard.store.result.manager import ResultStoreManager
from dataguard.store.audit.manager import AuditStoreManager
from unittest.mock import Mock
from dataguard.validation.node.validation_node import ValidationNode
from dataguard.validation.datasource.core import AbstractDatasource
from dataguard.validation.runner.simple_runner import SimpleRunner


class TestDataGuardSession:
    def teardown_method(self):
        # Code to run after each test method
        DataGuardSession._active_sessions.clear()

    def test_create_session(self):
        session = DataGuardSession(name="test_session")
        assert session.name == "test_session"
        assert session in DataGuardSession._active_sessions.values()

    def test_create_duplicate_session(self):
        DataGuardSession(name="test_session")
        with pytest.raises(SessionAlreadyExistsError):
            DataGuardSession(name="test_session")

    def test_get_or_create_session(self):
        session = DataGuardSession.get_or_create(name="test_session")
        assert session.name == "test_session"
        assert session in DataGuardSession._active_sessions.values()

    def test_get_or_create_session_multithreaded(self):
        def get_or_create_session(name):
            DataGuardSession.get_or_create(name=name)

        threads = []
        for i in range(10):
            thread = threading.Thread(target=get_or_create_session, args=(f"session_{i}",))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(DataGuardSession._active_sessions) == 10

    def test_get_or_create_single_session_multithreaded(self):
        def get_or_create_session():
            DataGuardSession.get_or_create(name="single_session")

        threads = []
        for _ in range(10):
            thread = threading.Thread(target=get_or_create_session)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(DataGuardSession._active_sessions) == 1

    def test_session_availability_across_threads(self):
        session_name = "shared_session"

        def create_session():
            DataGuardSession(name=session_name)

        def access_session():
            session = DataGuardSession.get_or_create(name=session_name)
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
        assert len(DataGuardSession._active_sessions) == 1
        assert session_name in DataGuardSession._active_sessions

    def test_get_or_create_without_name(self):
        session1 = DataGuardSession.get_or_create()
        session2 = DataGuardSession.get_or_create()
        assert session1 == session2

    def test_get_or_create_multiple_sessions_without_name(self):
        DataGuardSession(name="session1")
        DataGuardSession(name="session2")
        with pytest.raises(SessionNotSpecifiedError):
            DataGuardSession.get_or_create()

    def test_managers_initialization(self):
        session = DataGuardSession(name="test_session")
        assert isinstance(session.notifier_manager, NotifierManager)
        assert isinstance(session.result_store_manager, ResultStoreManager)
        assert isinstance(session.audit_store_manager, AuditStoreManager)

    def test_create_session_with_custom_managers(self):
        # Create mock instances of the managers
        custom_notifier_manager = Mock(spec=AbstractNotifierManager)
        custom_audit_store_manager = Mock(spec=AbstractAuditStoreManager)
        custom_result_store_manager = Mock(spec=AbstractResultStoreManager)

        # Create a session with the custom managers
        session = DataGuardSession(
            name="custom_session",
            notifier_manager=custom_notifier_manager,
            audit_store_manager=custom_audit_store_manager,
            result_store_manager=custom_result_store_manager
        )

        # Verify that the session uses the provided instances
        assert session.notifier_manager is custom_notifier_manager
        assert session.audit_store_manager is custom_audit_store_manager
        assert session.result_store_manager is custom_result_store_manager

        # Verify that the session is correctly registered
        assert session.name == "custom_session"
        assert session in DataGuardSession._active_sessions.values()

    def test_get_or_create_session_with_custom_managers(self):
        # Create mock instances of the managers
        custom_notifier_manager = Mock(spec=AbstractNotifierManager)
        custom_audit_store_manager = Mock(spec=AbstractAuditStoreManager)
        custom_result_store_manager = Mock(spec=AbstractResultStoreManager)

        # Use get_or_create to create a session with the custom managers
        session = DataGuardSession.get_or_create(
            name="custom_session",
            notifier_manager=custom_notifier_manager,
            audit_store_manager=custom_audit_store_manager,
            result_store_manager=custom_result_store_manager
        )

        # Verify that the session uses the provided instances
        assert session.notifier_manager is custom_notifier_manager
        assert session.audit_store_manager is custom_audit_store_manager
        assert session.result_store_manager is custom_result_store_manager

        # Verify that the session is correctly registered
        assert session.name == "custom_session"
        assert session in DataGuardSession._active_sessions.values()

    def test_run_validation_node(self):
        session = DataGuardSession(name="test_session")
        validation_node = Mock(spec=ValidationNode)
        dataset = Mock(spec=AbstractDatasource)
        runner = Mock(spec=SimpleRunner)

        session.run_validation_node(validation_node, dataset, runner)
        runner.run.assert_called_once_with(
            validation_node=validation_node,
            datasource=dataset,
            notifier_manager=session.notifier_manager,
            result_store_manager=session.result_store_manager
        )
