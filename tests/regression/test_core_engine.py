from __future__ import annotations

import pytest

from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, HookPoint
from hooks.registry import HookContext, HookRegistry


def test_core_audits_hook_failures_without_stopping_event_append(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    hooks = HookRegistry()

    def failing_hook(context: HookContext) -> None:
        raise ValueError(f"cannot process {context.event_type.value}")

    hooks.register(HookPoint.AFTER_APPEND, failing_hook)
    core = AuditCore(ledger, hooks=hooks)

    core.emit(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})

    records = ledger.iter_records()
    assert [record.event_type for record in records] == [EventType.ACTION_REQUESTED, EventType.ERROR]
    assert records[1].payload["exception_type"] == "ValueError"
    assert records[1].payload["error_category"] == ErrorCategory.HOOK.value
    assert records[1].payload["error_code"] == ErrorCode.HOOK_FAILED.value
    assert records[1].payload["hook_name"] == "failing_hook"


def test_before_append_hooks_cannot_mutate_appended_payload(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    hooks = HookRegistry()

    def mutating_hook(context: HookContext) -> None:
        context.payload["client_order_id"] = "mutated"

    hooks.register(HookPoint.BEFORE_APPEND, mutating_hook)
    core = AuditCore(ledger, hooks=hooks)

    core.emit(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})

    records = ledger.iter_records()
    assert [record.event_type for record in records] == [EventType.ACTION_REQUESTED, EventType.ERROR]
    assert records[0].payload["client_order_id"] == "order-1"
    assert records[1].payload["error_category"] == ErrorCategory.HOOK.value
    assert records[1].payload["hook_name"] == "mutating_hook"


def test_after_append_hooks_receive_immutable_record_snapshot(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    hooks = HookRegistry()

    def mutating_hook(context: HookContext) -> None:
        assert context.record is not None
        context.record.payload["client_order_id"] = "mutated"

    hooks.register(HookPoint.AFTER_APPEND, mutating_hook)
    core = AuditCore(ledger, hooks=hooks)

    record = core.emit(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})

    records = ledger.iter_records()
    assert record.payload["client_order_id"] == "order-1"
    assert [ledger_record.event_type for ledger_record in records] == [EventType.ACTION_REQUESTED, EventType.ERROR]
    assert records[0].payload["client_order_id"] == "order-1"
    assert records[1].payload["error_category"] == ErrorCategory.HOOK.value


def test_hook_registry_rejects_duplicate_or_non_callable_hooks():
    hooks = HookRegistry()

    def hook(context: HookContext) -> None:
        del context

    hooks.register(HookPoint.BEFORE_APPEND, hook)

    with pytest.raises(ValueError, match="already registered"):
        hooks.register(HookPoint.BEFORE_APPEND, hook)

    with pytest.raises(TypeError, match="callable"):
        hooks.register(HookPoint.AFTER_APPEND, "not-callable")
