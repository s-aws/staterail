from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.audit_archive_store import S3ArchiveStoreFactory, ledger_archive_store_from_config
from app.audit_anchor_store import S3AnchorStoreFactory, ledger_anchor_store_from_config
from app.config_fingerprint import application_config_startup_metadata
from actions.gateway import ActionCommand, ActionReceipt
from audit.anchors import LedgerAnchorStore
from audit.archives import LedgerArchiveStore
from audit.ledger import AuditLedger
from config.assembly import (
    CoinbaseBotConfig,
    CoinbaseRuntimeAssembly,
    WebSocketSourceFactory,
    assemble_coinbase_runtime,
    trigger_engine_from_config,
)
from core.clock import Clock
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, RuntimeTask
from core.errors import exception_to_error_payload
from exchanges.coinbase.advanced_trade_rest import HttpTransport
from exchanges.coinbase.auth import TokenProvider
from exchanges.coinbase.advanced_trade_ws import JwtFactory
from products.catalog import ProductCatalog
from products.tasks import ProductCatalogLookup
from risk.gate import RiskGate
from runtime.orchestrator import Sleep
from strategies import Strategy
from triggers.rules import TriggerEngine


DEFAULT_LEDGER_PATH = Path("data") / "audit.jsonl"


@dataclass(frozen=True)
class CoinbaseApplicationConfig:
    ledger_path: Path = DEFAULT_LEDGER_PATH
    bot: CoinbaseBotConfig = field(default_factory=CoinbaseBotConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_path, Path):
            raise TypeError("ledger_path must be a pathlib.Path")
        if not self.ledger_path.name:
            raise ValueError("ledger_path must include a file name")
        if not isinstance(self.bot, CoinbaseBotConfig):
            raise TypeError("bot must be a CoinbaseBotConfig")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "CoinbaseApplicationConfig":
        from app.config_loading import load_coinbase_application_config_from_mapping

        return load_coinbase_application_config_from_mapping(raw)

    @classmethod
    def from_json_file(cls, path: str | Path) -> "CoinbaseApplicationConfig":
        from app.config_loading import load_coinbase_application_config_from_json_file

        return load_coinbase_application_config_from_json_file(path)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "CoinbaseApplicationConfig":
        from app.config_loading import load_coinbase_application_config_from_env

        return load_coinbase_application_config_from_env(env)


@dataclass(frozen=True)
class CoinbaseApplicationRunResult:
    completed_cycles: int
    ledger_path: Path


@dataclass(frozen=True)
class CoinbaseApplication:
    assembly: CoinbaseRuntimeAssembly
    config: CoinbaseApplicationConfig
    core: AuditCore
    ledger: AuditLedger

    def stop(self) -> None:
        self.assembly.orchestrator.stop()
        if self.assembly.feed_supervisor is not None:
            self.assembly.feed_supervisor.stop()

    def submit_action(self, command: ActionCommand) -> ActionReceipt:
        return self.assembly.action_gateway.submit(command)

    def submit_and_execute_action(self, command: ActionCommand) -> ActionReceipt:
        return self.assembly.action_gateway.submit_and_execute(
            command,
            self.assembly.rest_executor,
        )

    async def run(
        self,
        *,
        max_cycles: int | None = None,
        stop_after_task: RuntimeTask | None = None,
        stop_after_task_count: int = 1,
    ) -> CoinbaseApplicationRunResult:
        try:
            if self.assembly.feed_supervisor is None:
                completed_cycles = await self.assembly.orchestrator.run_until(
                    max_cycles=max_cycles,
                    stop_after_task=stop_after_task,
                    stop_after_task_count=stop_after_task_count,
                )
            else:
                completed_cycles = await _run_with_feed_supervisor(
                    self.assembly,
                    max_cycles=max_cycles,
                    stop_after_task=stop_after_task,
                    stop_after_task_count=stop_after_task_count,
                )
            return CoinbaseApplicationRunResult(
                completed_cycles=completed_cycles,
                ledger_path=self.ledger.path,
            )
        except Exception as exc:
            self.core.emit(
                EventType.ERROR,
                exception_to_error_payload(
                    exc,
                    category=ErrorCategory.UNEXPECTED,
                    context={"stage": "application_run"},
                    error_code=ErrorCode.UNEXPECTED_EXCEPTION,
                ),
            )
            raise


def default_coinbase_application_config(*, ledger_path: Path = DEFAULT_LEDGER_PATH) -> CoinbaseApplicationConfig:
    return CoinbaseApplicationConfig(ledger_path=ledger_path)


def build_coinbase_application(
    config: CoinbaseApplicationConfig,
    *,
    audit_anchor_store: LedgerAnchorStore | None = None,
    audit_archive_store: LedgerArchiveStore | None = None,
    clock: Clock | None = None,
    jwt_factory: JwtFactory | None = None,
    product_catalog: ProductCatalog | None = None,
    product_catalog_client: ProductCatalogLookup | None = None,
    s3_anchor_store_factory: S3AnchorStoreFactory | None = None,
    s3_archive_store_factory: S3ArchiveStoreFactory | None = None,
    sleep: Sleep | None = None,
    risk_gate: RiskGate | None = None,
    token_provider: TokenProvider | None = None,
    transport: HttpTransport | None = None,
    triggers: TriggerEngine | None = None,
    strategies: tuple[Strategy, ...] = (),
    websocket_source_factory: WebSocketSourceFactory | None = None,
) -> CoinbaseApplication:
    if not isinstance(config, CoinbaseApplicationConfig):
        raise TypeError("config must be a CoinbaseApplicationConfig")

    ledger = AuditLedger(config.ledger_path, clock=clock)
    core: AuditCore | None = None
    try:
        if triggers is not None and config.bot.trigger_rules:
            raise ValueError("injected triggers cannot be combined with config trigger rules")
        resolved_triggers = triggers or trigger_engine_from_config(config.bot, clock=clock)
        core = AuditCore(ledger, triggers=resolved_triggers)
        resolved_audit_anchor_store = audit_anchor_store
        resolved_audit_archive_store = audit_archive_store
        if (
            resolved_audit_anchor_store is None
            and config.bot.audit_anchor_schedule.enabled
            and config.bot.audit_anchor_store is not None
        ):
            resolved_audit_anchor_store = ledger_anchor_store_from_config(
                config.bot.audit_anchor_store,
                s3_anchor_store_factory=s3_anchor_store_factory,
            )
        if (
            resolved_audit_archive_store is None
            and config.bot.audit_archive_schedule.enabled
            and config.bot.audit_archive_store is not None
        ):
            resolved_audit_archive_store = ledger_archive_store_from_config(
                config.bot.audit_archive_store,
                s3_archive_store_factory=s3_archive_store_factory,
            )
        assembly = assemble_coinbase_runtime(
            config=config.bot,
            core=core,
            audit_anchor_store=resolved_audit_anchor_store,
            audit_archive_store=resolved_audit_archive_store,
            clock=clock,
            jwt_factory=jwt_factory,
            product_catalog=product_catalog,
            product_catalog_client=product_catalog_client,
            sleep=sleep,
            startup_metadata=application_config_startup_metadata(config),
            risk_gate=risk_gate,
            strategies=strategies,
            token_provider=token_provider,
            transport=transport,
            websocket_source_factory=websocket_source_factory,
        )
    except Exception as exc:
        audit_core = core or AuditCore(ledger)
        audit_core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.CONFIG,
                context={"stage": "runtime_assembly"},
                error_code=ErrorCode.CONFIG_INVALID,
            ),
        )
        raise
    if core is None:
        raise RuntimeError("application core was not assembled")
    return CoinbaseApplication(
        assembly=assembly,
        config=config,
        core=core,
        ledger=ledger,
    )


async def _run_with_feed_supervisor(
    assembly: CoinbaseRuntimeAssembly,
    *,
    max_cycles: int | None,
    stop_after_task: RuntimeTask | None,
    stop_after_task_count: int,
) -> int:
    import asyncio

    feed_supervisor = assembly.feed_supervisor
    if feed_supervisor is None:
        return await assembly.orchestrator.run(max_cycles=max_cycles)

    feed_task = asyncio.create_task(feed_supervisor.run(), name="coinbase-feed-supervisor")
    try:
        await asyncio.sleep(0)
        return await assembly.orchestrator.run_until(
            max_cycles=max_cycles,
            stop_after_task=stop_after_task,
            stop_after_task_count=stop_after_task_count,
        )
    finally:
        feed_supervisor.stop()
        feed_task.cancel()
        await asyncio.gather(feed_task, return_exceptions=True)
