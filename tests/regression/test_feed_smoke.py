from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from app.bootstrap import CoinbaseApplicationConfig
from app.feed_smoke import feed_smoke_payload
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from audit.ledger import AuditLedger
from config.assembly import CoinbaseBotConfig, CoinbaseWebSocketSourceConfig, FeedRuntimeConfig
from core.enums import CoinbaseWebSocketChannel, CoinbaseWebSocketEndpoint, EventType, ReadinessStatus, RuntimeComponent
from core.errors import ConfigError
from feeds.router import FeedMessage


class WaitingFeedSource:
    def __init__(self, source_id: str, messages: tuple[FeedMessage, ...]) -> None:
        self._source_id = source_id
        self._messages = messages

    @property
    def source_id(self) -> str:
        return self._source_id

    async def stream(self) -> AsyncIterator[FeedMessage]:
        for message in self._messages:
            await asyncio.sleep(0)
            yield message
        await asyncio.Event().wait()


def test_feed_smoke_runs_websocket_supervisor_without_runtime_or_orders(workspace_tmp_path):
    config = _config(workspace_tmp_path / "feed-smoke.jsonl")

    payload = asyncio.run(
        feed_smoke_payload(
            config,
            duration=timedelta(milliseconds=10),
            websocket_source_factory=lambda source_config: WaitingFeedSource(
                source_config.source_id,
                (
                    FeedMessage(
                        source_config.source_id,
                        "BIT-29MAY26-CDE:level2:1",
                        EventType.DATA_RECEIVED,
                        {"sequence": 1},
                    ),
                ),
            ),
        )
    )

    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["order_endpoint_called"] is False
    assert payload["runtime_tasks_started"] is False
    assert payload["strategy_tasks_started"] is False
    assert payload["websocket_started"] is True
    assert payload["event_counts"][EventType.DATA_ACCEPTED.value] == 1
    assert payload["event_counts"][EventType.DATA_DUPLICATE.value] == 1
    assert payload["event_counts"][EventType.FEED_CONNECTED.value] == 2

    records = AuditLedger(config.ledger_path).iter_records()
    event_types = [record.event_type for record in records]
    assert EventType.ACTION_REQUESTED not in event_types
    assert EventType.ACTION_EXECUTION_STARTED not in event_types
    assert EventType.RUNTIME_TASK_STARTED not in event_types
    assert event_types[0] == EventType.SYSTEM_STARTED
    assert event_types[-1] == EventType.SYSTEM_STOPPED
    assert records[0].payload["component"] == RuntimeComponent.FEED_SMOKE.value
    assert records[-1].payload["component"] == RuntimeComponent.FEED_SMOKE.value


def test_cli_feed_smoke_can_fail_on_attention(workspace_tmp_path, capsys):
    config = _config(workspace_tmp_path / "feed-smoke-attention.jsonl")
    config_file = workspace_tmp_path / "feed-smoke-config.json"
    config_file.write_text(
        json.dumps(
            {
                "ledger_path": config.ledger_path.as_posix(),
                "bot": {
                    "feed": {
                        "min_live_sources": 2,
                        "stale_after_seconds": 30,
                    },
                    "websocket_sources": [
                        {
                            "channels": ["level2"],
                            "endpoint": "MARKET_DATA",
                            "product_ids": ["BIT-29MAY26-CDE"],
                            "source_id": "coinbase-cfm-market-primary",
                        },
                        {
                            "channels": ["level2"],
                            "endpoint": "MARKET_DATA",
                            "product_ids": ["BIT-29MAY26-CDE"],
                            "source_id": "coinbase-cfm-market-secondary",
                        },
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_file),
                feed_smoke=True,
                feed_smoke_fail_on_attention=True,
                feed_smoke_seconds=0.01,
                ledger_path=None,
                max_cycles=99,
            ),
            websocket_source_factory=lambda source_config: WaitingFeedSource(source_config.source_id, ()),
        )
    )

    output = capsys.readouterr().out

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert f'"status": "{ReadinessStatus.ATTENTION_REQUIRED.value}"' in output
    assert "no_feed_data_or_heartbeat" in output


def test_feed_smoke_rejects_unresolved_placeholders_before_connecting(workspace_tmp_path):
    factory_called = False
    config = _config(
        workspace_tmp_path / "feed-smoke-placeholder.jsonl",
        product_id="REPLACE_WITH_CFM_PRODUCT_ID",
    )

    def source_factory(source_config):
        nonlocal factory_called
        factory_called = True
        return WaitingFeedSource(source_config.source_id, ())

    with pytest.raises(ConfigError, match="REPLACE_WITH_"):
        asyncio.run(
            feed_smoke_payload(
                config,
                duration=timedelta(milliseconds=10),
                websocket_source_factory=source_factory,
            )
        )

    assert factory_called is False
    records = AuditLedger(config.ledger_path).iter_records()
    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["stage"] == "feed_smoke"
    assert records[0].payload["placeholder_paths"] == [
        "$.bot.websocket_sources[0].product_ids[0]",
        "$.bot.websocket_sources[1].product_ids[0]",
    ]


def _config(path, *, product_id: str = "BIT-29MAY26-CDE") -> CoinbaseApplicationConfig:
    return CoinbaseApplicationConfig(
        ledger_path=path,
        bot=CoinbaseBotConfig(
            feed=FeedRuntimeConfig(min_live_sources=2),
            websocket_sources=(
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-cfm-market-primary",
                    channels=(CoinbaseWebSocketChannel.LEVEL2,),
                    endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                    product_ids=(product_id,),
                ),
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-cfm-market-secondary",
                    channels=(CoinbaseWebSocketChannel.LEVEL2,),
                    endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                    product_ids=(product_id,),
                ),
            ),
        ),
    )
