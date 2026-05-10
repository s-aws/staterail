from __future__ import annotations

from enum import Enum


class StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class EventType(StringEnum):
    ACTION_ACCEPTED = "action.accepted"
    ACTION_EXECUTION_FAILED = "action.execution_failed"
    ACTION_EXECUTION_STARTED = "action.execution_started"
    ACTION_EXECUTED = "action.executed"
    ACTION_REJECTED = "action.rejected"
    ACTION_REQUESTED = "action.requested"
    AUDIT_ANCHOR_PUBLISHED = "audit.anchor_published"
    AUDIT_CHECKPOINT = "audit.checkpoint"
    AUDIT_LEDGER_ARCHIVED = "audit.ledger_archived"
    DATA_ACCEPTED = "data.accepted"
    DATA_DUPLICATE = "data.duplicate"
    DATA_OUT_OF_ORDER = "data.out_of_order"
    DATA_RECEIVED = "data.received"
    DATA_SEQUENCE_GAP = "data.sequence_gap"
    ERROR = "error"
    EXCHANGE_BALANCE_SNAPSHOT = "exchange.balance_snapshot"
    EXCHANGE_FILL = "exchange.fill"
    EXCHANGE_ORDER_UPDATE = "exchange.order_update"
    EXCHANGE_POSITION_SNAPSHOT = "exchange.position_snapshot"
    EXCHANGE_PRODUCT_SNAPSHOT = "exchange.product_snapshot"
    EXCHANGE_REQUEST_RETRY = "exchange.request_retry"
    FEED_CONNECTED = "feed.connected"
    FEED_DEGRADED = "feed.degraded"
    FEED_DISCONNECTED = "feed.disconnected"
    FEED_HEARTBEAT = "feed.heartbeat"
    FEED_RECONNECT_SCHEDULED = "feed.reconnect_scheduled"
    ORDER_LOGICAL_CREATED = "order.logical_created"
    ORDER_PLACEMENT_RECORDED = "order.placement_recorded"
    RECONCILIATION_DRIFT = "reconciliation.drift"
    RECONCILIATION_MISMATCH = "reconciliation.mismatch"
    RECONCILIATION_RECOVERY = "reconciliation.recovery"
    OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED = "operator.ledger_health_acknowledged"
    LIVE_PREFLIGHT_RESULT = "runtime.live_preflight_result"
    OPERATOR_CANARY_EVIDENCE_RESULT = "runtime.operator_canary_evidence_result"
    RUNTIME_HEALTH_CHECK_RESULT = "runtime.health_check_result"
    STRATEGY_SIMULATION_RESULT = "runtime.strategy_simulation_result"
    RUNTIME_TASK_COMPLETED = "runtime.task_completed"
    RUNTIME_TASK_STARTED = "runtime.task_started"
    STRATEGY_EVALUATION_COMPLETED = "strategy.evaluation_completed"
    STRATEGY_EVALUATION_FAILED = "strategy.evaluation_failed"
    STRATEGY_EVALUATION_STARTED = "strategy.evaluation_started"
    SYSTEM_STARTED = "system.started"
    SYSTEM_STOPPED = "system.stopped"
    TRIGGER_FIRED = "trigger.fired"


class ErrorCategory(StringEnum):
    ACTION_EXECUTOR = "action_executor"
    AUDIT_LEDGER = "audit_ledger"
    CONFIG = "config"
    EXCHANGE_AUTH = "exchange_auth"
    EXCHANGE_RATE_LIMIT = "exchange_rate_limit"
    EXCHANGE_TRANSPORT = "exchange_transport"
    FEED_SOURCE = "feed_source"
    HOOK = "hook"
    RECONCILIATION = "reconciliation"
    RUNTIME_TASK = "runtime_task"
    STRATEGY = "strategy"
    UNEXPECTED = "unexpected"


class ErrorCode(StringEnum):
    ACTION_EXECUTOR_CONTRACT_FAILED = "action_executor_contract_failed"
    ACTION_EXECUTOR_FAILED = "action_executor_failed"
    AUDIT_INTEGRITY_FAILED = "audit_integrity_failed"
    AUDIT_LEDGER_FAILED = "audit_ledger_failed"
    CONFIG_INVALID = "config_invalid"
    EXCHANGE_AUTH_FAILED = "exchange_auth_failed"
    EXCHANGE_RATE_LIMITED = "exchange_rate_limited"
    EXCHANGE_ORDER_UPDATE_INVALID = "exchange_order_update_invalid"
    EXCHANGE_STATE_SNAPSHOT_INVALID = "exchange_state_snapshot_invalid"
    EXCHANGE_TRANSPORT_FAILED = "exchange_transport_failed"
    FEED_SOURCE_FAILED = "feed_source_failed"
    FEED_SOURCE_MISMATCH = "feed_source_mismatch"
    FEED_UNEXPECTED_SOURCE = "feed_unexpected_source"
    FILL_ORDER_MISMATCH = "fill_order_mismatch"
    FILL_PAYLOAD_INVALID = "fill_payload_invalid"
    HOOK_FAILED = "hook_failed"
    PRODUCT_ID_MISSING = "product_id_missing"
    PRODUCT_METADATA_MISSING = "product_metadata_missing"
    RECONCILIATION_LOOKUP_FAILED = "reconciliation_lookup_failed"
    RUNTIME_TASK_FAILED = "runtime_task_failed"
    STRATEGY_CONTRACT_FAILED = "strategy_contract_failed"
    STRATEGY_ACTION_FAILED = "strategy_action_failed"
    STRATEGY_EVALUATION_FAILED = "strategy_evaluation_failed"
    STRATEGY_INPUT_UNAVAILABLE = "strategy_input_unavailable"
    UNSUPPORTED_ACTION_TYPE = "unsupported_action_type"
    UNSUPPORTED_PRODUCT_VENUE = "unsupported_product_venue"
    UNEXPECTED_EXCEPTION = "unexpected_exception"


class HttpMethod(StringEnum):
    GET = "GET"
    POST = "POST"


class DigestAlgorithm(StringEnum):
    SHA256 = "sha256"


class AnchorStoreType(StringEnum):
    LOCAL_FILE = "local_file"
    WORM_OBJECT = "worm_object"


class LedgerAnchorStoreProvider(StringEnum):
    AWS_S3_OBJECT_LOCK = "aws_s3_object_lock"
    LOCAL_FILE = "local_file"


class AnchorImmutabilityMode(StringEnum):
    COMPLIANCE = "compliance"
    GOVERNANCE = "governance"


class LedgerHealthCheckName(StringEnum):
    ANCHOR_COVERAGE = "anchor_coverage"
    ANCHOR_FRESHNESS = "anchor_freshness"
    ANCHOR_REMOTE_VERIFICATION = "anchor_remote_verification"
    ARCHIVE_FRESHNESS = "archive_freshness"
    ARCHIVE_REMOTE_VERIFICATION = "archive_remote_verification"
    AUDIT_INTEGRITY = "audit_integrity"
    DATA_FLOW_CONTRACT = "data_flow_contract"
    ERROR_EVENTS = "error_events"
    ACTION_EXECUTION_CONTRACT = "action_execution_contract"
    ACTION_LIFECYCLE_CONTRACT = "action_lifecycle_contract"
    EXCHANGE_STATE_CONTRACT = "exchange_state_contract"
    EXECUTION_UNCERTAINTY = "execution_uncertainty"
    FEED_DEGRADATION = "feed_degradation"
    FEED_LIFECYCLE_CONTRACT = "feed_lifecycle_contract"
    FILL_CONTRACT = "fill_contract"
    LEDGER_HEALTH_ACKNOWLEDGEMENT_CONTRACT = "ledger_health_acknowledgement_contract"
    LIVE_EXECUTION_VENUE = "live_execution_venue"
    LIVE_PREFLIGHT_CONTRACT = "live_preflight_contract"
    ORDER_IDENTITY_CONTRACT = "order_identity_contract"
    ORDER_LINEAGE_CONTRACT = "order_lineage_contract"
    ORDER_UPDATE_CONTRACT = "order_update_contract"
    OPERATOR_CANARY_EVIDENCE_RESULT_CONTRACT = "operator_canary_evidence_result_contract"
    PRODUCT_CATALOG_FRESHNESS = "product_catalog_freshness"
    RECONCILIATION = "reconciliation"
    RUNTIME_HEALTH_CHECK_RESULT_CONTRACT = "runtime_health_check_result_contract"
    RUNTIME_TASK_CONTRACT = "runtime_task_contract"
    SEQUENCE_ANOMALIES = "sequence_anomalies"
    STRATEGY_CONTRACT = "strategy_contract"
    STRATEGY_SIMULATION_CONTRACT = "strategy_simulation_contract"
    STARTUP_CONFIG_CONTRACT = "startup_config_contract"
    SYSTEM_LIFECYCLE_CONTRACT = "system_lifecycle_contract"
    TRIGGER_CONTRACT = "trigger_contract"


class LedgerHealthStatus(StringEnum):
    ATTENTION_REQUIRED = "attention_required"
    OK = "ok"


class LedgerHealthAcknowledgementIssue(StringEnum):
    ACKNOWLEDGED_THROUGH_MISMATCH = "acknowledged_through_mismatch"
    DIGEST_MISMATCH = "digest_mismatch"
    MISSING_OPERATOR_REVIEW = "missing_operator_review"
    STALE_AFTER_ACKNOWLEDGEMENT = "stale_after_acknowledgement"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"


class ReadinessCheckName(StringEnum):
    ANCHOR_STORE = "anchor_store"
    ARCHIVE_STORE = "archive_store"
    CONFIG_PLACEHOLDERS = "config_placeholders"
    CONFIG_FINGERPRINT = "config_fingerprint"
    CREDENTIALS = "credentials"
    LEDGER_HEALTH = "ledger_health"
    LEDGER_PATH = "ledger_path"
    LIVE_NO_ORDER_PREFLIGHT = "live_no_order_preflight"
    LIVE_RUNTIME_SAFETY = "live_runtime_safety"
    LIVE_TRADING_APPROVAL = "live_trading_approval"
    RISK_POLICY = "risk_policy"
    RUNTIME_TASKS = "runtime_tasks"
    STRATEGY_SIMULATION = "strategy_simulation"
    WEBSOCKET_SOURCES = "websocket_sources"


class ReadinessCheckSkipReason(StringEnum):
    LEDGER_LOCKED = "ledger_locked"
    LEDGER_PATH_IS_DIRECTORY = "ledger_path_is_directory"


class ReadinessRequirement(StringEnum):
    ANCHOR_REPRICING_POLICY = "anchor_repricing_policy"
    AUDIT_ANCHOR_STORE = "audit_anchor_store"
    AUDIT_ARCHIVE_STORE = "audit_archive_store"
    CONFIG_PLACEHOLDERS = "config_placeholders"
    CONSOLIDATION_POLICY = "consolidation_policy"
    FEED_HEALTH_TASK = "feed_health_task"
    FOLLOWUP_POLICY = "followup_policy"
    JWT_FACTORY = "jwt_factory"
    LIVE_NO_ORDER_PREFLIGHT = "live_no_order_preflight"
    LIVE_TRADING_APPROVAL = "live_trading_approval"
    OPERATOR_POLICY = "operator_policy"
    PASSIVE_MARKET_MAKING_POLICY = "passive_market_making_policy"
    PRODUCT_CATALOG = "product_catalog"
    RISK_POLICY = "risk_policy"
    STAGED_RELEASE_POLICY = "staged_release_policy"
    STRATEGY_IDS = "strategy_ids"
    STRATEGY_LIVE_APPROVAL = "strategy_live_approval"
    STRATEGY_PARAMETERS = "strategy_parameters"
    STRATEGY_RESOLUTION = "strategy_resolution"
    STRATEGY_SIMULATION = "strategy_simulation"
    TOKEN_PROVIDER = "token_provider"


class ReadinessStatus(StringEnum):
    ATTENTION_REQUIRED = "attention_required"
    OK = "ok"


class PolicyViabilityReason(StringEnum):
    MINIMUM_ORDER_NOTIONAL_EXCEEDS_MAX_ORDER_NOTIONAL = "minimum_order_notional_exceeds_max_order_notional"
    MINIMUM_ORDER_NOTIONAL_EXCEEDS_MAX_VISIBLE_NOTIONAL = "minimum_order_notional_exceeds_max_visible_notional"
    STRATEGY_NOT_SELECTED = "strategy_not_selected"


class StrategyManagerGate(StringEnum):
    ANCHOR_REPRICING_DISABLED = "anchor_repricing_disabled"
    ANCHOR_REPRICING_POLICY_UNSAFE = "anchor_repricing_policy_unsafe"
    CONSOLIDATION_DISABLED = "consolidation_disabled"
    FOLLOWUP_DISABLED = "followup_disabled"
    OPERATOR_POLICY_MISSING = "operator_policy_missing"
    PASSIVE_MARKET_MAKING_POLICY_UNSAFE = "passive_market_making_policy_unsafe"
    PRODUCT_CATALOG_MISSING = "product_catalog_missing"
    RELEASE_DISABLED = "release_disabled"
    STAGED_RELEASE_DISABLED = "staged_release_disabled"


class StrategyManagerSkipReason(StringEnum):
    ANCHOR_PRICE_UNCHANGED = "anchor_price_unchanged"
    FOLLOWUP_EXISTS = "followup_exists"
    MARKET_DATA_INVALID = "market_data_invalid"
    MOVE_EXISTS = "move_exists"
    ACTIVE_QUOTE_EXISTS = "active_quote_exists"
    ORDER_BOOK_NOT_FRESH = "order_book_not_fresh"
    ORDER_NOT_CANCELABLE = "order_not_cancelable"
    REPRICE_COOLDOWN_ACTIVE = "reprice_cooldown_active"
    REPRICE_LIMIT_REACHED = "reprice_limit_reached"
    RELEASE_CONDITIONS_NOT_MATCHED = "release_conditions_not_matched"
    PRODUCT_OUTSIDE_OPERATOR_POLICY_SCOPE = "product_outside_operator_policy_scope"
    SIDE_OUTSIDE_OPERATOR_POLICY = "side_outside_operator_policy"
    STRATEGY_CONTRACT_ERROR = "strategy_contract_error"
    STRATEGY_INPUT_UNAVAILABLE = "strategy_input_unavailable"


class ConfigWizardProfile(StringEnum):
    COINBASE_CFM_NO_ORDER = "coinbase_cfm_no_order"
    COINBASE_CFM_POLICY_PROBE = "coinbase_cfm_policy_probe"
    COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING = "coinbase_cfm_staged_passive_market_making"
    COINBASE_CFM_STAGED_RELEASE_MANAGER = "coinbase_cfm_staged_release_manager"
    DRY_RUN = "dry_run"


class StrategyWizardTemplate(StringEnum):
    CUSTOM = "custom"
    CURRENT_MARKET_DATA = "current_market_data"
    MARKET_WINDOW_STATS = "market_window_stats"
    METADATA_ONLY = "metadata_only"
    NOOP = "noop"


class FeedStatus(StringEnum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    STALE = "stale"


class ActionStatus(StringEnum):
    ACCEPTED = "accepted"
    EXECUTED = "executed"
    FAILED = "failed"
    REJECTED = "rejected"
    REQUESTED = "requested"


class ActionType(StringEnum):
    CANCEL_ORDER = "order.cancel"
    PLACE_ORDER = "order.place"


class ActionRejectionReason(StringEnum):
    DUPLICATE_ACTION_ID = "duplicate_action_id"
    DUPLICATE_ORDER_IDENTITY = "duplicate_order_identity"
    RISK_CHECK_FAILED = "risk_check_failed"
    VALIDATION_FAILED = "validation_failed"


class ActionFailureReason(StringEnum):
    EXECUTION_FAILED = "execution_failed"
    EXECUTION_REJECTED = "execution_rejected"
    EXECUTOR_ERROR = "executor_error"


class ExecutionMode(StringEnum):
    DRY_RUN = "dry_run"
    LIVE = "live"


class ExecutionStatus(StringEnum):
    ACCEPTED = "accepted"
    CANCELLED = "cancelled"
    FAILED = "failed"
    REJECTED = "rejected"


class RiskCheckStatus(StringEnum):
    FAIL = "fail"
    PASS = "pass"


class RiskRule(StringEnum):
    ALLOWED_ORDER_TYPE = "allowed_order_type"
    ALLOWED_PRODUCT = "allowed_product"
    ALLOWED_SIDE = "allowed_side"
    ALLOWED_TIME_IN_FORCE = "allowed_time_in_force"
    KILL_SWITCH = "kill_switch"
    LINEAGE_RELATION_ALLOWED = "lineage_relation_allowed"
    MAX_DAILY_NOTIONAL = "max_daily_notional"
    MAX_LEVERAGE = "max_leverage"
    MAX_OPEN_ORDERS = "max_open_orders"
    MAX_ORDER_NOTIONAL = "max_order_notional"
    MAX_ORDER_REPLACEMENTS = "max_order_replacements"
    MAX_ORDER_SIZE = "max_order_size"
    MAX_VISIBLE_NOTIONAL = "max_visible_notional"
    PLACEMENT_KIND_ALLOWED = "placement_kind_allowed"
    POST_ONLY_REQUIRED = "post_only_required"
    PRODUCT_BASE_SIZE = "product_base_size"
    PRODUCT_PRICE_INCREMENT = "product_price_increment"
    PRODUCT_QUOTE_NOTIONAL = "product_quote_notional"
    PRODUCT_TRADABLE = "product_tradable"
    REDUCE_ONLY_REQUIRED = "reduce_only_required"


class RiskControl(StringEnum):
    ALLOWED_LINEAGE_RELATIONS = "allowed_lineage_relations"
    ALLOWED_ORDER_TYPES = "allowed_order_types"
    ALLOWED_PLACEMENT_KINDS = "allowed_placement_kinds"
    ALLOWED_PRODUCTS = "allowed_products"
    ALLOWED_SIDES = "allowed_sides"
    ALLOWED_TIME_IN_FORCE = "allowed_time_in_force"
    KILL_SWITCH_ENABLED = "kill_switch_enabled"
    MAX_DAILY_NOTIONAL = "max_daily_notional"
    MAX_LEVERAGE = "max_leverage"
    MAX_OPEN_ORDERS = "max_open_orders"
    MAX_ORDER_NOTIONAL = "max_order_notional"
    MAX_ORDER_REPLACEMENTS = "max_order_replacements"
    MAX_ORDER_SIZE = "max_order_size"
    MAX_VISIBLE_NOTIONAL = "max_visible_notional"
    REQUIRE_POST_ONLY = "require_post_only"
    REQUIRE_REDUCE_ONLY = "require_reduce_only"
    REQUIRE_STAGED_RELEASE_ABOVE_VISIBLE_LIMIT = "require_staged_release_above_visible_limit"


class OrderSide(StringEnum):
    BUY = "buy"
    SELL = "sell"


class OrderBookSide(StringEnum):
    ASK = "ask"
    BID = "bid"


class OrderType(StringEnum):
    LIMIT = "limit"
    MARKET = "market"


class MarginType(StringEnum):
    CROSS = "cross"
    ISOLATED = "isolated"


class OrderLifecycleStatus(StringEnum):
    ACCEPTED = "accepted"
    CANCEL_QUEUED = "cancel_queued"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    EXECUTION_UNKNOWN = "execution_unknown"
    FAILED = "failed"
    FILLED = "filled"
    OPEN = "open"
    PENDING = "pending"
    REJECTED = "rejected"
    REQUESTED = "requested"


class OrderLineageRelation(StringEnum):
    CONSOLIDATION = "consolidation"
    EXTERNAL_IMPORT = "external_import"
    FOLLOWUP_AFTER_FILL = "followup_after_fill"
    MANUAL_ASSOCIATION = "manual_association"
    ROOT = "root"
    SPLIT_CHILD = "split_child"


class OrderPlacementKind(StringEnum):
    AMEND = "amend"
    CANCEL_REPLACE = "cancel_replace"
    EXTERNAL_IMPORT = "external_import"
    INITIAL = "initial"
    RELEASE = "release"
    STAGED_RELEASE = "staged_release"


class OrderPlacementStatus(StringEnum):
    ACCEPTED = "accepted"
    CANCELLED = "cancelled"
    FAILED = "failed"
    REJECTED = "rejected"
    STAGED = "staged"
    SUBMITTED = "submitted"


class OrderSizingDecisionStatus(StringEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class IncrementRoundingMode(StringEnum):
    DOWN = "down"
    NEAREST = "nearest"
    UP = "up"


class ProductRuleCheckStatus(StringEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class ProductRuleFailure(StringEnum):
    NOTIONAL_ABOVE_MAX = "notional_above_max"
    NOTIONAL_BELOW_MIN = "notional_below_min"
    NOTIONAL_REQUIRES_PRICE = "notional_requires_price"
    PRICE_INCREMENT = "price_increment"
    SIZE_ABOVE_MAX = "size_above_max"
    SIZE_BELOW_MIN = "size_below_min"
    SIZE_INCREMENT = "size_increment"
    VALUE_NOT_DECIMAL_COMPATIBLE = "value_not_decimal_compatible"
    VALUE_NOT_POSITIVE = "value_not_positive"


class OperatorPolicyVenue(StringEnum):
    COINBASE_CFM = "coinbase_cfm"


class OperatorPolicyLineageModel(StringEnum):
    FLAT_ROOT = "flat_root"


class OperatorPolicyPermission(StringEnum):
    ALLOWED = "allowed"
    DISABLED = "disabled"


class OperatorPolicyFollowupSizeMode(StringEnum):
    PERCENT_OF_FILLED_SIZE = "percent_of_filled_size"


class OperatorPolicyReferencePriceSource(StringEnum):
    MIDPOINT = "midpoint"


class OperatorPolicyDistanceType(StringEnum):
    PERCENT = "percent"


class OperatorPolicyUpdateMode(StringEnum):
    ADAPTIVE = "adaptive"


class OperatorPolicySizingStrategy(StringEnum):
    ADAPTIVE = "adaptive"
    FIXED = "fixed"
    TRANCHE = "tranche"


class OperatorPolicyScenarioName(StringEnum):
    ADAPTIVE_SIZING = "adaptive_sizing"
    ANCHOR_REPRICING_FORCED = "anchor_repricing_forced"
    FILLED_ORDER_CREATES_FOLLOWUP = "filled_order_creates_followup"
    FOLLOWUP_PARTIAL_FILL = "followup_partial_fill"
    HARD_SAFETY_STOP = "hard_safety_stop"
    HOTPOINT_AUTO_REPLICATE = "hotpoint_auto_replicate"
    MOVE_SAME_SIDE_ORDER = "move_same_side_order"
    SLIDE_MODE_ENABLED = "slide_mode_enabled"
    STALE_DATA_BLOCKS_ACTION = "stale_data_blocks_action"
    TIDY_NEARBY_ORDERS = "tidy_nearby_orders"
    TRANCHE_RELEASE = "tranche_release"


class OperatorPolicyScenarioStatus(StringEnum):
    DOCUMENTED_ONLY = "documented_only"
    FAILED = "failed"
    PASSED = "passed"


class OperatorActionSkipReason(StringEnum):
    MISSING_ORDER_IDENTIFIER = "missing_order_identifier"


class OperatorCanaryConfigRole(StringEnum):
    DRY_RUN = "dry_run"
    LIVE = "live"


class OperatorCanaryPlanIssue(StringEnum):
    DRY_RUN_CONFIG_NOT_DRY_RUN = "dry_run_config_not_dry_run"
    KILL_SWITCH_ENABLED = "kill_switch_enabled"
    LIVE_CONFIG_NOT_LIVE = "live_config_not_live"
    NON_POSITIVE_LIMIT_PRICE = "non_positive_limit_price"
    NON_POSITIVE_SIZE = "non_positive_size"
    NOTIONAL_ABOVE_RISK_LIMIT = "notional_above_risk_limit"
    ORDER_TYPE_OUTSIDE_RISK_SCOPE = "order_type_outside_risk_scope"
    PRICE_INCREMENT_INVALID = "price_increment_invalid"
    PRODUCT_METADATA_MISSING = "product_metadata_missing"
    PRODUCT_NOTIONAL_INVALID = "product_notional_invalid"
    PRODUCT_OUTSIDE_RISK_SCOPE = "product_outside_risk_scope"
    REDUCE_ONLY_REQUIRED = "reduce_only_required"
    SIDE_OUTSIDE_RISK_SCOPE = "side_outside_risk_scope"
    SIZE_INCREMENT_INVALID = "size_increment_invalid"
    TIME_IN_FORCE_OUTSIDE_RISK_SCOPE = "time_in_force_outside_risk_scope"
    UNSUPPORTED_ORDER_TYPE = "unsupported_order_type"
    UNSUPPORTED_POST_ONLY = "unsupported_post_only"


class OperatorCanaryEvidenceIssue(StringEnum):
    CANCEL_ACTION_MISSING = "cancel_action_missing"
    CANCEL_ACTION_NOT_EXECUTED = "cancel_action_not_executed"
    IDENTIFIER_MISMATCH = "identifier_mismatch"
    NO_MATCHING_ORDER = "no_matching_order"
    OPEN_ORDERS_REMAIN_FOR_PRODUCT = "open_orders_remain_for_product"
    ORDER_FILLED = "order_filled"
    ORDER_NOT_CANCELLED = "order_not_cancelled"
    ORDER_STILL_OPEN = "order_still_open"
    PLACE_ACTION_MISSING = "place_action_missing"
    PLACE_ACTION_NOT_EXECUTED = "place_action_not_executed"
    PRODUCT_MISMATCH = "product_mismatch"


class OperatorCanaryPlanStep(StringEnum):
    DRY_RUN_CANCEL_ALL_OPEN_ORDERS = "dry_run_cancel_all_open_orders"
    DRY_RUN_LEDGER_HEALTH = "dry_run_ledger_health"
    DRY_RUN_OPEN_ORDERS = "dry_run_open_orders"
    DRY_RUN_PLACE_ORDER = "dry_run_place_order"
    LEDGER_HEALTH = "ledger_health"
    LIVE_CANARY_EVIDENCE = "live_canary_evidence"
    LIVE_CANCEL_ORDER = "live_cancel_order"
    LIVE_LOOKUP_ORDER = "live_lookup_order"
    LIVE_NO_ORDER_PREFLIGHT = "live_no_order_preflight"
    LIVE_OPEN_ORDERS = "live_open_orders"
    LIVE_PLACE_ORDER = "live_place_order"
    LIVE_RUNTIME_GATE = "live_runtime_gate"
    READINESS = "readiness"
    SOURCE_OF_TRUTH = "source_of_truth"
    STRATEGY_SIMULATION = "strategy_simulation"


class ExchangeOrderStatus(StringEnum):
    CANCELLED = "CANCELLED"
    CANCEL_QUEUED = "CANCEL_QUEUED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
    FILLED = "FILLED"
    OPEN = "OPEN"
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"


class ExchangeLookupStatus(StringEnum):
    FAILED = "failed"
    FOUND = "found"
    NOT_FOUND = "not_found"


class ReconciliationIssue(StringEnum):
    BOT_POSITION_MISSING = "bot_position_missing"
    EXCHANGE_POSITION_MISSING = "exchange_position_missing"
    MISSING_EXECUTION_RESULT = "missing_execution_result"
    MISSING_USER_CONFIRMATION = "missing_user_confirmation"
    POSITION_SIZE_DRIFT = "position_size_drift"


class RuntimeComponent(StringEnum):
    EXCHANGE_STATE_SMOKE = "exchange_state_smoke"
    FEED_SMOKE = "feed_smoke"
    ORCHESTRATOR = "orchestrator"


class PreflightStep(StringEnum):
    EXCHANGE_STATE_SMOKE = "exchange_state_smoke"
    FEED_SMOKE = "feed_smoke"
    PRODUCT_CATALOG_SMOKE = "product_catalog_smoke"
    READINESS = "readiness"


class PreflightGateIssue(StringEnum):
    CONFIG_FINGERPRINT_MISMATCH = "config_fingerprint_mismatch"
    EXPIRED = "expired"
    MISSING = "missing"
    ORDER_ENDPOINT_CALLED = "order_endpoint_called"
    RUNTIME_TASKS_STARTED = "runtime_tasks_started"
    STATUS_NOT_OK = "status_not_ok"
    STEP_NOT_OK = "step_not_ok"
    STEPS_INCOMPLETE = "steps_incomplete"
    STRATEGY_TASKS_STARTED = "strategy_tasks_started"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"


class StrategySimulationGateIssue(StringEnum):
    CONFIG_FINGERPRINT_MISMATCH = "config_fingerprint_mismatch"
    EXECUTION_MODE_MISMATCH = "execution_mode_mismatch"
    EXPIRED = "expired"
    FAILED_EVALUATION = "failed_evaluation"
    MISSING = "missing"
    ORDER_ENDPOINT_CALLED = "order_endpoint_called"
    PAYLOAD_INVALID = "payload_invalid"
    READ_ONLY_FALSE = "read_only_false"
    REJECTED_ACTION_PREVIEW = "rejected_action_preview"
    RUNTIME_TASKS_STARTED = "runtime_tasks_started"
    STATUS_NOT_OK = "status_not_ok"
    STRATEGY_IDS_MISMATCH = "strategy_ids_mismatch"
    STRATEGY_TASKS_STARTED = "strategy_tasks_started"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"


class RuntimeStopReason(StringEnum):
    MAX_CYCLES = "max_cycles"
    STOP_REQUESTED = "stop_requested"
    TASK_COMPLETION_TARGET = "task_completion_target"


class RuntimeTask(StringEnum):
    AUDIT_ANCHOR = "audit.anchor"
    AUDIT_ARCHIVE = "audit.archive"
    EXCHANGE_STATE_RECONCILIATION = "reconciliation.exchange_state"
    FEED_HEALTH = "feed.health"
    FILL_RECONCILIATION = "reconciliation.fills"
    ORDER_RECOVERY = "reconciliation.recovery"
    PRODUCT_CATALOG_REFRESH = "products.catalog_refresh"
    STRATEGY_EVALUATION = "strategies.evaluate"
    TRIGGER_POLLING = "triggers.poll"
    WATCHDOG = "reconciliation.watchdog"


class StrategyEvaluationStatus(StringEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    STARTED = "started"


class StrategySimulationStatus(StringEnum):
    ATTENTION_REQUIRED = "attention_required"
    OK = "ok"


class StrategyHelperStatus(StringEnum):
    INSUFFICIENT_DATA = "insufficient_data"
    MISSING = "missing"
    OK = "ok"


class VenueCapabilityRequirement(StringEnum):
    ACCOUNT_LOOKUP = "account_lookup"
    CANCEL_ORDERS = "cancel_orders"
    FILL_LOOKUP = "fill_lookup"
    GOOD_UNTIL_CANCELLED_TIME_IN_FORCE = "good_until_cancelled_time_in_force"
    LIMIT_ORDERS = "limit_orders"
    LIVE_EXECUTION = "live_execution"
    MARKET_DATA_WEBSOCKET = "market_data_websocket"
    ORDER_LOOKUP = "order_lookup"
    PLACE_ORDERS = "place_orders"
    POSITION_LOOKUP = "position_lookup"
    POST_ONLY = "post_only"
    PRODUCT_METADATA_LOOKUP = "product_metadata_lookup"
    USER_ORDER_WEBSOCKET = "user_order_websocket"


class VenueContractRequirementSet(StringEnum):
    CFM_LIVE_ORDER_ROUTING = "cfm_live_order_routing"
    LIVE_ORDER_ROUTING = "live_order_routing"
    PRODUCT_METADATA_LOOKUP = "product_metadata_lookup"


class ScheduledSliceStatus(StringEnum):
    BLOCKED = "blocked"
    COMPLETE = "complete"
    DUE = "due"
    NOT_DUE = "not_due"


class MarketDataKind(StringEnum):
    ORDER_BOOK = "order_book"
    TICKER = "ticker"
    TRADE = "trade"


class MarketSeriesMembershipRule(StringEnum):
    FIXED_BUCKETS_FINAL_END_INCLUSIVE = "fixed_buckets_final_end_inclusive"
    START_INCLUSIVE_END_EXCLUSIVE = "start_inclusive_end_exclusive"
    START_INCLUSIVE_END_INCLUSIVE = "start_inclusive_end_inclusive"


class MarketSeriesTimeField(StringEnum):
    OBSERVED_AT = "observed_at"


class StrategyInputStatus(StringEnum):
    MISSING = "missing"
    OK = "ok"
    STALE = "stale"


class StrategyMarketDataStatus(StringEnum):
    INSUFFICIENT_DATA = "insufficient_data"
    MISSING = "missing"
    OK = "ok"
    STALE = "stale"


class ProductType(StringEnum):
    FUTURE = "FUTURE"
    SPOT = "SPOT"
    UNKNOWN = "UNKNOWN_PRODUCT_TYPE"


class ProductVenue(StringEnum):
    CBE = "CBE"
    FCM = "FCM"
    INTX = "INTX"
    UNKNOWN = "UNKNOWN_VENUE_TYPE"


class TimeInForce(StringEnum):
    FILL_OR_KILL = "fill_or_kill"
    GOOD_UNTIL_CANCELLED = "good_until_cancelled"
    IMMEDIATE_OR_CANCEL = "immediate_or_cancel"


class FeedStopReason(StringEnum):
    CANCELLED = "cancelled"
    ERROR = "error"
    SEQUENCE_ANOMALY = "sequence_anomaly"
    SOURCE_MISMATCH = "source_mismatch"
    STOP_REQUESTED = "stop_requested"
    STREAM_ENDED = "stream_ended"


class CoinbaseWebSocketChannel(StringEnum):
    CANDLES = "candles"
    FUTURES_BALANCE_SUMMARY = "futures_balance_summary"
    HEARTBEATS = "heartbeats"
    LEVEL2 = "level2"
    MARKET_TRADES = "market_trades"
    STATUS = "status"
    TICKER = "ticker"
    TICKER_BATCH = "ticker_batch"
    USER = "user"


class CoinbaseWebSocketEndpoint(StringEnum):
    MARKET_DATA = "wss://advanced-trade-ws.coinbase.com"
    USER_ORDER_DATA = "wss://advanced-trade-ws-user.coinbase.com"


class WebSocketOperation(StringEnum):
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"


class HookPoint(StringEnum):
    AFTER_APPEND = "after_append"
    BEFORE_APPEND = "before_append"


class TriggerRelation(StringEnum):
    AFTER = "after"
    BEFORE = "before"
    ON = "on"


class TriggerRuleType(StringEnum):
    MESSAGE = "message"
    TIME = "time"
