from enum import StrEnum


class MembershipRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    DEVELOPER = "developer"
    VIEWER = "viewer"


class VersionStatus(StrEnum):
    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"


class ToolKind(StrEnum):
    HTTP = "http"
    INTERNAL = "internal"
    WORKFLOW = "workflow"
    HUMAN_APPROVAL = "human_approval"


class ToolSideEffectLevel(StrEnum):
    READ_ONLY = "read_only"
    IDEMPOTENT_WRITE = "idempotent_write"
    NON_IDEMPOTENT_WRITE = "non_idempotent_write"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunStepStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class RunStepType(StrEnum):
    AGENT = "agent"
    MODEL = "model"
    TOOL = "tool"
    BRANCH = "branch"
    PARALLEL = "parallel"
    APPROVAL = "approval"
    EVALUATOR = "evaluator"


class RunEventActor(StrEnum):
    API = "api"
    WORKFLOW = "workflow"
    AGENT = "agent"
    TOOL = "tool"
    USER = "user"
    EVALUATOR = "evaluator"
    SYSTEM = "system"


class FailureCode(StrEnum):
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    MAX_STEPS_EXCEEDED = "MAX_STEPS_EXCEEDED"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    TOOL_EXECUTION_ERROR = "TOOL_EXECUTION_ERROR"
    TOOL_VALIDATION_ERROR = "TOOL_VALIDATION_ERROR"
