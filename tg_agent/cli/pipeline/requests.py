"""Explicit immutable inputs for each pipeline CLI leaf."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from argparse import Namespace


@dataclass(frozen=True, kw_only=True)
class PipelineCommand:
    """Parsed command values; entrypoints attach configuration before dispatch."""

    config: str | None = None
    pipeline_action: str = ""


@dataclass(frozen=True, kw_only=True)
class PipelineListCommand(PipelineCommand):
    pipeline_action: Literal["list"] = "list"


@dataclass(frozen=True, kw_only=True)
class PipelineShowCommand(PipelineCommand):
    pipeline_action: Literal["show"] = "show"
    id: int


@dataclass(frozen=True, kw_only=True)
class PipelineAddCommand(PipelineCommand):
    pipeline_action: Literal["add"] = "add"
    name: str
    prompt_template: str | None = None
    json_file: str | None = None
    source: list[int] | None = None
    target: list[str] | None = None
    llm_model: str | None = None
    image_model: str | None = None
    publish_mode: str = "moderated"
    generation_backend: str = "chain"
    interval: int = 60
    inactive: bool = False
    ab_variants: int = 1
    ab_auto_select: bool = False
    node_specs: list[str] | None = None
    edge: list[str] | None = None
    node_configs: list[str] | None = None
    run_after: bool = False
    since_value: int = 24
    since_unit: str = "h"


@dataclass(frozen=True, kw_only=True)
class PipelineDryRunCountCommand(PipelineCommand):
    pipeline_action: Literal["dry-run-count"] = "dry-run-count"
    source: list[int]
    since_value: int = 24
    since_unit: str = "h"


@dataclass(frozen=True, kw_only=True)
class PipelineEditCommand(PipelineCommand):
    pipeline_action: Literal["edit"] = "edit"
    id: int
    name: str | None = None
    prompt_template: str | None = None
    source: list[int] | None = None
    target: list[str] | None = None
    llm_model: str | None = None
    image_model: str | None = None
    publish_mode: str | None = None
    generation_backend: str | None = None
    interval: int | None = None
    active: bool | None = None
    ab_variants: int | None = None
    ab_auto_select: bool | None = None


@dataclass(frozen=True, kw_only=True)
class PipelineDeleteCommand(PipelineCommand):
    pipeline_action: Literal["delete"] = "delete"
    id: int


@dataclass(frozen=True, kw_only=True)
class PipelineToggleCommand(PipelineCommand):
    pipeline_action: Literal["toggle"] = "toggle"
    id: int


@dataclass(frozen=True, kw_only=True)
class PipelineRunCommand(PipelineCommand):
    pipeline_action: Literal["run"] = "run"
    id: int
    preview: bool = False
    publish: bool = False
    limit: int = 8
    max_tokens: int = 256
    temperature: float = 0.0


@dataclass(frozen=True, kw_only=True)
class PipelineGenerateCommand(PipelineCommand):
    pipeline_action: Literal["generate"] = "generate"
    id: int
    max_tokens: int = 512
    temperature: float = 0.7
    model: str | None = None
    preview: bool = False
    ab_variants: int | None = None
    auto_select: bool = False


@dataclass(frozen=True, kw_only=True)
class PipelineGenerateStreamCommand(PipelineCommand):
    pipeline_action: Literal["generate-stream"] = "generate-stream"
    id: int
    model: str | None = None
    max_tokens: int = 256
    temperature: float = 0.0
    limit: int = 8


@dataclass(frozen=True, kw_only=True)
class PipelineRunsCommand(PipelineCommand):
    pipeline_action: Literal["runs"] = "runs"
    id: int
    limit: int = 20
    status: str | None = None


@dataclass(frozen=True, kw_only=True)
class PipelineRunShowCommand(PipelineCommand):
    pipeline_action: Literal["run-show"] = "run-show"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelineVariantsCommand(PipelineCommand):
    pipeline_action: Literal["variants"] = "variants"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelineSelectVariantCommand(PipelineCommand):
    pipeline_action: Literal["select-variant"] = "select-variant"
    run_id: int
    index: int


@dataclass(frozen=True, kw_only=True)
class PipelineAutoSelectCommand(PipelineCommand):
    pipeline_action: Literal["auto-select"] = "auto-select"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelineQueueCommand(PipelineCommand):
    pipeline_action: Literal["queue"] = "queue"
    id: int
    limit: int = 20


@dataclass(frozen=True, kw_only=True)
class PipelineModerationListCommand(PipelineCommand):
    pipeline_action: Literal["moderation-list"] = "moderation-list"
    pipeline_id: int | None = None
    limit: int = 20


@dataclass(frozen=True, kw_only=True)
class PipelineModerationViewCommand(PipelineCommand):
    pipeline_action: Literal["moderation-view"] = "moderation-view"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelinePublishCommand(PipelineCommand):
    pipeline_action: Literal["publish"] = "publish"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelineApproveCommand(PipelineCommand):
    pipeline_action: Literal["approve"] = "approve"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelineRejectCommand(PipelineCommand):
    pipeline_action: Literal["reject"] = "reject"
    run_id: int


@dataclass(frozen=True, kw_only=True)
class PipelineResolveUnconfirmedCommand(PipelineCommand):
    pipeline_action: Literal["resolve-unconfirmed"] = "resolve-unconfirmed"
    run_id: int
    target: str
    action: str = "delivered"


@dataclass(frozen=True, kw_only=True)
class PipelineBulkApproveCommand(PipelineCommand):
    pipeline_action: Literal["bulk-approve"] = "bulk-approve"
    run_ids: list[int]


@dataclass(frozen=True, kw_only=True)
class PipelineBulkRejectCommand(PipelineCommand):
    pipeline_action: Literal["bulk-reject"] = "bulk-reject"
    run_ids: list[int]


@dataclass(frozen=True, kw_only=True)
class PipelineRefinementStepsCommand(PipelineCommand):
    pipeline_action: Literal["refinement-steps"] = "refinement-steps"
    id: int
    steps_json: str | None = None


@dataclass(frozen=True, kw_only=True)
class PipelineExportCommand(PipelineCommand):
    pipeline_action: Literal["export"] = "export"
    id: int
    output: str | None = None
    force: bool = False


@dataclass(frozen=True, kw_only=True)
class PipelineImportCommand(PipelineCommand):
    pipeline_action: Literal["import"] = "import"
    file: str
    name: str | None = None


@dataclass(frozen=True, kw_only=True)
class PipelineTemplatesCommand(PipelineCommand):
    pipeline_action: Literal["templates"] = "templates"
    category: str | None = None


@dataclass(frozen=True, kw_only=True)
class PipelineFromTemplateCommand(PipelineCommand):
    pipeline_action: Literal["from-template"] = "from-template"
    template_id: int
    name: str
    source_ids: str = ""
    target_refs: str = ""


@dataclass(frozen=True, kw_only=True)
class PipelineAiEditCommand(PipelineCommand):
    pipeline_action: Literal["ai-edit"] = "ai-edit"
    id: int
    instruction: str
    show: bool = False


@dataclass(frozen=True, kw_only=True)
class PipelineGraphCommand(PipelineCommand):
    pipeline_action: Literal["graph"] = "graph"
    id: int


@dataclass(frozen=True, kw_only=True)
class PipelineFilterSetCommand(PipelineCommand):
    pipeline_action: Literal["filter"] = "filter"
    filter_action: Literal["set"] = "set"
    id: int
    message_kinds: list[str] | None = None
    service_actions: list[str] | None = None
    media_types: list[str] | None = None
    sender_kinds: list[str] | None = None
    keywords: list[str] | None = None
    regex: str | None = None
    forwarded: str | None = None
    has_text: str | None = None


@dataclass(frozen=True, kw_only=True)
class PipelineFilterShowCommand(PipelineCommand):
    pipeline_action: Literal["filter"] = "filter"
    filter_action: Literal["show"] = "show"
    id: int


@dataclass(frozen=True, kw_only=True)
class PipelineFilterClearCommand(PipelineCommand):
    pipeline_action: Literal["filter"] = "filter"
    filter_action: Literal["clear"] = "clear"
    id: int


@dataclass(frozen=True, kw_only=True)
class PipelineNodeAddCommand(PipelineCommand):
    pipeline_action: Literal["node"] = "node"
    node_action: Literal["add"] = "add"
    pipeline_id: int
    node_spec: str


@dataclass(frozen=True, kw_only=True)
class PipelineNodeReplaceCommand(PipelineCommand):
    pipeline_action: Literal["node"] = "node"
    node_action: Literal["replace"] = "replace"
    pipeline_id: int
    node_id: str
    node_spec: str


@dataclass(frozen=True, kw_only=True)
class PipelineNodeRemoveCommand(PipelineCommand):
    pipeline_action: Literal["node"] = "node"
    node_action: Literal["remove"] = "remove"
    pipeline_id: int
    node_id: str


@dataclass(frozen=True, kw_only=True)
class PipelineEdgeAddCommand(PipelineCommand):
    pipeline_action: Literal["edge"] = "edge"
    edge_action: Literal["add"] = "add"
    pipeline_id: int
    from_node: str
    to_node: str


@dataclass(frozen=True, kw_only=True)
class PipelineEdgeRemoveCommand(PipelineCommand):
    pipeline_action: Literal["edge"] = "edge"
    edge_action: Literal["remove"] = "remove"
    pipeline_id: int
    from_node: str
    to_node: str


_REQUEST_TYPES: dict[tuple[str, str | None], type[PipelineCommand]] = {
    ("list", None): PipelineListCommand,
    ("show", None): PipelineShowCommand,
    ("add", None): PipelineAddCommand,
    ("dry-run-count", None): PipelineDryRunCountCommand,
    ("edit", None): PipelineEditCommand,
    ("delete", None): PipelineDeleteCommand,
    ("toggle", None): PipelineToggleCommand,
    ("run", None): PipelineRunCommand,
    ("generate", None): PipelineGenerateCommand,
    ("generate-stream", None): PipelineGenerateStreamCommand,
    ("runs", None): PipelineRunsCommand,
    ("run-show", None): PipelineRunShowCommand,
    ("variants", None): PipelineVariantsCommand,
    ("select-variant", None): PipelineSelectVariantCommand,
    ("auto-select", None): PipelineAutoSelectCommand,
    ("queue", None): PipelineQueueCommand,
    ("moderation-list", None): PipelineModerationListCommand,
    ("moderation-view", None): PipelineModerationViewCommand,
    ("publish", None): PipelinePublishCommand,
    ("approve", None): PipelineApproveCommand,
    ("reject", None): PipelineRejectCommand,
    ("resolve-unconfirmed", None): PipelineResolveUnconfirmedCommand,
    ("bulk-approve", None): PipelineBulkApproveCommand,
    ("bulk-reject", None): PipelineBulkRejectCommand,
    ("refinement-steps", None): PipelineRefinementStepsCommand,
    ("export", None): PipelineExportCommand,
    ("import", None): PipelineImportCommand,
    ("templates", None): PipelineTemplatesCommand,
    ("from-template", None): PipelineFromTemplateCommand,
    ("ai-edit", None): PipelineAiEditCommand,
    ("graph", None): PipelineGraphCommand,
    ("filter", "set"): PipelineFilterSetCommand,
    ("filter", "show"): PipelineFilterShowCommand,
    ("filter", "clear"): PipelineFilterClearCommand,
    ("node", "add"): PipelineNodeAddCommand,
    ("node", "replace"): PipelineNodeReplaceCommand,
    ("node", "remove"): PipelineNodeRemoveCommand,
    ("edge", "add"): PipelineEdgeAddCommand,
    ("edge", "remove"): PipelineEdgeRemoveCommand,
}


def request_from_namespace(args: Namespace) -> PipelineCommand:
    """Adapt legacy inputs, retaining no-op dispatch for unsupported actions."""
    action = args.pipeline_action
    subaction = getattr(args, action + "_action", None) if action in {"filter", "node", "edge"} else None
    request_type = _REQUEST_TYPES.get((action, subaction))
    if request_type is None:
        return PipelineCommand(config=getattr(args, "config", None))
    values = {item.name: getattr(args, item.name) for item in fields(request_type) if hasattr(args, item.name)}
    return request_type(**values)
