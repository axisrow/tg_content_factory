from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from src.services.pipeline_service import PipelineValidationError

if TYPE_CHECKING:
    from src.services.pipeline_service import PipelineTargetRef


def parse_pipeline_target_refs(
    values: str | Iterable[str],
    *,
    missing_separator_message: str = "Target must be in PHONE|DIALOG_ID format.",
    invalid_dialog_id_message: str = "Target dialog id must be numeric.",
) -> list[PipelineTargetRef]:
    from src.services.pipeline_service import PipelineTargetRef

    raw_values = values.split(",") if isinstance(values, str) else values
    refs: list[PipelineTargetRef] = []
    for value in raw_values:
        part = str(value).strip()
        if not part:
            continue
        phone, separator, raw_dialog_id = part.partition("|")
        if not separator:
            raise PipelineValidationError(missing_separator_message.format(part=part))
        try:
            dialog_id = int(raw_dialog_id.strip())
        except ValueError as exc:
            raise PipelineValidationError(invalid_dialog_id_message.format(part=part)) from exc
        refs.append(PipelineTargetRef(phone=phone.strip(), dialog_id=dialog_id))
    return refs
