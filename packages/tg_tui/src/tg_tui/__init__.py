"""Legacy interactive agent-chat TUI for tg-agent (#1720 wave 2).

Optional surface: ``tg-agent agent chat`` without a prompt imports it lazily;
the core never depends on this package. Frozen under the automation-first
contract — kept working, no new features.
"""

from tg_tui.app import AgentTuiApp

__all__ = ["AgentTuiApp"]
