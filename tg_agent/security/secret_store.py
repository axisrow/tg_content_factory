"""Machine-level OS secret store for SESSION_ENCRYPTION_KEY (#1699).

Opt-in resolution layer: the key lands here only when the owner runs
``tg-agent key set`` — nothing is ever stored silently, and absence in the
store means "user chose not to store". Readers consult it only after
config.yaml/env leave the key empty, so machines without a stored key behave
exactly as before #1699 (warn + degraded pool).

Backends are system CLIs driven over subprocess — no new heavy deps:

- macOS   — Keychain (``security find/add/delete-generic-password``)
- Windows — Credential Manager (PowerShell + CredRead/CredWrite P/Invoke)
- Linux   — secret-service (``secret-tool``); headless fallback is a 0600
  file inside the global store root (``~/.tg-agent``, ``TG_AGENT_GLOBAL_DIR``)

``TG_AGENT_SECRET_STORE=off`` short-circuits the whole layer (resolution
behaves byte-for-byte as before #1699); tests set it for hermeticity, and it
doubles as an operational hard off-switch. Read results are cached for the
process lifetime — picking up a ``key set``/``key clear`` needs a daemon
restart, same as any env change.
"""

from __future__ import annotations

import base64
import functools
import os
import shutil
import subprocess
import sys
from pathlib import Path

#: Identity of the stored secret inside the platform store.
SERVICE_NAME = "tg-agent"
ACCOUNT_NAME = "session-encryption-key"

#: ``TG_AGENT_SECRET_STORE=off`` (or 0/false/no) disables the layer entirely.
OFF_ENV = "TG_AGENT_SECRET_STORE"
_OFF_VALUES = {"off", "0", "false", "no"}

#: Exit code ``security`` uses for "item could not be found in the keychain"
#: (verified empirically on macOS). Reused as the Windows "not found" exit.
_NOT_FOUND_RC = 44

#: Wall-clock cap for one store CLI call — a hung keyring agent must not stall
#: startup; a timeout degrades to "not stored" like any backend failure.
_CLI_TIMEOUT_SEC = 10

#: Human-readable failure from the last read, surfaced by ``key status``.
_backend_error: str | None = None


class SecretStoreError(RuntimeError):
    """A store backend failed while writing or clearing (read failures degrade to 'absent')."""


def store_enabled() -> bool:
    """False when ``TG_AGENT_SECRET_STORE`` carries an off value."""
    return os.environ.get(OFF_ENV, "").strip().lower() not in _OFF_VALUES


def backend_name() -> str:
    """Which backend a read/write would use on this machine right now."""
    if not store_enabled():
        return "off"
    if sys.platform == "darwin":
        return "keychain"
    if sys.platform == "win32":
        return "credential-manager"
    if shutil.which("secret-tool"):
        return "secret-tool"
    return "file"


def _run(cmd: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=_CLI_TIMEOUT_SEC,
            check=False,
        )
    except subprocess.TimeoutExpired:
        # A hung keyring agent degrades like any backend failure: reads treat
        # non-0/44 as "not stored" + recorded error, writes raise.
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=124,
            stdout="",
            stderr=f"timed out after {_CLI_TIMEOUT_SEC}s",
        )


def _set_backend_error(message: str) -> None:
    global _backend_error
    _backend_error = message


def last_backend_error() -> str | None:
    """Human-readable failure from the last read, for ``key status`` diagnostics."""
    return _backend_error


# --------------------------------------------------------------------------- #
# macOS — Keychain (contract verified on a live login keychain)
# --------------------------------------------------------------------------- #


def _read_keychain() -> str | None:
    proc = _run(["security", "find-generic-password", "-s", SERVICE_NAME, "-a", ACCOUNT_NAME, "-w"])
    if proc.returncode == 0:
        return proc.stdout.rstrip("\n") or None
    if proc.returncode == _NOT_FOUND_RC:
        return None
    _set_backend_error(f"security find-generic-password rc={proc.returncode}: {proc.stderr.strip()}")
    return None


def _write_keychain(key: str) -> None:
    # The password rides argv — the documented non-interactive `security` write
    # form (piped stdin triggers the interactive double prompt instead);
    # visible to local ps only for the duration of this one call.
    proc = _run(
        ["security", "add-generic-password", "-U", "-s", SERVICE_NAME, "-a", ACCOUNT_NAME, "-w", key]
    )
    if proc.returncode != 0:
        raise SecretStoreError(f"security add-generic-password rc={proc.returncode}: {proc.stderr.strip()}")


def _delete_keychain() -> bool:
    proc = _run(["security", "delete-generic-password", "-s", SERVICE_NAME, "-a", ACCOUNT_NAME])
    if proc.returncode == 0:
        return True
    if proc.returncode == _NOT_FOUND_RC:
        return False
    raise SecretStoreError(f"security delete-generic-password rc={proc.returncode}: {proc.stderr.strip()}")


# --------------------------------------------------------------------------- #
# Windows — Credential Manager (PowerShell + CredRead/CredWrite P/Invoke).
# The P/Invoke script travels on stdin (`-Command -`), never argv; the key is
# base64 inside the script so any encoding survives, and the secret comes back
# base64 on stdout. Blob bytes are UTF-8 on both write and read — our writer
# is the only reader, no interop with cmdkey-written items is attempted.
# --------------------------------------------------------------------------- #

_PS_CRED_TYPE = (
    "using System;using System.Runtime.InteropServices;"
    "public class TgCred{"
    "[StructLayout(LayoutKind.Sequential,CharSet=CharSet.Unicode)]"
    "public struct CREDENTIAL{"
    "public int Flags;public int Type;public string TargetName;public string Comment;"
    "public System.Runtime.InteropServices.ComTypes.FILETIME LastWritten;"
    "public int CredentialBlobSize;public IntPtr CredentialBlob;public int Persist;"
    "public string UserName;}"
    "[DllImport(\"advapi32.dll\",EntryPoint=\"CredReadW\",CharSet=CharSet.Unicode,SetLastError=true)]"
    "public static extern bool CredRead(string target,int type,int flags,out IntPtr credPtr);"
    "[DllImport(\"advapi32.dll\",EntryPoint=\"CredWriteW\",CharSet=CharSet.Unicode,SetLastError=true)]"
    "public static extern bool CredWrite(ref CREDENTIAL credential,int flags);"
    "[DllImport(\"advapi32.dll\",EntryPoint=\"CredDeleteW\",CharSet=CharSet.Unicode,SetLastError=true)]"
    "public static extern bool CredDelete(string target,int type,int flags);"
    "[DllImport(\"advapi32.dll\")]public static extern void CredFree(IntPtr cred);"
    "}"
)

_WIN_TARGET = f"{SERVICE_NAME}:{ACCOUNT_NAME}"

_PS_READ_SCRIPT = (
    "$ErrorActionPreference='Stop'\n"
    f"Add-Type -TypeDefinition '{_PS_CRED_TYPE}'\n"
    "$p=[IntPtr]::Zero\n"
    f"if(-not [TgCred]::CredRead('{_WIN_TARGET}',1,0,[ref]$p)){{exit {_NOT_FOUND_RC}}}\n"
    "$c=[System.Runtime.InteropServices.Marshal]::PtrToStructure($p,[type][TgCred+CREDENTIAL])\n"
    "$b=New-Object byte[] $c.CredentialBlobSize\n"
    "[System.Runtime.InteropServices.Marshal]::Copy($c.CredentialBlob,$b,0,$b.Length)\n"
    "[TgCred]::CredFree($p)\n"
    "[Console]::Out.Write([Convert]::ToBase64String($b))\n"
)

_PS_WRITE_TEMPLATE = (
    "$ErrorActionPreference='Stop'\n"
    f"Add-Type -TypeDefinition '{_PS_CRED_TYPE}'\n"
    "$key=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('@KEY_B64@'))\n"
    "$bytes=[Text.Encoding]::UTF8.GetBytes($key)\n"
    "$ptr=[System.Runtime.InteropServices.Marshal]::AllocHGlobal($bytes.Length)\n"
    "[System.Runtime.InteropServices.Marshal]::Copy($bytes,0,$ptr,$bytes.Length)\n"
    "$c=New-Object TgCred+CREDENTIAL\n"
    "$c.Type=1\n"
    f"$c.TargetName='{_WIN_TARGET}'\n"
    f"$c.UserName='{ACCOUNT_NAME}'\n"
    "$c.CredentialBlobSize=$bytes.Length\n"
    "$c.CredentialBlob=$ptr\n"
    "$c.Persist=3\n"  # CRED_PERSIST_ENTERPRISE — roams with the user profile
    "$ok=[TgCred]::CredWrite([ref]$c,0)\n"
    "[System.Runtime.InteropServices.Marshal]::FreeHGlobal($ptr)\n"
    "if(-not $ok){exit 1}\n"
)

_PS_DELETE_SCRIPT = (
    "$ErrorActionPreference='Stop'\n"
    f"Add-Type -TypeDefinition '{_PS_CRED_TYPE}'\n"
    f"if(-not [TgCred]::CredDelete('{_WIN_TARGET}',1,0)){{exit {_NOT_FOUND_RC}}}\n"
)


def _run_powershell(script: str) -> subprocess.CompletedProcess[str]:
    return _run(["powershell", "-NoProfile", "-NonInteractive", "-Command", "-"], input_text=script)


def _read_credential_manager() -> str | None:
    proc = _run_powershell(_PS_READ_SCRIPT)
    if proc.returncode == 0:
        try:
            return base64.b64decode(proc.stdout.strip()).decode("utf-8") or None
        except (ValueError, UnicodeDecodeError):
            _set_backend_error("credential-manager returned an undecodable password blob")
            return None
    if proc.returncode == _NOT_FOUND_RC:
        return None
    _set_backend_error(f"powershell CredRead rc={proc.returncode}: {proc.stderr.strip()}")
    return None


def _write_credential_manager(key: str) -> None:
    script = _PS_WRITE_TEMPLATE.replace(
        "@KEY_B64@", base64.b64encode(key.encode("utf-8")).decode("ascii")
    )
    proc = _run_powershell(script)
    if proc.returncode != 0:
        raise SecretStoreError(f"powershell CredWrite rc={proc.returncode}: {proc.stderr.strip()}")


def _delete_credential_manager() -> bool:
    proc = _run_powershell(_PS_DELETE_SCRIPT)
    if proc.returncode == 0:
        return True
    if proc.returncode == _NOT_FOUND_RC:
        return False
    raise SecretStoreError(f"powershell CredDelete rc={proc.returncode}: {proc.stderr.strip()}")


# --------------------------------------------------------------------------- #
# Linux — secret-tool (secret-service), 0600-file fallback for headless boxes
# --------------------------------------------------------------------------- #


def _read_secret_tool() -> str | None:
    proc = _run(["secret-tool", "lookup", "service", SERVICE_NAME, "account", ACCOUNT_NAME])
    if proc.returncode == 0 and proc.stdout:
        return proc.stdout.rstrip("\n") or None
    if proc.returncode != 0 and proc.stderr.strip():
        _set_backend_error(f"secret-tool lookup rc={proc.returncode}: {proc.stderr.strip()}")
    return None


def _write_secret_tool(key: str) -> None:
    # secret-tool reads the secret from stdin to EOF (the documented form).
    proc = _run(
        ["secret-tool", "store", "--label", "tg-agent session encryption key",
         "service", SERVICE_NAME, "account", ACCOUNT_NAME],
        input_text=key,
    )
    if proc.returncode != 0:
        raise SecretStoreError(f"secret-tool store rc={proc.returncode}: {proc.stderr.strip()}")


def _delete_secret_tool() -> bool:
    proc = _run(["secret-tool", "clear", "service", SERVICE_NAME, "account", ACCOUNT_NAME])
    if proc.returncode != 0:
        raise SecretStoreError(f"secret-tool clear rc={proc.returncode}: {proc.stderr.strip()}")
    return True


def fallback_file_path() -> Path:
    """0600 key file inside the global store root (headless Linux fallback).

    Mirrors ``config.global_store_root()`` inline — importing it would make the
    config→secret_store edge circular.
    """
    override = os.environ.get("TG_AGENT_GLOBAL_DIR", "").strip()
    root = Path(override).expanduser() if override else Path.home() / ".tg-agent"
    return root.resolve() / "session_encryption_key"


def _read_file_fallback() -> str | None:
    path = fallback_file_path()
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except FileNotFoundError:
        return None
    except OSError as exc:
        _set_backend_error(f"cannot read {path}: {exc}")
        return None


def _write_file_fallback(key: str) -> None:
    path = fallback_file_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Create with 0600 up front — no window where the file sits world-
        # readable under a permissive umask; the final chmod enforces 0600
        # even where umask would have widened touch()'s mode.
        path.touch(mode=0o600)
        path.write_text(key + "\n", encoding="utf-8")
        path.chmod(0o600)
    except OSError as exc:
        raise SecretStoreError(f"cannot write {path}: {exc}") from exc


def _delete_file_fallback() -> bool:
    path = fallback_file_path()
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SecretStoreError(f"cannot remove {path}: {exc}") from exc


# --------------------------------------------------------------------------- #
# Public read/write/clear API (platform dispatch)
# --------------------------------------------------------------------------- #


@functools.lru_cache(maxsize=1)
def read_stored_key() -> str | None:
    """The stored session key, or ``None`` when off/absent/unreadable.

    Cached for the process lifetime: a ``key set``/``key clear`` from another
    process needs a daemon restart to be picked up (same as any env change).
    """
    backend = backend_name()
    if backend == "keychain":
        return _read_keychain()
    if backend == "credential-manager":
        return _read_credential_manager()
    if backend == "secret-tool":
        return _read_secret_tool()
    if backend == "file":
        return _read_file_fallback()
    return None


def store_key(key: str) -> None:
    """Store *key* in the platform store (explicit ``tg-agent key set`` only)."""
    if not store_enabled():
        raise SecretStoreError(f"secret store is disabled ({OFF_ENV}=off)")
    backend = backend_name()
    if backend == "keychain":
        _write_keychain(key)
    elif backend == "credential-manager":
        _write_credential_manager(key)
    elif backend == "secret-tool":
        _write_secret_tool(key)
    else:
        _write_file_fallback(key)
    read_stored_key.cache_clear()


def delete_key() -> bool:
    """Remove the stored key; ``False`` when nothing was stored."""
    if not store_enabled():
        raise SecretStoreError(f"secret store is disabled ({OFF_ENV}=off)")
    backend = backend_name()
    if backend == "keychain":
        removed = _delete_keychain()
    elif backend == "credential-manager":
        removed = _delete_credential_manager()
    elif backend == "secret-tool":
        removed = _delete_secret_tool()
    else:
        removed = _delete_file_fallback()
    read_stored_key.cache_clear()
    return removed


def resolve_session_secret_from_env_or_store() -> str | None:
    """Shared env→store chain for config-less callers (``Database`` default, #1699).

    The env var is the escape hatch and wins; the store is consulted only when
    it is unset. Keeps the two former raw ``os.getenv`` sites behind one
    fallback instead of duplicating it.
    """
    env_value = os.environ.get("SESSION_ENCRYPTION_KEY", "").strip()
    if env_value:
        return env_value
    return read_stored_key()
