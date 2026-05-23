import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_dialogs_download_media_first_collected_media(run_cli, assert_cli_ok, live_media_message, tmp_path):
    chat_ref, message_id = live_media_message
    output_dir = tmp_path / "media"

    result = run_cli(
        "dialogs",
        "download-media",
        chat_ref,
        message_id,
        "--output-dir",
        str(output_dir),
        timeout=180,
    )
    assert_cli_ok(result)
    combined = f"{result.stdout}\n{result.stderr}"
    assert "Downloaded:" in combined, f"unexpected `dialogs download-media` output: {combined!r}"
    assert any(output_dir.iterdir()), f"`dialogs download-media` did not create files in {output_dir}"
