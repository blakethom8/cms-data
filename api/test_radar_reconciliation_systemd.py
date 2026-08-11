from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent


def test_systemd_reconciliation_is_staging_only_and_publisher_driven() -> None:
    service = (
        REPOSITORY_ROOT
        / "deploy/systemd/cms-nppes-radar-reconciliation.service"
    ).read_text(encoding="utf-8")
    timer = (
        REPOSITORY_ROOT / "deploy/systemd/cms-nppes-radar-reconciliation.timer"
    ).read_text(encoding="utf-8")

    assert "pipeline.data_platform acquire nppes_monthly_v2" in service
    assert "pipeline.data_platform acquire nppes_weekly_incremental_v2" in service
    assert "pipeline.radar_reconciliation" in service
    assert "--data-root /srv/cms-data-platform/data" in service
    assert " pipeline.data_platform promote " not in service
    assert "pipeline.production_cutover" not in service
    assert "OnCalendar=" in timer
