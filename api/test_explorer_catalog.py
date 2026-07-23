from explorer import CATALOG, SAMPLES, STATE_ONLY_SAMPLES


def test_pecos_enrollment_is_a_state_scoped_catalog_dataset() -> None:
    entry = next(entry for entry in CATALOG if entry["key"] == "pecos_enrollment")

    assert entry["table"] == "raw_pecos_enrollment"
    assert entry["join_keys"] == ["NPI", "ENRLMT_ID"]
    assert "employment" in entry["description"]
    assert "pecos_enrollment" in SAMPLES
    assert "pecos_enrollment" in STATE_ONLY_SAMPLES
