from research import RESEARCH_PAYMENT_CAVEAT, aggregate_investigator_rows


def test_aggregates_pi_and_recipient_roles_without_double_counting():
    rows = [
        {
            "record_id": "payment-1",
            "recipient_npi": "1111111111",
            "pi_1_npi": "1111111111",
            "amount": 1000,
            "study_name": "Precision study",
            "sponsor": "Example Pharma",
            "nct_id": "NCT12345678",
            "program_year": 2025,
            "source_link": "https://example.test/research",
        },
        {
            "record_id": "payment-2",
            "recipient_npi": "2222222222",
            "amount": 250,
            "study_name": "Registry",
            "program_year": 2024,
        },
    ]

    result = aggregate_investigator_rows(
        rows, ["1111111111", "2222222222"], ["NCT12345678"]
    )

    assert result[0]["npi"] == "1111111111"
    assert result[0]["research_payment_count"] == 1
    assert result[0]["pi_payment_count"] == 1
    assert result[0]["evidence_level"] == "current_trial_match"
    assert result[1]["evidence_level"] == "research_payment_evidence"


def test_research_payment_caveat_explains_dollars_are_not_compensation():
    assert "institution" in RESEARCH_PAYMENT_CAVEAT
    assert "do not represent personal physician compensation" in RESEARCH_PAYMENT_CAVEAT
