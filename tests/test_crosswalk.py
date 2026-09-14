from ingest.crosswalk import detect_id_type, is_uvcb_name, normalize_cas


def test_normalize_cas_with_dashes():
    assert normalize_cas("50-00-0") == "50-00-0"


def test_normalize_cas_without_dashes():
    assert normalize_cas("50000") == "50-00-0"


def test_normalize_cas_bad_checksum():
    assert normalize_cas("50-00-1") is None


def test_detect_dtxsid_case_insensitive():
    assert detect_id_type("dtxsid7020005") == "dtxsid"


def test_uvcb_detection_name_markers():
    assert is_uvcb_name("Reaction mass of hydrocarbons, C7-9") is True


def test_detect_name_when_null_ec():
    assert detect_id_type("formaldehyde solution") == "name"
