"""Expert/auto subcategory pills persist in the score lookup sqlite."""

from pathlib import Path

from utils import p2oasys_score_lookup
from utils.p2oasys_score_ribbon import (
    load_expert_subcat_scores,
    matrix_ribbon_html,
    _subcat_score_from_bundle,
)


def test_seed_and_load_expert_subcat_acetone(tmp_path: Path):
    src = Path(__file__).resolve().parents[1] / "data" / "p2oasys_score_lookup.sqlite"
    dst = tmp_path / "lookup.sqlite"
    dst.write_bytes(src.read_bytes())
    n = p2oasys_score_lookup.ensure_expert_subcat_seeded(db_path=dst, force=True)
    assert n > 0
    bundle = p2oasys_score_lookup.load_subcat_bundle("67-64-1", "expert", db_path=dst)
    assert bundle is not None
    health = _subcat_score_from_bundle(bundle.get("Acute Human Effects") or {}, "Health")
    assert health is not None
    html = matrix_ribbon_html(
        bundle,
        category_totals={"Acute Human Effects": 6.0},
        prefer_category_totals=True,
        title="Expert",
    )
    assert "Hlth" in html
    assert str(int(health)) in html or f"{health:.1f}" in html


def test_load_expert_subcat_scores_acetone():
    bundle = load_expert_subcat_scores("67-64-1")
    assert bundle is not None
    acute = bundle.get("Acute Human Effects") or {}
    assert _subcat_score_from_bundle(acute, "Health") is not None


def test_upsert_auto_draft_writes_subcats(tmp_path: Path):
    db = tmp_path / "lookup.sqlite"
    scores = {
        "Acute Human Effects": {
            "Oral Toxicity": {"_max": 8.0},
            "Health": {"_max": 4.0},
            "_category_max": 8.0,
        },
        "Physical Properties": {
            "Vapor Pressure": {"_max": 6.0},
            "_category_max": 6.0,
        },
    }
    row = p2oasys_score_lookup.upsert_auto_from_draft(
        {"ok": True, "cas": "67-64-1", "scores": scores, "chemical_name": "Acetone"},
        db_path=db,
    )
    assert row and row.get("has_auto")
    auto = p2oasys_score_lookup.load_subcat_bundle("67-64-1", "auto", db_path=db)
    assert auto is not None
    assert _subcat_score_from_bundle(auto["Acute Human Effects"], "Oral Toxicity") == 8.0
