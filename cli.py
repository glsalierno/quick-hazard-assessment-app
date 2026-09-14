"""CLI for ChemDB refresh and lookup workflows."""

from __future__ import annotations

import json
from pathlib import Path

import click

from chemdb.query import find_substance, get_hazard_summary, lookup_many, refresh_hazard_summary
from ingest.comptox_loader import main as comptox_main
from ingest.crosswalk import merge_substance_records
from ingest.echa_loader import main as echa_main


@click.group()
def cli() -> None:
    """ChemDB command line interface."""


@cli.command("lookup")
@click.argument("identifier")
@click.option("--json", "as_json", is_flag=True, help="Output lookup result as JSON.")
def lookup_cmd(identifier: str, as_json: bool) -> None:
    sub = find_substance(identifier)
    if not sub:
        click.echo("No match found.")
        return
    payload = {
        "substance_id": str(sub.substance_id),
        "name": sub.preferred_name,
        "cas_rn": sub.cas_rn,
        "ec_number": sub.ec_number,
        "dtxsid": sub.dtxsid,
        "summary": get_hazard_summary(sub.substance_id),
    }
    click.echo(json.dumps(payload, indent=2, default=str) if as_json else f"{payload['name']} ({payload['substance_id']})")


@cli.command("batch")
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option("--out", "output_file", required=True, type=click.Path(path_type=Path))
def batch_cmd(input_file: Path, output_file: Path) -> None:
    identifiers = [line.strip() for line in input_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    df = lookup_many(identifiers)
    df.to_csv(output_file, index=False)
    click.echo(f"Wrote {len(df)} rows to {output_file}")


@cli.command("refresh")
@click.option("--source", type=click.Choice(["comptox", "echa"]), required=True)
@click.option("--download", is_flag=True, help="Download upstream data before loading.")
@click.option(
    "--use-qsar-toolbox",
    is_flag=True,
    help="For echa: run local QSAR Toolbox WebSuite loader (same as USE_QSAR_TOOLBOX=true).",
)
def refresh_cmd(source: str, download: bool, use_qsar_toolbox: bool) -> None:
    if source == "comptox":
        argv = ["--download"] if download else []
        comptox_main(argv)
    elif source == "echa":
        argv = []
        if download:
            argv.append("--download")
        if use_qsar_toolbox:
            argv.append("--use-qsar-toolbox")
        echa_main(argv)
    merge_substance_records()
    refresh_hazard_summary()
    click.echo(f"Refresh completed for source={source}")


@cli.command("vacuum")
def vacuum_cmd() -> None:
    from chemdb.config import SessionLocal
    from sqlalchemy import text

    with SessionLocal() as session:
        session.execute(text("REINDEX DATABASE CURRENT_DATABASE();"))
        session.commit()
    refresh_hazard_summary()
    click.echo("Reindex complete and hazard_summary refreshed.")


if __name__ == "__main__":
    cli()
