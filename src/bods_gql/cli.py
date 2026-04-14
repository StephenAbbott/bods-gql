"""CLI for bods-gql: Convert BODS v0.4 data for GQL querying on BigQuery."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from bods_gql.converter.mapper import map_statements
from bods_gql.converter.reader import read_statements
from bods_gql.graph_schema.property_graph import (
    generate_create_graph_ddl,
    generate_create_graph_for_bodsdata,
    generate_drop_graph_ddl,
)
from bods_gql.queries import circular_ownership, corporate_groups, ubo_detection


@click.group()
@click.version_option()
def main():
    """bods-gql: Convert BODS v0.4 data for GQL graph querying."""


@main.command()
@click.argument("bods_file", type=click.Path(exists=True))
@click.option("--format", "output_format", type=click.Choice(["json", "summary"]), default="summary")
def info(bods_file: str, output_format: str):
    """Show information about a BODS file and its graph mapping."""
    statements = list(read_statements(bods_file))
    result = map_statements(iter(statements))

    if output_format == "json":
        click.echo(json.dumps({
            "total_statements": len(statements),
            "entity_nodes": len(result.entity_nodes),
            "person_nodes": len(result.person_nodes),
            "ownership_edges": len(result.ownership_edges),
            "mapping_errors": len(result.errors),
        }, indent=2))
    else:
        click.echo(f"BODS file: {bods_file}")
        click.echo(f"Total statements: {len(statements)}")
        click.echo(f"  Entity nodes:    {len(result.entity_nodes)}")
        click.echo(f"  Person nodes:    {len(result.person_nodes)}")
        click.echo(f"  Ownership edges: {len(result.ownership_edges)}")
        if result.errors:
            click.echo(f"  Mapping errors:  {len(result.errors)}")


@main.command()
@click.argument("bods_file", type=click.Path(exists=True))
@click.option("--output-dir", "-o", type=click.Path(), default=".", help="Output directory for CSV files")
def to_csv(bods_file: str, output_dir: str):
    """Convert BODS file to CSV node/edge tables for BigQuery upload."""
    import csv

    statements = read_statements(bods_file)
    result = map_statements(statements)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Write entity nodes
    if result.entity_nodes:
        entity_path = out / "entity_nodes.csv"
        fields = list(result.entity_nodes[0].to_dict().keys())
        with open(entity_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for node in result.entity_nodes:
                writer.writerow(node.to_dict())
        click.echo(f"Wrote {len(result.entity_nodes)} entity nodes to {entity_path}")

    # Write person nodes
    if result.person_nodes:
        person_path = out / "person_nodes.csv"
        fields = list(result.person_nodes[0].to_dict().keys())
        with open(person_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for node in result.person_nodes:
                writer.writerow(node.to_dict())
        click.echo(f"Wrote {len(result.person_nodes)} person nodes to {person_path}")

    # Write ownership edges
    if result.ownership_edges:
        edge_path = out / "ownership_edges.csv"
        fields = list(result.ownership_edges[0].to_dict().keys())
        with open(edge_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for edge in result.ownership_edges:
                writer.writerow(edge.to_dict())
        click.echo(f"Wrote {len(result.ownership_edges)} ownership edges to {edge_path}")

    if result.errors:
        click.echo(f"WARNING: {len(result.errors)} statements could not be mapped", err=True)


@main.command()
@click.option("--dataset", "-d", required=True, help="BigQuery dataset (e.g. my_project.my_dataset)")
@click.option("--graph-name", "-g", default="OwnershipGraph", help="Property graph name")
def schema(dataset: str, graph_name: str):
    """Generate CREATE PROPERTY GRAPH DDL for custom BODS tables."""
    ddl = generate_create_graph_ddl(dataset, graph_name)
    click.echo(ddl)


@main.command()
@click.option(
    "--source",
    type=click.Choice(["gleif_version_0_4", "uk_version_0_4"]),
    default="gleif_version_0_4",
    help="bodsdata dataset to use",
)
def bodsdata_schema(source: str):
    """Generate CREATE PROPERTY GRAPH DDL for existing bodsdata BigQuery datasets."""
    ddl = generate_create_graph_for_bodsdata(source)
    click.echo(ddl)


@main.command()
@click.option("--dataset", "-d", required=True, help="BigQuery dataset (e.g. my_project.my_dataset)")
@click.option("--graph-name", "-g", default="OwnershipGraph")
def drop_graph(dataset: str, graph_name: str):
    """Generate DROP PROPERTY GRAPH DDL."""
    ddl = generate_drop_graph_ddl(dataset, graph_name)
    click.echo(ddl)


@main.command()
@click.option("--dataset", "-d", required=True, help="BigQuery dataset")
@click.option("--graph-name", "-g", default="OwnershipGraph")
@click.option(
    "--query-type",
    "-q",
    type=click.Choice([
        "find-owners",
        "find-owned",
        "find-ubos",
        "find-ubos-sql",
        "entities-without-ubos",
        "corporate-group",
        "top-parents",
        "jurisdiction-analysis",
        "group-metrics",
        "all-groups",
        "find-cycles",
        "check-cycle",
        "mutual-ownership",
        "cycle-stats",
    ]),
    required=True,
    help="Which GQL query to generate",
)
@click.option("--max-depth", type=int, default=10, help="Max traversal depth")
@click.option("--threshold", type=float, default=25.0, help="UBO threshold percentage")
@click.option("--limit", type=int, default=100, help="Result limit")
def query(dataset: str, graph_name: str, query_type: str, max_depth: int, threshold: float, limit: int):
    """Generate GQL queries for beneficial ownership analysis."""
    queries = {
        "find-owners": lambda: ubo_detection.find_owners(dataset, graph_name, max_depth),
        "find-owned": lambda: ubo_detection.find_owned_entities(dataset, graph_name, max_depth),
        "find-ubos": lambda: ubo_detection.find_ubos_gql(dataset, graph_name, max_depth),
        "find-ubos-sql": lambda: ubo_detection.find_ubos_with_sql(dataset, graph_name, max_depth, threshold),
        "entities-without-ubos": lambda: ubo_detection.find_entities_without_ubos(dataset, graph_name),
        "corporate-group": lambda: corporate_groups.corporate_group(dataset, graph_name, max_depth),
        "top-parents": lambda: corporate_groups.top_level_parents(dataset, graph_name, limit),
        "jurisdiction-analysis": lambda: corporate_groups.group_jurisdiction_analysis(dataset, graph_name, max_depth),
        "group-metrics": lambda: corporate_groups.group_metrics(dataset, graph_name, max_depth),
        "all-groups": lambda: corporate_groups.all_groups(dataset, graph_name, limit),
        "find-cycles": lambda: circular_ownership.find_cycles(dataset, graph_name, max_depth),
        "check-cycle": lambda: circular_ownership.check_entity_cycle(dataset, graph_name, max_depth),
        "mutual-ownership": lambda: circular_ownership.mutual_ownership(dataset, graph_name),
        "cycle-stats": lambda: circular_ownership.cycle_stats(dataset, graph_name, max_depth),
    }
    click.echo(queries[query_type]())


@main.command()
@click.argument("bods_file", type=click.Path(exists=True))
@click.option("--project", "-p", required=True, help="GCP project ID")
@click.option("--dataset", "-d", required=True, help="BigQuery dataset name")
@click.option("--graph-name", "-g", default="OwnershipGraph")
@click.option("--location", default="US", help="BigQuery dataset location")
def load(bods_file: str, project: str, dataset: str, graph_name: str, location: str):
    """Load BODS file into BigQuery and create property graph.

    Requires google-cloud-bigquery credentials configured.
    """
    from bods_gql.converter.bigquery_loader import BigQueryLoader

    click.echo(f"Reading BODS file: {bods_file}")
    statements = read_statements(bods_file)
    result = map_statements(statements)

    click.echo(f"Mapped: {len(result.entity_nodes)} entities, "
               f"{len(result.person_nodes)} persons, "
               f"{len(result.ownership_edges)} relationships")

    if result.errors:
        click.echo(f"WARNING: {len(result.errors)} mapping errors", err=True)

    loader = BigQueryLoader(project, dataset)
    click.echo(f"Creating dataset {project}.{dataset} if needed...")
    loader.create_dataset_if_needed(location)

    click.echo("Loading tables...")
    counts = loader.load_tables(result)
    for table, count in counts.items():
        click.echo(f"  {table}: {count} rows")

    click.echo(f"Creating property graph {graph_name}...")
    loader.create_property_graph(graph_name)
    click.echo("Done. Property graph is ready for GQL queries.")


if __name__ == "__main__":
    main()
