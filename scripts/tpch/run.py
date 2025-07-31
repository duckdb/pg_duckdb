#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "pandas",
#     "matplotlib",
# ]
# ///
import argparse
import subprocess
import sys
import shlex
import os
import tempfile
import pandas as pd
import matplotlib.pyplot as plt
import glob
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote


def eprint(*args, **kwargs):
    """eprint prints to stderr"""

    print(*args, file=sys.stderr, **kwargs)


@contextmanager
def cd(path: Path | str):
    """Sets the cwd within the context"""

    origin = Path().resolve()
    try:
        eprint(f"+ cd {shlex.quote(str(path))}")
        os.chdir(path)
        yield
    finally:
        eprint(f"+ cd {shlex.quote(str(origin))}")
        os.chdir(origin)


def run(command, *args, check=True, shell=None, silent=False, **kwargs):
    """Runs the given command and prints it to stderr"""

    if shell is None:
        shell = isinstance(command, str)

    if not shell:
        command = list(map(str, command))

    if not silent:
        if shell:
            eprint(f"+ {command}")
        else:
            eprint(f"+ {shlex.join(command)}")
    if silent:
        kwargs.setdefault("stdout", subprocess.DEVNULL)
    return subprocess.run(command, *args, check=check, shell=shell, **kwargs)


def fails(command, *args, **kwargs):
    return run(command, *args, check=False, **kwargs).returncode != 0


def capture(command, *args, stdout=subprocess.PIPE, encoding="utf-8", **kwargs):
    return run(
        command, *args, stdout=stdout, encoding=encoding, **kwargs
    ).stdout.removesuffix("\n")


def confirm(question, default=True):
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default is True:
        prompt = " [Y/n] "
    elif default is False:
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return default
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def choose(question, choices):
    while True:
        choice = input(question + " [" + "/".join(choices) + "]\n").strip()
        if choice in choices:
            return choice
        eprint("Please respond with one of the following:", choices)


def get_pg_credentials(username=None, password=None):
    """Get consistent PostgreSQL credentials with CLI args taking precedence"""

    final_username = username or os.environ.get("PGUSER", "postgres")
    final_password = password or os.environ.get("PGPASSWORD", "")
    return final_username, final_password


def build_pg_env(username, password):
    """Build environment variables for PostgreSQL authentication"""

    env = {**os.environ}
    if username:
        env["PGUSER"] = username
    if password:
        env["PGPASSWORD"] = password
    return env


def create_tables(
    database_name, schema_name, username, password, no_indexes=False, pk_only=False
):
    """Create tables using SQL files"""

    pg_env = build_pg_env(username, password)

    # Create schema if it doesn't exist
    run(
        [
            "psql",
            "-d",
            database_name,
            "-c",
            f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"',
        ],
        env=pg_env,
    )

    # Determine which schema file to use
    if no_indexes:
        sql_file = "create-schema-no-indexes.sql"
        eprint("Using no-indexes schema")
    elif pk_only:
        sql_file = "create-schema-pk.sql"
        eprint("Using primary keys only schema")
    else:
        sql_file = "create-schema.sql"
        eprint("Using full schema with indexes")

    if not os.path.exists(sql_file):
        eprint(f"ERROR: Schema file {sql_file} not found")
        sys.exit(1)

    eprint(f"Executing schema file: {sql_file}")
    run(
        [
            "psql",
            "-d",
            database_name,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            f"SET search_path = '{schema_name}'",
            "-f",
            sql_file,
        ],
        env=pg_env,
    )


def generate_config(
    template_path,
    output_path,
    database_name,
    schema_name,
    scale_factor,
    force_duckdb_execution,
    username,
    password,
):
    """Generate config file from template"""

    if not os.path.exists(template_path):
        eprint(f"Template file {template_path} not found")
        sys.exit(1)

    with open(template_path, "r") as f:
        content = f.read()

    # Build options string
    options = [f"-c search_path={schema_name}"]
    if force_duckdb_execution:
        options.append("-c duckdb.force_execution=true")
    options_str = quote(" ".join(options))

    # Format template with values
    content = content.format(
        database=database_name,
        options=options_str,
        scalefactor=scale_factor,
        username=username,
        password=password,
    )

    with open(output_path, "w") as f:
        f.write(content)

    eprint(f"Generated config file: {output_path}")


def find_latest_result_file():
    """Find the most recent tpch result file"""

    pattern = "results/tpch_*.raw.csv"
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)


def parse_results(csv_file):
    """Parse benchmark results from CSV file"""

    df = pd.read_csv(csv_file)
    # Convert latency from microseconds to milliseconds
    df["Latency (ms)"] = df["Latency (microseconds)"] / 1000
    return df


def create_comparison_chart(result_files, output_file="comparison.png"):
    """Create a bar chart comparing query runtimes across multiple configurations"""

    # Parse all result files
    dataframes = {}
    for label, file_path in result_files.items():
        dataframes[label] = parse_results(file_path)

    # Merge all dataframes on Transaction Name
    comparison = None
    for label, df in dataframes.items():
        df_subset = df[["Transaction Name", "Latency (ms)"]].rename(
            columns={"Latency (ms)": f"Latency (ms)_{label}"}
        )
        if comparison is None:
            comparison = df_subset
        else:
            comparison = pd.merge(comparison, df_subset, on="Transaction Name")

    # Find PostgreSQL baseline (prefer any label containing "PostgreSQL")
    postgres_label = None
    for label in dataframes.keys():
        if "PostgreSQL" in label:
            postgres_label = label
            break

    # If no PostgreSQL found, use first label as baseline
    if postgres_label is None:
        postgres_label = list(dataframes.keys())[0]
        eprint(
            f"Warning: No PostgreSQL label found, using {postgres_label} as baseline"
        )

    # Check if we need log scale - if max/min ratio > 100, use log scale
    all_values = []
    for label in dataframes.keys():
        col_name = f"Latency (ms)_{label}"
        all_values.extend(comparison[col_name].values)

    max_val = max(all_values)
    min_val = min(v for v in all_values if v > 0)  # Exclude zeros
    use_log_scale = (max_val / min_val) > 100

    if use_log_scale:
        eprint(
            f"Using log scale due to large range: {max_val:.0f}ms / {min_val:.0f}ms = {max_val / min_val:.1f}x"
        )

    # Create speedup chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

    x = range(len(comparison))
    width = 0.8 / len(dataframes)
    colors = ["#336699", "#ff9933", "#66cc66", "#cc6666"]
    labels = list(dataframes.keys())

    # Plot 1: Absolute times (linear or log scale based on data)
    bars1 = []
    for i, label in enumerate(labels):
        col_name = f"Latency (ms)_{label}"
        bars1.append(
            ax1.bar(
                [pos + (i - len(labels) / 2 + 0.5) * width for pos in x],
                comparison[col_name],
                width,
                label=label,
                alpha=0.8,
                color=colors[i % len(colors)],
            )
        )

    ax1.set_xlabel("Query")
    if use_log_scale:
        ax1.set_ylabel("Latency (ms) - Log Scale")
        ax1.set_title("TPC-H Query Performance (Absolute Times)")
        ax1.set_yscale("log")
    else:
        ax1.set_ylabel("Latency (ms)")
        ax1.set_title("TPC-H Query Performance (Absolute Times)")

    ax1.set_xticks(x)
    ax1.set_xticklabels(comparison["Transaction Name"], rotation=45)
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    # Plot 2: Speedup relative to PostgreSQL
    postgres_col = f"Latency (ms)_{postgres_label}"

    for i, label in enumerate(labels):
        if label == postgres_label:
            continue  # Skip baseline

        col_name = f"Latency (ms)_{label}"
        # Calculate speedup: postgres_time / other_time
        # Values > 1 mean the other system is faster than PostgreSQL
        speedups = comparison[postgres_col] / comparison[col_name]

        # Use different colors for speedup vs slowdown
        bar_colors = ["#22cc22" if s >= 1.0 else "#cc2222" for s in speedups]

        bars = ax2.bar(
            [pos + (i - len(labels) / 2 + 0.5) * width for pos in x],
            speedups,
            width,
            label=f"{label} vs {postgres_label}",
            alpha=0.8,
            color=bar_colors,
        )

        # Add speedup labels on bars
        for j, bar in enumerate(bars):
            height = bar.get_height()
            if height >= 1.0:
                label_text = f"{height:.1f}x"
            else:
                label_text = f"{1 / height:.1f}x slower"

            ax2.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + 0.05 if height >= 1 else height - 0.1,
                label_text,
                ha="center",
                va="bottom" if height >= 1 else "top",
                fontsize=8,
                rotation=90,
            )

    # Add horizontal line at y=1 (no speedup/slowdown)
    ax2.axhline(y=1, color="black", linestyle="--", alpha=0.7, linewidth=1)
    ax2.set_xlabel("Query")
    ax2.set_ylabel(f"Speedup relative to {postgres_label}")
    ax2.set_title(f"TPC-H Query Speedup vs {postgres_label}")
    ax2.set_xticks(x)
    ax2.set_xticklabels(comparison["Transaction Name"], rotation=45)
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.show()

    eprint(f"Comparison chart saved to {output_file}")

    # Print detailed statistics
    print(f"\nDetailed Query Results (baseline: {postgres_label}):")
    print("=" * 80)

    postgres_col = f"Latency (ms)_{postgres_label}"

    for _, row in comparison.iterrows():
        query = row["Transaction Name"]
        postgres_time = row[postgres_col]
        print(f"\n{query}:")
        print(f"  {postgres_label:15}: {postgres_time:8.1f} ms (baseline)")

        for label in labels:
            if label == postgres_label:
                continue
            col_name = f"Latency (ms)_{label}"
            latency = row[col_name]
            speedup = postgres_time / latency

            if speedup >= 1.0:
                print(f"  {label:15}: {latency:8.1f} ms ({speedup:.2f}x faster)")
            else:
                print(f"  {label:15}: {latency:8.1f} ms ({1 / speedup:.2f}x slower)")

    print(f"\nSummary (baseline: {postgres_label}):")
    print("=" * 50)

    postgres_total = comparison[postgres_col].sum()
    print(f"Total {postgres_label} time: {postgres_total:.0f} ms (baseline)")

    for label in labels:
        if label == postgres_label:
            continue
        col_name = f"Latency (ms)_{label}"
        total = comparison[col_name].sum()
        speedup = postgres_total / total

        if speedup >= 1.0:
            print(f"Total {label} time: {total:.0f} ms ({speedup:.2f}x faster overall)")
        else:
            print(
                f"Total {label} time: {total:.0f} ms ({1 / speedup:.2f}x slower overall)"
            )


def run_benchmark(args, force_duckdb=False, results_suffix=""):
    """Run a single benchmark iteration"""

    dump_name = f"tpch{args.scale_factor}".replace(".", "")
    schema_name = args.schema_name or dump_name

    config_template = "tpch_config_template.xml"
    username, password = get_pg_credentials(args.username, args.password)
    pg_env = build_pg_env(username, password)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=f"_tpch_config{results_suffix}.xml"
    ) as temp_config:
        config_file = temp_config.name

        generate_config(
            config_template,
            config_file,
            args.database_name,
            schema_name,
            args.scale_factor,
            force_duckdb or args.force_duckdb_execution,
            username,
            password,
        )

        if not args.skip_generate and not args.skip_load:
            run(
                [
                    "duckdb",
                    "-c",
                    f"CALL dbgen(sf={args.scale_factor}); EXPORT DATABASE '{dump_name}' (FORMAT CSV, DELIMITER '|')",
                ]
            )

        if not args.skip_load:
            # Create schema using SQL files instead of benchbase
            create_tables(
                args.database_name,
                schema_name,
                username,
                password,
                args.no_indexes,
                args.pk_only,
            )

            # Load data
            dump_dir = schema_name
            if os.path.exists(dump_dir):
                run(
                    [
                        "psql",
                        "-d",
                        args.database_name,
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-c",
                        f"SET search_path = '{schema_name}'",
                        "-f",
                        "../load-psql.sql",
                    ],
                    cwd=dump_dir,
                    env=pg_env,
                )
            else:
                eprint(f"ERROR: Dump directory {dump_dir} not found")
                sys.exit(1)

        if not args.skip_execute:
            run(
                [
                    "docker",
                    "run",
                    "--network=host",
                    "--rm",
                    "--env",
                    "BENCHBASE_PROFILE=postgres",
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    "-v",
                    "./results:/benchbase/results",
                    "-v",
                    f"{os.path.dirname(config_file)}:/tmp_config/",
                    "benchbase.azurecr.io/benchbase",
                    "--bench",
                    "tpch",
                    "-c",
                    f"/tmp_config/{os.path.basename(config_file)}",
                    "--execute=true",
                    "--directory",
                    "/benchbase/results",
                ]
            )

        return find_latest_result_file()


def main():
    parser = argparse.ArgumentParser(description="Run TPCH generation script.")
    parser.add_argument(
        "--database-name",
        type=str,
        default="postgres",
        help="Database name to connect to.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        default=None,
        help="Schema name to export database to.",
    )
    parser.add_argument(
        "--scale-factor",
        type=str,
        default="1",
        help="Scale factor for data generation.",
    )
    parser.add_argument(
        "--username",
        "-U",
        type=str,
        help="PostgreSQL username (overrides PGUSER env var).",
    )
    parser.add_argument(
        "--password",
        type=str,
        help="PostgreSQL password (overrides PGPASSWORD env var).",
    )
    parser.add_argument(
        "--skip-generate", action="store_true", help="Skip the data generation step."
    )
    parser.add_argument(
        "--skip-load", action="store_true", help="Skip the data loading step."
    )
    parser.add_argument(
        "--skip-execute", action="store_true", help="Skip the benchmark execution step."
    )
    parser.add_argument(
        "--force-duckdb-execution",
        action="store_true",
        help="Set duckdb.force_execution to true",
    )
    parser.add_argument(
        "--cold", action="store_true", help="Run cold benchmark (after data loading)"
    )
    parser.add_argument(
        "--hot", action="store_true", help="Run hot benchmark (reusing loaded data)"
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Run benchmark twice (PostgreSQL vs DuckDB) and create comparison chart",
    )
    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Create schema without indexes (uses create-schema-no-indexes.sql)",
    )
    parser.add_argument(
        "--pk-only",
        action="store_true",
        help="Create schema with primary keys only (uses create-schema-pk.sql)",
    )

    args = parser.parse_args()

    # Validate mutually exclusive flags
    if args.no_indexes and args.pk_only:
        eprint("ERROR: --no-indexes and --pk-only cannot be used together")
        sys.exit(1)

    # Build filename suffix for schema type
    schema_suffix = ""
    if args.no_indexes:
        schema_suffix = "_no_indexes"
    elif args.pk_only:
        schema_suffix = "_pk_only"

    file_prefix = f"tpch{args.scale_factor}".replace(".", "") + schema_suffix

    # Default to hot run if neither --cold nor --hot specified
    if not args.cold and not args.hot:
        args.hot = True

    if args.compare and args.cold and args.hot:
        # Full comparison: PostgreSQL vs DuckDB, hot vs cold
        eprint("Running full comparison: PostgreSQL vs DuckDB, hot vs cold")
        if args.force_duckdb_execution:
            eprint(
                "WARNING: --force-duckdb-execution is ignored in full comparison mode"
            )
            args.force_duckdb_execution = False

        result_files = {}

        # PostgreSQL cold
        eprint("=== Running PostgreSQL benchmark (cold) ===")
        result_files["PostgreSQL (Cold)"] = run_benchmark(
            args, force_duckdb=False, results_suffix="_pg_cold"
        )

        # PostgreSQL hot
        eprint("=== Running PostgreSQL benchmark (hot) ===")
        args.skip_load = True  # Reuse loaded data for hot run
        result_files["PostgreSQL (Hot)"] = run_benchmark(
            args, force_duckdb=False, results_suffix="_pg_hot"
        )

        # DuckDB cold - need to reset skip_load for new engine
        args.skip_load = False
        # but reuse existing data
        args.skip_generate = True
        eprint("=== Running DuckDB benchmark (cold) ===")
        result_files["DuckDB (Cold)"] = run_benchmark(
            args, force_duckdb=True, results_suffix="_duck_cold"
        )

        # DuckDB hot
        eprint("=== Running DuckDB benchmark (hot) ===")
        args.skip_load = True  # Reuse loaded data for hot run
        result_files["DuckDB (Hot)"] = run_benchmark(
            args, force_duckdb=True, results_suffix="_duck_hot"
        )

        if all(result_files.values()):
            create_comparison_chart(result_files, f"{file_prefix}_full_comparison.png")
        else:
            eprint("ERROR: Could not find all result files for comparison")
            sys.exit(1)

    elif args.compare:
        eprint("Running comparison benchmark: PostgreSQL vs DuckDB")
        if args.force_duckdb_execution:
            eprint("WARNING: --force-duckdb-execution is ignored in comparison mode")
            args.force_duckdb_execution = False

        result_files = {}

        # PostgreSQL
        eprint("=== Running PostgreSQL benchmark ===")
        result_files["PostgreSQL"] = run_benchmark(
            args, force_duckdb=False, results_suffix="_postgres"
        )

        # DuckDB
        eprint("=== Running DuckDB benchmark ===")
        result_files["DuckDB"] = run_benchmark(
            args, force_duckdb=True, results_suffix="_duckdb"
        )

        if all(result_files.values()):
            create_comparison_chart(
                result_files, f"{file_prefix}_postgres_duckdb_comparison.png"
            )
        else:
            eprint("ERROR: Could not find result files for comparison")
            sys.exit(1)

    elif args.cold and args.hot:
        # Both cold and hot runs for current configuration
        eprint("Running both cold and hot benchmarks")
        engine = "DuckDB" if args.force_duckdb_execution else "PostgreSQL"

        result_files = {}

        # Cold run first
        eprint(f"=== Running {engine} benchmark (cold) ===")
        result_files[f"{engine} (Cold)"] = run_benchmark(args, results_suffix="_cold")

        # Hot run - reuse loaded data
        eprint(f"=== Running {engine} benchmark (hot) ===")
        args.skip_load = True
        result_files[f"{engine} (Hot)"] = run_benchmark(args, results_suffix="_hot")

        if all(result_files.values()):
            create_comparison_chart(
                result_files, f"{file_prefix}_{engine.lower()}_cold_hot.png"
            )
        else:
            eprint("ERROR: Could not find result files for comparison")
            sys.exit(1)

    else:
        # Single benchmark run
        if args.cold:
            eprint("Running cold benchmark")
        else:
            eprint("Running hot benchmark")
            if not args.skip_load:
                eprint("Note: For true hot run, consider using --skip-load")

        run_benchmark(args)


if __name__ == "__main__":
    main()
