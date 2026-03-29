import os
import re
import shlex
from typing import Any, Dict, List, Sequence, Tuple

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


DEFAULT_PARQUET_URL = (
    "https://huggingface.co/datasets/"
    "yantipin/afdb-complexes-metadata-19mil/resolve/main/"
    "model_entity_metadata_mapping.parquet"
)

NUMERIC_TYPE_TOKENS = (
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "hugeint",
    "real",
    "float",
    "double",
    "decimal",
    "numeric",
)
TEXT_TYPE_TOKENS = ("varchar", "char", "string", "text")
BOOL_TYPE_TOKENS = ("bool",)
FORCE_CATEGORICAL_COLUMNS = {"taxId"}
EXCLUDED_HISTOGRAM_COLUMNS = {"modelEntityId", "uniprotAccession", "chunk"}
CHUNK_DOWNLOAD_BASE_URL = (
    "https://ftp.ebi.ac.uk/pub/databases/alphafold/collaborations/nvda/models"
)
README_TXT_URL = (
    "https://huggingface.co/datasets/"
    "yantipin/afdb-complexes-metadata-19mil/resolve/main/README.txt"
)


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def chunk_to_tar_name(chunk_value: Any) -> str | None:
    if chunk_value is None:
        return None
    raw = str(chunk_value).strip()
    if not raw:
        return None
    digits = re.search(r"(\d+)", raw)
    if not digits:
        return None
    chunk_num = int(digits.group(1))
    return f"chunk_{chunk_num:04d}.tar"


def chunk_to_download_url(chunk_value: Any) -> str | None:
    tar_name = chunk_to_tar_name(chunk_value)
    if not tar_name:
        return None
    return f"{CHUNK_DOWNLOAD_BASE_URL}/{tar_name}"


def build_wget_script(chunk_model_pairs: Sequence[Tuple[str, str]]) -> str:
    normalized_pairs: List[Tuple[str, str]] = []
    for tar_name, model_entity_id in chunk_model_pairs:
        tar = str(tar_name).strip()
        model_id = str(model_entity_id).strip()
        if not tar or not model_id:
            continue
        normalized_pairs.append((tar, model_id))

    lines = [
        "#!/usr/bin/env bash",
        "set -u",
        "",
        "# Auto-generated from current filtered preview rows.",
        "# Directory where files will be downloaded. Change this if needed.",
        "download_dir=./",
        "flat_files_dir=\"${download_dir}/flat-files\"",
        f"base_url={shlex.quote(CHUNK_DOWNLOAD_BASE_URL)}",
        'mkdir -p "${download_dir}"',
        'mkdir -p "${flat_files_dir}"',
        "",
        "chunk_model_pairs=(",
    ]
    for tar_name, model_entity_id in normalized_pairs:
        pair_value = f"{tar_name}|{model_entity_id}"
        lines.append(f"  {shlex.quote(pair_value)}")
    lines.extend(
        [
            ")",
            "",
            'for pair in "${chunk_model_pairs[@]}"; do',
            '  IFS="|" read -r tar_name model_entity_id <<< "${pair}"',
            '  tar_path="${download_dir}/${tar_name}"',
            '  url="${base_url}/${tar_name}"',
            '  if [ ! -f "${tar_path}" ]; then',
            '    echo "Downloading ${url}"',
            '    wget -c -P "${download_dir}" "${url}"',
            "    rc=$?",
            "    if [ ${rc} -ne 0 ]; then",
            '      echo "wget failed for ${url} (exit code: ${rc})"',
            "      exit ${rc}",
            "    fi",
            "  else",
            '    echo "Chunk already downloaded: ${tar_path}"',
            "  fi",
            "",
            '  echo "Extracting files for ${model_entity_id} from ${tar_name}"',
            '  pattern="${model_entity_id}*"',
            '  tar -xf "${tar_path}" -C "${flat_files_dir}" --wildcards --no-anchored "${pattern}"',
            "  rc=$?",
            "  if [ ${rc} -ne 0 ]; then",
            '    echo "Extraction failed for modelEntityId ${model_entity_id} from ${tar_name} (exit code: ${rc})"',
            "    exit ${rc}",
            "  fi",
            "done",
            "",
            'echo "All downloads and extractions completed successfully."',
        ]
    )
    return "\n".join(lines) + "\n"


def normalize_hf_url(url: str) -> str:
    """Convert Hugging Face /blob/ URL into direct /resolve/ URL."""
    cleaned = (url or "").strip()
    if "/blob/" in cleaned:
        return cleaned.replace("/blob/", "/resolve/")
    return cleaned


@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    return con


def source_from_clause(parquet_url: str) -> str:
    return f"read_parquet({quote_str(parquet_url)})"


@st.cache_data(show_spinner=False, ttl=3600)
def get_schema(parquet_url: str) -> pd.DataFrame:
    con = get_connection()
    sql = f"DESCRIBE SELECT * FROM {source_from_clause(parquet_url)}"
    return con.execute(sql).fetchdf()


def classify_columns(schema_df: pd.DataFrame) -> Dict[str, List[str]]:
    numeric_cols: List[str] = []
    text_cols: List[str] = []
    categorical_cols: List[str] = []
    bool_cols: List[str] = []

    for row in schema_df.itertuples(index=False):
        col = str(row.column_name)
        col_type = str(row.column_type).lower()
        if col in FORCE_CATEGORICAL_COLUMNS:
            categorical_cols.append(col)
        elif any(tok in col_type for tok in NUMERIC_TYPE_TOKENS):
            numeric_cols.append(col)
        elif any(tok in col_type for tok in BOOL_TYPE_TOKENS):
            bool_cols.append(col)
            categorical_cols.append(col)
        elif any(tok in col_type for tok in TEXT_TYPE_TOKENS):
            text_cols.append(col)
            categorical_cols.append(col)
        else:
            # Non-numeric columns are often useful as categories.
            categorical_cols.append(col)

    return {
        "numeric": numeric_cols,
        "text": text_cols,
        "categorical": categorical_cols,
        "bool": bool_cols,
    }


@st.cache_data(show_spinner=False, ttl=3600)
def get_numeric_bounds(parquet_url: str, column: str) -> Tuple[float | None, float | None]:
    con = get_connection()
    col_id = quote_ident(column)
    sql = (
        "SELECT MIN({col}) AS min_v, MAX({col}) AS max_v "
        "FROM {src} WHERE {col} IS NOT NULL"
    ).format(col=col_id, src=source_from_clause(parquet_url))
    row = con.execute(sql).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


@st.cache_data(show_spinner=False, ttl=3600)
def get_top_categories(
    parquet_url: str,
    column: str,
    top_n: int | None = 50,
) -> List[str]:
    con = get_connection()
    col_id = quote_ident(column)
    if top_n is None:
        sql = (
            "SELECT CAST({col} AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM {src} WHERE {col} IS NOT NULL "
            "GROUP BY 1 ORDER BY n DESC"
        ).format(col=col_id, src=source_from_clause(parquet_url))
        df = con.execute(sql).fetchdf()
    else:
        sql = (
            "SELECT CAST({col} AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM {src} WHERE {col} IS NOT NULL "
            "GROUP BY 1 ORDER BY n DESC LIMIT ?"
        ).format(col=col_id, src=source_from_clause(parquet_url))
        df = con.execute(sql, [int(top_n)]).fetchdf()
    return df["v"].tolist()


@st.cache_data(show_spinner=False, ttl=3600)
def get_total_rows(parquet_url: str) -> int:
    con = get_connection()
    sql = f"SELECT COUNT(*) FROM {source_from_clause(parquet_url)}"
    return int(con.execute(sql).fetchone()[0])


def execute_df(sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
    con = get_connection()
    return con.execute(sql, list(params or [])).fetchdf()


def execute_scalar(sql: str, params: Sequence[Any] | None = None) -> Any:
    con = get_connection()
    row = con.execute(sql, list(params or [])).fetchone()
    return row[0] if row else None


def build_where_clause(
    text_search: str,
    text_search_columns: List[str],
    numeric_ranges: Dict[str, Tuple[float, float]],
    categorical_filters: Dict[str, List[str]],
    categorical_text_filters: Dict[str, str],
    categorical_any_value_filters: Dict[str, List[str]],
) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []

    search_value = text_search.strip()
    if search_value and text_search_columns:
        wildcard = f"%{search_value}%"
        search_terms: List[str] = []
        for col in text_search_columns:
            search_terms.append(f"CAST({quote_ident(col)} AS VARCHAR) ILIKE ?")
            params.append(wildcard)
        clauses.append("(" + " OR ".join(search_terms) + ")")

    for col, (min_val, max_val) in numeric_ranges.items():
        col_id = quote_ident(col)
        clauses.append(f"{col_id} IS NOT NULL AND {col_id} BETWEEN ? AND ?")
        params.extend([min_val, max_val])

    for col, values in categorical_filters.items():
        if not values:
            continue
        col_id = quote_ident(col)
        placeholders = ", ".join(["?"] * len(values))
        clauses.append(f"CAST({col_id} AS VARCHAR) IN ({placeholders})")
        params.extend(values)

    for col, value in categorical_text_filters.items():
        cleaned = value.strip()
        if not cleaned:
            continue
        col_id = quote_ident(col)
        clauses.append(f"CAST({col_id} AS VARCHAR) ILIKE ?")
        params.append(f"%{cleaned}%")

    for col, values in categorical_any_value_filters.items():
        cleaned_values = [v.strip() for v in values if v.strip()]
        if not cleaned_values:
            continue
        col_id = quote_ident(col)
        value_clauses = ["CAST({col} AS VARCHAR) = ?".format(col=col_id)] * len(cleaned_values)
        clauses.append("(" + " OR ".join(value_clauses) + ")")
        params.extend(cleaned_values)

    if not clauses:
        return "", params
    return "WHERE " + " AND ".join(clauses), params


def render_numeric_histogram(
    parquet_url: str,
    column: str,
    where_clause: str,
    where_params: Sequence[Any],
    bins: int,
) -> None:
    col_id = quote_ident(column)
    source = source_from_clause(parquet_url)
    sql = f"""
    WITH filtered AS (
        SELECT {col_id}::DOUBLE AS v
        FROM {source}
        {where_clause}
        {"AND" if where_clause else "WHERE"} {col_id} IS NOT NULL
    ),
    stats AS (
        SELECT MIN(v) AS min_v, MAX(v) AS max_v FROM filtered
    ),
    binned AS (
        SELECT
            CASE
                WHEN stats.max_v = stats.min_v THEN 0
                ELSE LEAST({bins} - 1, GREATEST(0, CAST(FLOOR((v - stats.min_v) / NULLIF(stats.max_v - stats.min_v, 0) * {bins}) AS INTEGER)))
            END AS bin_id,
            v
        FROM filtered, stats
    )
    SELECT
        bin_id,
        CASE
            WHEN stats.max_v = stats.min_v THEN stats.min_v
            ELSE stats.min_v + ((bin_id + 0.5) * ((stats.max_v - stats.min_v) / {bins}))
        END AS bin_center,
        COUNT(*) AS count
    FROM binned, stats
    GROUP BY 1, 2
    ORDER BY 1
    """
    df = execute_df(sql, where_params)
    if df.empty:
        st.info(f"No data to plot for `{column}` with current filters.")
        return

    fig = px.bar(
        df,
        x="bin_center",
        y="count",
        labels={"bin_center": "Bin center", "count": "Count"},
    )
    fig.update_layout(title=f"Numeric distribution: {column}", height=340)
    st.plotly_chart(fig, width="stretch")


def render_categorical_histogram(
    parquet_url: str,
    column: str,
    where_clause: str,
    where_params: Sequence[Any],
    top_n: int,
) -> None:
    col_id = quote_ident(column)
    source = source_from_clause(parquet_url)
    sql = f"""
    SELECT CAST({col_id} AS VARCHAR) AS category, COUNT(*) AS count
    FROM {source}
    {where_clause}
    {"AND" if where_clause else "WHERE"} {col_id} IS NOT NULL
    GROUP BY 1
    ORDER BY count DESC
    LIMIT ?
    """
    params = list(where_params) + [int(top_n)]
    df = execute_df(sql, params)
    if df.empty:
        st.info(f"No categories to plot for `{column}` with current filters.")
        return

    fig = px.bar(df, x="category", y="count", labels={"category": column, "count": "Count"})
    fig.update_layout(title=f"Categorical histogram: {column}", height=360)
    st.plotly_chart(fig, width="stretch")


def main() -> None:
    st.set_page_config(page_title="AlphaFold Complexes Download Dashboard", layout="wide")
    st.title("AlphaFold Complexes Download Dashboard")
    st.caption("Interactive dashboard powered by DuckDB over parquet on Hugging Face.")
    st.markdown(
        f"[README.txt]({README_TXT_URL}) - columns description",
    )

    parquet_url = normalize_hf_url(
        os.getenv(
            "AFDB_PARQUET_URL",
            DEFAULT_PARQUET_URL,
        )
    )

    with st.sidebar:
        st.header("Data source")
        user_url = st.text_input("Parquet URL", value=parquet_url)
        parquet_url = normalize_hf_url(user_url)
        st.caption("Tip: /blob/ links are automatically converted to /resolve/ links.")

    try:
        schema_df = get_schema(parquet_url)
    except Exception as exc:
        st.error(f"Failed to read parquet schema from source URL.\n\n{exc}")
        st.stop()

    column_groups = classify_columns(schema_df)
    numeric_cols = column_groups["numeric"]
    categorical_cols = column_groups["categorical"]
    categorical_cols = [col for col in categorical_cols if col != "taxId"]
    preferred_categorical_order = ["organismScientificName", "gene"]
    ordered_categorical_cols = [
        col for col in preferred_categorical_order if col in categorical_cols
    ]
    remaining_categorical_cols = [
        col for col in categorical_cols if col not in set(ordered_categorical_cols)
    ]
    categorical_cols = ordered_categorical_cols + remaining_categorical_cols
    text_cols = column_groups["text"]
    default_search_cols = text_cols[: min(4, len(text_cols))]

    with st.sidebar:
        st.header("Search and filters")
        text_search = st.text_input("Global text search")
        search_cols = st.multiselect(
            "Search in columns",
            options=text_cols,
            default=default_search_cols,
            help="Text search applies ILIKE over selected text columns.",
        )

        categorical_filter_cols = st.multiselect(
            "Categorical filters",
            options=categorical_cols,
            default=categorical_cols[: min(2, len(categorical_cols))],
        )
        categorical_filters: Dict[str, List[str]] = {}
        gene_text = ""
        uniprot_accessions_text = ""
        for col in categorical_filter_cols:
            if col == "gene":
                gene_text = st.text_input("gene contains", value="")
            elif col == "uniprotAccession":
                uniprot_accessions_text = st.text_input(
                    "uniprotAccession values (space-separated)",
                    value="",
                    help="Enter multiple accession values separated by spaces.",
                )
            else:
                top_n = None if col == "organismScientificName" else 60
                opts = get_top_categories(parquet_url, col, top_n=top_n)
                default_values: List[str] = []
                if col == "organismScientificName" and "Homo sapiens" in opts:
                    default_values = ["Homo sapiens"]
                selected = st.multiselect(
                    f"{col} values",
                    options=opts,
                    default=default_values,
                )
                if selected:
                    categorical_filters[col] = selected

        categorical_text_filters: Dict[str, str] = {}
        if "gene" in categorical_filter_cols and gene_text.strip():
            categorical_text_filters["gene"] = gene_text.strip()
        categorical_any_value_filters: Dict[str, List[str]] = {}
        if "uniprotAccession" in categorical_filter_cols and uniprot_accessions_text.strip():
            categorical_any_value_filters["uniprotAccession"] = re.split(
                r"\s+",
                uniprot_accessions_text.strip(),
            )

        numeric_filter_cols = st.multiselect(
            "Numeric filters",
            options=numeric_cols,
            default=numeric_cols[: min(3, len(numeric_cols))],
        )
        numeric_ranges: Dict[str, Tuple[float, float]] = {}
        for col in numeric_filter_cols:
            min_v, max_v = get_numeric_bounds(parquet_url, col)
            if min_v is None or max_v is None:
                continue
            if min_v == max_v:
                st.caption(f"`{col}` has a single value: {min_v}")
                continue
            selected = st.slider(
                f"{col} range",
                min_value=float(min_v),
                max_value=float(max_v),
                value=(float(min_v), float(max_v)),
            )
            numeric_ranges[col] = selected

        st.header("Chart options")
        hist_bins = st.slider("Numeric histogram bins", min_value=10, max_value=100, value=30)
        cat_top_n = st.slider("Categorical top-N", min_value=10, max_value=100, value=30)
        preview_limit = st.slider("Preview row limit", min_value=10, max_value=500, value=100, step=10)

    where_clause, where_params = build_where_clause(
        text_search=text_search,
        text_search_columns=search_cols,
        numeric_ranges=numeric_ranges,
        categorical_filters=categorical_filters,
        categorical_text_filters=categorical_text_filters,
        categorical_any_value_filters=categorical_any_value_filters,
    )

    source = source_from_clause(parquet_url)
    total_rows = get_total_rows(parquet_url)
    filtered_count_raw = execute_scalar(
        f"SELECT COUNT(*) FROM {source} {where_clause}",
        where_params,
    )
    filtered_count = int(filtered_count_raw) if filtered_count_raw is not None else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total rows", f"{total_rows:,}")
    c2.metric("Rows after filters", f"{filtered_count:,}")
    c3.metric("Columns", f"{len(schema_df):,}")
    c4.metric("Total dataset size", "~30 TB")
    c5.metric("Average chunk .tar size", "7.6 GB")

    if filtered_count == 0:
        st.warning("No rows match the current filters. Adjust filters or search terms.")
        st.stop()

    preview_sql = f"SELECT * FROM {source} {where_clause} LIMIT ?"
    preview_df = execute_df(preview_sql, list(where_params) + [int(preview_limit)])
    dataframe_column_config = None
    chunk_model_pairs: List[Tuple[str, str]] = []
    if "chunk" in preview_df.columns:
        preview_df["chunk_download"] = preview_df["chunk"].map(chunk_to_download_url)
        preferred_columns = ["chunk_download", "chunk"]
        ordered_columns = preferred_columns + [
            col for col in preview_df.columns if col not in set(preferred_columns)
        ]
        preview_df = preview_df[ordered_columns]
        if "modelEntityId" in preview_df.columns:
            seen_pairs: set[Tuple[str, str]] = set()
            for row in preview_df[["chunk", "modelEntityId"]].itertuples(index=False):
                tar_name = chunk_to_tar_name(row.chunk)
                model_entity_id = str(row.modelEntityId).strip()
                if not tar_name or not model_entity_id:
                    continue
                pair = (tar_name, model_entity_id)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                chunk_model_pairs.append(pair)
            
            def chunk_sort_key(pair: Tuple[str, str]) -> Tuple[int, str, str]:
                match = re.search(r"(\d+)", pair[0])
                chunk_num = int(match.group(1)) if match else 10**9
                return (chunk_num, pair[0], pair[1])

            chunk_model_pairs.sort(
                key=chunk_sort_key
            )
        dataframe_column_config = {
            "chunk_download": st.column_config.LinkColumn(
                "⬇",
                help="Direct tar archive URL for this chunk",
                display_text="⬇",
            )
        }

    button_col, explanation_col = st.columns([1.2, 4], gap="small")
    with button_col:
        st.download_button(
            "Make download script (.sh)",
            data=build_wget_script(chunk_model_pairs),
            file_name="download_selected_chunks.sh",
            mime="text/x-shellscript",
            disabled=not chunk_model_pairs,
            help="Generate a shell script from current filtered rows.",
            width="content",
        )
    with explanation_col:
        st.markdown(
            "<div style='text-align:left; margin-top:0.25rem;'>"
            "<span style='font-size:1.0rem;'>"
            "Generates a shell script that downloads chunk .tar files for the current "
            "filtered selection with wget and extracts only the listed models into a single "
            "flat directory."
            "</span>"
            "</div>",
            unsafe_allow_html=True,
        )

    st.subheader("Filtered preview")
    st.dataframe(
        preview_df,
        width="stretch",
        hide_index=True,
        column_config=dataframe_column_config,
    )

    st.subheader("Categorical histograms")
    if "organismScientificName" not in categorical_cols:
        st.info("`organismScientificName` is not available in this dataset.")
    else:
        render_categorical_histogram(
            parquet_url,
            "organismScientificName",
            where_clause,
            where_params,
            top_n=cat_top_n,
        )

    numeric_histogram_cols = [col for col in numeric_cols if col not in EXCLUDED_HISTOGRAM_COLUMNS]
    st.subheader("Numeric distributions")
    numeric_chart_cols = st.multiselect(
        "Choose numeric columns to chart",
        options=numeric_histogram_cols,
        default=numeric_histogram_cols[: min(3, len(numeric_histogram_cols))],
    )
    if not numeric_chart_cols:
        st.info("Select at least one numeric column to render numeric distribution charts.")
    for col in numeric_chart_cols:
        render_numeric_histogram(parquet_url, col, where_clause, where_params, bins=hist_bins)


if __name__ == "__main__":
    main()
