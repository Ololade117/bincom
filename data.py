import re
import csv
from typing import Dict, List, Optional, Tuple
import pandas as pd


def _parse_insert_blocks(sql_text: str, table: str) -> List[Dict]:
    """Extract INSERT blocks for a table and return list of dict rows."""
    # Find INSERT ... (col1, col2, ...) VALUES (...),(...); blocks for the table
    pattern = re.compile(r"INSERT INTO `" + re.escape(table) + r"`\s*\(([^)]+)\)\s*VALUES\s*(.*?);",
                         re.IGNORECASE | re.DOTALL)
    rows = []
    for match in pattern.finditer(sql_text):
        cols_raw, values_raw = match.groups()
        cols = [c.strip().strip('`') for c in cols_raw.split(',')]

        # Find each parenthesized tuple (non-greedy)
        tuples = re.findall(r"\(([^)]*)\)", values_raw, re.DOTALL)
        for tup in tuples:
            # Use csv reader with single-quote quoting to split values robustly
            reader = csv.reader([tup], delimiter=',', quotechar="'", skipinitialspace=True)
            parsed = next(reader)
            parsed = [_convert_sql_value(v) for v in parsed]
            # If number of values doesn't match columns, skip
            if len(parsed) != len(cols):
                # fallback: pad or truncate conservatively
                if len(parsed) < len(cols):
                    parsed += [None] * (len(cols) - len(parsed))
                else:
                    parsed = parsed[: len(cols)]
            rows.append(dict(zip(cols, parsed)))
    return rows


def _convert_sql_value(token: str):
    token = token.strip()
    if token.upper() == 'NULL' or token == '':
        return None
    # if quoted string (csv already removed quotes) it will be raw string; try to parse numbers
    # try int
    try:
        if re.fullmatch(r"-?\d+", token):
            return int(token)
        if re.fullmatch(r"-?\d+\.\d+", token):
            return float(token)
    except Exception:
        pass
    # leave as string
    return token


def load_tables_from_sql(sql_path: str, tables: List[str]) -> Dict[str, pd.DataFrame]:
    """Load specified tables from a SQL dump and return DataFrames.

    This function only parses INSERT statements and does not rely on a live DB.
    It is tolerant of MySQL-specific DDL by only extracting the data rows.
    """
    with open(sql_path, 'r', encoding='utf-8', errors='ignore') as f:
        sql_text = f.read()

    result = {}
    for table in tables:
        rows = _parse_insert_blocks(sql_text, table)
        if not rows:
            # return empty DataFrame with no rows if table missing
            result[table] = pd.DataFrame()
        else:
            result[table] = pd.DataFrame(rows)
    return result


def _load_state_mapping(sql_path: str, mapping_csv: Optional[str] = None, mapping_dict: Optional[Dict] = None) -> Dict[int, str]:
    """Return a mapping from state_id -> state_name.

    Priority:
      1. mapping_dict param if provided
      2. mapping_csv path if provided (CSV with columns 'state_id','state_name')
      3. 'state' table found in SQL dump (columns 'state_id'/'state_name' or similar)
      4. fallback to creating names like 'State <id>' based on `lga.state_id` values
    """
    if mapping_dict:
        return {int(k): v for k, v in mapping_dict.items()}

    if mapping_csv:
        df = pd.read_csv(mapping_csv)
        if 'state_id' in df.columns and 'state_name' in df.columns:
            return {int(row['state_id']): row['state_name'] for _, row in df.iterrows()}

    # Try to parse a state table from SQL
    tables = load_tables_from_sql(sql_path, ['state'])
    state_table = tables.get('state', pd.DataFrame())
    if not state_table.empty:
        # try to find state id/name columns
        id_col = None
        name_col = None
        for c in state_table.columns:
            if 'id' in c.lower():
                id_col = c
            if 'name' in c.lower():
                name_col = c
        if id_col and name_col:
            return {int(r[id_col]): r[name_col] for _, r in state_table.iterrows()}

    # Fallback: derive unique ids from lga table
    tables = load_tables_from_sql(sql_path, ['lga'])
    lga = tables.get('lga', pd.DataFrame())
    mapping = {}
    if not lga.empty and 'state_id' in lga.columns:
        for sid in sorted(lga['state_id'].dropna().unique()):
            mapping[int(sid)] = f"State {int(sid)}"

    # Overrides for known state name fixes
    # Ensure state id 25 is correctly labeled as 'Delta State'
    mapping[25] = 'Delta State'

    return mapping


def build_polling_unit_results_df(sql_path: str, state_mapping: Optional[Dict[int, str]] = None) -> pd.DataFrame:
    """Build a DataFrame where each row is a polling unit and columns are state, lga, ward and parties.

    Adds a `state_name` column using `state_mapping` if provided or by inferring from the SQL.
    """
    tables = load_tables_from_sql(sql_path, ['polling_unit', 'ward', 'lga', 'announced_pu_results'])

    pu = tables['polling_unit']
    ward = tables['ward']
    lga = tables['lga']
    announced = tables['announced_pu_results']

    if pu.empty:
        raise ValueError('polling_unit table not found or empty in SQL file')
    if announced.empty:
        raise ValueError('announced_pu_results table not found or empty in SQL file')

    announced = announced.rename(columns={
        'polling_unit_uniqueid': 'polling_unit_uniqueid',
        'party_abbreviation': 'party_abbreviation',
        'party_score': 'party_score'
    })

    announced['polling_unit_uniqueid'] = announced['polling_unit_uniqueid'].apply(lambda v: int(v) if v is not None else None)

    party_pivot = announced.pivot_table(index='polling_unit_uniqueid',
                                        columns='party_abbreviation',
                                        values='party_score',
                                        aggfunc='sum',
                                        fill_value=0).reset_index()

    pu = pu.rename(columns={'uniqueid': 'polling_unit_uniqueid',
                            'polling_unit_name': 'polling_unit_name',
                            'lga_id': 'lga_id',
                            'ward_id': 'ward_id'})

    merged = pu.merge(party_pivot, on='polling_unit_uniqueid', how='left')

    # merge ward and lga
    if not ward.empty and 'ward_id' in ward.columns and 'ward_name' in ward.columns:
        merged = merged.merge(ward[['ward_id', 'ward_name']], on='ward_id', how='left')
    if not lga.empty and 'lga_id' in lga.columns and 'lga_name' in lga.columns:
        merged = merged.merge(lga[['lga_id', 'lga_name', 'state_id']], on='lga_id', how='left')

    # State mapping
    if state_mapping is None:
        state_mapping = _load_state_mapping(sql_path)
    merged['state_name'] = merged['state_id'].map(state_mapping) if 'state_id' in merged.columns else None

    # Reorder columns and ensure party columns are ints
    known = ['polling_unit_uniqueid', 'polling_unit_name', 'polling_unit_number', 'ward_id', 'lga_id', 'ward_name', 'lga_name', 'state_id', 'state_name']
    # Determine party columns from the pivot (safer than guessing from merged columns)
    party_columns = [c for c in party_pivot.columns if c != 'polling_unit_uniqueid']
    party_columns = [c for c in party_columns if c in merged.columns]
    final_cols = [c for c in known if c in merged.columns] + sorted(party_columns)
    final_df = merged[final_cols].copy()
    for c in party_columns:
        final_df[c] = final_df[c].fillna(0).astype(int)

    return final_df


# Helper functions for filtering and UI

def append_polling_unit_to_sql(sql_path: str, pu_row: Dict) -> str:
    """Append a new `polling_unit` INSERT statement to the SQL dump file.

    pu_row should contain keys for the main columns. Missing values will be written as NULL.
    Returns the SQL statement that was written.
    """
    col_order = ['uniqueid', 'polling_unit_id', 'ward_id', 'lga_id', 'uniquewardid',
                 'polling_unit_number', 'polling_unit_name', 'polling_unit_description',
                 'lat', 'long', 'entered_by_user', 'date_entered', 'user_ip_address']

    def _format_val(v):
        if v is None:
            return 'NULL'
        if isinstance(v, (int, float)):
            return str(int(v))
        s = str(v).replace("'", "\\'")
        return f"'{s}'"

    values = [_format_val(pu_row.get(c)) for c in col_order]
    stmt = f"INSERT INTO `polling_unit` (`{'`,`'.join(col_order)}`) VALUES ({', '.join(values)});\n"

    with open(sql_path, 'a', encoding='utf-8') as f:
        f.write(stmt)

    return stmt


def add_polling_unit_to_df(df: pd.DataFrame, pu_row: Dict) -> pd.DataFrame:
    """Return a new DataFrame with the polling unit row appended.

    Ensures party columns are present and set to 0 for the new row.
    """
    new = {}
    for col in df.columns:
        if col in pu_row:
            new[col] = pu_row[col]
        elif col in ['polling_unit_uniqueid', 'polling_unit_name', 'polling_unit_number', 'ward_id', 'lga_id', 'ward_name', 'lga_name', 'state_id', 'state_name']:
            new[col] = pu_row.get(col, None)
        else:
            # party column
            new[col] = 0

    new_df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
    # Ensure party columns are ints
    party_cols = [c for c in new_df.columns if c not in ['polling_unit_uniqueid', 'polling_unit_name', 'polling_unit_number', 'ward_id', 'lga_id', 'ward_name', 'lga_name', 'state_id', 'state_name']]
    for c in party_cols:
        new_df[c] = new_df[c].fillna(0).astype(int)
    return new_df



def get_states(df: pd.DataFrame) -> List[Tuple[int, str]]:
    """Return list of (state_id, state_name) sorted by name."""
    if 'state_id' not in df.columns and 'state_name' not in df.columns:
        return []
    df_states = df[['state_id', 'state_name']].drop_duplicates()
    df_states = df_states.dropna(subset=['state_id'])
    df_states['state_id'] = df_states['state_id'].astype(int)
    df_states['state_name'] = df_states['state_name'].fillna(df_states['state_id'].astype(str))
    rows = df_states.sort_values('state_name')[['state_id', 'state_name']].values.tolist()
    return [(int(r[0]), r[1]) for r in rows]


def get_lgas_by_state(df: pd.DataFrame, state_id: Optional[int] = None) -> List[Tuple[int, str]]:
    if state_id is None:
        df_lgas = df[['lga_id', 'lga_name']].drop_duplicates()
    else:
        df_lgas = df[df['state_id'] == state_id][['lga_id', 'lga_name']].drop_duplicates()
    df_lgas = df_lgas.dropna(subset=['lga_id'])
    df_lgas['lga_id'] = df_lgas['lga_id'].astype(int)
    return [(int(r[0]), r[1]) for r in df_lgas.sort_values('lga_name')[['lga_id', 'lga_name']].values.tolist()]


def get_wards_by_lga(df: pd.DataFrame, lga_id: Optional[int] = None) -> List[Tuple[int, str]]:
    if lga_id is None:
        df_wards = df[['ward_id', 'ward_name']].drop_duplicates()
    else:
        df_wards = df[df['lga_id'] == lga_id][['ward_id', 'ward_name']].drop_duplicates()
    df_wards = df_wards.dropna(subset=['ward_id'])
    df_wards['ward_id'] = df_wards['ward_id'].astype(int)
    return [(int(r[0]), r[1]) for r in df_wards.sort_values('ward_name')[['ward_id', 'ward_name']].values.tolist()]


def filter_results(df: pd.DataFrame, state_id: Optional[int] = None, lga_id: Optional[int] = None, ward_id: Optional[int] = None, polling_unit_id: Optional[int] = None) -> pd.DataFrame:
    out = df.copy()
    if state_id is not None and 'state_id' in out.columns:
        out = out[out['state_id'] == int(state_id)]
    if lga_id is not None and 'lga_id' in out.columns:
        out = out[out['lga_id'] == int(lga_id)]
    if ward_id is not None and 'ward_id' in out.columns:
        out = out[out['ward_id'] == int(ward_id)]
    if polling_unit_id is not None and 'polling_unit_uniqueid' in out.columns:
        out = out[out['polling_unit_uniqueid'] == int(polling_unit_id)]
    return out


if __name__ == '__main__':
    import pathlib
    sql_path = pathlib.Path(__file__).parent / 'bincom_test.sql'

    # Build df and print small sample
    df = build_polling_unit_results_df(str(sql_path))
    print('Built DataFrame with', df.shape[0], 'rows and', df.shape[1], 'columns')
    print('Sample states:', get_states(df)[:5])
    print(df.head())
    print(df.head())