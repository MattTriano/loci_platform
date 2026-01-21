import csv
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, Optional

from psycopg2 import sql
from psycopg2.extras import execute_values

from db.core import PostgresConnector


class PgType(Enum):
    """PostgreSQL data types for CSV ingestion."""

    BOOLEAN = "BOOLEAN"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE = "DOUBLE PRECISION"
    TEXT = "TEXT"
    VARCHAR = "VARCHAR"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    JSON = "JSONB"
    UUID = "UUID"


TYPE_HIERARCHY = [
    PgType.BOOLEAN,
    PgType.SMALLINT,
    PgType.INTEGER,
    PgType.BIGINT,
    PgType.NUMERIC,
    PgType.DOUBLE,
    PgType.DATE,
    PgType.TIMESTAMP,
    PgType.TIMESTAMPTZ,
    PgType.UUID,
    PgType.JSON,
    PgType.TEXT,
]


@dataclass
class ColumnSchema:
    name: str
    pg_type: PgType
    nullable: bool = True
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

    def to_sql_type(self) -> str:
        if self.pg_type == PgType.VARCHAR and self.max_length:
            return f"VARCHAR({self.max_length})"
        if self.pg_type == PgType.NUMERIC and self.precision:
            if self.scale:
                return f"NUMERIC({self.precision},{self.scale})"
            return f"NUMERIC({self.precision})"
        return self.pg_type.value


class TypeInferrer:
    UUID_PATTERN = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    TIMESTAMP_PATTERN = re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?$"
    )
    TIMESTAMPTZ_PATTERN = re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})$"
    )
    JSON_PATTERN = re.compile(r"^[\[{].*[\]}]$", re.DOTALL)

    SMALLINT_RANGE = (-32768, 32767)
    INTEGER_RANGE = (-2147483648, 2147483647)
    BIGINT_RANGE = (-9223372036854775808, 9223372036854775807)

    @classmethod
    def infer_type(cls, value: str) -> tuple[PgType, dict[str, Any]]:
        """
        Infer the PostgreSQL type for a single value.

        Returns:
            Tuple of (PgType, metadata dict with length/precision info)
        """
        if value is None or value.strip() == "":
            return PgType.TEXT, {"nullable": True}

        value = value.strip()
        metadata: dict[str, Any] = {"nullable": False}

        if value.lower() in ("true", "false", "t", "f", "yes", "no", "1", "0"):
            if value.lower() in ("true", "false", "t", "f", "yes", "no"):
                return PgType.BOOLEAN, metadata

        if cls.UUID_PATTERN.match(value):
            return PgType.UUID, metadata

        if cls.TIMESTAMPTZ_PATTERN.match(value):
            return PgType.TIMESTAMPTZ, metadata
        if cls.TIMESTAMP_PATTERN.match(value):
            return PgType.TIMESTAMP, metadata
        if cls.DATE_PATTERN.match(value):
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return PgType.DATE, metadata
            except ValueError:
                pass

        try:
            int_val = int(value)
            if cls.SMALLINT_RANGE[0] <= int_val <= cls.SMALLINT_RANGE[1]:
                return PgType.SMALLINT, metadata
            if cls.INTEGER_RANGE[0] <= int_val <= cls.INTEGER_RANGE[1]:
                return PgType.INTEGER, metadata
            if cls.BIGINT_RANGE[0] <= int_val <= cls.BIGINT_RANGE[1]:
                return PgType.BIGINT, metadata
            return PgType.NUMERIC, metadata
        except ValueError:
            pass

        try:
            dec_val = Decimal(value)
            sign, digits, exponent = dec_val.as_tuple()

            if exponent < 0:
                scale = abs(exponent)
                precision = len(digits)
                metadata["precision"] = precision
                metadata["scale"] = scale
                return PgType.NUMERIC, metadata
            else:
                if "e" in value.lower() or "E" in value:
                    return PgType.DOUBLE, metadata
                return PgType.NUMERIC, metadata
        except InvalidOperation:
            pass

        if cls.JSON_PATTERN.match(value):
            try:
                import json

                json.loads(value)
                return PgType.JSON, metadata
            except (json.JSONDecodeError, ValueError):
                pass

        metadata["length"] = len(value)
        return PgType.TEXT, metadata


class TypePromoter:
    """Handles type promotion when combining multiple inferred types."""

    @staticmethod
    def promote(type1: PgType, type2: PgType) -> PgType:
        """
        Promote two types to a common compatible type.

        Returns the more general type that can hold values of both types.
        """
        if type1 == type2:
            return type1

        if type1 == PgType.TEXT or type2 == PgType.TEXT:
            specific = type1 if type2 == PgType.TEXT else type2
            if specific in (PgType.VARCHAR,):
                return PgType.TEXT
            return PgType.TEXT

        numeric_types = {
            PgType.SMALLINT,
            PgType.INTEGER,
            PgType.BIGINT,
            PgType.NUMERIC,
            PgType.DOUBLE,
        }
        if type1 in numeric_types and type2 in numeric_types:
            order = [
                PgType.SMALLINT,
                PgType.INTEGER,
                PgType.BIGINT,
                PgType.NUMERIC,
                PgType.DOUBLE,
            ]
            return order[max(order.index(type1), order.index(type2))]

        datetime_types = {PgType.DATE, PgType.TIMESTAMP, PgType.TIMESTAMPTZ}
        if type1 in datetime_types and type2 in datetime_types:
            if PgType.TIMESTAMPTZ in (type1, type2):
                return PgType.TIMESTAMPTZ
            if PgType.TIMESTAMP in (type1, type2):
                return PgType.TIMESTAMP
            return PgType.DATE

        if PgType.BOOLEAN in (type1, type2):
            other = type1 if type2 == PgType.BOOLEAN else type2
            if other in numeric_types:
                return other
        return PgType.TEXT


@dataclass
class TableSchema:
    """Complete schema for a database table."""

    table_name: str
    columns: list[ColumnSchema]
    include_ingested_at: bool = True

    def to_create_statement(
        self, schema: str = "public", if_not_exists: bool = True
    ) -> str:
        """Generate CREATE TABLE statement."""
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        column_defs = []
        for col in self.columns:
            null_clause = "" if col.nullable else " NOT NULL"
            column_defs.append(
                f"    {quote_identifier(col.name)} {col.to_sql_type()}{null_clause}"
            )

        if self.include_ingested_at:
            column_defs.append(
                "    ingested_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')"
            )

        columns_sql = ",\n".join(column_defs)

        return f"""CREATE TABLE {exists_clause}{quote_identifier(schema)}.{quote_identifier(self.table_name)} (
{columns_sql}
);"""


def quote_identifier(name: str) -> str:
    """Safely quote a PostgreSQL identifier."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def clean_column_name(name: str) -> str:
    """
    Clean a column name for PostgreSQL compatibility.

    - Converts to lowercase
    - Replaces spaces and special chars with underscores
    - Ensures it starts with a letter or underscore
    """
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    if not cleaned:
        cleaned = "unnamed_column"
    return cleaned


class CSVAnalyzer:
    """Analyzes CSV files to determine schema."""

    def __init__(
        self,
        sample_size: int = 10000,
        clean_column_names: bool = True,
        varchar_threshold: int = 255,
        use_bigint: bool = True,
        use_text: bool = True,
        use_numeric: bool = True,
    ):
        """
        Initialize the analyzer.

        Args:
            sample_size: Number of rows to sample for type inference
            clean_column_names: Whether to clean column names for PostgreSQL
            varchar_threshold: Max length before using TEXT instead of VARCHAR
                              (only applies if use_text=False)
            use_bigint: If True, use BIGINT for all integer types (default: True)
            use_text: If True, use TEXT for all string columns instead of VARCHAR
                      (default: True, avoids truncation errors)
            use_numeric: If True, use NUMERIC for all decimal types instead of
                         REAL/DOUBLE (default: True, avoids precision loss)
        """
        self.sample_size = sample_size
        self.clean_column_names = clean_column_names
        self.varchar_threshold = varchar_threshold
        self.use_bigint = use_bigint
        self.use_text = use_text
        self.use_numeric = use_numeric

    def analyze_file(
        self,
        filepath: str | Path,
        delimiter: str = ",",
        encoding: str = "utf-8",
        table_name: Optional[str] = None,
    ) -> TableSchema:
        """
        Analyze a CSV file and return the inferred schema.

        Args:
            filepath: Path to the CSV file
            delimiter: CSV delimiter character
            encoding: File encoding
            table_name: Name for the table (defaults to filename)

        Returns:
            TableSchema with inferred column types
        """
        filepath = Path(filepath)

        if table_name is None:
            table_name = clean_column_name(filepath.stem)

        with open(filepath, "r", encoding=encoding, newline="") as f:
            return self._analyze_stream(f, delimiter, table_name)

    def analyze_string(
        self,
        csv_content: str,
        delimiter: str = ",",
        table_name: str = "imported_data",
    ) -> TableSchema:
        """Analyze CSV content from a string."""
        return self._analyze_stream(StringIO(csv_content), delimiter, table_name)

    def _analyze_stream(
        self,
        stream,
        delimiter: str,
        table_name: str,
    ) -> TableSchema:
        """Internal method to analyze a CSV stream."""
        reader = csv.DictReader(stream, delimiter=delimiter)

        if reader.fieldnames is None:
            raise ValueError("CSV file has no headers")

        column_info: dict[str, dict[str, Any]] = {}
        for name in reader.fieldnames:
            clean_name = clean_column_name(name) if self.clean_column_names else name
            column_info[name] = {
                "clean_name": clean_name,
                "inferred_type": None,
                "nullable": False,
                "max_length": 0,
                "precision": 0,
                "scale": 0,
            }
        for i, row in enumerate(reader):
            if i >= self.sample_size:
                break

            for original_name, value in row.items():
                if original_name not in column_info:
                    continue

                info = column_info[original_name]
                if value is None or value.strip() == "":
                    info["nullable"] = True
                    continue

                inferred, metadata = TypeInferrer.infer_type(value)
                if info["inferred_type"] is None:
                    info["inferred_type"] = inferred
                else:
                    info["inferred_type"] = TypePromoter.promote(
                        info["inferred_type"], inferred
                    )

                if "length" in metadata:
                    info["max_length"] = max(info["max_length"], metadata["length"])
                if "precision" in metadata:
                    info["precision"] = max(info["precision"], metadata["precision"])
                if "scale" in metadata:
                    info["scale"] = max(info["scale"], metadata["scale"])

        columns = []
        for original_name in reader.fieldnames:
            info = column_info[original_name]
            pg_type = info["inferred_type"] or PgType.TEXT
            if self.use_bigint and pg_type in (PgType.SMALLINT, PgType.INTEGER):
                pg_type = PgType.BIGINT
            if self.use_numeric and pg_type in (PgType.REAL, PgType.DOUBLE):
                pg_type = PgType.NUMERIC

            max_len = info["max_length"]
            if pg_type == PgType.TEXT:
                if self.use_text:
                    pass
                elif 0 < max_len <= self.varchar_threshold:
                    pg_type = PgType.VARCHAR
                    max_len = min(
                        max_len * 2, 65535
                    )  # Double observed length as buffer
            elif pg_type == PgType.VARCHAR and self.use_text:
                pg_type = PgType.TEXT
                max_len = 0

            col = ColumnSchema(
                name=info["clean_name"],
                pg_type=pg_type,
                nullable=info["nullable"],
                max_length=max_len if pg_type == PgType.VARCHAR else None,
                precision=info["precision"]
                if info["precision"] > 0 and not self.use_numeric
                else None,
                scale=info["scale"]
                if info["scale"] > 0 and not self.use_numeric
                else None,
            )
            columns.append(col)

        return TableSchema(table_name=table_name, columns=columns)


class ValueParser:
    """Parses string values into Python types based on PostgreSQL column types."""

    @staticmethod
    def parse(value: str, pg_type: PgType) -> Any:
        """
        Parse a string value into the appropriate Python type.

        Args:
            value: String value from CSV
            pg_type: Target PostgreSQL type

        Returns:
            Parsed Python value, or None for empty/null values
        """
        if value is None or value.strip() == "":
            return None

        value = value.strip()

        try:
            if pg_type == PgType.BOOLEAN:
                return ValueParser._parse_boolean(value)
            elif pg_type in (PgType.SMALLINT, PgType.INTEGER, PgType.BIGINT):
                return int(value)
            elif pg_type in (PgType.NUMERIC, PgType.REAL, PgType.DOUBLE):
                return Decimal(value)
            elif pg_type == PgType.DATE:
                return datetime.strptime(value, "%Y-%m-%d").date()
            elif pg_type == PgType.TIMESTAMP:
                return ValueParser._parse_timestamp(value)
            elif pg_type == PgType.TIMESTAMPTZ:
                return ValueParser._parse_timestamptz(value)
            elif pg_type == PgType.UUID:
                return value
            elif pg_type == PgType.JSON:
                import json

                return json.loads(value)
            else:
                return value
        except (ValueError, InvalidOperation) as e:
            # If parsing fails, return as string and let PostgreSQL handle it
            # This provides more graceful degradation
            return value

    @staticmethod
    def _parse_boolean(value: str) -> bool:
        """Parse boolean values."""
        lower = value.lower()
        if lower in ("true", "t", "yes", "y", "1"):
            return True
        elif lower in ("false", "f", "no", "n", "0"):
            return False
        raise ValueError(f"Cannot parse '{value}' as boolean")

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        """Parse timestamp without timezone."""
        formats = [
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse '{value}' as timestamp")

    @staticmethod
    def _parse_timestamptz(value: str) -> datetime:
        """Parse timestamp with timezone."""
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        import re

        match = re.match(r"(.+)([+-])(\d{2})(\d{2})$", value)
        if match:
            value = f"{match.group(1)}{match.group(2)}{match.group(3)}:{match.group(4)}"
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass

        formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse '{value}' as timestamptz")


class TableInspector:
    """Inspects existing PostgreSQL tables to retrieve their schema."""

    PG_TYPE_MAP = {
        "boolean": PgType.BOOLEAN,
        "bool": PgType.BOOLEAN,
        "smallint": PgType.SMALLINT,
        "int2": PgType.SMALLINT,
        "integer": PgType.INTEGER,
        "int": PgType.INTEGER,
        "int4": PgType.INTEGER,
        "bigint": PgType.BIGINT,
        "int8": PgType.BIGINT,
        "numeric": PgType.NUMERIC,
        "decimal": PgType.NUMERIC,
        "real": PgType.REAL,
        "float4": PgType.REAL,
        "double precision": PgType.DOUBLE,
        "float8": PgType.DOUBLE,
        "text": PgType.TEXT,
        "character varying": PgType.VARCHAR,
        "varchar": PgType.VARCHAR,
        "character": PgType.VARCHAR,
        "char": PgType.VARCHAR,
        "date": PgType.DATE,
        "timestamp without time zone": PgType.TIMESTAMP,
        "timestamp": PgType.TIMESTAMP,
        "timestamp with time zone": PgType.TIMESTAMPTZ,
        "timestamptz": PgType.TIMESTAMPTZ,
        "json": PgType.JSON,
        "jsonb": PgType.JSON,
        "uuid": PgType.UUID,
    }

    def __init__(self, connector: PostgresConnector):
        self.connector = connector

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """Check if a table exists in the database."""
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """
        result = self.connector.query(sql, (schema, table_name))
        return result[0]["exists"] if result else False

    def get_table_schema(self, table_name: str, schema: str = "public") -> TableSchema:
        """
        Retrieve the schema of an existing table.

        Args:
            table_name: Name of the table
            schema: PostgreSQL schema name

        Returns:
            TableSchema object representing the table structure
        """
        sql = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s 
            AND table_name = %s
            ORDER BY ordinal_position;
        """

        rows = self.connector.query(sql, (schema, table_name))

        if not rows:
            raise ValueError(f"Table {schema}.{table_name} not found or has no columns")

        columns = []
        has_ingested_at = False

        for row in rows:
            col_name = row["column_name"]

            if col_name == "ingested_at":
                has_ingested_at = True
                continue

            data_type = row["data_type"].lower()
            pg_type = self.PG_TYPE_MAP.get(data_type, PgType.TEXT)

            col = ColumnSchema(
                name=col_name,
                pg_type=pg_type,
                nullable=(row["is_nullable"] == "YES"),
                max_length=row["character_maximum_length"],
                precision=row["numeric_precision"],
                scale=row["numeric_scale"],
            )
            columns.append(col)

        return TableSchema(
            table_name=table_name,
            columns=columns,
            include_ingested_at=has_ingested_at,
        )


class CSVIngester:
    """Handles CSV ingestion into PostgreSQL."""

    def __init__(
        self,
        connector: PostgresConnector,
        analyzer: Optional[CSVAnalyzer] = None,
    ):
        """
        Initialize the ingester.

        Args:
            connector: PostgresConnector instance (should be connected)
            analyzer: Optional CSVAnalyzer (creates default if not provided)
        """
        self.connector = connector
        self.analyzer = analyzer or CSVAnalyzer()
        self.inspector = TableInspector(connector)

    def ingest(
        self,
        filepath: str | Path,
        table_name: Optional[str] = None,
        schema: str = "public",
        delimiter: str = ",",
        encoding: str = "utf-8",
        batch_size: int = 1000,
        drop_existing: bool = False,
        analyze_only: bool = False,
        strict_parsing: bool = False,
    ) -> dict[str, Any]:
        """
        Ingest a CSV file into PostgreSQL.

        If the table exists, uses its schema for parsing. Otherwise, infers
        the schema from the CSV and creates the table.

        Args:
            filepath: Path to the CSV file
            table_name: Target table name (defaults to filename)
            schema: PostgreSQL schema name
            delimiter: CSV delimiter
            encoding: File encoding
            batch_size: Rows per INSERT batch
            drop_existing: Whether to drop existing table first
            analyze_only: If True, only return schema without ingesting
            strict_parsing: If True, raise errors on parse failures

        Returns:
            Dictionary with ingestion results:
            - table_schema: The TableSchema object
            - rows_inserted: Number of rows inserted
            - create_statement: The CREATE TABLE SQL (if table was created)
            - table_existed: Whether the table existed before ingestion
            - parse_errors: Count of values that failed to parse
        """
        filepath = Path(filepath)

        if table_name is None:
            table_name = clean_column_name(filepath.stem)

        if not self.connector.is_connected():
            self.connector.connect()

        result = {
            "table_schema": None,
            "create_statement": None,
            "rows_inserted": 0,
            "table_existed": False,
            "parse_errors": 0,
        }

        if drop_existing:
            self.connector.execute(
                f"DROP TABLE IF EXISTS {quote_identifier(schema)}.{quote_identifier(table_name)}"
            )

        table_exists = self.inspector.table_exists(table_name, schema)
        result["table_existed"] = table_exists

        if table_exists:
            table_schema = self.inspector.get_table_schema(table_name, schema)
            result["table_schema"] = table_schema
        else:
            table_schema = self.analyzer.analyze_file(
                filepath, delimiter, encoding, table_name
            )
            result["table_schema"] = table_schema

            create_stmt = table_schema.to_create_statement(schema)
            result["create_statement"] = create_stmt

            if not analyze_only:
                self.connector.execute(create_stmt)

        if analyze_only:
            return result

        with open(filepath, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            csv_headers = next(reader)

        column_mapping = self._build_column_mapping(csv_headers, table_schema)

        if not column_mapping:
            raise ValueError("No matching columns found between CSV and table schema")
        insert_columns = [col for col, _ in column_mapping.values()]
        column_names = [col.name for col in insert_columns]

        columns_sql = ", ".join(quote_identifier(name) for name in column_names)
        full_table_name = f"{quote_identifier(schema)}.{quote_identifier(table_name)}"

        rows_inserted = 0
        parse_errors = 0

        with open(filepath, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            next(reader)  # Skip header
            batch = []
            for row_num, row in enumerate(
                reader, start=2
            ):  # Start at 2 (1-indexed, after header)
                values = []

                for csv_idx, (col_schema, _) in column_mapping.items():
                    if csv_idx < len(row):
                        raw_value = row[csv_idx]
                        try:
                            parsed_value = ValueParser.parse(
                                raw_value, col_schema.pg_type
                            )
                            values.append(parsed_value)
                        except Exception as e:
                            parse_errors += 1
                            if strict_parsing:
                                raise ValueError(
                                    f"Row {row_num}, column '{col_schema.name}': "
                                    f"Cannot parse '{raw_value}' as {col_schema.pg_type.value}: {e}"
                                )
                            # Graceful fallback: use raw value
                            values.append(raw_value if raw_value.strip() else None)
                    else:
                        values.append(None)

                batch.append(tuple(values))

                if len(batch) >= batch_size:
                    rows_inserted += self._insert_batch(
                        full_table_name, columns_sql, batch
                    )
                    batch = []

            if batch:
                rows_inserted += self._insert_batch(full_table_name, columns_sql, batch)

        result["rows_inserted"] = rows_inserted
        result["parse_errors"] = parse_errors
        return result

    def _build_column_mapping(
        self,
        csv_headers: list[str],
        table_schema: TableSchema,
    ) -> dict[int, tuple[ColumnSchema, str]]:
        """
        Build a mapping from CSV column indices to table columns.

        Returns:
            Dict mapping csv_index -> (ColumnSchema, cleaned_csv_header)
        """
        mapping = {}
        table_columns = {col.name.lower(): col for col in table_schema.columns}
        for idx, header in enumerate(csv_headers):
            cleaned = clean_column_name(header)
            if cleaned.lower() in table_columns:
                mapping[idx] = (table_columns[cleaned.lower()], cleaned)
            elif header.lower() in table_columns:
                mapping[idx] = (table_columns[header.lower()], header)
        return mapping

    def _insert_batch(
        self,
        table_name: str,
        columns_sql: str,
        batch: list[tuple],
    ) -> int:
        """Insert a batch of rows using execute_values for performance."""
        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES %s"

        with self.connector.get_cursor() as cursor:
            execute_values(cursor, insert_sql, batch, page_size=len(batch))
            return cursor.rowcount


def ingest_csv(
    filepath: str | Path,
    pg_connector: PostgresConnector,
    schema: str,
    table_name: str,
    **kwargs,
) -> dict[str, Any]:
    """
    Convenience function to ingest a CSV file into PostgreSQL.

    If the target table exists, uses its schema for type parsing.
    If the table doesn't exist, infers types from the CSV and creates it.

    Args:
        filepath: Path to CSV file
        pg_connector: A PostgresConnector instance
        table_name: Target table name
        schema: PostgreSQL schema
        **kwargs: Additional arguments passed to CSVIngester.ingest():
            - delimiter: CSV delimiter (default: ',')
            - encoding: File encoding (default: 'utf-8')
            - batch_size: Rows per batch (default: 1000)
            - drop_existing: Drop table if exists (default: False)
            - analyze_only: Only return schema (default: False)
            - strict_parsing: Raise on parse errors (default: False)

    Returns:
        Ingestion results dictionary with keys:
        - table_schema: TableSchema object
        - rows_inserted: Number of rows inserted
        - create_statement: CREATE TABLE SQL (if table was created)
        - table_existed: Whether table existed before ingestion
        - parse_errors: Count of parse failures
    """
    with pg_connector as conn:
        ingester = CSVIngester(conn)
        return ingester.ingest(filepath, table_name, schema, **kwargs)
