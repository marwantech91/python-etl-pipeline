import pandas as pd
import logging
from typing import Callable, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
import time

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """Context passed through pipeline stages."""
    data: Optional[pd.DataFrame] = None
    metadata: dict = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.utcnow)
    errors: list = field(default_factory=list)


class Pipeline:
    """ETL Pipeline framework."""

    def __init__(self, name: str):
        self.name = name
        self._extractors: list[Callable] = []
        self._transformers: list[Callable] = []
        self._loaders: list[Callable] = []
        self._error_handlers: list[Callable] = []
        self._before_hooks: list[Callable] = []
        self._after_hooks: list[Callable] = []

    def extract(self, func: Callable) -> Callable:
        """Decorator to register an extractor."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        self._extractors.append(wrapper)
        return wrapper

    def transform(self, func: Callable) -> Callable:
        """Decorator to register a transformer."""
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            return func(df, *args, **kwargs)
        self._transformers.append(wrapper)
        return wrapper

    def load(self, func: Callable) -> Callable:
        """Decorator to register a loader."""
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            return func(df, *args, **kwargs)
        self._loaders.append(wrapper)
        return wrapper

    def on_error(self, func: Callable) -> Callable:
        """Decorator to register an error handler."""
        self._error_handlers.append(func)
        return func

    def before(self, func: Callable) -> Callable:
        """Decorator to register a before hook."""
        self._before_hooks.append(func)
        return func

    def after(self, func: Callable) -> Callable:
        """Decorator to register an after hook."""
        self._after_hooks.append(func)
        return func

    def run(self) -> PipelineContext:
        """Execute the pipeline."""
        context = PipelineContext()
        logger.info(f"Starting pipeline: {self.name}")

        try:
            # Before hooks
            for hook in self._before_hooks:
                hook(context)

            # Extract
            start = time.time()
            dfs = []
            for extractor in self._extractors:
                df = extractor()
                if isinstance(df, pd.DataFrame):
                    dfs.append(df)
                elif isinstance(df, list):
                    dfs.extend(df)

            context.data = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            logger.info(f"Extract: {len(context.data)} rows in {time.time() - start:.2f}s")

            # Transform
            start = time.time()
            for transformer in self._transformers:
                context.data = transformer(context.data)
                logger.debug(f"Transform {transformer.__name__}: {len(context.data)} rows")

            logger.info(f"Transform: {len(context.data)} rows in {time.time() - start:.2f}s")

            # Load
            start = time.time()
            for loader in self._loaders:
                loader(context.data)

            logger.info(f"Load: {len(context.data)} rows in {time.time() - start:.2f}s")

            # After hooks
            for hook in self._after_hooks:
                hook(context)

            duration = (datetime.utcnow() - context.start_time).total_seconds()
            logger.info(f"Pipeline {self.name} completed in {duration:.2f}s")

        except Exception as e:
            logger.error(f"Pipeline {self.name} failed: {e}")
            context.errors.append(str(e))

            for handler in self._error_handlers:
                handler(e, context)

            raise

        return context


class Extract:
    """Data extraction utilities."""

    @staticmethod
    def from_csv(path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Extracting from CSV: {path}")
        return pd.read_csv(path, **kwargs)

    @staticmethod
    def from_json(path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Extracting from JSON: {path}")
        return pd.read_json(path, **kwargs)

    @staticmethod
    def from_sql(query: str, connection: Any, **kwargs) -> pd.DataFrame:
        logger.info(f"Extracting from SQL")
        return pd.read_sql(query, connection, **kwargs)

    @staticmethod
    def from_excel(path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Extracting from Excel: {path}")
        return pd.read_excel(path, **kwargs)

    @staticmethod
    def from_parquet(path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Extracting from Parquet: {path}")
        return pd.read_parquet(path, **kwargs)

    @staticmethod
    def from_api(url: str, headers: dict = None, params: dict = None) -> pd.DataFrame:
        import requests
        logger.info(f"Extracting from API: {url}")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json())


class Transform:
    """Data transformation utilities."""

    @staticmethod
    def clean(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """Clean and type-cast columns based on schema."""
        for col, dtype in schema.items():
            if col not in df.columns:
                continue
            if dtype == "string":
                df[col] = df[col].astype(str).str.strip()
            elif dtype == "integer":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "email":
                df[col] = df[col].str.lower().str.strip()
        return df

    @staticmethod
    def filter(df: pd.DataFrame, query: str) -> pd.DataFrame:
        """Filter rows using a query expression."""
        return df.query(query)

    @staticmethod
    def aggregate(df: pd.DataFrame, group_by: list, aggregations: dict) -> pd.DataFrame:
        """Aggregate data by groups."""
        agg_dict = {}
        for key, value in aggregations.items():
            if isinstance(value, tuple):
                agg_dict[key] = pd.NamedAgg(column=value[0], aggfunc=value[1])
            else:
                agg_dict[key] = pd.NamedAgg(column=key, aggfunc=value)
        return df.groupby(group_by).agg(**agg_dict).reset_index()

    @staticmethod
    def join(df: pd.DataFrame, other: pd.DataFrame, on: str, how: str = "left") -> pd.DataFrame:
        """Join two DataFrames."""
        return df.merge(other, on=on, how=how)


class Load:
    """Data loading utilities."""

    @staticmethod
    def to_sql(df: pd.DataFrame, table: str, connection: Any, **kwargs) -> None:
        logger.info(f"Loading {len(df)} rows to SQL table: {table}")
        df.to_sql(table, connection, index=False, **kwargs)

    @staticmethod
    def to_csv(df: pd.DataFrame, path: str, **kwargs) -> None:
        logger.info(f"Loading {len(df)} rows to CSV: {path}")
        df.to_csv(path, **kwargs)

    @staticmethod
    def to_json(df: pd.DataFrame, path: str, **kwargs) -> None:
        logger.info(f"Loading {len(df)} rows to JSON: {path}")
        df.to_json(path, **kwargs)

    @staticmethod
    def to_parquet(df: pd.DataFrame, path: str, **kwargs) -> None:
        logger.info(f"Loading {len(df)} rows to Parquet: {path}")
        df.to_parquet(path, **kwargs)


class Validate:
    """Data validation utilities."""

    @staticmethod
    def not_empty(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            raise ValueError("DataFrame is empty")
        return df

    @staticmethod
    def no_nulls(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                raise ValueError(f"Column '{col}' has {null_count} null values")
        return df

    @staticmethod
    def unique(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        duplicates = df.duplicated(subset=columns)
        if duplicates.any():
            raise ValueError(f"Found {duplicates.sum()} duplicate rows on columns {columns}")
        return df


def retry(attempts: int = 3, delay: int = 60):
    """Retry decorator for pipeline stages."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    logger.warning(f"Attempt {attempt + 1}/{attempts} failed: {e}")
                    if attempt < attempts - 1:
                        time.sleep(delay)
            raise last_error
        return wrapper
    return decorator


def row_count(df: pd.DataFrame) -> int:
    """Return number of rows in a DataFrame."""
    return len(df)


def column_stats(df: pd.DataFrame, column: str) -> dict:
    """Return basic statistics for a numeric column."""
    series = df[column]
    return {
        "min": series.min(),
        "max": series.max(),
        "mean": series.mean(),
        "null_count": int(series.isnull().sum()),
    }
