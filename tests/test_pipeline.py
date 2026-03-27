import pytest
import pandas as pd
from unittest.mock import MagicMock

from etl.pipeline import (
    Pipeline,
    PipelineContext,
    Extract,
    Transform,
    Load,
    Validate,
    retry,
    row_count,
    column_stats,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
        "email": ["ALICE@TEST.COM", " bob@test.com ", "charlie@test.com"],
        "score": [88.5, 92.0, 76.3],
    })


@pytest.fixture
def pipeline():
    return Pipeline("test-pipeline")


# ---------------------------------------------------------------------------
# PipelineContext
# ---------------------------------------------------------------------------

class TestPipelineContext:
    def test_defaults(self):
        ctx = PipelineContext()
        assert ctx.data is None
        assert ctx.metadata == {}
        assert ctx.errors == []

    def test_start_time_set(self):
        ctx = PipelineContext()
        assert ctx.start_time is not None


# ---------------------------------------------------------------------------
# Pipeline creation and step chaining
# ---------------------------------------------------------------------------

class TestPipelineCreation:
    def test_pipeline_name(self, pipeline):
        assert pipeline.name == "test-pipeline"

    def test_register_extractor(self, pipeline, sample_df):
        @pipeline.extract
        def get_data():
            return sample_df

        assert len(pipeline._extractors) == 1

    def test_register_transformer(self, pipeline):
        @pipeline.transform
        def upper_names(df):
            df["name"] = df["name"].str.upper()
            return df

        assert len(pipeline._transformers) == 1

    def test_register_loader(self, pipeline):
        @pipeline.load
        def save(df):
            pass

        assert len(pipeline._loaders) == 1

    def test_chained_registration(self, pipeline, sample_df):
        @pipeline.extract
        def get_data():
            return sample_df

        @pipeline.transform
        def clean(df):
            return df

        @pipeline.load
        def save(df):
            pass

        assert len(pipeline._extractors) == 1
        assert len(pipeline._transformers) == 1
        assert len(pipeline._loaders) == 1


# ---------------------------------------------------------------------------
# Pipeline.run – extract / transform / load
# ---------------------------------------------------------------------------

class TestPipelineRun:
    def test_basic_run(self, pipeline, sample_df):
        @pipeline.extract
        def get_data():
            return sample_df

        ctx = pipeline.run()
        assert isinstance(ctx, PipelineContext)
        assert len(ctx.data) == 3

    def test_transform_applied(self, pipeline, sample_df):
        @pipeline.extract
        def get_data():
            return sample_df

        @pipeline.transform
        def upper_names(df):
            df = df.copy()
            df["name"] = df["name"].str.upper()
            return df

        ctx = pipeline.run()
        assert list(ctx.data["name"]) == ["ALICE", "BOB", "CHARLIE"]

    def test_multiple_extractors_concat(self, pipeline):
        df1 = pd.DataFrame({"x": [1, 2]})
        df2 = pd.DataFrame({"x": [3, 4]})

        @pipeline.extract
        def ext1():
            return df1

        @pipeline.extract
        def ext2():
            return df2

        ctx = pipeline.run()
        assert len(ctx.data) == 4

    def test_loader_receives_data(self, pipeline, sample_df):
        loaded = {}

        @pipeline.extract
        def get_data():
            return sample_df

        @pipeline.load
        def save(df):
            loaded["rows"] = len(df)

        pipeline.run()
        assert loaded["rows"] == 3

    def test_before_and_after_hooks(self, pipeline, sample_df):
        calls = []

        @pipeline.before
        def pre(ctx):
            calls.append("before")

        @pipeline.after
        def post(ctx):
            calls.append("after")

        @pipeline.extract
        def get_data():
            return sample_df

        pipeline.run()
        assert calls == ["before", "after"]

    def test_error_handler_called(self, pipeline):
        errors_caught = []

        @pipeline.extract
        def bad_extract():
            raise RuntimeError("extraction failed")

        @pipeline.on_error
        def handle(err, ctx):
            errors_caught.append(str(err))

        with pytest.raises(RuntimeError, match="extraction failed"):
            pipeline.run()

        assert len(errors_caught) == 1
        assert "extraction failed" in errors_caught[0]

    def test_empty_pipeline_returns_empty_df(self, pipeline):
        ctx = pipeline.run()
        assert ctx.data is not None
        assert ctx.data.empty


# ---------------------------------------------------------------------------
# Extract helpers
# ---------------------------------------------------------------------------

class TestExtract:
    def test_from_csv(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2]}).to_csv(csv_file, index=False)
        df = Extract.from_csv(str(csv_file))
        assert len(df) == 2
        assert "a" in df.columns

    def test_from_json(self, tmp_path):
        json_file = tmp_path / "data.json"
        pd.DataFrame({"b": [10, 20]}).to_json(json_file)
        df = Extract.from_json(str(json_file))
        assert len(df) == 2


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

class TestTransform:
    def test_clean_string(self, sample_df):
        result = Transform.clean(sample_df.copy(), {"name": "string"})
        assert result["name"].dtype == object

    def test_clean_integer(self):
        df = pd.DataFrame({"val": ["1", "2", "bad"]})
        result = Transform.clean(df, {"val": "integer"})
        assert result["val"].dtype == pd.Int64Dtype()
        assert pd.isna(result["val"].iloc[2])

    def test_clean_float(self):
        df = pd.DataFrame({"val": ["1.5", "bad"]})
        result = Transform.clean(df, {"val": "float"})
        assert pd.isna(result["val"].iloc[1])

    def test_clean_datetime(self):
        df = pd.DataFrame({"dt": ["2024-01-01", "not-a-date"]})
        result = Transform.clean(df, {"dt": "datetime"})
        assert pd.isna(result["dt"].iloc[1])

    def test_clean_email(self, sample_df):
        result = Transform.clean(sample_df.copy(), {"email": "email"})
        assert result["email"].iloc[0] == "alice@test.com"
        assert result["email"].iloc[1] == "bob@test.com"

    def test_clean_ignores_missing_column(self, sample_df):
        result = Transform.clean(sample_df.copy(), {"nonexistent": "string"})
        assert len(result) == 3  # no crash

    def test_filter(self, sample_df):
        result = Transform.filter(sample_df, "age > 28")
        assert len(result) == 2

    def test_aggregate(self):
        df = pd.DataFrame({
            "dept": ["A", "A", "B"],
            "salary": [100, 200, 300],
        })
        result = Transform.aggregate(df, ["dept"], {"salary": "sum"})
        assert len(result) == 2
        row_a = result[result["dept"] == "A"]
        assert row_a["salary"].iloc[0] == 300

    def test_join(self):
        left = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        right = pd.DataFrame({"id": [1, 2], "score": [10, 20]})
        result = Transform.join(left, right, on="id")
        assert "score" in result.columns
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

class TestLoad:
    def test_to_csv(self, tmp_path, sample_df):
        path = str(tmp_path / "out.csv")
        Load.to_csv(sample_df, path, index=False)
        loaded = pd.read_csv(path)
        assert len(loaded) == 3

    def test_to_json(self, tmp_path, sample_df):
        path = str(tmp_path / "out.json")
        Load.to_json(sample_df, path)
        loaded = pd.read_json(path)
        assert len(loaded) == 3


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

class TestValidate:
    def test_not_empty_passes(self, sample_df):
        result = Validate.not_empty(sample_df)
        assert len(result) == 3

    def test_not_empty_fails(self):
        with pytest.raises(ValueError, match="empty"):
            Validate.not_empty(pd.DataFrame())

    def test_column_exists_passes(self, sample_df):
        result = Validate.column_exists(sample_df, ["name", "age"])
        assert len(result) == 3

    def test_column_exists_fails(self, sample_df):
        with pytest.raises(ValueError, match="Missing required columns"):
            Validate.column_exists(sample_df, ["name", "nonexistent"])

    def test_no_nulls_passes(self, sample_df):
        result = Validate.no_nulls(sample_df, ["name", "age"])
        assert len(result) == 3

    def test_no_nulls_fails(self):
        df = pd.DataFrame({"a": [1, None, 3]})
        with pytest.raises(ValueError, match="null values"):
            Validate.no_nulls(df, ["a"])

    def test_unique_passes(self, sample_df):
        result = Validate.unique(sample_df, ["name"])
        assert len(result) == 3

    def test_unique_fails(self):
        df = pd.DataFrame({"a": [1, 1, 2]})
        with pytest.raises(ValueError, match="duplicate"):
            Validate.unique(df, ["a"])


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_row_count(self, sample_df):
        assert row_count(sample_df) == 3

    def test_row_count_empty(self):
        assert row_count(pd.DataFrame()) == 0

    def test_column_stats(self, sample_df):
        stats = column_stats(sample_df, "age")
        assert stats["min"] == 25
        assert stats["max"] == 35
        assert stats["null_count"] == 0
        assert "mean" in stats
        assert "median" in stats
        assert "std" in stats

    def test_column_stats_with_nulls(self):
        df = pd.DataFrame({"x": [10, None, 30]})
        stats = column_stats(df, "x")
        assert stats["null_count"] == 1
        assert stats["min"] == 10
        assert stats["max"] == 30


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

class TestRetry:
    def test_retry_succeeds_first_try(self):
        @retry(attempts=3, delay=0)
        def ok():
            return "done"

        assert ok() == "done"

    def test_retry_succeeds_after_failures(self):
        call_count = {"n": 0}

        @retry(attempts=3, delay=0)
        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise RuntimeError("fail")
            return "ok"

        assert flaky() == "ok"
        assert call_count["n"] == 3

    def test_retry_exhausted(self):
        @retry(attempts=2, delay=0)
        def always_fail():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            always_fail()


# ---------------------------------------------------------------------------
# Error handling for invalid data
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_transform_with_bad_query(self, sample_df):
        with pytest.raises(Exception):
            Transform.filter(sample_df, "nonexistent_col > 5")

    def test_column_stats_bad_column(self, sample_df):
        with pytest.raises(KeyError):
            column_stats(sample_df, "nonexistent")

    def test_pipeline_propagates_extractor_error(self, pipeline):
        @pipeline.extract
        def bad():
            raise ValueError("bad data source")

        with pytest.raises(ValueError, match="bad data source"):
            pipeline.run()

    def test_pipeline_propagates_transformer_error(self, pipeline, sample_df):
        @pipeline.extract
        def get_data():
            return sample_df

        @pipeline.transform
        def bad_transform(df):
            raise TypeError("wrong type")

        with pytest.raises(TypeError, match="wrong type"):
            pipeline.run()
