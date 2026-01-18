from src.formula1.formula1_utils import (
    add_ingestion_date,
    re_arrange_partition_column,
    df_column_to_list,
)


def test_add_ingestion_date(spark):
    data = [("James", 34), ("Anna", 28)]
    schema = ["name", "age"]
    df = spark.createDataFrame(data, schema)

    result_df = add_ingestion_date(df)

    assert "ingestion_date" in result_df.columns
    assert result_df.count() == 2


def test_re_arrange_partition_column(spark):
    data = [("James", 34, "New York"), ("Anna", 28, "Los Angeles")]
    schema = ["name", "age", "city"]
    df = spark.createDataFrame(data, schema)

    # Move 'age' to the end
    result_df = re_arrange_partition_column(df, "age")

    assert result_df.columns == ["name", "city", "age"]


def test_df_column_to_list(spark):
    data = [("Red",), ("Blue",), ("Red",), ("Green",)]
    schema = ["color"]
    df = spark.createDataFrame(data, schema)

    result_list = df_column_to_list(df, "color")

    assert set(result_list) == {"Red", "Blue", "Green"}
    assert len(result_list) == 3
