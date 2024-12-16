from chispa.dataframe_comparer import *
from ..jobs.actors_history_scd_job import do_actors_history_scd_transformation
from collections import namedtuple
ActorHistory = namedtuple("ActorHistory", "actor actorid current_year quality_class is_active")
ActorScd = namedtuple("ActorScd", "actor actorid quality_class is_active start_date end_date current_year")



def test_scd_generation(spark):
    source_data = [
        ActorHistory("First Actor", "nm00001", 2020, "Good", True),
        ActorHistory("Second Actor", "nm00002", 2019, "Average", False),
        ActorHistory("Third Actor", "nm00003", 2010, "Bad", True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_history_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("First Actor", "nm00001", "Good", True, 2020, 2020, 2020),
        ActorScd("Second Actor", "nm00002", "Average", False, 2019, 2019, 2020),
        ActorScd("Third Actor", "nm00003", "Bad", True, 2010, 2010, 2020)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)