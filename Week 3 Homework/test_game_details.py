from chispa.dataframe_comparer import *
from ..jobs.game_details_job import do_game_details_deduped_transformation
from collections import namedtuple
GameDetails = namedtuple("GameDetails", "game_id game_date_est season team_id player_id player_name start_position row_num")
GameDetailsDeduped = namedtuple("GameDetailsDeduped", "dim_game_date dim_season dim_team_id dim_player_id dim_player_name dim_start_position")




def test_scd_generation(spark):
    source_data = [
        GameDetails(1, "2020-01-01", 2020, 20, 10001, "Player One", "C", 1),
        GameDetails(1, "2020-01-01", 2020, 20, 10001, "Player One", "C", 2),
        GameDetails(1, "2020-01-01", 2020, 20, 10001, "Player One", "C", 3),
        GameDetails(2, "2020-11-15", 2021, 15, 10050, "Player Two", "G", 1)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_game_details_deduped_transformation(spark, source_df)
    expected_data = [
        GameDetailsDeduped("2020-01-01", 2020, 20, 10001, "Player One", "C"),
        GameDetailsDeduped("2020-11-15", 2021, 15, 10050, "Player Two", "G")
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)