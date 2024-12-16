from pyspark.sql import SparkSession

query = """

WITH deduped AS (
	SELECT 
        game_id,
		game_date_est,
		season,
		team_id,
        player_id,
        player_name,
        start_position,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id ORDER BY game_date_est) AS row_num
	FROM game_details
)

SELECT 
	game_date_est AS dim_game_date,
	season AS dim_season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_position
FROM deduped
WHERE row_num = 1;

"""


def do_game_details_deduped_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_details_deduped") \
      .getOrCreate()
    output_df = do_game_details_deduped_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("game_details_deduped")

