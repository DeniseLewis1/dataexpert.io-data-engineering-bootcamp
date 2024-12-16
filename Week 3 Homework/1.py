import org.apache.spark.sql.functions.{broadcast, split, lit}

val matchesBucketed = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/matches.csv")
val matchDetailsBucketed = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/match_details.csv")
val medalsMatchesPlayersBucketed = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/medals_matches_players.csv")
val medals = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/medals.csv")
val maps = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/maps.csv")


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
val bucketedMatchesDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    mapid STRING,
    playlist_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedMatchesDDL)



spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
val bucketedDetailsDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedDetailsDDL)



spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")
val bucketedPlayersDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id INTEGER,
    count INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedPlayersDDL)


matchesBucketed.select($"match_id", $"mapid", $"playlist_id")
    .write.mode("overwrite")
    .bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")



matchDetailsBucketed.select($"match_id", $"player_gamertag", $"player_total_kills", $"player_total_deaths")
    .write.mode("overwrite")
    .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")



medalsMatchesPlayersBucketed.select($"match_id", $"player_gamertag", $"medal_id", $"count")
    .write.mode("overwrite")
    .bucketBy(16, "match_id").saveAsTable("bootcamp.medal_matches_players_bucketed")


val df = spark.sql("""
  SELECT
      mb.match_id, 
      mb.mapid, 
      mb.playlist_id,
      mdb.player_gamertag,
      mdb.player_total_kills,
      mdb.player_total_deaths,
      mmpb.medal_id,
      mmpb.count
  FROM bootcamp.matches_bucketed mb
  JOIN bootcamp.match_details_bucketed mdb 
      ON mb.match_id = mdb.match_id
  JOIN bootcamp.medal_matches_players_bucketed mmpb
      ON mb.match_id = mmpb.match_id        
""")

df.show()


val explicitBroadcastMedals = df.join(broadcast(medals), "medal_id")
val medalsJoined = explicitBroadcastMedals.withColumnRenamed("name", "medal_name")
val explicitBroadcastMaps = medalsJoined.join(broadcast(maps), "mapid")
val mapsJoined = explicitBroadcastMaps.withColumnRenamed("name", "map_name")

mapsJoined.createOrReplaceTempView("joinedData")


// Which player averages the most kills per game?

spark.sql("""
    SELECT 
        player_gamertag,
        AVG(player_total_kills) AS avg_kills
    FROM joinedData
    GROUP BY player_gamertag, match_id
    ORDER BY 2 DESC
    LIMIT 1;
""").show()


// Which playlist gets played the most?

spark.sql("""
    SELECT 
        playlist_id,
        COUNT(DISTINCT match_id) AS count
    FROM joinedData
    GROUP BY playlist_id
    ORDER BY 2 DESC
    LIMIT 1;
""").show()


// Which map gets played the most?

spark.sql("""
    SELECT 
        DISTINCT mapid,
        map_name,
        COUNT(DISTINCT match_id) AS count      
    FROM joinedData
    GROUP BY mapid, map_name
    ORDER BY 3 DESC
    LIMIT 1;
""").show()


// Which map do players get the most Killing Spree medals on?

spark.sql("""
    SELECT 
        mapid,
        map_name,
        COUNT(DISTINCT match_id) AS count
    FROM joinedData
    WHERE medal_name = 'Killing Spree'
    GROUP BY mapid, map_name
    ORDER BY 3 DESC
    LIMIT 1;
""").show()


val newDF = mapsJoined.select("match_id", "mapid", "medal_id", "playlist_id", "player_gamertag", "player_total_kills", "player_total_deaths", "medal_name", "map_name")

newDF.show()


val start_df = newDF.repartition(4, col("match_id"))

val first_sort_df = start_df.sortWithinPartitions(col("match_id"), col("mapid"), col("playlist_id"))


start_df.write.mode("overwrite").saveAsTable("bootcamp.matches_unsorted")
first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.matches_sorted")


spark.sql("""
    SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
    FROM bootcamp.matches_sorted.files

    UNION ALL
    SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
    FROM bootcamp.matches_unsorted.files
""").show()

