library(DBI)
library(duckdb)

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "jocas_debug.duckdb")

endpoint <- Sys.getenv("AWS_S3_ENDPOINT")
access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
session_token <- Sys.getenv("AWS_SESSION_TOKEN")
region <- Sys.getenv("AWS_DEFAULT_REGION", unset = "us-east-1")

endpoint <- gsub("^https?://", "", endpoint)
endpoint <- sub("/+$", "", endpoint)

DBI::dbExecute(con, "INSTALL httpfs;")
DBI::dbExecute(con, "LOAD httpfs;")
DBI::dbExecute(con, sprintf("SET s3_region='%s';", region))
DBI::dbExecute(con, sprintf("SET s3_access_key_id='%s';", access_key))
DBI::dbExecute(con, sprintf("SET s3_secret_access_key='%s';", secret_key))
DBI::dbExecute(con, sprintf("SET s3_endpoint='%s';", endpoint))
DBI::dbExecute(con, "SET s3_use_ssl=true;")
DBI::dbExecute(con, "SET s3_url_style='path';")

if (nzchar(session_token)) {
  DBI::dbExecute(con, sprintf("SET s3_session_token='%s';", session_token))
}

cat("Création vue jocas...\n")

DBI::dbExecute(con, "
  CREATE OR REPLACE VIEW jocas AS
  SELECT *
  FROM read_parquet(
    's3://projet-jocas-prod/diffusion/JOCAS/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true,
    filename = true
  )
")

cat("Vue jocas créée.\n")

DBI::dbGetQuery(con, "
  DESCRIBE jocas
")

DBI::dbGetQuery(con, "
  SELECT COUNT(DISTINCT location_departement) AS nb_dep_brut
  FROM jocas
")

DBI::dbGetQuery(con, "
  SELECT
    location_departement,
    COUNT(*) AS n
  FROM jocas
  WHERE location_departement IS NOT NULL
    AND regexp_matches(location_departement, '^([0-9]{2}|2A|2B|97[1-6])$')
  GROUP BY location_departement
  ORDER BY location_departement
")
