#brouillax
# connexion dbi pour importer le format json lourd de scanr
con <- DBI::dbConnect(duckdb::duckdb(), "scanr.duckdb")

conn <- tryCatch(DBI::dbExecute(con, "LOAD json"), error = function(e) FALSE)

if(isFALSE(conn)){

DBI::dbExecute(con, "INSTALL json")

conn <- tryCatch(DBI::dbExecute(con, "LOAD json"), error = function(e) FALSE)

  if(isFALSE(conn)) warning("ERROR : CAN'T READ THE json FILE : ", json_path)
  
}
# et voila on peut lire les data : soit en df tranquille

req <- paste0("COPY (
SELECT * FROM read_ndjson_auto('", json_path, "',sample_size = -1, union_by_name = true)
                    ) TO 'organizations.parquet'
                    (FORMAT PARQUET)")

# df <- DBI::dbGetQuery(con, req)

df <- DBI::dbExecute(con, req)

#df <- DBI::dbGetQuery(con, "SELECT * FROM organizations")

return(df)


###ou encore

  #ancien code :
  # Read JSONL and convert to .parquet
 trash <- DBI::dbExecute(con, sprintf("
    COPY (
      SELECT *
      FROM read_ndjson_auto(
        '%s',
        sample_size = -1,
        union_by_name = true
      )
    ) TO '%s' (FORMAT PARQUET)
  ", json_file, parquet_full_path))

DBI::dbDisconnect(con, shutdown = TRUE)


# ou enfin un version avec progress bar:

# trick for the progress bar : n_lines
n_lines <- DBI::dbExecute(con, sprintf("
  CREATE OR REPLACE TABLE tmp_scanr AS
  SELECT *
  FROM read_ndjson_auto('%s', sample_size=-1, union_by_name=TRUE, ignore_errors=TRUE)
", json_file))
  
  if(.verbose) { pb <- txtProgressBar(min = 0, max = n_lines, style = 3) }

   offset <- 0
  
  while(offset < n_lines) {
    DBI::dbExecute(con, sprintf("
      INSERT INTO tmp_scanr
      SELECT *
      FROM read_ndjson_auto(
        '%s',
        sample_size=-1,
        union_by_name=TRUE,
        ignore_errors=TRUE
      )
      LIMIT %d OFFSET %d
    ", json_file, chunk_size, offset))
    
    offset <- offset + chunk_size
    if(.verbose) { setTxtProgressBar(pb, min(offset, n_lines)) }
  }
  
if(.verbose) close(pb)
  
  # copy to a .parquet file
DBI::dbExecute(con, sprintf("COPY tmp_scanr TO '%s' (FORMAT PARQUET)", parquet_full_path))
