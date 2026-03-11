# brouillax evolue
bind_tbl_df_to_cols <- function(df, colname, .verbose = T){

  if(.verbose) cat("Bind col' : \033[0;32m", colname, "\033[0m\n" )

col <- df[[colname]]
  
#df_to_bind <- table_df[names(col)][[1]] 

table_df <- cbind(df, df_to_bind)
  
table_df[colname] <-  NULL

cat("ncol : ", ncol(table_df), "\n" )

return(table_df)

}

get_colnames_matching_classes <- function(table_df, class_to_match){

#get the classes
types <- sapply(table_df, class) 
# retain tbl_df class
type_matched <- sapply(types, FUN = function(x) class_to_match %in% x)
#keep only the TRUE values = tbl_df
type_matched <- type_matched[type_matched]

matched_colnames <- names(type_matched)  
  
return(matched_colnames)

}


# autres bouts de codes pour traiter les gps coordinates
  # n_lines_within_a_col <- vapply(col, nrow, numeric(1))
    # we will keep only the first line : sometimes there is several adress
lol <- function(){
    truncated_columns <- lapply(col
      , FUN = function(x) {
        table <- x[[1]][1, ]
        table$gps_lon <- table$gps$lon
        table$gps_lat <- table$gps$lat
        table$gps <- NULL
        table$localisationSuggestions <-  paste0(table$localisationSuggestions[[1]], collapse = " || ")
        return(table)
        })
    
   columns_to_add <- do.call(rbind, truncated_columns)
   df[[num_col]] <- NULL
   df <- cbind(df, columns_to_add)
    
   list_cols <- list_cols[!list_cols == num_col]
  
 cat("\r"); cat("Unnesting columns : ", list_cols)

  } 

# 2nde fonction : unnesting des arrow_list
unnest_nested_col_arrow_list <- function(table_df = NULL){

 if(is.null(table_df)) message("You have to give a data.frame to the unnest_nested_col_tbl_df() function") 
  
#arrow_list_colnames <- get_colnames_matching_classes(table_df, "arrow_list")

df_classes <- sapply(table_df, class) # get classes

arrow_list_columns <- lapply(df_classes, FUN = function(x) "arrow_list" %in% x) #filter arrow_list
  
  
expanded <- lapply(df_cols, function(nm) {
  cat("col : ", nm)
  flatten_dfcol(df[[nm]], nm)
})

expanded <- expanded[!sapply(expanded, is.null)]

df_final <- cbind(
  df[, !(names(df) %in% df_cols), drop = FALSE],
  do.call(cbind, expanded)
) 
  
df_final <- cbind(
  df[ , !(names(df) %in% df_cols), drop = FALSE],
  do.call(cbind, expanded)
)
    
}

# brouillax peu evolue
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


