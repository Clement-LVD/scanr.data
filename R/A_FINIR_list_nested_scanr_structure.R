#library(DBI)
#library(duckdb)
#library(dplyr)
#' @examples
#' \dontrun{
#' struc <- list_nested_scanr_structure()
#' 
#' }
list_nested_scanr_structure <- function(
  parquet_file = "~/Documents/data/scanr/organizations_denormalized.parquet"
) {
  
con <- establish_connection_with_DBI()

  # 1. Get complete schema
  schema <- DBI::dbGetQuery(con, sprintf("SELECT * FROM parquet_schema('%s')", parquet_file))
  
  # 2. Identify STRUCT et LIST
  # hereabove we've added the 'converted_type' entry implicitly (result of parquet_schema)
  complex_cols <- schema[grepl("STRUCT|LIST",schema$converted_type), ]
  
  # 3. Créer un data frame tidy
  tidy_list <- list()
  
  for(i in seq_along(complex_cols$file_name)){
    col_name <- complex_cols$name[i]
    col_type <- complex_cols$converted_type[i]
    
    # Pour STRUCT, get les sous-champs via SELECT ... LIMIT 1
    if(grepl("STRUCT", col_type)){
      query <- sprintf("SELECT * FROM '%s' LIMIT 1", parquet_file)
      df <- dbGetQuery(con, query)
      # si la colonne existe et est une liste de STRUCT
       if(length(df[[col_name]]) > 0){ if(is.list(df[[col_name]])){
        subfields <- names(df[[col_name]][[1]])
      }} else {
        subfields <- NA
      }
      
      tidy_list[[i]] <- data.frame(
        column = col_name,
        type = "STRUCT",
        subfields = paste(subfields, collapse = ", "),
        stringsAsFactors = FALSE
      )
    }
    
    # Pour LIST, get le type element
    if(grepl("LIST", col_type)){
      query <- sprintf("SELECT UNNEST(%s) AS element FROM '%s' LIMIT 1", col_name, parquet_file)
      # hereabove we've created the 'element' entry
      df <- try(DBI::dbGetQuery(con, query), silent = TRUE)
      
      if(!inherits(df, "try-error")){
        if(length(df$element) > 0){ if( is.list(df$element[[1]]) ){
          subfields <- names(df$element[[1]])
        }} else {
          subfields <- NA
        }
      } else {
        subfields <- NA
      }

      tidy_list[[i]] <- data.frame(
        column = col_name,
        type = "LIST",
        subfields = paste(subfields, collapse = ", "),
        stringsAsFactors = FALSE
      )
    }
  }
  
  DBI::dbDisconnect(con, shutdown = TRUE)
  
  # Combiner tous les résultats
  df_to_return <- do.call(rbind, tidy_list)

  return(df_to_return)
}
