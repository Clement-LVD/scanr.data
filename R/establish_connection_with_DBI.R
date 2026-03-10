
#' Connect to an empty database and install json if needed
#' 
#' @importFrom DBI dbConnect dbExecute
#' @importFrom duckdb duckdb
#' @examples con <- establish_connection_with_DBI()
#' 
establish_connection_with_DBI <- function(){

# Create DuckDB connexion (persisting accordingly to the name)
con <- DBI::dbConnect(duckdb::duckdb(),  "scanr.duckdb")
   
conn <- tryCatch(DBI::dbExecute(con, "LOAD json"), error = function(e) FALSE)

if(isFALSE(conn)){

DBI::dbExecute(con, "INSTALL json")

conn <- tryCatch(DBI::dbExecute(con, "LOAD json"), error = function(e) FALSE)

  if(isFALSE(conn)) warning("ERROR : CAN'T READ A json FILE : the returned DBI connexion is a logical FALSE value")
  
}
  
return(con)
  
}
