#' Create a .parquet database from a jsonl or a jsonl.gz
#' 
#' @param json_file `character` - The file path of a json file or a jsonl.gz archive
#' @param parquet_file `character` - Name of the .parquet to create from the json
#' 
#' @importFrom DBI dbExecute dbDisconnect
#' 
#' @examples
#' \dontrun{
#'  convert_json_to_parquet( '~/Documents/data/scanr/patents_denormalized.jsonl.gz')
#'  # 1. import a classical r data.frame with arrow (arrow_list)
#' # orgs <- arrow::read_parquet("organizations.parquet")
#' 
#'  # 2. execute sql on the .parquet file with DBI
#' # DBI::dbGetQuery(con, "SELECT * FROM 'organizations.parquet' LIMIT 10")
#' }
#' 
#' @export
#' 
convert_json_to_parquet <- function(json_file= '~/Documents/data/scanr/organizations_denormalized.jsonl.gz' 
, chunk_size = 20000
, .verbose = TRUE) {

#replacing '.jsonl' or '.json', or 'jsonl.gz' or 'json.gz' with '.parquet' (end of the name)
parquet_filename <- sub(x = basename(json_file) , pattern = "\\.jsonl?(\\.gz)?$", ".parquet", perl = T)
parquet_full_path <- file.path(dirname(json_file), parquet_filename)
 
if(.verbose){cat( "|===> Read", basename(json_file), "\n") }
  
con <- establish_connection_with_DBI()
 
n_lines <- DBI::dbExecute(con, sprintf("COPY ( SELECT * FROM read_json( '%s'
        , format='newline_delimited'
        , union_by_name=true  ) 
         ) TO '%s' (FORMAT PARQUET); "  , json_file, parquet_full_path ))

DBI::dbDisconnect(con, shutdown = TRUE)

if(.verbose) {if(file.exists(parquet_full_path)) cat("|====> .parquet created : ", parquet_full_path, "\n")}

return(parquet_full_path)

}
