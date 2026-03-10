#' Create a .parquet database from a jsonl or a jsonl.gz
#' 
#' @param json_file `character` - The file path of a json file or a jsonl.gz archive
#' @param threads `numeric` - Threads (Core) to exploit multiprocessing
#' 
#' @importFrom DBI dbExecute dbDisconnect
#' 
#' @examples
#' \dontrun{
#'  convert_json_to_parquet_multithread( 'C:/Users/claverdet/Documents/data/scanr/patents_denormalized.jsonl.gz')
#'  # 1. import a classical r data.frame with arrow (arrow_list)
#' # orgs <- arrow::read_parquet("organizations.parquet")
#' 
#'  # 2. execute sql on the .parquet file with DBI
#' # DBI::dbGetQuery(con, "SELECT * FROM 'organizations.parquet' LIMIT 10")
#' }
#' 
#' @export
#' 
convert_json_to_parquet_multithread <- function(json_file, threads = 8) {

parquet_filename <- sub(x = basename(json_file) , pattern = "\\.jsonl?(\\.gz)?$", ".parquet", perl = T)
parquet_file <- file.path(dirname(json_file), parquet_filename)
  
con <- establish_connection_with_DBI()
  
DBI::dbExecute(con, sprintf("PRAGMA threads=%s;", threads))

cat("|==> Converting '\033[0;32m", basename(json_file), "\033[0m' =[to]=> \033[0;32m", parquet_filename, "\033[0m\n")

  n_lines <- DBI::dbExecute(con, sprintf("
    COPY (
        SELECT *
        FROM read_json(
            '%s',
            format='newline_delimited',
            union_by_name=true
        )
    )
    TO '%s'
    (FORMAT PARQUET, COMPRESSION ZSTD);
  ", json_file, parquet_file))

  DBI::dbDisconnect(con, shutdown=TRUE)

  if(file.exists(parquet_file)) {cat("\033[0;32m|===> Converting ok :", basename(parquet_file), "\033[0m\n")
  } else message("|===> Error during converting from .json to .parquet : ", basename(parquet_file), "\n")
  
  return(parquet_file)
}
