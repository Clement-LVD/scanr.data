#' Convert all the .json within a folder to .parquet files
#' 
#' Read all .json or .json.gz from a folder and create a .parquet database for each json files
#' 
#' @param json_folder `character` - The folder where there is .json to convert into .parquet
#' 
#' @return Return a `character` `vector` with the file path of the .parquet files created
#' 
#' @examples
#' \dontrun{
#' names <- import_scanr_dataset(json_folder = "C:/Users/claverdet/Documents/data/scanr")
#' }
#' 
import_scanr_dataset <- function(json_folder = "C:/Users/claverdet/Documents/data/scanr"
, .verbose = TRUE){

if(.verbose){cat( "|=> Folder with .json to read :", json_folder, "\n") }

files_df <- as.data.frame( list.files(json_folder, full.names = T, pattern = "\\.jsonl?(\\.gz)?") )
 
names <- vapply(seq_along(files_df[[1]])
  , FUN.VALUE = character(1)
  , FUN = function(i){

path_to_import <- files_df[[1]][i]
    
  names <- convert_json_to_parquet_multithread(json_file = path_to_import)
 # names <- convert_json_to_parquet(json_file = path_to_import)
  
  return(names)
  })

}
