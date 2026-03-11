#' Unnest the 'tbl_df' class of columns answered by arrow::read_parquet() 
#' @examples
#' \dontrun{
#' table_df <- arrow::read_parquet("~/Documents/data/scanr/organizations_denormalized.parquet")
#' table_df <- unnest_nested_col_tbl_df(table_df)
#' }
unnest_nested_col_tbl_df <- function(table_df = NULL){

if(is.null(table_df)) message("You've don't provided a data.frame to the unnest_nested_col_tbl_df() function") 
  
# custom func : get names of columns accordingly to a class
tbl_df_colnames <- get_colnames_matching_classes(table_df, "tbl_df")
  
columns_to_add <- lapply(tbl_df_colnames , FUN = function(colname) {

cols <- table_df[[colname]]

colnames(cols) <- paste0(colname, "_", colnames(cols))
  
# table_df <- bind_tbl_df_to_cols(table_df, colname = colname) #utility func' not needed anymore
  
return(cols)
  
} )

columns_to_add <- do.call(cbind, columns_to_add)
  
table_df[, tbl_df_colnames]  <- NULL #suppress nested columns
 
table_df <- cbind(table_df, columns_to_add)  #  add flat columns

return(table_df)
}

