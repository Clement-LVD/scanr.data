unnest_parquet_nested_columns <- function(
  
  parquet_filepath =  "~/Documents/data/scanr/organizations_denormalized.parquet"

){

table_df <- arrow::read_parquet(parquet_filepath)
  
table_df <- unnest_nested_col_tbl_df(table_df) 


  
}