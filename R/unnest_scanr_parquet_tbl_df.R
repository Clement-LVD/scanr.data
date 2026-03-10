unnest_scanr_parquet_tbl_df <- function(parquet_filepath =  "C:/Users/claverdet/Documents/data/scanr/organizations_denormalized.parquet"){

table_df <- arrow::read_parquet(parquet_filepath)
#get the classes
types <- sapply(table_df, class) 
# retain tbl_df classe
type_tbl_df <- sapply(types, FUN = function(x) "tbl_df" %in% x)
#keep only the TRUE values = tbl_df
type_tbl_df <- type_tbl_df[type_tbl_df]

for(colname in names(type_tbl_df)){

cat("Bind col' : \033[0;32m", colname, "\033[0m\n" )
  
table_df <- bind_tbl_df_to_cols(table_df, colname = colname)

 cat("ncol : ", ncol(table_df), "\n" )
  
}

  return(table_df)
}
