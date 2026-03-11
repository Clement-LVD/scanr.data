main_code <- function(){
  
# Lire un parquet
# tbl <- arrow::open_dataset("C:/Users/claverdet/Documents/data/scanr/organizations_denormalized.parquet")

table_df <- arrow::read_parquet("~/Documents/data/scanr/organizations_denormalized.parquet")

table_df <- unnest_nested_col_tbl_df(table_df)

#struct_cols <- names(tbl$schema)[
#  vapply(tbl$schema$fields, function(f) {
#    inherits(f$type, "StructType")
#  }, logical(1)) ]


#plusieurs lignes par entrees : a envoyer ds une autre table separee
list_struct_cols <- names(ds$schema)[
  vapply(ds$schema$fields, function(f) {
    inherits(f$type, "ListType") &&
    inherits(f$type$value_type, "StructType")
  }, logical(1))
]

  # suppress other columns : nested list of data.frame for example
  #save these col
other_columns <- table_df[, list_struct_cols]
  #erase these col
table_df[, list_struct_cols] <- NULL
  
table_df <- unnest_list_nested_col(table_df)
#almost_flat_cols <- names(table_df)[vapply(table_df, detect_almost_flat, logical(1))]

#test <- table_df[almost_flat_cols]

#test <- unnest_list_nested_col(test[, almost_flat_cols])
  

}
