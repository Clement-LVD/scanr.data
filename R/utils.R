bind_tbl_df_to_cols <- function(df, colname){

col <- df[[colname]]
  
#df_to_bind <- table_df[names(col)][[1]] 

table_df <- cbind(df, df_to_bind)
  
table_df[colname] <-  NULL 

  return(table_df)

}
