
# fonction qui prends une colonne composee d'entrees de liste : reponds une liste avec un nombre d'entree STABLE 
harmonize_number_of_entries_in_a_list <- function(col_list, max_depth = 100){

#des fois les listes font 1 entrees, ou bien 3, etc.
n_entries <- vapply(col_list, FUN = function(entry) length(unique(entry)), numeric(1))

n_entries_to_have <- max(n_entries_within_a_col) 

if(n_entries_to_have > max_depth) {n_entries_to_have <- max_depth}
  
# Completer chaque sous-liste automatiquement
col_list <- mapply(
  function(x, n) {
    x <- unique(x)
    if (length(x) < n_entries_to_have) {
      return( c(x, rep(NA, n_entries_to_have - length(x))) )
    } else {
      if(length(x) > n_entries_to_have) {return(x[1:n_entries_to_have])} else return(x)
    }
  },
  col_list,
  n_entries,
  SIMPLIFY = FALSE
)
  
return(do.call(rbind, col_list))
  
}


# detection colonnes presque plate :
detect_almost_flat <- function(col) {
  
  if (!is.list(col))
    return(FALSE)
  
  dfs <- Filter(function(x) inherits(x, "data.frame"), col)
  
  if (length(dfs) == 0) return(FALSE)
  
  colnames_list <- lapply(dfs, names)
  
  structure_col_text <- vapply(colnames_list, paste, "", collapse = "|")

  returned_value <- length(unique(structure_col_text)) == 1

  return(returned_value)
} 

# col with data.frames belonging in a single structure (equivalent colnames in each of the df)
#les colonnes presque plates :

unnest_list_nested_col <- function(df) {

  list_cols <- which(sapply(df, is.list) )

  cat("Unesting columns : ", list_cols)

  for (num_col in list_cols) {
   
    nom_col <- names(list_cols[list_cols == num_col])

   cat("Col ", num_col, "(",nom_col , ")\n")
    
   col_to_add <- harmonize_number_of_entries_in_a_list(df[[num_col]])
    
    colnames(col_to_add) <- paste0(nom_col, "_", 1:ncol(col_to_add))

    df[[num_col]] <- NULL

    df <- cbind(df, col_to_add)
  }

  return(df)
}
    
  
      
