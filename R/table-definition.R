#' Calculate Character Size for Column
#'
#' This function calculates the appropriate character size for a given column in the data frame,
#' which is used to determine the VARCHAR size for Redshift table creation.
#'
#' @param col A column from a data frame.
#' @return An integer representing the maximum character size for the column.
#' @keywords internal
#' @importFrom utils head
calculate_char_size <- function(col) {
  col <- as.character(col)
  max_char <- max(nchar(col, type = 'bytes'), na.rm = TRUE)
  if (is.infinite(max_char)) {
    max_char <- 1000
    warning('Empty column found, setting to 1024 length')
  }

  sizes <- c(2^c(3:15), 65535) # From 8 to 65535, max varchar size in Redshift
  fsizes <- sizes[sizes > max_char]
  if (length(fsizes) == 0) {
    warning("Character column over maximum size of 65535, set to that value but will fail if not trimmed before uploading!")
    warning(paste0('Example offending value: ', head(col[nchar(col) > 65535], 1)))
    return(max(sizes, na.rm = TRUE))
  } else {
    return(min(fsizes, na.rm = TRUE))
  }
}

#' Convert Column to Redshift Type
#'
#' This function determines the appropriate Redshift data type for a given column in the data frame.
#'
#' @param col A column from a data frame.
#' @param compression A logical value indicating whether compression should be applied.
#' @return A character string representing the Redshift data type for the column.
#' @keywords internal
col_to_redshift_type <- function(col, compression) {
  col_class <- class(col)[[1]]
  if (inherits(col, "sfc")) {
    return('GEOMETRY')
  }
  switch(col_class,
         logical = {
           return('boolean')
         },
         numeric = {
           return('float8')
         },
         integer = {
           if (all(is.na(col))) { # Unknown column, all null
             return('int4')
           } else {
             return('int4')
           }
         },
         character = {
           char_size <- calculate_char_size(col)
           return(paste0('varchar(', char_size, ')'))
         },
         factor = {
           return(paste0('varchar(', calculate_char_size(levels(col)), ')'))
         },
         Date = {
           return('date')
         },
         POSIXct = {
           return('timestamp')
         },
         stop('Unsupported column type:', col_class))
}

#' Create Redshift Table Statement
#'
#' This function generates a SQL CREATE TABLE statement for Redshift based on the provided data frame and parameters.
#'
#' @param df A data frame to use for generating the CREATE TABLE statement.
#' @param table_name A character string representing the name of the table to be created.
#' @param sortkeys A character vector of column names to be used as sort keys (optional).
#' @param sortkey_style A character string specifying the sort key style, either 'compound' or 'interleaved' (default is 'compound').
#' @param distkey A character string representing the distribution key column (optional).
#' @param distkey_style A character string specifying the distribution style, either 'even' or 'all' (default is 'even').
#' @param compression A logical value indicating whether compression should be applied (default is TRUE).
#' @return A character string representing the CREATE TABLE statement for Redshift.
#' @keywords internal
rs_create_statement <- function(df, table_name, sortkeys = NULL, sortkey_style = 'compound', distkey = NULL, distkey_style = 'even', compression = TRUE) {
  column_defs <- purrr::map_chr(names(df), ~ paste(.x, col_to_redshift_type(df[[.x]], compression)))
  create_sql <- paste0(
    "CREATE TABLE ", table_name, " (",
    paste(column_defs, collapse = ", "), ")"
  )

  if (!is.null(sortkeys)) {
    create_sql <- paste0(create_sql, " SORTKEY (", paste(sortkeys, collapse = ", "), ")")
  }

  if (!is.null(distkey)) {
    create_sql <- paste0(create_sql, " DISTKEY (", distkey, ")")
  }

  return(create_sql)
}
