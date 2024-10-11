#' @importFrom "utils" "head"
calculate_char_size <- function(col) {
  col <- as.character(col)
  max_char <- max(nchar(col, type = 'bytes'), na.rm = TRUE)
  if (is.infinite(max_char)) {
    max_char <- 1000
    warning('Empty column found, setting to 1024 length')
  }

  sizes <- c(2^c(3:15), 65535) # From 8 to 65535, max varchar size in redshift
  fsizes <- sizes[sizes > max_char]
  if (length(fsizes) == 0) {
    warning("Character column over maximum size of 65535, set to that value but will fail if not trimmed before uploading!")
    warning(paste0('Example offending value: ', head(col[nchar(col) > 65535], 1)))
    return(max(sizes, na.rm = TRUE))
  } else {
    return(min(fsizes, na.rm = TRUE))
  }
}

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
