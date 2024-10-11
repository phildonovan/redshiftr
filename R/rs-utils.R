#' List Redshift tables
#'
#' Lists all tables available in Redshift database
#'
#' @param db_con a DBI connection to the Redshift server
#'
#' @return a data.frame of table names
#' @export
list_tables <- function(db_con) {
  dbGetQuery(db_con, "SELECT schemaname AS schema, tablename AS table, schemaname || '.' || tablename AS full_name FROM pg_tables")
}

#' Show table information
#'
#' @param db_con a DBI connection to the Redshift server
#' @param table_name table name
#' @param schema_name schema name, defaults to 'public'
#'
#' @return a data.frame containing table information
#' @export
show_table_info <- function(db_con, table_name, schema_name = 'public') {
  df <- dbGetQuery(db_con, sprintf("SET search_path TO %s; SELECT * FROM pg_table_def WHERE tablename = '%s'", schema_name, table_name))

  return(df)
}
