#' Begin a Database Transaction
#'
#' This function starts a transaction by executing the `BEGIN` statement.
#'
#' @param con A DBI connection object.
#' @keywords internal
begin_transaction <- function(con) {
  queryStmt(con, "BEGIN;")
}

#' Commit a Database Transaction
#'
#' This function commits a transaction by executing the `COMMIT` statement.
#'
#' @param con A DBI connection object.
#' @keywords internal
commit_transaction <- function(con) {
  queryStmt(con, "COMMIT;")
}

#' Rollback a Database Transaction
#'
#' This function rolls back a transaction by executing the `ROLLBACK` statement.
#'
#' @param con A DBI connection object.
#' @keywords internal
rollback_transaction <- function(con) {
  queryStmt(con, "ROLLBACK;")
}
