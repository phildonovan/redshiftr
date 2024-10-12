#' Copy a data frame to Redshift with chunked insertion, with an optional overall rollback on failure
#'
#' This function copies a data frame to a Redshift table, generating the `CREATE TABLE` SQL
#' and inserting the data in chunks. Each chunk is inserted within its own transaction,
#' and an optional overarching transaction can be controlled with `rollback_on_failure`.
#' If `rollback_on_failure` is set to TRUE, the function will attempt to insert all chunks in
#' one overarching transaction, rolling back entirely in case of any errors.
#'
#' The function splits the data frame into smaller chunks to avoid exceeding Redshift's
#' statement size limits. If a chunk is too large to insert, it will be repeatedly split in half
#' until the size is acceptable or the process fails.
#'
#' @param con A DBI connection object to Redshift. Should be an open connection.
#' @param df The data frame to be copied to Redshift. This data frame should contain the data
#'   to be inserted.
#' @param table_name The name of the table to be created or inserted into. The table name is
#'   internally wrapped in `I()` to ensure it is safely quoted.
#' @param chunk_size The initial number of rows per chunk to insert (default is 10000). This
#'   value will be adjusted if the SQL statement exceeds the maximum allowed size.
#' @param rollback_on_failure Logical flag indicating whether to roll back the entire transaction
#'   on failure (default is TRUE). If set to FALSE, only individual chunks that fail will be rolled back.
#' @return NULL. The function performs the insertion and does not return a value.
#' @details The function uses transactions to ensure data integrity during the insertion process.
#'   Each chunk of data is inserted within its own transaction, and an overarching transaction
#'   is used if `rollback_on_failure` is set to TRUE. This provides flexibility in handling failures.
#'   Note that this function may take some time for very large data frames, as chunks may need
#'   to be repeatedly split if they exceed the Redshift statement size limit.
#' @examples
#' \dontrun{
#' # Example using dbplyr's `simulate_redshift` for testing without a real Redshift connection:
#' library(dbplyr)
#' con <- simulate_redshift()
#' df <- data.frame(a = 1:10000, b = rnorm(10000))
#' redshift_copy_to(con, df, table_name = "my_table")
#' }
#' @export
redshift_copy_to <- function(con, df, table_name, chunk_size = 10000, rollback_on_failure = TRUE) {

  # Define the maximum statement size allowed by Redshift
  max_statement_size <- 16777216  # 16 MB

  # Protect the table name for use in db connections.
  table_name <- I(table_name)

  # Step 1: Create the table within a transaction
  begin_transaction(con)
  tryCatch({
    create_table_sql <- generate_create_table_sql(df, table_name)
    queryStmt(con, create_table_sql)
    commit_transaction(con)
    message("Table created: ", table_name)
  }, error = function(e) {
    rollback_transaction(con)
    stop("Table creation failed: ", e$message)
  })

  # Step 2: Begin overarching transaction if rollback_on_failure is TRUE
  if (rollback_on_failure) {
    begin_transaction(con)
  }

  # Step 3: Insert data in chunks
  tryCatch({
    # Split the data into chunks
    total_rows <- nrow(df)
    row_indices <- seq_len(total_rows)
    chunked_indices <- split(row_indices, ceiling(seq_along(row_indices) / chunk_size))

    # Insert data in chunks, each in its own transaction
    walk(
      chunked_indices,
      function(index_chunk) {
        # Keep attempting to insert until it's small enough to succeed
        df_chunk <- df[index_chunk, , drop = FALSE]
        while (TRUE) {
          # Generate the insert SQL
          insert_sql <- rs_insert_sql(df_chunk, table_name, con)

          # If the size is acceptable, execute it within its own transaction
          if (nchar(insert_sql) <= max_statement_size) {
            begin_transaction(con)
            tryCatch({
              queryStmt(con, insert_sql)
              commit_transaction(con)
              break
            }, error = function(e) {
              rollback_transaction(con)
              message("Chunk insertion failed and rolled back: ", e$message)
              break
            })
          } else {
            # If too large, halve the chunk and try again
            chunk_size <- ceiling(nrow(df_chunk) / 2)
            df_chunk <- df_chunk[seq_len(chunk_size), , drop = FALSE]
          }
        }
      },
      .progress = TRUE
    )

    # Commit overarching transaction if rollback_on_failure is TRUE
    if (rollback_on_failure) {
      commit_transaction(con)
      message("All chunks inserted successfully, transaction committed.")
    }

  }, error = function(e) {
    # Rollback overarching transaction if rollback_on_failure is TRUE and an error occurs
    if (rollback_on_failure) {
      rollback_transaction(con)
      message("Overarching transaction rolled back due to error: ", e$message)
    }
    stop(e)  # Rethrow the error after rollback
  })
}



#' Generate a single SQL insert command for multiple rows
#'
#' This function generates a combined SQL `INSERT INTO` statement for a chunk of data,
#' so that all rows in the chunk are inserted with one `dbExecute()` call.
#'
#' @param df_chunk A chunk of the data frame for which to generate a single `INSERT INTO` statement.
#' @param table_name The name of the table where data will be inserted.
#' @param con A DBI connection object to Redshift.
#' @return A single string containing a combined SQL `INSERT INTO` statement.
#' @export
rs_insert_sql <- function(df_chunk, table_name, con) {

  # Full table name with schema protection
  full_table_name <- I(table_name)

  # Trim character columns, replace empty strings with NA, and convert geometry to EWKB
  df_chunk <- purrr::imap_dfc(df_chunk, ~ {
    if (inherits(.x, 'sfc')) {
      paste0("ST_GeomFromEWKT('", sf::st_as_text(.x, EWKT = TRUE), "')")
    } else if (is.character(.x)) {
      ifelse(str_trim(.x) == '', NA, str_trim(.x))
    } else if (inherits(.x, 'Date')) {
      format(.x, '%Y-%m-%d')
    } else if (inherits(.x, 'POSIXt')) {
      format(.x, '%Y-%m-%d %H:%M:%S')
    } else {
      .x
    }
  })

  # return(df_chunk)
  # Process each column based on its type for SQL insertion
  df_chunk <- df_chunk |>
    mutate(
      across(
        everything(),
        ~ case_when(
          is.na(.) ~ "DEFAULT",
          is.numeric(.) ~ as.character(.),
          is.character(.) ~ ifelse(
            grepl("^ST_GeomFromEWKT", .),
            .,
            as.character(dbQuoteString(con, as.character(.)))
          )
        )
      )
    )

  # Use unite() to combine all columns into a single value string for each row
  values_list <- df_chunk |>
    unite("value_string", everything(), sep = ", ", remove = FALSE) |>
    mutate(value_string = paste0("(", value_string, ")")) |>
    pull(value_string)

  # Combine all rows into one `INSERT INTO` statement
  combined_values <- paste(values_list, collapse = ", ")

  # Generate the final SQL statement
  insert_sql <- paste0("INSERT INTO ", full_table_name, " VALUES ", combined_values, ";")

  return(insert_sql)
}
