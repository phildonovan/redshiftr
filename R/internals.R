# Internal utility functions used by the redshift tools

if(getRversion() >= "2.15.1")  utils::globalVariables(c("i", "obj"))

#' Upload Data to S3 Bucket
#'
#' This function uploads a data frame to an S3 bucket, splitting it into multiple files if necessary.
#' The function checks if the bucket exists and retries in case of errors during upload.
#'
#' @param data A data frame to upload to S3.
#' @param bucket The name of the S3 bucket.
#' @param split_files Number of files to split the data into for upload.
#' @param key AWS access key for authentication.
#' @param secret AWS secret key for authentication.
#' @param session AWS session token for authentication (optional).
#' @param region AWS region where the bucket is located.
#' @return A prefix string used for the uploaded files, or NA if the upload fails.
#' @keywords internal
#' @importFrom paws s3
#' @importFrom readr format_csv
#' @importFrom purrr map2
#' @importFrom progress progress_bar
uploadToS3 <- function(data, bucket, split_files, key, secret, session, region){

  prefix <- paste0(sample(rep(letters, 10), 50), collapse = "")

  resp <- bucket_exists(bucket)

  if (is.null(attributes(resp))) {
    # Do nothing
  } else if (attributes(resp)$status_code %in% 404) {
    stop("Bucket does not exist")
  } else if (attributes(resp)$status_code %in% 403) {
    stop("Access denied; please check AWS credentials")
  }

  splitted <- suppressWarnings(split(data, seq(1:split_files)))

  message(paste("Uploading", split_files, "files with prefix", prefix, "to bucket", bucket))

  pb <- progress_bar$new(total = split_files, format = 'Uploading file :current/:total [:bar]')
  pb$tick(0)

  upload_part <- function(part, i) {

    s3Name <- paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep = "")

    # Put part on S3; retry 500 errors (three times)
    upload_response <- NULL
    attempt_count <- 0

    while (is.null(upload_response) | inherits(upload_response, "http_500") & attempt_count < 3) {
      attempt_count <- attempt_count + 1
      upload_response <- tryCatch(put_object(.data = part, bucket = bucket, key = s3Name),
                                  error = function(e) e)
      if (inherits(upload_response, "http_500") & attempt_count < 3) {
        print(paste0("Request failed with 500 error and message: ", upload_response$message))
        print("Retrying after two-second sleep")
        Sys.sleep(2)
      } else if (inherits(upload_response, "error")) {
        # Re-raise the error object on non-500 error or 500 error with maxed retries
        stop(upload_response)
      }
    }

    pb$tick()
    return(upload_response)
  }

  res <- map2(splitted, 1:split_files, upload_part)

  if (length(which(!unlist(res))) > 0) {
    warning("Error uploading data!")
    return(NA)
  } else {
    message("Upload to S3 complete!")
    return(prefix)
  }
}

#' Delete S3 Objects with a Given Prefix
#'
#' This function deletes multiple objects in an S3 bucket that share a common prefix.
#' It retries the deletion process in case of 500 errors.
#'
#' @param prefix The prefix of the objects to delete in the S3 bucket.
#' @param bucket The name of the S3 bucket.
#' @param split_files Number of files to delete.
#' @param key AWS access key for authentication.
#' @param secret AWS secret key for authentication.
#' @param session AWS session token for authentication (optional).
#' @param region AWS region where the bucket is located.
#' @keywords internal
#' @importFrom purrr map
deletePrefix <- function(prefix, bucket, split_files, key, secret, session, region){

  s3Names <- paste(prefix, ".", formatC(1:split_files, width = 4, format = "d", flag = "0"), sep = "")

  message(paste("Deleting", split_files, "files with prefix", prefix, "from bucket", bucket))

  pb <- progress_bar$new(total = split_files, format = 'Deleting file :current/:total [:bar]')
  pb$tick(0)

  deleteObj <- function(key) {

    # Delete object from S3; retry 500 errors (three times)
    delete_response <- NULL
    attempt_count <- 0

    while (is.null(delete_response) | inherits(delete_response, "http_500") & attempt_count < 3) {
      attempt_count <- attempt_count + 1
      delete_response <- tryCatch(delete_object(bucket, key = key),
                                  error = function(e) e)
      if (inherits(delete_response, "http_500") & attempt_count < 3) {
        print(paste0("Request failed with 500 error and message: ", delete_response$message))
        print("Retrying after two-second sleep")
        Sys.sleep(2)
      } else if (inherits(delete_response, "error")) {
        # Re-raise the error object on non-500 error or 500 error with maxed retries
        stop(delete_response)
      }
    }

    pb$tick()
  }

  res <- map(s3Names, deleteObj)
}

#' Execute a SQL Query and Return Results
#'
#' This function executes a SQL query and returns the results.
#'
#' @param dbcon A database connection object.
#' @param query A SQL query string to be executed.
#' @return A data frame containing the query results.
#' @keywords internal
#' @importFrom DBI dbGetQuery
queryDo <- function(dbcon, query){
  dbGetQuery(dbcon, query)
}

#' Execute a SQL Statement Without Returning Results
#'
#' This function executes a SQL statement that does not return results (e.g., CREATE, INSERT).
#'
#' @param dbcon A database connection object.
#' @param query A SQL statement string to be executed.
#' @keywords internal
#' @importFrom DBI dbExecute
queryStmt <- function(dbcon, query){
  if (inherits(dbcon, 'JDBCConnection')) {
    RJDBC::dbSendUpdate(dbcon, query)
  } else {
    dbExecute(dbcon, query)
  }
}

#' Determine Optimal Split Size for Data Upload
#'
#' This function determines the optimal number of files to split the data into for uploading to Redshift.
#' It calculates the number of slices in Redshift and adjusts the split size accordingly.
#'
#' @param dbcon A database connection object.
#' @param numRows Number of rows in the data.
#' @param rowSize Estimated size of each row in bytes.
#' @return The number of files to split the data into.
#' @keywords internal
splitDetermine <- function(dbcon, numRows, rowSize){
  message("Getting number of slices from Redshift")
  slices <- queryDo(dbcon, "select count(*) from stv_slices")
  slices_num <- pmax(as.integer(round(slices[1, 'count'])), 1)
  split_files <- slices_num

  bigSplit <- pmin(floor((numRows * rowSize) / (256 * 1024 * 1024)), 5000) # 200MB per file, up to 5000 files
  smallSplit <- pmax(ceiling((numRows * rowSize) / (10 * 1024 * 1024)), 1) # 10MB per file, very small files

  if (bigSplit > slices_num) {
    split_files <- slices_num * round(bigSplit / slices_num) # Round to nearest multiple of slices, optimizes the load
  } else if (smallSplit < slices_num) {
    split_files <- smallSplit
  } else {
    split_files <- slices_num
  }

  message(sprintf("%s slices detected, will split into %s files", slices, split_files))
  return(split_files)
}

#' Copy Data from S3 to Redshift
#'
#' This function copies data from an S3 bucket to a Redshift table using the COPY command.
#' It
