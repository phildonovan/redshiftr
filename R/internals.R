# Internal utility functions used by the Redshift tools

#' @importFrom paws s3
#' @importFrom readr format_csv
#' @importFrom purrr map2
upload_to_s3 <- function(data, bucket, split_files, key, secret, session, region) {
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

  upload_part <- function(part, i) {
    s3_name <- sprintf("%s/part_%02d.csv", prefix, i)
    s3$put_object(Bucket = bucket, Key = s3_name, Body = format_csv(part))
  }

  walk2(splitted, seq_along(splitted), upload_part, .progress = TRUE)
}
