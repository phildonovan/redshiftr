
<!-- README.md is generated from README.Rmd. Please edit that file -->

# redshiftr

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

`redshiftr` is an R package designed to help users interact with Amazon
Redshift, particularly in situations where direct access to an S3 bucket
is not available. This package is a continuation of the original
`redshiftTools`, which is no longer actively maintained. While building
on the excellent foundation provided by `redshiftTools`, `redshiftr`
introduces a few new features and enhancements:

- **Table Creation Without S3 Access:** Unlike the original package,
  `redshiftr` allows users to create tables in Redshift directly,
  without the need for an S3 bucket. Please note, however, that this
  process is considerably slower than the standard method and is
  recommended only for small tables.

- **Spatial Table Creation (Experimental):** The package also offers the
  ability to create spatial tables in Redshift, a feature that is
  currently under active development and is considered highly
  experimental.

This package is made possible thanks to the original authors of
`redshiftTools`, whose work has been extended and improved upon here.
Their contributions have laid the groundwork for this ongoing
development effort.

## Installation

You can install the latest version of redshiftr directly from GitHub
using the following command:

``` r
# Install remotes package if you haven't already
#install.packages("remotes")

# Install redshiftr from GitHub
remotes::install_github("phildonovan/redshiftr")
```

## Example

Below is a simple example demonstrating how to use the
`redshift_copy_to()` function to create a table in a Redshift instance.
In this example, we’ll use the iris dataset and the nc dataset from the
sf package.

Before starting, make sure you are connected to your Redshift database
using `DBI::dbConnect()`.

``` r
# Load necessary libraries
library(DBI)
library(redshiftr)
library(sf)

# Connect to your Redshift database (replace with your connection details)
con <- dbConnect(RPostgres::Postgres(),
                 dbname = "your_db_name",
                 host = "your_host",
                 port = 5439,
                 user = "your_username",
                 password = "your_password")

# Example 1: Copying the iris dataset to Redshift
# Create a table in Redshift using the iris dataset
redshift_copy_to(con, df = iris, table_name = "iris_table")

# Example 2: Copying the nc dataset (from sf package) to Redshift
# Load the nc dataset
nc <- st_read(system.file("shape/nc.shp", package = "sf"))

# Create a spatial table in Redshift using the nc dataset
redshift_copy_to(con, df = nc, table_name = "nc_table")

# Disconnect from the database
dbDisconnect(con)
```
