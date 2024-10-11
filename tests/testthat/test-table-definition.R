# Define the test for rs_create_statement using mtcars dataset
test_that("rs_create_statement generates correct SQL for mtcars dataset", {
  # Use the built-in mtcars dataset
  df <- mtcars

  # Set parameters for the table
  table_name <- "mtcars_table"
  sortkeys <- c("mpg", "cyl")
  distkey <- "gear"

  # Generate the CREATE TABLE statement
  create_sql <- rs_create_statement(df, table_name, sortkeys = sortkeys, distkey = distkey)

  # Expected SQL pattern
  expect_true(grepl("CREATE TABLE mtcars_table", create_sql))
  expect_true(grepl("mpg float8", create_sql))
  expect_true(grepl("cyl float8", create_sql))
  expect_true(grepl("SORTKEY \\(mpg, cyl\\)", create_sql))
  expect_true(grepl("DISTKEY \\(gear\\)", create_sql))
})


# Define the test for rs_create_statement using iris dataset
test_that("rs_create_statement generates correct SQL for iris dataset", {
  # Use the built-in iris dataset
  df <- iris

  # Set parameters for the table
  table_name <- "iris_table"
  sortkeys <- c("Sepal.Length", "Species")
  distkey <- "Species"

  # Generate the CREATE TABLE statement
  create_sql <- rs_create_statement(df, table_name, sortkeys = sortkeys, distkey = distkey)

  # Expected SQL pattern
  expect_true(grepl("CREATE TABLE iris_table", create_sql))
  expect_true(grepl("Sepal\\.Length float8", create_sql))
  expect_true(grepl("Species varchar\\(.*\\)", create_sql))
  expect_true(grepl("SORTKEY \\(Sepal\\.Length, Species\\)", create_sql))
  expect_true(grepl("DISTKEY \\(Species\\)", create_sql))
})



# Define the test for rs_create_statement using sf geometry dataset
test_that("rs_create_statement generates correct SQL for sf geometry dataset", {
  # Load the sf library
  library(sf)

  # Create a simple sf object with geometry
  df <- st_as_sf(data.frame(id = 1:5, x = runif(5), y = runif(5)), coords = c("x", "y"), crs = 4326)

  # Set parameters for the table
  table_name <- "geometry_table"
  sortkeys <- c("id")
  distkey <- "id"

  # Generate the CREATE TABLE statement
  create_sql <- rs_create_statement(df, table_name, sortkeys = sortkeys, distkey = distkey)

  # Expected SQL pattern
  expect_true(grepl("CREATE TABLE geometry_table", create_sql))
  expect_true(grepl("id int4", create_sql))
  expect_true(grepl("geometry GEOMETRY", create_sql))
  expect_true(grepl("SORTKEY \\(id\\)", create_sql))
  expect_true(grepl("DISTKEY \\(id\\)", create_sql))
})
