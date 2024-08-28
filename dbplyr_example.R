# Simple example of a dbplyr workflow
# See https://dbplyr.tidyverse.org/

# This workflow can be used as an alternative to reading large tables into R
# It can speed up processes, particularly over VPN and
# save your machine from running out of memory for particularly large tables

library(tidyverse) # functions used in this script are qualified with the
# package name for clarity, e.g. dplyr::filter, loading tidyverse for %>% pipe

# create demo table and load to database ----------------------------------

server <- "" # change this for your ADM server
database <- "" # change this for your ADM database
schema <- "" # change this for your schema in the database

db_table_name <- "test_agg_tbl" # change this for the name of a table you
                                # will create in ADM database for testing

# create demo table with dummy data
number_of_rows <- 100000

val_col <- runif(number_of_rows, 0, 1000)

cat_col <- sample(c("a", "b", "c", "d"),
                  number_of_rows,
                  replace = TRUE,
                  prob = c(0.19, 0.3, 0.5, 0.01)
)

load_df <- data.frame(cat_col = cat_col, val_col = val_col)

# Load into database using RtoSQLServer write_dataframe_to_db -----------------
RtoSQLServer::write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = db_table_name,
  dataframe = load_df,
  append_to_existing = TRUE,
  batch_size = 10000,
  versioned_table = FALSE
)

# tidy up env now data frame loaded into database
rm(list = c("load_df", "cat_col", "val_col"))

# Query table without reading it into local memory ---------------------------

# Firstly make a connection object using DBI::dbConnect
con <- DBI::dbConnect(odbc::odbc(),
                 Driver = "SQL Server",
                 Server = server,
                 Database = database,
)

# use dplyr::tbl to make reference to object using dbplyr::in_schema
# the tbl function creates a list in R environment
test_table <- dplyr::tbl(con, dbplyr::in_schema(schema, "test_agg_tbl"))

# preview the table - a good way to see table structure without reading full
# table into R is dplyr::glimpse
dplyr::glimpse(test_table)

# An alternative to glimpse if want a data frame 
# is to use dplyr slice_sample - need to collect(), see below
test_table %>% dplyr::slice_sample(n = 10) %>% dplyr::collect()

# if we pipe a dplyr function to show_query we can see the SQL it will use
# This is recommended to check your SQL will do what you expect
test_table %>%
  dplyr::filter(cat_col != "d") %>%
  dplyr::show_query()

# can filter, group_by, summarise
# Finally return aggregated result as data frame using `dplyr::collect()`

my_result <- test_table %>%
  dplyr::filter(val_col > 123.5) %>%
  dplyr::group_by(cat_col) %>%
  dplyr::summarise(test_total = sum(val_col)) %>%
  dplyr::collect()


# Clean-up - drop table from database (RtoSQLServer) and disconnect (DBI)------

RtoSQLServer::drop_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = db_table_name
)

DBI::dbDisconnect(con)
