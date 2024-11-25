# Relational databases such as SQL Server are optimised for joins
# therefore if you have two or more tables in ADM SQL database you wish to join
# it is a good idea to join them in SQL before reading into R, rather than
# reading full tables into R and joining them in your R session. This
# code gives an example of doing this using dbplyr and a simple workflow using
# dplyr::left_join

library(tidyverse)

set.seed(42) # reproducible example by setting seed val

# Set the connection details for your setup
server <- ""
database <- ""
schema <- ""


# Generate synthetic customers and orders tables
customers <- data.frame(
  customer_id = 1:10000,
  name = paste0("Customer_", 1:10000),
  age = sample(18:75, 10000, replace = TRUE)
)

orders <- data.frame(
  order_id = 1:10000,
  customer_id = sample(1:12000, 10000, replace = TRUE),
  amount = round(runif(10000, 20, 1000), 2)
)


# Load customers and orders into database
# using RtoSQLServer write_dataframe_to_db -----------------
RtoSQLServer::write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_customers",
  dataframe = customers,
  append_to_existing = TRUE,
  batch_size = 10000,
  versioned_table = FALSE
)

RtoSQLServer::write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_orders",
  dataframe = orders,
  append_to_existing = TRUE,
  batch_size = 10000,
  versioned_table = FALSE
)

# tidy up environment
rm(list = c("orders", "customers"))

# Using dplyr / dbplyr to make a join in database
# and return aggregated result ------------------------------------------------

# make the con
con <- RtoSQLServer::create_sqlserver_connection(
  server = server,
  database = database
)

# connect to database test_customers / test_orders tables
test_customers <- dplyr::tbl(con, dbplyr::in_schema(schema, "test_customers"))

test_orders <- dplyr::tbl(con, dbplyr::in_schema(schema, "test_orders"))

# Analysis process
# Request: I want a sum of order amounts per age for all customers 45 or under

# 1. Filter on age,
# 2. Join customers and orders
# 3. group by age
# 4. sum amount
# (steps 1 - 4 entirely done on database side)
# 5. Finally, collect result into R data frame.

test_joined_df <- test_customers %>%
  dplyr::filter(age <= 45) %>%
  dplyr::left_join(test_orders, by = "customer_id") %>%
  dplyr::group_by(age) %>%
  dplyr::summarise(total_amount = sum(amount)) %>%
  dplyr::arrange(age) %>%
  dplyr::collect()

# Clean-up test example - 
# drop tables from database (RtoSQLServer) and disconnect (DBI) ---------------

RtoSQLServer::drop_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_customers"
)

RtoSQLServer::drop_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_orders"
)

DBI::dbDisconnect(con)
