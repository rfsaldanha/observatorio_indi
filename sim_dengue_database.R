# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)
library(brpop)
library(RPostgres)
library(DBI)

# Database connection
con <- dbConnect(
  drv = Postgres(),
  host = Sys.getenv("psql_local_host"),
  dbname = Sys.getenv("psql_db"),
  user = Sys.getenv("psql_local_user"),
  password = Sys.getenv("psql_local_psw")
)

schema <- "eventos_saude_2"

# Files list
files_list <- list.files(
  path = "../dados/sim/",
  pattern = "*.parquet",
  full.names = TRUE
)

# Lazy dataset
dataset <- open_dataset(sources = files_list)

# Data preparation
prep_dataset <- dataset |>
  # Filter dengue deaths
  filter(str_detect(string = CAUSABAS, pattern = "^A90|^A91")) |>
  # Select variables
  select(
    CODMUNRES,
    date = DTOBITO,
    sex = SEXO,
    age = IDADEanos
  ) |>
  # Age as numeric
  # mutate(
  #   age = as.numeric(age)
  # ) |>
  # Filter out records with invalid municipality codes
  mutate(mn_nchar = nchar(CODMUNRES)) |>
  filter(mn_nchar == 6) |>
  select(-mn_nchar) |>
  # Municipality, UF and health region code
  mutate(
    geocodmu = as.numeric(substr(CODMUNRES, 0, 6)),
    geocoduf = as.numeric(substr(CODMUNRES, 0, 2))
  ) |>
  select(-CODMUNRES) |>
  left_join(mun_reg_saude_449, by = c("geocodmu" = "code_muni")) |>
  rename(geocodrs = codi_reg_saude) |>
  relocate(c(geocoduf, geocodrs, geocodmu), .before = date) |>
  # Fix age
  mutate(across(age, ~ coalesce(.x, "0"))) |>
  mutate(age = as.numeric(age))

## Write to database
if (dbExistsTable(con, Id(schema = schema, table = "sim_dengue"))) {
  dbRemoveTable(con, Id(schema = schema, table = "sim_dengue"))
}

dbWriteTable(
  conn = con,
  name = Id(schema = schema, table = "sim_dengue"),
  value = prep_dataset |> collect()
)

## Verify records
tbl(con, Id(schema = schema, table = "sim_dengue")) |> tally()

## Database disconnect
dbDisconnect(con)
