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
  path = "../dados/sinan_zika/",
  pattern = "*.parquet",
  full.names = TRUE
)

# Lazy dataset
dataset <- open_dataset(sources = files_list)

# Data preparation
prep_dataset <- dataset |>
  # Filter confirmed cases only
  filter(CLASSI_FIN %in% c("Confirmado")) |>
  # Select variables
  select(
    ID_MN_RESI,
    date = DT_SIN_PRI,
    sex = CS_SEXO,
    age = IDADEanos
  ) |>
  # Age as numeric
  mutate(
    age = as.numeric(age)
  ) |>
  # Filter out records with invalid municipality codes
  mutate(mn_nchar = nchar(ID_MN_RESI)) |>
  filter(mn_nchar >= 6 & mn_nchar <= 7) |>
  select(-mn_nchar) |>
  # Municipality, UF and health region code
  mutate(
    geocodmu = as.numeric(substr(ID_MN_RESI, 0, 6)),
    geocoduf = as.numeric(substr(ID_MN_RESI, 0, 2))
  ) |>
  select(-ID_MN_RESI) |>
  left_join(mun_reg_saude_449, by = c("geocodmu" = "code_muni")) |>
  rename(geocodrs = codi_reg_saude) |>
  relocate(c(geocoduf, geocodrs, geocodmu), .before = date)

## Write to database
if (dbExistsTable(con, Id(schema = schema, table = "sinan_zika"))) {
  dbRemoveTable(con, Id(schema = schema, table = "sinan_zika"))
}

dbWriteTable(
  conn = con,
  name = Id(schema = schema, table = "sinan_zika"),
  value = prep_dataset |> collect()
)

## Verify records
tbl(con, Id(schema = schema, table = "sinan_zika")) |> tally()

## Database disconnect
dbDisconnect(con)
