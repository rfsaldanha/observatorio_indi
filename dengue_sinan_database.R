# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)
library(brpop)

# Files list
files_list <- list.files(
  path = "../dados/dengue_sinan/",
  pattern = "*.parquet",
  full.names = TRUE
)

# Lazy dataset
dataset <- open_dataset(sources = files_list)

# Data preparation
dataset |>
  select(
    ID_MN_RESI,
    date = DT_SIN_PRI,
    sex = CS_SEXO,
    age = IDADEanos,
    CLASSI_FIN
  ) |>
  collect() |>
  mutate(
    geocodmu = as.numeric(substr(ID_MN_RESI, 0, 6)),
    geocoduf = as.numeric(substr(ID_MN_RESI, 0, 2))
  ) |>
  select(-ID_MN_RESI) |>
  left_join(mun_reg_saude_449, by = c("geocodmu" = "code_muni")) |>
  rename(geocodrs = codi_reg_saude) |>
  relocate(c(geocoduf, geocodrs, geocodmu), .before = date)
