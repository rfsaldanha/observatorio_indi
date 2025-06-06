# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(stringr)
library(glue)

# Parameters
years <- 2000:2023
ufs <- c(
  "AC",
  "AL",
  "AP",
  "AM",
  "BA",
  "CE",
  "DF",
  "ES",
  "GO",
  "MA",
  "MT",
  "MS",
  "MG",
  "PA",
  "PB",
  "PR",
  "PE",
  "PI",
  "RJ",
  "RN",
  "RS",
  "RO",
  "RR",
  "SC",
  "SP",
  "SE",
  "TO"
)

# Download data
files_list <- list.files(path = "../dados/sim/")

for (uf in ufs) {
  for (y in years) {
    # Skip if file already exists
    if (glue("sim_{uf}_{y}.parquet") %in% files_list) next

    # Skip file with problem
    if (uf == "GO" & y == 2005) next
    if (uf == "PA" & y == 2002) next

    message(glue("UF: {uf} Year: {y}"))

    tmp1 <- fetch_datasus(
      year_start = y,
      year_end = y,
      uf = uf,
      information_system = "SIM-DO"
    )

    tmp2 <- process_sim(data = tmp1, municipality_data = FALSE)
    rm(tmp1)

    write_parquet(
      x = tmp2,
      sink = glue("../dados/sim/sim_{uf}_{y}.parquet")
    )
    rm(tmp2)
  }
}
