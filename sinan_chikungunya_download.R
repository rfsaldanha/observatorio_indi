# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)

# Parameters
years <- 2015:2024

# Download data
files_list <- list.files(path = "../dados/sinan_chikungunya/")

for (y in years) {
  # Skip if file already exists
  if (glue("sinan_chikungunya_{y}.parquet") %in% files_list) {
    next
  }

  message(glue("Year: {y}"))

  tmp1 <- fetch_datasus(
    year_start = y,
    year_end = y,
    information_system = "SINAN-CHIKUNGUNYA"
  )

  tmp2 <- process_sinan_chikungunya(data = tmp1, municipality_data = FALSE)
  rm(tmp1)

  write_parquet(
    x = tmp2,
    sink = glue("../dados/sinan_chikungunya/sinan_chikungunya_{y}.parquet")
  )
  rm(tmp2)
}
