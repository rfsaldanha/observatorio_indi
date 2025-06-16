# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)

# Parameters
years <- 2000:2023

# Download data
files_list <- list.files(path = "../dados/sinan_chagas/")

for (y in years) {
  # Skip if file already exists
  if (glue("sinan_chagas_{y}.parquet") %in% files_list) {
    next
  }

  message(glue("Year: {y}"))

  tmp1 <- fetch_datasus(
    year_start = y,
    year_end = y,
    information_system = "SINAN-CHAGAS"
  )

  tmp2 <- process_sinan_chagas(data = tmp1, municipality_data = FALSE)
  rm(tmp1)

  write_parquet(
    x = tmp2,
    sink = glue("../dados/sinan_chagas/sinan_chagas_{y}.parquet")
  )
  rm(tmp2)
}
