# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)

# Parameters
years <- 2016:2024

# Download data
for (y in years) {
  message(glue("Year: {y}"))

  tmp1 <- fetch_datasus(
    year_start = y,
    year_end = y,
    information_system = "SINAN-ZIKA"
  )

  tmp2 <- process_sinan_zika(data = tmp1, municipality_data = FALSE)
  rm(tmp1)

  write_parquet(
    x = tmp2,
    sink = glue("../dados/sinan_zika/sinan_zika_{y}.parquet")
  )
  rm(tmp2)
}
