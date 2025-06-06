# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)

# Parameters
years <- 2000:2024

# Download data
files_list <- list.files(path = "../dados/sinan_dengue/")

for (y in years) {
  # Skip if file already exists
  if (glue("sinan_dengue_{y}.parquet") %in% files_list) next

  # Skip file with problem
  if (y == 2008) next

  message(glue("Year: {y}"))

  tmp1 <- fetch_datasus(
    year_start = y,
    year_end = y,
    information_system = "SINAN-DENGUE"
  )

  tmp2 <- process_sinan_dengue(data = tmp1, municipality_data = FALSE)
  rm(tmp1)

  if ("CON_CLASSI" %in% names(tmp2)) {
    tmp2 <- tmp2 |>
      mutate(
        CLASSI_FIN = case_match(
          CON_CLASSI,
          "1" ~ "Dengue leve",
          "6" ~ "Dengue leve",
          "10" ~ "Dengue leve",
          "2" ~ "Dengue moderada",
          "7" ~ "Dengue moderada",
          "11" ~ "Dengue moderada",
          "3" ~ "Dengue grave",
          "4" ~ "Dengue grave",
          "8" ~ "Inconclusivo",
          "12" ~ "Dengue grave",
          "5" ~ "Descartado",
          "13" ~ "Chikungunya",
          .default = CON_CLASSI
        )
      )
  }

  tmp2 <- tmp2 |>
    mutate(
      CLASSI_FIN = case_match(
        CLASSI_FIN,
        "Dengue clássico" ~ "Dengue leve",
        "Dengue com complicações" ~ "Dengue moderada",
        "Febre hemorrágica do dengue" ~ "Dengue grave",
        "Síndrome do choque do dengue" ~ "Dengue moderada",
        "Dengue" ~ "Dengue leve",
        "Dengue com sinais de alarme" ~ "Dengue moderada",
        .default = CLASSI_FIN
      )
    )

  write_parquet(
    x = tmp2,
    sink = glue("../dados/sinan_dengue/sinan_dengue_{y}.parquet")
  )
  rm(tmp2)
}

# Dengue classification following
# https://epidemiologista.quarto.pub/relatorio-butantan/
