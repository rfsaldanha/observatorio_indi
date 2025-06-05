# Packages
library(microdatasus)
library(dplyr)
library(arrow)
library(glue)

# Parameters
years <- 2009:2024

# Download data
for (y in years) {
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
    sink = glue("../dados/dengue_sinan/dengue_sinan_{y}.parquet")
  )
  rm(tmp2)
}

# Dengue classification following
# https://epidemiologista.quarto.pub/relatorio-butantan/
