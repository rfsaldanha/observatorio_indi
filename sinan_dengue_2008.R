# Packages
library(dplyr)
library(arrow)
library(glue)

tmp <- read_parquet(file = "../dados/backup/dengue_2008_bkp.parquet")

tmp <- tmp |>
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
  x = tmp,
  sink = "../dados/sinan_dengue/sinan_dengue_2008.parquet"
)
