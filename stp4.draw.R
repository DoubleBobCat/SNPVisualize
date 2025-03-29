library(tidyverse)
library(gridExtra)
library(dplyr)
library(patchwork)

# Why use snake: The IDE told me do this or they mark all unuse :(

# Function to generate hsl colors
generate_hsl_colors <- function(n, s = 100, l = 80) {
  hues <- seq(0, 360, length.out = n + 1)[1:n]
  hcl(h = hues, l = l, c = s)
}

# Get all csv files in stp3
csv_files <- list.files(
  "../stp3_drawPrepare",
  pattern = "\\.csv$",
  full.names = TRUE
)

# Loop each csv
for (file in csv_files) {
  # Prepare data
  df <- read_csv(file, col_types = cols())
  n <- nrow(df)
  m <- ncol(df)
  file_name <- tools::file_path_sans_ext(basename(file))
  print(paste(file_name, "...Begin"))

  # Replace NA to avoid potential bug
  df_processed <- df %>%
    mutate(
      across(starts_with("value_"), ~ replace_na(as.character(.), "VNA")),
      across(starts_with("count_"), ~ replace_na(as.numeric(.), 0))
    )

  # Split source df to df_form, df_value, and df_count
  df_form <- df_processed %>% mutate_all(as.character)
  df_value <- df_processed %>%
    select(-starts_with("count_")) %>%
    select(-value_1)
  df_count <- df_processed %>%
    select(-starts_with("value_"))

  # Get colors
  unique_values <- df_value %>%
    select(-POS) %>%
    unlist() %>%
    subset(. != "VNA") %>%
    unique()
  value_color <- generate_hsl_colors(length(unique_values))
  names(value_color) <- unique_values

  # Get pos levels
  pos_levels <- rev(as.character(unique(df_form$POS)))

  # Replace back VNA and 0 to empty but not NA
  df_form[df_form == "VNA"] <- ""
  df_form[df_form == "0"] <- ""

  # Count fig_count data
  fig_count_x_max <- df_count %>%
    select(-POS) %>%
    rowSums() %>%
    max()
  df_count <- df_count %>%
    select(-count_1)
  plot_data <- df_count %>%
    pivot_longer(-POS, names_to = "cat") %>%
    mutate(i = str_replace(cat, "count_", "")) %>%
    left_join(
      df_value %>%
        pivot_longer(-POS, names_to = "val_col") %>%
        mutate(i = str_replace(val_col, "value_", "")),
      by = c("POS", "i")
    ) %>%
    mutate(POS = factor(POS, levels = pos_levels))

  # Prepare fig_form data
  df_form <- df_form %>%
    pivot_longer(-POS, names_to = "Column") %>%
    mutate(
      POS = factor(POS, levels = pos_levels),
      Column = factor(Column, levels = unique(Column))
    )
  df_form$Column <- as.character(df_form$Column)

  # Fraw Form Fig
  fig_form <- ggplot(
    df_form,
    aes(x = Column, y = POS),
  ) +
    geom_tile(
      stat = "identity",
      data = . %>% filter(str_starts(Column, "count_")),
      aes(fill = as.numeric(value)),
      color = "gray80"
    ) +
    geom_tile(
      stat = "identity",
      data = . %>% filter(str_starts(Column, "value_")),
      fill = "white",
      color = "gray80"
    ) +
    geom_text(
      aes(label = value),
      size = 3,
      stat = "identity",
      family = "serif"
    ) +
    scale_fill_gradient(low = "white", high = "red", na.value = "white") +
    scale_x_discrete(
      limits = unique(df_form$Column),
      position = "bottom",
      labels = ~ case_when(
        .x == "value_1" ~ "Ref",
        str_starts(.x, "value_") ~ "SNP",
        str_starts(.x, "count_") ~ "count",
        TRUE ~ .x
      )
    ) +
    theme_void() +
    theme(
      axis.text = element_text(),
      axis.text.x = element_text(angle = 90, hjust = 1),
      axis.text.y = element_text(angle = 0, hjust = 1),
      legend.position = "bottom",
      text = element_text(family = "serif")
    ) +
    labs(fill = "")

  # Fraw Count Fig
  fig_count <- ggplot(
    plot_data,
    aes(
      x = value.x,
      y = POS,
      fill = value.y,
      label = ifelse(value.x > 0, value.x, "")
    )
  ) +
    geom_col(position = "stack", width = 0.8) +
    geom_text(
      position = position_stack(vjust = 0.5),
      family = "serif",
      size = 3
    ) +
    scale_fill_manual(values = value_color) +
    theme_void() +
    scale_x_continuous(
      limits = c(0, fig_count_x_max),
      breaks = seq(0, fig_count_x_max, by = 20),
      minor_breaks = scales::breaks_width(10)
    ) +
    guides(
      x = guide_axis(cap = "lower"),
    ) +
    theme(
      axis.text.y = element_blank(),
      axis.text.x = element_text(family = "serif"),
      axis.line.x.bottom = element_line(),
      axis.ticks.x = element_line(),
      axis.ticks.length = unit(0.1, "cm"),
      panel.grid = element_blank(),
      legend.position = "right",
      text = element_text(family = "serif")
    ) +
    labs(fill = "")

  combined <- fig_form + fig_count + plot_layout(guides = "collect")

  dir.create("../figs/jpg", recursive = TRUE, showWarnings = FALSE)
  dir.create("../figs/svg", recursive = TRUE, showWarnings = FALSE)
  ggsave(
    paste0("../figs/jpg/", file_name, ".jpg"),
    combined,
    width = 10,
    height = (35 * n / 200) + 1,
    units = "in",
    limitsize = FALSE
  )
  ggsave(
    paste0("../figs/svg/", file_name, ".svg"),
    combined,
    width = 10,
    height = (35 * n / 200) + 1,
    units = "in",
    limitsize = FALSE
  )

  print(paste(file_name, "...Done"))
}
