
packages <- c("DBI", "duckdb", "shiny", "bslib", "dplyr", "plotly","DT", "glue", "htmltools","shinybusy",
              "readxl","sf","purrr","stringr","tidyr", "leaflet")

for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}

cat("Packages chargés :", paste(packages, collapse = ", "), "\n")

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) return(y)
  if (length(x) == 1 && is.na(x)) return(y)
  if (length(x) == 1 && identical(x, "")) return(y)
  x
}

VALID_DEPARTMENTS <- c(
  sprintf("%02d", 1:19), "2A", "2B", sprintf("%02d", 21:95), "971", "972", "973", "974", "975", "976"
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
show_app_spinner <- function(session, message) {
  shiny::withReactiveDomain(session, {
    shinybusy::show_modal_spinner(
      spin = "double-bounce",
      text = tags$div(
        class = "app-spinner-text",
        message
      )
    )
  })
}
check_s3_credentials <- function() {
  required_vars <- c("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  missing_vars <- required_vars[!nzchar(Sys.getenv(required_vars))]
  list(ok = length(missing_vars) == 0, missing = missing_vars)
}

sql_escape_like <- function(x) {
  x <- gsub("'", "''", x, fixed = TRUE)
  x
}

fmt_n <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x)) return("N/A")
  format(round(x), big.mark = " ", scientific = FALSE)
}

metric_card <- function(label, value) {
  div(
    class = "metric-card",
    div(class = "metric-label", label),
    div(class = "metric-value", value)
  )
}

chip <- function(text, class = "chip") {
  tags$span(class = class, text)
}

detect_first_col <- function(df, candidates) {
  hit <- candidates[candidates %in% names(df)]
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

# -----------------------------------------------------------------------------
# Reference data
# -----------------------------------------------------------------------------

load_reference_data <- function(data_dir = "datas") {
  library(sf)
  library(dplyr)
  library(readxl)
  library(readr)
  
  # --- GeoJSON départements
  geo_path <- file.path(data_dir, "departement_avec_outremer_rapprochée.geojson")
  if (!file.exists(geo_path)) {
    stop("GeoJSON introuvable : ", geo_path)
  }
  
  geo <- sf::st_read(geo_path, quiet = TRUE)
  geo <- sf::st_make_valid(geo)
  
  if (is.na(sf::st_crs(geo))) {
    stop("Le GeoJSON n'a pas de CRS défini.")
  }
  
  geo <- sf::st_transform(geo, 4326)
  cat("[load_reference_data] CRS geo =", sf::st_crs(geo)$input, "\n")
  print(sf::st_bbox(geo))
  if (!all(c("code", "nom") %in% names(geo))) {
    stop(
      "Le geojson doit contenir au moins les colonnes 'code' et 'nom'. Colonnes trouvées : ",
      paste(names(geo), collapse = ", ")
    )
  }
  
  deps_geo <- geo %>%
    dplyr::mutate(
      departement = as.character(.data$code),
      nom_departement = as.character(.data$nom)
    )
  
  deps_ref <- deps_geo %>%
    sf::st_drop_geometry() %>%
    dplyr::select(.data$departement, .data$nom_departement)
  
  # --- Référentiel régions optionnel
  region_csv <- file.path(data_dir, "departements_regions.csv")
  
  if (file.exists(region_csv)) {
    dep_region <- readr::read_csv(region_csv, show_col_types = FALSE) %>%
      dplyr::mutate(
        departement = trimws(as.character(.data$departement)),
        region = trimws(as.character(.data$region))
      ) %>%
      dplyr::select(.data$departement, .data$region)
    
    deps_ref <- deps_ref %>%
      dplyr::mutate(departement = trimws(as.character(.data$departement))) %>%
      dplyr::left_join(dep_region, by = "departement")
    
    regions <- deps_ref %>%
      dplyr::filter(!is.na(.data$region), .data$region != "") %>%
      dplyr::distinct(.data$region) %>%
      dplyr::arrange(.data$region) %>%
      dplyr::pull(.data$region)
    
    deps_region_df <- deps_ref %>%
      dplyr::filter(!is.na(.data$region), .data$region != "") %>%
      dplyr::mutate(
        region = trimws(as.character(.data$region)),
        departement = trimws(as.character(.data$departement))
      ) %>%
      dplyr::arrange(.data$region, .data$departement)
    
    region_to_depts <- split(deps_region_df$departement, deps_region_df$region)
    
    cat("[load_reference_data] nb régions =", length(regions), "\n")
    cat("[load_reference_data] exemples régions =\n")
    print(utils::head(regions, 20))
    cat("[load_reference_data] exemple mapping région -> départements =\n")
    print(utils::head(region_to_depts, 5))
    
  } else {
    deps_ref$region <- NA_character_
    regions <- character(0)
    region_to_depts <- list()
  }
  
  # --- Famille ROME
  famille_path <- file.path(data_dir, "Famille.xlsx")
  familles_rome <- NULL
  
  if (file.exists(famille_path)) {
    fam <- readxl::read_excel(famille_path)
    names(fam) <- trimws(names(fam))
    
    cat("[load_reference_data] colonnes Famille.xlsx =", paste(names(fam), collapse = ", "), "\n")
    
    if (all(c("Code ROME", "Famille Rome") %in% names(fam))) {
      familles_rome <- fam %>%
        dplyr::transmute(
          code_rome = as.character(`Code ROME`),
          famille_rome = as.character(`Famille Rome`)
        ) %>%
        dplyr::filter(!is.na(.data$code_rome), !is.na(.data$famille_rome))
      
      cat("[load_reference_data] familles_rome chargé, nrow =", nrow(familles_rome), "\n")
      print(utils::head(familles_rome, 10))
    } else {
      cat("[load_reference_data] colonnes attendues absentes dans Famille.xlsx\n")
    }
  }
  
  # --- ROME détaillé
  rome_path <- file.path(data_dir, "ROME.xlsx")
  rome_ref <- NULL
  
  if (file.exists(rome_path)) {
    rome <- readxl::read_excel(rome_path)
    names(rome) <- trimws(names(rome))
    
    cat("[load_reference_data] colonnes ROME.xlsx =", paste(names(rome), collapse = ", "), "\n")
    
    if (all(c("Intituler", "Code OGR", "Merger") %in% names(rome))) {
      rome_ref <- rome %>%
        dplyr::transmute(
          intituler = as.character(Intituler),
          code_ogr = as.character(`Code OGR`),
          merger = as.character(Merger)
        )
      
      cat("[load_reference_data] rome_ref chargé, nrow =", nrow(rome_ref), "\n")
      print(utils::head(rome_ref, 10))
    } else {
      cat("[load_reference_data] colonnes attendues absentes dans ROME.xlsx\n")
    }
  }
  
  list(
    geo = deps_geo,
    deps_ref = deps_ref,
    regions = regions,
    region_to_depts = region_to_depts,
    familles_rome = familles_rome,
    rome_ref = rome_ref
  )
}
# -----------------------------------------------------------------------------
# DuckDB / JOCAS
# -----------------------------------------------------------------------------

setup_duckdb_connection <- function(dbdir = "jocas.duckdb") {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  
  endpoint <- Sys.getenv("AWS_S3_ENDPOINT")
  access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  session_token <- Sys.getenv("AWS_SESSION_TOKEN")
  region <- Sys.getenv("AWS_DEFAULT_REGION", unset = "us-east-1")
  
  endpoint <- gsub("^https?://", "", endpoint)
  endpoint <- sub("/+$", "", endpoint)
  
  DBI::dbExecute(con, "INSTALL httpfs;")
  DBI::dbExecute(con, "LOAD httpfs;")
  DBI::dbExecute(con, sprintf("SET s3_region='%s';", region))
  DBI::dbExecute(con, sprintf("SET s3_access_key_id='%s';", access_key))
  DBI::dbExecute(con, sprintf("SET s3_secret_access_key='%s';", secret_key))
  DBI::dbExecute(con, sprintf("SET s3_endpoint='%s';", endpoint))
  DBI::dbExecute(con, "SET s3_use_ssl=true;")
  DBI::dbExecute(con, "SET s3_url_style='path';")
  
  if (nzchar(session_token)) {
    DBI::dbExecute(con, sprintf("SET s3_session_token='%s';", session_token))
  }
  
  cat("[setup_duckdb_connection] création vue jocas\n")
  
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW jocas AS
    SELECT *
    FROM read_parquet(
      's3://projet-jocas-prod/diffusion/JOCAS/**/*.parquet',
      hive_partitioning = true,
      union_by_name = true,
      filename = true
    )
  ")
  
  cat("[setup_duckdb_connection] création vue jocas_clean\n")
  
  valid_deps_sql <- paste(sprintf("'%s'", VALID_DEPARTMENTS), collapse = ", ")
  
  DBI::dbExecute(con, glue("
    CREATE OR REPLACE VIEW jocas_clean AS
    WITH base AS (
      SELECT
        *,
        CASE
          WHEN location_departement IN ({valid_deps_sql})
            THEN location_departement
          WHEN regexp_matches(location_zipcode, '^97[1-6][0-9]{{2}}$')
            THEN SUBSTR(location_zipcode, 1, 3)
          WHEN regexp_matches(location_zipcode, '^[0-9]{{5}}$')
            THEN SUBSTR(location_zipcode, 1, 2)
          ELSE NULL
        END AS departement_tmp
      FROM jocas
    )
    SELECT
      *,
      CASE
        WHEN departement_tmp IN ({valid_deps_sql})
          THEN departement_tmp
        ELSE NULL
      END AS departement_clean
    FROM base
  "))
  
  cat("[setup_duckdb_connection] vues prêtes\n")
  con
}

get_choices <- function(con) {
  annees <- DBI::dbGetQuery(con, "
    SELECT DISTINCT annee
    FROM jocas_clean
    WHERE annee IS NOT NULL
    ORDER BY annee
  ")$annee
  
  contrats <- DBI::dbGetQuery(con, "
    SELECT DISTINCT COALESCE(NULLIF(contractType, ''), 'Non spécifié') AS contrat
    FROM jocas_clean
    ORDER BY contrat
  ")$contrat
  
  departements <- DBI::dbGetQuery(con, "
    SELECT DISTINCT departement_clean
    FROM jocas_clean
    WHERE departement_clean IS NOT NULL
    ORDER BY departement_clean
  ")$departement_clean
  
  cat("[get_choices] nb années =", length(annees), "\n")
  cat("[get_choices] nb contrats =", length(contrats), "\n")
  cat("[get_choices] nb départements propres =", length(departements), "\n")
  
  list(
    annees = annees,
    contrats = contrats,
    departements = departements
  )
}

build_where_clause <- function(input, refs = NULL) {
  clauses <- c()
  
  if (nzchar(input$annee %||% "")) {
    clauses <- c(clauses, sprintf("annee = %s", as.integer(input$annee)))
  }
  
  if (nzchar(input$region %||% "") && !is.null(refs)) {
    region_name <- input$region
    depts <- refs$region_to_depts[[region_name]] %||% character(0)
    
    if (length(depts) > 0) {
      dep_sql <- paste(sprintf("'%s'", gsub("'", "''", depts)), collapse = ", ")
      clauses <- c(clauses, sprintf("departement_clean IN (%s)", dep_sql))
    }
  }
  
  if (nzchar(input$departement %||% "")) {
    dep <- gsub("'", "''", input$departement)
    clauses <- c(clauses, sprintf("departement_clean = '%s'", dep))
  }
  
  if (nzchar(input$contrat %||% "")) {
    contrat <- gsub("'", "''", input$contrat)
    if (contrat == "Non spécifié") {
      clauses <- c(clauses, "(contractType IS NULL OR contractType = '')")
    } else {
      clauses <- c(clauses, sprintf("contractType = '%s'", contrat))
    }
  }
  
  if (nzchar(input$famille_rome %||% "") && !is.null(refs) && !is.null(refs$familles_rome)) {
    selected_family <- input$famille_rome
    
    codes <- refs$familles_rome |>
      dplyr::filter(famille_rome == selected_family) |>
      dplyr::pull(code_rome) |>
      unique()
    
    if (length(codes) > 0) {
      code_sql <- paste(sprintf("'%s'", gsub("'", "''", codes)), collapse = ", ")
      clauses <- c(clauses, sprintf("job_ROME_code IN (%s)", code_sql))
    }
  }

  title_query <- trimws(input$title_query %||% "")
  if (nzchar(title_query)) {
    q <- sql_escape_like(title_query)
    if (identical(input$whole_word, "true")) {
      clauses <- c(
        clauses,
        sprintf(
          "regexp_matches(lower(COALESCE(job_title, '')), '(^|[^[:alnum:]])%s([^[:alnum:]]|$)')",
          tolower(q)
        )
      )
    } else {
      clauses <- c(
        clauses,
        sprintf("lower(COALESCE(job_title, '')) LIKE '%%%s%%'", tolower(q))
      )
    }
  }
  
  desc_query <- trimws(input$desc_query %||% "")
  if (nzchar(desc_query)) {
    q <- sql_escape_like(desc_query)
    clauses <- c(
      clauses,
      sprintf("lower(COALESCE(description_full, '')) LIKE '%%%s%%'", tolower(q))
    )
  }
  
  if (length(clauses) == 0) "" else paste("WHERE", paste(clauses, collapse = " AND "))
}

sql_with_extra_condition <- function(base_select, where_clause = "", extra_condition = NULL) {
  if (!nzchar(where_clause) && is.null(extra_condition)) return(base_select)
  if (!nzchar(where_clause) && !is.null(extra_condition)) {
    return(paste(base_select, "WHERE", extra_condition))
  }
  if (nzchar(where_clause) && is.null(extra_condition)) return(paste(base_select, where_clause))
  paste(base_select, where_clause, "AND", extra_condition)
}

# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------

ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  use_busy_spinner(),
  tags$head(
    tags$script(HTML("
      window.addEventListener('load', function() {
        const saved = localStorage.getItem('darkMode');
        if (saved === null || saved === 'true') {
          document.body.classList.add('dark');
          localStorage.setItem('darkMode', 'true');
        }
      });

      Shiny.addCustomMessageHandler('toggle-dark', function(x) {
        document.body.classList.toggle('dark');
        localStorage.setItem('darkMode', document.body.classList.contains('dark'));
      });

      $(document).on('keydown', '#title_query, #desc_query', function(e) {
        if (e.key === 'Enter') {
          e.preventDefault();
          $('#search_btn').click();
        }
      });
    ")),
    tags$style(HTML("
      :root {
        --app-bg: #f6f8fc;
        --card-bg: #ffffff;
        --border: #e5e7eb;
        --primary: #3158b8;
        --primary-soft: #eef3ff;
        --text: #172033;
        --muted: #697386;
        --danger: #ef4444;
        --sidebar-bg: #ffffff;
        --input-bg: #ffffff;
        --tab-bg: transparent;
      }

      body.dark {
        --app-bg: #0f172a;
        --card-bg: #1e293b;
        --border: #334155;
        --primary: #60a5fa;
        --primary-soft: #1e3a5f;
        --text: #e2e8f0;
        --muted: #94a3b8;
        --danger: #f87171;
        --sidebar-bg: #111827;
        --input-bg: #1e293b;
        --tab-bg: transparent;
      }

      body, .container-fluid {
        background: var(--app-bg);
        color: var(--text);
      }

      .app-shell {
        display: flex;
        min-height: 100vh;
        gap: 0;
      }

      .sidebar {
        width: 290px;
        background: var(--sidebar-bg);
        border-right: 1px solid var(--border);
        display: flex;
        flex-direction: column;
        flex-shrink: 0;
      }

      .sidebar-header {
        padding: 18px 18px 12px 18px;
        font-weight: 700;
        color: var(--primary);
        border-bottom: 1px solid var(--border);
        font-size: 20px;
      }

      .sidebar-section {
        padding: 14px 16px;
        border-bottom: 1px solid var(--border);
      }

      .sidebar-section-title {
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--primary);
        margin-bottom: 10px;
      }

      .sidebar-help {
        font-size: 11px;
        color: var(--muted);
        margin-top: -2px;
        margin-bottom: 8px;
      }

      .main {
        flex: 1;
        padding: 18px 22px;
      }

      .topbar-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 16px;
        margin-bottom: 16px;
      }

      .brand-block {
        display: flex;
        align-items: center;
        gap: 10px;
        min-width: 220px;
      }

      .brand-logo {
        width: 36px;
        height: 36px;
        background: linear-gradient(135deg, #ff8f70, #3158b8);
        border-radius: 8px;
      }

      .brand-title {
        font-weight: 700;
        font-size: 28px;
        line-height: 1;
      }

      .brand-subtitle {
        font-size: 14px;
        color: var(--muted);
      }

      .search-panel {
        flex: 1;
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 12px;
      }

      .search-grid {
        display: grid;
        grid-template-columns: 1.1fr 1.1fr auto auto;
        gap: 8px;
        align-items: center;
      }

      .search-grid .form-group {
        margin-bottom: 0;
      }

      .search-subrow {
        margin-top: 8px;
      }

      .search-subrow .form-group {
        margin-bottom: 0;
      }

      .filter-summary {
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 10px 12px;
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
        margin-bottom: 14px;
      }

      .chip {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: var(--primary-soft);
        color: var(--primary);
        border-radius: 999px;
        padding: 4px 10px;
        font-size: 12px;
        font-weight: 600;
      }

      .metrics-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 16px;
      }

      .metric-card {
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 16px;
      }

      .metric-label {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: .08em;
        font-weight: 700;
        margin-bottom: 8px;
      }

      .metric-value {
        font-size: 28px;
        font-weight: 700;
        line-height: 1.1;
      }

      .card-block, .text-summary, .panel-card {
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 18px;
      }

      .card-block {
        padding: 22px;
      }

      .text-summary {
        padding: 16px 18px;
        margin-bottom: 16px;
      }

      .panel-card {
        padding: 18px;
      }

      .welcome-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 18px;
      }

      .step-item {
        display: flex;
        gap: 14px;
        margin-bottom: 18px;
      }

      .step-num {
        color: #c0cadb;
        font-weight: 700;
        min-width: 28px;
      }

      body.dark .step-num {
        color: #64748b;
      }

      .limit-item {
        color: var(--muted);
        margin-bottom: 14px;
      }

      .btn-primary-custom {
        background: var(--primary);
        color: #fff;
        border: none;
        border-radius: 12px;
        padding: 10px 14px;
        font-weight: 700;
      }

      .btn-danger-custom {
        background: #ef4444;
        color: #fff;
        border: none;
        border-radius: 12px;
        padding: 10px 14px;
        font-weight: 700;
      }

      .error-banner {
        background: #fff1f2;
        border: 1px solid #fecdd3;
        color: #b91c1c;
        border-radius: 14px;
        padding: 12px 14px;
        margin-bottom: 16px;
      }

      .tab-content {
        padding-top: 16px;
      }

      .nav-tabs {
        border-bottom: 1px solid var(--border);
      }

      .nav-tabs > li > a {
        color: var(--muted) !important;
        font-weight: 600;
        background: var(--tab-bg) !important;
      }

      .nav-tabs > li.active > a,
      .nav-tabs > li.active > a:hover,
      .nav-tabs > li.active > a:focus {
        color: var(--primary) !important;
        border: none;
        border-bottom: 2px solid var(--primary);
        background: var(--card-bg) !important;
      }
input:not([type='radio']):not([type='checkbox']),
select,
textarea,
.form-control,
.selectize-input,
.selectize-dropdown,
.dropdown-menu {
  background: var(--input-bg) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
}

      .selectize-dropdown-content .option,
      .dropdown-menu > li > a {
        background: var(--input-bg);
        color: var(--text);
      }

      .control-label,
      .radio label,
      .checkbox label {
        color: var(--text) !important;
      }

      .dataTables_wrapper,
      table.dataTable,
      table.dataTable tbody tr,
      table.dataTable thead th,
      .table {
        background: var(--card-bg) !important;
        color: var(--text) !important;
      }

      .table > thead > tr > th,
      .table > tbody > tr > td {
        border-color: var(--border) !important;
      }

      body.dark .error-banner {
        background: #3a1117;
        border-color: #7f1d1d;
        color: #fecaca;
      }

body.dark .modal-backdrop.in {
  opacity: 0.65 !important;
  background-color: #020617 !important;
}
/* modal shinybusy */
body.dark .shinybusy-modal .modal-content {
  background: #f3f4f6 !important;
  border: 1px solid #cbd5e1 !important;
  border-radius: 18px !important;
  box-shadow: 0 20px 50px rgba(0,0,0,0.45) !important;
}

body.dark .shinybusy-modal .modal-body {
  background: #f3f4f6 !important;
  text-align: center !important;
}

/* texte custom du spinner */
body.dark .shinybusy-modal .app-spinner-text {
  color: #1e3a8a !important;
  font-weight: 700 !important;
  font-size: 16px !important;
  margin-top: 12px !important;
  opacity: 1 !important;
}

/* spinner visible */
body.dark .shinybusy-modal .sk-child:before,
body.dark .shinybusy-modal .sk-circle .sk-child:before,
body.dark .shinybusy-modal .sk-fading-circle .sk-circle:before,
body.dark .shinybusy-modal .sk-three-bounce .sk-child,
body.dark .shinybusy-modal .sk-double-bounce .sk-child,
body.dark .shinybusy-modal .sk-spinner-pulse,
body.dark .shinybusy-modal .spinner-border,
body.dark .shinybusy-modal .spinner-grow {
  background-color: #64748b !important;
  border-color: #64748b !important;
}

body.dark .modal-backdrop,
body.dark .modal-backdrop.in {
  background-color: #020617 !important;
  opacity: 0.72 !important;
}

.leaflet-container {
  background: #0f172a !important;
  border-radius: 18px;
}

.leaflet-control,
.leaflet-control-attribution,
.leaflet-control-layers {
  background: #111827 !important;
  color: #e2e8f0 !important;
  border: 1px solid #334155 !important;
}

.leaflet-container a {
  color: #e2e8f0 !important;
}

/* radios Partiel / Entier */
.shiny-options-group {
  display: flex;
  align-items: center;
  gap: 14px;
}

.radio-inline,
.radio-inline label,
.radio label,
.shiny-options-group label {
  color: var(--text) !important;
  font-weight: 600 !important;
}

.radio-inline + .radio-inline {
  margin-left: 14px !important;
}

.radio-inline input[type='radio'],
.radio input[type='radio'] {
  -webkit-appearance: none;
  appearance: none;
  width: 18px;
  height: 18px;
  border: 2px solid var(--muted);
  border-radius: 50%;
  background: transparent;
  display: inline-block;
  vertical-align: middle;
  margin-right: 6px;
  margin-top: 0;
  position: relative;
  cursor: pointer;
}

.radio-inline input[type='radio']:checked,
.radio input[type='radio']:checked {
  border-color: var(--primary);
  background: radial-gradient(circle at center, var(--primary) 0 38%, transparent 42%);
}

.radio-inline input[type='radio']:focus,
.radio input[type='radio']:focus {
  outline: none;
  box-shadow: 0 0 0 3px rgba(49, 88, 184, 0.18);
}

body.dark .radio-inline input[type='radio'],
body.dark .radio input[type='radio'] {
  border-color: #64748b;
}

body.dark .radio-inline input[type='radio']:checked,
body.dark .radio input[type='radio']:checked {
  border-color: #60a5fa;
  background: radial-gradient(circle at center, #60a5fa 0 38%, transparent 42%);
}

/* placeholder inputs en mode sombre */
body.dark input::placeholder,
body.dark textarea::placeholder,
body.dark .form-control::placeholder {
  color: #94a3b8 !important;
  opacity: 1 !important;
}

body.dark input::-webkit-input-placeholder,
body.dark textarea::-webkit-input-placeholder,
body.dark .form-control::-webkit-input-placeholder {
  color: #94a3b8 !important;
}

body.dark input:-ms-input-placeholder,
body.dark textarea:-ms-input-placeholder,
body.dark .form-control:-ms-input-placeholder {
  color: #94a3b8 !important;
}

body.dark input::-ms-input-placeholder,
body.dark textarea::-ms-input-placeholder,
body.dark .form-control::-ms-input-placeholder {
  color: #94a3b8 !important;
}
    "))
  ),
  
  div(
    class = "app-shell",
    
    div(
      class = "sidebar",
      div(class = "sidebar-header", "EDEP – JOCAS Explorer"),
      
      div(
        class = "sidebar-section",
        div(class = "sidebar-section-title", "Filtres")
      ),
      
      div(
        class = "sidebar-section",
        div(class = "sidebar-section-title", "Géographie"),
        selectInput("region", "Région", choices = c("Toutes les régions" = "")),
        selectInput("departement", "Département", choices = c("Tous les départements" = ""))
      ),
      
      div(
        class = "sidebar-section",
        div(class = "sidebar-section-title", "Période"),
        selectInput("annee", "Année", choices = c("Toutes les années" = ""))
      ),
      
      div(
        class = "sidebar-section",
        div(class = "sidebar-section-title", "Type d'emploi"),
        selectInput("contrat", "Type de contrat", choices = c("Tous les contrats" = "")),
        tags$div(style = "height: 10px;"),
        div(class = "sidebar-section-title", style = "margin-bottom: 4px;", "Famille ROME"),
        div(class = "sidebar-help", "Tapez un métier pour détecter automatiquement sa famille :"),
        textInput("rome_search_text", NULL, placeholder = "Tapez un métier..."),
        div(class = "sidebar-help", "... ou sélectionnez directement une famille :"),
        selectInput("famille_rome", NULL, choices = c("Toutes les familles" = ""))
      ),
      
      div(
        class = "sidebar-section",
        actionButton("apply_filters", "Appliquer les filtres", class = "btn-primary-custom", width = "100%"),
        tags$div(style = "height:8px;"),
        actionButton("reset_filters", "Réinitialiser les filtres", class = "btn-danger-custom", width = "100%")
      )
    ),
    
    div(
      class = "main",
      
      div(
        class = "topbar-row",
        
        # div(
        #   class = "brand-block",
        #   div(class = "brand-logo"),
        #   div(
        #     div(class = "brand-title", "EDEP – JOCAS Explorer"),
        #     div(class = "brand-subtitle", "Marché du Travail")
        #   )
        # ),
        
        div(
          class = "search-panel",
          div(
            class = "search-grid",
            textInput("title_query", NULL, placeholder = "Titre du poste..."),
            textInput("desc_query", NULL, placeholder = "Mots-clés dans la description..."),
            div(
              style = "display:flex; align-items:center; padding-top:6px;",
              radioButtons(
                "whole_word", NULL,
                choices = c("Partiel" = "false", "Entier" = "true"),
                selected = "false",
                inline = TRUE
              )
            ),
            actionButton("search_btn", "Rechercher", class = "btn-primary-custom")
          ),
          div(
            class = "search-subrow",
            actionButton("clear_search", "Effacer la recherche")
          )
        ),
        
        div(
          style = "display:flex; align-items:center;",
          actionButton("toggle_dark", "🌙 Mode sombre", class = "btn-primary-custom")
        )
      ),
      
      uiOutput("error_banner"),
      uiOutput("filter_summary"),
      uiOutput("metrics_grid"),
      
      tabsetPanel(
        id = "main_tabs",
        tabPanel(
          "Accueil",
          div(
            class = "tab-content",
            div(
              class = "card-block",
              tags$div(style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em;", "Tableau de bord"),
              tags$h1("EDEP – JOCAS Explorer"),
              tags$p(
                "Ce tableau de bord vous permet d'explorer ",
                tags$strong("67 millions d'offres d'emploi"),
                " françaises collectées depuis 2019 par la DARES via le système JOCAS — ",
                tags$em("Job Offers Collection and Analysis System"),
                "."
              )
            ),
            tags$div(style = "height:18px;"),
            div(
              class = "welcome-grid",
              div(
                class = "panel-card",
                tags$div(style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;", "Mode d'emploi"),
                div(
                  class = "step-item",
                  div(class = "step-num", "01"),
                  div(
                    tags$strong("Choisissez vos filtres"),
                    tags$div(style = "color:var(--muted);", "Utilisez la barre latérale pour filtrer par région, département, année, type de contrat ou famille ROME.")
                  )
                ),
                div(
                  class = "step-item",
                  div(class = "step-num", "02"),
                  div(
                    tags$strong("Lancez l'analyse"),
                    tags$div(style = "color:var(--muted);", "Cliquez sur Appliquer les filtres. Les résultats s'affichent dans les onglets dédiés.")
                  )
                ),
                div(
                  class = "step-item",
                  div(class = "step-num", "03"),
                  div(
                    tags$strong("Explorez et exportez"),
                    tags$div(style = "color:var(--muted);", "Naviguez entre les onglets pour accéder aux statistiques, données et exports.")
                  )
                )
              ),
              div(
                class = "panel-card",
                tags$div(style = "font-size:12px; font-weight:700; color:#d9480f; text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;", "Limites à connaître"),
                div(class = "limit-item", "— Le premier calcul peut prendre 1 à 2 minutes selon la requête."),
                div(class = "limit-item", "— Les salaires ne concernent que les offres déclarant une rémunération."),
                div(class = "limit-item", "— La recherche textuelle dépend de la qualité des intitulés et descriptions."),
                div(class = "limit-item", "— Ces données ne représentent pas l'intégralité du marché du travail.")
              )
            ),
            tags$div(style = "height:18px;"),
            uiOutput("text_summary")
          )
        ),
        tabPanel(
          "Carte",
          div(
            class = "tab-content",
            div(
              class = "panel-card",
              tags$h3("Carte des offres"),
              tags$p("Répartition du volume d'offres par département."),
              leafletOutput("map_france", height = "720px")
            )
          )
        ),
        tabPanel("Statistiques", div(class = "tab-content", uiOutput("stats_tab"))),
        tabPanel("Données", div(class = "tab-content", uiOutput("data_tab"))),
        tabPanel("Export", div(class = "tab-content", uiOutput("export_tab"))),
        tabPanel(
          "À propos",
          div(
            class = "tab-content",
            
            div(
              class = "welcome-grid",
              
              div(
                class = "panel-card",
                tags$div(
                  style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                  "Projet"
                ),
                tags$h3("EDEP – JOCAS Explorer"),
                tags$p(
                  "Cette application a été développée par l'équipe ",
                  tags$strong("EDEP"),
                  " pour faciliter l’exploration, l’analyse et la valorisation des données issues du système ",
                  tags$strong("JOCAS"),
                  "."
                ),
                tags$p(
                  "L’objectif est de proposer une interface simple pour explorer les offres d’emploi, ",
                  "suivre leur répartition territoriale, analyser leur dynamique temporelle ",
                  "et documenter les principaux usages du projet."
                )
              ),
              
              div(
                class = "panel-card",
                tags$div(
                  style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                  "Source des données"
                ),
                tags$h3("Référentiel JOCAS"),
                tags$p(
                  tags$strong("JOCAS"),
                  " (Job Offers Collection and Analysis System) est un dispositif de la ",
                  tags$strong("Dares"),
                  " dédié à la collecte et à l’analyse des offres d’emploi en France."
                ),
                tags$p(
                  "L’application EDEP s’appuie sur ces données pour proposer une lecture opérationnelle ",
                  "et exploratoire du marché du travail."
                )
              )
            ),
            
            tags$div(style = "height:18px;"),
            
            div(
              class = "panel-card",
              tags$div(
                style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                "Références et liens utiles"
              ),
              
              tags$h4("Sources institutionnelles"),
              tags$ul(
                tags$li(
                  "Source institutionnelle Dares — page JOCAS : ",
                  tags$a(
                    href = "https://dares.travail-emploi.gouv.fr/enquete-source/jocas",
                    target = "_blank",
                    "Voir la page"
                  )
                ),
                tags$li(
                  "Application SSPCloud “JOCAS - Marché du Travail” : ",
                  tags$a(
                    href = "https://jocas-v3.user.lab.sspcloud.fr/",
                    target = "_blank",
                    "Accéder à l'application"
                  )
                ),
                tags$li(
                  "Dépôt GitLab SSPCloud de l’application d’analyse JOCAS : ",
                  tags$a(
                    href = "https://git.lab.sspcloud.fr/baali/v3-deploy",
                    target = "_blank",
                    "Voir le dépôt"
                  )
                )
              )
            ),
            
            tags$div(style = "height:18px;"),
            
            div(
              class = "welcome-grid",
              
              div(
                class = "panel-card",
                tags$div(
                  style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                  "Application EDEP"
                ),
                tags$h4("Dépôt de l'application"),
                tags$p(
                  "Ce bloc peut documenter le dépôt principal de développement de l’application EDEP."
                ),
                tags$ul(
                  tags$li("Nom du dépôt : à compléter"),
                  tags$li(
                    "Lien GitLab / GitHub : ",
                    tags$em("à renseigner")
                  )
                ),
                tags$p(
                  style = "color:var(--muted); margin-bottom:0;",
                  "À utiliser pour documenter le code source fonctionnel de l’application."
                )
              ),
              
              div(
                class = "panel-card",
                tags$div(
                  style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                  "Déploiement"
                ),
                tags$h4("Dépôt de déploiement"),
                tags$p(
                  "Ce bloc peut documenter le dépôt ou la chaîne de déploiement de l’application."
                ),
                tags$ul(
                  tags$li("Nom du dépôt de déploiement : à compléter"),
                  tags$li(
                    "Lien GitLab / GitHub : ",
                    tags$em("à renseigner")
                  ),
                  tags$li("Environnement cible : SSPCloud / autre environnement à préciser")
                ),
                tags$p(
                  style = "color:var(--muted); margin-bottom:0;",
                  "À utiliser pour distinguer le code applicatif du code d’infrastructure ou de mise en production."
                )
              )
            ),
            
            tags$div(style = "height:18px;"),
            
            div(
              class = "welcome-grid",
              
              div(
                class = "panel-card",
                tags$div(
                  style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                  "Méthodologie"
                ),
                tags$h4("Principes de fonctionnement"),
                tags$ul(
                  tags$li("Exploration des offres via filtres territoriaux, temporels et métier."),
                  tags$li("Recherche textuelle sur les intitulés et descriptions."),
                  tags$li("Agrégations calculées à la volée via DuckDB."),
                  tags$li("Visualisations synthétiques : indicateurs, graphiques, carte, extraction tabulaire.")
                )
              ),
              
              div(
                class = "panel-card",
                tags$div(
                  style = "font-size:12px; font-weight:700; color:#d9480f; text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                  "Limites"
                ),
                tags$h4("Points d'attention"),
                tags$ul(
                  tags$li("Les données ne couvrent pas l’intégralité du marché du travail."),
                  tags$li("La qualité de la recherche dépend des intitulés et descriptions disponibles."),
                  tags$li("Les salaires ne concernent que les offres renseignant une rémunération."),
                  tags$li("Les regroupements métiers et familles ROME reposent sur les référentiels chargés dans l’application.")
                )
              )
            ),
            
            tags$div(style = "height:18px;"),
            
            div(
              class = "panel-card",
              tags$div(
                style = "font-size:12px; font-weight:700; color:var(--primary); text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;",
                "Contacts / maintenance"
              ),
              tags$p(
                "Ce bloc peut être utilisé pour indiquer le responsable applicatif, l’équipe projet, ",
                "ou les modalités de maintenance et d’évolution."
              ),
              tags$ul(
                tags$li("Équipe : EDEP"),
                tags$li("Référent fonctionnel : à compléter"),
                tags$li("Référent technique : à compléter"),
                tags$li("Canal de suivi / ticketing : à compléter")
              )
            )
          )
        )
          
      )
    )
  )
)

# -----------------------------------------------------------------------------
# Server
# -----------------------------------------------------------------------------

server <- function(input, output, session) {
  state <- reactiveValues(
    con = NULL,
    choices = NULL,
    refs = NULL,
    error = NULL
  )
  observe({
    cat("[DEBUG whole_word] valeur =", input$whole_word, "\n")
  })
  observe({
    creds <- check_s3_credentials()
    if (!creds$ok) {
      state$error <- paste("Variables d'environnement manquantes :", paste(creds$missing, collapse = ", "))
      return()
    }
    
    if (is.null(state$con)) {
      
      show_app_spinner(session, "Chargement de JOCAS en cours...")
      
      tryCatch({
        state$con <- setup_duckdb_connection()
        state$choices <- get_choices(state$con)
        state$refs <- load_reference_data("datas")
        cat("\n[init] refs chargées\n")
        cat("[init] state$refs NULL ? ", is.null(state$refs), "\n")
        
        cat("[init] familles_rome NULL ? ", is.null(state$refs$familles_rome), "\n")
        if (!is.null(state$refs$familles_rome)) {
          cat("[init] nrow(familles_rome) =", nrow(state$refs$familles_rome), "\n")
          cat("[init] colonnes familles_rome =", paste(names(state$refs$familles_rome), collapse = ", "), "\n")
          cat("[init] exemples familles_rome =\n")
          print(utils::head(state$refs$familles_rome, 10))
        }
        
        cat("[init] rome_ref NULL ? ", is.null(state$refs$rome_ref), "\n")
        if (!is.null(state$refs$rome_ref)) {
          cat("[init] nrow(rome_ref) =", nrow(state$refs$rome_ref), "\n")
          cat("[init] colonnes rome_ref =", paste(names(state$refs$rome_ref), collapse = ", "), "\n")
          cat("[init] exemples rome_ref =\n")
          print(utils::head(state$refs$rome_ref, 10))
        }
        # cat("[rome_search_text] merger_val présent dans les choix ? ", merger_val %in% unique(state$refs$familles_rome$famille_rome), "\n")
        updateSelectInput(
          session, "annee",
          choices = c("Toutes les années" = "", state$choices$annees)
        )
        
        updateSelectInput(
          session, "contrat",
          choices = c("Tous les contrats" = "", state$choices$contrats)
        )
        
        updateSelectInput(
          session, "region",
          choices = c("Toutes les régions" = "", state$refs$regions)
        )
        
        updateSelectInput(
          session, "departement",
          choices = c("Tous les départements" = "", state$refs$departements)
        )
        
        familles_choices <- sort(unique(state$refs$familles_rome$famille_rome))
        
        updateSelectInput(
          session,
          "famille_rome",
          choices = c("Toutes les familles" = "", familles_choices),
          selected = ""
        )
        
        state$error <- NULL
      }, error = function(e) {
        state$error <- paste("Erreur lors de l'initialisation :", conditionMessage(e))
      }, finally = {
        shinybusy::remove_modal_spinner()
      })
    }
  })
  query_map <- eventReactive(search_trigger(), {
    req(state$con)
    
    where_clause <- build_where_clause(input, refs = state$refs)
    
    sql <- sql_with_extra_condition(
      base_select = "
      SELECT
        departement_clean AS departement,
        COUNT(*) AS offres
      FROM jocas_clean
    ",
      where_clause = where_clause,
      extra_condition = "departement_clean IS NOT NULL"
    )
    
    sql <- paste(sql, "GROUP BY departement_clean")
    
    cat("[query_map] SQL =\n")
    cat(sql, "\n")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  map_data <- reactive({
    req(state$refs)
    req(state$refs$geo)
    
    dep_counts <- query_map()
    
    geo_map <- state$refs$geo %>%
      dplyr::left_join(dep_counts, by = c("departement" = "departement")) %>%
      dplyr::mutate(
        offres = dplyr::coalesce(as.numeric(offres), 0),
        label_map = paste0(
          "<strong>", nom_departement, "</strong><br/>",
          "Département : ", departement, "<br/>",
          "Offres : ", format(offres, big.mark = " ", scientific = FALSE)
        )
      )
    
    geo_map
  })
  
  output$map_france <- renderLeaflet({
    geo_map <- map_data()
    req(nrow(geo_map) > 0)
    
    cat("[map] CRS =", sf::st_crs(geo_map)$input, "\n")
    print(sf::st_bbox(geo_map))
    
    bbox <- sf::st_bbox(geo_map)
    xmin <- unname(bbox["xmin"])
    ymin <- unname(bbox["ymin"])
    xmax <- unname(bbox["xmax"])
    ymax <- unname(bbox["ymax"])
    
    pal <- leaflet::colorNumeric(
      palette = "YlOrRd",
      domain = geo_map$offres,
      na.color = "#334155"
    )
    
    leaflet::leaflet(
      data = geo_map,
      options = leaflet::leafletOptions(zoomControl = TRUE)
    ) %>%
      leaflet::addProviderTiles(leaflet::providers$CartoDB.DarkMatter) %>%
      leaflet::fitBounds(
        lng1 = xmin,
        lat1 = ymin,
        lng2 = xmax,
        lat2 = ymax
      ) %>%
      leaflet::addPolygons(
        fillColor = ~pal(offres),
        fillOpacity = 0.85,
        color = "#e2e8f0",
        weight = 1,
        opacity = 1,
        smoothFactor = 0.2,
        highlightOptions = leaflet::highlightOptions(
          weight = 2,
          color = "#ffffff",
          fillOpacity = 0.95,
          bringToFront = TRUE
        ),
        label = ~lapply(label_map, htmltools::HTML)
      ) %>%
      leaflet::addLegend(
        position = "bottomright",
        pal = pal,
        values = ~offres,
        title = "Nombre d'offres",
        opacity = 0.9
      )
  })
  
  
  observeEvent(input$region, {
    req(state$refs)
    
    selected_region <- trimws(input$region %||% "")
    
    cat("\n[region change] région sélectionnée =", selected_region, "\n")
    cat("[region change] régions disponibles dans region_to_depts ? ",
        selected_region %in% names(state$refs$region_to_depts), "\n")
    
    if (!nzchar(selected_region)) {
      dept_choices <- state$choices$departements %||% character(0)
    } else {
      cat("[region change] noms des régions connues (extrait) =\n")
      print(utils::head(names(state$refs$region_to_depts), 20))
      
      dept_choices <- state$refs$region_to_depts[[selected_region]]
      if (is.null(dept_choices) || length(dept_choices) == 0) {
        dept_choices <- character(0)
      } else {
        dept_choices <- sort(unique(as.character(dept_choices)))
      }
    }
    
    cat("[region change] nb départements proposés =", length(dept_choices), "\n")
    cat("[region change] départements proposés =", paste(dept_choices, collapse = ", "), "\n")
    
    updateSelectInput(
      session,
      "departement",
      choices = c("Tous les départements" = "", dept_choices),
      selected = ""
    )
  }, ignoreInit = TRUE)
  
  observeEvent(input$rome_search_text, {
    cat("\n====================\n")
    cat("[rome_search_text] observeEvent déclenché\n")
    
    req(state$refs)
    
    txt <- trimws(input$rome_search_text %||% "")
    cat("[rome_search_text] texte saisi =", txt, "\n")
    
    if (is.null(state$refs$rome_ref)) {
      cat("[rome_search_text] ERREUR : rome_ref est NULL\n")
      return()
    }
    
    if (is.null(state$refs$familles_rome)) {
      cat("[rome_search_text] ERREUR : familles_rome est NULL\n")
      return()
    }
    
    # Si texte vide ou trop court : on restaure la liste complète
    if (!nzchar(txt) || nchar(txt) < 2) {
      cat("[rome_search_text] texte vide ou trop court, restauration liste complète\n")
      
      familles_choices <- sort(unique(state$refs$familles_rome$famille_rome))
      
      freezeReactiveValue(input, "famille_rome")
      updateSelectInput(
        session,
        "famille_rome",
        choices = c("Toutes les familles" = "", familles_choices),
        selected = ""
      )
      
      cat("[rome_search_text] liste complète restaurée\n")
      cat("====================\n")
      return()
    }
    
    hits <- state$refs$rome_ref |>
      dplyr::filter(!is.na(intituler)) |>
      dplyr::mutate(
        intitule_lower = stringr::str_to_lower(intituler),
        txt_lower = stringr::str_to_lower(txt),
        score = dplyr::case_when(
          intitule_lower == txt_lower ~ 1,
          stringr::str_detect(intitule_lower, paste0("^", txt_lower, "\\b")) ~ 2,
          stringr::str_detect(intitule_lower, paste0("\\b", txt_lower, "\\b")) ~ 3,
          stringr::str_detect(intitule_lower, stringr::fixed(txt_lower)) ~ 4,
          TRUE ~ 99
        )
      ) |>
      dplyr::filter(score < 99) |>
      dplyr::arrange(score, intituler)
    
    cat("[rome_search_text] nb hits =", nrow(hits), "\n")
    
    if (nrow(hits) > 0) {
      cat("[rome_search_text] premiers intitulés trouvés =\n")
      print(utils::head(hits[, c("intituler", "code_ogr", "merger", "score")], 10))
    }
    
    if (nrow(hits) == 0) {
      cat("[rome_search_text] aucune correspondance trouvée dans rome_ref\n")
      
      familles_choices <- sort(unique(state$refs$familles_rome$famille_rome))
      
      freezeReactiveValue(input, "famille_rome")
      updateSelectInput(
        session,
        "famille_rome",
        choices = c("Toutes les familles" = "", familles_choices),
        selected = ""
      )
      
      cat("[rome_search_text] liste complète restaurée car aucun hit\n")
      cat("====================\n")
      return()
    }
    
    merger_vals <- hits$merger |>
      unique() |>
      stats::na.omit()
    
    cat("[rome_search_text] merger_vals retenus =", paste(merger_vals, collapse = ", "), "\n")
    
    if (length(merger_vals) == 0) {
      cat("[rome_search_text] aucun merger exploitable\n")
      cat("====================\n")
      return()
    }
    
    fam_matches <- state$refs$familles_rome |>
      dplyr::filter(code_rome %in% merger_vals) |>
      dplyr::distinct(famille_rome) |>
      dplyr::arrange(famille_rome)
    
    cat("[rome_search_text] nb familles candidates =", nrow(fam_matches), "\n")
    
    if (nrow(fam_matches) == 0) {
      cat("[rome_search_text] aucune famille trouvée pour les codes détectés\n")
      cat("====================\n")
      return()
    }
    
    familles_candidates <- fam_matches$famille_rome
    famille_detectee <- familles_candidates[[1]]
    
    cat("[rome_search_text] familles candidates =", paste(familles_candidates, collapse = " | "), "\n")
    cat("[rome_search_text] famille sélectionnée =", famille_detectee, "\n")
    
    freezeReactiveValue(input, "famille_rome")
    updateSelectInput(
      session,
      "famille_rome",
      choices = c("Toutes les familles" = "", familles_candidates),
      selected = famille_detectee
    )
    
    cat("[rome_search_text] updateSelectInput avec liste réduite envoyé\n")
    cat("====================\n")
  })
  
  observeEvent(input$famille_rome, {
    cat("\n[DEBUG famille_rome] input$famille_rome = ", input$famille_rome, "\n", sep = "")
  }, ignoreInit = FALSE)
  search_trigger <- reactiveVal(0)
  
  observeEvent(input$apply_filters, {
    show_app_spinner(session, "Application des filtres en cours...")
    cat("[apply_filters] clic\n")
    search_trigger(search_trigger() + 1)
  })
  
  observeEvent(input$search_btn, {
    show_app_spinner(session, "Recherche des offres en cours...")
    cat("[search_btn] clic\n")
    search_trigger(search_trigger() + 1)
  })
  
  observeEvent(input$clear_search, {
    show_app_spinner(session, "Réinitialisation de la recherche en cours...")
    
    cat("[clear_search] clic\n")
    updateTextInput(session, "title_query", value = "")
    updateTextInput(session, "desc_query", value = "")
    updateRadioButtons(session, "whole_word", selected = "false")
    search_trigger(search_trigger() + 1)
  })
  
  observeEvent(input$reset_filters, {
    show_app_spinner(session, "Réinitialisation des filtres en cours...")
    
    cat("[reset_filters] clic\n")
    updateSelectInput(session, "region", selected = "")
    updateSelectInput(session, "departement", selected = "")
    updateSelectInput(session, "annee", selected = "")
    updateSelectInput(session, "contrat", selected = "")
    updateSelectInput(session, "famille_rome", selected = "")
    updateTextInput(session, "rome_search_text", value = "")
    updateTextInput(session, "title_query", value = "")
    updateTextInput(session, "desc_query", value = "")
    updateRadioButtons(session, "whole_word", selected = "false")
    search_trigger(search_trigger() + 1)
  })
  
  active_filters <- reactive({
    list(
      region = input$region %||% "",
      annee = input$annee %||% "",
      departement = input$departement %||% "",
      contrat = input$contrat %||% "",
      famille_rome = input$famille_rome %||% "",
      title_query = trimws(input$title_query %||% ""),
      desc_query = trimws(input$desc_query %||% ""),
      whole_word = input$whole_word %||% "false"
    )
  })
  
  query_metrics <- eventReactive(search_trigger(), {
    req(state$con)
    on.exit(shinybusy::remove_modal_spinner(), add = TRUE)
    
    where_clause <- build_where_clause(input, refs = state$refs)
    
    sql <- glue("
      SELECT
        COUNT(*) AS total_offres,
        COUNT(DISTINCT departement_clean) AS departements_couverts,
        COUNT(DISTINCT entreprise_nom) AS total_entreprises,
        AVG(
          CASE
            WHEN TRY_CAST(salary_value AS DOUBLE) IS NOT NULL
              THEN TRY_CAST(salary_value AS DOUBLE)
            ELSE NULL
          END
        ) AS salaire_moyen
      FROM jocas_clean
      {where_clause}
    ")
    
    cat('[query_metrics] SQL =\n')
    cat(sql, '\n')
    
    res <- DBI::dbGetQuery(state$con, sql)
    print(res)
    res
  }, ignoreInit = FALSE)
  
  query_contracts <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input, refs = state$refs)
    
    sql <- glue("
      SELECT
        COALESCE(NULLIF(contractType, ''), 'Non spécifié') AS type_contrat,
        COUNT(*) AS nombre
      FROM jocas_clean
      {where_clause}
      GROUP BY type_contrat
      ORDER BY nombre DESC
    ")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  query_quarters <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input, refs = state$refs)
    
    sql <- glue("
      WITH q AS (
        SELECT
          CONCAT(
            annee, '-T',
            CASE
              WHEN CAST(mois AS INTEGER) BETWEEN 1 AND 3 THEN '1'
              WHEN CAST(mois AS INTEGER) BETWEEN 4 AND 6 THEN '2'
              WHEN CAST(mois AS INTEGER) BETWEEN 7 AND 9 THEN '3'
              WHEN CAST(mois AS INTEGER) BETWEEN 10 AND 12 THEN '4'
            END
          ) AS trimestre,
          COUNT(*) AS nombre_total
        FROM jocas_clean
        {where_clause}
        GROUP BY trimestre, annee
      )
      SELECT *
      FROM q
      ORDER BY trimestre
    ")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  query_daily <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input, refs = state$refs)
    
    sql <- glue("
      SELECT
        date_firstSeenDay,
        COUNT(*) AS n
      FROM jocas_clean
      {where_clause}
      GROUP BY date_firstSeenDay
      ORDER BY date_firstSeenDay
    ")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  query_data <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input, refs = state$refs)
    
    sql <- glue("
      SELECT
        ID_JOCAS,
        site_name,
        date_firstSeenDay,
        job_title,
        contractType,
        location_label,
        departement_clean,
        annee,
        mois,
        job_ROME_code
      FROM jocas_clean
      {where_clause}
      LIMIT 1000
    ")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  output$error_banner <- renderUI({
    if (is.null(state$error)) return(NULL)
    div(class = "error-banner", tags$strong("Erreur lors du chargement"), tags$br(), state$error)
  })
  
  output$filter_summary <- renderUI({
    f <- active_filters()
    chips <- list()
    
    if (nzchar(f$region)) chips <- c(chips, list(chip(paste("Région :", f$region))))
    if (nzchar(f$annee)) chips <- c(chips, list(chip(paste("Année :", f$annee))))
    if (nzchar(f$departement)) chips <- c(chips, list(chip(paste("Département :", f$departement))))
    if (nzchar(f$contrat)) chips <- c(chips, list(chip(paste("Contrat :", f$contrat))))
    if (nzchar(f$famille_rome)) chips <- c(chips, list(chip(paste("Famille ROME :", f$famille_rome))))
    if (nzchar(f$title_query)) chips <- c(chips, list(chip(paste("Titre :", f$title_query))))
    if (nzchar(f$desc_query)) chips <- c(chips, list(chip(paste("Description :", f$desc_query))))
    
    if (length(chips) == 0) return(NULL)
    
    metrics <- tryCatch(query_metrics(), error = function(e) NULL)
    total <- if (!is.null(metrics) && nrow(metrics) > 0) fmt_n(metrics$total_offres[1]) else NULL
    
    div(
      class = "filter-summary",
      tags$span(style = "font-size:12px; color:var(--muted); font-weight:600;", "Filtres actifs :"),
      chips,
      if (!is.null(total)) tags$span(style = "margin-left:auto; font-size:12px; font-weight:700;", paste(total, "offres"))
    )
  })
  
  output$metrics_grid <- renderUI({
    m <- query_metrics()
    req(nrow(m) == 1)
    
    div(
      class = "metrics-grid",
      metric_card("Total offres", fmt_n(m$total_offres[1])),
      metric_card("Départements", fmt_n(m$departements_couverts[1])),
      metric_card("Salaire moyen", ifelse(is.na(m$salaire_moyen[1]), "N/A", paste0(fmt_n(m$salaire_moyen[1]), " €"))),
      metric_card("Entreprises", fmt_n(m$total_entreprises[1]))
    )
  })
  
  output$text_summary <- renderUI({
    m <- query_metrics()
    ctab <- tryCatch(query_contracts(), error = function(e) NULL)
    
    req(nrow(m) == 1)
    total <- m$total_offres[1]
    deps <- m$departements_couverts[1]
    ents <- m$total_entreprises[1]
    
    txt <- glue(
      "La sélection courante couvre {fmt_n(total)} offres, ",
      "{fmt_n(deps)} départements et ",
      "{fmt_n(ents)} entreprises."
    )
    
    top_contract <- NULL
    if (!is.null(ctab) && nrow(ctab) > 0) top_contract <- ctab$type_contrat[1]
    
    div(
      class = "text-summary",
      tags$div(style = "font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); margin-bottom:8px;", "Résumé de la sélection"),
      tags$p(txt),
      if (!is.null(top_contract)) tags$p(glue("Le type de contrat le plus fréquent est : {top_contract}."), style = "margin-bottom:0;")
    )
  })
  
  observeEvent(input$toggle_dark, {
    session$sendCustomMessage("toggle-dark", list())
  })
  
  # output$map_tab <- renderUI({
  #   div(
  #     class = "panel-card",
  #     tags$h3("Carte"),
  #     tags$p("Version 1 : tableau départemental filtré, prêt à être branché ensuite au GeoJSON."),
  #     DTOutput("table_map")
  #   )
  # })
  
  # output$table_map <- renderDT({
  #   req(state$con)
  #   
  #   where_clause <- build_where_clause(input, refs = state$refs)
  #   
  #   sql <- sql_with_extra_condition(
  #     base_select = "
  #       SELECT
  #         departement_clean AS departement,
  #         COUNT(*) AS offres
  #       FROM jocas_clean
  #     ",
  #     where_clause = where_clause,
  #     extra_condition = "departement_clean IS NOT NULL"
  #   )
  #   
  #   sql <- paste(sql, "GROUP BY departement_clean ORDER BY offres DESC LIMIT 100")
  #   
  #   cat("[table_map] SQL =\n")
  #   cat(sql, "\n")
  #   
  #   dep <- DBI::dbGetQuery(state$con, sql)
  #   datatable(dep, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  # })
  
  output$stats_tab <- renderUI({
    tagList(
      div(class = "panel-card", tags$h3("Évolution trimestrielle"), plotlyOutput("plot_quarters", height = "320px")),
      tags$div(style = "height:16px;"),
      div(class = "panel-card", tags$h3("Répartition des contrats"), plotlyOutput("plot_contracts", height = "420px")),
      tags$div(style = "height:16px;"),
      div(class = "panel-card", tags$h3("Volume journalier"), plotlyOutput("plot_daily", height = "320px"))
    )
  })
  
  output$plot_quarters <- renderPlotly({
    df <- query_quarters()
    validate(need(nrow(df) > 0, "Aucune donnée."))
    plot_ly(
      data = df,
      x = ~trimestre,
      y = ~nombre_total,
      type = "scatter",
      mode = "lines+markers"
    ) %>% layout(
      xaxis = list(title = "Trimestre"),
      yaxis = list(title = "Offres")
    )
  })
  
  output$plot_contracts <- renderPlotly({
    df <- query_contracts()
    validate(need(nrow(df) > 0, "Aucune donnée."))
    plot_ly(
      data = df,
      labels = ~type_contrat,
      values = ~nombre,
      type = "pie",
      textinfo = "percent+label"
    )
  })
  
  output$plot_daily <- renderPlotly({
    df <- query_daily()
    validate(need(nrow(df) > 0, "Aucune donnée."))
    plot_ly(
      data = df,
      x = ~date_firstSeenDay,
      y = ~n,
      type = "scatter",
      mode = "lines"
    ) %>% layout(
      xaxis = list(title = "Date"),
      yaxis = list(title = "Offres")
    )
  })
  
  output$data_tab <- renderUI({
    div(
      class = "panel-card",
      tags$h3("Données"),
      tags$p("Extraction limitée à 1000 lignes pour l'affichage."),
      DTOutput("table_data")
    )
  })
  
  output$table_data <- renderDT({
    df <- query_data()
    datatable(df, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  })
  
  output$export_tab <- renderUI({
    div(
      class = "panel-card",
      tags$h3("Export"),
      tags$p("Export CSV du sous-ensemble courant (limité ici à 100 000 lignes)."),
      downloadButton("download_csv", "Télécharger le CSV")
    )
  })
  
  output$download_csv <- downloadHandler(
    filename = function() {
      paste0("jocas_export_", Sys.Date(), ".csv")
    },
    content = function(file) {
      where_clause <- build_where_clause(input, refs = state$refs)
      sql <- glue("
        SELECT *
        FROM jocas_clean
        {where_clause}
        LIMIT 100000
      ")
      df <- DBI::dbGetQuery(state$con, sql)
      write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
    }
  )
  
  session$onSessionEnded(function() {
    con_to_close <- isolate(state$con)
    if (!is.null(con_to_close)) {
      try(DBI::dbDisconnect(con_to_close, shutdown = TRUE), silent = TRUE)
    }
  })
}

shinyApp(ui, server)
