
packages <- c("DBI", "duckdb", "shiny", "bslib", "dplyr", "plotly","DT", "glue", "htmltools","shinybusy")

for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}

cat("Packages chargés :", paste(packages, collapse = ", "), "\n")

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || x == "") y else x

check_s3_credentials <- function() {
  required_vars <- c("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  missing_vars <- required_vars[!nzchar(Sys.getenv(required_vars))]
  list(ok = length(missing_vars) == 0, missing = missing_vars)
}

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
  
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW jocas_clean AS
    WITH base AS (
      SELECT
        *,
        CASE
          WHEN location_departement IN (
            '01','02','03','04','05','06','07','08','09',
            '10','11','12','13','14','15','16','17','18','19',
            '2A','2B',
            '21','22','23','24','25','26','27','28','29',
            '30','31','32','33','34','35','36','37','38','39',
            '40','41','42','43','44','45','46','47','48','49',
            '50','51','52','53','54','55','56','57','58','59',
            '60','61','62','63','64','65','66','67','68','69',
            '70','71','72','73','74','75','76','77','78','79',
            '80','81','82','83','84','85','86','87','88','89',
            '90','91','92','93','94','95','971','972','973','974','975','976'
          )
            THEN location_departement
          
          WHEN regexp_matches(location_zipcode, '^97[1-6][0-9]{2}$')
            THEN SUBSTR(location_zipcode, 1, 3)
          
          WHEN regexp_matches(location_zipcode, '^[0-9]{5}$')
            THEN SUBSTR(location_zipcode, 1, 2)
          
          ELSE NULL
        END AS departement_tmp
      FROM jocas
    )
    SELECT
      *,
      CASE
        WHEN departement_tmp IN (
          '01','02','03','04','05','06','07','08','09',
          '10','11','12','13','14','15','16','17','18','19',
          '2A','2B',
          '21','22','23','24','25','26','27','28','29',
          '30','31','32','33','34','35','36','37','38','39',
          '40','41','42','43','44','45','46','47','48','49',
          '50','51','52','53','54','55','56','57','58','59',
          '60','61','62','63','64','65','66','67','68','69',
          '70','71','72','73','74','75','76','77','78','79',
          '80','81','82','83','84','85','86','87','88','89',
          '90','91','92','93','94','95','971','972','973','974','975','976'
        )
          THEN departement_tmp
        ELSE NULL
      END AS departement_clean
    FROM base
  ")
  
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
  print(departements)
  
  list(
    annees = annees,
    contrats = contrats,
    departements = departements
  )
}


sql_escape_like <- function(x) {
  x <- gsub("'", "''", x, fixed = TRUE)
  x
}

build_where_clause <- function(input) {
  clauses <- c()
  
  if (nzchar(input$annee %||% "")) {
    clauses <- c(clauses, sprintf("annee = %s", as.integer(input$annee)))
  }
  
  if (nzchar(input$departement %||% "")) {
    dep <- gsub("'", "''", input$departement)
    clauses <- c(clauses, sprintf("departement_clean = '%s'", dep))
  }
  
  if (nzchar(input$contrat %||% "")) {
    contrat <- gsub("'", "''", input$contrat)
    if (contrat == "Non spécifié") {
      clauses <- c(
        clauses,
        "(contractType IS NULL OR contractType = '')"
      )
    } else {
      clauses <- c(clauses, sprintf("contractType = '%s'", contrat))
    }
  }
  
  title_query <- trimws(input$title_query %||% "")
  if (nzchar(title_query)) {
    q <- sql_escape_like(title_query)
    if (identical(input$whole_word, "true")) {
      clauses <- c(
        clauses,
        sprintf("regexp_matches(lower(COALESCE(job_title, '')), '(^|[^[:alnum:]])%s([^[:alnum:]]|$)')", tolower(q))
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
  
  if (length(clauses) == 0) {
    ""
  } else {
    paste("WHERE", paste(clauses, collapse = " AND "))
  }
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

ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  use_busy_spinner(),
  tags$head(
    
    tags$script(HTML("
  $(document).on('keydown', '#title_query, #desc_query', function(e) {
    if (e.key === 'Enter') {
      e.preventDefault();
      $('#search_btn').click();
    }
  });
")),
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
"))
    ,
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
input, select, textarea {
  background: var(--input-bg) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
}

.form-control, .selectize-input, .selectize-dropdown, .dropdown-menu {
  background: var(--input-bg) !important;
  color: var(--text) !important;
  border-color: var(--border) !important;
}

.selectize-dropdown-content .option {
  background: var(--input-bg);
  color: var(--text);
}

.nav-tabs > li > a {
  color: var(--muted) !important;
  background: var(--tab-bg) !important;
}

.nav-tabs > li.active > a,
.nav-tabs > li.active > a:hover,
.nav-tabs > li.active > a:focus {
  color: var(--primary) !important;
  background: var(--card-bg) !important;
  border-color: var(--border) !important;
}

.dataTables_wrapper,
table.dataTable,
table.dataTable tbody tr,
table.dataTable thead th {
  background: var(--card-bg) !important;
  color: var(--text) !important;
}

.shiny-input-radiogroup label,
.control-label {
  color: var(--text) !important;
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

      .card-block {
  background: var(--card-bg);
  border: 1px solid var(--border);
        border-radius: 18px;
        padding: 22px;
      }

      .text-summary {
        background: var(--sidebar-bg);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 16px 18px;
        margin-bottom: 16px;
      }

      .panel-card {
        background: var(--sidebar-bg);
        border: 1px solid var(--border);
        border-radius: 18px;
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
        color: var(--muted);
        font-weight: 600;
      }

      .nav-tabs > li.active > a,
      .nav-tabs > li.active > a:hover,
      .nav-tabs > li.active > a:focus {
        color: var(--primary);
        border: none;
        border-bottom: 2px solid var(--primary);
        background: transparent;
      }

      @media (max-width: 1100px) {
        .metrics-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .welcome-grid { grid-template-columns: 1fr; }
        .search-grid { grid-template-columns: 1fr; }
        .topbar-row { flex-direction: column; }
      }

      @media (max-width: 900px) {
        .app-shell { flex-direction: column; }
        .sidebar { width: 100%; border-right: none; border-bottom: 1px solid var(--border); }
      }
    "))
  ),
  
  div(
    class = "app-shell",
    
    div(
      class = "sidebar",
      div(class = "sidebar-header", "JOCAS DataViz"),
      
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
        selectInput("contrat", "Type de contrat", choices = c("Tous les contrats" = ""))
      ),
      
      div(
        class = "sidebar-section",
        actionButton("apply_filters", "Appliquer les filtres", class = "btn-primary-custom", width = "100%"),
        tags$div(style = "height:8px;"),
        actionButton("reset_filters", "Réinitialiser", class = "btn-danger-custom", width = "100%")
      )
    ),
    
    div(
      class = "main",
      
      div(
        class = "topbar-row",
        
        div(
          class = "brand-block",
          div(class = "brand-logo"),
          div(
            div(class = "brand-title", "JOCAS DataViz"),
            div(class = "brand-subtitle", "Marché du Travail")
          )
        ),
        
        div(
          class = "search-panel",
          div(
            class = "search-grid",
            textInput("title_query", NULL, placeholder = "Titre du poste..."),
            textInput("desc_query", NULL, placeholder = "Mots-clés dans la description..."),
            radioButtons(
              "whole_word", NULL,
              choices = c("Partiel" = "false", "Entier" = "true"),
              selected = "false",
              inline = TRUE
            ),
            actionButton("search_btn", "Rechercher", class = "btn-primary-custom")
          ),
          div(
            class = "search-subrow",
            actionButton("clear_search", "Effacer la recherche"),
            actionButton("toggle_dark", "🌙", class = "btn-primary-custom")
          )
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
              tags$div(style = "font-size:12px; font-weight:700; color:#3158b8; text-transform:uppercase; letter-spacing:.08em;", "Tableau de bord"),
              tags$h1("JOCAS DataViz"),
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
                tags$div(style = "font-size:12px; font-weight:700; color:#3158b8; text-transform:uppercase; letter-spacing:.08em; margin-bottom:18px;", "Mode d'emploi"),
                div(
                  class = "step-item",
                  div(class = "step-num", "01"),
                  div(
                    tags$strong("Choisissez vos filtres"),
                    tags$div(style = "color:#697386;", "Utilisez la barre latérale pour filtrer par année, département ou type de contrat.")
                  )
                ),
                div(
                  class = "step-item",
                  div(class = "step-num", "02"),
                  div(
                    tags$strong("Lancez l'analyse"),
                    tags$div(style = "color:#697386;", "Cliquez sur Appliquer les filtres. Les résultats s'affichent dans les onglets dédiés.")
                  )
                ),
                div(
                  class = "step-item",
                  div(class = "step-num", "03"),
                  div(
                    tags$strong("Explorez et exportez"),
                    tags$div(style = "color:#697386;", "Naviguez entre les onglets pour accéder aux statistiques, données et exports.")
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
        tabPanel("Carte", div(class = "tab-content", uiOutput("map_tab"))),
        tabPanel("Statistiques", div(class = "tab-content", uiOutput("stats_tab"))),
        tabPanel("Données", div(class = "tab-content", uiOutput("data_tab"))),
        tabPanel("Export", div(class = "tab-content", uiOutput("export_tab")))
      )
    )
  )
)

server <- function(input, output, session) {
  state <- reactiveValues(
    con = NULL,
    choices = NULL,
    error = NULL
  )
  
  
  observe({
    creds <- check_s3_credentials()
    if (!creds$ok) {
      state$error <- paste("Variables d'environnement manquantes :", paste(creds$missing, collapse = ", "))
      return()
    }
    
    if (is.null(state$con)) {
      shiny::withReactiveDomain(session, {
        shinybusy::show_modal_spinner(
          text = "Chargement de JOCAS en cours..."
        )
      })
      
      tryCatch({
        state$con <- setup_duckdb_connection()
        state$choices <- get_choices(state$con)
        
        updateSelectInput(
          session, "annee",
          choices = c("Toutes les années" = "", state$choices$annees)
        )
        
        updateSelectInput(
          session, "contrat",
          choices = c("Tous les contrats" = "", state$choices$contrats)
        )
        
        updateSelectInput(
          session, "departement",
          choices = c("Tous les départements" = "", state$choices$departements)
        )
        
        state$error <- NULL
      }, error = function(e) {
        state$error <- paste("Erreur lors de l'initialisation :", conditionMessage(e))
      }, finally = {
        shinybusy::remove_modal_spinner()
      })
    }
  })
  
  search_trigger <- reactiveVal(0)
  

  observeEvent(input$search_btn, {
    shiny::withReactiveDomain(session, {
      shinybusy::show_modal_spinner(
        text = "Recherche textuelle dans les offres en cours..."
      )
    })
    cat("[search_btn] clic\n")
    search_trigger(search_trigger() + 1)
  })
  
  observeEvent(input$apply_filters, {
    shiny::withReactiveDomain(session, {
      shinybusy::show_modal_spinner(
        text = "Application des filtres en cours..."
      )
    })
    cat("[apply_filters] clic\n")
    search_trigger(search_trigger() + 1)
  })
  
  observeEvent(input$clear_search, {
    shiny::withReactiveDomain(session, {
      shinybusy::show_modal_spinner(
        text = "Réinitialisation de la recherche..."
      )
    })
    cat("[clear_search] clic\n")
    updateTextInput(session, "title_query", value = "")
    updateTextInput(session, "desc_query", value = "")
    updateRadioButtons(session, "whole_word", selected = "false")
    search_trigger(search_trigger() + 1)
  })
  
  observeEvent(input$reset_filters, {
    shiny::withReactiveDomain(session, {
      shinybusy::show_modal_spinner(
        text = "Réinitialisation des filtres..."
      )
    })
    cat("[reset_filters] clic\n")
    updateSelectInput(session, "region", selected = "")
    updateSelectInput(session, "departement", selected = "")
    updateSelectInput(session, "annee", selected = "")
    updateSelectInput(session, "contrat", selected = "")
    updateTextInput(session, "title_query", value = "")
    updateTextInput(session, "desc_query", value = "")
    updateRadioButtons(session, "whole_word", selected = "false")
    search_trigger(search_trigger() + 1)
  })


  
  active_filters <- reactive({
    list(
      annee = input$annee %||% "",
      departement = input$departement %||% "",
      contrat = input$contrat %||% "",
      title_query = trimws(input$title_query %||% ""),
      desc_query = trimws(input$desc_query %||% ""),
      whole_word = input$whole_word %||% "false"
    )
  })
  

  query_metrics <- eventReactive(search_trigger(), {
    req(state$con)
    
    shiny::withReactiveDomain(session, {
      shinybusy::show_modal_spinner(
        text = "Calcul des indicateurs JOCAS en cours..."
      )
    })
    
    where_clause <- build_where_clause(input)
    
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
    
    shinybusy::remove_modal_spinner()
    
    res
  }, ignoreInit = FALSE)
  
  query_contracts <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input)
    
    sql <- glue("
      SELECT
        COALESCE(NULLIF(contractType, ''), 'Non spécifié') AS type_contrat,
        COUNT(*) AS nombre
      FROM jocas
      {where_clause}
      GROUP BY type_contrat
      ORDER BY nombre DESC
    ")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  query_quarters <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input)
    
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
        FROM jocas
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
    where_clause <- build_where_clause(input)
    
    sql <- glue("
      SELECT
        date_firstSeenDay,
        COUNT(*) AS n
      FROM jocas
      {where_clause}
      GROUP BY date_firstSeenDay
      ORDER BY date_firstSeenDay
    ")
    
    DBI::dbGetQuery(state$con, sql)
  }, ignoreInit = FALSE)
  
  query_data <- eventReactive(search_trigger(), {
    req(state$con)
    where_clause <- build_where_clause(input)
    
    sql <- glue("
      SELECT
        ID_JOCAS,
        site_name,
        date_firstSeenDay,
        job_title,
        contractType,
        location_label,
        location_departement,
        annee,
        mois
      FROM jocas
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
    
    if (nzchar(f$annee)) chips <- c(chips, list(chip(paste("Année :", f$annee))))
    if (nzchar(f$departement)) chips <- c(chips, list(chip(paste("Département :", f$departement))))
    if (nzchar(f$contrat)) chips <- c(chips, list(chip(paste("Contrat :", f$contrat))))
    if (nzchar(f$title_query)) chips <- c(chips, list(chip(paste("Titre :", f$title_query))))
    if (nzchar(f$desc_query)) chips <- c(chips, list(chip(paste("Description :", f$desc_query))))
    
    if (length(chips) == 0) return(NULL)
    
    metrics <- tryCatch(query_metrics(), error = function(e) NULL)
    total <- if (!is.null(metrics) && nrow(metrics) > 0) fmt_n(metrics$total_offres[1]) else NULL
    
    div(
      class = "filter-summary",
      tags$span(style = "font-size:12px; color:#697386; font-weight:600;", "Filtres actifs :"),
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
    if (!is.null(ctab) && nrow(ctab) > 0) {
      top_contract <- ctab$type_contrat[1]
    }
    
    div(
      class = "text-summary",
      tags$div(style = "font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.08em; color:#697386; margin-bottom:8px;", "Résumé de la sélection"),
      tags$p(txt),
      if (!is.null(top_contract)) tags$p(glue("Le type de contrat le plus fréquent est : {top_contract}."), style = "margin-bottom:0;")
    )
  })
  observeEvent(input$toggle_dark, {
    session$sendCustomMessage("toggle-dark", list())
  })
  output$map_tab <- renderUI({
    dep <- tryCatch(
      DBI::dbGetQuery(state$con, glue("
        SELECT
          location_departement,
          COUNT(*) AS n
        FROM jocas
        {build_where_clause(input)}
        AND location_departement IS NOT NULL
        GROUP BY location_departement
        ORDER BY n DESC
        LIMIT 30
      ")),
      error = function(e) NULL
    )
    
    div(
      class = "panel-card",
      tags$h3("Carte"),
      tags$p("V1 : tableau départemental en attendant une vraie carte."),
      if (!is.null(dep)) DTOutput("table_map")
    )
  })
  
  output$table_map <- renderDT({
    dep <- DBI::dbGetQuery(state$con, glue("
      SELECT
        location_departement AS departement,
        COUNT(*) AS offres
      FROM jocas
      {build_where_clause(input)}
      AND location_departement IS NOT NULL
      GROUP BY location_departement
      ORDER BY offres DESC
      LIMIT 100
    "))
    datatable(dep, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  })
  
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
      where_clause <- build_where_clause(input)
      sql <- glue("
        SELECT *
        FROM jocas
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