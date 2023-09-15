library(magrittr)

userNameService = "redShiftUserName" # example: "this is key ring service that securely stores credentials"
passwordService = "redShiftPassword"

httr::set_config(httr::config(ssl_verifypeer = FALSE))

ROhdsiWebApi::authorizeWebApi(Sys.getenv("baseUrl"), "windows") # Windows authentication

cdmSources <-
  ROhdsiWebApi::getCdmSources(baseUrl = Sys.getenv("baseUrl")) %>%
  dplyr::filter(!stringr::str_detect(string = .data$sourceKey, pattern = 'CEM_|VOCABULARY')) %>%
  dplyr::mutate(
    baseUrl = Sys.getenv("baseUrl"),
    dbms = 'redshift',
    sourceDialect = 'redshift',
    port = 5439,
    database = .data$sourceKey %>% substr(., 5, nchar(.) - 6),
    version = .data$sourceKey %>% substr(., nchar(.) - 3, nchar(.)) %>% readr::parse_number() %>% suppressWarnings()
  ) %>%
  dplyr::mutate(
    database = stringr::str_replace(
      string = .data$database,
      pattern = stringr::fixed('_cdm_health_verity_vaccine'),
      replacement = 'health_verity'
    )
  ) %>%
  dplyr::mutate(databaseRHealth = .data$database) %>%
  dplyr::mutate(
    databaseRHealth = stringr::str_replace(
      string = .data$databaseRHealth,
      pattern = stringr::fixed('truven_ccae'),
      replacement = 'ibm'
    )
  ) %>%
  dplyr::group_by(.data$database) %>%
  dplyr::arrange(dplyr::desc(.data$version)) %>%
  dplyr::mutate(sequence = dplyr::row_number()) %>%
  dplyr::ungroup() %>%
  dplyr::arrange(.data$database, .data$sequence) %>%
  dplyr::mutate(server = tolower(paste0(
    Sys.getenv("serverRoot"), "/", .data$database
  ))) %>%
  dplyr::mutate(
    serverRHealth = dplyr::case_when(
      stringr::str_detect(string = tolower(.data$database),
                          pattern = 'mdcr|mdcd') ~
        paste0(
          'rhealth-prod-1.cldcoxyrkflo.us-east-1.redshift.amazonaws.com',
          "/",
          .data$databaseRHealth
        ),
      stringr::str_detect(string = tolower(.data$database),
                          pattern = 'germany|france|australia') ~
        paste0(
          'rhealth-prod-2.cldcoxyrkflo.us-east-1.redshift.amazonaws.com',
          "/",
          .data$databaseRHealth
        ),
      stringr::str_detect(string = tolower(.data$database),
                          pattern = 'dod|ses') ~
        paste0(
          'rhealth-prod-3.cldcoxyrkflo.us-east-1.redshift.amazonaws.com',
          "/",
          .data$databaseRHealth
        ),
      stringr::str_detect(string = tolower(.data$database),
                          pattern = 'pharmetrics|jmdc|cprd|ccae') ~
        paste0(
          'rhealth-prod-4.cldcoxyrkflo.us-east-1.redshift.amazonaws.com',
          "/",
          .data$databaseRHealth
        ),
      stringr::str_detect(string = tolower(.data$database),
                          pattern = 'panther|optum_ehr') ~
        paste0(
          'rhealth-prod-5.cldcoxyrkflo.us-east-1.redshift.amazonaws.com',
          "/",
          .data$databaseRHealth
        ),
      TRUE ~ ''
    )
  ) %>%
  dplyr::mutate(cdmDatabaseSchemaRhealth = 'cdm',
                vocabDatabaseSchemaRhealth = 'cdm') %>%
  dplyr::mutate(cohortDatabaseSchema = paste0("scratch_", keyring::key_get(service = userNameService))) %>%
  dplyr::mutate(cohortDatabaseSchemaRHealth = paste0('scratch_r_', .data$database))



