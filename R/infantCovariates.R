#' Create Infant Covariate Settings
#' @description
#' Covariate settings for infants to compute different age groups
#' All features are defined in aggregate only.
#'
#' @param useDemographicsInfantAgeGroup               Standard format of age groups <1 years; 0 - <2 years; 2-5 years; 6-11 years; 12-17 years
#' @param useDemographicsInfantAgeGroupNsch           Grouping used at https://datacenter.kidscount.org/ - 0-4; 5-11; 12-14; 15-17; 1-17; 3-17; 10-17
#' @param temporal
createInfantCovariateSettings <- function(useDemographicsInfantAgeGroup = TRUE,
                                          useDemographicsInfantAgeGroupNsch = TRUE,
                                          temporal = TRUE) {
  covariateSettings <- list(DemographicsInfantAgeGroup = useDemographicsInfantAgeGroup,
                            DemographicsInfantAgeGroupNsch = useDemographicsInfantAgeGroupNsch,
                            temporalSequence = FALSE,
                            temporal = temporal)
  attr(covariateSettings, "fun") <- "getDbInfantCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}


getDbInfantCovariateData <-function(connectionDetails = NULL,
                                    connection = NULL,
                                    oracleTempSchema = NULL,
                                    cdmDatabaseSchema,
                                    cdmVersion = "5",
                                    cohortTable = "cohort",
                                    cohortDatabaseSchema = cdmDatabaseSchema,
                                    cohortTableIsTemp = FALSE,
                                    cohortId = -1,
                                    rowIdField = "subject_id",
                                    covariateSettings,
                                    aggregated = FALSE) {

  if (is.null(connection) & is.null(connectionDetails))
    stop("Must specify connection")

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  ParallelLogger::logInfo("Getting infant covariate settings")

  if (!covariateSettings$DemographicsInfantAgeGroup & !covariateSettings$DemographicsInfantAgeGroupNsch)
    return(NULL)

  if (!aggregated) {
    warning("Only computaable for aggregated stats")
    return(NULL)
  }

  # Construct covariate reference:
  covariateRef <- read.csv(system.file("settings/AgeGroupCovariates.csv", package = "PedatricCharacterization"))

  # Construct analysis reference:
  analysisRef <- rbind(
    data.frame(analysisId = 10000,
               analysisName = "Infant Age Group (NSCH)",
               domainId = "Demographics",
               isBinary = "Y",
               missingMeansZero = "Y"),
    data.frame(analysisId = 20000,
               analysisName = "Infant Age Group",
               domainId = "Demographics",
               isBinary = "Y",
               missingMeansZero = "Y")
  )


  result <- Andromeda::andromeda(covariateRef = covariateRef,
                                 analysisRef = analysisRef)

  sql <- SqlRender::loadRenderTranslateSql(sqlFileName = "DemographicsInfantAge.sql",
                                           packageName = "PediatricCharacterization",
                                           dbms = DatabaseConnector::dbms(connection),
                           cohort_table = cohortTable,
                           use_infant_age_nsch = covariateSettings$DemographicsInfantAgeGroupNsch,
                           use_infant_age = covariateSettings$DemographicsInfantAgeGroup,
                           temporal = covariateSettings$temporal,
                           cdm_database_schema = cdmDatabaseSchema)

  DatabaseConnector::executeSql(connection = connection, sql = sql)

  covariates <- DatabaseConnector::renderTranslateQuerySqlToAndromeda(connection,
                                                                      "SELECT * FROM #covariate_result",
                                                                      snakeCaseToCamelCase = TRUE,
                                                                      andromeda = result,
                                                                      andromedaTableName = "covariates")
  cleanupSql <-
    "TRUNCATE TABLE #covariate_result;
  DROP TABLE #covariate_result;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection, cleanupSql)

  # Construct analysis reference:
  metaData <- list(sql = sql, call = match.call())
  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"

  return(result)
}
