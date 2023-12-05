# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of Covid19SubjectsAesiIncidenceRate
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#'
#' @export
execute <- function(connectionDetails,
                    outputFolder,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema,
                    cohortTable,
                    #cohortTablePrefix = "aesi",
                    databaseId = "Unknown",
                    databaseName = "Unknown",
                    databaseDescription = "Unknown",
                    verifyDependencies = TRUE,
                    createCohortsAndRef = TRUE,
                    runCohortDiagnostics = TRUE,
                    runIR = TRUE,
                    minCellCount = 5){

  ################################
  # Setup
  ################################
  start <- Sys.time()

  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }

  ParallelLogger::addDefaultFileLogger(file.path(outputFolder,
                                                 paste0(getThisPackageName(), "_log.txt")))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, paste0(getThisPackageName(),
                                                                             "_ErrorReportR.txt")))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)

  # Write out the system information
  ParallelLogger::logInfo(.systemInfo())

  if (verifyDependencies) {
    ParallelLogger::logInfo("Checking whether correct package versions are installed")
    verifyDependencies()
  }

  #Variables---------------------
  tempEmulationSchema <- getOption("sqlRenderTempEmulationSchema")
  minCellCount= minCellCount
  incrementalFolder = file.path(outputFolder, "incrementalFolder")

  cohorts <- cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
            settingsFileName = file.path(
                packageRoot, "inst/settings/CohortsToCreate.csv"
              ),
               jsonFolder = file.path(packageRoot, "inst/cohorts"),
               sqlFolder = file.path(packageRoot, "inst/sql/sql_server")
             )

  ################################
  # STEP 1 - Create Cohorts
  ################################
  if(createCohortsAndRef){
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo("  ---- Creating exposure and outcome cohorts ---- ")
    ParallelLogger::logInfo("**********************************************************")
    CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                        incremental = TRUE,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
                                        )
    CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                       cohortDefinitionSet = cohorts,
                                       cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
                                       incremental = TRUE,
                                       incrementalFolder = incrementalFolder)
  }

  ################################
  # STEP 2 - Run Cohort Diagnostics
  ################################
  if(runCohortDiagnostics){
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo("  ---- Running cohort diagnostics ---- ")
    ParallelLogger::logInfo("**********************************************************")
    exportFolder <- file.path(outputFolder, "cohortDiagnostics")


    # cohortsToCD <- read.csv("inst/settings/CohortsToCreate.csv")
    #
    #
    # cohorts <- data.frame(
    #   cohortId = cohortsToCD$cohortId[2],
    #   cohortName = cohortsToCD$cohortName[2],
    #   logicDescription = c("NA"),
    #   sql = c(readLines(system.file("sql/sql_server/566.sql",
    #                                 package = getThisPackageName(),
    #                                 mustWork = TRUE))),
    #   json = c(readLines(system.file("cohorts/566.json",
    #                                 package = getThisPackageName(),
    #                                 mustWork = TRUE)))
    # )



    CohortDiagnostics::executeDiagnostics(  cohortDefinitionSet = cohorts,
                                            exportFolder = exportFolder,
                                            connectionDetails = connectionDetails,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                            cohortTable = cohortTable,
                                            databaseId = databaseId,
                                            databaseName = databaseName,
                                            databaseDescription = databaseDescription,
                                            cdmVersion = 5,
                                            runInclusionStatistics = FALSE,
                                            runIncludedSourceConcepts = TRUE,
                                            runOrphanConcepts = TRUE,
                                            runVisitContext = TRUE,
                                            runBreakdownIndexEvents = TRUE,
                                            runIncidenceRate = TRUE,
                                            runTimeSeries = FALSE,
                                            runTemporalCohortCharacterization = TRUE,
                                            minCellCount = 5,
                                            incremental = TRUE,
                                            incrementalFolder = incrementalFolder)


    # CohortDiagnostics::executeDiagnostics(
    #   cohortDefinitionSet = cohortDefinitionSet,
    #   exportFolder= file.path(projectName, 'exportFolder', dbList[[dbUp]]$sourceKey),
    #   databaseId = dbList[[dbUp]]$sourceKey,
    #   connectionDetails = connectionDetails,
    #   cdmDatabaseSchema = dbList[[dbUp]]$cdmDatabaseSchema,
    #   cohortDatabaseSchema = paste0("scratch_", keyring::key_get(service = userNameService)),
    #   cohortTable = dbList[[dbUp]]$cohortTable,
    #   cohortTableNames = cohortTableNames,
    #   databaseName = dbList[[dbUp]]$sourceName,
    #   databaseDescription  = dbList[[dbUp]]$sourceName,
    #   cdmVersion = 5,
    #   runInclusionStatistics = TRUE,
    #   runIncludedSourceConcepts = TRUE,
    #   runOrphanConcepts = TRUE,
    #   runVisitContext = TRUE,
    #   runBreakdownIndexEvents = TRUE,
    #   runIncidenceRate = TRUE,
    #   runTimeSeries = FALSE,
    #   runTemporalCohortCharacterization = TRUE,
    #   minCellCount = 5,
    #   incremental = F,
    #   incrementalFolder = file.path(outFolder, #would insert cdm_incrementalFolder object here instead (next proj)
    #                                 'cdm_incrementalFolder')
    #)







  }

  ################################
  # STEP 3 - Run Incidence Rate Analysis
  ################################
  if(runIR){
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo("  ---- Running incidence rates ---- ")
    ParallelLogger::logInfo("**********************************************************")
    exportFolder <- file.path(outputFolder, "incidenceRate")
    runIR(connectionDetails = connectionDetails,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          #cohortTablePrefix = cohortTablePrefix,
          exportFolder = exportFolder,
          databaseId = databaseId,
          databaseName = databaseName,
          databaseDescription = databaseDescription,
          incremental = TRUE,
          minCellCount = minCellCount)

  }

}

renv::init()
library(Andromeda)
library(CirceR)
library(CohortDiagnostics)
library(CohortIncidence)
library(DBI)
library(DatabaseConnector)
library(FeatureExtraction)
library(OhdsiRTools)
library(OhdsiShinyModules)
library(ResultModelManager)
library(ParallelLogger)
library(R6)
library(RColorBrewer)
library(RJSONIO)
library(ROhdsiWebApi)
library(RSQLite)
library(Rcpp)
library(SqlRender)
library(askpass)
library(assertthat)
library(backports)
library(base64enc)
library(bit)
library(bit64)
library(blob)
library(bookdown)
library(cachem)
library(callr)
library(checkmate)
library(cli)
library(clipr)
library(clock)
library(codetools)
library(colorspace)
library(cpp11)
library(crayon)
library(curl)
library(zip)
library(yaml)
library(xfun)
library(withr)
library(webshot)
library(vroom)
library(viridisLite)
library(vctrs)
library(utf8)
library(urltools)
library(tzdb)
library(triebeard)
library(tinytex)
library(tidyselect)
library(tidyr)
library(tibble)
library(systemfonts)
library(sys)
library(svglite)
library(stringr)
library(stringi)
library(sodium)
library(snow)
library(selectr)
library(scales)
library(rvest)
library(rstudioapi)
library(rmarkdown)
library(rlang)
library(rJava)
library(renv)
library(remotes)
library(readr)
library(rappdirs)
library(purrr)
library(ps)
library(progress)
library(processx)
library(prettyunits)
library(plogr)
library(pkgconfig)
library(pillar)
library(openxlsx)
library(openssl)
library(munsell)
library(mime)
library(memoise)
library(markdown)
library(magrittr)
library(lubridate)
library(lifecycle)
library(labeling)
library(knitr)
library(keyring)
library(kableExtra)
library(jsonlite)
library(jquerylib)
library(httr)
library(htmltools)
library(hms)
library(highr)
library(glue)
library(generics)
library(formatR)
library(filelock)
library(fastmap)
library(farver)
library(fansi)
library(evaluate)
library(ellipsis)
library(dplyr)
library(digest)
library(dbplyr)
library(PediatricCharacterization)








































































































































































































































































































































































































