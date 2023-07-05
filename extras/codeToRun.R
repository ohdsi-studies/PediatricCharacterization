# ##############################################################################
# Adverse Events of Special Interest within COVID-19 Subjects
# ##############################################################################

# --- SETUP --------------------------------------------------------------------
library(PediatricCharacterization)

options(andromedaTempFolder = "D:/andromedaTemp")
options(sqlRenderTempEmulationSchema = NULL)

userNameService = "redShiftUserName" # example: "this is key ring service that securely stores credentials"
passwordService = "redShiftPassword"
dbUp = 1


# Details for connecting to the server:
optum_extended_dod <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'optum_extended_dod')

dbList <- list(optum_extended_dod)

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = 'redshift',
  user = keyring::key_get(service = userNameService),
  password = keyring::key_get(service = passwordService),
  server = dbList[[dbUp]]$server
)

projectName = 'AESIVid'
tempTable <- paste0(projectName, "_cohort_table_", dbList[[dbUp]]$cdmDatabaseSchema) #this will hold all the cohorts for all the runs
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = tempTable)



# cdmDatabaseSchema =
# cohortDatabaseSchema =
# cohortTableNames = cohortTableNames
# baseUrl <- "https://epi.jnj.com:8443/WebAPI"
# ROhdsiWebApi::authorizeWebApi(baseUrl, "windows") # Windows
# cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl,
#                                                                cohortIds = cohortsToCD)

#declare folder where cd results will be stored
# folder <- paste("D:/StudyResults/2023/AESIVid/", projectName, sep="")
#
# #incremental folder
# incrementalFolder = file.path(folder,
#                               'incrementalFolder')
# cdm_incrementalFolder <- file.path(incrementalFolder,
#                                    dbList[[dbUp]]$cdmDatabaseSchema)



outputFolder <- "D:/StudyResults/2023/PediatricCharacterization/results"
cdmDatabaseSchema <- dbList[[dbUp]]$cdmDatabaseSchema
cohortDatabaseSchema <- paste0("scratch_", keyring::key_get(service = userNameService))
cohortTablePrefix <- "aesi"
cohortTable <- "aesi_cohort"
databaseId <- dbList[[dbUp]]$sourceKey
databaseName <- dbList[[dbUp]]$sourceName
databaseDescription  = dbList[[dbUp]]$sourceName



# --- EXECUTE ------------------------------------------------------------------
PediatricCharacterization::execute(connectionDetails = connectionDetails,
                                          outputFolder = outputFolder,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTablePrefix = cohortTablePrefix,
                                          cohortTable = cohortTable,
                                          databaseId = databaseId,
                                          databaseName = databaseName,
                                          databaseDescription = databaseDescription,
                                          createCohortsAndRef = TRUE,
                                          runCohortDiagnostics = TRUE,
                                          runIR = TRUE)
# --- SHARE RESULTS ------------------------------------------------------------
# Upload the results to the OHDSI SFTP server:
privateKeyFileName <- "<file>"
userName <- "<name>"
PediatricCharacterization::uploadDiagnosticsResults(file.path(outputFolder,"cohortDiagnostics"), privateKeyFileName, userName)
PediatricCharacterization::uploadStudyResults(file.path(outputFolder, "incidenceRate"), privateKeyFileName, userName)

# --- VIEW COHORT DIAGNOSTICS --------------------------------------------------
# If CohortDiagnostics has been run, you can call the RShiney viewer like this:
CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = file.path(outputFolder,"cohortDiagnostics"))
