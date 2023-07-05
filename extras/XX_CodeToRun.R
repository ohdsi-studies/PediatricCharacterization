# --- SETUP --------------------------------------------------------------------
#keyring::key_set_with_value("redShiftPassword", password = "Rup@IsC00l!")
#renv::deactivate()
#remove.packages("PediatricCharacterization")
#renv::purge("PediatricCharacterization")
setwd("D:/Projects/PediatricCharacterization")
install.packages("renv")
#download.file("https://raw.githubusercontent.com/ohdsi-studies/PediatricCharacterization/anaphylaxis/renv.lock", "renv.lock")
download.file("https://raw.githubusercontent.com/ohdsi-studies/PediatricCharacterization/master/renv.lock", "renv.lock")
renv::init()
renv::restore()
library(PediatricCharacterization)


# --- CHOOSE DB ----------------------------------------------------------------
databases <- read.csv("XX_databases.csv",header=TRUE)
database <- databases[1,]
#DONE:  #1,2,3

# --- RUN ----------------------------------------------------------------------
options(andromedaTempFolder = "D:/andromedaTemp")
options(sqlRenderTempEmulationSchema = NULL)

# Details for connecting to the server:
# See ?DatabaseConnector::createConnectionDetails for help
connectionDetails <-
  DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                             server = database$server, #paste0(Sys.getenv("DB_SERVER3"),"/optum_extended_ses"),
                                             user = keyring::key_get("redShiftUserName"),
                                             password = keyring::key_get("redShiftPassword"),
                                             port = 5439,
                                             extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory")

connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)

outputFolder <- database$outputFolder
cdmDatabaseSchema <- database$cdmDatabaseSchema
cohortDatabaseSchema <- database$cohortDatabaseSchema
cohortTablePrefix <- database$cohortTablePrefix
cohortTable <- database$cohortTable
databaseId <- database$databaseId
databaseName <- database$databaseName
databaseDescription <- database$databaseDescription

# --- EXECUTE ------------------------------------------------------------------
PediatricCharacterization::execute(connectionDetails = connectionDetails,
                                          outputFolder = outputFolder,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          #cohortTablePrefix = cohortTablePrefix,
                                          cohortTable = cohortTable,
                                          databaseId = databaseId,
                                          databaseName = databaseName,
                                          createCohortsAndRef = TRUE,
                                          runCohortDiagnostics = TRUE,
                                          runIR = FALSE,
                                          minCellCount = 5)

# --- SHARE RESULTS ------------------------------------------------------------
# Upload the results to the OHDSI SFTP server:
#privateKeyFileName <- "D:/keys/study-data-site-covid19aesi" #location and name of file
#userName <- "study-data-site-covid19aesi"
#PediatricCharacterization::uploadDiagnosticsResults(file.path(outputFolder,"cohortDiagnostics"), privateKeyFileName, userName)
#PediatricCharacterization::uploadStudyResults(file.path(outputFolder, "incidenceRate"), privateKeyFileName, userName)

# --- VIEW COHORT DIAGNOSTICS --------------------------------------------------
# If CohortDiagnostics has been run, you can call the RShiney viewer like this:
# outputFolder <- "D:/Projects/PediatricCharacterization/results_UCCS_RS"
# CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "cohortDiagnostics"))
# CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = file.path(outputFolder,"cohortDiagnostics"))

folder <- file.path('D:/StudyResults/2023/PediatricCharacterization/results_optumses/cohortDiagnostics')

CohortDiagnostics::createMergedResultsFile(dataFolder = folder, overwrite = TRUE,
                                           sqliteDbPath = file.path(folder,
                                                                    "MergedCohortDiagnosticsData.sqlite"
                                           )
)

CohortDiagnostics::createDiagnosticsExplorerZip(outputZipfile = file.path(folder,
                                                                          "DiagnosticsExplorer.zip"),
                                                sqliteDbPath = file.path(folder,
                                                                         "MergedCohortDiagnosticsData.sqlite"),
                                                overwrite = T)

CohortDiagnostics::launchDiagnosticsExplorer(overwritePublishDir = TRUE, makePublishable=TRUE,
                                             sqliteDbPath = file.path(folder, "MergedCohortDiagnosticsData.sqlite"))

