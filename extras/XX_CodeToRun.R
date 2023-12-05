# --- SETUP --------------------------------------------------------------------
#keyring::key_set_with_value("redShiftPassword", password = "Rup@IsC00l!")
#renv::deactivate()
#remove.packages("PediatricCharacterization")
#renv::purge("PediatricCharacterization")
#setwd("D:/StudyResults/2023/PediatricCharacterization")
install.packages("renv")
#download.file("https://raw.githubusercontent.com/ohdsi-studies/PediatricCharacterization/anaphylaxis/renv.lock", "renv.lock")
download.file("https://raw.githubusercontent.com/ohdsi-studies/PediatricCharacterization/master/renv.lock", "renv.lock")
renv::init()
installed.packages() #gives list of all packages installed but do I really need all these n=171?

renv::restore()
library(PediatricCharacterization)


# --- CHOOSE DB ----------------------------------------------------------------

# outputFolder <- getwd() #D:\Git\2023\PediatricCharacterization\inst\settings
packageRoot <- getwd()
projectName = 'PediatricCharacterization'
databases <- read.csv(file.path(packageRoot, "XX_databases.csv"))
# CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "cohortDiagnostics"))
#databases <- read.csv("XX_databases.csv",header=TRUE)
database <- databases[3,] #may wish to change this
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

 #connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)

outputFolder <- database$outputFolder
cdmDatabaseSchema <- database$cdmDatabaseSchema
cohortDatabaseSchema <- database$cohortDatabaseSchema
#cohortTablePrefix <- database$cohortTablePrefix
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
                                          runIR = TRUE,
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

folder <- file.path('D:/StudyResults/2023/PediatricCharacterization/results_ccae/cohortDiagnostics')

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

CohortDiagnostics::launchDiagnosticsExplorer(overwritePublishDir = TRUE, makePublishable=FALSE,
                                             sqliteDbPath = file.path(folder, "MergedCohortDiagnosticsData.sqlite"))


#THIS CAN BE DELETED BC IT IS LOCATED IN CREATEJSONSPEC.R
###############################################################################################################################
#----PULL BACK cohortIds and use cohortgenerator to save them as jsons etc------------------------------------------------
###############################################################################################################################
#ADHD	10640; type 2 diabetes mellitus	10647; urinary tract infections	12396; anaphylaxis 10659; migraine	12468
#epilepsy	12403; Crohn’s disease	10616; ulcerative colitis	10606; chronic lymphocytic leukemia	10642; autism spectrum disorder	3417
#major depressive disorder	10628; Multiple Sclerosis	10641	psoriasis	10626	(plaque)

# cohorts <- c(10640, 10647, 10616, 10647, 12396, 10659, 12468, 12403, 10616, 10606, 10642, 3417, 10628, 10641, 10626, 8334)
# cohorts <- c(10606, 3417, 8334)
#
# baseUrl <- "https://epi.jnj.com:8443/WebAPI"
# ROhdsiWebApi::authorizeWebApi(baseUrl, "windows") # Windows
# cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl,cohortIds = cohorts)
#
#
#
# #save off cohorts
#
# CohortGenerator::saveCohortDefinitionSet(
#   cohortDefinitionSet = cohortDefinitionSet,
#   settingsFileName = file.path(
#     packageRoot,
#     "inst/settings/CohortsToCreate.csv"
#   ),
#   jsonFolder = file.path(
#     packageRoot,
#     "inst/cohorts"
#   ),
#   sqlFolder = file.path(
#     packageRoot,
#     "inst/sql/sql_server"
#   )
# )
#
# #for DPs use this
# cohortDefinitionSet <- getCohortDefinitionSet(
#   settingsFileName = file.path(
#     packageRoot, "inst/settings/CohortsToCreate.csv"
#   ),
#   jsonFolder = file.path(packageRoot, "inst/cohorts"),
#   sqlFolder = file.path(packageRoot, "inst/sql/sql_server")
# )
#
#
#
# cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()
#
# # Fill the cohort set using  cohorts included in this
# # package as an example
# cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
# for (i in 1:length(cohortJsonFiles)) {
#   cohortJsonFileName <- cohortJsonFiles[i]
#   cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
#   # Here we read in the JSON in order to create the SQL
#   # using [CirceR](https://ohdsi.github.io/CirceR/)
#   # If you have your JSON and SQL stored differenly, you can
#   # modify this to read your JSON/SQL files however you require
#   cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
#   cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
#   cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
#   cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = i,
#                                                        cohortName = cohortName,
#                                                        sql = cohortSql,
#                                                        stringsAsFactors = FALSE))
# }
#
#


