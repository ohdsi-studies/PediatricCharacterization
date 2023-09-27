library("CohortIncidence")
packageRoot <- file.path('D:/Git/2023/PediatricCharacterization') #dps need to revise or make working directory

# build the design
# alternatively, could read a json via: readr::read_file("irDesign.json")


#for DPs use this
cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
  settingsFileName = file.path(
    packageRoot, "inst/settings/CohortsToCreate.csv"
  ),
  jsonFolder = file.path(packageRoot, "inst/cohorts"),
  sqlFolder = file.path(packageRoot, "inst/sql/sql_server")
)



cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using  cohorts included in this
# package as an example
# cohortJsonFiles <- list.files(path = system.file(file.path(packageRoot, "inst/cohorts"),
#                                                 package = "CohortGenerator"), full.names = TRUE)

cohortJsonFiles <- list.files(path = file.path(packageRoot, "inst/cohorts"),
                                                 full.names = TRUE)



for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  # Here we read in the JSON in order to create the SQL
  # using [CirceR](https://ohdsi.github.io/CirceR/)
  # If you have your JSON and SQL stored differenly, you can
  # modify this to read your JSON/SQL files however you require
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
  cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = cohortName, #changed from i
                                                       cohortName = cohortName,
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))
}

#all persons at risk 2016-2022
targets <- list(CohortIncidence::createCohortRef(id=8334, name="At risk, 2016+"));


#
outcomes <- list(CohortIncidence::createOutcomeDef(id=1,name="Autism", cohortId=3417, cleanWindow=9999),
             CohortIncidence::createOutcomeDef(id=2,name="Ulcerative Colitis", cohortId=10606, cleanWindow=9999)
                 # CohortIncidence::createOutcomeDef(id=1,name="Crohn's Disease", cohortId=10616, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Plaque Psoriasis", cohortId=10626, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Major Depressive Disorder", cohortId=10628, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Attention deficit hyperactivity disorder", cohortId=10640, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Multiple sclerosis", cohortId=10641, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Chronic lymphocytic leukemia", cohortId=10642, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Type 2 diabetes mellitus", cohortId=10647, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Anaphylaxis non envirnomental", cohortId=10659, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Urinary tract infection", cohortId=12396, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Epilepsy", cohortId=12403, cleanWindow=9999),
                 # CohortIncidence::createOutcomeDef(id=1,name="Migraine", cohortId=12468, cleanWindow=9999)
             # atopic dermatitis, neurofibromatosis, Otitis media, attention deficit hyperactivity disorder, type 1 diabetes mellitus,
             # type 2 diabetes mellitus, urinary tract infections, anaphylaxis, motor vehicle accidents, asthma, migraine, epilepsy,
             # Crohn’s disease, ulcerative colitis, chronic lymphocytic leukemia, skin burns, autism spectrum disorder,
             # generalized anxiety disorder, major depressive disorder, neuroblastoma, down syndrome, cystic fibrosis,
             # atopic dermatitis AND other phenotypes that collaborators deem of interest. 


);


tars <- list(CohortIncidence::createTimeAtRiskDef(id=1, startWith="start", endWith="start", endOffset = 9999)
);

# Note: c() is used when dealing with an array of numbers,
# later we use list() when dealing with an array of objects
analysis1 <- CohortIncidence::createIncidenceAnalysis(targets = sapply(targets, function(t) { return(t$id);}),
                                                      outcomes = sapply(outcomes, function(o) { return(o$id);}),
                                                      tars = sapply(tars, function(t) { return(t$id);}));



irDesign <- CohortIncidence::createIncidenceDesign(targetDefs = targets,
                                                   outcomeDefs = outcomes,
                                                   tars=tars,
                                                   analysisList = list(analysis1),
                                                   strataSettings = CohortIncidence::createStrataSettings(byYear=T,
                                                                                                          byGender=T,
                                                                                                          byAge = T,
                                                                                            ageBreaks = c(2,5,12,18,34,65))
);

irDesign$asJSON(pretty = T)

#save design
readr::write_file(jsonlite::prettify(irDesign$asJSON(), indent=2), "irDesign.json", append = F)

#this would go below into the loop that the DPs would run - above is a one time
#everything above this line is in createJsonSpec.R file

irDesign2 <-readr::read_file(file=file.path(packageRoot, 'irDesign.json') )
test <- IncidenceDesign$new(data=irDesign2)


analysisSql <- CohortIncidence::buildQuery(incidenceDesign =  as.character(irDesign2),
                                                        buildOptions = CohortIncidence::buildOptions())
cat(analysisSql)


#
# userNameService = "redShiftUserName" # example: "this is key ring service that securely stores credentials"
# passwordService = "redShiftPassword"
source(file.path(packageRoot, "extras/buildCDMSources.R"))

#declare project name
projectName <- "PediatricCharacterization"

#declare project-specific cohort ids
# cohortsToIR <- c(8334, 10606, 3417)


#declare folder where cd results will be stored
folder <- paste("D:/StudyResults/2023/", projectName, sep="") #tell them to use whatever path they want

#source(authorize.R)

#incremental folder
incrementalFolder = file.path(folder,
                              'incrementalFolder')

# connection details ----
truven_mdcr <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_mdcr')

truven_mdcd <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_mdcd')

optum_extended_dod <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'optum_extended_dod')

optum_extended_ses <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'optum_extended_ses')

optum_ehr <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'optum_ehr')

truven_ccae <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_ccae')

ims_australia_lpd <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'ims_australia_lpd')

ims_france <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'ims_france')

ims_germany <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'ims_germany')

iqvia_amb_emr <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'iqvia_amb_emr')

iqvia_pharmetrics_plus <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'iqvia_pharmetrics_plus')

jmdc <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'jmdc')

#choose DBs based on project you're working on
dbList <- list(truven_mdcd,
               truven_ccae,
               optum_ehr,
               optum_extended_dod
               #,
               #ims_australia_lpd,
               #ims_france,
               #ims_germany,
               #iqvia_amb_emr,
               #iqvia_pharmetrics_plus,
               #jmdc
)
#dps need this from line 181-end

irDesign2 <-readr::read_file(file=file.path(packageRoot, 'irDesign.json') )


analysisSql <- CohortIncidence::buildQuery(incidenceDesign =  as.character(irDesign2),
                                           buildOptions = CohortIncidence::buildOptions())
cat(analysisSql)


baseUrl <- Sys.getenv("baseUrl")

resultsFolder <- file.path(folder, "Results/Incidence")


if (!dir.exists(resultsFolder)) {

  dir.create(resultsFolder, recursive = TRUE)

}

for(dbUp in 1:length(dbList)) {
  httr::set_config(httr::config(ssl_verifypeer = FALSE))
  ROhdsiWebApi::authorizeWebApi(baseUrl, "windows")
  # cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl,
  #                                                                cohortIds = cohortsToIR)
  #for DPs use this
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = file.path(
      packageRoot, "inst/settings/CohortsToCreate.csv"
    ),
    jsonFolder = file.path(packageRoot, "inst/cohorts"),
    sqlFolder = file.path(packageRoot, "inst/sql/sql_server")
  )
  tempTable <- paste0(projectName, "_cohort_table_", dbList[[dbUp]]$cdmDatabaseSchema) #this will hold all the cohorts for all the runs
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = tempTable)


  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbList[[dbUp]]$dbms,
    user = keyring::key_get(service = userNameService),
    password = keyring::key_get(service = passwordService),
    port = dbList[[dbUp]]$port,
    server = dbList[[dbUp]]$server
  )


  outFolder <- file.path(resultsFolder, dbList[[dbUp]]$sourceKey)

  cdm_incrementalFolder <- file.path(outFolder,
                                     'incrementalFolder')

  CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                      cohortTableNames = cohortTableNames,
                                      cohortDatabaseSchema = dbList[[dbUp]]$cohortDatabaseSchema,
                                      incremental = TRUE)

  cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                         cdmDatabaseSchema = dbList[[dbUp]]$cdmDatabaseSchema,
                                                         cohortDatabaseSchema = dbList[[dbUp]]$cohortDatabaseSchema,
                                                         cohortTableNames = cohortTableNames,
                                                         cohortDefinitionSet = cohortDefinitionSet,
                                                         incrementalFolder = cdm_incrementalFolder,
                                                         incremental = TRUE)


  dbList[[dbUp]]$cohortTable <- tempTable



  # outFolder <- file.path(folder, dbList[[dbUp]]$sourceKey)
}


db <- dbList[[1]]

doAnalysis <- function (db) {

  tempTable <- paste0(projectName, "_cohort_table_", db$cdmDatabaseSchema) #this will hold all the cohorts for all the runs
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = tempTable)

  # build options
  buildOptions <- CohortIncidence::buildOptions(cohortTable =  paste0(db$cohortDatabaseSchema,'.',cohortTableNames$cohortTable),
                                                cdmDatabaseSchema = db$cdmDatabaseSchema,
                                                sourceName = db$sourceName,
                                                refId = 1)


  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbList[[dbUp]]$dbms,
    user = keyring::key_get(service = userNameService),
    password = keyring::key_get(service = passwordService),
    port = dbList[[dbUp]]$port,
    server = dbList[[dbUp]]$server
  )


  # Using executeAnalysis()

  executeResults <- CohortIncidence::executeAnalysis(connectionDetails = connectionDetails,
                                                     incidenceDesign = test,
                                                     buildOptions = buildOptions)

  outFolder <- file.path(resultsFolder, dbList[[dbUp]]$sourceKey)

  # resultsFolder <- file.path(folder, "Results/Incidence")
  #
  #
  # if (!dir.exists(resultsFolder)) {
  #
  #       dir.create(resultsFolder, recursive = TRUE)
  #
  #  }


  write.csv(executeResults, paste(path=file.path(outFolder), "/", db$sourceKey, "_results.csv", sep=""), row.names = F)
}

lapply(dbList, doAnalysis)

#concat the files

library(dplyr)

allResults <- list.files(path=file.path(folder, projectName), pattern="_results.csv", full.names=T) %>%
  lapply(read.csv) %>%
  bind_rows

write.csv(allResults, paste(path=file.path(folder, projectName), "/", "combined_results_6.12.23.csv", sep=""), row.names = F)



