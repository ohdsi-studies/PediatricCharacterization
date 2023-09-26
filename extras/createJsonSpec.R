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
readr::write_file(jsonlite::prettify(irDesign$asJSON(), indent=2), "inst/settings/irDesign.json", append = F)
