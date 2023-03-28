library(tidyverse)
library(data.table)
library(parallel)
library(plyr)
library(dplyr)
library(anytime)  

filePath = '/home/choccrwd3/work/CHOC/CausalInference/CI1/ADcohortsetmed.csv'
pdMeds = fread(filePath, header = TRUE)
#Remove words in between brackets [example]
pdMeds$drugcode = gsub("\\[[^\\]]*\\]", "", pdMeds$drugcode, perl=TRUE)
#Remove '.' periods
pdMeds$drugcode = str_remove_all(pdMeds$drugcode, '\\.')
#Remove numbers 0-9
pdMeds$drugcode = gsub('[0-9]+', '', pdMeds$drugcode)

pdMeds$drugcode = gsub('/', '', pdMeds$drugcode)
pdMeds$drugcode = gsub("[()]", '', pdMeds$drugcode)
#remove whitespace both sides trailing and leading
pdMeds$drugcode = trimws(pdMeds$drugcode, "both")
#make everything lowercase
pdMeds$drugcode = tolower(pdMeds$drugcode)
#remove everything that is not 'carbidopa', 'levodopa', 'benztropine', 'selegiline'
pdMeds$drugcode = gsub("(donepezil|galantamine|memantine|rivastigmine|aricept|namenda|namzaric|razadyne|exelon)|.", "\\1", pdMeds$drugcode)
#Add space between carbidoplevodopa
pdMeds$drugcode = ifelse(pdMeds$drugcode == "donepezilmemantine", "donepezilmemantine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "donepezilmemantinedonepezilmemantinedonepezilmemantinedonepezilmemantine", "donepezilmemantine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "memantinememantine", "memantine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "memantinememantinememantinememantine", "memantine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "aricept", "donepezil", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "namzaric", "donepezilmemantine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "exelon", "rivastigmine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "razadyne", "galantamine", pdMeds$drugcode)
pdMeds$drugcode = ifelse(pdMeds$drugcode == "namenda", "memantine", pdMeds$drugcode)
write.csv(pdMeds,file='/home/choccrwd3/work/CHOC/CausalInference/CI1/ADcohortsetmed5.csv', row.names=FALSE)
filePath = "/home/choccrwd3/work/CHOC/CausalInference/CI1/ADcohortsetdemo.csv"

pdDemo = fread(filePath, header = TRUE)

pdDemo = pdDemo[,-c(1,14)]
names(pdDemo) = c("personid","Birthdate","DateofDeath","Gender","MaritalStatus","primaryDisplay_race","primaryDisplay_ethnicity","deceased","source","zip_code","active","tenant","first_dx_date")
pdDemo$first_dx_year = format(as.Date(pdDemo$first_dx_date, format="%d/%m/%Y"),"%Y")

pdDemo$Birthdate  = as.character(pdDemo$Birthdate)
pdDemo$DateofDeath   =  as.character(pdDemo$DateofDeath)
pdDemo$first_dx_date = as.character(pdDemo$first_dx_date)

pdDemo <- pdDemo[pdDemo$first_dx_year == '2016', ]
pdDemo = pdDemo[,-c(14)]
#Grab unique IDs and bin them by ID
#Uses parallel processing to acheive this
uniqueIDs = unique(pdDemo$personid)
binnedData = mclapply(uniqueIDs, function(x) {
  group = pdDemo[pdDemo$personid == x,]
  return(group)
})

outPutData = binnedData


for(i in 1: length(binnedData)){
  table1 = data.frame(binnedData[[i]])
  
  listOfDeaths = table1$DateofDeath[table1$DateofDeath != ""]
  if(length(listOfDeaths) == 0){
    listOfDeaths = NA
  }
  
  listOfZipCodes = table1$zip_code[!is.na(table1$zip_code)]
  
  
  mostOccurringPersonID = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$personid), collapse=","),",")[[1]]))))
  mostOccurringBirthD = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$Birthdate), collapse=","),",")[[1]]))))
  mostOccurringDeath = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", listOfDeaths), collapse=","),",")[[1]]))))
  mostOccurringGender = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$Gender), collapse=","),",")[[1]]))))
  mostOccurringMaritalStatus = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$MaritalStatus), collapse=","),",")[[1]]))))
  mostOccurringRace = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$primaryDisplay_race), collapse=","),",")[[1]]))))
  mostOccurringEth = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$primaryDisplay_ethnicity), collapse=","),",")[[1]]))))
  mostOccurringDeceased = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$deceased), collapse=","),",")[[1]]))))
  mostOccurringSource = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$source), collapse=","),",")[[1]]))))
  mostOccurringZipCode = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", listOfZipCodes), collapse=","),",")[[1]]))))
  mostOccurringActive = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$active), collapse=","),",")[[1]]))))
  mostOccurringTenant = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$tenant), collapse=","),",")[[1]]))))
  mostOccurringDiagnosisDate = gsub("\\'|\\'","", names(which.max(table(strsplit(paste(gsub("\\[|\\]", "", table1$first_dx_date), collapse=","),",")[[1]]))))
  
  mostOccurringRace = trimws(mostOccurringRace, "both")
  if(length(mostOccurringRace) == 0 || mostOccurringRace == "None" || mostOccurringRace == "" || mostOccurringRace == "Unknown racial group" ||
     mostOccurringRace == "Mixed racial group"|| mostOccurringRace == "Refusal by patient to provide information about racial group" || mostOccurringRace == "Other Race" ||
     mostOccurringRace == "Patient data refused" || mostOccurringRace == "Race not stated" || mostOccurringRace == "Patient not asked"
     ){
    mostOccurringRace = 'Unknown'
  }
  
  if(mostOccurringRace == "black" || mostOccurringRace == "Black" || mostOccurringRace == "African" || mostOccurringRace == "Black or African American" || mostOccurringRace == "Jamaican" || mostOccurringRace == "Bahamian"
     || mostOccurringRace == "Barbadian"){
    mostOccurringRace = "African American"
  }
  if(mostOccurringRace == "white" || mostOccurringRace == "White" || mostOccurringRace == "European"){
    mostOccurringRace = "Caucasian"
  }
  
  if(mostOccurringRace == "Native Hawaiian" || mostOccurringRace == "Other Pacific Islander" || mostOccurringRace == "Carolinian" || mostOccurringRace == "Native Hawaiian or Other Pacific Islander") {
    mostOccurringRace = "Pacific Islander"
  }
  
  if(mostOccurringRace == "Alaska Native" || mostOccurringRace == "American Indian" || mostOccurringRace == "Mexican American Indian") {
    mostOccurringRace = "American Indian or Alaska Native"
  }
  
  if(mostOccurringRace == "Asian Indian" || mostOccurringRace == "Chinese" || mostOccurringRace == "Filipino" || mostOccurringRace == "Japanese" || mostOccurringRace == "Korean" || mostOccurringRace == "Malaysian"
     || mostOccurringRace == "Sri Lankan" || mostOccurringRace == "Taiwanese" || mostOccurringRace == "Polynesian" || mostOccurringRace == "Thai" || mostOccurringRace == "West Indian" || mostOccurringRace == "Indonesian"
     || mostOccurringRace == "Vietnamese" || mostOccurringRace == "Bangladeshi" || mostOccurringRace == "Indian" || mostOccurringRace == "Cambodian" || mostOccurringRace == "Burmese" || mostOccurringRace == "Laotian"){
    mostOccurringRace = "Asian"
  }
  
  if(mostOccurringRace == "Middle Eastern or North African" || mostOccurringRace == "Pakistani"){
    mostOccurringRace = "Unknown"
  }
  
  mostOccurringMaritalStatus = trimws(mostOccurringMaritalStatus, "both")
  if(length(mostOccurringMaritalStatus) == 0 || mostOccurringMaritalStatus == ""){
    mostOccurringMaritalStatus = "Unknown"
  }
  
  if(mostOccurringMaritalStatus == "Legally Separated" || mostOccurringMaritalStatus == "Separated" || mostOccurringMaritalStatus == "unmarried" || mostOccurringMaritalStatus == "Widowed" || mostOccurringMaritalStatus == "Never Married"
     ){
    mostOccurringMaritalStatus = "Single"
  }
  
  if(mostOccurringMaritalStatus == "Domestic partner" || mostOccurringMaritalStatus == "Polygamous"){
    mostOccurringMaritalStatus = "Married"
  }
  
  if(mostOccurringMaritalStatus == "unknown" || mostOccurringMaritalStatus == "Other"){
    mostOccurringMaritalStatus = "Unknown"
  }
  
  
  mostOccurringEth = trimws(mostOccurringEth, "both")
  if(length(mostOccurringEth) == 0 || mostOccurringEth == "" || mostOccurringEth == "Ethnic group not given - patient refused" || mostOccurringEth == "Ethnic group unknown" || mostOccurringEth == "Ethnic group not recorded"
     || mostOccurringEth == "None" || mostOccurringEth == "Other"
     ){
    mostOccurringEth = "Unknown"
  }
  
  if(mostOccurringEth == "Central American Indian" || mostOccurringEth == "Honduran" || mostOccurringEth == "Nicaraguan" || mostOccurringEth == "Salvadoran" || mostOccurringEth == "Cuban" || mostOccurringEth == "Dominican"
     || mostOccurringEth == "Ecuadorian" || mostOccurringEth == "Mexican" || mostOccurringEth == "Puerto Rican"){
    mostOccurringEth = "Central American"
  }
  
  if(mostOccurringEth == "Venezuelan" || mostOccurringEth == "Bolivian" || mostOccurringEth == "Chilean" || mostOccurringEth == "Colombian" || mostOccurringEth == "Peruvian"){
    mostOccurringEth = "South American"
  }
  
  if(mostOccurringEth == "English" || mostOccurringEth == "Italians" || mostOccurringEth == "Vietnamese" || mostOccurringEth == "Chippewa" || mostOccurringEth == "Shoshone"|| mostOccurringEth == "Blackfeet"){
    mostOccurringEth = "Not Hispanic or Latino"
  }
  
  if(mostOccurringEth == "Spaniard"){
    mostOccurringEth = "Hispanic or Latino"
  }
  
  if(mostOccurringEth == "South American" || mostOccurringEth == "Central American"){
    mostOccurringEth = "Latin American"
  }
  
  if(length(mostOccurringZipCode) == 0 || mostOccurringZipCode == ""){
    mostOccurringZipCode = "0"
  }
  
  mostOccurringGender = trimws(mostOccurringGender, "both")
  if(length(mostOccurringGender) == 0 || mostOccurringGender == "" || mostOccurringGender == "Gender unknown" || mostOccurringGender == "Gender unspecified" || mostOccurringGender == "Other"){
    mostOccurringGender = "Unknown"
  }
  
  if(is.na(mostOccurringDeceased) || mostOccurringDeceased == "NA"){
    mostOccurringDeceased = FALSE
    
  }
  
  if(mostOccurringDeath != "NA"){
    mostOccurringDeceased = "TRUE"
  }else{
    mostOccurringDeceased = "FALSE"
  }

  #Create New Table with 1 Row
  columns = c("personid","Birthdate","DateofDeath","Gender","MaritalStatus","Race","Ethnicity","deceased","source","zip_code","active","tenant","first_diagnosis_date")
  myData = data.frame(matrix(nrow=1, ncol = length(columns)))
  colnames(myData) = columns
  
  #Set values of the 1st Row
  myData$personid = mostOccurringPersonID
  myData$Birthdate = mostOccurringBirthD
  myData$DateofDeath = mostOccurringDeath
  myData$Gender = mostOccurringGender
  myData$MaritalStatus = mostOccurringMaritalStatus
  myData$Race = mostOccurringRace
  myData$Ethnicity = mostOccurringEth
  myData$deceased = mostOccurringDeceased
  myData$source = mostOccurringSource
  myData$zip_code = mostOccurringZipCode
  myData$active = mostOccurringActive
  myData$tenant = mostOccurringTenant
  myData$first_diagnosis_date = mostOccurringDiagnosisDate
  
  
  outPutData[[i]] = myData
  
}

ADDemographics = do.call(rbind, outPutData)


counts = ddply(ADDemographics, .(ADDemographics$Race, ADDemographics$Ethnicity), nrow)
names(counts) = c("Race","Ethnicity", "Freq")
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "African American", "Not Hispanic or Latino", ADDemographics$Ethnicity)
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "American Indian or Alaska Native", "Not Hispanic or Latino", ADDemographics$Ethnicity)
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "Asian", "Not Hispanic or Latino", ADDemographics$Ethnicity)
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "Hispanic", "Hispanic or Latino", ADDemographics$Ethnicity)
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "Pacific Islander", "Not Hispanic or Latino", ADDemographics$Ethnicity)
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "Caucasian", "Not Hispanic or Latino", ADDemographics$Ethnicity)
ADDemographics$Ethnicity = ifelse(ADDemographics$Race == "Unknown", "Unknown", ADDemographics$Ethnicity)
counts = ddply(ADDemographics, .(ADDemographics$Race, ADDemographics$Ethnicity), nrow)
names(counts) = c("Race","Ethnicity", "Freq")
write.csv(ADDemographics, "/home/choccrwd3/work/CHOC/CausalInference/CI1/AD_demographics_final2016.csv", row.names = FALSE)
filePath  = "/home/choccrwd3/work/CHOC/CausalInference/CI1/ADcohortsetmed5.csv"
filePath1 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/AD_demographics_final2016.csv"
filePath2 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/Diabetes.csv"
filePath3 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/CohortSetOverweightandObesity.csv"
filePath4 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/CerebralInfarction.csv"
filePath5 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/Otherformsofheartdisease1.csv"
filePath6 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/Otherformsofheartdisease2.csv"
filePath7 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/Otherformsofheartdisease3.csv"
filePath8 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/HypertensiveDiseases.csv"
filePath9 = "/home/choccrwd3/work/CHOC/CausalInference/CI1/CohortSetAcuteKidneyFailure.csv"

install.packages("anytime")
library(anytime)  
pdMed = fread(filePath, header = TRUE)
pdDem = fread(filePath1, header = TRUE)

pdDiabetes = fread(filePath2, header = TRUE)
pdDiabetes$Diabetes <- "E10-E13"
pdDiabetes = pdDiabetes[,-c(1,3)]

pdOverweightandObesity = fread(filePath3, header = TRUE)
pdOverweightandObesity$OverWeightAndObesity <- "E66"
pdOverweightandObesity = pdOverweightandObesity[,-c(1,3)]

CerebralInfarction = fread(filePath4, header = TRUE)
CerebralInfarction$CerebralInfarction <- "I60-I69"
CerebralInfarction = CerebralInfarction[,-c(1,3)]

OtherFormsofHeartDisease1 = fread(filePath5, header = TRUE)
OtherFormsofHeartDisease2 = fread(filePath6, header = TRUE)
OtherFormsofHeartDisease3 = fread(filePath7, header = TRUE)
OtherFormsofHeartDisease = rbind(OtherFormsofHeartDisease1, OtherFormsofHeartDisease2)
OtherFormsofHeartDisease$OtherFormsofHeartDisease <- "I3-I5"
OtherFormsofHeartDisease = OtherFormsofHeartDisease[,-c(1,3)]
OtherFormsofHeartDisease = OtherFormsofHeartDisease[!duplicated(personid), ]

HypertensiveDiseases = fread(filePath8, header = TRUE)
HypertensiveDiseases$HypertensiveDiseases <- "I10"
HypertensiveDiseases = HypertensiveDiseases[,-c(1,3)]

CohortSetAcuteKidneyFailure = fread(filePath9, header = TRUE)
CohortSetAcuteKidneyFailure$CohortSetAcuteKidneyFailure <- "N17-N19"
CohortSetAcuteKidneyFailure = CohortSetAcuteKidneyFailure[,-c(1,3)]


pdMed2 <- dcast(setDT(pdMed), personid + encounterid ~drugcode, length)
#length(unique(pdMed$personid))

pdMed2 <- within(pdMed2, {
        
        donepezil[is.na(donepezil)] <- 0
        donepezil.cat <- ifelse(donepezil > 0, "Donepezil", "")
        memantine[is.na(memantine)] <- 0
        memantine.cat <- ifelse(memantine > 0, "Memantine", "")
        rivastigmine[is.na(rivastigmine)] <- 0
        rivastigmine.cat <- ifelse(rivastigmine > 0, "Rivastigmine", "")
        galantamine[is.na(galantamine)] <- 0
        galantamine.cat <- ifelse(galantamine > 0, "Galantamine", "")
        donepezilmemantine[is.na(donepezilmemantine)] <- 0
        donepezilmemantine.cat <- ifelse(donepezilmemantine > 0, "Done/Mema", "")              
})

pdMed2$Drug <- paste(pdMed2$donepezil.cat,pdMed2$memantine.cat,pdMed2$rivastigmine.cat,pdMed2$galantamine.cat,pdMed2$donepezilmemantine.cat)

#remove whitespace both sides trailing and leading
pdMed2$Drug = trimws(pdMed2$Drug, "both")

pdMed2 = dplyr::select(pdMed2, c("personid","Drug" ))

pdMed1 <- dcast(setDT(pdMed2), personid~Drug, length)
colnames(pdMed1)
pdMed1$DoneMema.cat <- ifelse(pdMed1$`Done/Mema`> 0, "Done/Mema", "")
pdMed1$Donepezil.cat <- ifelse(pdMed1$`Donepezil`> 0, "Donepezil", "")
pdMed1$Memantine.cat <- ifelse(pdMed1$Memantine> 0, "Memantine", "")
pdMed1$Galantamine.cat <- ifelse(pdMed1$Galantamine> 0, "Galantamine", "")
pdMed1$Rivastigmine.cat <- ifelse(pdMed1$Rivastigmine> 0, "Rivastigmine", "")

pdMed1$DonepezilDoneMema.cat <- ifelse(pdMed1$`Donepezil    Done/Mema`> 0, "Donepezil/Done/Mema", "")  
pdMed1$DonepezilGalantamine.cat <- ifelse(pdMed1$`Donepezil   Galantamine`> 0, "Donepezil/Galantamine", "") 
pdMed1$DonepezilMemantine.cat <- ifelse(pdMed1$`Donepezil Memantine`> 0, "Donepezil/Memantine", "") 
pdMed1$DonepezilRivastigmine.cat <- ifelse(pdMed1$`Donepezil  Rivastigmine`> 0, "Donepezil/Rivastigmine", "") 
pdMed1$GalantamineDoneMema.cat <- ifelse(pdMed1$`Galantamine Done/Mema`> 0, "Galantamine/Done/Mema", "")   
pdMed1$MemantineDoneMema.cat <- ifelse(pdMed1$`Memantine   Done/Mema`> 0, "Memantine/Done/Mema", "")  
pdMed1$MemantineGalantamine.cat <- ifelse(pdMed1$`Memantine  Galantamine`> 0, "Memantine/Galantamine", "")   
pdMed1$RivastigmineDoneMema.cat <- ifelse(pdMed1$`Rivastigmine  Done/Mema`> 0, "Rivastigmine/Done/Mema", "")    
pdMed1$RivastigmineGalantamine.cat <- ifelse(pdMed1$`Rivastigmine Galantamine`> 0, "Rivastigmine/Galantamine", "")   
pdMed1$MemantineRivastigmine.cat <- ifelse(pdMed1$`Memantine Rivastigmine`> 0, "Memantine/Rivastigmine", "")    

pdMed1$DonepezilGalantamineDoneMema.cat <- ifelse(pdMed1$`Donepezil   Galantamine Done/Mema`> 0, "Donepezil/Galantamine/Done/Mema", "")  
pdMed1$DonepezilRivastigmineDoneMema.cat <- ifelse(pdMed1$`Donepezil  Rivastigmine  Done/Mema`> 0, "Donepezil/Rivastigmine/Done/Mema", "")  
pdMed1$DonepezilRivastigmineGalantamine.cat <- ifelse(pdMed1$`Donepezil  Rivastigmine Galantamine`> 0, "Donepezil/Rivastigmine/Galantamine", "")   
pdMed1$DonepezilMemantineDoneMema.cat <- ifelse(pdMed1$`Donepezil Memantine   Done/Mema`> 0, "Donepezil/Memantine/Done/Mema", "")  
pdMed1$DonepezilMemantineGalantamine.cat <- ifelse(pdMed1$`Donepezil Memantine  Galantamine`> 0, "Donepezil/Memantine/Galantamine", "")  
pdMed1$DonepezilMemantineRivastigmine.cat <- ifelse(pdMed1$`Donepezil Memantine Rivastigmine`> 0, "Donepezil/Memantine/Rivastigmine", "")  
pdMed1$MemantineGalantamineDoneMema.cat <- ifelse(pdMed1$`Memantine  Galantamine Done/Mema`> 0, "Memantine/Galantamine/Done/Mema", "") 
pdMed1$MemantineRivastigmineDoneMema.cat <- ifelse(pdMed1$`Memantine Rivastigmine  Done/Mema`> 0, "Memantine/Rivastigmine/Done/Mema", "")    
pdMed1$MemantineRivastigmineGalantamine.cat <- ifelse(pdMed1$`Memantine Rivastigmine Galantamine`> 0, "Memantine/Rivastigmine/Galantamine", "")    
pdMed1$RivastigmineGalantamineDoneMema.cat <- ifelse(pdMed1$`Rivastigmine Galantamine Done/Mema`> 0, "Rivastigmine/Galantamine/Done/Mema", "")  

pdMed1$DonepezilMemantineRivastigmineDoneMema.cat <- ifelse(pdMed1$`Donepezil Memantine Rivastigmine  Done/Mema`> 0, "Donepezil/Memantine/Rivastigmine/Done/Mema", "")  
pdMed1$DonepezilMemantineRivastigmineGalantamine.cat <- ifelse(pdMed1$`Donepezil Memantine Rivastigmine Galantamine`> 0, "Donepezil/Memantine/Rivastigmine/Galantamine", "")
pdMed1$DonepezilRivastigmineGalantamineDoneMema.cat <- ifelse(pdMed1$`Donepezil  Rivastigmine Galantamine Done/Mema`> 0, "Donepezil/Rivastigmine/Galantamine/Done/Mema", "")   

pdMed1$DonepezilMemantineRivastigmineGalantamineDoneMema.cat <- ifelse(pdMed1$`Donepezil Memantine Rivastigmine Galantamine Done/Mema`> 0, "Donepezil/Memantine/Rivastigmine/Galantamine/Done/Mema", "")   

pdMed1$Drug <- paste(pdMed1$DoneMema.cat, 
                     pdMed1$Donepezil.cat, 
                     pdMed1$DonepezilDoneMema.cat, 
                     pdMed1$DonepezilGalantamine.cat, 
                     pdMed1$DonepezilGalantamineDoneMema.cat, 
                     pdMed1$DonepezilRivastigmine.cat, 
                     pdMed1$DonepezilRivastigmineDoneMema.cat, 
                     pdMed1$DonepezilRivastigmineGalantamine.cat, 
                     pdMed1$DonepezilRivastigmineGalantamineDoneMema.cat, 
                     pdMed1$DonepezilMemantine.cat, 
                     pdMed1$DonepezilMemantineDoneMema.cat, 
                     pdMed1$DonepezilMemantineGalantamine.cat, 
                     pdMed1$DonepezilMemantineRivastigmine.cat, 
                     pdMed1$DonepezilMemantineRivastigmineDoneMema.cat, 
                     pdMed1$DonepezilMemantineRivastigmineGalantamine.cat, 
                     pdMed1$DonepezilMemantineRivastigmineGalantamineDoneMema.cat, 
                     pdMed1$Galantamine.cat, 
                     pdMed1$GalantamineDoneMema.cat, 
                     pdMed1$Memantine.cat, 
                     pdMed1$MemantineDoneMema.cat, 
                     pdMed1$MemantineGalantamine.cat, 
                     pdMed1$MemantineGalantamineDoneMema.cat, 
                     pdMed1$MemantineRivastigmine.cat, 
                     pdMed1$MemantineRivastigmineDoneMema.cat, 
                     pdMed1$MemantineRivastigmineGalantamine.cat, 
                     pdMed1$Rivastigmine.cat, 
                     pdMed1$RivastigmineDoneMema.cat, 
                     pdMed1$RivastigmineGalantamine.cat, 
                     pdMed1$RivastigmineGalantamineDoneMema.cat 
)

pdMed1$Drug = trimws(pdMed1$Drug, "both")

df2 <- merge(x=pdDem , y= pdMed1, by="personid", all.x=TRUE)     
#df2 <- merge(x = pdMed1 , y = pdDem, by="personid")  

df <-df2
df <- merge(x = df,y = pdDiabetes, by="personid", all.x=TRUE)
df <- merge(x = df,y = pdOverweightandObesity, by="personid", all.x=TRUE)
df <- merge(x = df,y = CerebralInfarction, by="personid", all.x=TRUE)
df <- merge(x = df,y = OtherFormsofHeartDisease, by="personid", all.x=TRUE)
df <- merge(x = df,y = HypertensiveDiseases, by="personid", all.x=TRUE)
df <- merge(x = df,y = CohortSetAcuteKidneyFailure, by="personid", all.x=TRUE)

df$Yearsofsurvival <- difftime(anydate(df$DateofDeath),anydate(df$first_diagnosis_date), units = "days")/365
df$AgeatDiagnosis <- as.numeric(round(difftime(anydate(df$first_diagnosis_date),anydate(df$Birthdate), units = "days")/365))
df$survived <- ifelse(df$Yearsofsurvival>=5, 1, 0)
df$survived[is.na(df$survived)] <- 1
length(unique(df$personid))
df <- df[df$Drug %in% c(NA,"Done/Mema", "Donepezil", "Memantine", "Galantamine", "Rivastigmine", "Donepezil/Done/Mema", 
                        "Donepezil/Galantamine", "Donepezil/Memantine", "Donepezil/Rivastigmine", "Galantamine/Done/Mema",  
                        "Memantine/Done/Mema", "Memantine/Galantamine", "Rivastigmine/Done/Mema", 
                        "Rivastigmine/Galantamine", "Memantine/Rivastigmine",  "Donepezil/Galantamine/Done/Mema", 
                        "Donepezil/Rivastigmine/Done/Mema", "Donepezil/Rivastigmine/Galantamine",
                        "Donepezil/Memantine/Done/Mema", "Donepezil/Memantine/Galantamine", "Donepezil/Memantine/Rivastigmine", 
                        "Memantine/Galantamine/Done/Mema", 
                        "Memantine/Rivastigmine/Done/Mema", "Memantine/Rivastigmine/Galantamine","Rivastigmine/Galantamine/Done/Mema", 
                        "Donepezil/Memantine/Rivastigmine/Done/Mema", "Donepezil/Memantine/Rivastigmine/Galantamine", 
                        "Donepezil/Rivastigmine/Galantamine/Done/Mema", "Donepezil/Memantine/Rivastigmine/Galantamine/Done/Mema"),]

df = dplyr::select(df, c("personid","Birthdate","DateofDeath","Gender" ,"MaritalStatus","Race" ,"Ethnicity","deceased" ,"first_diagnosis_date", "Drug" ,                                                 
                         "Diabetes","OverWeightAndObesity" ,"CerebralInfarction","OtherFormsofHeartDisease",                      
                         "HypertensiveDiseases","CohortSetAcuteKidneyFailure",                           
                         "Yearsofsurvival","AgeatDiagnosis",                                        
                         "survived" ))

       
write.csv(df, "/home/choccrwd3/work/CHOC/CausalInference/CI1/FinalDataFrameAD.csv")

df <- within(df, {
        Diabetes.Cat <- ifelse(is.na(Diabetes),0, 1)
        OverWeightAndObesity.Cat <- ifelse(is.na(OverWeightAndObesity),0, 1)
        CerebralInfarction.Cat <- ifelse(is.na(CerebralInfarction),0, 1)
        OtherFormsofHeartDisease.Cat <- ifelse(is.na(OtherFormsofHeartDisease),0, 1)
        HypertensiveDiseases.Cat <- ifelse(is.na(HypertensiveDiseases),0, 1)
        CohortSetAcuteKidneyFailure.Cat <- ifelse(is.na(CohortSetAcuteKidneyFailure),0, 1)
})

#remove whitespace both sides trailing and leading
df$Drug = trimws(df$Drug, "both")
df <- df[is.na(df$Drug) |df$Drug == "Memantine"|df$Drug == "Donepezil"|df$Drug == "Donepezil/Memantine",]

df$i = ifelse(is.na(df$Drug), 0, df$Drug)
df$i = ifelse(df$Drug == "Memantine", 1, df$i)
df$i = ifelse(df$Drug == "Donepezil", 2, df$i)
df$i = ifelse(df$Drug == "Donepezil/Memantine", 3, df$i)

df = dplyr::select(df, c("personid","survived","Gender","MaritalStatus","Race","AgeatDiagnosis",
                         "CohortSetAcuteKidneyFailure.Cat", "HypertensiveDiseases.Cat",
                         "OtherFormsofHeartDisease.Cat", "CerebralInfarction.Cat",
                         "OverWeightAndObesity.Cat", "Diabetes.Cat",
                         "CohortSetAcuteKidneyFailure.Cat", "i"))

df <- within(df, {
        Gender <- as.factor(Gender)
        MaritalStatus <- factor(MaritalStatus)
        Race <- as.factor(Race)
        Agesquared <- AgeatDiagnosis^2                 
})

df[is.na(df)] = 0
write.csv(df, "/home/choccrwd3/work/CHOC/CausalInference/CI1/ADFinalresult1.csv")

filePath = "/home/choccrwd3/work/CHOC/CausalInference/CI1/ADcohortsetwithencounterv1.csv"
df = fread(filePath, header = TRUE)

df$daysinhospital <- difftime(anydate(df$Dischargedate),anydate(df$ActualArrivalDate), units = "days")+1

df = dplyr::select(df, c("personid","encounterid","ActualArrivalDate","Dischargedate","daysinhospital"))

write.csv(df, "/home/choccrwd3/work/CHOC/CausalInference/CI1/daysinhospital.csv")

