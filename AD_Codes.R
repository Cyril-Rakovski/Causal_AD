#library(tidyverse)
library(data.table)
library(parallel)
library(plyr)
library(anytime) 
library(dplyr)
library(nnet)


filePath = "C:/Users/ehsan/Documents/Alzheimer disease/ADdf.csv"
df = fread(filePath, header = TRUE)
df = dplyr::select(df, c("personid","Birthdate","DateofDeath","Gender" ,"MaritalStatus","Race" ,"Ethnicity","deceased" ,"first_diagnosis_date", "Drug" ,                                                 
                         "Diabetes","OverWeightAndObesity" ,"CerebralInfarction","OtherFormsofHeartDisease",                      
                         "HypertensiveDiseases","CohortSetAcuteKidneyFailure",                           
                         "Yearsofsurvival","AgeatDiagnosis",                                        
                         "survived" ))

# Create categorical variables
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

# Filter rows based on Drug column values
df <- df[is.na(df$Drug) |df$Drug == "Memantine"|df$Drug == "Donepezil"|df$Drug == "Donepezil/Memantine",]

# Create i variable based on Drug column
df$i = ifelse(is.na(df$Drug), 0, df$Drug)
df$i = ifelse(df$Drug == "Memantine", 1, df$i)
df$i = ifelse(df$Drug == "Donepezil", 2, df$i)
df$i = ifelse(df$Drug == "Donepezil/Memantine", 3, df$i)
df = dplyr::select(df, c("personid","survived","Gender","MaritalStatus","Race","AgeatDiagnosis",
                         "CohortSetAcuteKidneyFailure.Cat", "HypertensiveDiseases.Cat",
                         "OtherFormsofHeartDisease.Cat", "CerebralInfarction.Cat",
                         "OverWeightAndObesity.Cat", "Diabetes.Cat",
                         "CohortSetAcuteKidneyFailure.Cat", "i"))
# Convert variables to appropriate types
df <- within(df, {
        Gender <- as.factor(Gender)
        MaritalStatus <- factor(MaritalStatus)
        Race <- as.factor(Race)
        Agesquared <- AgeatDiagnosis^2                 
})
df[is.na(df)] = 0
dre1 <- data.frame(matrix(ncol = 6, nrow = 0))
colnames(dre1) = c("iteration","estimator","T0","T1","T2","T3")
df$personid = 1:nrow(df)


base.model = multinom(as.factor(i)~.+ "Interaction between treatment and confounders is included if estimable from the data.", data = df[,-c(1,2)])


# Predict probabilities
predictdf <- predict(base.model, newdata = df[,-c(1,2)], "probs")
predictdf <- as.data.frame(predictdf)
predictdf <- cbind(predictdf, df[,c(1,2,13)])
predictdf[is.na(predictdf)] = 0
my_vector = c()

# Calculate inverse probability weights
predictdf$ipw = ifelse(predictdf$i == 0, 1/(predictdf$`0`),NA)
predictdf$ipw = ifelse(predictdf$i == 1, 1/(predictdf$`1`), predictdf$ipw) 
predictdf$ipw = ifelse(predictdf$i == 2, 1/(predictdf$`2`),predictdf$ipw)
predictdf$ipw = ifelse(predictdf$i == 3, 1/(predictdf$`3`),predictdf$ipw)
predictdf$ap = predictdf$survived*predictdf$ipw
for(ii in 1:4) {
        
        my_vector[ii] <- sum(as.numeric(predictdf[predictdf$i == ii-1, ]$ap))/nrow(predictdf)
        
}
my_vector 

df$i <- as.factor(df$i)
base.model2 = glm(survived ~. + "Interaction between treatment and confounders is included if estimable from the data.",family = binomial, data = df[,-1])

# Create subsets of data for each treatment group
df1=df
df2=df
df3=df
df$i <- "0"
df1$i <- "1"
df2$i <- "2"
df3$i <- "3"
dftempMerged <- do.call("rbind", list(df, df1, df2, df3))
dftempMerged$p1 <- predict(base.model2,newdata = dftempMerged[,-1],type = "response")
dftempMerged1 = dplyr::select(dftempMerged, c("personid","i","p1"))
TdftempMerged1 <- dcast(setDT(dftempMerged1), personid ~i,fun.aggregate =sum, value.var = "p1")
colnames(TdftempMerged1) = c("personid","T0","T1","T2","T3")
final <- merge(x = predictdf,y = TdftempMerged1, by=c("personid"), all.x=TRUE)
final$ft = ifelse(final$i == 0, as.numeric(final$ipw*final$T0), NA)
final$ft = ifelse(final$i == 1, as.numeric(final$ipw*final$T1), final$ft)
final$ft = ifelse(final$i == 2, as.numeric(final$ipw*final$T2), final$ft)
final$ft = ifelse(final$i == 3, as.numeric(final$ipw*final$T3), final$ft)
final1 = dplyr::select(final, c("personid","i","ft"))

# Reshape data
final2 <- dcast(setDT(final1), personid ~i, fun.aggregate =sum,value.var = "ft")
final2[is.na(final2)] = 0

# Compute estimates and add to summary data frame
jj=1
dre1 = rbind(dre1,c(jj,"IP weighted",my_vector[1],my_vector[2],my_vector[3],my_vector[4]))
dre1 = rbind(dre1,c(jj,"Y for patients",mean(final2$`0`),mean(final2$`1`),mean(final2$`2`),mean(final2$`3`)))
dre1 = rbind(dre1,c(jj,"standardized estimators",mean(final$T0),mean(final$T1),mean(final$T2),mean(final$T3)))
dre1 = rbind(dre1,c(jj,"doubly robust estimators",
                    (my_vector[1]+mean(final$T0)-mean(final2$`0`)),
                    (my_vector[2]+mean(final$T1)-mean(final2$`1`)),
                    (my_vector[3]+mean(final$T2)-mean(final2$`2`)),
                    (my_vector[4]+mean(final$T3)-mean(final2$`3`))))
View(dre1)
