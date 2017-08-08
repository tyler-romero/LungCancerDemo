
USE lung_cancer_database

print 'GenerateFeatures'
EXECUTE [dbo].[GenerateFeatures];

print 'TrainTestSplit'
EXECUTE [dbo].[TrainTestSplit];

print 'FitPCA'
EXECUTE [dbo].[FitPCA];

print 'ApplyPCA'
EXECUTE [dbo].[ApplyPCA];





