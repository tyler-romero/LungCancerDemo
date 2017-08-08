DECLARE @PredictionResultSP FLOAT;
EXECUTE lung_cancer_database.dbo.PredictLungCancer @PatientIndex = 5, @ModelName = "rx_fast_trees", @PredictionResult = @PredictionResultSP;