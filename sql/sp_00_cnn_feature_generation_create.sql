USE [lung_cancer_database]
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


IF OBJECT_ID('[dbo].[GenerateFeatures]', 'P') IS NOT NULL  
    DROP PROCEDURE [dbo].GenerateFeatures;  
GO  

CREATE PROCEDURE [dbo].GenerateFeatures 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    -- Insert statements for procedure here
	EXECUTE sp_execute_external_script
      @language = N'Python'
    , @script = N'
from revoscalepy import RxInSqlServer, RxLocalSeq, rx_set_compute_context, RxSqlServerData, rx_data_step
from lung_cancer.lung_cancer_utils import gather_image_paths, compute_features, average_pool
from lung_cancer.connection_settings import IMAGES_FOLDER, MICROSOFTML_MODEL_NAME, TABLE_FEATURES, get_connection_string

connection_string = get_connection_string()
sql = RxInSqlServer(connection_string=connection_string)
local = RxLocalSeq()
rx_set_compute_context(local)

data = InputDataSet

print("Preparing list of images")
n_patients = 200    # How many patients do we featurize images for?
data = data.head(n_patients)
data_to_featurize = gather_image_paths(data, IMAGES_FOLDER)

featurized_data = compute_features(data_to_featurize, MICROSOFTML_MODEL_NAME, compute_context=local)
pooled_data = average_pool(data, featurized_data)

features_sql = RxSqlServerData(table=TABLE_FEATURES, connection_string=connection_string)
rx_data_step(pooled_data, features_sql, overwrite=True)	
'
    , @input_data_1 = N'SELECT patient_id, label FROM labels;';
END
GO


IF OBJECT_ID('[dbo].[TrainTestSplit]', 'P') IS NOT NULL  
    DROP PROCEDURE [dbo].TrainTestSplit;  
GO  

CREATE PROCEDURE [dbo].TrainTestSplit 
AS
BEGIN
	DROP TABLE IF EXISTS train_id;
	SELECT DISTINCT patient_id INTO train_id FROM patients WHERE ABS(CAST(BINARY_CHECKSUM(idx, NEWID()) as int)) % 100 < 80 ;
END
GO


IF OBJECT_ID('[dbo].[FitPCA]', 'P') IS NOT NULL  
    DROP PROCEDURE [dbo].FitPCA;  
GO  

CREATE PROCEDURE [dbo].FitPCA 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    -- Insert statements for procedure here
	DROP TABLE IF EXISTS [dbo].[pca_transform]
	CREATE TABLE [dbo].[pca_transform](
		[info] [varbinary](max) NOT NULL
	)

	INSERT INTO pca_transform
	EXECUTE sp_execute_external_script
      @language = N'Python'
    , @script = N'
import pandas as pd
import dill
from sklearn.decomposition import IncrementalPCA

train_data = pd.DataFrame(InputDataSet)

# Perform PCA transform
n_patients = len(train_data)
n = min(485, n_patients)    # 485 features is the most that can be handled right now
print("PCA with n_components={}".format(n))
pca = IncrementalPCA(n_components=n, whiten=True)

train_data = train_data.drop(["label", "patient_id"], axis=1)
pca.fit(train_data)

OutputDataSet = pd.DataFrame({"payload": dill.dumps(pca)}, index=[0])
'
    , @input_data_1 = N'SELECT * FROM dbo.features WHERE patient_id IN (SELECT patient_id FROM train_id);';
END
GO


IF OBJECT_ID('[dbo].[ApplyPCA]', 'P') IS NOT NULL  
    DROP PROCEDURE [dbo].ApplyPCA;  
GO  

CREATE PROCEDURE [dbo].ApplyPCA 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    -- Insert statements for procedure here
	DECLARE @pca_transform varbinary(max) = (select * from [dbo].[pca_transform]);

	EXECUTE sp_execute_external_script
      @language = N'Python'
    , @script = N'
import pandas as pd
import dill

from revoscalepy import rx_data_step, RxSqlServerData
from lung_cancer.connection_settings import get_connection_string, TABLE_PCA_FEATURES

connection_string = get_connection_string()

all_data = pd.DataFrame(InputDataSet)
pca = dill.loads(pca_transform)

feats = all_data.drop(["label", "patient_id"], axis=1)
feats = pca.transform(feats)
feats = pd.DataFrame(data=feats, index=all_data.index.values, columns=["pc" + str(i) for i in range(feats.shape[1])])
pca_data = pd.concat([all_data[["label", "patient_id"]], feats], axis=1)

pca_features_sql = RxSqlServerData(table=TABLE_PCA_FEATURES, connection_string=connection_string)
rx_data_step(pca_data, pca_features_sql, overwrite=True)    # Using rx_data_step because number of columns does not have to be predetermined
'
    , @input_data_1 = N'SELECT * FROM features;'
	, @params = N' @pca_transform varbinary(max)'
	, @pca_transform = @pca_transform;

END
GO