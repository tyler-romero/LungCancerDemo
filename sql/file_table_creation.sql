-- Follow the instruction in readme file to enable FileTable on your database

-- Conigure new database  
CREATE DATABASE LungCancer  
WITH FILESTREAM ( NON_TRANSACTED_ACCESS = FULL, DIRECTORY_NAME = N'FileTableData' )

-- Configure existing database
ALTER DATABASE LungCancer  
SET FILESTREAM ( NON_TRANSACTED_ACCESS = FULL, DIRECTORY_NAME = N'FileTableData' )

-- Creating a FileTable table
USE LungCancer
GO
CREATE TABLE MriData AS FileTable  
WITH (   
	FileTable_Directory = 'MriData',  
	FileTable_Collate_Filename = database_default  
);  
