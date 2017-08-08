USE master
go

-- CHange authentication mode to mixed SQL and Windows authentication
EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'LoginMode', REG_DWORD, 2
go

-- Create a new SQL login user
-- Replace login name 'sqldba' and password with your choice of user name/password
CREATE LOGIN demo
    WITH PASSWORD = N'D@tascience',
	CHECK_POLICY     = OFF,
    CHECK_EXPIRATION = OFF;

-- Change role of new user to sysadmin role
-- Replace user name 'sqldba' with the one created above
EXEC sp_addsrvrolemember 
    @loginame = N'demo', 
    @rolename = N'sysadmin';