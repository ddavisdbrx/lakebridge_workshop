-- The DDL syntax should change, including contraints and CLUSTERED COLUMNSTORE INDEX upon conversion to Databricks syntax



CREATE OR REPLACE TABLE Test.DimGeography
(
	GeographySKey int NOT NULL,
	CountryOfficeRegionNKey int NOT NULL,
	CountryNKey int NOT NULL,
	OfficeRegionName STRING,
	CountryName STRING ,
	Region STRING ,
	Area STRING ,
	IsIFIAR9 BOOLEAN ,
	HasOfficeRegionList BOOLEAN ,
	HasCountryList BOOLEAN ,
	CreateDate timestamp ,
	CreateUserID STRING ,
	ModifyDate timestamp ,
	ModifyUserID STRING ,
	Auditstatus STRING ,
	PipelineRunId STRING ,
	AuditCreateDate timestamp NOT NULL,
	AuditModifyDate timestamp ,
	Country2Cd STRING ,
	Country3Cd STRING ,
	CountryNumCd STRING ,
	CountryNameISO STRING ,
 CONSTRAINT PK_Test_DimGeography PRIMARY KEY (GeographySKey) NOT ENFORCED 
)

;
