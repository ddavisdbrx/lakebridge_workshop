/****** Object:  StoredProcedure [EDW].[Load_EntityName]    ******/




CREATE OR REPLACE PROCEDURE `lakebridge`.`Load_EntityName`(
IN :FROM timestamp,
IN :To timestamp,
IN :ExecutionStart timestamp,
IN :PipelineRunId STRING,
IN :IsIncremental `BOOLEAN`)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

/*          
Procedure Name	: EDW.Load_EntityName
Create Date		: 2022-08-24
Description		: To load EntityName data based on 2022 GIS format
*/
BEGIN

-- Verify old GIS version exists and has data 
	--		(don't want to destroy that yet, or have to go through the trouble of getting it from external file archive)

RESIGNAL

	;
IF 1 > (select count(*) from `lakebridge`.`EntityName_oldGIS`)
		;
RESIGNAL

	ELSE
THEN
-- Empty the existing data for reload

TRUNCATE table `lakebridge`.`EntityName`

			-- EDW.EntityName (GIS Customer.vwEntityName) Replacement query
;
			insert into `lakebridge`.`EntityName`
					(	EsdItemId, EDWIsDeleted, EDWLastUpdated, EntityNameWoAccentNm, EntityNm, EsdEntityNameId, FilteredEntityNm, NameTypeCd, NameTypeDesc
					,	AuditCreateDate, AuditStatus, PipelineRunId, GisVersionID)
			select		be.EntityId
					,	case when be.EDWIsDeletedInd = 1 then 'Y' else 'N' end 							-- Simple conversion 1-->Y and 0-->N
					,	be.EDWUpdatedDtm 																-- ! These dates do not match across views (Never null in Customer.vwEntity)
					,	be.EntityLegalNmTxt
					,	be.EntityLegalNmTxt
					,	-10000000
					,	upper(replace(EntityLegalNmTxt, ' ', "))	-- May need a little more work, but this is the basic idea
					,	'LEGAL'
					,	'Legal Name/Official Name'
					,	current_timestamp()
					,	'I'
					,	:PipelineRunId
					,	gis.GisVersionID
			from		EDW.DimBusinessEntity be
			join		EDW.GisVersion gis on gis.`Name` = 'Post2022'
			where		be.EDWIsCurrentInd = 1

			-- Merge prior GIS data that does not conflict (EsdItemId not in those rows inserted above)
;
			insert into `lakebridge`.`EntityName`
					(	EsdItemId, EDWIsDeleted, EDWLastUpdated, EntityNameWoAccentNm, EntityNm, EsdEntityNameId, FilteredEntityNm, NameTypeCd, NameTypeDesc
					,	AuditCreateDate, AuditModifyDate, AuditStatus, PipelineRunId, GisVersionID)
			select		en.EsdItemId
					,	en.EDWIsDeleted
					,	en.EDWLastUpdated
					,	en.EntityNameWoAccentNm
					,	en.EntityNm
					,	en.EsdEntityNameId
					,	en.FilteredEntityNm
					,	en.NameTypeCd
					,	en.NameTypeDesc
					,	en.AuditCreateDate
					,	en.AuditModifyDate
					,	en.AuditStatus
					,	en.PipelineRunId
					,	gis.GisVersionID
			from		EDW.EntityName_oldGIS en
			join		EDW.GisVersion gis on gis.`Name` = 'Pre2022'
			left join	EDW.EntityName x on x.EsdItemId = en.EsdItemId
			where		x.EsdItemId is null

		;

END IF
;
END;
