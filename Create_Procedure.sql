-- Create Procedure

CREATE OR REPLACE PROCEDURE "STG".PRC_AGG_TRIGGERS()
LANGUAGE 'plpsql'
AS
$BODY$
DECLARE
	v_runtime TIMESTAMP;
	v_status TEXT;
	v_msg TEXT;
BEGIN
	v_runtime := NOW();
	v_status := 'SUCCESS';
	v_msg := NULL;

	----INSERT LOGIC



	----LOGGING OUTCOME



EXCEPTION

END;