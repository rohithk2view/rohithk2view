/////////////////////////////////////////////////////////////////////////
// Project Web Services
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.lu.k2_ws.D2D;

import com.k2view.cdbms.shared.user.WebServiceUserCode;
import com.k2view.fabric.api.endpoint.Endpoint.MethodType;
import com.k2view.fabric.api.endpoint.Endpoint.Produce;
import com.k2view.fabric.api.endpoint.Endpoint.param;
import com.k2view.fabric.api.endpoint.Endpoint.webService;
import com.k2view.fabric.common.Json;

import ch.qos.logback.classic.encoder.JsonEncoder;

import java.util.*;
import java.util.Map.Entry;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.Db.Rows;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.interfaces.pubsub.JsonSerializer;
import com.k2view.broadway.actors.builtin.DeepCopy;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.fabric.api.endpoint.Endpoint.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class Logic extends WebServiceUserCode {


	@webService(path = "", verb = {MethodType.GET, MethodType.POST, MethodType.PUT, MethodType.DELETE}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static String wsCreateOrCleanD2DTables(@param(description="Creates D2D Tables if not exists", required=true) Boolean create, @param(description="Deletes the Records from D2D Tables", required=true) Boolean clean, @param(required=true) String lu_name) throws Exception {
		if(!(create || clean))
			 return "No option Selected";
		
		fabric().execute(String.format("broadway %s.bwCreateD2DTablesNdViews createTables = ?, lu_name = ?, cleanTables = ?", lu_name), create, lu_name, clean);
		return String.format("D2D Tables %s succesfully", (create)? "created" : "cleaned");
	}

	@webService(path = "report-data", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static List<Object> wsGetReportData(@param(description="sets the interface for the query", required=true) String reportInterface, @param(description="SQL query", required=true) String reportQuery) throws Exception {
		List<Object> results = new ArrayList<>();
		try(Rows rows = db(reportInterface).fetch(reportQuery, new Object[]{})){
		    for (Db.Row row : rows){
		        Map<String, Object> rowMap = new HashMap<>();
		        for(int i=0;i<row.cells().length;i++){
		            String k = String.valueOf(row.keySet().toArray()[i]);
		            Object v = row.cell(i);
		            rowMap.put(k, v);
		        }
		        results.add(rowMap);
		    }
		    return results;
		}catch (SQLException sqlException){
		    log.error(sqlException);
		}
				
		return null; 
	}
	
    @webService(path = "d2d/execution-summary", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetExecutionSummary(@param(required = false) String luType) throws SQLException{

        if(luType == null){
            luType = "D2D";
        }

        String reportSchema = getGlobalValue(luType+".D2D_LOAD_SCHEMA");
        String reportInterface = getGlobalValue(luType+".D2D_RESULT_INTERFACE");
        String tableName = "ref_d2d_entity_summary";

        Db.Rows results = null;
        
       
       /**
        if (!reportInterface.equalsIgnoreCase("fabric")){
            tableName = "d2d_entity_summary";
        }
        */

        results = db(reportInterface).fetch(String.format(
            "select execution_id, " + 
                "case when count(case when match_result='Mismatch' then 1 end) > 0 then 'Mismatch' else 'Match' end as match_result, " +
                "min(update_time) as update_time, " +
                "count(iid) as iids " +
                "from %s.%s " +
            "group by execution_id order by update_time desc", reportSchema, tableName));
        return normalizeResults(results);

    } 
	private static String getGlobalValue(String global){
        String globalVal="";
        try {
            globalVal = fabric().fetch("set "+global).firstValue().toString();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return globalVal;
    }
    @webService(path = "d2d/entity-summary", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetEntitySummary(@param(required = false) String luType, String executionId) throws SQLException{
        if(luType == null){
            luType = "D2D";
        }

        String reportSchema = getGlobalValue(luType+".D2D_LOAD_SCHEMA");
        String reportInterface = getGlobalValue(luType+".D2D_RESULT_INTERFACE");
        String reportMaxSize = getGlobalValue(luType+".D2D_REPORT_MAX_SIZE");

        String tableName = "ref_d2d_entity_summary";

        Db.Rows results = null;
        
        /**
        if (!reportInterface.equalsIgnoreCase("fabric")){
            tableName = "d2d_entity_summary";
        }

        */

        String sqlCmd = String.format(
            "select * " +
                "from %s.%s where 1=1 ", reportSchema, tableName);

        if(executionId != null && !executionId.isEmpty()){
            sqlCmd += String.format(" and EXECUTION_ID = '%s'", executionId);
        }
        sqlCmd += " order by iid";

        sqlCmd += " limit " + reportMaxSize;

        results = db(reportInterface).fetch(sqlCmd);

        return normalizeResults(results);

    } 
    @webService(path = "d2d/table-summary", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetTableSummary(@param(required = false) String luType, String executionId, String iid) throws SQLException{
        if(luType == null){
            luType = "D2D";
        }
        String reportSchema = getGlobalValue(luType+".D2D_LOAD_SCHEMA");
        String reportInterface = getGlobalValue(luType+".D2D_RESULT_INTERFACE");
        String tableName = "ref_d2d_table_summary";

        Db.Rows results = null;
        
        /**
        if (!reportInterface.equalsIgnoreCase("fabric")){
            tableName = "d2d_table_summary";
        }
        */

        String sqlCmd = String.format(
            "select * " +
                "from %s.%s where 1=1 ", reportSchema, tableName);

        if(executionId != null && !executionId.isEmpty()){
            sqlCmd += String.format(" and EXECUTION_ID = '%s'", executionId);
        }
        if(iid != null && !iid.isEmpty()){
            sqlCmd += String.format(" and IID = '%s'", iid);
        }
        sqlCmd += " order by iid";

        results = db(reportInterface).fetch(sqlCmd);

        return normalizeResults(results);

    } 
    @webService(path = "d2d/record-summary", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetRecordSummary(@param(required = false) String luType, String executionId, String iid, String sourceTableName) throws SQLException{
        if(luType == null){
            luType = "D2D";
        }
        String reportSchema = getGlobalValue(luType+".D2D_LOAD_SCHEMA");
        String reportInterface = getGlobalValue(luType+".D2D_RESULT_INTERFACE");
        String tableName = "ref_d2d_record_summary";

        Db.Rows results = null;
        
        /**
        if (!reportInterface.equalsIgnoreCase("fabric")){
            tableName = "d2d_record_summary";
        }
        */

        String sqlCmd = String.format(
            "select * " +
                "from %s.%s where 1=1 ", reportSchema, tableName);

        if(executionId != null && !executionId.isEmpty()){
            sqlCmd += String.format(" and EXECUTION_ID = '%s'", executionId);
        }
        if(iid != null && !iid.isEmpty()){
            sqlCmd += String.format(" and IID = '%s'", iid);
        }
        if(sourceTableName != null && !sourceTableName.isEmpty()){
            sqlCmd += String.format(" and SOURCE_TABLE_NAME = '%s'", sourceTableName);
        }
        sqlCmd += " order by iid";

        results = db(reportInterface).fetch(sqlCmd);

        return normalizeResults(results);

    } 
    @webService(path = "d2d/field-summary", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetFieldSummary(@param(required = false) String luType, String executionId, String iid, String sourceTableName, String customizedKey) throws SQLException{
        if(luType == null){
            luType = "D2D";
        }
        String reportSchema = getGlobalValue(luType+".D2D_LOAD_SCHEMA");
        String reportInterface = getGlobalValue(luType+".D2D_RESULT_INTERFACE");
        String tableName = "ref_d2d_field_summary";

        Db.Rows results = null; 
        
        /**
        if (!reportInterface.equalsIgnoreCase("fabric")){
            tableName = "d2d_field_summary";
        }
        */

        String sqlCmd = String.format(
            "select * " +
                "from %s.%s where 1=1 ", reportSchema, tableName);

        if(executionId != null && !executionId.isEmpty()){
            sqlCmd += String.format(" and EXECUTION_ID = '%s'", executionId);
        }
        if(iid != null && !iid.isEmpty()){
            sqlCmd += String.format(" and IID = '%s'", iid);
        }
        if(sourceTableName != null && !sourceTableName.isEmpty()){
            sqlCmd += String.format(" and SOURCE_TABLE_NAME = '%s'", sourceTableName);
        }
        if(customizedKey != null && !customizedKey.isEmpty()){
            sqlCmd += String.format(" and CUSTOMIZED_KEY = '%s'", customizedKey);
        }
        sqlCmd += " order by iid";

        results = db(reportInterface).fetch(sqlCmd);
       
        return normalizeResults(results);
    } 
    @webService(path = "d2d/excel-summary", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetExcelSummary(@param(required=true) String exec_id, @param(required = false) String luType) throws SQLException{
        if(luType == null){
            luType = "D2D";
        }

        String reportSchema = getGlobalValue(luType+".D2D_LOAD_SCHEMA");
        String reportInterface = getGlobalValue(luType+".D2D_RESULT_INTERFACE");
       
        String command = "broadway D2D.bwPrintD2Dv2Result strict=?, luName=?, taskExecutionID=?, file_type=?";
        
        Db.Rows results = fabric().fetch(command, new Object[]{false, luType, exec_id, "xlsx"});
        String filename = results.firstValue().toString();
        Map<String, String> response = new HashMap<String, String>();
        response.put("filename", filename);
        return response;
    } 
    private static List<Object> normalizeResults(Db.Rows results){
        List<Object> normalizedResults = new ArrayList<>();
        for(Db.Row row : results){
            Map<String, Object> normalizedRow = new LinkedHashMap<>(); 
            row.entrySet().forEach(e->{
                normalizedRow.put(e.getKey().toUpperCase(), e.getValue());
            });
            normalizedResults.add(normalizedRow);
        } 
        return normalizedResults;
    }
    
    @webService(path = "d2d/get-schema", verb = {MethodType.GET}, version = "1", isRaw = false, isCustomPayload = false, produce = {Produce.XML, Produce.JSON}, elevatedPermission = true)
	public static Object wsGetSchema(@param(required=true) String interfaceName, @param(required = false) String environment) throws SQLException{
        if(environment == null || environment.isEmpty()){
            environment = "_dev";
        }

        DbInterface dbInterface = (DbInterface)InterfacesManager.getInstance().getInterface(interfaceName, environment);
        String schema = dbInterface.dbScheme;
        return schema;
    } 
}
