/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.D2D;

import java.util.*;
import java.util.stream.Collectors;
import java.sql.*;

import com.k2view.cdbms.interfaces.FabricInterface;
import com.k2view.cdbms.interfaces.InterfacesUtils;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.fabric.common.ClusterUtil;
import com.k2view.fabric.common.io.basic.IoSimpleRow;
import org.json.JSONObject;

import static com.k2view.cdbms.shared.user.UserCode.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.broadway.lookahead.LookaheadDescribed;
import com.k2view.broadway.util.DescribedIoResult;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.fabric.events.*;
import com.k2view.fabric.fabricdb.datachange.TableDataChange;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

@SuppressWarnings({ "unused", "DefaultAnnotationParam" })
public class SharedLogic {

    // Added Below Line to split based on the Global Delimitter
    static String DELIMITTER;
    static {
        DELIMITTER = '\\' + getLuType().ludbGlobals.get("D2D_CONF_SEPERATOR");
    }

    @out(name = "result", type = Object.class, desc = "")
    @out(name = "customizedKeyComparison", type = String.class, desc = "")
    public static Object fnD2DCompare(Object row, String luName, String Source_Transformation_Function_Name,
            String Target_Transformation_Function_Name, String customizedKeyComparison,
            String source_columns_to_Ignore_null, String target_columns_to_Ignore_null) throws Exception {
        Map<String, String> sourceMapTransformationFunction = getFunction2Column(Source_Transformation_Function_Name);
        Map<String, String> targetMapTransformationFunction = getFunction2Column(Target_Transformation_Function_Name);

        LUType luType = LUTypeFactoryImpl.getInstance().getTypeByName(luName);

        Map<String, String> sourceMap = new HashMap<>();
        Map<String, String> targetMap = new HashMap<>();

        StringBuilder stringBuilder = new StringBuilder();
        IoSimpleRow ioSimpleRow = (IoSimpleRow) row;

        for (Map.Entry<String, Object> entryRow : ioSimpleRow.entrySet()) {
            String columnName;
            Object columnValue = entryRow.getValue();
            String customFunctionName = null;

            if (entryRow.getKey().startsWith("source")) {
                columnName = entryRow.getKey().replace("source_", "");
                sourceMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                if (sourceMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = sourceMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                sourceMap.put(columnName, columnValue == null ? null : columnValue.toString());
            } else if (entryRow.getKey().startsWith("target")) {
                columnName = entryRow.getKey().replace("target_", "");
                targetMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                if (targetMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = targetMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                targetMap.put(columnName, columnValue == null ? null : columnValue.toString());
            }
        }

        List<String> tctin = new ArrayList<>();
        for (String column : target_columns_to_Ignore_null.split(DELIMITTER)) {
            tctin.add(column.toUpperCase());
        }

        List<String> sctin = new ArrayList<>();
        for (String column : source_columns_to_Ignore_null.split(DELIMITTER)) {
            sctin.add(column.toUpperCase());
        }

        Map<String, Map<String, String>> compareResult = new HashMap<>();
        sourceMap.forEach((key, value) -> {
            if (key.contains("_k2orig"))
                return;
            Map<String, String> columnResult = new HashMap<>();
            columnResult.put("column_name", key);
            columnResult.put("source_value", value);
            columnResult.put("target_value", targetMap.get(key));
            columnResult.put("source_column_orig_value", sourceMap.get(key + "_k2orig"));
            columnResult.put("target_column_orig_value", targetMap.get(key + "_k2orig"));
            Object targetValue = targetMap.get(key);
            if ((value == null && targetValue == null) || (value != null && value.equals(targetValue))
                    || (value != null && targetValue == null && tctin.contains(key.toUpperCase()))
                    || (value == null && targetValue != null && sctin.contains(key))) {
                columnResult.put("result", "Match");
            } else {
                columnResult.put("result", "Mismatch");
            }
            compareResult.put(key, columnResult);
        });

        JSONObject jsonObject = new JSONObject();
        for (String key : customizedKeyComparison.split(DELIMITTER)) {
            jsonObject.put(key, sourceMap.get(key.toUpperCase()));
        }

        return new Object[] { compareResult,
                "[" + jsonObject.toString().substring(1, jsonObject.toString().length() - 1) + "]" };
    }

    @out(name = "result", type = Object.class, desc = "")
    @out(name = "customizedKeyComparison", type = String.class, desc = "")
    public static Object fnD2Dv2Compare(Object row, String luName, String Source_Transformation_Function_Name,
            String Target_Transformation_Function_Name, String customizedKeyComparison,
            String source_columns_to_Ignore_null, String target_columns_to_Ignore_null) throws Exception {
        Map<String, String> sourceMapTransformationFunction = getFunction2Column(Source_Transformation_Function_Name);
        Map<String, String> targetMapTransformationFunction = getFunction2Column(Target_Transformation_Function_Name);

        LUType luType = LUTypeFactoryImpl.getInstance().getTypeByName(luName);

        Map<String, String> sourceMap = new HashMap<>();
        Map<String, String> targetMap = new HashMap<>();

        StringBuilder stringBuilder = new StringBuilder();
        LinkedHashMap<String, Object> ioSimpleRow = (LinkedHashMap) row;

        for (Map.Entry<String, Object> entryRow : ioSimpleRow.entrySet()) {
            String columnName;
            Object columnValue = entryRow.getValue();
            String customFunctionName = null;

            if (entryRow.getKey().startsWith("source")) {
                columnName = entryRow.getKey().replace("source_", "");
                sourceMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                if (sourceMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = sourceMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                sourceMap.put(columnName, columnValue == null ? null : columnValue.toString());
            } else if (entryRow.getKey().startsWith("target")) {
                columnName = entryRow.getKey().replace("target_", "");
                targetMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                if (targetMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = targetMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                targetMap.put(columnName, columnValue == null ? null : columnValue.toString());
            }
        }

        List<String> tctin = new ArrayList<>();
        for (String column : target_columns_to_Ignore_null.split(DELIMITTER)) {
            tctin.add(column.toUpperCase());
        }

        List<String> sctin = new ArrayList<>();
        for (String column : source_columns_to_Ignore_null.split(DELIMITTER)) {
            sctin.add(column.toUpperCase());
        }

        Map<String, Map<String, String>> compareResult = new HashMap<>();
        sourceMap.forEach((key, value) -> {
            if (key.contains("_k2orig"))
                return;

            Map<String, String> columnResult = new HashMap<>();
            columnResult.put("column_name", key);
            columnResult.put("source_value", value);
            columnResult.put("target_value", targetMap.get(key));
            columnResult.put("source_column_orig_value", sourceMap.get(key + "_k2orig"));
            columnResult.put("target_column_orig_value", targetMap.get(key + "_k2orig"));
            Object targetValue = targetMap.get(key);
            if ((value == null && targetValue == null) || (value != null && value.equals(targetValue))
                    || (value != null && targetValue == null && tctin.contains(key.toUpperCase()))
                    || (value == null && targetValue != null && sctin.contains(key))) {
                columnResult.put("result", "Match");
            } else {
                columnResult.put("result", "Mismatch");
            }
            compareResult.put(key, columnResult);
        });

        JSONObject jsonObject = new JSONObject();
        for (String key : customizedKeyComparison.split(DELIMITTER)) {

            if (sourceMap.get(key.toUpperCase()) == null && sourceMap.get(key.toLowerCase()) != null) {
                jsonObject.put(key, sourceMap.get(key.toLowerCase()));
            } else if (sourceMap.get(key.toUpperCase()) != null && sourceMap.get(key.toLowerCase()) == null) {
                jsonObject.put(key, sourceMap.get(key.toUpperCase()));
            }
            // jsonObject.put(key, sourceMap.get(key.toUpperCase()));
        }

        return new Object[] { compareResult,
                "[" + jsonObject.toString().substring(1, jsonObject.toString().length() - 1) + "]" };
    }

    @out(name = "result", type = Object.class, desc = "")
    @out(name = "customizedKeyComparison", type = String.class, desc = "")
    public static Object fnF2FCompareSourceNTarget(Object row, String customizedKeyComparison,
            String source_columns_to_Ignore_null, String target_columns_to_Ignore_null, String sourceEnv,
            String targetEnv, String Source_Transformation_Function_Name, String Target_Transformation_Function_Name)
            throws Exception {
        Map<String, String> sourceMapTransformationFunction = getFunction2Column(Source_Transformation_Function_Name);
        Map<String, String> targetMapTransformationFunction = getFunction2Column(Target_Transformation_Function_Name);

        LUType luType = getLuType();

        Map<String, Object> sourceMap = new HashMap<>();
        Map<String, Object> targetMap = new HashMap<>();

        StringBuilder stringBuilder = new StringBuilder();
        LinkedHashMap<String, Object> ioSimpleRow = (LinkedHashMap) row;

        for (Map.Entry<String, Object> entryRow : ioSimpleRow.entrySet()) {
            String columnName;
            Object columnValue = entryRow.getValue();
            String customFunctionName = null;

            if (entryRow.getKey().startsWith(sourceEnv)) {
                columnName = entryRow.getKey().replaceFirst(sourceEnv + "_", "");
                sourceMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                // sourceMap.put(columnName + "_k2orig", columnValue);
                if (sourceMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = sourceMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                sourceMap.put(columnName, columnValue == null ? null : columnValue.toString());
                // sourceMap.put(columnName, columnValue);
            } else if (entryRow.getKey().startsWith(targetEnv)) {
                columnName = entryRow.getKey().replaceFirst(targetEnv + "_", "");
                targetMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                // targetMap.put(columnName + "_k2orig", columnValue);
                if (targetMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = targetMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                targetMap.put(columnName, columnValue == null ? null : columnValue.toString());
                // targetMap.put(columnName, columnValue);
            }
        }

        List<String> tctin = new ArrayList<>();
        for (String column : target_columns_to_Ignore_null.split(DELIMITTER)) {
            tctin.add(column.toUpperCase());
        }

        List<String> sctin = new ArrayList<>();
        for (String column : source_columns_to_Ignore_null.split(DELIMITTER)) {
            sctin.add(column.toUpperCase());
        }

        Map<String, Map<String, Object>> compareResult = new HashMap<>();
        sourceMap.forEach((key, value) -> {
            if (key.contains("_k2orig"))
                return;

            Map<String, Object> columnResult = new HashMap<>();
            columnResult.put("column_name", key);
            columnResult.put("source_value", value);
            columnResult.put("target_value", targetMap.get(key));
            columnResult.put("source_column_orig_value", sourceMap.get(key + "_k2orig"));
            columnResult.put("target_column_orig_value", targetMap.get(key + "_k2orig"));
            Object targetValue = targetMap.get(key);
            if ((value == null && targetValue == null) || (value != null && value.equals(targetValue))
                    || (value != null && targetValue == null && tctin.contains(key.toUpperCase()))
                    || (value == null && targetValue != null && sctin.contains(key))) {
                columnResult.put("result", "Match");
            } else {
                columnResult.put("result", "Mismatch");
            }
            compareResult.put(key, columnResult);
        });
        targetMap.forEach((key, value) -> {
            if (key.contains("_k2orig"))
                return;

            Map<String, Object> columnResult = new HashMap<>();
            columnResult.put("column_name", key);
            columnResult.put("source_value", sourceMap.get(key));
            columnResult.put("target_value", value);
            columnResult.put("source_column_orig_value", sourceMap.get(key + "_k2orig"));
            columnResult.put("target_column_orig_value", targetMap.get(key + "_k2orig"));
            Object srcValue = sourceMap.get(key);
            if ((value == null && srcValue == null) || (value != null && value.equals(srcValue))
                    || (value != null && srcValue == null && sctin.contains(key.toUpperCase()))
                    || (value == null && srcValue != null && tctin.contains(key))) {
                columnResult.put("result", "Match");
            } else {
                columnResult.put("result", "Mismatch");
            }
            compareResult.put(key, columnResult);
        });
        JSONObject jsonObject = new JSONObject();
        for (String key : customizedKeyComparison.split(DELIMITTER)) {

            if (sourceMap.get(key.toUpperCase()) == null && sourceMap.get(key.toLowerCase()) != null) {
                jsonObject.put(key, sourceMap.get(key.toLowerCase()));
            } else if (sourceMap.get(key.toUpperCase()) != null && sourceMap.get(key.toLowerCase()) == null) {
                jsonObject.put(key, sourceMap.get(key.toUpperCase()));
            } else if (targetMap.get(key.toUpperCase()) != null && targetMap.get(key.toLowerCase()) == null) {
                jsonObject.put(key, targetMap.get(key.toUpperCase()));
            } else if (targetMap.get(key.toUpperCase()) != null && targetMap.get(key.toLowerCase()) == null) {
                jsonObject.put(key, targetMap.get(key.toUpperCase()));
            }
            // jsonObject.put(key, sourceMap.get(key.toUpperCase()));
        }

        return new Object[] { compareResult,
                "[" + jsonObject.toString().substring(1, jsonObject.toString().length() - 1) + "]" };
    }

    @out(name = "select", type = String.class, desc = "")
    public static String fnD2DSourceInTarget(String luName, String sourceTableName,
            String source_Transformation_Function_Name, String targetTableName,
            String target_Transformation_Function_Name, String customizedKeyComparison, String excludedColumnsNames)
            throws Exception {
        List<String> excludedColumnsNamesList = new ArrayList<>();
        for (String columnToIgnore : excludedColumnsNames.split(DELIMITTER)) {
            excludedColumnsNamesList.add(columnToIgnore.toUpperCase());
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder prefix = new StringBuilder();
        LUType luType = getLuType();
        Map<String, LudbColumn> columnsMap = luType.ludbObjects.get(sourceTableName).getLudbColumnMap();
        columnsMap.values().stream()
                .filter(columnName -> !excludedColumnsNamesList.contains(columnName.getName().toUpperCase()))
                .forEach(columnName -> {
                    columns.append(prefix)
                            .append(" source." + columnName.getName() + " as source_" + columnName.getName()
                                    + ", target." + columnName.getName() + " as target_" + columnName.getName());
                    if (prefix.length() == 0)
                        prefix.append(",");
                });

        String select = "select %s from %s source, %s target on %s";
        StringBuilder stringBuilder = new StringBuilder();
        prefix.setLength(0);
        for (String customizedKey : customizedKeyComparison.split(DELIMITTER)) {
            stringBuilder.append(prefix + " source." + customizedKey + " = " + "target." + customizedKey);
            if (prefix.length() == 0)
                prefix.append(" and ");
        }

        return String.format(select, columns.toString(), sourceTableName, targetTableName, stringBuilder.toString());
    }

    @out(name = "select", type = String.class, desc = "")
    public static String fnD2DSourceNotInTarget(String luName, String sourceTableName, String targetTableName,
            String customizedKeyComparison, String searchIND) throws Exception {
        if ("T".equalsIgnoreCase(searchIND)) {
            String tmpSourceTable = sourceTableName;
            sourceTableName = targetTableName;
            targetTableName = tmpSourceTable;
        }

        String select = "select * from %s source where not exists (select 1 from %s target where %s)";
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        String prefix = "";
        String prefix2 = "";
        for (String customizedKey : customizedKeyComparison.split(DELIMITTER)) {
            stringBuilder.append(prefix2 + " source." + customizedKey + " = " + "target." + customizedKey);
            columns.append(prefix + " source." + customizedKey);
            prefix = " , ";
            prefix2 = " and ";
        }

        return String.format(select, sourceTableName, targetTableName, stringBuilder.toString());
    }

    public static String fnD2DGetSource(String luName, String sourceTableName, String customizedKeyComparison,
            String source_Transformation_Function_Name, String excludedColumnsNames) throws Exception {
        List<String> excludedColumnsNamesList = new ArrayList<>();
        for (String columnToIgnore : excludedColumnsNames.split(DELIMITTER)) {
            excludedColumnsNamesList.add(columnToIgnore.toUpperCase());
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder prefix = new StringBuilder();
        LUType luType = LUType.getTypeByName(luName);
        Map<String, LudbColumn> columnsMap = luType.ludbObjects.get(sourceTableName).getLudbColumnMap();
        columnsMap.values().stream()
                .filter(columnName -> !excludedColumnsNamesList.contains(columnName.getName().toUpperCase()))
                .forEach(columnName -> {
                    columns.append(prefix).append(" " + columnName.getName() + " as source_" + columnName.getName());
                    if (prefix.length() == 0)
                        prefix.append(",");
                });

        String select = "select %s from %s where %s ";
        StringBuilder stringBuilder = new StringBuilder();
        prefix.setLength(0);
        for (String customizedKey : customizedKeyComparison.split(DELIMITTER)) {
            stringBuilder.append(prefix + "  " + customizedKey + " =? ");
            if (prefix.length() == 0)
                prefix.append(" and ");
        }

        return String.format(select, columns.toString(), sourceTableName, stringBuilder);
    }

    public static String fnD2DGetTarget(String luName, String targetTableName,
            String target_Transformation_Function_Name, String customizedKeyComparison, String excludedColumnsNames)
            throws Exception {
        List<String> excludedColumnsNamesList = new ArrayList<>();
        for (String columnToIgnore : excludedColumnsNames.split(DELIMITTER)) {
            excludedColumnsNamesList.add(columnToIgnore.toUpperCase());
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder prefix = new StringBuilder();
        // LUType luType = getLuType();
        LUType luType = LUType.getTypeByName(luName);
        Map<String, LudbColumn> columnsMap = luType.ludbObjects.get(targetTableName).getLudbColumnMap();
        columnsMap.values().stream()
                .filter(columnName -> !excludedColumnsNamesList.contains(columnName.getName().toUpperCase()))
                .forEach(columnName -> {
                    columns.append(prefix).append(" " + columnName.getName() + " as target_" + columnName.getName());
                    if (prefix.length() == 0)
                        prefix.append(",");
                });

        String select = "select %s from %s where %s ";
        // String select = "select %s from %s target ";
        StringBuilder stringBuilder = new StringBuilder();
        prefix.setLength(0);
        for (String customizedKey : customizedKeyComparison.split(DELIMITTER)) {
            stringBuilder.append(prefix + " " + customizedKey + " =? ");
            if (prefix.length() == 0)
                prefix.append(" and ");
        }

        return String.format(select, columns.toString(), targetTableName, stringBuilder);
    }

    private static Map<String, String> getFunction2Column(String transformation_Function_Name) {
        Map<String, String> map = new HashMap<>();
        if ("".equals(transformation_Function_Name))
            return map;
        for (String functionNdColumn : transformation_Function_Name.split(DELIMITTER)) {
            String[] functionNdColumnArr = functionNdColumn.split(":");
            map.put(functionNdColumnArr[0].toUpperCase(), functionNdColumnArr[1]);
        }

        return map;
    }

    private static Object getTransformedValue(String customFunctionName, LUType luType, Object columnValue)
            throws ReflectiveOperationException, InterruptedException, SQLException {
        String luName = getLuType().luName;
        if (customFunctionName != null) {
            Db.Row row = fabric()
                    .fetch(String.format("Broadway %s.%s value=?", luName, customFunctionName), columnValue).firstRow();
            columnValue = row.get("value");
            // columnValue = luType.invokeFunction(customFunctionName, null, new
            // Object[]{columnValue});
        }
        return columnValue == null ? null : columnValue.toString();
    }

    @out(name = "ignoreMatchIND", type = Boolean.class, desc = "")
    public static Boolean fnD2DGetIgnoreMatchIND(String match) throws Exception {
        if ("Match".equals(match) && Boolean.parseBoolean(
                fabric().fetch(String.format("set %s.IGNOREMATCH", getLuType().luName)).firstValue().toString())) {
            return false;
        } else {
            return true;
        }
    }

    @out(name = "customizedKey", type = String.class, desc = "")
    public static String fnD2DGetCustomizedKey(Map<String, Object> rowMap, String customizedKey) throws Exception {
        JSONObject jsonObject = new JSONObject();
        String[] cusKeyArr = customizedKey.split(DELIMITTER);
        for (String cusKey : cusKeyArr) {
            Object cusKeyVal = rowMap.get(cusKey.toUpperCase());
            jsonObject.put(cusKey, cusKeyVal == null ? "" : cusKeyVal.toString());
        }

        return "[" + jsonObject.toString().substring(1, jsonObject.toString().length() - 1) + "]";
    }

    @out(name = "result", type = Object.class, desc = "")
    public static Object fnD2DGetD2DConfiguration() throws Exception {

        return getTranslationsData("D2D.D2DConfig");
    }

    @desc("Get Resporce FIle of LU")
    @out(name = "result", type = Object.class, desc = "")
    public static Object fnLoadFromResource(String path) throws Exception {
        return loadResource(path);
    }

    @out(name = "tablePopDetails", type = List.class, desc = "")
    public static List<Map<String, String>> fnGetTablePopulationDetails(String luName, String tableName)
            throws Exception {
        List<Map<String, String>> tablePopDetails = new ArrayList<>();
        List<TablePopulationObject> popObj = ((TableObject) (LUTypeFactoryImpl.getInstance()
                .getTypeByName(luName)).ludbObjects.get(tableName)).getAllTablePopulationObjects();
        popObj.forEach(obj -> {
            Map<String, String> pop = new HashMap<>();
            pop.put("popName", obj.getPopulationName());
            pop.put("popOrder", obj.gettablePopulationOrder() + "");
            pop.put("luTable", obj.getTableObject().schemaAndTableName);
            tablePopDetails.add(pop);
        });
        return tablePopDetails;
    }

    @desc("")
    @out(name = "output1", type = String.class, desc = "")
    public static String fnD2DFilter(@desc("") String iid) throws Exception {
        if (iid.startsWith("D2D_")) {
            iid = iid.replace("D2D_", "");
        }
        return iid;
    }

    @out(name = "luType", type = String.class, desc = "")
    public static String fnGetLuType() throws Exception {
        return getLuType().luName;
    }

    @desc("")
    @out(name = "SessionName", type = String.class, desc = "")
    public static String createFabricSession(@desc("") String sessionName) throws Exception {
        openFabricSession(sessionName);
        return sessionName;

    }

    @out(name = "result", type = Object.class, desc = "")
    @out(name = "customizedKeyComparison", type = String.class, desc = "")
    public static Object fnF2Fv2Compare(Object row, String customizedKeyComparison,
            String source_columns_to_Ignore_null, String target_columns_to_Ignore_null, String sourceEnv,
            String targetEnv, String Source_Transformation_Function_Name, String Target_Transformation_Function_Name)
            throws Exception {
        Map<String, String> sourceMapTransformationFunction = getFunction2Column(Source_Transformation_Function_Name);
        Map<String, String> targetMapTransformationFunction = getFunction2Column(Target_Transformation_Function_Name);

        LUType luType = getLuType();

        Map<String, Object> sourceMap = new HashMap<>();
        Map<String, Object> targetMap = new HashMap<>();

        StringBuilder stringBuilder = new StringBuilder();
        LinkedHashMap<String, Object> ioSimpleRow = (LinkedHashMap) row;

        for (Map.Entry<String, Object> entryRow : ioSimpleRow.entrySet()) {
            String columnName;
            Object columnValue = entryRow.getValue();
            String customFunctionName = null;

            if (entryRow.getKey().startsWith(sourceEnv)) {
                columnName = entryRow.getKey().replaceFirst(sourceEnv + "_", "");
                sourceMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                // sourceMap.put(columnName + "_k2orig", columnValue);
                if (sourceMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = sourceMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                sourceMap.put(columnName, columnValue == null ? null : columnValue.toString());
                // sourceMap.put(columnName, columnValue);
            } else if (entryRow.getKey().startsWith(targetEnv)) {
                columnName = entryRow.getKey().replaceFirst(targetEnv + "_", "");
                targetMap.put(columnName + "_k2orig", columnValue != null ? columnValue.toString() : null);
                // targetMap.put(columnName + "_k2orig", columnValue);
                if (targetMapTransformationFunction.containsKey(columnName)) {
                    customFunctionName = targetMapTransformationFunction.get(columnName);
                    columnValue = getTransformedValue(customFunctionName, luType, columnValue);
                }
                targetMap.put(columnName, columnValue == null ? null : columnValue.toString());
                // targetMap.put(columnName, columnValue);
            }
        }

        List<String> tctin = new ArrayList<>();
        for (String column : target_columns_to_Ignore_null.split(DELIMITTER)) {
            tctin.add(column.toUpperCase());
        }

        List<String> sctin = new ArrayList<>();
        for (String column : source_columns_to_Ignore_null.split(DELIMITTER)) {
            sctin.add(column.toUpperCase());
        }

        Map<String, Map<String, Object>> compareResult = new HashMap<>();
        sourceMap.forEach((key, value) -> {
            if (key.contains("_k2orig"))
                return;

            Map<String, Object> columnResult = new HashMap<>();
            columnResult.put("column_name", key);
            columnResult.put("source_value", value);
            columnResult.put("target_value", targetMap.get(key));
            columnResult.put("source_column_orig_value", sourceMap.get(key + "_k2orig"));
            columnResult.put("target_column_orig_value", targetMap.get(key + "_k2orig"));
            Object targetValue = targetMap.get(key);
            if ((value == null && targetValue == null) || (value != null && value.equals(targetValue))
                    || (value != null && targetValue == null && tctin.contains(key.toUpperCase()))
                    || (value == null && targetValue != null && sctin.contains(key))) {
                columnResult.put("result", "Match");
            } else {
                columnResult.put("result", "Mismatch");
            }
            compareResult.put(key, columnResult);
        });

        JSONObject jsonObject = new JSONObject();
        for (String key : customizedKeyComparison.split(DELIMITTER)) {

            if (sourceMap.get(key.toUpperCase()) == null && sourceMap.get(key.toLowerCase()) != null) {
                jsonObject.put(key, sourceMap.get(key.toLowerCase()));
            } else if (sourceMap.get(key.toUpperCase()) != null && sourceMap.get(key.toLowerCase()) == null) {
                jsonObject.put(key, sourceMap.get(key.toUpperCase()));
            }
            // jsonObject.put(key, sourceMap.get(key.toUpperCase()));
        }

        return new Object[] { compareResult,
                "[" + jsonObject.toString().substring(1, jsonObject.toString().length() - 1) + "]" };
    }

    @out(name = "schema", type = String.class, desc = "")
    public static String getSchemaFromInterface(String interfaceName, String env) throws Exception {
        // Map<String, String> props = getCustomProperties(interfaceName);
        // Map<String, String> props = InterfacesUtils.
        // String schema = getConnection(interfaceName).getMetaData().getURL();

        DbInterface dbInterface = (DbInterface) InterfacesManager.getInstance().getInterface(interfaceName,
                env.isEmpty() ? "_dev" : env);
        String database = dbInterface.dbScheme;
        FabricInterface interfaceDetails = InterfacesManager.getInstance().getInterface(interfaceName, "_dev");
        // ((DbInterface) interfaceDetails);

        return database;
    }

    @type(RootFunction)
    @out(name = "IID", type = String.class, desc = "")
    @out(name = "MAX_DC_UPDATE", type = Long.class, desc = "")
    public static void fnIIDFConsMsgsD2D(String IID) throws Exception {
        UserCode.yield(new Object[] { IID, 0 });
    }

    @out(name = "output", type = Object.class, desc = "")
    public static Object convertTableMetadata(Object inputData) throws Exception {
        ArrayList<Map<String, Object>> input = new ArrayList<>();
        Map<String, Map<String, Object>> tables = new HashMap<>();
        Iterator itr = null;
        if (inputData instanceof LookaheadDescribed) {
            itr = ((LookaheadDescribed<?>) inputData).iterator();
        } else if (inputData instanceof DescribedIoResult) {
            itr = ((DescribedIoResult) inputData).iterator();
        }
        if (itr != null) {
            while (itr.hasNext()) {
                IoSimpleRow ioSimpleRow = (IoSimpleRow) itr.next();
                Map<String, Object> row = new HashMap<>();
                ioSimpleRow.entrySet().forEach(stringObjectEntry -> {
                    row.put(stringObjectEntry.getKey(), stringObjectEntry.getValue());
                });
                input.add(row);
            }

            for (Map<String, Object> entry : input) {
                String schema = (String) entry.get("schema");
                String table = ((String) entry.get("table")).toLowerCase();
                String column = ((String) entry.get("column")).toLowerCase();
                boolean isPk = (boolean) entry.get("pk");
                if (table.startsWith("_k2")) {
                    continue;
                }
                tables.putIfAbsent(table, new HashMap<>());
                Map<String, Object> tableData = tables.get(table);
                tableData.put("Lu_Name", schema);
                tableData.put("Source_Table_Name", table);
                tableData.put("Source_Transformation_Function_Name", null);
                tableData.put("source_columns_to_Ignore_null", null);
                tableData.put("Target_Table_Name", table);
                tableData.put("Target_Transformation_Function_Name", null);
                tableData.put("target_columns_to_Ignore_null", null);
                tableData.put("Customized_Key_Comparison",
                        tableData.getOrDefault("Customized_Key_Comparison", "")
                                + (isPk ? (tableData.getOrDefault("Customized_Key_Comparison", "").equals("") ? column
                                        : "|" + column) : ""));
                tableData.put("Mismatch_Columns", null);
                tableData.put("Excluded_Columns_Names", null);
                tableData.put("Active", "TRUE");
                tableData.put("Mode", "EXACT");
                tableData.put("Excluded_Rows_Sql", null);
            }
        }
        // return new ArrayList<>(tables.values()).toArray();
        return tables.values();
    }

    @out(name = "output", type = Object.class, desc = "")
    public static Object filterTableMetadata(List<Map<String, Object>> tableList) throws Exception {
        List<Object> filteredTableList = tableList.stream()
                .filter(entry -> entry.get("Active") != null && !entry.get("Active").equals(false))
                .filter(entry -> entry.get("Target_Table_Name") != null
                        && !entry.get("Target_Table_Name").toString().isEmpty())
                .filter(entry -> entry.get("Source_Table_Name") != null
                        && !entry.get("Source_Table_Name").toString().isEmpty())
                .collect(Collectors.toList());
        return filteredTableList;
    }

    public static Object mergeTableMetadata(Object mtableList, Object discoveredList) throws Exception {
        List<Map<String, Object>> mtList = (List<Map<String, Object>>) mtableList;
        List<Map<String, Object>> diList = new ArrayList<>((Collection) discoveredList);
        Map<String, Map<String, Object>> merged = new HashMap<>();

        for (Map<String, Object> entry : diList) {
            String key = entry.get("Lu_Name") + "|" + entry.get("Source_Table_Name");
            merged.put(key, entry);
        }

        for (Map<String, Object> entry : mtList) {
            String key = entry.get("Lu_Name") + "|" + entry.get("Source_Table_Name");
            merged.put(key, entry); // Override if exists
        }

        return new ArrayList<>(merged.values());
    }

}
