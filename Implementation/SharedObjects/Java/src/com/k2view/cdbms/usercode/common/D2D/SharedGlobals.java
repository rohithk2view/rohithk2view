package com.k2view.cdbms.usercode.common.D2D;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.category;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.desc;

public class SharedGlobals {
	@category("D2D")
	public static String IGNOREMATCH = "false";
	@desc("Track the execution Id, Inside a Get Session, No default value")
	@category("D2D")
	public static String D2D_EXEC_ID = "";
	@desc("SOURCE Tables ENVIRONMENT")
	@category("D2D")
	public static String D2D_SOURCE_ENV = "SRC";
	@desc("DESTINATION Tables ENVIRONMENT")
	@category("D2D")
	public static String D2D_TARGET_ENV = "TAR";
	@desc("Location where to store the D2D result tables. To store in Fabric's Common table, use fabric")
	@category("D2D")
	public static String D2D_RESULT_INTERFACE = "POSTGRESQL_ADMIN";

	@desc("Name of the schema where the result tables are stored. When stored in fabric Common tables, use the schema name specified in the Common table properti")
	@category("D2D")
	public static String D2D_LOAD_SCHEMA = "d2dref";

	@desc("D2DConfiguration (MTable) column Delimitter.")
	@category("D2D")
	public static String D2D_CONF_SEPERATOR = "|";

    @desc("D2D Execution Mode: D2D (D2D LU Based), Direct (TDM LU Based, using TAR keys to select data in DBs), Extract (TDM LU Based, loading LUI from source and target into Fabric), F2F (Using Remote and/or Local Fabric Connections to compare LUI to LUI)")
	@category("D2D")
	public static String D2D_EXEC_MODE = "D2D";

    @desc("SRC LU Name or Schema Name for F2F Mode: if value is 'Interface' or empty - D2D will try to get it from Interface as defined in D2D_FABRIC_INTERFACE_SRC, otherwise use it from global")
	@category("D2D")
	public static String D2D_F2F_SCHEMA_SRC = "k2view_d2d_src";

    @desc("TAR LU Name or Schema Name for F2F Mode: if value is 'Interface' or empty - D2D will try to get it from Interface as defined in D2D_FABRIC_INTERFACE_TAR, otherwise use it from global")
	@category("D2D")
	public static String D2D_F2F_SCHEMA_TAR = "k2view_d2d_tar";

    @desc("D2D sync mode, FORCE, OFF or ON")
	@category("D2D")
	public static String D2D_SYNC_MODE = "OFF";

    @desc("D2D Instance TTL ")
	@category("D2D")
	public static String D2D_INSTANCE_TTL = "0";

	@desc("Fabric remote interface name for Source Fabric Clsuter (for fabric to fabric mode)")
	@category("D2D")
	public static String D2D_FABRIC_INTERFACE_SRC = "RemoteCassandra";

	@desc("Fabric remote interface name for Target Fabric Clsuter (for fabric to fabric mode)")
	@category("D2D")
	public static String D2D_FABRIC_INTERFACE_TAR = "LocalCassandra";

    @desc("Table Discovery Strategy ('MTable' - work only with Mtable values; 'Full' - discovers tables from the remote interface)")
	@category("D2D")
	public static String D2D_TABLE_DISCOVERY_STRATEGY = "MTable";
    
    @desc("Report max size - defines the max size of the report, higher than this threshold process will register only mismatches")
	@category("D2D")
	public static final Integer D2D_REPORT_MAX_SIZE = 1000;

	@category("D2D")
	public static String D2D_LKP_INTERFACE_SRC = "POSTGRESQL_ADMIN";
	@category("D2D")
	public static String D2D_LKP_INTERFACE_TAR = "POSTGRESQL_ADMIN";
	@category("D2D")
	public static String D2D_LKP_SCHEMA_SRC = "public";
	@category("D2D")
	public static String D2D_LKP_SCHEMA_TAR = "public";
	@category("D2D")
	public static String D2D_LKP_TABLE_SRC = "customer";
	@category("D2D")
	public static String D2D_LKP_TABLE_TAR = "customer";
	@category("D2D")
	public static String D2D_LKP_KEYS_TABLE_INTERFACE = "dbCassandra";
	@category("D2D")
	public static String D2D_LKP_KEYS_TABLE_SCHEMA = "k2system_mar_d2d_solutions";
	@category("D2D")
	public static String D2D_LKP_KEYS_TABLE_NAME = "keysTable";
	@category("D2D")
	public static String D2D_LKP_GROUPS_COUNT = "1000";
	@category("D2D")
	public static String D2D_LKP_TOKENS_STEP_SIZE = "10^14";
}

