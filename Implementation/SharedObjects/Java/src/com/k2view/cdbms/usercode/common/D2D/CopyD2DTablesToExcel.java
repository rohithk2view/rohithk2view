package com.k2view.cdbms.usercode.common.D2D;

import com.aspose.cells.ListObject;
import com.aspose.cells.SaveFormat;
import com.aspose.cells.Workbook;
import com.aspose.cells.Worksheet;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.util.ByteStreamDescribed;
import com.k2view.broadway.util.InputStreamIterator;
import com.k2view.cdbms.lut.LUType;
import com.k2view.cdbms.shared.Db;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.session.broadway.FabricAbstractActor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.Map;

import static com.k2view.cdbms.shared.user.UserCode.db;
import static com.k2view.cdbms.shared.user.UserCode.getLuType;

/**
 * The actor creates Aspose Workbook file based on a stream input, and masks its content based on the masking patterns.
 * <p>
 * During class load, Aspose license will be initialized. The class Aspose.Total license file.
 */
@SuppressWarnings({"unchecked", "deprecation"})
public class CopyD2DTablesToExcel extends FabricAbstractActor {
    private static Log log = Log.a(CopyD2DTablesToExcel.class);
    public static final String LICENSE_TOTAL_FILENAME = "d2d/Aspose.Total.Java.lic";
    public static final String LICENSES_DIR = "D2D";

    static {
        com.aspose.cells.License license = null;
        InputStream stream = null;
        try {
            stream = ClassLoader.getSystemClassLoader().getResourceAsStream(LICENSE_TOTAL_FILENAME);
            if (null == stream) {
                stream = CopyD2DTablesToExcel.class.getClassLoader().getResourceAsStream(LICENSE_TOTAL_FILENAME);
            }
            if (null != stream) {
                license = new com.aspose.cells.License();
                license.setLicense(stream);
            } else {
                log.warn("License file was not found in the directory '{}' under classpath. The evaluation artifacts will be present!", LICENSES_DIR);
            }
        } catch (Exception e) {
            log.error("Failed to initialize the license file! The evaluation artifacts will be present!");
            e.printStackTrace();
        } finally {
            if (null != stream) {
                try {
                    stream.close();
                } catch (IOException e) {
                    log.warn("There was a failure to close an open stream");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @param input   file stream and masking patterns map.
     * @param output  output file stream
     * @param context the flow context
     * @throws Exception
     */

    @Override
    public void fabricAction(Data input, Data output, Context context) throws Exception {

        Workbook wb = new Workbook();
        boolean data_found_wb = false;
        String EXECUTION_ID = input.get("execution_id") + "";
        String lu_name = input.get("luName") + "";
        Db interfaceConn = db(LUType.getTypeByName(lu_name).ludbGlobals.get("D2D_RESULT_INTERFACE") + "");
        Map<String, String> queries = (Map<String, String>) input.get("queries");
		boolean withParam = Boolean.parseBoolean(input.get("strict") + "");
		Object[] params = new Object[]{EXECUTION_ID};


        for (Map.Entry<String, String> mapEnt : queries.entrySet()) {
            boolean data_found_table = false;
            String sheet_name = mapEnt.getKey();
            wb.getWorksheets().add(sheet_name.toLowerCase());
            Worksheet sheet = wb.getWorksheets().get(sheet_name.toLowerCase());
			String sqlOnSheet = mapEnt.getValue();
			if(withParam){
				params = new Object[]{EXECUTION_ID, input.get("iid") + ""};
			}
				
	
            ResultSet resultSet = interfaceConn.fetch(mapEnt.getValue(), params).resultSet();
            try {
                int rowIndex = 0;
                int colIndex = 0;

                // Write headers
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    String columnName = resultSet.getMetaData().getColumnName(i).toUpperCase();
                    sheet.getCells().get(rowIndex, colIndex).putValue(columnName);
                    colIndex++;
                }
                rowIndex++;

                // Write data rows
                while (resultSet.next()) {
                    colIndex = 0;
                    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                        Object value = resultSet.getObject(i);
                        if (value != null)
							sheet.getCells().get(rowIndex, colIndex).putValue(String.valueOf(value), true);
                            
						//sheet.getCells().get(rowIndex, colIndex).putValue(value + "");
                        colIndex++;
                    }
                    rowIndex++;

                    data_found_table = true;
                    if (!data_found_wb)
                        data_found_wb = true;
                }

                if (!data_found_table) {
                    wb.getWorksheets().removeAt(sheet_name);
                    continue;
                }

                //Table Formatting and hiding the ExecutionID column
                ListObject tableObject = sheet.getListObjects().get(sheet.getListObjects().add(0, 0, rowIndex - 1, colIndex - 1, true));
                tableObject.applyStyleToRange();
                sheet.freezePanes(1, 0, 1, 0);

            } finally {
                resultSet.close();

            }
        }

        if (data_found_wb) {
            wb.getWorksheets().removeAt("Sheet1");
            wb.getWorksheets().setActiveSheetIndex(0);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // save workbook into the output stream
            if ("xls".equals(input.get("file_type") + "")) {
                wb.save(outputStream, SaveFormat.EXCEL_97_TO_2003);
            } else{
                wb.save(outputStream, SaveFormat.XLSX);
            }

            ByteArrayInputStream resultStream = new ByteArrayInputStream(outputStream.toByteArray());
            ByteStreamDescribed iterator = (ByteStreamDescribed) InputStreamIterator.byteStream(resultStream, !context.isDebugEnabled());
            output.put("result", ParamConvertor.toBuffer(iterator));
        }
    }
}
