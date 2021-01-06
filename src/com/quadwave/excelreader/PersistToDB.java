package com.quadwave.excelreader;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quadwave.nbt.common.execution.entity.WorkflowInstance;
import com.quadwave.nbt.common.execution.entity.WorkflowNode;
import com.quadwave.nbt.common.execution.entity.WorkflowVariable;
import com.quadwave.nbt.common.execution.java.JavaNodeHandler;

public class PersistToDB extends JavaNodeHandler {

	private String url;
	private String metaQuery;
	private String insertQuery;
	private String mappingQuery;
	private String username;
	private String password;

	public PersistToDB() throws IOException {
		Properties prop = new Properties();
		prop.load(this.getClass().getResourceAsStream("/db.properties"));
		this.url = prop.getProperty("url");
		this.metaQuery = prop.getProperty("metaquery");
		this.insertQuery = prop.getProperty("insertquery");
		this.mappingQuery = prop.getProperty("mappingquery");
		this.username = prop.getProperty("username");
		this.password = prop.getProperty("password");
	}

	@Override
	public void execute(WorkflowInstance instance, WorkflowNode node) {

		HashSet<String> columnHeader = new HashSet<String>();
		try (Connection connection = DriverManager.getConnection(url, username, password)) {
			try (Statement statement = connection.createStatement()) {
				try (ResultSet set = statement.executeQuery(metaQuery)) {
					while (set.next()) {
						columnHeader.add(set.getString(1));
					} // end of result set loop
				} // closing result set
			} // closing statement
			WorkflowVariable variable = instance.getVariableByName("header_var");
			HashSet<String> variableSet = new HashSet<String>(Arrays.asList(variable.getValue().split(",")));
			boolean check = columnHeader.containsAll(variableSet) && variableSet.containsAll(columnHeader);
			try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
				WorkflowVariable fileupload = instance.getVariableByName("fileupload");
				ObjectMapper mapper = new ObjectMapper();
				String value = fileupload.getValue();
				VariableDTO dto = null;
				try {
					dto = mapper.readValue(value.substring(1, value.length() - 1), VariableDTO.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (dto != null) {
					String fileName = dto.getLocalPath();
					// creating stram for
					try (FileInputStream fis = new FileInputStream(fileName)) {
						Workbook workbook = null;
						if (fileName.toLowerCase().endsWith("xlsx")) {
							workbook = new XSSFWorkbook(fis);
						} else if (fileName.toLowerCase().endsWith("xls")) {
							workbook = new HSSFWorkbook(fis);
						}
						Sheet sheet = workbook.getSheetAt(0);
						Iterator<Row> rowIterator = sheet.iterator();
						rowIterator.next();
						String[] headers = variable.getValue().split(",");
						if (check) {
							while (rowIterator.hasNext()) {
								Row row = rowIterator.next();
								Iterator<Cell> cellIterator = row.cellIterator();
								int count = 0;
								while (cellIterator.hasNext()) {
									Cell cell = cellIterator.next();
									switch (headers[count]) {
									case "customer_id":
										preparedStatement.setString(1, cell.getNumericCellValue() + "");
										break;
									case "first_name":
										preparedStatement.setString(2, cell.getStringCellValue());
										break;
									case "mid_name":
										preparedStatement.setString(3, cell.getStringCellValue());
										break;
									case "last_name":
										preparedStatement.setString(4, cell.getStringCellValue());
										break;
									case "addr_id":
										preparedStatement.setString(5, cell.getStringCellValue());
										break;
									case "addr_line_1":
										preparedStatement.setString(6, cell.getStringCellValue());
										break;
									case "addr_line_2":
										preparedStatement.setString(7, cell.getStringCellValue());
										break;
									case "city":
										preparedStatement.setString(8, cell.getStringCellValue());
										break;
									case "state":
										preparedStatement.setString(9, cell.getStringCellValue());
										break;
									case "zip":
										preparedStatement.setString(10, cell.getStringCellValue());
										break;
									case "CUST_CRDT_LIMIT":
										preparedStatement.setString(11, cell.getStringCellValue());
										break;
									case "CUST_TYPE":
										preparedStatement.setString(12, cell.getStringCellValue());
										break;
									case "CUST_PYT_MODE":
										preparedStatement.setString(13, cell.getStringCellValue());
										break;
									case "CUST_PYT_TERMS":
										preparedStatement.setString(14, cell.getStringCellValue());
										break;
									}
								} // end of cell iterator loop
								preparedStatement.execute();
							} // end of row iterator loop
						} else {
							try (PreparedStatement mappingStatement = connection.prepareStatement(mappingQuery)) {
								for (int i = 0; i < headers.length; i++) {
									mappingStatement.setString(i + 1, headers[i]);
								} // end of for loop
								try (ResultSet mappingSet = mappingStatement.executeQuery()) {
									String[] headerArr = new String[14];
									while (mappingSet.next()) {
										switch (mappingSet.getString("PreQualificationDataHeader")) {
										case "customer_id":
											headerArr[0] = mappingSet.getString("InputFileHeader");
											break;
										case "first_name":
											headerArr[1] = mappingSet.getString("InputFileHeader");
											break;
										case "mid_name":
											headerArr[2] = mappingSet.getString("InputFileHeader");
											break;
										case "last_name":
											headerArr[3] = mappingSet.getString("InputFileHeader");
											break;
										case "addr_id":
											headerArr[4] = mappingSet.getString("InputFileHeader");
											break;
										case "addr_line_1":
											headerArr[5] = mappingSet.getString("InputFileHeader");
											break;
										case "addr_line_2":
											headerArr[6] = mappingSet.getString("InputFileHeader");
											break;
										case "city":
											headerArr[7] = mappingSet.getString("InputFileHeader");
											break;
										case "state":
											headerArr[8] = mappingSet.getString("InputFileHeader");
											break;
										case "zip":
											headerArr[9] = mappingSet.getString("InputFileHeader");
											break;
										case "CUST_CRDT_LIMIT":
											headerArr[10] = mappingSet.getString("InputFileHeader");
											break;
										case "CUST_TYPE":
											headerArr[11] = mappingSet.getString("InputFileHeader");
											break;
										case "CUST_PYT_MODE":
											headerArr[12] = mappingSet.getString("InputFileHeader");
											break;
										case "CUST_PYT_TERMS":
											headerArr[13] = mappingSet.getString("InputFileHeader");
											break;
										} // end of switch case
									} // end of mappingSet iteration
									while (rowIterator.hasNext()) {
										Row row = rowIterator.next();
										Iterator<Cell> cellIterator = row.cellIterator();
										int count = 0;
										while (cellIterator.hasNext()) {
											Cell cell = cellIterator.next();
											if (headers[count].equals(headerArr[0]))
												preparedStatement.setString(1, cell.getNumericCellValue() + "");
											if (headers[count].equals(headerArr[1]))
												preparedStatement.setString(2, cell.getStringCellValue());
											if (headers[count].equals(headerArr[2]))
												preparedStatement.setString(3, cell.getStringCellValue());
											if (headers[count].equals(headerArr[3]))
												preparedStatement.setString(4, cell.getStringCellValue());
											if (headers[count].equals(headerArr[4]))
												preparedStatement.setString(5, cell.getStringCellValue());
											if (headers[count].equals(headerArr[5]))
												preparedStatement.setString(6, cell.getStringCellValue());
											if (headers[count].equals(headerArr[6]))
												preparedStatement.setString(7, cell.getStringCellValue());
											if (headers[count].equals(headerArr[7]))
												preparedStatement.setString(8, cell.getStringCellValue());
											if (headers[count].equals(headerArr[8]))
												preparedStatement.setString(9, cell.getStringCellValue());
											if (headers[count].equals(headerArr[9]))
												preparedStatement.setString(10, cell.getStringCellValue());
											if (headers[count].equals(headerArr[10]))
												preparedStatement.setString(11, cell.getStringCellValue());
											if (headers[count].equals(headerArr[11]))
												preparedStatement.setString(12, cell.getStringCellValue());
											if (headers[count].equals(headerArr[12]))
												preparedStatement.setString(13, cell.getStringCellValue());
											if (headers[count].equals(headerArr[13]))
												preparedStatement.setString(14, cell.getStringCellValue());
										} // end of cell iterator loop
										preparedStatement.execute();
									} // end of row iterator loop
								} // closing mappingSet
							} // closing mapping statement using try with resources
						} // end of if else for column header check
					} // closing file input stream (excel)
				} // end of null check for dto
			} // closing prepared statement
		} catch (Exception e) {
			e.printStackTrace();
		} // closing connection using try with resources and catch block
	} // end of execute method
} // end of class
