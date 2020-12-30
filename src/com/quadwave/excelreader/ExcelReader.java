package com.quadwave.excelreader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.quadwave.nbt.common.execution.entity.WorkflowInstance;
import com.quadwave.nbt.common.execution.entity.WorkflowNode;
import com.quadwave.nbt.common.execution.entity.WorkflowVariable;
import com.quadwave.nbt.common.execution.java.JavaNodeHandler;

public class ExcelReader extends JavaNodeHandler {

	@Override
	public void execute(WorkflowInstance workflowInstance, WorkflowNode workflowNode) {
		String fileName = "Sample_Input_File.xlsx";
		try(FileInputStream fis = new FileInputStream(fileName)) {

			// Create Workbook instance for xlsx/xls file input stream
			Workbook workbook = null;
			if (fileName.toLowerCase().endsWith("xlsx")) {
				workbook = new XSSFWorkbook(fis);
			} else if (fileName.toLowerCase().endsWith("xls")) {
				workbook = new HSSFWorkbook(fis);
			}

			// Get the number of sheets in the xlsx file
			int numberOfSheets = workbook.getNumberOfSheets();

			// loop through each of the sheets
			for (int i = 0; i < numberOfSheets; i++) {

				// Get the nth sheet from the workbook
				Sheet sheet = workbook.getSheetAt(i);

				// every sheet has rows, iterate over them
				Iterator<Row> rowIterator = sheet.iterator();
				if (rowIterator.hasNext()) {
					String name = "";

					// Get the row object
					Row row = rowIterator.next();

					// Every row has columns, get the column iterator and iterate over them
					Iterator<Cell> cellIterator = row.cellIterator();
					
					List<WorkflowVariable> requestVariables = new ArrayList<WorkflowVariable>();
					while (cellIterator.hasNext()) {
						// Get the Cell object
						Cell cell = cellIterator.next();

						// check the cell type and process accordingly
						switch (cell.getCellType()) {
						case Cell.CELL_TYPE_STRING:
							name = cell.getStringCellValue().trim();
							WorkflowVariable variable = new WorkflowVariable();
							variable.setName(name);
							requestVariables.add(variable);
							break;
						}
						workflowInstance.setRequestVariables(requestVariables);
					} // end of cell iterator
				} // end of rows iterator

			} // end of sheets for loop

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
