package cis552project.iterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import cis552project.CIS552SO;
import cis552project.SQLDataType;
import cis552project.TableColumnData;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableIT extends BaseIT {
	private File tableFile = null;
	private TableResult tableRes = null;
	// private Scanner fileScanner = null;
	private List<Column> colList = null;
	private TableColumnData selectTableTemp = null;
	private CIS552SO cis552SO;
	private BufferedReader csvBufferedReader;

	public TableIT(Table table, CIS552SO cis552SO) {

		String aliasName = table.getAlias() != null ? table.getAlias() : table.getName();
		this.cis552SO = cis552SO;
		tableRes = new TableResult();
		try {
			tableFile = new File(cis552SO.dataPath, table.getName() + ".csv");
			// fileScanner = new Scanner(tableFile);
			csvBufferedReader = new BufferedReader(new FileReader(tableFile));
		} catch (FileNotFoundException e) {
			System.out.println("Exception : " + e.getLocalizedMessage());
		} finally {
			this.selectTableTemp = cis552SO.tables.get(table.getName());
			this.colList = selectTableTemp.colList;
			int colPos = 0;
			for (Column col : selectTableTemp.colList) {
				tableRes.colPosWithTableAlias.put(new Column(new Table(aliasName), col.getColumnName()), colPos);
				colPos++;
			}
			tableRes.fromTables.add(table);
			tableRes.aliasandTableName.put(aliasName, table.getName());
		}

	}

	@Override
	public TableResult getNext() {
		return tableRes;
	}

	@Override
	public boolean hasNext() {
		tableRes.resultTuples = new ArrayList<>();
		int n = cis552SO.tupleLimit;
		try {
			String line = csvBufferedReader.readLine();
			while (line != null && n != 0) {
				Tuple tuple = fetchConvertedTupleFromStringArray(line.split("\\|"));
				tableRes.resultTuples.add(tuple);
				n--;
				if (n != 0) {
					line = csvBufferedReader.readLine();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return CollectionUtils.isNotEmpty(tableRes.resultTuples);
	}

	@Override
	public void reset() {
		try {
			csvBufferedReader = new BufferedReader(new FileReader(tableFile));
//			fileScanner = new Scanner(tableFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private Tuple fetchConvertedTupleFromStringArray(String[] stringArray) {
		PrimitiveValue[] resultRow = new PrimitiveValue[stringArray.length];
		for (int i = 0; i < stringArray.length; i++) {

			resultRow[i] = convertStringToPV(stringArray[i], i);
		}
		return new Tuple(resultRow);
	}

	private PrimitiveValue convertStringToPV(String stringValue, int colPos) {

		ColumnDefinition colDef = selectTableTemp.colDefMap.get(colList.get(colPos).getColumnName());
		SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());
		switch (colSqlDataType) {
		case CHAR:
		case VARCHAR:
		case STRING:
			return new StringValue(stringValue);
		case DATE:
			return new DateValue(stringValue);
		case DECIMAL:
			return new DoubleValue(stringValue);
		case INT:
			return new LongValue(stringValue);
		}
		throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods,
	}

}
