package cis552project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

public class CIS552ProjectUtils {

	public static String[] combineArrays(String[] a, String[] b) {
		int length = a.length + b.length;
		String[] result = new String[length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	public static List<String[]> readTable(String filePath, String tableName) throws IOException {
		String wholeFilePath = filePath + tableName;
		List<String[]> resultRows = new ArrayList<>();
		try (FileReader file = new FileReader(wholeFilePath + ".dat")) {
			BufferedReader fileStream = new BufferedReader(file);
			String temp = fileStream.readLine();
			while (temp != null) {
				resultRows.add(temp.split("\\|"));
				temp = fileStream.readLine();
			}
		} catch (FileNotFoundException ex) {
			try (FileReader file = new FileReader(wholeFilePath + ".csv")) {
				BufferedReader fileStream = new BufferedReader(file);
				String temp = fileStream.readLine();
				while (temp != null) {
					resultRows.add(temp.split("\\|"));
					temp = fileStream.readLine();
				}
			} catch (FileNotFoundException ex1) {
				Logger.getLogger(CIS552ProjectUtils.class.getName()).log(Level.SEVERE, null, ex1);
			}
		}
		return resultRows;

	}

	public static List<String> readCommands(String filePath) throws IOException {
		List<String> commandsList = new ArrayList<>();
		try (FileReader file = new FileReader(filePath)) {
			BufferedReader fileStream = new BufferedReader(file);
			String temp = fileStream.readLine();
			String previousString = "";
			while (temp != null) {
				if (!temp.endsWith(";")) {
					previousString += " " + temp;
				} else {
					temp = previousString + " " + temp;
					previousString = "";
					commandsList.add(temp);
				}
				temp = fileStream.readLine();
			}
		}

		return commandsList;
	}

	public static TableColumnData getTableSchemaForColumnFromFromItems(Column column, List<FromItem> fromItems,
			CIS552SO cis552SO) {
		for (FromItem fromItem : fromItems) {
			if (fromItem instanceof Table) {
				Table table = (Table) fromItem;
				TableColumnData tableSchema = cis552SO.tables.get(table.getName());
				if (tableSchema.containsColumn(column.getColumnName())) {
					return tableSchema;
				}
			}
		}
		return null;
	}

	public static Table getTable(Column column, Set<Column> cloumnSet, CIS552SO cis552SO) {
		for (Column col : cloumnSet) {
			if (column.getColumnName().equals(col.getColumnName())) {
				return col.getTable();
			}
		}
		return null;
	}

	public static Column determineColumnNameFromWholeName(String columnName, Map<String, String> aliasandTableName) {
		Table table = null;
		if (columnName.contains(".")) {
			String aliasTableName = columnName.substring(0, columnName.indexOf("."));
			columnName = columnName.substring(columnName.indexOf(".") + 1, columnName.length());
			String tableName = aliasandTableName.get(aliasTableName);
			table = new Table(tableName);
			table.setAlias(aliasTableName);
		}
		return new Column(table, columnName);

	}

}
