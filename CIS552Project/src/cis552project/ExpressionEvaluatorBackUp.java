package cis552project;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cis552project.iterator.TableResult;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ExpressionEvaluatorBackUp {

	public static PrimitiveValue applyCondition(PrimitiveValue[] rowResult, Expression where, TableResult tabResult,
			CIS552SO cis552SO) throws SQLException {
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column column) throws SQLException {
				Table table = column.getTable();
				if (table == null || table.getName() == null) {
//					table = getTableSchemaForColumnFromFromItems(column, tabResult.fromItems, cis552SO).table;
					table = getTable(column, tabResult.colPosWithTableAlias.keySet(), cis552SO);
					column.setTable(table);
				}

				int pos = tabResult.colPosWithTableAlias.get(column);
				return rowResult[pos];
			}

		};
		return eval.eval(expressionEvaluator(where, tabResult, cis552SO));

	}

	private static Expression expressionEvaluator(Expression exp, TableResult tabResult, CIS552SO cis552SO) {
		if (exp instanceof Between) {
			Between bet = (Between) exp;
			if (bet.getLeftExpression() instanceof Column) {

				Column column = (Column) bet.getLeftExpression();
				Table table = column.getTable();
				ColumnDefinition colDef = null;
				if (table == null || table.getName() == null) {
					table = getTableSchemaForColumnFromFromItems(column, tabResult.fromTables, cis552SO).table;
				}
				String tableName = tabResult.aliasandTableName.get(table.getName());
				colDef = cis552SO.tables.get(tableName).colDefMap.get(column.getColumnName());
				SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());
				if (SQLDataType.DATE.equals(colSqlDataType)) {
					bet.setBetweenExpressionEnd(
							new DateValue(bet.getBetweenExpressionEnd().toString().replace("'", "")));
					bet.setBetweenExpressionStart(
							new DateValue(bet.getBetweenExpressionStart().toString().replace("'", "")));
				}
			}
		}
		return exp;
	}

	protected static TableColumnData getTableSchemaForColumnFromFromItems(Column column, List<Table> fromTables,
			CIS552SO cis552SO) {
		for (Table fromTable : fromTables) {
			Table table = fromTable;
			TableColumnData tableSchema = cis552SO.tables.get(table.getName());
			if (tableSchema.containsColumn(column.getColumnName())) {
				return tableSchema;
			}
		}
		return null;
	}

	protected static Table getTable(Column column, Set<Column> cloumnSet, CIS552SO cis552SO) {
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
