package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import cis552project.TableColumnData;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectItemIT extends BaseIT {

	BaseIT result = null;
	List<SelectItem> selectItems = null;
	CIS552SO cis552SO = null;
	TableResult newTableResult;

	public SelectItemIT(List<SelectItem> selectItems, BaseIT result, CIS552SO cis552SO) throws SQLException {
		this.selectItems = selectItems;
		this.result = result;
		this.cis552SO = cis552SO;
	}

	@Override
	public TableResult getNext() {
		try {

			TableResult oldTableResult = result.getNext();
			if (newTableResult == null) {
				newTableResult = new TableResult();
				updateColDefMap(oldTableResult, newTableResult);
			}
			return solveSelectItemExpression(oldTableResult, newTableResult);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean hasNext() {
		if (result == null || !result.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public void reset() {
		result.reset();
	}

	private TableResult solveSelectItemExpression(TableResult oldTableResult, TableResult newTableResult)
			throws SQLException {
		if (selectItems.get(0) instanceof AllColumns) {
			newTableResult.resultTuples = oldTableResult.resultTuples;
		} else {
			List<Expression> finalExpItemList = new ArrayList<>();
			for (SelectItem selectItem : selectItems) {
				if (selectItem instanceof SelectExpressionItem) {
					Expression expr = ((SelectExpressionItem) selectItem).getExpression();

					finalExpItemList.add(expr);
				} else if (selectItem instanceof AllTableColumns) {
					AllTableColumns allTableColumn = (AllTableColumns) selectItem;
					TableColumnData tableSchema = cis552SO.tables
							.get(oldTableResult.aliasandTableName.get(allTableColumn.getTable().getName()));

					finalExpItemList.addAll(tableSchema.colList.stream()
							.map(col -> (Expression) new Column(allTableColumn.getTable(), col.getColumnName()))
							.collect(Collectors.toList()));
				}
			}
			newTableResult.resultTuples = new ArrayList<>();
			for (Tuple tuple : oldTableResult.resultTuples) {
				PrimitiveValue[] primValue = new PrimitiveValue[finalExpItemList.size()];
				for (int i = 0; i < finalExpItemList.size(); i++) {
					Eval eval = new ExpressionEvaluator(tuple, oldTableResult, cis552SO, null);
					primValue[i] = eval.eval(finalExpItemList.get(i));
				}
				newTableResult.resultTuples.add(new Tuple(primValue));
			}

		}

		return newTableResult;
	}

	private void updateColDefMap(TableResult oldTableResult, TableResult newTableResult) {
		newTableResult.fromTables.addAll(oldTableResult.fromTables);
		newTableResult.aliasandTableName.putAll(oldTableResult.aliasandTableName);
		if (selectItems.get(0) instanceof AllColumns) {
			newTableResult.colPosWithTableAlias.putAll(oldTableResult.colPosWithTableAlias);
		} else {
			int pos = 0;
			for (SelectItem si : selectItems) {
				if (si instanceof AllTableColumns) {
					for (Entry<Column, Integer> entrySet : oldTableResult.colPosWithTableAlias.entrySet()) {
						newTableResult.colPosWithTableAlias.put(entrySet.getKey(), pos);
					}
				} else if (si instanceof SelectExpressionItem) {
					SelectExpressionItem sei = (SelectExpressionItem) si;
					Expression exp = sei.getExpression();
					String columnName = exp.toString();
					if (exp instanceof Column) {
						Column column = (Column) exp;
						columnName = column.getColumnName();
					}
					String columnAlias = sei.getAlias() != null ? sei.getAlias() : columnName;
					newTableResult.colPosWithTableAlias.put(new Column(null, columnAlias), pos);

				}
				pos++;
			}
		}
	}
}
