package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByIT extends BaseIT {

	TableResult finalTableResult = null;
	Iterator<Tuple> resIT = null;
	Collection<Tuple> finalResultTuples = null;

	public GroupByIT(List<Column> groupByColumnList, List<SelectItem> selectItems, BaseIT result, CIS552SO cis552SO)
			throws SQLException {

		finalTableResult = new TableResult();
		updateColDefMap(finalTableResult, selectItems);
		finalResultTuples = new ArrayList<>();
		Map<Tuple, Tuple> groupByMap = evaluateSelectionOnGroupedColumns(groupByColumnList, selectItems, result,
				cis552SO);
		finalResultTuples = groupByMap.values();
		resIT = finalResultTuples.iterator();

	}

	@Override
	public TableResult getNext() {
		finalTableResult.resultTuples = new ArrayList<>();
		finalTableResult.resultTuples.add(resIT.next());
		return finalTableResult;
	}

	@Override
	public boolean hasNext() {
		return resIT.hasNext();
	}

	@Override
	public void reset() {
		resIT = finalResultTuples.iterator();

	}

	private void updateColDefMap(TableResult newTableResult, List<SelectItem> selectItems) {
		int pos = 0;
		for (SelectItem si : selectItems) {
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

	private Map<Tuple, Tuple> evaluateSelectionOnGroupedColumns(List<Column> groupByColumnList,
			List<SelectItem> selectItems, BaseIT result, CIS552SO cis552SO) throws SQLException, InvalidPrimitive {

		Map<Tuple, Tuple> groupByMap = new HashMap<>();
		Map<Tuple, Long> tupleCount = new HashMap<>();
		while (result.hasNext()) {
			TableResult initialTabRes = result.getNext();

			for (Tuple tuple : initialTabRes.resultTuples) {
				PrimitiveValue primValeArray[] = new PrimitiveValue[groupByColumnList.size()];
				for (int i = 0; i < groupByColumnList.size(); i++) {
					Column column = groupByColumnList.get(i);
					Eval eval = new ExpressionEvaluator(tuple, initialTabRes, cis552SO, null);
					primValeArray[i] = eval.eval(column);
				}
				Tuple groupedTuple = new Tuple(primValeArray);
				if (!groupByMap.containsKey(groupedTuple)) {
					PrimitiveValue[] resultprimitive = new PrimitiveValue[selectItems.size()];
					groupByMap.put(groupedTuple, new Tuple(resultprimitive));
					tupleCount.put(groupedTuple, (long) 0);
				}

				tupleCount.put(groupedTuple, tupleCount.get(groupedTuple) + 1);
				for (int i = 0; i < selectItems.size(); i++) {
					SelectExpressionItem sei = (SelectExpressionItem) selectItems.get(i);
					Expression exp = sei.getExpression();
					if (exp instanceof Column) {
						int index = groupByColumnList.indexOf(exp);
						groupByMap.get(groupedTuple).resultRow[i] = groupedTuple.resultRow[index];
					} else {
						Eval eval = new ExpressionEvaluator(tuple, initialTabRes, cis552SO, null);
						PrimitiveValue primValue = eval.eval(exp);

						if (groupByMap.get(groupedTuple).resultRow[i] != null) {
							if (exp.toString().toUpperCase().contains("COUNT")) {
								primValue = new DoubleValue(tupleCount.get(groupedTuple));
							} else if (exp.toString().toUpperCase().contains("SUM")) {
								primValue = new DoubleValue(
										groupByMap.get(groupedTuple).resultRow[i].toDouble() + primValue.toDouble());
							} else if (exp.toString().toUpperCase().contains("MIN")) {
								double min = primValue.toDouble();
								if (min > groupByMap.get(groupedTuple).resultRow[i].toDouble())
									primValue = groupByMap.get(groupedTuple).resultRow[i];
							} else if (exp.toString().toUpperCase().contains("MAX")) {
								double max = primValue.toDouble();
								if (max < groupByMap.get(groupedTuple).resultRow[i].toDouble())
									primValue = groupByMap.get(groupedTuple).resultRow[i];
							} else if (exp.toString().toUpperCase().contains("AVG")) {
								double previous = groupByMap.get(groupedTuple).resultRow[i].toDouble()
										* (tupleCount.get(groupedTuple) - 1);
								primValue = new DoubleValue(
										(primValue.toDouble() + previous) / tupleCount.get(groupedTuple));
							}
						}
						groupByMap.get(groupedTuple).resultRow[i] = primValue;
					}

				}
			}

		}
		return groupByMap;
	}

}
