package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections4.CollectionUtils;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;

public class JoinIT extends BaseIT {

	BaseIT result1 = null;
	BaseIT result2 = null;
	private TableResult tableResult1 = null;
	private TableResult newTableResult = null;
	Map<Tuple, Expression> joinedConditionsExpPushedDown;
	Expression joiningResultsOtherExpressions = null;
	Map<Tuple, EqualsTo> joinsExpPushedDown;
	Map<String, List<Column>> joiningColumns = new HashMap<>();
	CIS552SO cis552SO;

	public JoinIT(BaseIT result1, BaseIT result2, Map<Tuple, Expression> joinedConditionsExpPushedDown,
			Map<Tuple, EqualsTo> joinsExpPushedDown, CIS552SO cis552SO) {
		this.result1 = result1;
		this.result2 = result2;
		this.joinedConditionsExpPushedDown = joinedConditionsExpPushedDown;
		this.joinsExpPushedDown = joinsExpPushedDown;
		this.cis552SO = cis552SO;
	}

	@Override
	public TableResult getNext() {
		return newTableResult;
	}

	@Override
	public boolean hasNext() {

		if (result1 == null || result2 == null) {
			return false;
		}

		boolean result2HasNext = result2.hasNext();
		while (result2HasNext || result1.hasNext()) {
			if (tableResult1 == null) {
				if (!result1.hasNext()) {
					return false;
				}
				tableResult1 = result1.getNext();
			}
			if (!result2HasNext) {
				result2.reset();
				tableResult1 = result1.getNext();
				result2HasNext = result2.hasNext();
			}
			TableResult tableResult2 = result2.getNext();
			if (newTableResult == null) {
				newTableResult = new TableResult();
				newTableResult.fromTables.addAll(tableResult1.fromTables);
				newTableResult.fromTables.addAll(tableResult2.fromTables);
				for (Table leftTable : tableResult1.fromTables) {
					for (Table rightTable : tableResult2.fromTables) {
						String leftTableAlias = leftTable.getAlias() != null ? leftTable.getAlias()
								: leftTable.getName();
						String rightTableAlias = rightTable.getAlias() != null ? rightTable.getAlias()
								: rightTable.getName();
						List<String> list = Arrays.asList(leftTableAlias, rightTableAlias);
						Collections.sort(list);

						PrimitiveValue[] joiningTables = { new StringValue(list.get(0)), new StringValue(list.get(1)) };
//						PrimitiveValue[] joiningTables = { new StringValue(leftTableAlias),
//								new StringValue(rightTableAlias) };
						Tuple joiningtuple = new Tuple(joiningTables);
						Expression joiningOtherExpression = joinedConditionsExpPushedDown.get(joiningtuple);
						if (joiningOtherExpression != null) {
							if (joiningResultsOtherExpressions == null) {
								joiningResultsOtherExpressions = joinedConditionsExpPushedDown.get(joiningtuple);
							} else {
								joiningResultsOtherExpressions = new AndExpression(joiningResultsOtherExpressions,
										joinedConditionsExpPushedDown.get(joiningtuple));
							}
						}

						EqualsTo joiningTableExpression = joinsExpPushedDown.get(joiningtuple);
						if (joiningTableExpression != null) {
							Column col1 = (Column) joiningTableExpression.getLeftExpression();
							String table1 = col1.getTable().getName();
							if (joiningColumns.get(table1) == null) {
								joiningColumns.put(col1.getTable().getName(), new ArrayList<Column>());
							}
							joiningColumns.get(table1).add(col1);
							Column col2 = (Column) joiningTableExpression.getRightExpression();
							String table2 = col2.getTable().getName();
							if (joiningColumns.get(table2) == null) {
								joiningColumns.put(table2, new ArrayList<Column>());
							}
							joiningColumns.get(table2).add(col2);
						}
					}
				}
				newTableResult.colPosWithTableAlias.putAll(tableResult1.colPosWithTableAlias);
				int colPos = newTableResult.colPosWithTableAlias.size();
				for (Entry<Column, Integer> entrySet : tableResult2.colPosWithTableAlias.entrySet()) {
					newTableResult.colPosWithTableAlias.put(entrySet.getKey(), colPos + entrySet.getValue());
				}
				newTableResult.aliasandTableName.putAll(tableResult1.aliasandTableName);
				newTableResult.aliasandTableName.putAll(tableResult2.aliasandTableName);
			}

			newTableResult.resultTuples = new ArrayList<>();
			try {
				// External Hash Join Implementation
				if (joiningColumns.size() != 0) {
					Map<Tuple, List<Tuple>> map = new HashMap<>();
					for (Tuple table1ResultTuple : tableResult1.resultTuples) {
						List<Column> columnList = new ArrayList<>();
						for (Table fromItem : tableResult1.fromTables) {
							String tableAlias = fromItem.getAlias() != null ? fromItem.getAlias() : fromItem.getName();
							List<Column> columns = joiningColumns.get(tableAlias);
							if (columns == null)
								continue;
							columnList.addAll(columns);
						}
						PrimitiveValue[] keyTuple = new PrimitiveValue[columnList.size()];
						for (int i = 0; i < columnList.size(); i++) {
							Eval eval = new ExpressionEvaluator(table1ResultTuple, tableResult1, cis552SO, null);
							keyTuple[i] = eval.eval(columnList.get(i));
						}
						Tuple tuple = new Tuple(keyTuple);
						List<Tuple> list = map.get(tuple);
						if (list == null) {
							map.put(tuple, new ArrayList<Tuple>());
						}
						map.get(tuple).add(table1ResultTuple);
					}
					for (Tuple table2ResultTuple : tableResult2.resultTuples) {
						List<Column> columnList = new ArrayList<>();
						for (Table fromItem : tableResult2.fromTables) {
							String tableAlias = fromItem.getAlias() != null ? fromItem.getAlias() : fromItem.getName();
							List<Column> columns = joiningColumns.get(tableAlias);
							if (CollectionUtils.isEmpty(columns))
								continue;
							columnList.addAll(columns);
						}
						PrimitiveValue[] keyTuple = new PrimitiveValue[columnList.size()];
						for (int i = 0; i < columnList.size(); i++) {
							Eval eval = new ExpressionEvaluator(table2ResultTuple, tableResult2, cis552SO, null);
							keyTuple[i] = eval.eval(columnList.get(i));
						}
						Tuple tuple = new Tuple(keyTuple);
						List<Tuple> list = map.get(tuple);
						if (CollectionUtils.isNotEmpty(list)) {
							for (Tuple table1ResultTuple : list) {
								Tuple resultTuple = combineTuples(table2ResultTuple, table1ResultTuple);
								applyConditionForJoinedResult(resultTuple);
							}
						}
					}
				} else {
					for (Tuple table1ResultTuple : tableResult1.resultTuples) {
						for (Tuple table2ResultTuple : tableResult2.resultTuples) {
							Tuple tuple = combineTuples(table2ResultTuple, table1ResultTuple);
							applyConditionForJoinedResult(tuple);
						}
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			if (CollectionUtils.isNotEmpty(newTableResult.resultTuples)) {
				return true;
			}
			result2HasNext = result2.hasNext();
		}
		return false;
	}

	private void applyConditionForJoinedResult(Tuple tuple) throws SQLException, InvalidPrimitive {
		if (joiningResultsOtherExpressions != null) {

			Eval joinedEval = new ExpressionEvaluator(tuple, newTableResult, cis552SO, null);
			if (joiningResultsOtherExpressions == null)
				System.out.println(tuple);
			PrimitiveValue primValue = joinedEval.eval(joiningResultsOtherExpressions);
			if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
				newTableResult.resultTuples.add(tuple);
			}

		} else {
			newTableResult.resultTuples.add(tuple);
		}
	}

	private Tuple combineTuples(Tuple table2ResultTuple, Tuple table1ResultTuple) {
		int length = table1ResultTuple.resultRow.length + table2ResultTuple.resultRow.length;
		PrimitiveValue[] result = new PrimitiveValue[length];
		System.arraycopy(table1ResultTuple.resultRow, 0, result, 0, table1ResultTuple.resultRow.length);
		System.arraycopy(table2ResultTuple.resultRow, 0, result, table1ResultTuple.resultRow.length,
				table2ResultTuple.resultRow.length);
		return new Tuple(result);
	}

	@Override
	public void reset() {
		result1.reset();
		result2.reset();
	}

}
