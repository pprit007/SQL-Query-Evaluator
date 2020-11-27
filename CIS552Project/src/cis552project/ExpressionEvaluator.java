package cis552project;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections4.CollectionUtils;

import cis552project.iterator.BaseIT;
import cis552project.iterator.FunctionEvaluation;
import cis552project.iterator.SubSelectIT;
import cis552project.iterator.TableResult;
import cis552project.iterator.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionEvaluator extends Eval {
	private Tuple resultTuple;
	private TableResult tabResult;
	private CIS552SO cis552SO;
	private Map<Column, PrimitiveValue> outerQueryColResult;
	List<PrimitiveValue> inExpressionResult;

	public ExpressionEvaluator(Tuple resultTuples, TableResult tabResult, CIS552SO cis552SO,
			Map<Column, PrimitiveValue> outerQueryColResult) {

		this.resultTuple = resultTuples;
		this.tabResult = tabResult;
		this.cis552SO = cis552SO;

		this.outerQueryColResult = outerQueryColResult;
		inExpressionResult = new ArrayList<>();
	}

	@Override
	public PrimitiveValue eval(Column column) throws SQLException {
		Table table = column.getTable();
		if (table == null || table.getName() == null) {
			table = CIS552ProjectUtils.getTable(column, tabResult.colPosWithTableAlias.keySet(), cis552SO);
			column.setTable(table);
		}

		if (tabResult.colPosWithTableAlias.get(column) == null) {
			return outerQueryColResult.get(column);
		}
		return resultTuple.resultRow[tabResult.colPosWithTableAlias.get(column)];
	}

	@Override
	public PrimitiveValue eval(Function function) throws SQLException {
		return FunctionEvaluation.applyFunction(resultTuple, function, tabResult, cis552SO);
	}

	@Override
	public PrimitiveValue eval(ExistsExpression existExp) throws SQLException {
		Eval eval = new ExpressionEvaluator(resultTuple, tabResult, cis552SO, null);
		PrimitiveValue existsValue = BooleanValue.FALSE;
		if (existExp.getRightExpression() instanceof SubSelect) {
			existsValue = evalSubSelect((SubSelect) existExp.getRightExpression());
		} else {
			existsValue = eval.eval(existExp.getRightExpression());
		}
		return existsValue;
	}

	@Override
	public PrimitiveValue eval(InExpression inExp) throws SQLException {
		if (inExp.getItemsList() instanceof SubSelect) {
			if (CollectionUtils.isEmpty(inExpressionResult)) {
				BaseIT result = new SubSelectIT((SubSelect) inExp.getItemsList(), cis552SO, null);
				while (result.hasNext()) {
					for (Tuple tuple : result.getNext().resultTuples) {
						inExpressionResult.add(tuple.resultRow[0]);
					}
				}
			}
		} else {
			for (Expression expr : ((ExpressionList) inExp.getItemsList()).getExpressions()) {
				Eval inEval = new ExpressionEvaluator(resultTuple, tabResult, cis552SO, null);
				inExpressionResult.add(inEval.eval(expr));
			}
		}
		Eval evalLeftExp = new ExpressionEvaluator(resultTuple, tabResult, cis552SO, null);
		PrimitiveValue leftHSV = evalLeftExp.eval(inExp.getLeftExpression());
		if (inExpressionResult.contains(leftHSV)) {
			return BooleanValue.TRUE;
		}
		return BooleanValue.FALSE;
	}

	public PrimitiveValue evalSubSelect(SubSelect subSelect) throws SQLException {
		Map<Column, PrimitiveValue> outerQueryColResult = new HashMap<>();

		for (Entry<Column, Integer> entrySet : tabResult.colPosWithTableAlias.entrySet()) {
			outerQueryColResult.put(entrySet.getKey(), resultTuple.resultRow[entrySet.getValue()]);
		}
		BaseIT result = new SubSelectIT(subSelect, cis552SO, outerQueryColResult);
		if (result.hasNext()) {
			return BooleanValue.TRUE;
		}
		return BooleanValue.FALSE;
	}

//	@Override
//	public PrimitiveValue eval(Between between) throws SQLException {
//		if (between.getLeftExpression() instanceof Column) {
//			between.setBetweenExpressionStart(extractPrimitiveValueExpression((Column) between.getLeftExpression(),
//					between.getBetweenExpressionStart()));
//			between.setBetweenExpressionEnd(extractPrimitiveValueExpression((Column) between.getLeftExpression(),
//					between.getBetweenExpressionEnd()));
//		}
//		return super.eval(between);
//
//	}
//
//	@Override
//	public PrimitiveValue eval(GreaterThan greaterExp) throws SQLException {
//		if (greaterExp.getLeftExpression() instanceof Column) {
//			greaterExp.setRightExpression(extractPrimitiveValueExpression((Column) greaterExp.getLeftExpression(),
//					greaterExp.getRightExpression()));
//		}
//		return super.eval(greaterExp);
//	}
//
//	private Expression extractPrimitiveValueExpression(Column column, Expression expression) {
//
//		Table table = column.getTable();
//		ColumnDefinition colDef = null;
//		if (table == null || table.getName() == null) {
//			table = CIS552ProjectUtils.getTableSchemaForColumnFromFromItems(column, tabResult.fromItems,
//					cis552SO).table;
//		}
//		String tableName = tabResult.aliasandTableName.get(table.getName());
//		colDef = cis552SO.tables.get(tableName).colDefMap.get(column.getColumnName());
//		SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());
//		if (SQLDataType.DATE.equals(colSqlDataType)) {
//			return new DateValue(expression.toString().replace("'", ""));
//		}
//
//		return expression;
//
//	}

}
