package cis552project.iterator;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import cis552project.CIS552SO;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class PlainSelectIT extends BaseIT {

	BaseIT result = null;
	CIS552SO cis552SO;

	public PlainSelectIT(PlainSelect plainSelect, CIS552SO cis552SO, Map<Column, PrimitiveValue> outerQueryColResult)
			throws SQLException {
		this.cis552SO = cis552SO;
		Map<String, Expression> selectionPushedDown = new HashMap<>();
		Expression where = null;
		Map<Tuple, Expression> joinedConditionsExpPushedDown = new HashMap<>();
		Map<Tuple, EqualsTo> joinsExpPushedDown = new HashMap<>();
		Map<String, BaseIT> inMemoryTableResult = new HashMap<>();
		if (plainSelect.getWhere() != null) {
			where = extractExpressions(plainSelect.getWhere(), selectionPushedDown, joinedConditionsExpPushedDown,
					joinsExpPushedDown, false);
		}
//		evaluateInMemoryTable(inMemoryTableResult, plainSelect.getFromItem(), plainSelect.getJoins(),
//				selectionPushedDown);
		FromItem fromItem = plainSelect.getFromItem();
		if (fromItem instanceof Table && inMemoryTableResult.containsKey(((Table) fromItem).getName())) {
			result = inMemoryTableResult.get(((Table) fromItem).getName());
		} else {
			result = new FromItemIT(plainSelect.getFromItem(), selectionPushedDown, cis552SO);
		}
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				FromItem joinFromItem = join.getRightItem();
				if (joinFromItem instanceof Table
						&& inMemoryTableResult.containsKey(((Table) joinFromItem).getName())) {

					BaseIT result2 = inMemoryTableResult.get(((Table) joinFromItem).getName());
					result = new JoinIT(result, result2, joinedConditionsExpPushedDown, joinsExpPushedDown, cis552SO);

				} else {
					BaseIT result2 = new FromItemIT(join.getRightItem(), selectionPushedDown, cis552SO);
					result = new JoinIT(result2, result, joinedConditionsExpPushedDown, joinsExpPushedDown, cis552SO);
				}
			}
		}
		if (where != null) {
			result = new WhereIT(plainSelect.getWhere(), result, cis552SO, outerQueryColResult);
		}
		if (plainSelect.getGroupByColumnReferences() != null) {
			result = new GroupByIT(plainSelect.getGroupByColumnReferences(), plainSelect.getSelectItems(), result,
					cis552SO);
		} else {
			boolean isAgg = false;
			if (!(plainSelect.getSelectItems().get(0) instanceof AllColumns)
					&& !(plainSelect.getSelectItems().get(0) instanceof AllTableColumns)) {
				outer: for (SelectItem selectItem : plainSelect.getSelectItems()) {
					if (((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
						switch (((Function) ((SelectExpressionItem) selectItem).getExpression()).getName()
								.toUpperCase()) {
						case "COUNT":
						case "SUM":
						case "AVG":
						case "MIN":
						case "MAX":
							isAgg = true;
							break outer;
						}
					}
				}
				if (isAgg) {
					result = new AggFunctionIT(result, plainSelect.getSelectItems(), cis552SO);
				} else {
					result = new SelectItemIT(plainSelect.getSelectItems(), result, cis552SO);
				}
			}
		}

		if (plainSelect.getDistinct() != null) {
			result = new DistinctIT(result);
		}
		if (plainSelect.getOrderByElements() != null) {
			result = new OrderByIT(plainSelect.getOrderByElements(), result, cis552SO);
		}
		if (plainSelect.getLimit() != null) {
			result = new LimitIT(plainSelect.getLimit(), result);
		}
	}

	private void evaluateInMemoryTable(Map<String, BaseIT> inMemoryTableResult, FromItem fromItem, List<Join> joins,
			Map<String, Expression> selectionPushedDown) throws SQLException {
		Table largestTable = null;
		long filesize = 0;
		if (fromItem instanceof Table) {
			File tableFile = new File(cis552SO.dataPath, ((Table) fromItem).getName() + ".csv");
			largestTable = (Table) fromItem;
			filesize = tableFile.length();
		}
		if (CollectionUtils.isNotEmpty(joins)) {
			for (Join join : joins) {

				if (join.getRightItem() instanceof Table) {
					File tableFile = new File(cis552SO.dataPath, ((Table) join.getRightItem()).getName() + ".csv");
					if (tableFile.length() > filesize) {
						largestTable = (Table) join.getRightItem();
						filesize = tableFile.length();
					}
				}
			}
		}
		if (largestTable != null) {
			inMemoryTableResult.put(largestTable.getName(),
					new InMemoryTableIT(new FromItemIT(largestTable, selectionPushedDown, cis552SO), cis552SO));
		}
	}

	@Override
	public TableResult getNext() {
		return result.getNext();
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

	private Expression extractExpressions(Expression where, Map<String, Expression> selectionPushedDown,
			Map<Tuple, Expression> joinedConditionsExpPushedDown, Map<Tuple, EqualsTo> joinsExpPushedDown,
			boolean isAndExpression) throws SQLException {
		if (where instanceof AndExpression) {
			Expression leftExpression = extractExpressions(((AndExpression) where).getLeftExpression(),
					selectionPushedDown, joinedConditionsExpPushedDown, joinsExpPushedDown, true);
			Expression rightExpression = extractExpressions(((AndExpression) where).getRightExpression(),
					selectionPushedDown, joinedConditionsExpPushedDown, joinsExpPushedDown, true);
			if (leftExpression == null && rightExpression == null) {
				return null;
			} else if (leftExpression == null) {
				return rightExpression;
			} else if (rightExpression == null) {
				return leftExpression;
			} else {
				return new AndExpression(leftExpression, rightExpression);
			}
		} else {
			if (where instanceof GreaterThan) {
				GreaterThan exp = (GreaterThan) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getRightExpression() instanceof PrimitiveValue) {

							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getRightExpression() instanceof Column) {
							Table rightExpTable = ((Column) exp.getRightExpression()).getTable();
							if (rightExpTable != null && StringUtils.isNotEmpty(rightExpTable.getName())) {
								if (leftExpTable.equals(rightExpTable)) {
									extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
									return null;
								} else {
									extractedJoinedColumn(where, joinedConditionsExpPushedDown, isAndExpression,
											leftExpTable.getName(), rightExpTable.getName());
									return null;
								}
							}
						} else if (exp.getRightExpression() instanceof Function) {
							Function func = (Function) exp.getRightExpression();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null, cis552SO);
							extractedColumn(new GreaterThan(exp.getLeftExpression(), primValue), selectionPushedDown,
									isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof GreaterThanEquals) {
				GreaterThanEquals exp = (GreaterThanEquals) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getRightExpression() instanceof PrimitiveValue) {

							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getRightExpression() instanceof Column) {
							Table rightExpTable = ((Column) exp.getRightExpression()).getTable();
							if (rightExpTable != null && StringUtils.isNotEmpty(rightExpTable.getName())) {
								if (leftExpTable.equals(rightExpTable)) {
									extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
									return null;
								} else {
									extractedJoinedColumn(where, joinedConditionsExpPushedDown, isAndExpression,
											leftExpTable.getName(), rightExpTable.getName());
									return null;
								}
							}
						} else if (exp.getRightExpression() instanceof Function) {
							Function func = (Function) exp.getRightExpression();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null, cis552SO);
							extractedColumn(new GreaterThanEquals(exp.getLeftExpression(), primValue),
									selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof MinorThan) {
				MinorThan exp = (MinorThan) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getRightExpression() instanceof PrimitiveValue) {

							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getRightExpression() instanceof Column) {
							Table rightExpTable = ((Column) exp.getRightExpression()).getTable();
							if (rightExpTable != null && StringUtils.isNotEmpty(rightExpTable.getName())) {
								if (leftExpTable.equals(rightExpTable)) {
									extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
									return null;
								} else {
									extractedJoinedColumn(where, joinedConditionsExpPushedDown, isAndExpression,
											leftExpTable.getName(), rightExpTable.getName());
									return null;
								}
							}
						} else if (exp.getRightExpression() instanceof Function) {
							Function func = (Function) exp.getRightExpression();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null, cis552SO);
							extractedColumn(new MinorThan(exp.getLeftExpression(), primValue), selectionPushedDown,
									isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof MinorThanEquals) {
				MinorThanEquals exp = (MinorThanEquals) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getRightExpression() instanceof PrimitiveValue) {

							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getRightExpression() instanceof Column) {
							Table rightExpTable = ((Column) exp.getRightExpression()).getTable();
							if (rightExpTable != null && StringUtils.isNotEmpty(rightExpTable.getName())) {
								if (leftExpTable.equals(rightExpTable)) {
									extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
									return null;
								} else {
									extractedJoinedColumn(where, joinedConditionsExpPushedDown, isAndExpression,
											leftExpTable.getName(), rightExpTable.getName());
									return null;
								}
							}
						} else if (exp.getRightExpression() instanceof Function) {
							Function func = (Function) exp.getRightExpression();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null, cis552SO);
							extractedColumn(new MinorThanEquals(exp.getLeftExpression(), primValue),
									selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof EqualsTo) {
				EqualsTo exp = (EqualsTo) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getRightExpression() instanceof PrimitiveValue) {

							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getRightExpression() instanceof Column) {
							Table rightExpTable = ((Column) exp.getRightExpression()).getTable();
							if (rightExpTable != null && StringUtils.isNotEmpty(rightExpTable.getName())) {
								if (leftExpTable.equals(rightExpTable)) {
									extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
									return null;
								} else {
									List<String> list = Arrays.asList(leftExpTable.getName(), rightExpTable.getName());
									Collections.sort(list);

									PrimitiveValue[] resultRow = { new StringValue(list.get(0)),
											new StringValue(list.get(1)) };
//									PrimitiveValue[] resultRow = { new StringValue(leftExpTable.getName()),
//											new StringValue(rightExpTable.getName()) };
									joinsExpPushedDown.put(new Tuple(resultRow), exp);
									return null;
								}
							}
						} else if (exp.getRightExpression() instanceof Function) {
							Function func = (Function) exp.getRightExpression();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null, cis552SO);
							extractedColumn(new EqualsTo(exp.getLeftExpression(), primValue), selectionPushedDown,
									isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof NotEqualsTo) {
				NotEqualsTo exp = (NotEqualsTo) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getRightExpression() instanceof PrimitiveValue) {

							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getRightExpression() instanceof Column) {
							Table rightExpTable = ((Column) exp.getRightExpression()).getTable();
							if (rightExpTable != null && StringUtils.isNotEmpty(rightExpTable.getName())) {
								if (leftExpTable.equals(rightExpTable)) {
									extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
									return null;
								} else {
									extractedJoinedColumn(where, joinedConditionsExpPushedDown, isAndExpression,
											leftExpTable.getName(), rightExpTable.getName());
									return null;
								}
							}
						} else if (exp.getRightExpression() instanceof Function) {
							Function func = (Function) exp.getRightExpression();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null, cis552SO);
							extractedColumn(new NotEqualsTo(exp.getLeftExpression(), primValue), selectionPushedDown,
									isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof Between) {
				Between exp = (Between) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (exp.getBetweenExpressionStart() instanceof PrimitiveValue) {
							extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						} else if (exp.getBetweenExpressionStart() instanceof Function) {
							Function func = (Function) exp.getBetweenExpressionStart();
							for (Expression funcParamExp : func.getParameters().getExpressions()) {
								if (!(funcParamExp instanceof PrimitiveValue)) {
									return where;
								}
							}
							PrimitiveValue primValueStart = FunctionEvaluation.applyFunction(null, func, null,
									cis552SO);
							func = (Function) exp.getBetweenExpressionEnd();
							PrimitiveValue primValueEnd = FunctionEvaluation.applyFunction(null, func, null, cis552SO);

							Between bet = new Between();
							bet.setLeftExpression(exp.getLeftExpression());
							bet.setBetweenExpressionStart(primValueStart);
							bet.setBetweenExpressionEnd(primValueEnd);
							extractedColumn(bet, selectionPushedDown, isAndExpression, leftExpTable);
							return null;
						}
					}
				}
			} else if (where instanceof InExpression) {
				InExpression exp = (InExpression) where;
				if (exp.getLeftExpression() instanceof Column) {

					Table leftExpTable = ((Column) exp.getLeftExpression()).getTable();
					if (leftExpTable != null && StringUtils.isNotEmpty(leftExpTable.getName())) {
						if (!(exp.getItemsList() instanceof SubSelect)) {
							if (((ExpressionList) exp.getItemsList()).getExpressions()
									.get(0) instanceof PrimitiveValue) {
								extractedColumn(where, selectionPushedDown, isAndExpression, leftExpTable);
								return null;
							} else if (((ExpressionList) exp.getItemsList()).getExpressions()
									.get(0) instanceof Function) {
								List<Expression> inExpressionList = new ArrayList<>();
								for (Expression inExpres : ((ExpressionList) exp.getItemsList()).getExpressions()) {

									Function func = (Function) inExpres;
									for (Expression funcParamExp : func.getParameters().getExpressions()) {
										if (!(funcParamExp instanceof PrimitiveValue)) {
											return where;
										}
									}
									PrimitiveValue primValue = FunctionEvaluation.applyFunction(null, func, null,
											cis552SO);
									inExpressionList.add(primValue);
								}
								ItemsList itemsList = new ExpressionList(inExpressionList);
								InExpression inExp = new InExpression(exp.getLeftExpression(), itemsList);
								extractedColumn(inExp, selectionPushedDown, isAndExpression, leftExpTable);
								return null;
							}
						}
					}
				}
			}
		}
		return where;
	}

	private void extractedJoinedColumn(Expression where, Map<Tuple, Expression> joinsPushedDown,
			boolean isAndExpression, String leftExpTable, String rightExpTable) {
		List<String> list = Arrays.asList(rightExpTable, leftExpTable);
		Collections.sort(list);

		PrimitiveValue[] resultRow = { new StringValue(list.get(0)), new StringValue(list.get(1)) };
		Expression exp = joinsPushedDown.get(new Tuple(resultRow));
		if (exp != null) {
			if (isAndExpression) {
				exp = new AndExpression(exp, where);
			}
		} else {
			exp = where;
		}
		joinsPushedDown.put(new Tuple(resultRow), exp);
	}

	private void extractedColumn(Expression where, Map<String, Expression> selectionPushedDown, boolean isAndExpression,
			Table leftExpTable) {
		Expression exp = selectionPushedDown.get(leftExpTable.getName());
		if (exp != null) {
			if (isAndExpression) {
				exp = new AndExpression(exp, where);
			}
		} else {
			exp = where;
		}
		selectionPushedDown.put(leftExpTable.getName(), exp);
	}

}
