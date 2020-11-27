package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class AggFunctionIT extends BaseIT {

	TableResult finalTableResult = null;
	CIS552SO cis552SO = null;
	int counter = 0;

	public AggFunctionIT(BaseIT result, List<SelectItem> selectItems, CIS552SO cis552SO) throws SQLException {
		this.cis552SO = cis552SO;
		TableResult initialTabRes = null;
		PrimitiveValue[] resultRow = new PrimitiveValue[selectItems.size()];
		long tupleCount = 0;
		while (result.hasNext()) {
			initialTabRes = result.getNext();
			for (Tuple tuple : initialTabRes.resultTuples) {
				tupleCount++;
				for (int i = 0; i < selectItems.size(); i++) {
					Expression exp = ((SelectExpressionItem) selectItems.get(i)).getExpression();
					Eval eval = new ExpressionEvaluator(tuple, initialTabRes, cis552SO, null);
					PrimitiveValue primValue = eval.eval(exp);
					if (primValue instanceof DoubleValue && resultRow[i] != null) {
						if (exp.toString().toUpperCase().contains("COUNT")) {
							primValue = new DoubleValue(tupleCount);
						} else if (exp.toString().toUpperCase().contains("SUM")) {
							primValue = new DoubleValue(resultRow[i].toDouble() + primValue.toDouble());
						} else if (exp.toString().toUpperCase().contains("MIN")) {
							double min = primValue.toDouble();
							if (min > resultRow[i].toDouble())
								primValue = resultRow[i];
						} else if (exp.toString().toUpperCase().contains("MAX")) {
							double max = primValue.toDouble();
							if (max < resultRow[i].toDouble())
								primValue = resultRow[i];
						} else if (exp.toString().toUpperCase().contains("AVG")) {
							double previous = resultRow[i].toDouble() * (tupleCount - 1);
							primValue = new DoubleValue((primValue.toDouble() + previous) / tupleCount);
						}
					}
					resultRow[i] = primValue;
				}
			}
		}

		finalTableResult = new TableResult();
		updateColDefMap(finalTableResult, selectItems);
		finalTableResult.resultTuples.add(new Tuple(resultRow));
	}

	@Override
	public TableResult getNext() {
            counter = 1;
		return finalTableResult;
	}

	@Override
	public boolean hasNext() {
		return counter == 0;
	}

	@Override
	public void reset() {
		counter = 0;
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

}
