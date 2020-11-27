package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

public class WhereIT extends BaseIT {
	BaseIT result = null;
	Expression where = null;
	CIS552SO cis552SO = null;
	Map<Column, PrimitiveValue> outerQueryColResult = null;
	TableResult tableResult;

//	public WhereIT(Expression where, BaseIT result, CIS552SO cis552SO) {
//		this.cis552SO = cis552SO;
//		this.result = result;
//		this.where = where;
//	}

	public WhereIT(Expression where, BaseIT result, CIS552SO cis552SO,
			Map<Column, PrimitiveValue> outerQueryColResult) {
		this.cis552SO = cis552SO;
		this.result = result;
		this.where = where;
		this.outerQueryColResult = outerQueryColResult;
	}

	@Override
	public TableResult getNext() {
		return tableResult;
	}

	@Override
	public boolean hasNext() {
		if (result == null) {
			return false;
		}

		while (result.hasNext()) {
			tableResult = result.getNext();
			try {
				List<Tuple> resultTuples = new ArrayList<>();
				for (Tuple tuple : tableResult.resultTuples) {
					Eval eval = new ExpressionEvaluator(tuple, tableResult, cis552SO, outerQueryColResult);
					PrimitiveValue primValue = eval.eval(where);
					// System.out.println(primValue);
					if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
						resultTuples.add(tuple);
					}
				}
				if (CollectionUtils.isNotEmpty(resultTuples)) {
					tableResult.resultTuples = resultTuples;
					return true;
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	@Override
	public void reset() {
		result.reset();
	}

}
