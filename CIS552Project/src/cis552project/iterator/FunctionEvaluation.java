package cis552project.iterator;

import java.sql.SQLException;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class FunctionEvaluation {

	public static PrimitiveValue applyFunction(Tuple initialResult, Function funExp, TableResult finalTableResult,
			CIS552SO cis552so) throws SQLException {
		String funName = funExp.getName().toUpperCase();
		switch (funName) {
		case "DATE_PART":
		case "DATEPART":
			return evaluateDatePart(initialResult, funExp, finalTableResult, cis552so);
		case "DATE":
			return evaluateDate(initialResult, funExp, finalTableResult, cis552so);
		case "COUNT":
			return evaluateCount(initialResult, funExp, finalTableResult, cis552so);
		default:
			return fetchEvaluatedExpressions(initialResult, funExp.getParameters().getExpressions().get(0),
					finalTableResult, cis552so);
		}
	}

	private static PrimitiveValue evaluateDate(Tuple initialResult, Function funExp, TableResult finalTableResult,
			CIS552SO cis552so) throws SQLException {

		PrimitiveValue pValue = fetchEvaluatedExpressions(initialResult, funExp.getParameters().getExpressions().get(0),
				finalTableResult, cis552so);

		return new DateValue(pValue.toString().replace("'", ""));

	}

	private static PrimitiveValue evaluateDatePart(Tuple initialResult, Function funExp, TableResult finalTableResult,
			CIS552SO cis552so) throws SQLException {

		PrimitiveValue pValue = fetchEvaluatedExpressions(initialResult, funExp.getParameters().getExpressions().get(1),
				finalTableResult, cis552so);

		long value = 0;
		switch (funExp.getParameters().getExpressions().get(0).toString().toUpperCase().replace("'", "")) {
		case "YEAR":
		case "YYYY":
		case "YY":
			value = 1900 + new DateValue(pValue.toString().replace("'", "")).getYear();
			break;
		case "MONTH":
		case "MM":
		case "M":
			value = new DateValue(pValue.toString().replace("'", "")).getMonth();
			break;
		case "DAY":
		case "DD":
		case "D":
			value = new DateValue(pValue.toString().replace("'", "")).getDate();
			break;
		}
		return new LongValue(value);

	}

	private static PrimitiveValue evaluateCount(Tuple initialResult, Function funExp, TableResult finalTableResult,
			CIS552SO cis552so) throws SQLException {
		return new DoubleValue(1);
	}

	private static PrimitiveValue fetchEvaluatedExpressions(Tuple initialResult, Expression expression,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		if (initialResult != null) {
			Eval eval = new ExpressionEvaluator(initialResult, finalTableResult, cis552so, null);
			PrimitiveValue value = eval.eval(expression);
			return value;
		} else {
			Eval eval = new ExpressionEvaluator(null, finalTableResult, cis552so, null);
			return eval.eval(expression);
		}
	}

}
