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
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromItemIT extends BaseIT {

	BaseIT result = null;
	Map<String, Expression> selectionPushedDown;
	CIS552SO cis552SO;
	TableResult tableResult;

	public FromItemIT(FromItem fromItem, Map<String, Expression> selectionPushedDown, CIS552SO cis552SO)
			throws SQLException {
		this.cis552SO = cis552SO;
		this.selectionPushedDown = selectionPushedDown;
		if (fromItem instanceof Table) {
			result = new TableIT((Table) fromItem, cis552SO);
		}

		if (fromItem instanceof SubSelect) {
			result = new SubSelectIT((SubSelect) fromItem, cis552SO, null);
		}
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
			Expression exp = null;
			for (Table table : tableResult.fromTables) {
				String aliasName = table.getAlias() != null ? table.getAlias() : table.getName();
				Expression nextExp = selectionPushedDown.get(aliasName);
				if (nextExp != null) {
					if (exp != null) {
						exp = new AndExpression(exp, nextExp);
					} else {
						exp = nextExp;
					}
				}
			}
			if (exp != null) {
				try {
					List<Tuple> resulTuples = new ArrayList<>();
					for (Tuple tuple : tableResult.resultTuples) {
						Eval eval = new ExpressionEvaluator(tuple, tableResult, cis552SO, null);
						PrimitiveValue primValue = eval.eval(exp);
						if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
							resulTuples.add(tuple);
						}
					}
					if (CollectionUtils.isNotEmpty(resulTuples)) {
						tableResult.resultTuples = resulTuples;
						return true;
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}

			} else {
				return true;
			}
		}
		return false;
	}

	@Override
	public void reset() {
		result.reset();
	}

}
