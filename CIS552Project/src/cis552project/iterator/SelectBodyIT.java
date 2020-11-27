package cis552project.iterator;

import java.sql.SQLException;
import java.util.Map;

import cis552project.CIS552SO;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;

public class SelectBodyIT extends BaseIT {

	BaseIT result = null;

//	public SelectBodyIT(SelectBody selectBody, CIS552SO cis552SO) throws SQLException {
//		if (selectBody instanceof PlainSelect) {
//			result = new PlainSelectIT((PlainSelect) selectBody, cis552SO, null);
//		}
//	}

	public SelectBodyIT(SelectBody selectBody, CIS552SO cis552so, Map<Column, PrimitiveValue> outerQueryColResult)
			throws SQLException {
		if (selectBody instanceof PlainSelect) {
			result = new PlainSelectIT((PlainSelect) selectBody, cis552so, outerQueryColResult);
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

}
