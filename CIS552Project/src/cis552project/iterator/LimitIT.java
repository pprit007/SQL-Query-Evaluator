/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.statement.select.Limit;

/**
 *
 * @author anush
 */
public class LimitIT extends BaseIT {
	Limit limit;
	long current_position = 0;
	List<Tuple> resultTuples = new ArrayList<>();
	BaseIT result = null;
	Iterator<Tuple> resIT;
	TableResult tableResult;

	LimitIT(Limit limit, BaseIT result) {
		this.limit = limit;
		this.result = result;
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

		long rowsRequired = limit.getRowCount() - current_position;
		while (rowsRequired > 0 && result.hasNext()) {
			tableResult = result.getNext();
			if (rowsRequired > tableResult.resultTuples.size()) {
				current_position += tableResult.resultTuples.size();
				return true;
			} else {
				List<Tuple> tupleList = new ArrayList<>();
				for (long i = rowsRequired; i > 0; i = limit.getRowCount() - current_position) {
					current_position++;
					tupleList.add(tableResult.resultTuples.get((int) i));
				}
			}
		}
		return false;
	}

	@Override
	public void reset() {
		current_position = 0;
		result.reset();
	}

}