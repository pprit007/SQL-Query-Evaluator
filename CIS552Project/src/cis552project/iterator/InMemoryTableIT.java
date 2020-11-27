package cis552project.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import cis552project.CIS552SO;

public class InMemoryTableIT extends BaseIT {
	TableResult tableRes = null;
	Iterator<Tuple> iterator;
	List<Tuple> resultTuple = new ArrayList<>();

	public InMemoryTableIT(BaseIT result, CIS552SO cis552SO) {
		while (result.hasNext()) {
			tableRes = result.getNext();
			resultTuple.addAll(tableRes.resultTuples);
		}
		iterator = resultTuple.iterator();

	}

	@Override
	public TableResult getNext() {
		tableRes.resultTuples = Arrays.asList(iterator.next());
		return tableRes;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public void reset() {
		iterator = resultTuple.iterator();
	}

}
