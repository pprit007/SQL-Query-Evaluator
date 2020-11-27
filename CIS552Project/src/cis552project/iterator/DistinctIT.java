package cis552project.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DistinctIT extends BaseIT {

	TableResult finalTableResult = null;
	Iterator<Tuple> resIT = null;
	List<Tuple> finalResultTuples = null;

	public DistinctIT(BaseIT result) {
		List<Tuple> resultTuples = new ArrayList<>();
		while (result.hasNext()) {
			TableResult initialTabRes = result.getNext();
			if (finalTableResult == null) {
				finalTableResult = new TableResult();
				finalTableResult.aliasandTableName.putAll(initialTabRes.aliasandTableName);
//				finalTableResult.colDefMap.putAll(initialTabRes.colDefMap);
				finalTableResult.colPosWithTableAlias.putAll(initialTabRes.colPosWithTableAlias);
				finalTableResult.fromTables.addAll(initialTabRes.fromTables);
			}
			resultTuples.addAll(initialTabRes.resultTuples);
		}
		finalResultTuples = applyDistinct(resultTuples);
		resIT = finalResultTuples.iterator();

	}

	@Override
	public TableResult getNext() {
		finalTableResult.resultTuples = new ArrayList<>();
		finalTableResult.resultTuples.add(resIT.next());
		return finalTableResult;
	}

	@Override
	public boolean hasNext() {
		return resIT.hasNext();
	}

	@Override
	public void reset() {
		resIT = finalResultTuples.iterator();
	}

	private static List<Tuple> applyDistinct(List<Tuple> initialResult) {
		List<Tuple> finalResultList = new ArrayList<>();
		firstLoop: for (Tuple result : initialResult) {
			secondLoop: for (Tuple finalResult : finalResultList) {
				if (!result.equals(finalResult)) {
					continue secondLoop;
				}
				continue firstLoop;
			}
			finalResultList.add(result);
		}

		return finalResultList;
	}

}
