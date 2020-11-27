package cis552project.iterator;

import java.util.Arrays;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class Tuple {

	public PrimitiveValue[] resultRow = null;

	public Tuple(PrimitiveValue[] resultRow) {
		this.resultRow = resultRow;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(resultRow);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
//		String[] result1 = Arrays.stream(resultRow).map(PrimitiveValue::toRawString).toArray(String[]::new);
//		String[] result2 = Arrays.stream(other.resultRow).map(PrimitiveValue::toRawString).toArray(String[]::new);
//		Arrays.sort(result1);
//		Arrays.sort(result2);
		if (!Arrays.equals(resultRow, other.resultRow))
			return false;
		return true;
	}

	@Override
	public String toString() {
		String[] resultString = new String[resultRow.length];
		for (int i = 0; i < resultRow.length; i++) {
			resultString[i] = resultRow[i].toRawString();
		}
		return String.join("|", resultString);
	}

}
