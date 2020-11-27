package cis552project.iterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class TableResult {
	// Result Information
	public Map<Column, Integer> colPosWithTableAlias = new HashedMap<>();
	public Map<String, String> aliasandTableName = new HashMap<>();
	public List<Table> fromTables = new ArrayList<>();
//	public Map<String, ColumnDefinition> colDefMap = new HashMap<>();

	// Result Rows
	public List<Tuple> resultTuples = new ArrayList<>();

}
