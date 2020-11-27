/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableColumnData {

	public Table table = new Table();
	public Collection<ColumnDefinition> colDefList = new ArrayList<>();
	public List<Column> colList = new ArrayList<>();
	public Map<String, ColumnDefinition> colDefMap = new HashMap<>();

	public TableColumnData(Table table, Collection<ColumnDefinition> colDefList) {
		this.table = table;
		this.colDefList = colDefList;
		for (ColumnDefinition colDef : colDefList) {
			String columnName = colDef.getColumnName();
			Column column = new Column(table, columnName);
			colList.add(column);
			colDefMap.put(columnName, colDef);
		}
	}

	public boolean containsColumn(String columnName) {
		for (Column col : colList) {
			if (col.getColumnName().equals(columnName)) {
				return true;
			}
		}
		return false;
	}

}
