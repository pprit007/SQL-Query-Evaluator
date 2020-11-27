/*
` * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.List;

import cis552project.iterator.BaseIT;
import cis552project.iterator.SelectBodyIT;
import cis552project.iterator.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class CIS552Project {

	public static void main(String[] args) {
		CIS552SO cis552SO = new CIS552SO();
		String commandsLoc = args[0];
		cis552SO.dataPath = args[1];

		try {
			List<String> commands = CIS552ProjectUtils.readCommands(commandsLoc);
			for (String command : commands) {
				try (Reader reader = new StringReader(command)) {
					CCJSqlParser parser = new CCJSqlParser(reader);
					Statement statement = parser.Statement();

					if (statement instanceof CreateTable) {
						createTable(statement, cis552SO);
						System.out.println("Table Create Successfully");
					} else if (statement instanceof Select) {
						Select select = (Select) statement;
						BaseIT result = new SelectBodyIT(select.getSelectBody(), cis552SO, null);
						while (result.hasNext()) {
							printResult(result.getNext().resultTuples);
						}
					}
				} catch (ParseException | SQLException e) {
					System.out.println("Exception : " + e.getLocalizedMessage());
				} finally {
					System.out.println("=");
				}
			}
		} catch (IOException e) {

			System.out.println("Commands location was not identified. Please see the below exception.");
			System.out.println("Exception : " + e.getLocalizedMessage());
		}

	}

	private static void createTable(Statement statement, CIS552SO cis552SO) {
		CreateTable createTable = (CreateTable) statement;
		String tableName = createTable.getTable().getName();
		TableColumnData tableColData = new TableColumnData(new Table(tableName), createTable.getColumnDefinitions());
		cis552SO.tables.put(tableName, tableColData);
	}

	private static void printResult(List<Tuple> resultTuples) throws IOException {
		for (Tuple resultTuple : resultTuples) {
			System.out.println(resultTuple.toString());
		}
	}
}
