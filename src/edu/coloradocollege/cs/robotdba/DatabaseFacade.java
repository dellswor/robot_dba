package edu.coloradocollege.cs.robotdba;

import java.util.ArrayList;
import java.util.List;

public interface DatabaseFacade {
	/**
	 * Produces the list of table names currently in this DB
	 * 
	 * @return Table names as strings
	 */
	public ArrayList<String> getTableNames();
	
	/**
	 * Produces the list of columns for a particular table
	 * 
	 * @return ColumnInfo instances
	 */
	public ArrayList<ColumnInfo> getColumns(String table);
	
	/**
	 * Drops all of the tables in the current DB
	 * 
	 */
	public void dropAllTables(List<String> tables);
}
