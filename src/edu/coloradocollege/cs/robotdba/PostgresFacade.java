package edu.coloradocollege.cs.robotdba;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class PostgresFacade implements DatabaseFacade {
	private final String DB_URI;
	private final Connection conn;

	private static final String tablesQuery = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'";
	private static final String columnQuery = "SELECT columns.column_name,columns.data_type FROM information_schema.columns where table_name=?";

	/**
	 * Connects the Facade to a specific database on a specific host using a
	 * specific username/password pair
	 * 
	 * @param host hostname or IP of the DB server
	 * @param db   database name to connect to
	 * @param user user to connect as
	 * @param pass password for that user
	 * @throws SQLException when something goes wrong with the driver manager
	 */
	public PostgresFacade(String host, String db, String user, String pass) throws SQLException {
		DB_URI = "jdbc:postgresql://" + host + "/" + db + "?user=" + user + "&password=" + pass;
		conn = DriverManager.getConnection(DB_URI);
	}

	/**
	 * Produces the list of table names currently in this DB
	 * <p><b>Note:</b> If any errors occur, this method will cause the calling program to exit with error code 1.</p>
	 * 
	 * @return Table names as strings
	 */
	public ArrayList<String> getTableNames() {
		ArrayList<String> ret = new ArrayList<String>();
		Statement st;
		try {
			st = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Could not create a Statement to the DB... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		ResultSet rs;
		try {
			rs = st.executeQuery(tablesQuery);
		} catch (SQLException e) {
			System.out.println("Query to gather the tables failed when executed... Exiting...");
			System.out.println(e);
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		try {
			while (rs.next()) {
				ret.add(rs.getString("tablename"));
			}
		} catch (SQLException e) {
			System.out.println("Could not get check for rows in the ResultSet... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		try {
			st.close();
		} catch (SQLException e) {
			System.out.println("Statement could not be closed... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		return ret;
	}

	/**
	 * Produces the list of columns for a particular table
	 * <p><b>Note:</b> If any errors occur, this method will cause the calling program to exit with error code 1.</p>
	 * 
	 * @return ColumnInfo instances
	 */
	public ArrayList<ColumnInfo> getColumns(String table) {
		ArrayList<ColumnInfo> ret = new ArrayList<ColumnInfo>();
		PreparedStatement st;
		try {
			st = conn.prepareStatement(columnQuery);
			st.setString(1, table);
		} catch (SQLException e) {
			System.out.println("Could not create a Statement to the DB... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		ResultSet rs;
		try {
			rs = st.executeQuery();
		} catch (SQLException e) {
			System.out.println("Query to gather the columns failed when executed... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		try {
			while (rs.next()) {
				ret.add(new ColumnInfo(rs.getString(1), rs.getString(2)));
			}
		} catch (SQLException e) {
			System.out.println("Could not get check for rows in the ResultSet... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		try {
			st.close();
		} catch (SQLException e) {
			System.out.println("Statement could not be closed... Exiting...");
			System.exit(1);
			return null; // Execution never gets here but JAVA static analyzer doesn't know exit stops
							// execution
		}
		return ret;
	}

	/**
	 * Drops all of the tables in the current DB
	 * <p><b>Note:</b> If any errors occur, this method will not commit partial changes and will cause the calling program to exit with error code 1.</p>
	 * 
	 */
	public void dropAllTables(List<String> tables) {
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			System.out.println("Could not turn off autocommit... Exiting...");
			System.exit(1);
		}
		Statement st;
		try {
			st = conn.createStatement();
		} catch (SQLException e1) {
			System.out.println("Could not create a Statement to the DB... Exiting...");
			System.exit(1);
			return; // Execution never gets here but JAVA static analyzer doesn't know exit stops execution
		}
		for (String table : tables) {
			try {
				String dropQuery = "DROP TABLE IF EXISTS "+table+" CASCADE";
				st.execute(dropQuery);
			} catch (SQLException e) {
				System.out.println("Query to drop table failed when executed... Exiting...");
				System.out.println(e);
				System.exit(1);
				return;
			}
		}
		try {
			st.close();
		} catch (SQLException e) {
			System.out.println("Statement could not be closed... Exiting...");
			System.exit(1);
			return;
		}
		try {
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			System.out.println("Could not commit the drops... Exiting...");
			System.exit(1);
		}
	}

	
	/**
	 * Generates a new PostgresFacade based on CLI input from the user.
	 * <p><b>Note:</b> If the connection cannot be established, this method will cause the calling program to exit with error code 1.</p>
	 * @return the new instance
	 */
	public static DatabaseFacade getInstanceCLI(Scanner sc) {
		System.out.print("Database host: ");
		String host = sc.next();
		System.out.print("Database name: ");
		String name = sc.next();
		System.out.print("Database username: ");
		String user = sc.next();
		System.out.print("Database password: ");
		String pass = sc.next();
		try {
			return new PostgresFacade(host, name, user, pass);
		} catch (SQLException e) {
			System.out.println("Could not connect to the DB... Exiting...");
			System.exit(1);
			return null;
		}
	}
}
