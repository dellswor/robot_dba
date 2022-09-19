package edu.coloradocollege.cs.robotdba;

import java.util.List;
import java.util.Scanner;

/** Tool for performing some simple introspective tasks in a Postgres database.
 * Intended to help support using Postgres in CP274 deliveries (because Dan has strong feelings about
 * Postgres vs MySQL)
 * @author dellsworth
 *
 */
public class RobotDBA {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		DatabaseFacade db = PostgresFacade.getInstanceCLI(sc);
		String r = "y";
		do {
			System.out.println("1) Display table names");
			System.out.println("2) Display table columns");
			System.out.println("3) Drop all tables");
			System.out.print("Choice: ");
			int o = sc.nextInt();
			if(o<1 || o>3) {
				System.out.println("Not a valid option. Should be 1, 2, or 3");
				continue;
			}
			if(o==1) {
				List<String> names = db.getTableNames();
				for(String name:names) {
					System.out.println("  "+name);
				}
			}
			if(o==2) {
				System.out.print("Table name: ");
				String table = sc.next();
				List<ColumnInfo> cols = db.getColumns(table);
				for(ColumnInfo c:cols) {
					System.out.println("  "+c.colname()+"\t"+c.coltype());
				}
			}
			if(o==3) {
				db.dropAllTables(db.getTableNames());
			}
			System.out.print("Execute another task (Y/N)? ");
			r = sc.next();
		} while(r.toLowerCase().startsWith("y")); 
		System.out.println("RobotDBA execution complete!");
	}
}
