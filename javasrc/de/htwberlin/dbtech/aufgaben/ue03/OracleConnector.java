package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import de.htwberlin.dbtech.utils.DbCred;

public class OracleConnector {
    public static void main(String[] args) {
        try {
            // Load Oracle JDBC driver
            Class.forName(DbCred.driverClass);

            // Establish connection
            Connection conn = DriverManager.getConnection(DbCred.url, DbCred.user, DbCred.password);
            System.out.println("✅ Connected successfully to Oracle Database!");

            // Simple test query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 'Hello from Oracle!' AS msg FROM dual");

            while (rs.next()) {
                System.out.println("📢 " + rs.getString("msg"));
            }

            // Clean up
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
