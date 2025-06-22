package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.utils.DbCred;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * Systematische Oracle-Verbindungsdiagnose
 */
public class debug {
    
    public static void main(String[] args) {
        System.out.println("=== Oracle Connection Debug Test ===\n");
        
        // 1. Credentials anzeigen (ohne Passwort)
        showCredentials();
        
        // 2. Treiber testen
        testDriver();
        
        // 3. Verbindung testen
        testConnection();
        
        // 4. Schema-Zugriff testen
        testSchemaAccess();
    }
    
    private static void showCredentials() {
        System.out.println("--- 1. Credentials Check ---");
        System.out.println("Driver: " + DbCred.driverClass);
        System.out.println("URL: " + DbCred.url);
        System.out.println("User: " + DbCred.user);
        System.out.println("Schema: " + DbCred.schema);
        System.out.println("Password: " + (DbCred.password != null && !DbCred.password.isEmpty() ? 
            "[" + DbCred.password.length() + " Zeichen gesetzt]" : "[LEER oder NULL]"));
        System.out.println();
    }
    
    private static void testDriver() {
        System.out.println("--- 2. Driver Test ---");
        try {
            Class.forName(DbCred.driverClass);
            System.out.println("✅ Oracle JDBC Driver erfolgreich geladen");
        } catch (ClassNotFoundException e) {
            System.out.println("❌ Oracle JDBC Driver nicht gefunden!");
            System.out.println("   Stelle sicher, dass ojdbc.jar im Classpath ist");
            System.out.println("   Error: " + e.getMessage());
            return;
        }
        System.out.println();
    }
    
    private static void testConnection() {
        System.out.println("--- 3. Connection Test ---");
        Connection conn = null;
        
        try {
            System.out.println("Versuche Verbindung herzustellen...");
            conn = DriverManager.getConnection(DbCred.url, DbCred.user, DbCred.password);
            System.out.println("✅ Verbindung erfolgreich hergestellt!");
            
            // Basis-Info abrufen
            System.out.println("   Connection-Klasse: " + conn.getClass().getName());
            System.out.println("   AutoCommit: " + conn.getAutoCommit());
            System.out.println("   ReadOnly: " + conn.isReadOnly());
            
        } catch (Exception e) {
            System.out.println("❌ Verbindung fehlgeschlagen!");
            System.out.println("   Error: " + e.getClass().getSimpleName());
            System.out.println("   Message: " + e.getMessage());
            
            // Spezifische Hilfen
            analyzeConnectionError(e);
            return;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    System.out.println("⚠️  Fehler beim Schließen: " + e.getMessage());
                }
            }
        }
        System.out.println();
    }
    
    private static void testSchemaAccess() {
        System.out.println("--- 4. Schema Access Test ---");
        Connection conn = null;
        
        try {
            conn = DriverManager.getConnection(DbCred.url, DbCred.user, DbCred.password);
            
            try (Statement stmt = conn.createStatement()) {
                // Aktueller User
                ResultSet rs = stmt.executeQuery("SELECT USER FROM DUAL");
                if (rs.next()) {
                    System.out.println("✅ Aktueller DB-User: " + rs.getString(1));
                }
                rs.close();
                
                // Aktuelles Schema
                rs = stmt.executeQuery("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL");
                if (rs.next()) {
                    System.out.println("✅ Aktuelles Schema: " + rs.getString(1));
                }
                rs.close();
                
                // Tabellen im Schema
                rs = stmt.executeQuery("SELECT COUNT(*) FROM USER_TABLES");
                if (rs.next()) {
                    int tableCount = rs.getInt(1);
                    System.out.println("✅ Anzahl Tabellen im Schema: " + tableCount);
                    
                    if (tableCount == 0) {
                        System.out.println("⚠️  WARNUNG: Keine Tabellen gefunden!");
                        System.out.println("   Möglicherweise müssen die SQL-Scripts ausgeführt werden:");
                        System.out.println("   1. tables-create.sql");
                        System.out.println("   2. data-insert.sql");
                    }
                }
                rs.close();
                
                // Prüfe spezifische Testtabellen
                checkTestTables(stmt);
                
            }
            
        } catch (Exception e) {
            System.out.println("❌ Schema-Zugriff fehlgeschlagen: " + e.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    System.out.println("⚠️  Fehler beim Schließen: " + e.getMessage());
                }
            }
        }
        System.out.println();
    }
    
    private static void checkTestTables(Statement stmt) {
        System.out.println("\n📋 Test-Tabellen Check:");
        String[] testTables = {"VERTRAG", "DECKUNGSART", "KUNDE", "PRODUKT", "DECKUNG"};
        
        for (String table : testTables) {
            try {
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
                if (rs.next()) {
                    System.out.println("   ✅ " + table + ": " + rs.getInt(1) + " Datensätze");
                }
                rs.close();
            } catch (Exception e) {
                System.out.println("   ❌ " + table + ": nicht gefunden oder nicht zugreifbar");
            }
        }
    }
    
    private static void analyzeConnectionError(Exception e) {
        String message = e.getMessage();
        
        System.out.println("\n💡 Lösungsvorschläge:");
        
        if (message.contains("ORA-01017")) {
            System.out.println("   ORA-01017: Ungültige Credentials oder nicht autorisiert");
            System.out.println("   → Prüfe Benutzername und Passwort");
            System.out.println("   → Prüfe ob der User existiert und Rechte hat");
            System.out.println("   → Teste mit SQL Developer/Toad");
            System.out.println("   → Mögliche User: system, scott, hr, dein_schema_name");
        }
        
        if (message.contains("ORA-17868") || message.contains("Unknown host")) {
            System.out.println("   ORA-17868: Host nicht erreichbar");
            System.out.println("   → Prüfe die URL: " + DbCred.url);
            System.out.println("   → Ist Oracle-Server/Service gestartet?");
            System.out.println("   → Netzwerkverbindung verfügbar?");
        }
        
        if (message.contains("listener")) {
            System.out.println("   Listener-Problem:");
            System.out.println("   → Oracle Listener läuft nicht");
            System.out.println("   → Falsche Port-Nummer in URL");
            System.out.println("   → Kommando: lsnrctl status");
        }
        
        if (message.contains("TNS")) {
            System.out.println("   TNS-Problem:");
            System.out.println("   → TNS Names-Konfiguration prüfen");
            System.out.println("   → Direkte URL verwenden statt TNS-Name");
        }
    }
}