package de.htwberlin.dbtech.aufgaben.ue03;
/**
 * @author Ingo Classen
 */
import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * VersicherungService - Implementierung basierend auf dem korrekten Datenbankschema
 */
public class VersicherungService implements IVersicherungService {
    private static final Logger L = LoggerFactory.getLogger(VersicherungService.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("unused")
    private Connection useConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        L.info("createDeckung aufgerufen mit:");
        L.info("vertragsId: " + vertragsId);
        L.info("deckungsartId: " + deckungsartId);
        L.info("deckungsbetrag: " + deckungsbetrag);

        // Validierungskette 
        isVertragExisting(vertragsId);                                    
        isDeckungsartExisting(deckungsartId);                             
        isDeckungsartPassendZuProdukt(vertragsId, deckungsartId);         
        isDeckungsbetragGueltig(deckungsartId, deckungsbetrag);           
        isDeckungspreisVorhanden(deckungsartId, deckungsbetrag);          
        isDeckungsartRegelkonform(vertragsId, deckungsartId, deckungsbetrag); 

        // Deckung in Datenbank einfügen
        insertDeckung(vertragsId, deckungsartId, deckungsbetrag);   

        L.info("createDeckung erfolgreich beendet");
    }

    /**
     * @param vertragsId Die ID des zu prüfenden Vertrags
     * @throws VertragExistiertNichtException wenn der Vertrag nicht existiert
     */
    private void isVertragExisting(Integer vertragsId) {
        L.debug("Überprüfe Existenz von Vertrag mit ID: " + vertragsId);
        
        String sql = "SELECT 1 FROM Vertrag WHERE ID = ?";
        
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    L.warn("Vertrag mit ID " + vertragsId + " existiert nicht");
                    throw new VertragExistiertNichtException(vertragsId);
                }
                L.debug("✅ Vertrag mit ID " + vertragsId + " existiert");
            }
        } catch (SQLException e) {
            L.error("Datenbankfehler beim Prüfen der Vertragsexistenz", e);
            throw new DataException("Fehler beim Prüfen der Vertragsexistenz: " + e.getMessage(), e);
        }
    }

    /**
     * Überprüft, ob eine Deckungsart mit der gegebenen ID existiert
     * @param deckungsartId Die ID der zu prüfenden Deckungsart
     * @throws DeckungsartExistiertNichtException wenn die Deckungsart nicht existiert
     */
    private void isDeckungsartExisting(Integer deckungsartId) {
        L.debug("Überprüfe Existenz von Deckungsart mit ID: " + deckungsartId);
        
        String sql = "SELECT 1 FROM Deckungsart WHERE ID = ?";
        
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    L.warn("Deckungsart mit ID " + deckungsartId + " existiert nicht");
                    throw new DeckungsartExistiertNichtException(deckungsartId);
                }
                L.debug("✅ Deckungsart mit ID " + deckungsartId + " existiert");
            }
        } catch (SQLException e) {
            L.error("Datenbankfehler beim Prüfen der Deckungsart-Existenz", e);
            throw new DataException("Fehler beim Prüfen der Deckungsart-Existenz: " + e.getMessage(), e);
        }
    }

    /**
     * @param vertragsId Die ID des Vertrags
     * @param deckungsartId Die ID der Deckungsart
     * @throws DeckungsartPasstNichtZuProduktException wenn die Deckungsart nicht zum Produkt passt
     */
    private void isDeckungsartPassendZuProdukt(Integer vertragsId, Integer deckungsartId) {
        L.debug("Überprüfe ob Deckungsart " + deckungsartId + " zu Produkt von Vertrag " + vertragsId + " passt");
        
        // Prüfe ob Deckungsart.Produkt_FK = Vertrag.Produkt_FK
        String sql = "SELECT 1 FROM Vertrag v, Deckungsart d " +
                     "WHERE v.ID = ? AND d.ID = ? AND v.Produkt_FK = d.Produkt_FK";
        
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);
            stmt.setInt(2, deckungsartId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    L.warn("Deckungsart " + deckungsartId + " passt nicht zum Produkt von Vertrag " + vertragsId);
                    throw new DeckungsartPasstNichtZuProduktException();
                }
                L.debug("✅ Deckungsart " + deckungsartId + " passt zum Produkt von Vertrag " + vertragsId);
            }
        } catch (SQLException e) {
            L.error("Datenbankfehler beim Prüfen der Deckungsart-Produkt-Kompatibilität", e);
            throw new DataException("Fehler beim Prüfen der Deckungsart-Produkt-Kompatibilität: " + e.getMessage(), e);
        }
    }

    /**
     * Überprüft, ob der Deckungsbetrag für die Deckungsart gültig ist.
     * @param deckungsartId Die ID der Deckungsart
     * @param deckungsbetrag Der zu prüfende Deckungsbetrag
     * @throws UngueltigerDeckungsbetragException wenn der Deckungsbetrag nicht gültig ist
     */
    private void isDeckungsbetragGueltig(Integer deckungsartId, BigDecimal deckungsbetrag) {
        L.debug("Überprüfe Gültigkeit von Deckungsbetrag " + deckungsbetrag + " für Deckungsart " + deckungsartId);
        
        String sql = "SELECT 1 FROM Deckungsbetrag WHERE Deckungsart_FK = ? AND Deckungsbetrag = ?";
        
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);
            stmt.setBigDecimal(2, deckungsbetrag);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    L.warn("Deckungsbetrag " + deckungsbetrag + " ist nicht gültig für Deckungsart " + deckungsartId);
                    throw new UngueltigerDeckungsbetragException(deckungsbetrag);
                }
                L.debug("✅ Deckungsbetrag " + deckungsbetrag + " ist gültig für Deckungsart " + deckungsartId);
            }
        } catch (SQLException e) {
            L.error("Datenbankfehler beim Prüfen des Deckungsbetrags", e);
            throw new DataException("Fehler beim Prüfen des Deckungsbetrags: " + e.getMessage(), e);
        }
    }

    /**
     * @param deckungsartId Die ID der Deckungsart
     * @param deckungsbetrag Der Deckungsbetrag
     * @throws DeckungspreisNichtVorhandenException wenn kein gültiger Preis vorhanden ist
     */
    private void isDeckungspreisVorhanden(Integer deckungsartId, BigDecimal deckungsbetrag) {
        L.debug("Überprüfe Deckungspreis für Deckungsart " + deckungsartId + " und Betrag " + deckungsbetrag);
        
        // Prüfe ob ein gültiger Preis zum heutigen Datum existiert
        String sql = "SELECT 1 FROM Deckungspreis dp " +
                     "JOIN Deckungsbetrag db ON dp.Deckungsbetrag_FK = db.ID " +
                     "WHERE db.Deckungsart_FK = ? AND db.Deckungsbetrag = ? " +
                     "AND SYSDATE BETWEEN dp.Gueltig_Von AND dp.Gueltig_Bis";
        
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);
            stmt.setBigDecimal(2, deckungsbetrag);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    L.warn("Kein gültiger Deckungspreis für Deckungsart " + deckungsartId + 
                           " und Betrag " + deckungsbetrag + " verfügbar");
                    throw new DeckungspreisNichtVorhandenException(deckungsbetrag);
                }
                L.debug("✅ Gültiger Deckungspreis gefunden");
            }
        } catch (SQLException e) {
            L.error("Datenbankfehler beim Prüfen des Deckungspreises", e);
            throw new DataException("Fehler beim Prüfen des Deckungspreises: " + e.getMessage(), e);
        }
    }

    /**
     * Überprüft Ablehnungsregeln.
     */
    private void isDeckungsartRegelkonform(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
    L.debug("Überprüfe Regelkonformität für Vertrag " + vertragsId + 
            ", Deckungsart " + deckungsartId + ", Betrag " + deckungsbetrag);

    String sql = "SELECT ar.R_Betrag, ar.R_Alter, " +
                 "TRUNC(MONTHS_BETWEEN(v.Versicherungsbeginn, k.Geburtsdatum) / 12) as \"Alter\" " +
                 "FROM Ablehnungsregel ar, Vertrag v, Kunde k " +
                 "WHERE ar.Deckungsart_FK = ? AND v.ID = ? AND k.ID = v.Kunde_FK " +
                 "ORDER BY ar.LfdNr ";

    try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
        stmt.setInt(1, deckungsartId);
        stmt.setInt(2, vertragsId);

        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String rBetrag = rs.getString("R_Betrag");
                String rAlter = rs.getString("R_Alter");
                int kundenAlter = rs.getInt("Alter");

                L.debug("Prüfe Regel: R_Betrag=" + rBetrag + ", R_Alter=" + rAlter + ", KundenAlter=" + kundenAlter);

                boolean alterVerletzt = isAlterRegelVerletzt(rAlter, kundenAlter);
                boolean betragVerletzt = isBetragRegelVerletzt(rBetrag, deckungsbetrag);

                L.debug("Alterregel verletzt? " + alterVerletzt + ", Betragsregel verletzt? " + betragVerletzt);

                // Beide Regeln müssen erfüllt sein, um abzulehnen
                if (alterVerletzt || betragVerletzt) {
                    L.warn("Ablehnungsregel verletzt (Alter & Betrag): R_Alter=" + rAlter + ", R_Betrag=" + rBetrag + 
                           ", KundenAlter=" + kundenAlter + ", Deckungsbetrag=" + deckungsbetrag);
                    throw new DeckungsartNichtRegelkonformException(deckungsartId);
                }
            }
            L.debug("✅ Alle Regelprüfungen bestanden");
        }
    } catch (SQLException e) {
        L.error("Datenbankfehler beim Prüfen der Regelkonformität", e);
        throw new DataException("Fehler beim Prüfen der Regelkonformität: " + e.getMessage(), e);
    }
}


/**
 *  Prüft ob eine Altersregel verletzt ist
 */
private boolean isAlterRegelVerletzt(String regelAlter, int alter) {
    if (regelAlter == null || regelAlter.trim().isEmpty()) return false;

    regelAlter = regelAlter.trim();

    try {
        if (regelAlter.startsWith(">=")) {
            int grenzwert = Integer.parseInt(regelAlter.substring(2).trim());
            return alter >= grenzwert;
        } else if (regelAlter.startsWith(">")) {
            int grenzwert = Integer.parseInt(regelAlter.substring(1).trim());
            return alter > grenzwert;
        } else if (regelAlter.startsWith("<=")) {
            int grenzwert = Integer.parseInt(regelAlter.substring(2).trim());
            return alter <= grenzwert;
        } else if (regelAlter.startsWith("<")) {
            int grenzwert = Integer.parseInt(regelAlter.substring(1).trim());
            return alter < grenzwert;
        } else if (regelAlter.matches("\\d+")) {
            int grenzwert = Integer.parseInt(regelAlter);
            return alter == grenzwert;
        }
    } catch (NumberFormatException e) {
        L.warn("❌ Ungültiges Format für R_Alter: " + regelAlter);
    }

    return false;
}


private boolean isBetragRegelVerletzt(String regelBetrag, BigDecimal betrag) {
    if (regelBetrag == null || regelBetrag.trim().isEmpty() || regelBetrag.equals("- -")) return false;

    regelBetrag = regelBetrag.trim();
    if (regelBetrag.startsWith(">=")) {
        BigDecimal grenzwert = new BigDecimal(regelBetrag.substring(2).trim());
        return betrag.compareTo(grenzwert) >= 0;
    }
    return false;
}


    /**
     * @param vertragsId Die ID des Vertrags
     * @param deckungsartId Die ID der Deckungsart
     * @param deckungsbetrag Der Deckungsbetrag
     */
    private void insertDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        L.debug(" Füge Deckung ein: Vertrag=" + vertragsId + ", Deckungsart=" + deckungsartId + ", Betrag=" + deckungsbetrag);
        
        String sql = "INSERT INTO Deckung (Vertrag_FK, Deckungsart_FK, Deckungsbetrag) VALUES (?, ?, ?)";
        
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);
            stmt.setInt(2, deckungsartId);
            stmt.setBigDecimal(3, deckungsbetrag);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected == 1) {
                L.info("✅ Deckung erfolgreich eingefügt");
            } else {
                L.error("Unerwartete Anzahl betroffener Zeilen: " + rowsAffected);
                throw new DataException("Fehler beim Einfügen der Deckung");
            }
        } catch (SQLException e) {
            L.error("Datenbankfehler beim Einfügen der Deckung", e);
            throw new DataException("Fehler beim Einfügen der Deckung: " + e.getMessage(), e);
        }
    }
}