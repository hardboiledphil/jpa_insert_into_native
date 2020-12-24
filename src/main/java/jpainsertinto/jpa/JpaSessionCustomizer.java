package jpainsertinto.jpa;


import org.eclipse.persistence.config.SessionCustomizer;
import org.eclipse.persistence.sessions.Session;

/**
 * Eclipselink component to customize the session.
 * We currently use this to set the DB schema name used by the service via config.
 */
public final class JpaSessionCustomizer implements SessionCustomizer {
    private final String schemaName;

    public JpaSessionCustomizer() {
        schemaName = "public";
    }

    @Override
    public void customize(final Session session) {
        session.getLogin().setTableQualifier(schemaName);
    }
}
