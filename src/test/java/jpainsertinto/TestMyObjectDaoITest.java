package jpainsertinto;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import jpainsertinto.jpa.MyObjectJpa;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import jpainsertinto.jpa.MyObjectJpaDao;
import jpainsertinto.jpa.MyObjectJpaDaoImpl;
import jpainsertinto.model.MyObjectPojo;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestMyObjectDaoITest {

    private static EntityManagerFactory emf;
    private static MyObjectJpaDao myObjectDao;
    private static EmbeddedPostgres embeddedPostgres;
    private static DataSource dataSource;
    private static int port;
    private static final String SCHEMA_NAME = "public";

    private final String hash = "abc123";
    private final String value1 = "value1";
    private final String value2 = "value2";
    private final String value3 = "value3";
    private final String value4 = "value4";
    private final LocalDate created1 = LocalDate.now();
    private final LocalDate created2 = created1.plusDays(1L);
    private final LocalDateTime updated1 = LocalDateTime.now();
    private final LocalDateTime updated2 = updated1.plusSeconds(1);

    @BeforeAll
    @SneakyThrows
    public static void setup() {

        embeddedPostgres = EmbeddedPostgres.start();
        dataSource = embeddedPostgres.getPostgresDatabase();
        port = embeddedPostgres.getPort();

        Flyway.configure()
                .schemas(SCHEMA_NAME)
                .dataSource(dataSource)
                .load()
                .migrate();

        var jpaProps = new Properties();
        jpaProps.putAll(Map.of(
                "javax.persistence.jdbc.driver", "org.postgresql.Driver",
                "javax.persistence.jdbc.url", "jdbc:postgresql://localhost:" + port + "/postgres",
                "javax.persistence.jdbc.user", "postgres",
                "eclipselink.ddl-generation", "none",
                "eclipselink.ddl-generation.output-mode", "database",
                "eclipselink.logging.level", "FINE",
                "eclipselink.logging.level.sql", "FINE",
                "eclipselink.logging.parameters", "true"
        ));

        emf = Persistence.createEntityManagerFactory("test_objects_pu", jpaProps);
        myObjectDao = new MyObjectJpaDaoImpl(emf);
    }

    @BeforeEach
    public void before() {

    }

    @AfterEach
    public void afterEach() {
        var em = emf.createEntityManager();
        em.getTransaction().begin();
        em.createNativeQuery("TRUNCATE TABLE public.TEST_OBJECTS;").executeUpdate();
        em.getTransaction().commit();
    }

    /**
     * Uses the standard JPA persist method
     * First insert will complete without issues
     * Second insert will fail as it tries to write a record with the same consistent-hash (unique field)
     */
    @Test
    void testUsingNormalPersist() {


        var myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value1)
                .column2(value2)
                .created(created1)
                .updated(updated1)
                .build();

        myObjectDao.insertUsingQuery(myObjectPojo);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage1(myObjectPojo, results1);

        // create a new slightly different version with same hash
        myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value3)
                .column2(value4)
                .created(created2)
                .updated(updated2)
                .build();

        myObjectDao.insertUsingQuery(myObjectPojo);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage2(myObjectPojo, results2);

    }

    /**
     * Uses a JPA native query to run INSERT INTO ... ON CONFLICT DO NOTHING
     * First insert will complete without issues
     * Second insert will effective be skipped as it tries to write a record with the same consistent-hash (unique field)
     */
    @Test
    void testInsertUsingNativeQueryWithDoNothing() {


        var myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value1)
                .column2(value2)
                .created(created1)
                .updated(updated1)
                .build();

        myObjectDao.insertUsingNativeQueryWithDoNothing(myObjectPojo);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage1(myObjectPojo, results1);

        // create a new slightly different version with same hash
        myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value3)
                .column2(value4)
                .created(created2)
                .updated(updated2)
                .build();

        myObjectDao.insertUsingNativeQueryWithDoNothing(myObjectPojo);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage2(myObjectPojo, results2);

    }

    /**
     * Uses a JPA native query to run INSERT INTO ... ON CONFLICT DO NOTHING
     * First insert will complete without issues
     * Second insert will effective be skipped as it tries to write a record with the same consistent-hash (unique field)
     */
    @Test
    void testInsertUsingNativeQueryWithDoUpdate() {


        var myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value1)
                .column2(value2)
                .created(created1)
                .updated(updated1)
                .build();

        myObjectDao.insertUsingNativeQueryWithDoUpdate(myObjectPojo);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage1(myObjectPojo, results1);

        var updated2 = LocalDateTime.now();
        ;

        // create a new slightly different version with same hash
        myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value3)
                .column2(value4)
                .created(created2)
                .updated(updated2)
                .build();

        myObjectDao.insertUsingNativeQueryWithDoUpdate(myObjectPojo);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage2(myObjectPojo, results2);

    }

    /**
     * Uses a JPA native query to run INSERT INTO ... ON CONFLICT DO NOTHING
     * First merge will complete without issues as it's effectively just doing an insert
     * Second merge will fail as it's not actually completing a merge but just trying to insert
     * a record with the same consistent-hash (unique field)
     */
    @Test
    void testUsingNormalMerge() {

        var myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value1)
                .column2(value2)
                .created(created1)
                .updated(updated1)
                .build();

        myObjectDao.insertUsingMerge(myObjectPojo);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage1(myObjectPojo, results1);

        // create a new slightly different version with same hash
        myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value3)
                .column2(value4)
                .created(created2)
                .updated(updated2)
                .build();

        myObjectDao.insertUsingMerge(myObjectPojo);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage2(myObjectPojo, results2);

    }

    /**
     * Uses the standard JPA persist/merge methods
     * First lookup will fail but the insert will complete without issues
     * Second lookup will find the first record and then merge will complete without issue as it will update the
     * exiting record
     */
    @Test
    void testUsingNormalFindAndMerge() {

        var myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value1)
                .column2(value2)
                .created(created1)
                .updated(updated1)
                .build();

        myObjectDao.insertUsingFindAndMerge(myObjectPojo);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage1(myObjectPojo, results1);

        // create a new slightly different version with same hash
        myObjectPojo = MyObjectPojo.builder()
                .hash(hash)
                .column1(value3)
                .column2(value4)
                .created(created2)
                .updated(updated2)
                .build();

        myObjectDao.insertUsingFindAndMerge(myObjectPojo);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage2(myObjectPojo, results2);

    }

    private void checkResultsStage1(MyObjectPojo myObjectPojo, List<MyObjectJpa> results1) {
        assertThat(results1).hasSize(1)
                .allMatch(myObjectJpa -> hash.equals(myObjectJpa.getHash()),
                        "hash SHOULD BE: " + myObjectPojo.getHash())
                .allMatch(myObjectJpa -> value1.equals(myObjectJpa.getColumn1()),
                        "column1 should be: " + myObjectPojo.getColumn1())
                .allMatch(myObjectJpa -> value2.equals(myObjectJpa.getColumn2()),
                        "column2 SHOULD BE: " + myObjectPojo.getHash());
    }

    private void checkResultsStage2(MyObjectPojo myObjectPojo, List<MyObjectJpa> results1) {
        assertThat(results1).hasSize(1)
                .allMatch(myObjectJpa -> hash.equals(myObjectJpa.getHash()),
                        "hash SHOULD BE: " + myObjectPojo.getHash())
                .allMatch(myObjectJpa -> value3.equals(myObjectJpa.getColumn1()),
                        "column1 should be: " + myObjectPojo.getColumn1())
                .allMatch(myObjectJpa -> value4.equals(myObjectJpa.getColumn2()),
                        "column2 SHOULD BE: " + myObjectPojo.getHash());
    }

}
