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
    private static final String SCHEMA_NAME = "public";

    private static final String HASH = "abc123";
    private static final String VALUE1 = "value1";
    private static final String VALUE2 = "value2";
    private static final String VALUE3 = "value3";
    private static final String VALUE4 = "value4";
    private static final LocalDate CREATED1 = LocalDate.now();
    private static final LocalDate CREATED2 = CREATED1.plusDays(1L);
    private static final LocalDateTime UPDATED1 = LocalDateTime.now();
    private static final LocalDateTime UPDATED2 = UPDATED1.plusSeconds(1);
    private static MyObjectPojo myObjectPojo1;
    private static MyObjectPojo myObjectPojo2;

    @BeforeAll
    @SneakyThrows
    public static void setup() {

         myObjectPojo1 = MyObjectPojo.builder()
                .hash(HASH)
                .column1(VALUE1)
                .column2(VALUE2)
                .created(CREATED1)
                .updated(UPDATED1)
                .build();

        // create a new slightly different version with same hash
        myObjectPojo2 = MyObjectPojo.builder()
                .hash(HASH)
                .column1(VALUE3)
                .column2(VALUE4)
                .created(CREATED2)
                .updated(UPDATED2)
                .build();

        EmbeddedPostgres embeddedPostgres = EmbeddedPostgres.start();
        DataSource dataSource = embeddedPostgres.getPostgresDatabase();
        int port = embeddedPostgres.getPort();

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
        myObjectDao = new MyObjectJpaDaoImpl(emf, "org.postgresql.Driver", "jdbc:postgresql://localhost:" + port + "/postgres", "postgres");
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

        myObjectDao.insertUsingQuery(myObjectPojo1);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage(results1, myObjectPojo1);

        myObjectDao.insertUsingQuery(myObjectPojo2);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage(results2, myObjectPojo2);

    }

    /**
     * Uses a JPA native query to run INSERT INTO ... ON CONFLICT DO NOTHING
     * First insert will complete without issues
     * Second insert will effective be skipped as it tries to write a record with the same consistent-hash (unique field)
     */
    @Test
    void testInsertUsingNativeQueryWithDoNothing() {

        myObjectDao.insertUsingNativeQueryWithDoNothing(myObjectPojo1);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage(results1, myObjectPojo1);

        myObjectDao.insertUsingNativeQueryWithDoNothing(myObjectPojo2);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage(results2, myObjectPojo2);

    }

    /**
     * Uses a JPA native query to run INSERT INTO ... ON CONFLICT DO NOTHING
     * First insert will complete without issues
     * Second insert will effective be skipped as it tries to write a record with the same consistent-hash (unique field)
     */
    @Test
    void testInsertUsingNativeQueryWithDoUpdate() {


        myObjectDao.insertUsingNativeQueryWithDoUpdate(myObjectPojo1);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage(results1, myObjectPojo1);

        var updated2 = LocalDateTime.now();

        myObjectDao.insertUsingNativeQueryWithDoUpdate(myObjectPojo2);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage(results2, myObjectPojo2);

    }

    /**
     * Uses a JPA native query to run INSERT INTO ... ON CONFLICT DO NOTHING
     * First merge will complete without issues as it's effectively just doing an insert
     * Second merge will fail as it's not actually completing a merge but just trying to insert
     * a record with the same consistent-hash (unique field)
     */
    @Test
    void testUsingNormalMerge() {


        myObjectDao.insertUsingMerge(myObjectPojo1);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage(results1, myObjectPojo1);


        myObjectDao.insertUsingMerge(myObjectPojo2);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage(results2, myObjectPojo2);

    }

    /**
     * Uses the standard JPA persist/merge methods
     * First lookup will fail but the insert will complete without issues
     * Second lookup will find the first record and then merge will complete without issue as it will update the
     * exiting record
     */
    @Test
    void testUsingNormalFindAndMerge() {


        myObjectDao.insertUsingFindAndMerge(myObjectPojo1);

        var results1 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage(results1, myObjectPojo1);


        myObjectDao.insertUsingFindAndMerge(myObjectPojo1);

        var results2 = myObjectDao.getAll();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage(results2, myObjectPojo2);

    }

    @Test
    void testUsingNativeJDBC() {


        myObjectDao.insertUsingNativeJDBC(myObjectPojo1);

        var results1 = myObjectDao.getAllUsingNativeJDBC();

        log.info("*****************");
        log.info("results1: {}", results1.toString());
        log.info("*****************");

        checkResultsStage(results1, myObjectPojo1);


        myObjectDao.insertUsingNativeJDBC(myObjectPojo2);

        var results2 = myObjectDao.getAllUsingNativeJDBC();

        log.info("*****************");
        log.info("results2: {}", results2.toString());
        log.info("*****************");

        checkResultsStage(results2, myObjectPojo2);

    }

    private void checkResultsStage(List<MyObjectJpa> results, MyObjectPojo myObjectPojo) {
        assertThat(results).hasSize(1)
                .allMatch(myObjectJpa -> myObjectJpa.getHash().equals(myObjectPojo.getHash()),
                        "hash SHOULD BE: " + myObjectPojo.getHash())
                .allMatch(myObjectJpa -> myObjectJpa.getColumn1().equals(myObjectPojo.getColumn1()),
                        "column1 should be: " + myObjectPojo.getColumn1())
                .allMatch(myObjectJpa -> myObjectJpa.getColumn2().equals(myObjectPojo.getColumn2()),
                        "column2 SHOULD BE: " + myObjectPojo.getColumn2());
//                .allMatch(myObjectJpa -> myObjectJpa.getCreated().compareTo(myObjectPojo.getCreated()) == 0,
//                        "created SHOULD BE: " + myObjectPojo.getCreated());
    }


}
