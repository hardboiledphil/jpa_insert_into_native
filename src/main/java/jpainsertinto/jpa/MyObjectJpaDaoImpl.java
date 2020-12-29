package jpainsertinto.jpa;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import jpainsertinto.model.MyObjectPojo;

import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class MyObjectJpaDaoImpl implements MyObjectJpaDao {

    private final EntityManagerFactory emf;
    private final static ZoneId UTC_TIME_ZONE = ZoneId.of("UTC");
    private final String driver;
    private final String serverURL;
    private final String user;

    public MyObjectJpaDaoImpl(final EntityManagerFactory emf,
                              final String driver,
                              final String serverURL,
                              final String user) {
        this.emf = emf;
        this.driver = driver;
        this.serverURL = serverURL;
        this.user = user;
    }

    @Override
    public void insertUsingQuery(MyObjectPojo myObjectPojo) {

        var em = emf.createEntityManager();

        em.getTransaction().begin();

        // map pojo to dao
        var myObjectJpa = MyObjectJpa.builder()
                .hash(myObjectPojo.getHash())
                .column1(myObjectPojo.getColumn1())
                .column2(myObjectPojo.getColumn2())
                .created(Date.from(myObjectPojo.getCreated().atStartOfDay(UTC_TIME_ZONE).toInstant()))
                .updated(Date.from(myObjectPojo.getUpdated().atZone(ZoneId.systemDefault()).toInstant()))
                .build();

        em.persist(myObjectJpa);

        em.getTransaction().commit();
        em.close();

    }

    public void insertUsingNativeQueryWithDoNothing(MyObjectPojo myObjectPojo) {

        var em = emf.createEntityManager();

        em.getTransaction().begin();

        // map pojo to dao
        var myObjectJpa = MyObjectJpa.builder()
                .hash(myObjectPojo.getHash())
                .column1(myObjectPojo.getColumn1())
                .column2(myObjectPojo.getColumn2())
                .created(Date.from(myObjectPojo.getCreated().atStartOfDay(UTC_TIME_ZONE).toInstant()))
                .updated(Date.from(myObjectPojo.getUpdated().atZone(ZoneId.systemDefault()).toInstant()))
                .build();

        em.createNativeQuery("INSERT INTO public.TEST_OBJECTS"
                + " ( HASH, COLUMN_1, COLUMN_2, CREATED, UPDATED )"
                + " VALUES (?, ? ,?, ?, ?)"
                + " ON CONFLICT (HASH)"
                + " DO NOTHING ;")
                .setParameter(1, myObjectJpa.getHash())
                .setParameter(2, myObjectJpa.getColumn1())
                .setParameter(3, myObjectJpa.getColumn2())
                .setParameter(4, myObjectJpa.getCreated())
                .setParameter(5, myObjectJpa.getUpdated())
                .executeUpdate();

        em.getTransaction().commit();
        em.close();

    }

    public void insertUsingNativeQueryWithDoUpdate(MyObjectPojo myObjectPojo) {

        var em = emf.createEntityManager();

        em.getTransaction().begin();

        // map pojo to dao
        var myObjectJpa = MyObjectJpa.builder()
                .hash(myObjectPojo.getHash())
                .column1(myObjectPojo.getColumn1())
                .column2(myObjectPojo.getColumn2())
                .created(Date.from(myObjectPojo.getCreated().atStartOfDay(UTC_TIME_ZONE).toInstant()))
                .updated(Date.from(myObjectPojo.getUpdated().atZone(ZoneId.systemDefault()).toInstant()))
                .build();

        em.createNativeQuery("INSERT INTO public.TEST_OBJECTS "
                + " ( HASH, COLUMN_1, COLUMN_2, CREATED, UPDATED )"
                + " VALUES (?, ? ,?, ?, ?)"
                + " ON CONFLICT (HASH)"
                + " DO UPDATE"
                + " SET COLUMN_1 = excluded.COLUMN_1,"
                + "     COLUMN_2 = excluded.COLUMN_2,"
                + "     CREATED  = excluded.CREATED,"
                + "     UPDATED  = excluded.UPDATED" )
                .setParameter(1, myObjectJpa.getHash())
                .setParameter(2, myObjectJpa.getColumn1())
                .setParameter(3, myObjectJpa.getColumn2())
                .setParameter(4, myObjectJpa.getCreated())
                .setParameter(5, myObjectJpa.getUpdated())
                .executeUpdate();

        em.getTransaction().commit();
        em.close();

    }

    public void insertUsingMerge(MyObjectPojo myObjectPojo) {

        var em = emf.createEntityManager();

        em.getTransaction().begin();

        // map pojo to dao
        var myObjectJpa = MyObjectJpa.builder()
                .hash(myObjectPojo.getHash())
                .column1(myObjectPojo.getColumn1())
                .column2(myObjectPojo.getColumn2())
                .created(Date.from(myObjectPojo.getCreated().atStartOfDay(UTC_TIME_ZONE).toInstant()))
                .updated(Date.from(myObjectPojo.getUpdated().atZone(ZoneId.systemDefault()).toInstant()))
                .build();

        em.merge(myObjectJpa);

        em.getTransaction().commit();
        em.close();

    }

    public void insertUsingFindAndMerge(MyObjectPojo myObjectPojo) {

        var em = emf.createEntityManager();

        em.getTransaction().begin();

        // map pojo to dao
        var myObjectJpa = MyObjectJpa.builder()
                .hash(myObjectPojo.getHash())
                .column1(myObjectPojo.getColumn1())
                .column2(myObjectPojo.getColumn2())
                .created(Date.from(myObjectPojo.getCreated().atStartOfDay(UTC_TIME_ZONE).toInstant()))
                .updated(Date.from(myObjectPojo.getUpdated().atZone(ZoneId.systemDefault()).toInstant()))
                .build();

        // Look for the current entry in the db
        var query = em.createQuery("SELECT P FROM TEST_OBJECTS P"
            + " WHERE P.hash = :hash", MyObjectJpa.class);
        query.setParameter("hash", myObjectJpa.getHash());

        MyObjectJpa objectInDb;
        try {
            objectInDb = query.getSingleResult();
        } catch (final NoResultException nre) {
            objectInDb = null;
        }

        // If we have a previous object then update it
        if (objectInDb != null ) {
            objectInDb.setColumn1(myObjectJpa.getColumn1());
            objectInDb.setColumn2(myObjectJpa.getColumn2());
            em.merge(objectInDb);
        } else {
            // no previous object so just persist it
            em.persist(myObjectJpa);
        }

        em.getTransaction().commit();
        em.close();

    }

    @Override
    public void insertUsingNativeJDBC(final MyObjectPojo myObjectPojo) {

        try
        {
            // Step 1: "Load" the JDBC driver
            Class.forName("com.opentable.db.postgres.embedded.EmbeddedPostgres");

            // Step 2: Establish the connection to the database
            var conn = DriverManager.getConnection(serverURL, this.user, "password");

            var sqlQuery = "INSERT INTO public.TEST_OBJECTS"
                            + "   ( HASH, COLUMN_1, COLUMN_2, CREATED, UPDATED ) "
                            + "   VALUES (?, ?, ?, ?, ?)"
                            + " ON CONFLICT (HASH)"
                            + " DO UPDATE"
                            + " SET COLUMN_1 = excluded.COLUMN_1,"
                            + "     COLUMN_2 = excluded.COLUMN_2,"
                            + "     CREATED  = excluded.CREATED,"
                            + "     UPDATED  = excluded.UPDATED;";
            PreparedStatement ps = conn.prepareStatement(sqlQuery);
            ps.setString(1, myObjectPojo.getHash());
            ps.setString(2, myObjectPojo.getColumn1());
            ps.setString(3, myObjectPojo.getColumn2());
            ps.setDate(4, java.sql.Date.valueOf(myObjectPojo.getCreated()));
            ps.setTime(5, java.sql.Time.valueOf(myObjectPojo.getUpdated().toLocalTime()));

            var countInserted = ps.executeUpdate();

            System.out.println("inserted: " + countInserted);

            conn.close();
        }
        catch (Exception e)
        {
            System.err.println("D'oh! Got an exception!");
            System.err.println(e.getMessage());
        }
    }

    @Override
    public List<MyObjectJpa> getAll() {

        var em = emf.createEntityManager();
        em.getTransaction().begin();

        var recordsFound = em.createQuery("SELECT O FROM TEST_OBJECTS O", MyObjectJpa.class)
                .getResultList();

        em.getTransaction().commit();
        em.close();

        return recordsFound;
    }

    @Override
    @SneakyThrows
    public List<MyObjectJpa> getAllUsingNativeJDBC() {

        ResultSet results = null;
        var objects = new ArrayList<MyObjectJpa>();

        try
        {
            // Step 1: "Load" the JDBC driver
            Class.forName("com.opentable.db.postgres.embedded.EmbeddedPostgres");

            // Step 2: Establish the connection to the database
            var conn = DriverManager.getConnection(serverURL, this.user, "password");

            var sqlQuery = "SELECT * FROM public.TEST_OBJECTS;";
            var statement = conn.createStatement();
            results = statement.executeQuery(sqlQuery);

            System.out.println("results: " + results);

            while (results.next()) {
                objects.add(convertResults(results));
            }

            conn.close();

        }
        catch (Exception e)
        {
            System.err.println("D'oh! Got an exception!");
            System.err.println(e.getMessage());
        }

        return objects;
    }

    @SneakyThrows
    private MyObjectJpa convertResults(final ResultSet resultSet) {
        var newObject = new MyObjectJpa();
//        newObject.setId_pk(new BigInteger(resultSet.getLong(1)));
        newObject.setHash(resultSet.getString(2));
        newObject.setColumn1(resultSet.getString(3));
        newObject.setColumn2(resultSet.getString(4));
        newObject.setCreated(resultSet.getDate(5));
        newObject.setUpdated(resultSet.getTime(6));
        return newObject;

    }
}
