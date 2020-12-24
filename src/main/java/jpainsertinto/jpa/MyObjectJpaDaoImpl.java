package jpainsertinto.jpa;

import lombok.extern.slf4j.Slf4j;
import jpainsertinto.model.MyObjectPojo;

import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

@Slf4j
public class MyObjectJpaDaoImpl implements MyObjectJpaDao {

    private final EntityManagerFactory emf;
    private final static ZoneId UTC_TIME_ZONE = ZoneId.of("UTC");

    public MyObjectJpaDaoImpl(final EntityManagerFactory emf) {
        this.emf = emf;
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
    public List<MyObjectJpa> getAll() {

        var em = emf.createEntityManager();
        em.getTransaction().begin();

        var recordsFound = em.createQuery("SELECT O FROM TEST_OBJECTS O", MyObjectJpa.class)
                .getResultList();

        em.getTransaction().commit();
        em.close();

        return recordsFound;
    }

}
