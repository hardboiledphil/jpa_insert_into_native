package jpainsertinto.jpa;

import jpainsertinto.model.MyObjectPojo;

import java.util.List;

public interface MyObjectJpaDao {

    void insertUsingQuery(MyObjectPojo myObjectPojo);

    void insertUsingNativeQueryWithDoNothing(MyObjectPojo myObjectPojo);

    void insertUsingNativeQueryWithDoUpdate(MyObjectPojo myObjectPojo);

    void insertUsingMerge(MyObjectPojo myObjectPojo);

    void insertUsingFindAndMerge(MyObjectPojo myObjectPojo);

    List<MyObjectJpa> getAll();

}
