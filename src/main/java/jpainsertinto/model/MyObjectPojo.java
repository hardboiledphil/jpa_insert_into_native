package jpainsertinto.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.time.LocalDate;
import java.time.LocalDateTime;


@Builder
@Value
@ToString
public class MyObjectPojo {

    public long     id;
    public String   hash;
    public String   column1;
    public String   column2;
    public LocalDate created;
    public LocalDateTime updated;

}