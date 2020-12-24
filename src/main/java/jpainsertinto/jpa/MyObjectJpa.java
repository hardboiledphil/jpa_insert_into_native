package jpainsertinto.jpa;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.math.BigInteger;
import java.util.Date;

@Entity(name = "TEST_OBJECTS")
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MyObjectJpa {

    @Id
    @SequenceGenerator(name="test_objects_id_pk_seq", sequenceName="test_objects_id_pk_seq", allocationSize=1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator="test_objects_id_pk_seq")
    @Column(name = "id", columnDefinition = "SERIAL")
    private BigInteger id_pk;

    @Column(name = "hash", columnDefinition = "VARCHAR(15) UNIQUE")
    private String hash;

    @Column(name = "column_1", columnDefinition = "VARCHAR(15) NOT NULL")
    private String column1;

    @Column(name = "column_2", columnDefinition = "VARCHAR(15) NOT NULL")
    private String column2;

    @Column(name = "created", columnDefinition = "TIME NO TIME ZONE")
    @Temporal(TemporalType.DATE)
    private Date created;

    @Column(name = "updated", columnDefinition = "TIMESTAMP NO TIME ZONE")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

}
