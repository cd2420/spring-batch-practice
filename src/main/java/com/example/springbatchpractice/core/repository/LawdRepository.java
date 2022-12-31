package com.example.springbatchpractice.core.repository;

import com.example.springbatchpractice.core.entity.Lawd;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface LawdRepository extends JpaRepository<Lawd, Long> {

    Optional<Lawd> findByLawdCd(String lawdCd);

    @Query(
            "select distinct substr(l.lawdCd, 1, 5)" +
                    "from lawd l" +
                    "where l.exist = 1" +
                    "and l.lawdCd not like '%00000000'"
    )
    List<String> findDistinctGuLawdCd();

}
