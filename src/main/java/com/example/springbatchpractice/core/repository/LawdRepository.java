package com.example.springbatchpractice.core.repository;

import com.example.springbatchpractice.core.entity.Lawd;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LawdRepository extends JpaRepository<Lawd, Long> {
}
