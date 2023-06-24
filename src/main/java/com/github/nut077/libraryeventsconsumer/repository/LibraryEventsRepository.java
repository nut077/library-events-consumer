package com.github.nut077.libraryeventsconsumer.repository;

import com.github.nut077.libraryeventsconsumer.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventsRepository extends JpaRepository<LibraryEvent, Integer> {
}
