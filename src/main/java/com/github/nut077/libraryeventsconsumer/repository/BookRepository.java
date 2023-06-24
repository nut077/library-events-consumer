package com.github.nut077.libraryeventsconsumer.repository;

import com.github.nut077.libraryeventsconsumer.entity.Book;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BookRepository extends JpaRepository<Book, Integer> {
}
