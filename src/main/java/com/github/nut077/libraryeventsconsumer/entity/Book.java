package com.github.nut077.libraryeventsconsumer.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import lombok.Data;

@Entity
@Data
public final class Book {

  @Id
  private Long id;

  private String bookName;
  private String bookAuthor;

  @JsonIgnore
  @OneToOne
  @JoinColumn(name = "library_event_id")
  private LibraryEvent libraryEvent;
}
