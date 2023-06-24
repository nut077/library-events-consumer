package com.github.nut077.libraryeventsconsumer.entity;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data
public final class LibraryEvent {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Enumerated(EnumType.STRING)
  private LibraryEventType libraryEventType;

  @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
  private Book book;
}
