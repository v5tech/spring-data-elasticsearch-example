package net.aimeizi.spring.data.elasticsearch.example.service;

import java.util.List;

import net.aimeizi.spring.data.elasticsearch.example.Book;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public interface BookService {

	List<Book> findByNameAndPrice(String name, Float price);

    List<Book> findByNameOrPrice(String name, Float price);

    Page<Book> findByName(String name,Pageable page);

    Page<Book> findByNameNot(String name,Pageable page);

    Page<Book> findByPriceBetween(Float price,Pageable page);

    Page<Book> findByNameLike(String name,Pageable page);

    Page<Book> findByPrice(Float price, Pageable pageable);
}
