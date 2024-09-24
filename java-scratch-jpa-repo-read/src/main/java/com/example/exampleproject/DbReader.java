package com.example.exampleproject;

import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.example.exampleproject.entity.Student;
import com.example.exampleproject.repository.StudentRepository;

@Component
public class DbReader implements ItemReader<List<Student>> {

    @Autowired
    @Lazy
    StudentRepository studentRepository;

    @Override
    public List<Student> read()  {
        List<Student> students = studentRepository.findFirst10ByStatusOrderByIdDesc(false);
        if(students == null || students.size()==0){
            return null;
        }
        return students;
    }
}
