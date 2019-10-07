package com.les.bigdata.lab1;

import com.les.bigdata.model.Pair;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Lab1Main {

    public void run() throws Exception {
        Mapper mapper = new Mapper();
        Shuffler<String, Integer> shuffler = new Shuffler<>();
        Reducer<String, Integer> reducer = new Reducer<>();

        List<Pair<String, Integer>> mappedFile1 = mapper.map(readFile("lab1/file1"));
        List<Pair<String, Integer>> mappedFile2 = mapper.map(readFile("lab1/file2"));

        System.out.println("Mapped values from file1:");
        mappedFile1.forEach(System.out::println);
        System.out.println("Mapped values from file2:");
        mappedFile2.forEach(System.out::println);

        Map<String, List<Pair<String, Integer>>> shuffled = shuffler.shuffle(mappedFile1, mappedFile2);

        System.out.println("Shuffled values:");
        shuffled.forEach((key, value) -> System.out.printf("%s -> %s%n", key, value));

        List<Pair<String, Integer>> reduced = reducer.reduce(
                shuffled,
                (word, listOfPairs) -> new Pair<>(word, listOfPairs.stream().mapToInt(Pair::getValue).sum())
        );

        System.out.println("Reduced values:");
        reduced.forEach(System.out::println);
    }

    private static String readFile(String path) {

        return new BufferedReader(
                new InputStreamReader(ClassLoader.getSystemResourceAsStream(path))
        )
                .lines()
                .collect(Collectors.joining("\n"));
    }
}
