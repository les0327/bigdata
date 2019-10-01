package com.les.bigdata.lab1;

import com.les.bigdata.model.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Mapper {

    private final String SPLIT_PATTERN = "[ \n\t]";

    public List<Pair<String, Integer>> map (Path path) throws IOException {
        String all = new String(Files.readAllBytes(path));

        String[] words = all.split(SPLIT_PATTERN);

        return Stream.of(words)
                .map(word -> new Pair<>(word, 1))
                .collect(Collectors.toCollection(LinkedList::new));
    }
}
