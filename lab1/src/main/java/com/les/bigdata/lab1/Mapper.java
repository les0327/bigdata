package com.les.bigdata.lab1;

import com.les.bigdata.lab1.model.Pair;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Mapper {

    private final String SPLIT_PATTERN = "[ \n\t]";

    public List<Pair<String, Integer>> map(String string) throws IOException {

        String[] words = string.split(SPLIT_PATTERN);

        return Stream.of(words)
                .map(word -> new Pair<>(word, 1))
                .collect(Collectors.toCollection(LinkedList::new));
    }
}
