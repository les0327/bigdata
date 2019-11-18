package com.les.bigdata.lab1;

import com.les.bigdata.lab1.model.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class Reducer<K, V> {

    public List<Pair<K, V>> reduce(Map<K, List<Pair<K, V>>> shuffled,
                                   BiFunction<K, List<Pair<K, V>>, Pair<K, V>> reduceFunction) {

        return shuffled.entrySet()
                .stream()
                .map(entry -> reduceFunction.apply(entry.getKey(), entry.getValue()))
                .collect(Collectors.toCollection(LinkedList::new));
    }
}
