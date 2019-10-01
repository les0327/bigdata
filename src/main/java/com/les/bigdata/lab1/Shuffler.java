package com.les.bigdata.lab1;

import com.les.bigdata.model.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Shuffler<K extends Comparable, V> {

    public Map<K, List<Pair<K, V>>> shuffle(List<Pair<K, V>>... mapped) {
        return Stream.of(mapped)
                .flatMap(List::stream)
                .collect(
                        Collectors.toMap(
                                Pair::getKey,
                                Arrays::asList,
                                this::merge,
                                TreeMap::new
                        )
                );
    }

    public List<Pair<K, V>>  merge(List<Pair<K, V>> list1, List<Pair<K, V>> list2) {
        List<Pair<K, V>> list = new ArrayList<>(list1);
        list.addAll(list2);
        return list;
    }
}
