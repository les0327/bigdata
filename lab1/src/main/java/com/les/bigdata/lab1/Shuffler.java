package com.les.bigdata.lab1;

import com.les.bigdata.lab1.model.Pair;

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
                                Collections::singletonList,
                                this::merge,
                                TreeMap::new
                        )
                );
    }

    private List<Pair<K, V>> merge(List<Pair<K, V>> list1, List<Pair<K, V>> list2) {
        List<Pair<K, V>> list = new LinkedList<>(list1);
        list.addAll(list2);
        return list;
    }
}
