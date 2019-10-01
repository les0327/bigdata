package com.les.bigdata;

import com.les.bigdata.lab1.Lab1Main;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            switch (args[0]) {
                case "lab1": {
                    new Lab1Main().run();
                }
            }
        }
    }
}
