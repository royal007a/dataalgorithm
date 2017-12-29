package com.zw.java.algorithm.Markov;

public class TableItem  {
    String fromState;
    String toState;
    int count;

    public TableItem(String fromState, String toState, int count) {
        this.fromState = fromState;
        this.toState = toState;
        this.count = count;
    }

    /**
     * for debugging ONLY
     */
    public String toString() {
        return "{"+fromState+"," +toState+","+count+"}";
    }
}