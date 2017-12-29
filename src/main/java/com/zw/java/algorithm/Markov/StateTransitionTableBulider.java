package com.zw.java.algorithm.Markov;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据spark结果  生成状态转移矩阵
 * 用于预测下一个结果
 */
public class StateTransitionTableBulider {

    //
    // model.states=SL,SE,SG,ML,ME,MG,LL,LE,LG
    //
    // states<key, value>: key is the state and value is row/column in table
    //
    private Map<String, Integer> states = null;
    private double[][] table = null;
    private int numberOfStates;
    private int scale = 100;

    private void initStates(){
        states = new HashMap<String, Integer>();
        states.put("SL", 0);
        states.put("SE", 1);
        states.put("SG", 2);
        states.put("ML", 3);
        states.put("ME", 4);
        states.put("MG", 5);
        states.put("LL", 6);
        states.put("LE", 7);
        states.put("LG", 8);
    }

    public void normalizeRows() {
        // Laplace correction: the usual solution is to do a
        // Laplacian correction by upping all the counts by 1
        // see: http://cs.nyu.edu/faculty/davise/ai/bayesText.html
        for (int r = 0; r < numberOfStates; r++) {
            boolean gotZeroCount = false;
            for (int c = 0; c < numberOfStates; c++) {
                if(table[r][c] == 0) {
                    gotZeroCount = true;
                    break;
                }
            }

            if (gotZeroCount) {
                for (int c = 0; c < numberOfStates; c++) {
                    table[r][c] += 1;
                }
            }
        }

        //normalize
        for (int r = 0; r < numberOfStates; r++) {
            double rowSum = getRowSum(r);
            for (int c = 0; c < numberOfStates; c++) {
                table[r][c] = table[r][c] / rowSum;  // 该行之和为1   ？？？
            }
        }
    }

    public String serializeRow(int rowNumber) {
        StringBuilder builder = new StringBuilder();
        for (int column = 0; column < numberOfStates; column++) {
            double element = table[rowNumber][column];
            builder.append(String.format("%.4g", element));
            if (column < (numberOfStates-1)) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public void persistTable() {
        for (int row = 0; row < numberOfStates; row++) {
            String serializedRow = serializeRow(row);
            System.out.println(serializedRow);
        }
    }

    /**
     * 算出 a 状态转向其他状态 的所有数目的之和
     * @param rowNumber
     * @return
     */
    public double getRowSum(int rowNumber) {
        double sum = 0.0;
        for (int column = 0; column < numberOfStates; column++) {
            sum += table[rowNumber][column];
        }
        return sum;
    }


    public StateTransitionTableBulider(int numberOfStates) {
        this.numberOfStates = numberOfStates;
        table = new double[numberOfStates][numberOfStates];
        initStates();

    }

    public void add(String fromState, String toState, int count) {
        int row = states.get(fromState);
        int column = states.get(toState);
        table[row][column] = count;
    }

    public static void  generateStateTransitionTable(String path) {
        List<TableItem> list = ReadDataFromHDFS.readDirectory(path);
        StateTransitionTableBulider tableBuilder = new StateTransitionTableBulider(9);
        for (TableItem item : list) {
            tableBuilder.add(item.fromState, item.toState, item.count);
        }
        // 生成概率转移矩阵   n*N
        tableBuilder.normalizeRows();
        tableBuilder.persistTable();
    }

    public static void main(String[] args) {
        String path = args[0];
        generateStateTransitionTable(path);
    }
}
