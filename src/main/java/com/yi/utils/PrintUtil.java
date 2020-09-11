package com.yi.utils;

/**
 * @author: YI
 * @description: 打印效果
 * @date: create in 2020/9/10 14:37
 */
public class PrintUtil {
    /**
     * @param content 要打印的内容
     */
    public static void printSingleColor(String content) {
        printSingleColor("", 33, 1, content);
    }

    /**
     * @param code    颜色代号：背景颜色代号(41-46)；前景色代号(31-36)
     * @param n       数字+m：1加粗；3斜体；4下划线
     * @param content 要打印的内容
     */
    public static void printSingleColor(int code, int n, String content) {
        printSingleColor("", code, n, content);
    }

    /**
     * @param pattern 前面的图案 such as "=============="
     * @param code    颜色代号：背景颜色代号(41-46)；前景色代号(31-36)
     * @param n       数字+m：1加粗；3斜体；4下划线
     * @param content 要打印的内容
     */
    public static void printSingleColor(String pattern, int code, int n, String content) {
        System.out.format("%s\33[%d;%dm%s%n", pattern, code, n, content);
    }
}
