package com.lcc.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class IsEmpty extends UDF {

    public Text evaluate(Text input) {
        if (input == null || input.toString().trim().equals("")) {
            return new Text("空");
        }
        return new Text("非空");
    }

    public static void main(String[] args) {
        IsEmpty isEmpty = new IsEmpty();
        String emptyString1 = "";
        String emptyString2 = "  ";
        String numberString2 = "1992";


        System.out.printf("null: %s\n", isEmpty.evaluate(null).toString());
        System.out.printf("emptyString1: %s\n", isEmpty.evaluate(new Text(emptyString1)).toString());
        System.out.printf("emptyString2: %s\n", isEmpty.evaluate(new Text(emptyString2)).toString());
        System.out.printf("numberString2: %s\n", isEmpty.evaluate(new Text(numberString2)).toString());
    }

}
