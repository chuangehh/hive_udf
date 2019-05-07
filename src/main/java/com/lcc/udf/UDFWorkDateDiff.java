package com.lcc.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * UDFWorkDateDiff.
 */
@Description(name = "work_datediff",
        value = "_FUNC_(date) - Returns the minute of date",
        extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n"
                + "Example:\n "
                + "  > SELECT _FUNC_('2019-05-07 15:00:00','2019-05-01 12:00:00') FROM src LIMIT 1;\n"
                + "  4.125\n"
                + "  > SELECT _FUNC_('2019-05-07','2019-05-01') FROM src LIMIT 1;\n"
                + "  4")
public class UDFWorkDateDiff extends UDF {


    public static void main(String[] args) throws ParseException {
        UDFWorkDateDiff udf = new UDFWorkDateDiff();
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");


        Long workDiffDate = udf.workDiffDate("2019-05-07 15:00:00", "2019-05-01 12:00:00");
        System.out.println(workDiffDate.doubleValue() / (1000 * 3600 * 24));

        Date parse = sdf1.parse("2019-05-07 15:00:00");
        Date parse2 = sdf1.parse("2019-05-01 12:00:00");
        DoubleWritable evaluate = udf.evaluate(new TimestampWritable(new java.sql.Timestamp(parse.getTime())), new TimestampWritable(new java.sql.Timestamp(parse2.getTime())));
        System.out.println(evaluate);


        Date parse22 = sdf2.parse("2019-05-07");
        Date parse222 = sdf2.parse("2019-05-01");
        DoubleWritable evaluate222 = udf.evaluate(new DateWritable(new java.sql.Date(parse22.getTime())), new DateWritable(new java.sql.Date(parse222.getTime())));
        System.out.println(evaluate222);
    }

    private final DoubleWritable result = new DoubleWritable();

    private final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
    private final int oneDay = 1000 * 3600 * 24;
    private final int twoDay = oneDay * 2;
    private final int fiveDay = oneDay * 5;
    private final int sevenDay = oneDay * 7;

    public DoubleWritable evaluate(Text date, Text date2) {
        if (date == null || date2 == null) {
            return null;
        }

        try {
            Long workDiffDate = workDiffDate(date.toString(), date2.toString());
            if (workDiffDate == null) {
                return null;
            }

            result.set(workDiffDate.doubleValue() / oneDay);
            return result;
        } catch (ParseException e) {
            return null;
        }
    }

    public DoubleWritable evaluate(DateWritable date, DateWritable date2) {
        if (date == null || date2 == null) {
            return null;
        }

        Long workDiffDate = workDiffDate(date.get(), date2.get());
        if (workDiffDate == null) {
            return null;
        }

        result.set(workDiffDate.doubleValue() / oneDay);
        return result;
    }

    public DoubleWritable evaluate(TimestampWritable date, TimestampWritable date2) {
        if (date == null || date2 == null) {
            return null;
        }

        Long workDiffDate = workDiffDate(date.getTimestamp(), date2.getTimestamp());
        if (workDiffDate == null) {
            return null;
        }

        result.set(workDiffDate.doubleValue() / oneDay);
        return result;
    }


    private Long workDiffDate(String dateStr, String dateStr2) throws ParseException {
        if (dateStr == null || dateStr2 == null
                || dateStr.trim().equals("") || dateStr2.trim().equals("")) {
            return null;
        }

        Date date;
        try {
            date = sdf1.parse(dateStr);
        } catch (ParseException e) {
            date = sdf2.parse(dateStr);
        }

        Date date2;
        try {
            date2 = sdf1.parse(dateStr2);
        } catch (ParseException e) {
            date2 = sdf2.parse(dateStr2);
        }

        return workDiffDate(date, date2);
    }


    /**
     * 获取两日期的工作日期
     *
     * @param date
     * @param date2
     * @return
     * @throws ParseException
     */
    private Long workDiffDate(Date date, Date date2) {
        if (date == null || date2 == null) {
            return null;
        }

        if (date.before(date2)) {
            return -workDiffDate(date2, date);
        }

        // 1.大的日期如果是周六周天,移動為下周一開始時間
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        // 1:周天,7:周六
        int weekNumber = calendar.get(Calendar.DAY_OF_WEEK);
        if (weekNumber == 1) {
            // 重置爲今天晚上
            calendar.set(Calendar.HOUR_OF_DAY, 24);
            // 本周五 = 本周天減1天
            calendar.add(Calendar.DATE, -2);
            date = calendar.getTime();
        } else if (weekNumber == 7) {
            // 重置爲今天晚上
            calendar.set(Calendar.HOUR_OF_DAY, 24);
            // 本周五 = 本周六減1天
            calendar.add(Calendar.DATE, -1);
            date = calendar.getTime();
        }

        // 日期如果是周六周天,移動為下周一開始時間
        Calendar calendar2 = Calendar.getInstance();
        calendar2.setTime(date2);
        // 1:周天,7:周六
        int weekNumber2 = calendar2.get(Calendar.DAY_OF_WEEK);
        if (weekNumber2 == 1) {
            // 重置爲今天凌晨
            calendar2.set(Calendar.HOUR_OF_DAY, 0);
            // 下周一 = 本周天加1天
            calendar2.add(Calendar.DATE, 1);
            date2 = calendar2.getTime();
        } else if (weekNumber2 == 7) {
            // 重置爲今天凌晨
            calendar2.set(Calendar.HOUR_OF_DAY, 0);
            // 下周一 = 本周六加2天
            calendar2.add(Calendar.DATE, 2);
            date2 = calendar2.getTime();
        }

        if (date.before(date2)) {
            return 0L;
        }

        long time = date.getTime();
        long time2 = date2.getTime();
        long workDiffTime = 0L;
        while (time > time2 + sevenDay) {
            // 加5天
            workDiffTime += fiveDay;
            // 加7天
            time2 = time2 + sevenDay;
        }

        long oneWeekDiff = time - time2;
        if (oneWeekDiff > fiveDay) {
            oneWeekDiff = oneWeekDiff - twoDay;
        }
        workDiffTime += oneWeekDiff;

        // 毫秒
        return workDiffTime;
    }

}


