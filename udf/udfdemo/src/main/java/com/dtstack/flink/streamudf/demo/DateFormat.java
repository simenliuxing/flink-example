package com.dtstack.flink.streamudf.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @Author yuange
 * @Create 2022/4/23 19:28
 * @Description 日期转换函数，按天加减
 */
public class DateFormat extends ScalarFunction {

    private final Logger LOG = LoggerFactory.getLogger(DateFormat.class);

    /**
     * 日期转换函数，按天加减
     *
     * @param date    输入日期
     * @param days    日期加减天数，负数代表减
     * @param pattern 输入日期的格式
     * @return 输出日期
     */
    public String eval(String date, int days, String pattern) {
        String outputDate = "";
        try {
            if (StringUtils.isBlank(date)) {
                throw new Exception(String.format("The field 'date' [%s] you entered invalid", date));
            }
            if (StringUtils.isBlank(pattern)) {
                throw new Exception(String.format("The field 'pattern' [%s] you entered invalid", pattern));
            }

            // 对date按pattern形式转成JavaDate
            Date _date = DateUtils.parseDate(date, pattern);
            // 执行日期加减，出参也是JavaDate
            Date _outputDate = DateUtils.addDays(_date, days);
            outputDate = DateFormatUtils.format(_outputDate, pattern);


        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());
        }
        return outputDate;
    }

}

