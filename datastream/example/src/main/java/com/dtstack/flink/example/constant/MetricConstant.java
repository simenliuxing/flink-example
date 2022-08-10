package com.dtstack.flink.example.constant;

/**
 * @author Ada Wong
 * @program flink-example
 * @create 2021/07/14
 * TODO metric名字请不要随意修改，不然无法对接数栈的数据曲线
 **/
public class MetricConstant {
    // 从source获取的数据解析失败的视为脏数据
    public static final String DT_DIRTY_DATA_COUNTER = "dtDirtyData";

    // source接受的记录数(未解析前)/s
    public static final String DT_NUM_RECORDS_IN_RATE = "dtNumRecordsInRate";

    // source接受的记录数(解析后)/s
    public static final String DT_NUM_RECORDS_RESOVED_IN_RATE = "dtNumRecordsInResolveRate";

    // source接受的字节数/s
    public static final String DT_NUM_BYTES_IN_RATE = "dtNumBytesInRate";

    // 写入的外部记录数/s
    public static final String DT_NUM_RECORDS_OUT_RATE = "dtNumRecordsOutRate";

    // source接受的总条数
    public static final String DT_NUM_RECORDS_IN_COUNTER = "dtNumRecordsIn";

    // source接受总数据字节数
    public static final String DT_NUM_BYTES_IN_COUNTER = "dtNumBytesIn";

    // source解析的总条数
    public static final String DT_NUM_RECORDS_RESOVED_IN_COUNTER = "dtNumRecordsInResolve";

    // sink总输出记录数
    public static final String DT_NUM_RECORDS_OUT = "dtNumRecordsOut";

    // sink总的输出脏数据
    public static final String DT_NUM_DIRTY_RECORDS_OUT = "dtNumDirtyRecordsOut";

}