package com.dtstack.flink.streamudaf.demo;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flink.streamudaf.demo.entity.StaffAmt;
import com.dtstack.flink.streamudaf.demo.util.LruLinkedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class StaffCountUDAF extends AggregateFunction<String, LruLinkedHashMap<String, Object>> {

    /**
     * 定义如何根据输入更新Accumulator
     */
    public void accumulate(LruLinkedHashMap<String, Object> acc, String ptfloSno, String appDate, String ordAmt, String ptfloOpType, String iaInvestAgrNo, String cancelFlag, String ptfloOrdStat, String opertion, String comrate, String staffno, String curScale, String timeS) {
        if (StringUtils.isNotBlank(staffno)) {
            //存储对象的主键
            String keyNo = ptfloSno.concat(appDate);
            //存储根据员工编码+日期主键
            String staffNo = staffno.concat(appDate);

            acc.put("staffNo", staffNo);
            //判断传入值是否纳入统计 True 纳入统计  False 不纳入统计
            Boolean statFlag = !("1".equals(cancelFlag) || "DELETE".equals(opertion) || Arrays.asList(new String[]{"4", "7"}).contains(ptfloOrdStat));

            //根据传入值生成 staffAmt 对象
            StaffAmt staffAmt = new StaffAmt(ptfloSno, appDate, ordAmt, ptfloOpType, iaInvestAgrNo, cancelFlag, ptfloOrdStat,
                    opertion, comrate, staffno, curScale, timeS, statFlag);

            Object obj = acc.get(staffNo);
            Map<String, List> map = JSONObject.parseObject(JSONObject.toJSONString(obj), HashMap.class);

            //新的主键 存储对象并且放到 存储 List 的 Map中
            if (acc.get(keyNo) == null) {
                acc.put(keyNo, staffAmt);
                // 如果 map 为空。 则new List 放到map中
                if (map == null) {
                    map = new HashMap<>();
                    List<String> list = new ArrayList();
                    list.add(keyNo);
                    map.put(staffNo, list);
                } else {
                    List<String> list = map.get(staffNo);
                    list.add(keyNo);
                }
            } else {
                StaffAmt oStaffAmt = (StaffAmt) acc.get(keyNo);
                //新加入的时间值大于已存的时间值（数据是最新的）
                if (oStaffAmt.getStatFlag() && staffAmt.getTimeS().compareTo(oStaffAmt.getTimeS()) > 0) {
                    acc.put(keyNo, staffAmt);
                }
            }
        } else {
            //  staffno为空业务处理
            throw new NullPointerException("staffno不能为空");
        }
    }

    @Override
    public String getValue(LruLinkedHashMap<String, Object> acc) {
        String staffNo = String.valueOf(acc.get("staffNo"));
        return acc + UUID.randomUUID().toString();
    }

    @Override
    public LruLinkedHashMap<String, Object> createAccumulator() {
        return new LruLinkedHashMap<>(1024, 0.75f);
    }
}
