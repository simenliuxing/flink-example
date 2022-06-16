package com.dtstack.flink.streamudtf.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * @author longxuan
 * @create 2021-09-18 10:19
 * @description json解析udtf
 **/

public class JsonParserUdtf extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParserUdtf.class);
    private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();


    /**
     * 定义属性，分隔符
     */
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING);
    }

    /**
     * @param input 输入参数
     * @return 返回参数
     */
    public void eval(String input) {
        try {
            String n2Oout = "";
            Row row = new Row(6);
            JsonNode jsonNode = OBJECTMAPPER.readTree(input);
            row.setField(0, jsonNode.get("name1").asText());
            row.setField(1, jsonNode.get("name2").asText());
            row.setField(2, jsonNode.get("name3").asText());
            row.setField(3, jsonNode.get("name4").asText());
            List<JsonNode> values = jsonNode.get("name5").findValues("N1");
            List<JsonNode> values1 = jsonNode.get("name5").findValues("N2");
            int i = 0;
            for (JsonNode node : values1) {
                row.setField(4, values.get(i).asText());
                i++;
                Iterator<JsonNode> n2Node = node.elements();
                while (n2Node.hasNext()) {
                    n2Oout += n2Node.next().asText() + "#";
                }
                n2Oout = n2Oout.substring(0, n2Oout.length() - 1);
                row.setField(5, n2Oout);
                collect(row);
                n2Oout = "";
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }
}

