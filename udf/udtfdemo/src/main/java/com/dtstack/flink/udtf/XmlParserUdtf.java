/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.udtf;


import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chuixue
 * @create 2022-09-23 13:47
 * @description
 **/
public class XmlParserUdtf extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(XmlParserUdtf.class);
    private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();
    public static int PRETTY_PRINT_INDENT_FACTOR = 4;

    /** 测试使用，具体解析代码根据实际情况来 */
    public static String TEST_XML_STRING =
                    "<root>" +
                            "<user>" +
                                "<firstName>John</firstName>" +
                                "<lastName>Snow</lastName>" +
                                "<age>25</age>" +
                                "<spouse/>" +
                                "<address>" +
                                    "<street>237 Harrison Street</street>" +
                                    "<city>Brooklyn, NY</city>" +
                                    "<state>New York</state>" +
                                    "<postalCode>11238</postalCode>" +
                                "</address>" +
                                "<phoneNumber>212-555-3346</phoneNumber>" +
                            "</user>" +

                            "<user>" +
                                "<firstName>111John</firstName>" +
                                "<lastName>111Snow</lastName>" +
                                "<age>1125</age>" +
                                "<spouse/>" +
                                "<address>" +
                                    "<street>11237 Harrison Street</street>" +
                                    "<city>11Brooklyn, NY</city>" +
                                    "<state>11New York</state>" +
                                    "<postalCode>1111238</postalCode>" +
                                "</address>" +
                                "<phoneNumber>11212-555-3346</phoneNumber>" +
                            "</user>" +
                    "</root>";

    public static void main(String[] args) {
        System.out.println(parseJson(parseXml(TEST_XML_STRING)));
    }

    public void eval(String str) {
        List<Row> rowList = parseJson(parseXml(str));
        for (Row row : rowList) {
            collect(row);
        }
    }

    /**
     * 抽取json string中的字段
     *
     * @param jsonString
     * @return
     */
    public static List<Row> parseJson(String jsonString) {
        List<Row> rowList = new ArrayList<>();
        try {
            JsonNode jsonNode = OBJECTMAPPER.readTree(jsonString);
            ArrayNode arrayNode = (ArrayNode) jsonNode.get("root").get("user");

            for (JsonNode node : arrayNode) {
                Row row = new Row(5);
                row.setField(0, node.get("firstName").asText());
                row.setField(1, node.get("lastName").asText());
                row.setField(2, node.get("age").asText());
                row.setField(3, node.get("phoneNumber").asText());
                row.setField(4, node.get("address").get("city").asText());

                rowList.add(row);
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return rowList;
    }

    /**
     * 将xml string 解析成 json string
     *
     * @param xmlString
     * @return
     */
    public static String parseXml(String xmlString) {
        try {
            JSONObject xmlJSONObj = XML.toJSONObject(xmlString);
            return xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR);
        } catch (JSONException e) {
            LOG.error(e.toString());
        }
        return "";
    }

    /**
     * 定义返回字段的个数和类型
     */
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }
}
