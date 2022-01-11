package com.dtstack.flink.pojo;

import java.util.Map;

/**
 * @author beifeng
 */
public class Response {
    private Map ddlChangeMonitorQuery;
    private String status;
    private int statusCode;

    @Override
    public String toString() {
        return "Response{" +
                "ddlChangeMonitorQuery=" + ddlChangeMonitorQuery +
                ", status='" + status + '\'' +
                ", statusCode=" + statusCode +
                '}';
    }

    public Map getDdlChangeMonitorQuery() {
        return ddlChangeMonitorQuery;
    }

    public void setDdlChangeMonitorQuery(Map ddlChangeMonitorQuery) {
        this.ddlChangeMonitorQuery = ddlChangeMonitorQuery;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}
