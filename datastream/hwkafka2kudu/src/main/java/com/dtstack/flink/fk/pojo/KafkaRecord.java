package com.dtstack.flink.fk.pojo;

/** @author beifeng */
public class KafkaRecord {

  private String Owner;
  private String tableName;
  private String OP;
  private String OP_TIME;
  private String LOADERTIME;
  private String SEQ_ID;
  private ColumnInfo columnInfo;

  public String getOwner() {
    return Owner;
  }

  public void setOwner(String owner) {
    Owner = owner;
  }

  @Override
  public String toString() {
    return "KafkaRecord{"
        + "Owner='"
        + Owner
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", OP='"
        + OP
        + '\''
        + ", OP_TIME="
        + OP_TIME
        + ", LOADERTIME="
        + LOADERTIME
        + ", SEQ_ID="
        + SEQ_ID
        + ", columnInfo="
        + columnInfo
        + '}';
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getOP() {
    return OP;
  }

  public void setOP(String OP) {
    this.OP = OP;
  }

  public String getOP_TIME() {
    return OP_TIME;
  }

  public void setOP_TIME(String OP_TIME) {
    this.OP_TIME = OP_TIME;
  }

  public String getLOADERTIME() {
    return LOADERTIME;
  }

  public void setLOADERTIME(String LOADERTIME) {
    this.LOADERTIME = LOADERTIME;
  }

  public String getSEQ_ID() {
    return SEQ_ID;
  }

  public void setSEQ_ID(String SEQ_ID) {
    this.SEQ_ID = SEQ_ID;
  }

  public ColumnInfo getColumnInfo() {
    return columnInfo;
  }

  public void setColumnInfo(ColumnInfo columnInfo) {
    this.columnInfo = columnInfo;
  }
}
