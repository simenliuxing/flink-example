package com.dtstack.flink.fk.pojo;

/** @author beifeng */
public class ColumnInfo {
  private String acct_no;
  private String rec_no;
  private String tran_type;
  private String stat;
  private String post_date;
  private String trn_date;
  private String system_date;
  private String system_time;
  private String tell_and_br;
  private String jrnl_no;
  private String trn_code;
  private String brterm;
  private String channel;
  private String deli;
  private String var_area;

  public String getAcct_no() {
    return acct_no;
  }

  public void setAcct_no(String acct_no) {
    this.acct_no = acct_no;
  }

  @Override
  public String toString() {
    return "ColumnInfo{"
        + "acct_no='"
        + acct_no
        + '\''
        + ", rec_no='"
        + rec_no
        + '\''
        + ", tran_type='"
        + tran_type
        + '\''
        + ", stat='"
        + stat
        + '\''
        + ", post_date="
        + post_date
        + ", trn_date="
        + trn_date
        + ", system_date="
        + system_date
        + ", system_time="
        + system_time
        + ", tell_and_br="
        + tell_and_br
        + ", jrnl_no="
        + jrnl_no
        + ", trn_code="
        + trn_code
        + ", brterm="
        + brterm
        + ", channel='"
        + channel
        + '\''
        + ", deli='"
        + deli
        + '\''
        + ", var_area='"
        + var_area
        + '\''
        + '}';
  }

  public String getRec_no() {
    return rec_no;
  }

  public void setRec_no(String rec_no) {
    this.rec_no = rec_no;
  }

  public String getTran_type() {
    return tran_type;
  }

  public void setTran_type(String tran_type) {
    this.tran_type = tran_type;
  }

  public String getStat() {
    return stat;
  }

  public void setStat(String stat) {
    this.stat = stat;
  }

  public String getPost_date() {
    return post_date;
  }

  public void setPost_date(String post_date) {
    this.post_date = post_date;
  }

  public String getTrn_date() {
    return trn_date;
  }

  public void setTrn_date(String trn_date) {
    this.trn_date = trn_date;
  }

  public String getSystem_date() {
    return system_date;
  }

  public void setSystem_date(String system_date) {
    this.system_date = system_date;
  }

  public String getSystem_time() {
    return system_time;
  }

  public void setSystem_time(String system_time) {
    this.system_time = system_time;
  }

  public String getTell_and_br() {
    return tell_and_br;
  }

  public void setTell_and_br(String tell_and_br) {
    this.tell_and_br = tell_and_br;
  }

  public String getJrnl_no() {
    return jrnl_no;
  }

  public void setJrnl_no(String jrnl_no) {
    this.jrnl_no = jrnl_no;
  }

  public String getTrn_code() {
    return trn_code;
  }

  public void setTrn_code(String trn_code) {
    this.trn_code = trn_code;
  }

  public String getBrterm() {
    return brterm;
  }

  public void setBrterm(String brterm) {
    this.brterm = brterm;
  }

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getDeli() {
    return deli;
  }

  public void setDeli(String deli) {
    this.deli = deli;
  }

  public String getVar_area() {
    return var_area;
  }

  public void setVar_area(String var_area) {
    this.var_area = var_area;
  }
}
