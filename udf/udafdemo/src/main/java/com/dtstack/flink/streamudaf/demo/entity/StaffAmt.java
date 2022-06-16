package com.dtstack.flink.streamudaf.demo.entity;

/**
 * @author chuixue
 */
public class StaffAmt {
    private String ptfloSno;

    private String appDate;

    private String ordAmt;

    private String ptfloOpType;

    private String iaInvestAgrNo;

    private String cancelFlag;

    private String ptfloOrdStat;

    private String opertion;

    private String comrate;

    private String staffno;

    private String curScale;

    private String timeS;

    //数据是否生效
    private Boolean statFlag;

    public StaffAmt(String ptfloSno, String appDate, String ordAmt, String ptfloOpType, String iaInvestAgrNo, String cancelFlag, String ptfloOrdStat, String opertion, String comrate, String staffno, String curScale, String timeS, Boolean statFlag) {
        this.ptfloSno = ptfloSno;
        this.appDate = appDate;
        this.ordAmt = ordAmt;
        this.ptfloOpType = ptfloOpType;
        this.iaInvestAgrNo = iaInvestAgrNo;
        this.cancelFlag = cancelFlag;
        this.ptfloOrdStat = ptfloOrdStat;
        this.opertion = opertion;
        this.comrate = comrate;
        this.staffno = staffno;
        this.curScale = curScale;
        this.timeS = timeS;
        this.statFlag = statFlag;
    }

    public String getPtfloSno() {
        return ptfloSno;
    }

    public void setPtfloSno(String ptfloSno) {
        this.ptfloSno = ptfloSno;
    }

    public String getAppDate() {
        return appDate;
    }

    public void setAppDate(String appDate) {
        this.appDate = appDate;
    }

    public String getOrdAmt() {
        return ordAmt;
    }

    public void setOrdAmt(String ordAmt) {
        this.ordAmt = ordAmt;
    }

    public String getPtfloOpType() {
        return ptfloOpType;
    }

    public void setPtfloOpType(String ptfloOpType) {
        this.ptfloOpType = ptfloOpType;
    }

    public String getIaInvestAgrNo() {
        return iaInvestAgrNo;
    }

    public void setIaInvestAgrNo(String iaInvestAgrNo) {
        this.iaInvestAgrNo = iaInvestAgrNo;
    }

    public String getCancelFlag() {
        return cancelFlag;
    }

    public void setCancelFlag(String cancelFlag) {
        this.cancelFlag = cancelFlag;
    }

    public String getPtfloOrdStat() {
        return ptfloOrdStat;
    }

    public void setPtfloOrdStat(String ptfloOrdStat) {
        this.ptfloOrdStat = ptfloOrdStat;
    }

    public String getOpertion() {
        return opertion;
    }

    public void setOpertion(String opertion) {
        this.opertion = opertion;
    }

    public String getComrate() {
        return comrate;
    }

    public void setComrate(String comrate) {
        this.comrate = comrate;
    }

    public String getStaffno() {
        return staffno;
    }

    public void setStaffno(String staffno) {
        this.staffno = staffno;
    }

    public String getCurScale() {
        return curScale;
    }

    public void setCurScale(String curScale) {
        this.curScale = curScale;
    }

    public String getTimeS() {
        return timeS;
    }

    public void setTimeS(String timeS) {
        this.timeS = timeS;
    }

    public Boolean getStatFlag() {
        return statFlag;
    }

    public void setStatFlag(Boolean statFlag) {
        this.statFlag = statFlag;
    }
}
