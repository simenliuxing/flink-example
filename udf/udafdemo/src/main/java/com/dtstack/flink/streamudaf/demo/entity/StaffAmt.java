package com.dtstack.flink.streamudaf.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
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

    public StaffAmt(String ptfloSno, String appDate, String ordAmt, String ptfloOpType, String iaInvestAgrNo, String cancelFlag, String ptfloOrdStat, String opertion, String comrate, String staffno, String curScale, String timeS) {
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
    }
}
