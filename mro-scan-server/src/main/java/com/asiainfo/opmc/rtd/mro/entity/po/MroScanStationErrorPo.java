package com.asiainfo.opmc.rtd.mro.entity.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * 
 * </p>
 *
 * @author shendan
 * @since 2021-01-28
 */
@TableName("mro_scan_station_error")
public class MroScanStationErrorPo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    private String opTime;

    @TableField("TS_IP")
    private String tsIp;

    private String serverType;

    private String errorState;

    private String errorDesc;

    private Date createDt;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOpTime() {
        return opTime;
    }

    public void setOpTime(String opTime) {
        this.opTime = opTime;
    }

    public String getTsIp() {
        return tsIp;
    }

    public void setTsIp(String tsIp) {
        this.tsIp = tsIp;
    }

    public String getServerType() {
        return serverType;
    }

    public void setServerType(String serverType) {
        this.serverType = serverType;
    }

    public String getErrorState() {
        return errorState;
    }

    public void setErrorState(String errorState) {
        this.errorState = errorState;
    }

    public String getErrorDesc() {
        return errorDesc;
    }

    public void setErrorDesc(String errorDesc) {
        this.errorDesc = errorDesc;
    }

    public Date getCreateDt() {
        return createDt;
    }

    public void setCreateDt(Date createDt) {
        this.createDt = createDt;
    }

    @Override
    public String toString() {
        return "MroScanStationErrorPo{" +
        "id=" + id +
        ", opTime=" + opTime +
        ", tsIp=" + tsIp +
        ", serverType=" + serverType +
        ", errorState=" + errorState +
        ", errorDesc=" + errorDesc +
        ", createDt=" + createDt +
        "}";
    }
}
