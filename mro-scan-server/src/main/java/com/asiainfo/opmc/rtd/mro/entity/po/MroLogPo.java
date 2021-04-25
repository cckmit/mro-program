package com.asiainfo.opmc.rtd.mro.entity.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 *
 * </p>
 *
 * @author shendan
 * @since 2021-01-08
 */
public class MroLogPo implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "ID", type = IdType.AUTO)
    private Integer id;

    @TableField("FILE_NAME")
    private String fileName;

    @TableField("TS_IP")
    private String tsIp;

    @TableField("STATION")
    private String station;

    @TableField("SOURCE_DATE")
    private String sourceDate;

    @TableField("FLAG")
    private String flag;

    @TableField("UPDATE_DATE")
    private Date updateDate;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getTsIp() {
        return tsIp;
    }

    public void setTsIp(String tsIp) {
        this.tsIp = tsIp;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getSourceDate() {
        return sourceDate;
    }

    public void setSourceDate(String sourceDate) {
        this.sourceDate = sourceDate;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Date getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }

    @Override
    public String toString() {
        return "MroLogPo{" +
                "id=" + id +
                ", fileName=" + fileName +
                ", tsIp=" + tsIp +
                ", station=" + station +
                ", sourceDate=" + sourceDate +
                ", flag=" + flag +
                ", updateDate=" + updateDate +
                "}";
    }
}
