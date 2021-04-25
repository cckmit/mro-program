package com.asiainfo.opmc.rtd.mro.entity.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;

import java.io.Serializable;

/**
 * <p>
 * 
 * </p>
 *
 * @author shendan
 * @since 2021-03-23
 */
@TableName("mro_server_file_delay_temp")
@AllArgsConstructor
public class MroServerFileDelayPo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String sourceIp;

    @TableField("fileName")
    private String filename;

    private Integer fileTime;

    private Integer timeDelay;


    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Integer getFileTime() {
        return fileTime;
    }

    public void setFileTime(Integer fileTime) {
        this.fileTime = fileTime;
    }

    public Integer getTimeDelay() {
        return timeDelay;
    }

    public void setTimeDelay(Integer timeDelay) {
        this.timeDelay = timeDelay;
    }

    @Override
    public String toString() {
        return "MroServerFileDelayPo{" +
        "sourceIp=" + sourceIp +
        ", filename=" + filename +
        ", fileTime=" + fileTime +
        ", timeDelay=" + timeDelay +
        "}";
    }
}
