package com.asiainfo.opmc.rtd.mro.service;

import com.asiainfo.opmc.rtd.mro.entity.po.MroServerFileDelayPo;
import com.baomidou.mybatisplus.extension.service.IService;
import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.text.ParseException;

/**
 * <p>
 * 服务类
 * </p>
 *
 * @author shendan
 * @since 2021-03-23
 */
public interface MroServerFileDelayTempService extends IService<MroServerFileDelayPo> {
    void computeFileDelay(String scanFile,String timeFormat) throws IOException, SftpException, ParseException;

    void scanFiles(String scanFile,String timeFormat);
}

