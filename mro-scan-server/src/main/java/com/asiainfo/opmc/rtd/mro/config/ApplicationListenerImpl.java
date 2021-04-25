package com.asiainfo.opmc.rtd.mro.config;

import com.asiainfo.opmc.rtd.mro.scheduler.ServerScanScheduler;
import com.asiainfo.opmc.rtd.mro.service.MroServerFileDelayTempService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

/**
 * @program: cmzj-opmc-rtd-parent
 * @description: 启动
 * @author: sd
 * @create: 2021-01-07 14:22
 **/
@Component
public class ApplicationListenerImpl implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired
    ServerScanScheduler serverScanScheduler;
    @Autowired
    MroServerFileDelayTempService mroServerFileDelayTempService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        try {
            serverScanScheduler.errorServerScan();
//            serverScanScheduler.test();
//            mroServerFileDelayTempService.scanFiles("FileScan.txt","yyyy-MM-dd");
//            mroServerFileDelayTempService.computeFileDelay("FileDelayServerInfo.txt","yyyyMMdd");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

