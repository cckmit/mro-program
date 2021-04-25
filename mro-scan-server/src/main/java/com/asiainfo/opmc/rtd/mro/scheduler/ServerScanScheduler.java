package com.asiainfo.opmc.rtd.mro.scheduler;

import com.asiainfo.opmc.rtd.mro.constant.ScanErrorTypeEnum;
import com.asiainfo.opmc.rtd.mro.constant.ServerTypeEnum;
import com.asiainfo.opmc.rtd.mro.entity.po.MroScanStationErrorPo;
import com.asiainfo.opmc.rtd.mro.service.MroScanStationErrorService;
import com.asiainfo.opmc.rtd.mro.service.ServerScanService;
import com.asiainfo.opmc.rtd.mro.utils.SftpUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @program: cmzj-opmc-rtd-parent
 * @description: 服务器定时扫描
 * @author: sd
 * @create: 2021-01-28 10:51
 **/
@Component
@Slf4j
public class ServerScanScheduler {
    @Autowired
    ServerScanService serverScanService;
    @Autowired
    MroScanStationErrorService mroScanStationErrorService;

    //所有异常服务器信息
    List<MroScanStationErrorPo> stationErrorPos = new ArrayList<>();

    /**
     * @Description: 每小时扫描异常基站信息
     * @Author: sd
     * @Date: 2021/1/28 10:53
     **/
//    @Scheduled(cron = "0 0 * * * ?")
    public void errorServerScan() {
        log.info("开始更新异常服务器信息" + new Date());
        //数据库账期/华为账期格式，洛基亚，
        String opTime = DateTime.now().toString("yyyyMMdd");
        //中兴账期格式
        String dateTimeZte = DateTime.now().toString("yyyy-MM-dd");

        //遍历华为异常服务器信息，通过SFTP连接，判断不通错误类型
        this.addErrorServer(ServerTypeEnum.HW, opTime, opTime);
        //遍历中兴异常服务器信息
        this.addErrorServer(ServerTypeEnum.ZTE, opTime, dateTimeZte);
        //遍历洛基亚服务器信息
        this.addErrorServer(ServerTypeEnum.NSN, opTime, opTime);
        //遍历大唐服务器信息
        this.addErrorServer(ServerTypeEnum.DT, opTime, dateTimeZte);
        //便利爱立信服务器信息
        this.addErrorServer(ServerTypeEnum.Eric, opTime, opTime);

        try {
            mroScanStationErrorService.remove(new QueryWrapper<MroScanStationErrorPo>().eq("op_time", opTime));
            mroScanStationErrorService.saveBatch(stationErrorPos);
        } catch (Exception e) {
            e.printStackTrace();
        }
        stationErrorPos.clear();
        log.info("更新异常服务器信息完成");
    }

    public void addErrorServer(ServerTypeEnum serverType, String opTime, String fileDic) {
        try {
            List<String[]> errorServers = serverScanService.getErrorServers(serverType.getId(), opTime);
            log.info("异常服务器数：" + errorServers.size());
            for (String[] errorServer : errorServers) {
                ChannelSftp connect = SftpUtil.connect(errorServer[2], errorServer[3], errorServer[0], Integer.parseInt(errorServer[1]));
                MroScanStationErrorPo mroScanStationErrorPo = new MroScanStationErrorPo();
                mroScanStationErrorPo.setId(UUID.randomUUID().toString().replace("-", "").substring(0, 16));
                mroScanStationErrorPo.setCreateDt(new Date());
                mroScanStationErrorPo.setOpTime(opTime);
                mroScanStationErrorPo.setTsIp(errorServer[0]);
                mroScanStationErrorPo.setServerType(serverType.getName());
                if (connect != null) {
                    try {
                        List<String> files = SftpUtil.listFiles(errorServer[4] + fileDic, connect);
                        if (files.size() == 0) {
                            log.info("无文件");
                            mroScanStationErrorPo.setErrorState(ScanErrorTypeEnum.FILE_NOT_EXIST.getCode());
                            mroScanStationErrorPo.setErrorDesc(ScanErrorTypeEnum.FILE_NOT_EXIST.getDesc());
                        } else {
                            log.info("采集异常");
                            mroScanStationErrorPo.setErrorState(ScanErrorTypeEnum.COLLECT_ERROR.getCode());
                            mroScanStationErrorPo.setErrorDesc(ScanErrorTypeEnum.COLLECT_ERROR.getDesc());
                        }
                    } catch (SftpException e) {
                        log.info("无文件夹");
                        mroScanStationErrorPo.setErrorState(ScanErrorTypeEnum.DIC_NOT_EXIST.getCode());
                        mroScanStationErrorPo.setErrorDesc(ScanErrorTypeEnum.DIC_NOT_EXIST.getDesc());
                    }
                } else {
                    log.info("登陆失败");
                    mroScanStationErrorPo.setErrorState(ScanErrorTypeEnum.SERVER_CANNOT_CONNECT.getCode());
                    mroScanStationErrorPo.setErrorDesc(ScanErrorTypeEnum.SERVER_CANNOT_CONNECT.getDesc());
                }
                SftpUtil.disConnect(connect);
                stationErrorPos.add(mroScanStationErrorPo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
