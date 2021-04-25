package com.asiainfo.opmc.rtd.mro.service.impl;

import com.asiainfo.opmc.rtd.mro.mapper.MroLogMapper;
import com.asiainfo.opmc.rtd.mro.service.ServerScanService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @program: cmzj-opmc-rtd-parent
 * @description: 服务器扫描
 * @author: sd
 * @create: 2021-01-28 10:54
 **/
@Service
public class ServerScanServiceImpl implements ServerScanService {
    @Autowired(required = false)
    MroLogMapper mroLogMapper;

    /**
     * @Description: 获取异常服务器信息
     * @Author: sd
     * @Date: 2021/1/28 15:13
     * @param type: 服务器类型
     * @param date: 账期
     **/
    @Override
    public List<String[]> getErrorServers(String type, String date) throws IOException {
        //获取所有服务器信息
        Map<String, String[]> allServerInfo = this.getAllServerInfo(type);
        //获取账期下正常采集的服务器信息
        List<String> stationList = mroLogMapper.getStationList(type, date);
        //过滤出未采集的异常服务器信息
        List<String[]> serverInfo = allServerInfo.entrySet().stream()
                .filter(server -> stationList.indexOf(server.getKey()) == -1)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        return serverInfo;
    }

    /**
     *
     * @param type 基站类型（文件名称）
     * @return ip：服务器信息  格式的map集合
     * @throws IOException
     */
    public Map<String, String[]> getAllServerInfo(String type) throws IOException {
        //读取文件信息
        Resource resource = new ClassPathResource("serverinfo/" + type + ".txt");
        InputStreamReader in = new InputStreamReader(resource.getInputStream(), "UTF-8");
        BufferedReader br = new BufferedReader(in);
        Map<String, String[]> map = new HashMap<>();
        String s = "";
        while ((s = br.readLine()) != null) {
            String[] s1 = s.split("\t");
            map.put(s1[0], s1);
        }
        return map;
    }
}
