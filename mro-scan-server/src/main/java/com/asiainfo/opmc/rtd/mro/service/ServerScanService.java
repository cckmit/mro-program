package com.asiainfo.opmc.rtd.mro.service;

import java.io.IOException;
import java.util.List;

public interface ServerScanService {
    List<String[]> getErrorServers(String type, String date) throws IOException;
}
