package com.asiainfo.opmc.rtd.mro.constant;

public enum ScanErrorTypeEnum {

    SERVER_CANNOT_CONNECT("1", "无法正常连接到服务器"),
    DIC_NOT_EXIST("2", "未生成当前账期的文件夹"),
    FILE_NOT_EXIST("3", "当前账期下无可采集文件"),
    COLLECT_ERROR("4", "采集程序未正常进行采集"),
    ;

    private final String code;
    private final String desc;

    private ScanErrorTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
