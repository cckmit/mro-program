package com.asiainfo.opmc.rtd.mro.constant;

public enum ServerTypeEnum {
    HW("hw", "华为"), ZTE("zte", "中兴"), NSN("nokia", "洛基亚"), DT("datang", "大唐"),Eric("eric","爱立信");

    private final String id;
    private final String name;

    private ServerTypeEnum(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
