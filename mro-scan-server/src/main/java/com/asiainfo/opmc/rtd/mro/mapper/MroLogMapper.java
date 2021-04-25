package com.asiainfo.opmc.rtd.mro.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 * Mapper 接口
 * </p>
 *
 * @author shendan
 * @since 2021-01-08
 */
public interface MroLogMapper {
    List<String> getStationList(@Param("type") String type, @Param("time") String time);
}
