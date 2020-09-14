package com.yi.streaming.socket.pojo;

import java.io.Serializable;

/**
 * Description: sdk传到后台Data解析
 *
 * @author zys
 */
public class KinesisData implements Serializable {

    /**
     * 事件上报id
     */
    private Integer event_id = 0;

    /**
     * 应用id
     */
    private Integer app_id = 0;

    /**
     * 平台id
     */
    private Integer platform_id = 0;

    /**
     * 客户端时间戳
     */
    private Long event_time_client = 0L;

    /**
     * 角色id
     */
    private String role_id = "null";

    /**
     * 服务端生成的open_id
     */
    private String open_id = "null";

    /**
     * 服务端生成的cp_open_id
     */
    private String cp_open_id = "null";

    public KinesisData() {

    }

    public Integer getEvent_id() {
        return event_id;
    }

    public void setEvent_id(Integer event_id) {
        this.event_id = event_id;
    }

    public Integer getApp_id() {
        return app_id;
    }

    public void setApp_id(Integer app_id) {
        this.app_id = app_id;
    }

    public Long getEvent_time_client() {
        return event_time_client;
    }

    public void setEvent_time_client(Long event_time_client) {
        this.event_time_client = event_time_client;
    }

    public Integer getPlatform_id() {
        return platform_id;
    }

    public void setPlatform_id(Integer platform_id) {
        this.platform_id = platform_id;
    }

    public String getRole_id() {
        return role_id;
    }

    public void setRole_id(String role_id) {
        this.role_id = role_id;
    }

    public String getOpen_id() {
        return open_id;
    }

    public void setOpen_id(String open_id) {
        this.open_id = open_id;
    }

    public String getCp_open_id() {
        return cp_open_id;
    }

    public void setCp_open_id(String cp_open_id) {
        this.cp_open_id = cp_open_id;
    }
}
