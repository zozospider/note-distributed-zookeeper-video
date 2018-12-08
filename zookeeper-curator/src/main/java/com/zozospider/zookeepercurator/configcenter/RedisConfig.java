package com.zozospider.zookeepercurator.configcenter;

/**
 * Redis 监听配置实体对象
 * 对应 ZooKeeper 节点数据格式: {"id":"1","type":"add","url":"ftp://192.168.10.123/config/redis.xml","remark":"add"}
 */
public class RedisConfig {

    /**
     * 配置文件标识
     */
    private int id;
    /**
     * add 新增配置
     * update 更新配置
     * delete 删除配置
     */
    private String type;
    /**
     * 如果是 add 或update，则提供下载地址
     */
    private String url;
    /**
     * 备注
     */
    private String remark;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @Override
    public String toString() {
        return "RedisConfig{" +
                "type='" + type + '\'' +
                ", url='" + url + '\'' +
                ", remark='" + remark + '\'' +
                '}';
    }

}
