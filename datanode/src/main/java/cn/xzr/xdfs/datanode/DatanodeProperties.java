package cn.xzr.xdfs.datanode;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatanodeProperties {

    @Value(value ="${server.port}")
    protected Integer location;

    public String getLocation(){
        return location.toString();
    }
}
