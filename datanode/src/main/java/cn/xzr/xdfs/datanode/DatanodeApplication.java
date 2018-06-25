package cn.xzr.xdfs.datanode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class DatanodeApplication {
	public static void main(String[] args) {
		SpringApplication.run(DatanodeApplication.class, args);
	}
}
