package cn.xzr.xdfs.datanode;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;

@EnableDiscoveryClient
@SpringBootApplication
public class DatanodeApplication {
	public static void main(String[] args) {
		SpringApplication.run(DatanodeApplication.class, args);
	}

	@Bean
	CommandLineRunner init(DatanodeService datanodeService){
		return (args)->{
			datanodeService.clear();
			datanodeService.init();
		};
	}
}
