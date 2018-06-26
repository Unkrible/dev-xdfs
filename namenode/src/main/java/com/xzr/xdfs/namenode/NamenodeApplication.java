package com.xzr.xdfs.namenode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.RestTemplate;

import javax.jws.WebService;

@EnableDiscoveryClient
@EnableEurekaServer
@SpringBootApplication
public class NamenodeApplication {

	public static void main(String[] args) {
		SpringApplication.run(NamenodeApplication.class, args);
	}

	@LoadBalanced    // Make sure to create the load-balanced template
	@Bean
	RestTemplate restTemplate() {
		return new RestTemplate();
	}
}
