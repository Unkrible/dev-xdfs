package com.xzr.xdfs.namenode;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.util.logging.Logger;

@Service
public class NamenodeService {

    @Autowired
    @LoadBalanced
    protected RestTemplate restTemplate;

    @Value(value="${block.block-size:${block.default-size}}")
    public int BLOCK_SIZE;

    @Value(value="${block.block-replicas:${block.default-replicas}}")
    public int REPLICA_NUM;

    protected Logger logger = Logger.getLogger(NamenodeService.class.getName());

    public boolean uploadFile(MultipartFile file, String directory, String fileName){
        return true;
    }

    public boolean deleteFile(String directory, String fileName){
        return true;
    }

}
