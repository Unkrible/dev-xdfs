package com.xzr.xdfs.namenode;

import com.xzr.xdfs.namenode.namespace.FileNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

@Service
public class NamenodeService {

    @Autowired
    protected RestTemplate restTemplate;

    protected FileNode rootNode = new FileNode("/");

    @Value(value="${block.block-size:${block.default-size}}")
    public int BLOCK_SIZE;

    @Value(value="${block.block-replicas:${block.default-replicas}}")
    public int REPLICA_NUM;

    protected Logger logger = Logger.getLogger(NamenodeService.class.getName());

    public String getTest(){
        return restTemplate.getForObject("http://localhost:2222/test/", String.class);
    }

    public boolean uploadFile(MultipartFile file, String strPath, String fileName){
        List<String> fielPath = Arrays.asList(strPath.split("/"));
        FileNode directory = rootNode.query(fielPath.iterator());
        return true;
    }

    public boolean deleteFile(String directory, String fileName){
        return true;
    }

}
