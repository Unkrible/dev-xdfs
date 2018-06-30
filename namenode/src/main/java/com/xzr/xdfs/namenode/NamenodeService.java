package com.xzr.xdfs.namenode;

import com.xzr.xdfs.namenode.namespace.DataNodeManager;
import com.xzr.xdfs.namenode.namespace.FileNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import sun.net.spi.nameservice.NameService;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

@Service
public class NamenodeService {

    private static final String blockCacheFormat = "%s%s";
    private static final String blockCacheDir = "tmp/blockCache";

    @Autowired
    protected RestTemplate restTemplate;

    protected FileNode fileNodeManager = new FileNode("/");
    protected DataNodeManager dataNodeManager = new DataNodeManager();

    @Value(value="${block.block-size:${block.default-size}}")
    public int BLOCK_SIZE;

    @Value(value="${block.block-replicas:${block.default-replicas}}")
    public int REPLICA_NUM;

    protected Logger logger = Logger.getLogger(NamenodeService.class.getName());

    public void init(){
        new File(blockCacheDir).mkdirs();
    }

    public String getTest(){
        return restTemplate.getForObject("http://localhost:2222/test/", String.class);
    }

    public boolean uploadFile(MultipartFile file, String strPath, String fileName){
        List<String> fielPath = Arrays.asList(strPath.split("/"));
        FileNode directory = fileNodeManager.query(fielPath.iterator(), true);
        directory.setType(FileNode.Type.FILE);
        try{
            long blockNum = (long)Math.ceil((double)file.getSize()/(double)BLOCK_SIZE);
            BufferedInputStream fileStream = new BufferedInputStream(file.getInputStream());
            for(long i=0; i<blockNum; ++i){
                String dataNode = dataNodeManager.getDataNode();
                String blockName = String.format(FileNode.blockFormat, strPath, fileName, i);
                byte[] blockBytes = new byte[BLOCK_SIZE];
                int blockSize = fileStream.read(blockBytes);
                File block = new File(String.format(blockCacheFormat, blockCacheDir, blockName.replaceAll("/", "-")));
                FileOutputStream writer = new FileOutputStream(block);
                writer.write(blockBytes, 0, blockSize);
                writer.close();
                FileSystemResource fileBlockResource = new FileSystemResource(block);
                MultiValueMap<String, Object> parameters = new LinkedMultiValueMap<>();
                parameters.add("block", fileBlockResource);
                Boolean result = restTemplate.postForObject(dataNode, parameters, Boolean.class);
                if(result==null || !result) {
                    --i;
                    continue;
                }
                dataNodeManager.dispatchBlock(dataNode, blockName);
                fileNodeManager.addBlock(blockName, dataNode);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public boolean deleteFile(String directory, String fileName){
        return true;
    }

    public boolean registerDataNode(String dataNodeUrl) {
        return dataNodeManager.registerDataNode(dataNodeUrl);
    }
}
