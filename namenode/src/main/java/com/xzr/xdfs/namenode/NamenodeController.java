package com.xzr.xdfs.namenode;

import com.netflix.appinfo.InstanceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceRegisteredEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;


import javax.servlet.http.HttpServletRequest;
import java.util.logging.Logger;


@RestController
public class NamenodeController {

    @Autowired
    protected NamenodeService namenodeService;

    protected Logger logger = Logger.getLogger(NamenodeController.class.getName());


    @GetMapping(value = "/**/{fileName}")
    public String downloadFile(HttpServletRequest request, @PathVariable("fileName") String fileName) {
        String directory = request.getRequestURI();
        logger.info("downloadFile() invoked: for " + directory + "/" + fileName);
        return directory + "#" + fileName;
    }

    @PostMapping(value = "/**/{fileName}")
    public ResponseEntity<?> uploadFile(HttpServletRequest request,
                                        @RequestParam("file")MultipartFile file,
                                        @PathVariable("fileName")String fileName){
        String directory = request.getRequestURI();
        logger.info("uploadFile() invoked: for " + directory + "/" + fileName);
        boolean result = namenodeService.uploadFile(file, directory, fileName);
        if(result) {
            logger.info("uploadFile() succeed: for " + directory + "/" + fileName);
            return ResponseEntity.ok().build();
        }
        else {
            logger.info("uploadFile() fail: for " + directory + "/" + fileName);
            return ResponseEntity.badRequest().build();
        }
    }

    @DeleteMapping(value = "/**/{fileName}")
    public ResponseEntity<?> deleteFile(HttpServletRequest request, @PathVariable("fileName")String fileName) {
        String directory = request.getRequestURI();
        boolean result = namenodeService.deleteFile(directory, fileName);
        if(result) {
            logger.info("deleteFile() succeed: for " + directory + "/" + fileName);
            return ResponseEntity.ok().build();
        }
        else {
            logger.info("deleteFile() fail: for " + directory + "/" + fileName);
            return ResponseEntity.badRequest().build();
        }
    }

    // Eureka服务注册, register DataNode.
    @EventListener
    public void listen(EurekaInstanceRegisteredEvent event) {
        InstanceInfo instanceInfo = event.getInstanceInfo();
        String dataNodeUrl = instanceInfo.getHomePageUrl();
        System.err.println(dataNodeUrl + "进行注册");
    }
}
