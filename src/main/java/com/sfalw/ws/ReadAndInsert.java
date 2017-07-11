package com.sfalw.ws;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * 循环读取每个文件，积累到100，批量更新es
 *
 * @author wusi
 * @version 2017/7/10 16:57
 */
@Service
public class ReadAndInsert {

    Logger logger = LoggerFactory.getLogger(ReadAndInsert.class);

    public void bulkUpdate() {
        byte[] jsonData = new byte[0];
        BulkRequestBuilder bulkRequestBuilder = ReadAndCreate.client.prepareBulk();
        // 依次读取json, 累计150
        File parentFile = new File("../data");
        RegexFileFilter filter = new RegexFileFilter("^(.*?)");
        Iterator<File> fileIterator = FileUtils.iterateFiles(parentFile, filter, DirectoryFileFilter.DIRECTORY);
        int bound = 150;
        int currentBound = 1;
        boolean isContinue = true;
        BulkResponse bulkResponse;
        File eachFile;
        while (fileIterator.hasNext() && isContinue) {
            eachFile = fileIterator.next();
            try {
                jsonData = Files.readAllBytes(Paths.get(eachFile.getAbsolutePath()));
            } catch (IOException e) {
                e.printStackTrace();
                isContinue = false;
            }
            if (currentBound <= bound) {
                bulkRequestBuilder.add(ReadAndCreate.client
                        .prepareIndex(ReadAndCreate.index, ReadAndCreate.type)
                        .setId(StringUtils.split(eachFile.getName(), ".")[0])
                        .setSource(jsonData));
                currentBound++;
            } else {
                bulkResponse = bulkRequestBuilder.execute().actionGet();
                // 复位
                isContinue = !bulkResponse.hasFailures();
                currentBound = 1;
                bulkRequestBuilder = ReadAndCreate.client.prepareBulk();
            }
        }
        bulkResponse = bulkRequestBuilder.execute().actionGet();
        Assert.state(!bulkResponse.hasFailures(), "插入成功");
    }
}


