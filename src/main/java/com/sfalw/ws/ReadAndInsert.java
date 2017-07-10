package com.sfalw.ws;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

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
public class ReadAndInsert {

    public static void main(String[] args) {
        bulkUpdate();
    }

    public static void bulkUpdate() {
        byte[] jsonData = new byte[0];
        BulkRequestBuilder bulkRequestBuilder = ReadAndCreat.client.prepareBulk();
        // 依次读取json, 累计100
        File parentFile = new File("../data");
        RegexFileFilter filter = new RegexFileFilter("^(.*?)");
        Iterator<File> fileIterator = FileUtils.iterateFiles(parentFile, filter, DirectoryFileFilter.DIRECTORY);
        int bound = 150;
        int currentBound = 1;
        boolean isContinue = true;
        boolean hasNext = true;
        BulkResponse bulkResponse;
        while (isContinue) {
            hasNext = fileIterator.hasNext();
            try {
                jsonData = Files.readAllBytes(Paths.get(fileIterator.next().getAbsolutePath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (currentBound <= bound) {
                bulkRequestBuilder.add(ReadAndCreat.client
                        .prepareIndex(ReadAndCreat.index, ReadAndCreat.type)
                        .setSource(jsonData));
                currentBound++;
                isContinue = hasNext;
            } else {
                bulkResponse = bulkRequestBuilder.execute().actionGet();
                // 复位
                isContinue = !bulkResponse.hasFailures() && hasNext;
                currentBound = 1;
                bulkRequestBuilder = ReadAndCreat.client.prepareBulk();
            }
        }
    }
}


