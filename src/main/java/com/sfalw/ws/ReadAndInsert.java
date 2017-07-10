package com.sfalw.ws;

import org.elasticsearch.action.bulk.BulkRequestBuilder;

/**
 * 循环读取每个文件，积累到100，批量更新es
 *
 * @author wusi
 * @version 2017/7/10 16:57
 */
public class ReadAndInsert {

    public static void bulkUpdate() {
        BulkRequestBuilder bulkRequestBuilder = ReadAndCreat.client.prepareBulk();
        // 依次读取json, 累计100

        for (;;){
            bulkRequestBuilder.add(ReadAndCreat.client
                    .prepareIndex(ReadAndCreat.index, ReadAndCreat.type)
                    .setSource(json));
        }
        bulkRequestBuilder.execute().actionGet();
    }
}
