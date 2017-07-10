package com.sfalw.ws;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

/**
 * 循环读取文件生成json文件，单位为每个文书，名字为 {id}.json
 *
 * @author wusi
 * @version 2017/7/10 16:57
 */
public class ReadAndCreat {

    private static String es_host = "172.18.12.125";
    private static int es_port = 9300;
    private String index = "writ_stick";
    private String type = "fycase";
    private static Client client;

    static {
        client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress(es_host, es_port));
    }

    public static void main(String[] args) {

    }

    private void readAndCreate() {
        // 从es批量查询
        RangeQueryBuilder queryBuilder = new RangeQueryBuilder("_timestamp")
                .from(new DateTime("2017-05-01T00:00:00").getMillis())
                .to(new Date().getTime());
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setSize(20)
                .setScroll(new TimeValue(60 * 1000))
                .setSearchType(SearchType.QUERY_THEN_FETCH);
        SearchResponse searchResponse = searchRequestBuilder.execute()
                .actionGet();
        // 获取第一次数据
        SearchHits firstSearchHits = searchResponse.getHits();
        long total = searchResponse.getHits().getTotalHits();
        long currentResultNum = firstSearchHits.getHits().length;
        String scrollId = searchResponse.getScrollId();
        if (currentResultNum == 0) {
            return;
        }
        // 写入文件，文件名为id.json
        for (SearchHit searchHit: firstSearchHits) {
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(searchHit.getId()+".json");
                FileChannel fileChannel = fileOutputStream.getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(1024);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
