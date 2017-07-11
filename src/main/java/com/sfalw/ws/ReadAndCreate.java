package com.sfalw.ws;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;

/**
 * 循环读取文件生成json文件，单位为每个文书，名字为 {id}.json
 *
 * @author wusi
 * @version 2017/7/10 16:57
 */
@Service
public class ReadAndCreate {

    Logger logger = LoggerFactory.getLogger(ReadAndCreate.class);

    private static String es_host = "172.18.12.125";
    private static int es_port = 9300;
    public static String index = "writ_stick";
    public static String type = "fycase";
    public static Client client;

    static {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "wmcloudTest").build();
        try {
            client = new TransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(es_host), es_port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void readAndCreate() {
        logger.info("es开始导出");
        MatchAllQueryBuilder queryBuilder;
        SearchResponse searchResponse;
        // 从es批量查询
//        RangeQueryBuilder queryBuilder = new RangeQueryBuilder("createTime")
//                .from(new DateTime("2015-05-01T00:00:00").getMillis())
//                .to(new Date().getTime());
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
//                .setTypes(type)
//                .setQuery(queryBuilder)
//                .setSize(10)
//                .setScroll(new TimeValue(60 * 1000))
//                .setSearchType(SearchType.QUERY_THEN_FETCH);
        queryBuilder = new MatchAllQueryBuilder();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setScroll(new TimeValue(60 * 1000))
                .setFrom(0)
                .setSize(10);
        searchResponse = searchRequestBuilder.execute()
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
        for (SearchHit searchHit : firstSearchHits) {
            try {
                Files.write(Paths.get("../data/" + searchHit.getId() + ".json"),
                        searchHit.source(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        firstSearchHits = null;
        //接着写剩下的
        long alreadyStore = 0;
        alreadyStore = alreadyStore + currentResultNum;
        SearchHits searchHits;
        while (alreadyStore < total) {
            searchResponse = client.prepareSearchScroll(scrollId)
                    .setScroll(new TimeValue(60 * 1000))
                    .execute().actionGet();
            searchHits = searchResponse.getHits();
            scrollId = searchResponse.getScrollId();
            // 开始写入文件
            for (SearchHit searchHit : searchHits) {
                try {
                    Files.write(Paths.get("../data/" + searchHit.getId() + ".json"),
                            searchHit.source(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            alreadyStore = alreadyStore + searchHits.getHits().length;
        }
    }
}
