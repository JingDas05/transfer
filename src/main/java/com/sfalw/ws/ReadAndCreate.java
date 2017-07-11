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
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
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

    private static String es_host = "10.23.12.17";
    private static int es_port = 9301;
    public static String index = "writ_stick";
    public static String type = "fycase";
    public static Client client;
    public static final int EACH_FILE_SIZE = 350000;
    public static String fileFolderName = "../data";
    public static long beginFileFolderNum = 0;

    static {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "sfalw2.0").build();
        try {
            client = new TransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(es_host), es_port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        // 数据分开文件夹存储
        for (int i = 0; i < 10; i++) {
            File file = new File(fileFolderName + i + "/");
            if (!file.exists()) {
                file.mkdirs();
            }
        }
    }

    public void readAndCreate() {
        logger.info("es开始导出");
        Date beginTime = new Date();
        RangeQueryBuilder queryBuilder;
        SearchResponse searchResponse;
        // 从es批量查询
        queryBuilder = new RangeQueryBuilder("createTime")
                .from(new DateTime("2017-05-01T00:00:00").getMillis())
                .to(new Date().getTime());
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setSize(10)
                .setScroll(new TimeValue(60 * 1000))
                .setSearchType(SearchType.QUERY_THEN_FETCH);
//        queryBuilder = new MatchAllQueryBuilder();
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
//                .setTypes(type)
//                .setQuery(queryBuilder)
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
//                .setScroll(new TimeValue(60 * 1000))
//                .setSize(10)
//                .addSort("id", SortOrder.DESC);
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
                Files.write(Paths.get(fileFolderName + beginFileFolderNum + "/" + searchHit.getId() + ".json"),
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
            if (StringUtils.isEmpty(scrollId)) {
                return;
            }
            searchResponse = client.prepareSearchScroll(scrollId)
                    .setScroll(new TimeValue(60 * 1000))
                    .execute().actionGet();
            searchHits = searchResponse.getHits();
            scrollId = searchResponse.getScrollId();
            // 开始写入文件
            for (SearchHit searchHit : searchHits) {
                try {
                    Files.write(Paths.get(fileFolderName + beginFileFolderNum + "/" + searchHit.getId() + ".json"),
                            searchHit.source(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            alreadyStore = alreadyStore + searchHits.getHits().length;
            beginFileFolderNum = alreadyStore / EACH_FILE_SIZE;
            logger.info("scrollSearch" + alreadyStore);
        }
        beginFileFolderNum = 0;
        logger.info("es导出结束");
        Date endTime = new Date();
        logger.info("耗时" + (endTime.getTime() - beginTime.getTime()) / 1000 + "秒");
    }
}
