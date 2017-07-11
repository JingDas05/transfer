package com.sfalw.ws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * @author wusi
 * @version 2017/7/11 9:36
 */

@SpringBootApplication
public class TransferApplication implements CommandLineRunner {

    @Autowired
    private ReadAndCreate readAndCreate;
    @Autowired
    private ReadAndInsert readAndInsert;

    public static void main(String[] args) {
        new SpringApplicationBuilder(TransferApplication.class).run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        readAndCreate.readAndCreate();
//        readAndInsert.bulkUpdate();
    }
}
