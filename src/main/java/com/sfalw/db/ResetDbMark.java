package com.sfalw.db;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;

/**
 * @author wusi
 * @version 2017/8/2 17:24
 */
public class ResetDbMark {

    private static DriverManagerDataSource dataSource;
    private static JdbcTemplate jdbcTemplate;

    static {
//        dataSource = new DriverManagerDataSource();
//        dataSource.setDriverClassName("org.postgresql.Driver");
//        dataSource.setUrl("jdbc:postgresql://172.18.12.118:5432/sfal2");
//        dataSource.setUsername("thunisoft");
//        dataSource.setPassword("123456");

        dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://172.18.7.38:5432/sfal2");
        dataSource.setUsername("sa");
        dataSource.setPassword("tusc@6789#JKL");
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public static void main(String[] args) {
        // 查询所有模式名
        List<String> schamas = jdbcTemplate.queryForList("SELECT DISTINCT schemaname FROM pg_tables WHERE schemaname NOT LIKE 'pg%' AND schemaname NOT LIKE 'sql_%' AND \n" +
                "schemaname NOT LIKE 'information%';", String.class);
        for (String s : schamas) {
            List<String> tableNames = jdbcTemplate.queryForList("select tablename from pg_tables where schemaname= ? ;",
                    String.class, s);
            // 查询每个模式下面的所有表
            for (String t : tableNames) {
//                System.out.print("update " + s + "." + t + " set n_mark = 0;");
                try {
                    jdbcTemplate.execute("update " + s + "." + t + " set n_mark = 0;");
                }catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
