package com.tj.jiangzh.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;

/**
 * @author taojie
 */
public class AdminSample {

//    public static AdminClient adminClient;

    public static AdminClient adminClient(){
        Properties properties=new Properties();

        AdminClient adminClient=AdminClient.create(properties);

        return adminClient;
    }
}
