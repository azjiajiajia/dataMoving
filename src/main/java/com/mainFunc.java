package com; /**
 * @(#)mainFunc.java, 12月 24, 2021.
 * <p>
 * Copyright 2021 pinghang.com. All rights reserved.
 * PINGHANG.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * <h2></h2>
 * @author jcc
 * date 2021/12/24
 */
public class mainFunc {

    private static String url = "jdbc:mysql://41.246.146.166:3306/xiankan2.0?" +
            "autoReconnect=true&characterEncoding=utf8";
    private static String pwd = "Pinghang@123";
    private static String user = "root";
    private static String[] tables = {"xk_case", "xk_app", "xk_app_certificate_character", "xk_app_certificate_file", "xk_app_company",
        "xk_app_ip", "xk_app_key_value", "xk_app_permission", "xk_app_permission_code", "xk_app_resource", "xk_app_string", "xk_assessment", "xk_assessment_group",
        "xk_bill", "xk_bill_account", "xk_case_attachment", "xk_case_detail", "xk_case_label", "xk_case_monitor", "xk_case_network_forensics",
        "xk_case_operate_log", "xk_case_statistic", "xk_devi_account", "xk_devi_address_book", "xk_devi_address_book_detail", "xk_devi_attachment",
        "xk_devi_call_record", "xk_devi_company", "xk_devi_domain", "xk_devi_evidence_file", "xk_devi_ip", "xk_devi_sim", "xk_devi_sms_record",
        "xk_devi_statistic", "xk_device", "xk_device_case_person", "xk_device_police", "xk_domain_cdn", "xk_domain_dns", "xk_domain_record", "xk_ftp_push",
        "xk_ftp_push_queue", "xk_hideout", "xk_hideout_case", "xk_hideout_pic", "xk_im_account", "xk_im_chat_attr", "xk_im_group_account", "xk_im_group_chat",
        "xk_im_group_members", "xk_im_oppo_account", "xk_im_oppo_chat", "xk_materials", "xk_materials_group", "xk_person", "xk_person_real_identity",
        "xk_person_virtual_identity", "xk_police", "xk_upload", "xk_upload_app", "xk_upload_log", "xk_url", "xk_url_company", "xk_url_detail",
        "xk_url_ip", "xk_url_label", "xk_url_reverse", "xk_url_trait"};

//    private static String url = "jdbc:mysql://192.168.2.181:3306/ph_xiankan_dev?" +
//        "autoReconnect=true&characterEncoding=utf8";
//    private static String pwd = "Pinghang@123";
//    private static String user = "root";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con = DriverManager.getConnection(url, user, pwd);
        System.out.println(con);

        Map<String, String> poliNoIdMap = new HashMap<String, String>();

        Statement stmt = con.createStatement();
        Statement stmt1 = con.createStatement();
        Statement stmt2 = con.createStatement();
        Statement stmt3 = con.createStatement();
        long total = 0L;

        //将所有警员号和警员id建立关系
        String policeSql = "SELECT poli_id,poli_no_src FROM xk_police";
        ResultSet policeSet = stmt2.executeQuery(policeSql);
        while (policeSet.next()) {
            String no = policeSet.getString("poli_no_src");
            if (no == null || "".equals(no)) {
                continue;
            }
            poliNoIdMap.put(no, policeSet.getString("poli_id"));
        }

//        ExecutorService executor = Executors.newFixedThreadPool(tables.length);

        for (int i = 0; i < tables.length; i++) {
            //获取所有列
            ResultSet columnsSet = stmt3.executeQuery("SHOW COLUMNS FROM " + tables[i] + "");
            //根据获取的列获取主键id字段名
            String keyName = getKeyName(columnsSet);
            //获取表里所有数据
            String sql = "SELECT * FROM " + tables[i] + "";

            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String modifier = resultSet.getString("modifier");
                String creator = resultSet.getString("creator");
                String id = resultSet.getString(keyName);
                String creatorId = Optional.ofNullable(poliNoIdMap.get(creator)).orElse("admin");
                String modifierId = Optional.ofNullable(poliNoIdMap.get(modifier)).orElse("admin");
                String updateSql = "UPDATE " + tables[i] + " SET modifier = '" + modifierId + "', creator = '" + creatorId
                        +"' WHERE " + keyName + " = '" + id + "'";
                stmt1.execute(updateSql);
                System.out.println("已更新" + ++total + "条");
            }
        }


        System.out.println("执行完成，关闭数据库连接");
        con.close();
    }

    private static String getKeyName(ResultSet columnsSet) throws SQLException {
        while (columnsSet.next()) {
            if ("PRI".equals(columnsSet.getString("Key"))) {
                return columnsSet.getString("Field");
            }
        }

        return "id";
    }
}
