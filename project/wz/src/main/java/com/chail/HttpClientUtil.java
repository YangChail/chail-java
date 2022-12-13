package com.chail;
 

import com.alibaba.fastjson.JSONObject;
import com.chail.datasupport.tools.CheckJob;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
 

public class HttpClientUtil {
    public static final Logger log = LogManager.getLogger(CheckJob.class);
    public static String doGet(String url, String proxyUrl, Map<String, String> header, Map<String, String> param) {
        log.info("request doGet, url:{}, proxyUrl:{}, header:{}, param:{}", url, proxyUrl, header, param);
 
        // 创建Httpclient对象
        CloseableHttpClient httpclient = null;
 
        // 走代理请求
        if (proxyUrl!=null&&!"".equals(proxyUrl)) {
            //设置代理IP、端口、协议
            HttpHost proxy = HttpHost.create(proxyUrl);
            //把代理设置到请求配置
            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setProxy(proxy)
                    .build();
            httpclient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();
        } else {
            httpclient = HttpClients.createDefault();
        }
 
        String resultString = "";
        CloseableHttpResponse response = null;
        try {
            // 创建uri
            URIBuilder builder = new URIBuilder(url);
            if (param != null) {
                for (String key : param.keySet()) {
                    builder.addParameter(key, param.get(key));
                }
            }
            URI uri = builder.build();
 
            // 创建http GET请求
            HttpGet httpGet = new HttpGet(uri);
 
            // header
            if (header != null) {
                Header[] allHeader = new BasicHeader[header.size()];
                int i = 0;
                for (Map.Entry<String, String> entry: header.entrySet()){
                    allHeader[i] = new BasicHeader(entry.getKey(), entry.getValue());
                    i++;
                }
                httpGet.setHeaders(allHeader);
            }
 
            // 执行请求
            response = httpclient.execute(httpGet);
            resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
            // 判断返回状态是否为200
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                log.error("doGet response error, url:{}, statusCode:{}, msg:{}", url, statusCode, resultString);
                throw new RuntimeException(resultString);
            }
        } catch (Exception e) {
            log.error("doGet request error:", e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                httpclient.close();
            } catch (IOException e) {
                log.error("doGet closeableHttpResponse close error:", e);
            }
        }
        return resultString;
    }
 
    public static String doGet(String url) {
        return doGet(url, null, null, null);
    }
 
    public static String doGet(String url, String proxyUrl) {
        return doGet(url, proxyUrl, null, null);
    }
 
    public static String doPost(String url, String proxyUrl, Map<String, String> header, String param) {
        if (header == null) {
            header = new HashMap<>();
        }
        header.put("Content-type", "application/json;charset=UTF-8");
        //log.info("request doPost, url:{}, proxyUrl:{}, header:{}, param:{}", url, proxyUrl, header, param);
 
        // 创建Httpclient对象
        CloseableHttpClient httpClient = null;
 
        // 走代理请求
        if (proxyUrl!=null&&!"".equals(proxyUrl)) {
            //设置代理IP、端口、协议
            HttpHost proxy = HttpHost.create(proxyUrl);
            //把代理设置到请求配置
            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setProxy(proxy)
                    .build();
            httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();
        } else {
            httpClient = HttpClients.createDefault();
        }
 
        CloseableHttpResponse response = null;
        String resultString = "";
        try {
            // 创建Http Post请求
            HttpPost httpPost = new HttpPost(url);
            // header
            Header[] allHeader = new BasicHeader[header.size()];
            int i = 0;
            for (Map.Entry<String, String> entry: header.entrySet()){
                allHeader[i] = new BasicHeader(entry.getKey(), entry.getValue());
                i++;
            }
            httpPost.setHeaders(allHeader);
            // 创建参数列表
            if (param != null) {
                StringEntity stringEntity = new StringEntity(param, "UTF-8");
                stringEntity.setContentEncoding("UTF-8");
                stringEntity.setContentType("application/json");
                httpPost.setEntity(stringEntity);
            }
            // 执行http请求
            response = httpClient.execute(httpPost);
            resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
            // 判断返回状态是否为200
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                log.error("doPost response error, url:{}, statusCode:{}, msg:{}", url, statusCode, resultString);
                throw new RuntimeException(resultString);
            }
        } catch (Exception e) {
            log.error("doPost request error:", e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                httpClient.close();
            } catch (IOException e) {
                log.error("doPost closeableHttpResponse close error:", e);
            }
        }
 
        return resultString;
    }
 
    public static String doPost(String url) {
        return doPost(url, null, null, null);
    }
 
    public static String doPost(String url, String proxyUrl) {
        return doPost(url, proxyUrl, null, null);
    }



    public static void send(String content){
        //String key = "18bf291f-d52b-4ae4-9812-accac5116e16";// 从建群的机器人那里获取
        String key = "c9be1ef0-95bc-4c56-9e6f-ec20a8275b41";
        String url = String.format("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=%s", key);
        Map<String, Object> param = new HashMap<>();
        String msgType = "text";
        param.put("msgtype", msgType);
        Map<String, Object> msg = new HashMap<>();
        msg.put("content", content);
        param.put(msgType, msg);
        String jsonParam = JSONObject.toJSONString(param);
        String result = HttpClientUtil.doPost(url, null, null, jsonParam);
        System.out.println(result);
    }
 
}