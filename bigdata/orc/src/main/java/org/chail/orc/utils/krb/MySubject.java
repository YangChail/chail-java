package org.chail.orc.utils.krb;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.net.URI;

/**
 * @ClassName : MySubject
 * @Description : Subject的包装
 * @Author : Chail
 * @Date: 2020-11-02 19:06
 */
public class MySubject {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySubject.class);

    public static final long LOGIN_TIMEOUT_MS=60*60*1000;

    /**
     * 上下文
     */
    private Subject subject;
    /**
     * 最后登录时间
     */
    private long lastLoginTime;
    /**
     * 验证问题
     */
    private String authority;


    /**
     * 认证UGI
     */
    private UserGroupInformation userGroupInformation;




    public MySubject(String filePath) {
        getAuthority(filePath);
        this.lastLoginTime=System.currentTimeMillis();
        subject=new Subject();
    }

    public Subject getSubject() {
        return subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public long getLastLoginTime() {
        return lastLoginTime;
    }

    public void setLastLoginTime(long lastLoginTime) {
        this.lastLoginTime = lastLoginTime;
    }

    public UserGroupInformation getUserGroupInformation() {
        return userGroupInformation;
    }

    public void setUserGroupInformation(UserGroupInformation userGroupInformation) {
        this.userGroupInformation = userGroupInformation;
    }

    public  void getAuthority(String filePath){
        try {
            URI uri = new URI(filePath);
            this.authority = uri.getAuthority();
        } catch (Exception e) {
            LOGGER.debug("获取权限错误",e);
            this.authority= filePath;
        }
    }


    public boolean checkTimeout(){
        return (System.currentTimeMillis()-lastLoginTime)>lastLoginTime;
    }


}
