package cjlib.demo2;

import cjlib.demo2.CglibMediaInterceptor;
import cjlib.demo2.LandlordSerivce;
import cjlib.demo2.LandlordSerivceImpl;

/**
 * 租客客户端
 * @author jkl
 *
 */
public class TenantsClient {
    public static void main(String[] args) {

        CglibMediaInterceptor cglib = new CglibMediaInterceptor();
        LandlordSerivce serivce = new LandlordSerivceImpl();
        cglib.setTarget(serivce);
        LandlordSerivce landlordSerivce =(LandlordSerivce)cglib.getProxy();
        landlordSerivce.rent();
        landlordSerivce.without();

    }

}
