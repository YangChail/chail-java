package com.mchz.ds.dsync;

import java.lang.instrument.Instrumentation;

public class DsyncAgent {


    private static String className = "com.mchz.dsync.deployer.DSyncStarter";


    public static void premain(String args, Instrumentation instrumentation) {
        instrumentation.addTransformer(new DsyncTransformer());
    }


    public static void premain(String agentArgs) {


    }
}
