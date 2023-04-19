package com.chail.js;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import javax.script.*;


/**
 * @author : yangc
 * @date :2023/4/17 18:21
 * @description :
 * @modyified By:
 */
public class JsApp {

    static String JS_CODE = "(function myFun(param){" +
            "console.log('hello '+param);" +
            "var jsApp=Java.type(\"HelloPolyglot\");"+
            "var res=jsApp.add(param);" +
            "console.log('res: '+res);" +
            "})";


    public static void main(String[] args) throws Exception {
        //hellow();
        //aa();
        //test1();
        //javaonJs();

        jv2Js();
    }

    private static void test1(){
        // 创建一个JavaScript上下文
        Context context = Context.create("js");
        // 在上下文中执行一段JavaScript代码
        context.eval("js", "var hello = function(name) { return 'Hello, ' + name + '!'; }");
        // 获取JavaScript函数并调用它
        Value helloFunc = context.getBindings("js").getMember("hello");
        String result = helloFunc.execute("World").asString();
        System.out.println(result); // 输出：Hello, World!
    }



    private static void jv2Js() throws ScriptException {
        //System.setProperty("graaljs.insecure-scriptengine-access","true");
        System.setProperty("polyglot.js.nashorn-compat","true");
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("graal.js");
        // 添加类路径参数和模块路径参数
        //engine.getBindings(javax.script.ScriptContext.ENGINE_SCOPE).put("classpath", System.getProperty("java.class.path"));
        //engine.getBindings(javax.script.ScriptContext.ENGINE_SCOPE).put("modulepath", System.getProperty("jdk.module.path"));
        // 将Java对象绑定到JavaScript上下文中
        engine.put("myObject", new MyJavaObject1());
        String script = "function add(a, b) " +
                "{ var res = myObject.addNumbers(a, b); " +
                "return res ;" +
                "} " +
                "add(2, 3);";
        Object result = engine.eval(script);
        System.out.println(result); // Output: 5

    }
    public static class MyJavaObject1 {
        public Object addNumbers(int a, int b) {
            return a + b;
        }
    }


    private  static  void javaonJs(){

// 在上下文中注册一个Java对象
        MyJavaObject myJavaObject = new MyJavaObject();

        Context context = Context.newBuilder("js")
                .allowHostAccess(true)
                .build();

        context.getBindings("js").putMember("myJavaObject", myJavaObject);

        // 在上下文中执行一段JavaScript代码
        context.eval("js", "var result = myJavaObject.sayHello('World');");

        // 获取JavaScript变量并打印它
        Value result = context.getBindings("js").getMember("result");
        System.out.println(result.asString()); // 输出：Hello, World!

    }


    public static class MyJavaObject {
        public String sayHello(String name) {
            return "Hello, " + name + "!";
        }
    }


    public static int add(int i){
        return i+2;
    }


    private static void aa(){


        Context context = Context.newBuilder("js")
                .allowHostClassLoading(true)
                .build();
        Value value = context.eval("js", JS_CODE);
        value.execute(1 );
    }

    private static void hellow(){
        System.out.println("Hello Java!");
        try (Context context = Context.create()) {
            Value value = context.eval("js", JS_CODE);
            value.execute(1 );
        }
    }
}
