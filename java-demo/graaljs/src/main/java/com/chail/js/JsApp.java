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

        jsv2();
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


    private static void jsv2() throws ScriptException {
        //入参定义config   json
        //返回值 message json
        String code="function main(idNum) {\n" +
                "  // 身份证号码正则表达式\n" +
                "  var idNumPattern = /^[1-9]\\d{5}((19\\d{2})|(20[0-2]\\d))((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$/;\n" +
                "   \n" +
                "  if (!idNumPattern.test(idNum)) {\n" +
                "    return false;\n" +
                "  }\n" +
                "   \n" +
                "  // 校验码计算\n" +
                "  var weightedSum = 0;\n" +
                "  for (var i = 0; i < 17; i++) {\n" +
                "    weightedSum += parseInt(idNum.charAt(i)) * [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2][i];\n" +
                "  }\n" +
                "  var checkCode = [\"1\", \"0\", \"X\", \"9\", \"8\", \"7\", \"6\", \"5\", \"4\", \"3\", \"2\"][weightedSum % 11];\n" +
                "  return checkCode === idNum.charAt(17).toUpperCase();\n" +
                "}\n" ;

                String codemain=
                "// 获取字段名\n" +
                "var col_name=ds_col_name;\n" +
                "var col_var=dsContext.getValueFromColumn(col_name)\n" +
                "main(col_var);";
        System.setProperty("polyglot.js.nashorn-compat","true");
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("graal.js");
        engine.put("ds_col_name","aa");
        engine.put("dsContext", new DsContext());
        Object result = engine.eval(code+codemain);
        System.out.println(result); // Output: 5

    }

    //上下文列表
    //1.字段名称
    //2.根据字段名获取字段值
    //3.

    public static class DsContext {
        public Object getValueFromColumn(String aa) {
            return "450481197804234432";
        }
    }


    private static void jv2Js() throws ScriptException {
        //先设置环境变量
        System.setProperty("polyglot.js.nashorn-compat","true");
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("graal.js");
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
