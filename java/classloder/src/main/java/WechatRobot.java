import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public class WechatRobot {
    private String robotKey;

    public WechatRobot(String robotKey) {
        this.robotKey = robotKey;
    }

    public void sendMarkdownMessage(String content) throws IOException {
        String url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=" + robotKey;
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        Map<String, Object> markdown = new HashMap<>();
        markdown.put("content", content);

        Map<String, Object> message = new HashMap<>();
        message.put("msgtype", "markdown");
        message.put("markdown", markdown);

        String json = JSONObject.toJSONString(message);

        OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
        writer.write(json);
        writer.flush();
        writer.close();

        connection.getResponseCode(); // Trigger the request
        connection.disconnect();
    }

    public static void main(String[] args) throws IOException {
        WechatRobot robot = new WechatRobot("0ffacdd3-bcb5-4a97-9a9d-68a1a82ad1f9");
        String title = "篮球比赛报名通知";
        String description = "我们将于下周末举办篮球比赛，现在开始报名。";
        String picUrl = "https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fc-ssl.duitang.com%2Fuploads%2Fitem%2F201808%2F04%2F20180804103635_uKQYr.jpeg&refer=http%3A%2F%2Fc-ssl.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=auto?sec=1681007327&t=df0c70b7ba8f6baf4dc6e20cafae65f0";
        String linkUrl = "https://doc.weixin.qq.com/forms/q/AIgAJQfqAAoAKIAKQYpAHU05jT65ggTVq";
        String time = "2023年3月18日，上午9:00-11:00";
        int signUpCount = 15;
        String content = String.format(
                "# %s\n\n" +
                        "![image](%s)\n\n" +
                        "> %s\n\n" +
                        "> 报名时间：%s\n\n" +
                        "> 已报名人数：%d人\n\n" +
                        "[点击报名](%s)",
                title, picUrl, description, time, signUpCount, linkUrl);
        robot.sendMarkdownMessage(content);
    }
}
