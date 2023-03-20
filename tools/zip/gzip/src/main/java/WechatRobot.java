
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class WechatRobot {
    private String robotKey;

    public WechatRobot(String robotKey) {
        this.robotKey = robotKey;
    }

    public void sendNewsMessage(String title, String description, String picUrl, String linkUrl) throws IOException {
        String url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=" + robotKey;
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        Map<String, Object> news = new HashMap<>();
        news.put("title", title);
        news.put("description", description);
        news.put("url", linkUrl);
        news.put("picurl", picUrl);

        Map<String, Object> article = new HashMap<>();
        article.put("articles", new Object[]{news});

        Map<String, Object> message = new HashMap<>();
        message.put("msgtype", "news");
        message.put("news", article);

        //String json = JSONObject.toJSONString(message);

        OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
        //writer.write(json);
        writer.flush();
        writer.close();

        connection.getResponseCode(); // Trigger the request
        connection.disconnect();
    }

    public static void main(String[] args) throws IOException {
        WechatRobot robot = new WechatRobot("0ffacdd3-bcb5-4a97-9a9d-68a1a82ad1f9");
        String title = "篮球比赛报名通知";
        String description = "我们将于下周末举办篮球比赛，现在开始报名。";
        String picUrl = "https://example.com/basketball.png";
        String linkUrl = "https://example.com/basketball/register";
        robot.sendNewsMessage(title, description, picUrl, linkUrl);
    }
}
