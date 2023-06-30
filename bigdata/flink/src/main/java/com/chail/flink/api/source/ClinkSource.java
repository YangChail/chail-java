package com.chail.flink.api.source;

import com.chail.flink.model.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author : yangc
 * @date :2023/6/30 11:37
 * @description :
 * @modyified By:
 */
public class ClinkSource implements SourceFunction<Event> {

    private boolean run=true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        //定义字段选取的数据集
        String[] user = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=100", "./prod?id=10"};
        int i=0;
        while (run&&i<10){
            ctx.collect(new Event(user[random.nextInt(user.length)],urls[random.nextInt(urls.length)],System.currentTimeMillis(),random.nextInt(100)));
            Thread.sleep(1000);
            i++;
        }
    }
    @Override
    public void cancel() {
        run=false;
    }
}
