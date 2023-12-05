package com.atguigu.flink.source;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class ClickSource implements SourceFunction<Event> {


    private volatile boolean isRunning = true;

    /*
     * 生成数据
     */
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        String[] users = {"zhangsan","lisi","Tom","Jerry"};
        String[] urls = {"/home","/detail","/login","/order","/pay"};
        // 模拟持续生成数据，1秒1条数据
        while (isRunning) {
            String user = users[RandomUtils.nextInt(0,users.length)];
            String url = urls[RandomUtils.nextInt(0,urls.length)];
            Event event = new Event(user,url,System.currentTimeMillis());

            // 收集数据
            ctx.collect(event);
            TimeUnit.SECONDS.sleep(1);
        }
    }


    /*
     * 退出，停止数据生成
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
