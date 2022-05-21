package com.xubowen.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

/**
 * @author XuBowen
 * @date 2022/5/1 20:15
 */
public class MySource implements SourceFunction<WaterSensor> {

    @Override
    public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
        int index=100;
        int id =0;
        long ts=System.currentTimeMillis();
        int wm=100;
        while (index!=0){
            sourceContext.collect(new WaterSensor(String.valueOf(id),ts,wm));
            id=new Random().nextInt(10);
            ts=System.currentTimeMillis();
            wm+=5;
            Thread.sleep(1000);
            index--;
        }
    }

    @Override
    public void cancel() {

    }
}
