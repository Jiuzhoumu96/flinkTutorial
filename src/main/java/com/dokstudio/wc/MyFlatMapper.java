package com.dokstudio.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName: MyFlatMapper
 * @Author: hechengyao
 * @Description:
 * @Date: Created in 16:40 2021/6/28
 * @Modified By:
 */
// 自定义类，实现FlatMapFunction接口
public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // 按空格分词
        String[] words = value.split(" ");
        // 遍历所有word，包成二元组输出
        for (String word : words) {
            out.collect(new Tuple2<>(word, 1));
        }
    }

}
