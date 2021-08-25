package com.dokstudio.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: WordCount
 * @Author: hechengyao
 * @Description:
 * @Date: Created in 16:07 2021/6/28
 * @Modified By:
 */
public class WordCount {

    private final static Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {

        logger.info(">>>>>> start");

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中获取数据
        String inputPath = "hdfs://beh001/tmp/hechengyao/wcInput.txt";
        // String inputPath = "src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        // DataSource<String> stringDataSource = env.readTextFile(inputPath);
        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)     //按照第一个位置的word分组
                .sum(1);        //将第二个位置的数据求和
        resultSet.print();
    }

}
