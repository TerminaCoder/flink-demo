package com.lxf.flinkdemo.frauddetection;

/**
 * @author created by LiuXF on 2023/11/23 11:36:07
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.setInteger("rest.port", 8081);
        config.setBoolean("web.submit.enable", true);
        config.setInteger("flink.ui.port", 8088);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

//      transactions.print();
//      Transaction{accountId=5, timestamp=1546295040000, amount=384.73}
//        DataStream<Transaction> transactions = env.socketTextStream("localhost", 9999).map((MapFunction<String, Transaction>) s -> {
//            String[] params = s.split(",");
//            return new Transaction(Long.parseLong(params[0]), Long.parseLong(params[1]), Double.parseDouble(params[2]));
//        });
        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetectorStatus())
                .name("fraud-detector");
        alerts.addSink(new AlertSink())
                .name("send-alerts");
        env.execute("Fraud Detection");
    }
}