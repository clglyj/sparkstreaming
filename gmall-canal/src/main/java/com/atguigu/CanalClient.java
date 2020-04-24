package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.KafkaSend;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;



public class CanalClient {


    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop104", 11111), "example", "", "");

        //解析mysql日志的数据结构   column <  rowdata < rowchange < entry < message
        while (true){

            //获取链接
            canalConnector.connect();
            //订阅表名称
            canalConnector.subscribe("gmall.*");
            //获取message对象
            Message message = canalConnector.get(100);

            //获取数据size
            int size = message.getEntries().size();

            if(size == 0){
                System.out.println("没有新数据。。。。。");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
//                column <  rowdata < rowchange < entry < message
                //解析message中数据

                for (CanalEntry.Entry entry:message.getEntries()) {
                    /**
                     *    message中的entry对象有以下几种类型，
                     *    public static enum EntryType implements ProtocolMessageEnum {
                     *         TRANSACTIONBEGIN(0, 1),  开启事务
                     *         ROWDATA(1, 2),  数据
                     *         TRANSACTIONEND(2, 3),  关闭四五
                     *         HEARTBEAT(3, 4),  心跳信息
                     *         GTIDLOG(4, 5); 记录日志
                     */
                    if(CanalEntry.EntryType.ROWDATA  == entry.getEntryType()){
                        //获取rowchange
                        CanalEntry.RowChange rowChange = null ;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //获取表名
                        String  tableName  =  entry.getHeader().getTableName() ;

                        //获取eventType对象，主要用于区分操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //获取行数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //处理行数据
                        handler(eventType, tableName, rowDatasList);
                    }else{
                        System.out.println("非数据信息，抛弃。。。。。");
                    }
                }

            }

        }

    }

    private static void handler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDatasList) {

        //                column <  rowdata < rowchange < entry < message
        /*只要指定表的数据以及插入的新数据，更新+删除+查询语句数据不做任何处理*/
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType) ){
            tableCommon(rowDatasList,GmallConstants.GMALL_ORDER_INFO_TOPIC);
        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            tableCommon(rowDatasList,GmallConstants.GMALL_ORDER_DETAIL_TOPIC);
        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) ||  CanalEntry.EventType.UPDATE.equals(eventType))){
            tableCommon(rowDatasList,GmallConstants.GMALL_USER_INFO_TOPIC);
        }
    }

    /**
     * 抽取方法快捷键：CTRL+ALT+M
     * @param rowDatasList
     * @param topic
     */
    private static void tableCommon(List<CanalEntry.RowData> rowDatasList,String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {

            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column  column:afterColumnsList) {
                jsonObject.put(column.getName(),column.getValue());
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //发送kafka
            KafkaSend.send(topic,jsonObject.toString());
        }
    }


}
