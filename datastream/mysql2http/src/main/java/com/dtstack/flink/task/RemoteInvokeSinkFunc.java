package com.dtstack.flink.task;

import com.dtstack.flink.pojo.Response;
import com.dtstack.flink.util.RemoteInvoke;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 调用接口发送结果
 *
 * @author beifeng
 */
public class RemoteInvokeSinkFunc extends RichSinkFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteInvokeSinkFunc.class);
    private final String ip;
    private final String port;

    public RemoteInvokeSinkFunc(String ip, String port) {
        this.ip = ip;
        this.port = port;
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {

        // 调用接口
        Response response = RemoteInvoke
                .remoteGetRequest(
                        getRemoteUrl(value.getField(0).toString())
                );
        if (response != null) {
            final String status = response.getStatus();
            final String success = "success";
            if (success.equals(status)) {
                // HE IS NOW NOTHING
            }
        }
        LOG.info("received on response : {}", response);

    }

    private String getRemoteUrl(String lsn) {
        return "http://" + ip + ":" + port + "/ddlresolver-api/monitorApi/ddlChangeMonitor?lsn=" + lsn;
    }
}
