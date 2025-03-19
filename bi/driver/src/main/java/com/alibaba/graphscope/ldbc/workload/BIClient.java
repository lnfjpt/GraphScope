package com.alibaba.graphscope.ldbc.workload;

import com.alibaba.pegasus.RpcChannel;

import com.alibaba.graphscope.ldbc.workload.proto.BiClient;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.protobuf.ByteString;
import com.alibaba.graphscope.ldbc.workload.proto.IrResult;
import com.alibaba.graphscope.ldbc.workload.BIRpcClient;
import com.alibaba.graphscope.ldbc.workload.BIRpcClient.ResultProcessor;

import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BIClient {
    private BIRpcClient rpcClient;

    public class ResultListener {
        private final StreamIterator<String> recordIterator;

        public ResultListener() {
            this.recordIterator = new StreamIterator<>();
        }

        public void onNext(String record) {
            try {
                this.recordIterator.putData(record);
            } catch (InterruptedException e) {
                onError(e);
            }
        }

        public void onCompleted() {
            try {
                this.recordIterator.finish();
            } catch (InterruptedException e) {
                onError(e);
            }
        }

        public void onError(Throwable t) {
            t = (t == null) ? new RuntimeException("Unknown error") : t;
            this.recordIterator.fail(t);
        }

        public StreamIterator<String> getIterator() {
            return this.recordIterator;
        }
    }

    public BIClient(List<String> endpoints) {
        List<RpcChannel> rpcChannels = new ArrayList<>();
        endpoints.forEach(
                k -> {
                    String[] host = k.split(":");
                    rpcChannels.add(new RpcChannel(host[0], Integer.valueOf(host[1])));
                });
        this.rpcClient = new BIRpcClient(rpcChannels);
    }

    public StreamIterator<String> submit(String jobName, Map<String, String> params) {
        ResultListener listener = new ResultListener();
//        List<BiClient.Argument> params_list = new ArrayList<>();
//        for (Map.Entry<String, String> entry : params.entrySet()) {
//            String key = entry.getKey();
//            String value = entry.getValue();
//            params_list.add(BiClient.Argument.newBuilder().setParamName(key).setValue(value).build());
//        }
        System.out.println(jobName);
//        BiClient.BIJobRequest jobRequest = BiClient.BIJobRequest.newBuilder()
//                .setJobName(jobName).addAllArguments(params_list)
//                .build();
//        jobRequest = jobRequest.toBuilder().build();
//        rpcClient.submit(
//                jobRequest,
//                new ResultProcessor() {
//                    @Override
//                    public void process(BiClient.BIJobResponse jobResponse) {
//                        try {
//                            listener.onNext(
//                                    jobResponse.getResp().toStringUtf8());
//                        } catch (Exception e) {
//                            throw new RuntimeException(e);
//                        }
//                    }
//
//                    @Override
//                    public void finish() {
//                        listener.onCompleted();
//                    }
//
//                    @Override
//                    public void error(Status status) {
//                        listener.onError(status.asException());
//                    }
//                },
//                100000);
        return listener.getIterator();
    }
}
