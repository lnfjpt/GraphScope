package com.alibaba.graphscope.ldbc.workload;

import com.alibaba.pegasus.RpcChannel;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.ResultProcessor;

import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.protobuf.ByteString;
import com.alibaba.graphscope.ldbc.workload.proto.IrResult;

import io.grpc.Status;

import java.util.ArrayList;
import java.util.List;

public class InsightClient {
  private RpcClient rpcClient;

  public class ResultListener {
    private final StreamIterator<IrResult.Record> recordIterator;

    public ResultListener() {
      this.recordIterator = new StreamIterator<>();
    }

    public void onNext(IrResult.Record record) {
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

    public StreamIterator<IrResult.Record> getIterator() {
      return this.recordIterator;
    }
  }

  public InsightClient(List<String> endpoints) {
    List<RpcChannel> rpcChannels = new ArrayList<>();
    endpoints.forEach(
        k -> {
          String[] host = k.split(":");
          rpcChannels.add(new RpcChannel(host[0], Integer.valueOf(host[1])));
        });
    this.rpcClient = new RpcClient(rpcChannels);
  }

  public StreamIterator<IrResult.Record> submit(byte[] physicalPlan, long jobId, String jobName, int workers) {
    ResultListener listener = new ResultListener();
    PegasusClient.JobRequest jobRequest = PegasusClient.JobRequest.newBuilder()
        .setPlan(ByteString.copyFrom(physicalPlan))
        .build();
    PegasusClient.JobConfig jobConfig = PegasusClient.JobConfig.newBuilder()
        .setJobId(jobId)
        .setJobName(jobName)
        .setWorkers(workers)
        .setAll(
            com.alibaba.pegasus.service.protocol.PegasusClient.Empty
                .newBuilder()
                .build())
        .build();
    jobRequest = jobRequest.toBuilder().setConf(jobConfig).build();
    rpcClient.submit(
        jobRequest,
        new ResultProcessor() {
          @Override
          public void process(PegasusClient.JobResponse jobResponse) {
            try {
              listener.onNext(
                  IrResult.Results.parseFrom(jobResponse.getResp()).getRecord());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void finish() {
            listener.onCompleted();
          }

          @Override
          public void error(Status status) {
            listener.onError(status.asException());
          }
        },
        100000);
    return listener.getIterator();
  }
}
