package com.alibaba.graphscope.ldbc.workload;

import com.alibaba.pegasus.RpcChannel;

import com.alibaba.graphscope.ldbc.workload.proto.BIJobServiceGrpc;
import com.alibaba.graphscope.ldbc.workload.proto.BIJobServiceGrpc.BIJobServiceStub;
import com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest;
import com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BIRpcClient {
  public interface ResultProcessor {
    void process(BIJobResponse response);

    void finish();

    void error(Status status);
  }

  private static final Logger logger = LoggerFactory.getLogger(BIRpcClient.class);
  private final List<RpcChannel> channels;
  private final List<BIJobServiceStub> serviceStubs;

  public BIRpcClient(List<RpcChannel> channels) {
    this.channels = Objects.requireNonNull(channels);
    this.serviceStubs =
      channels.stream()
        .map(k -> BIJobServiceGrpc.newStub(k.getChannel()))
        .collect(Collectors.toList());
  }

  public void submit(BIJobRequest jobRequest, ResultProcessor processor, long rpcTimeoutMS) {
    AtomicInteger counter = new AtomicInteger(this.channels.size());
    AtomicBoolean finished = new AtomicBoolean(false);
    serviceStubs.forEach(
      asyncStub -> {
        asyncStub
          .withDeadlineAfter(rpcTimeoutMS, TimeUnit.MILLISECONDS)
          .submit(
            jobRequest,
            new JobResponseObserver(processor, finished, counter));
      });
  }

  public void shutdown() throws InterruptedException {
    for (RpcChannel rpcChannel : channels) {
      rpcChannel.shutdown();
    }
  }

    private static class JobResponseObserver implements StreamObserver<BIJobResponse> {
      private final ResultProcessor processor;
      private final AtomicBoolean finished;
      private final AtomicInteger counter;

      public JobResponseObserver(
              ResultProcessor processor, AtomicBoolean finished, AtomicInteger counter) {
        this.processor = processor;
        this.finished = finished;
        this.counter = counter;
      }

      @Override
      public void onNext(BIJobResponse jobResponse) {
        if (finished.get()) {
          return;
        }
        processor.process(jobResponse);
      }

      @Override
      public void onError(Throwable throwable) {
        if (finished.getAndSet(true)) {
          return;
        }
        Status status = Status.fromThrowable(throwable);
        logger.error("get job response error: {}", status);
        processor.error(status);
      }

      @Override
      public void onCompleted() {
        logger.info("finish get job response from one server");
        if (counter.decrementAndGet() == 0) {
          logger.info("finish get job response from all servers");
          processor.finish();
        }
      }
    }
}