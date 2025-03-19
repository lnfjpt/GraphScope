package com.alibaba.graphscope.ldbc.workload.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.51.1)",
    comments = "Source: job_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BIJobServiceGrpc {

  private BIJobServiceGrpc() {}

  public static final String SERVICE_NAME = "protocol.BIJobService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse> getSubmitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Submit",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse> getSubmitMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse> getSubmitMethod;
    if ((getSubmitMethod = BIJobServiceGrpc.getSubmitMethod) == null) {
      synchronized (BIJobServiceGrpc.class) {
        if ((getSubmitMethod = BIJobServiceGrpc.getSubmitMethod) == null) {
          BIJobServiceGrpc.getSubmitMethod = getSubmitMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Submit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BIJobServiceMethodDescriptorSupplier("Submit"))
              .build();
        }
      }
    }
    return getSubmitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> getSubmitBatchInsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubmitBatchInsert",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> getSubmitBatchInsertMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> getSubmitBatchInsertMethod;
    if ((getSubmitBatchInsertMethod = BIJobServiceGrpc.getSubmitBatchInsertMethod) == null) {
      synchronized (BIJobServiceGrpc.class) {
        if ((getSubmitBatchInsertMethod = BIJobServiceGrpc.getSubmitBatchInsertMethod) == null) {
          BIJobServiceGrpc.getSubmitBatchInsertMethod = getSubmitBatchInsertMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubmitBatchInsert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BIJobServiceMethodDescriptorSupplier("SubmitBatchInsert"))
              .build();
        }
      }
    }
    return getSubmitBatchInsertMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> getSubmitBatchDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubmitBatchDelete",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> getSubmitBatchDeleteMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> getSubmitBatchDeleteMethod;
    if ((getSubmitBatchDeleteMethod = BIJobServiceGrpc.getSubmitBatchDeleteMethod) == null) {
      synchronized (BIJobServiceGrpc.class) {
        if ((getSubmitBatchDeleteMethod = BIJobServiceGrpc.getSubmitBatchDeleteMethod) == null) {
          BIJobServiceGrpc.getSubmitBatchDeleteMethod = getSubmitBatchDeleteMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubmitBatchDelete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BIJobServiceMethodDescriptorSupplier("SubmitBatchDelete"))
              .build();
        }
      }
    }
    return getSubmitBatchDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse> getSubmitPrecomputeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubmitPrecompute",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest,
      com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse> getSubmitPrecomputeMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse> getSubmitPrecomputeMethod;
    if ((getSubmitPrecomputeMethod = BIJobServiceGrpc.getSubmitPrecomputeMethod) == null) {
      synchronized (BIJobServiceGrpc.class) {
        if ((getSubmitPrecomputeMethod = BIJobServiceGrpc.getSubmitPrecomputeMethod) == null) {
          BIJobServiceGrpc.getSubmitPrecomputeMethod = getSubmitPrecomputeMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest, com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubmitPrecompute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BIJobServiceMethodDescriptorSupplier("SubmitPrecompute"))
              .build();
        }
      }
    }
    return getSubmitPrecomputeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BIJobServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BIJobServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BIJobServiceStub>() {
        @java.lang.Override
        public BIJobServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BIJobServiceStub(channel, callOptions);
        }
      };
    return BIJobServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BIJobServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BIJobServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BIJobServiceBlockingStub>() {
        @java.lang.Override
        public BIJobServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BIJobServiceBlockingStub(channel, callOptions);
        }
      };
    return BIJobServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BIJobServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BIJobServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BIJobServiceFutureStub>() {
        @java.lang.Override
        public BIJobServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BIJobServiceFutureStub(channel, callOptions);
        }
      };
    return BIJobServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class BIJobServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void submit(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitMethod(), responseObserver);
    }

    /**
     */
    public void submitBatchInsert(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitBatchInsertMethod(), responseObserver);
    }

    /**
     */
    public void submitBatchDelete(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitBatchDeleteMethod(), responseObserver);
    }

    /**
     */
    public void submitPrecompute(com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitPrecomputeMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSubmitMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest,
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse>(
                  this, METHODID_SUBMIT)))
          .addMethod(
            getSubmitBatchInsertMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest,
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse>(
                  this, METHODID_SUBMIT_BATCH_INSERT)))
          .addMethod(
            getSubmitBatchDeleteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest,
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse>(
                  this, METHODID_SUBMIT_BATCH_DELETE)))
          .addMethod(
            getSubmitPrecomputeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest,
                com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse>(
                  this, METHODID_SUBMIT_PRECOMPUTE)))
          .build();
    }
  }

  /**
   */
  public static final class BIJobServiceStub extends io.grpc.stub.AbstractAsyncStub<BIJobServiceStub> {
    private BIJobServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BIJobServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BIJobServiceStub(channel, callOptions);
    }

    /**
     */
    public void submit(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getSubmitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void submitBatchInsert(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitBatchInsertMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void submitBatchDelete(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitBatchDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void submitPrecompute(com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitPrecomputeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class BIJobServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<BIJobServiceBlockingStub> {
    private BIJobServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BIJobServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BIJobServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse> submit(
        com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getSubmitMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse submitBatchInsert(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitBatchInsertMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse submitBatchDelete(com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitBatchDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse submitPrecompute(com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitPrecomputeMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BIJobServiceFutureStub extends io.grpc.stub.AbstractFutureStub<BIJobServiceFutureStub> {
    private BIJobServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BIJobServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BIJobServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> submitBatchInsert(
        com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitBatchInsertMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse> submitBatchDelete(
        com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitBatchDeleteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse> submitPrecompute(
        com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitPrecomputeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SUBMIT = 0;
  private static final int METHODID_SUBMIT_BATCH_INSERT = 1;
  private static final int METHODID_SUBMIT_BATCH_DELETE = 2;
  private static final int METHODID_SUBMIT_PRECOMPUTE = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BIJobServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BIJobServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SUBMIT:
          serviceImpl.submit((com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BIJobResponse>) responseObserver);
          break;
        case METHODID_SUBMIT_BATCH_INSERT:
          serviceImpl.submitBatchInsert((com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse>) responseObserver);
          break;
        case METHODID_SUBMIT_BATCH_DELETE:
          serviceImpl.submitBatchDelete((com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.BatchUpdateResponse>) responseObserver);
          break;
        case METHODID_SUBMIT_PRECOMPUTE:
          serviceImpl.submitPrecompute((com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.BiClient.PrecomputeResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BIJobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BIJobServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.alibaba.graphscope.ldbc.workload.proto.BiClient.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BIJobService");
    }
  }

  private static final class BIJobServiceFileDescriptorSupplier
      extends BIJobServiceBaseDescriptorSupplier {
    BIJobServiceFileDescriptorSupplier() {}
  }

  private static final class BIJobServiceMethodDescriptorSupplier
      extends BIJobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BIJobServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (BIJobServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BIJobServiceFileDescriptorSupplier())
              .addMethod(getSubmitMethod())
              .addMethod(getSubmitBatchInsertMethod())
              .addMethod(getSubmitBatchDeleteMethod())
              .addMethod(getSubmitPrecomputeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
