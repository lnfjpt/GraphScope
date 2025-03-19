package com.alibaba.graphscope.ldbc.workload.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.51.1)",
    comments = "Source: job_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class JobServiceGrpc {

  private JobServiceGrpc() {}

  public static final String SERVICE_NAME = "protocol.JobService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getAddLibraryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AddLibrary",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getAddLibraryMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getAddLibraryMethod;
    if ((getAddLibraryMethod = JobServiceGrpc.getAddLibraryMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getAddLibraryMethod = JobServiceGrpc.getAddLibraryMethod) == null) {
          JobServiceGrpc.getAddLibraryMethod = getAddLibraryMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AddLibrary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("AddLibrary"))
              .build();
        }
      }
    }
    return getAddLibraryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getRemoveLibraryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemoveLibrary",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getRemoveLibraryMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getRemoveLibraryMethod;
    if ((getRemoveLibraryMethod = JobServiceGrpc.getRemoveLibraryMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getRemoveLibraryMethod = JobServiceGrpc.getRemoveLibraryMethod) == null) {
          JobServiceGrpc.getRemoveLibraryMethod = getRemoveLibraryMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RemoveLibrary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("RemoveLibrary"))
              .build();
        }
      }
    }
    return getRemoveLibraryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getCancelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Cancel",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getCancelMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> getCancelMethod;
    if ((getCancelMethod = JobServiceGrpc.getCancelMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getCancelMethod = JobServiceGrpc.getCancelMethod) == null) {
          JobServiceGrpc.getCancelMethod = getCancelMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Cancel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("Cancel"))
              .build();
        }
      }
    }
    return getCancelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse> getSubmitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Submit",
      requestType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest.class,
      responseType = com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest,
      com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse> getSubmitMethod() {
    io.grpc.MethodDescriptor<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse> getSubmitMethod;
    if ((getSubmitMethod = JobServiceGrpc.getSubmitMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getSubmitMethod = JobServiceGrpc.getSubmitMethod) == null) {
          JobServiceGrpc.getSubmitMethod = getSubmitMethod =
              io.grpc.MethodDescriptor.<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest, com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Submit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("Submit"))
              .build();
        }
      }
    }
    return getSubmitMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static JobServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobServiceStub>() {
        @java.lang.Override
        public JobServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobServiceStub(channel, callOptions);
        }
      };
    return JobServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static JobServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobServiceBlockingStub>() {
        @java.lang.Override
        public JobServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobServiceBlockingStub(channel, callOptions);
        }
      };
    return JobServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static JobServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobServiceFutureStub>() {
        @java.lang.Override
        public JobServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobServiceFutureStub(channel, callOptions);
        }
      };
    return JobServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class JobServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void addLibrary(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAddLibraryMethod(), responseObserver);
    }

    /**
     */
    public void removeLibrary(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRemoveLibraryMethod(), responseObserver);
    }

    /**
     */
    public void cancel(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCancelMethod(), responseObserver);
    }

    /**
     */
    public void submit(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAddLibraryMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource,
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>(
                  this, METHODID_ADD_LIBRARY)))
          .addMethod(
            getRemoveLibraryMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name,
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>(
                  this, METHODID_REMOVE_LIBRARY)))
          .addMethod(
            getCancelMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest,
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>(
                  this, METHODID_CANCEL)))
          .addMethod(
            getSubmitMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest,
                com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse>(
                  this, METHODID_SUBMIT)))
          .build();
    }
  }

  /**
   */
  public static final class JobServiceStub extends io.grpc.stub.AbstractAsyncStub<JobServiceStub> {
    private JobServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobServiceStub(channel, callOptions);
    }

    /**
     */
    public void addLibrary(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAddLibraryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void removeLibrary(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRemoveLibraryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cancel(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void submit(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getSubmitMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class JobServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<JobServiceBlockingStub> {
    private JobServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty addLibrary(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAddLibraryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty removeLibrary(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRemoveLibraryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty cancel(com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCancelMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse> submit(
        com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getSubmitMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class JobServiceFutureStub extends io.grpc.stub.AbstractFutureStub<JobServiceFutureStub> {
    private JobServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> addLibrary(
        com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAddLibraryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> removeLibrary(
        com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRemoveLibraryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty> cancel(
        com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ADD_LIBRARY = 0;
  private static final int METHODID_REMOVE_LIBRARY = 1;
  private static final int METHODID_CANCEL = 2;
  private static final int METHODID_SUBMIT = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final JobServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(JobServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ADD_LIBRARY:
          serviceImpl.addLibrary((com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.BinaryResource) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>) responseObserver);
          break;
        case METHODID_REMOVE_LIBRARY:
          serviceImpl.removeLibrary((com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Name) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>) responseObserver);
          break;
        case METHODID_CANCEL:
          serviceImpl.cancel((com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.CancelRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.Empty>) responseObserver);
          break;
        case METHODID_SUBMIT:
          serviceImpl.submit((com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.JobResponse>) responseObserver);
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

  private static abstract class JobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    JobServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.alibaba.graphscope.ldbc.workload.proto.PegasusClient.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("JobService");
    }
  }

  private static final class JobServiceFileDescriptorSupplier
      extends JobServiceBaseDescriptorSupplier {
    JobServiceFileDescriptorSupplier() {}
  }

  private static final class JobServiceMethodDescriptorSupplier
      extends JobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    JobServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (JobServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new JobServiceFileDescriptorSupplier())
              .addMethod(getAddLibraryMethod())
              .addMethod(getRemoveLibraryMethod())
              .addMethod(getCancelMethod())
              .addMethod(getSubmitMethod())
              .build();
        }
      }
    }
    return result;
  }
}
