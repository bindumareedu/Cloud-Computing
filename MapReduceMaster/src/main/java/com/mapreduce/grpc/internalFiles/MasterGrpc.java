package com.mapreduce.grpc.internalFiles;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.15.0)",
    comments = "Source: mapreduce.proto")
public final class MasterGrpc {

  private MasterGrpc() {}

  public static final String SERVICE_NAME = "Master";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.mapreduce.grpc.internalFiles.Mapreduce.Request,
      com.mapreduce.grpc.internalFiles.Mapreduce.Response> getMasterNodeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MasterNode",
      requestType = com.mapreduce.grpc.internalFiles.Mapreduce.Request.class,
      responseType = com.mapreduce.grpc.internalFiles.Mapreduce.Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mapreduce.grpc.internalFiles.Mapreduce.Request,
      com.mapreduce.grpc.internalFiles.Mapreduce.Response> getMasterNodeMethod() {
    io.grpc.MethodDescriptor<com.mapreduce.grpc.internalFiles.Mapreduce.Request, com.mapreduce.grpc.internalFiles.Mapreduce.Response> getMasterNodeMethod;
    if ((getMasterNodeMethod = MasterGrpc.getMasterNodeMethod) == null) {
      synchronized (MasterGrpc.class) {
        if ((getMasterNodeMethod = MasterGrpc.getMasterNodeMethod) == null) {
          MasterGrpc.getMasterNodeMethod = getMasterNodeMethod = 
              io.grpc.MethodDescriptor.<com.mapreduce.grpc.internalFiles.Mapreduce.Request, com.mapreduce.grpc.internalFiles.Mapreduce.Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "Master", "MasterNode"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mapreduce.grpc.internalFiles.Mapreduce.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mapreduce.grpc.internalFiles.Mapreduce.Response.getDefaultInstance()))
                  .setSchemaDescriptor(new MasterMethodDescriptorSupplier("MasterNode"))
                  .build();
          }
        }
     }
     return getMasterNodeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MasterStub newStub(io.grpc.Channel channel) {
    return new MasterStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MasterBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MasterBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MasterFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MasterFutureStub(channel);
  }

  /**
   */
  public static abstract class MasterImplBase implements io.grpc.BindableService {

    /**
     */
    public void masterNode(com.mapreduce.grpc.internalFiles.Mapreduce.Request request,
        io.grpc.stub.StreamObserver<com.mapreduce.grpc.internalFiles.Mapreduce.Response> responseObserver) {
      asyncUnimplementedUnaryCall(getMasterNodeMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getMasterNodeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.mapreduce.grpc.internalFiles.Mapreduce.Request,
                com.mapreduce.grpc.internalFiles.Mapreduce.Response>(
                  this, METHODID_MASTER_NODE)))
          .build();
    }
  }

  /**
   */
  public static final class MasterStub extends io.grpc.stub.AbstractStub<MasterStub> {
    private MasterStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MasterStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MasterStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MasterStub(channel, callOptions);
    }

    /**
     */
    public void masterNode(com.mapreduce.grpc.internalFiles.Mapreduce.Request request,
        io.grpc.stub.StreamObserver<com.mapreduce.grpc.internalFiles.Mapreduce.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMasterNodeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MasterBlockingStub extends io.grpc.stub.AbstractStub<MasterBlockingStub> {
    private MasterBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MasterBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MasterBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MasterBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.mapreduce.grpc.internalFiles.Mapreduce.Response masterNode(com.mapreduce.grpc.internalFiles.Mapreduce.Request request) {
      return blockingUnaryCall(
          getChannel(), getMasterNodeMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MasterFutureStub extends io.grpc.stub.AbstractStub<MasterFutureStub> {
    private MasterFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MasterFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MasterFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MasterFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mapreduce.grpc.internalFiles.Mapreduce.Response> masterNode(
        com.mapreduce.grpc.internalFiles.Mapreduce.Request request) {
      return futureUnaryCall(
          getChannel().newCall(getMasterNodeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_MASTER_NODE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MasterImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MasterImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_MASTER_NODE:
          serviceImpl.masterNode((com.mapreduce.grpc.internalFiles.Mapreduce.Request) request,
              (io.grpc.stub.StreamObserver<com.mapreduce.grpc.internalFiles.Mapreduce.Response>) responseObserver);
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

  private static abstract class MasterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MasterBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.mapreduce.grpc.internalFiles.Mapreduce.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Master");
    }
  }

  private static final class MasterFileDescriptorSupplier
      extends MasterBaseDescriptorSupplier {
    MasterFileDescriptorSupplier() {}
  }

  private static final class MasterMethodDescriptorSupplier
      extends MasterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MasterMethodDescriptorSupplier(String methodName) {
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
      synchronized (MasterGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MasterFileDescriptorSupplier())
              .addMethod(getMasterNodeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
