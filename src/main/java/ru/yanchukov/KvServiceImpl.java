package ru.yanchukov;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.tarantool.client.box.TarantoolBoxClient;
import ru.yanchukov.grpc.*;

import java.util.Arrays;
import java.util.List;

public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {
    private static final int BATCH_SIZE = 10000;
    private final TarantoolBoxClient client;

    public KvServiceImpl(TarantoolBoxClient client) {
        this.client = client;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;
            client.eval("return box.space.KV:replace({...,})",
                    Arrays.asList(request.getKey(), value)
            ).join();

            responseObserver.onNext(PutResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            List<?> result = client.eval(
                    "local tuple = box.space.KV:get(...) " +
                            "if tuple == nil then return nil end " +
                            "return tuple[1], tuple[2]",
                    Arrays.asList(request.getKey()),
                    Object.class
            ).join().get();

            GetResponse.Builder builder = GetResponse.newBuilder();
            if (result != null && !result.isEmpty() && result.get(0) != null) {
                if (result.size() > 1 && result.get(1) instanceof byte[] value) {
                    builder.setValue(ByteString.copyFrom(value));
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            client.eval(
                    "return box.space.KV:delete(...)",
                    Arrays.asList(request.getKey())
            ).join();

            responseObserver.onNext(DeleteResponse.newBuilder().build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        try {
            String currentKey = request.getKeySince();
            String keyTo = request.getKeyTo();
            boolean firstBatch = true;

            String script =
                    "local from, to, size, iter = ... " +
                    "local result = {} " +
                    "local it = box.index[iter] " +
                    "for _, tuple in box.space.KV.index.primary:pairs(from, {iterator = it}) do " +
                    "    if tuple[1] > to then break end " +
                    "    table.insert(result, {tuple[1], tuple[2]}) " +
                    "    if #result >= size then break end " +
                    "end " +
                    "return unpack(result)";

            while (true) {
                List<?> batch = client.eval(
                        script,
                        Arrays.asList(currentKey, keyTo, BATCH_SIZE, firstBatch ? "GE" : "GT"),
                        Object.class
                ).join().get();

                if (batch == null || batch.isEmpty()) break;

                int count = 0;
                for (Object item : batch) {
                    if (!(item instanceof List<?> pair)) continue;
                    if (pair.isEmpty()) continue;

                    String key = pair.getFirst().toString();
                    if (key.compareTo(keyTo) > 0) {
                        responseObserver.onCompleted();
                        return;
                    }
                    RangeResponse.Builder builder = RangeResponse.newBuilder().setKey(key);

                    byte[] value = pair.size() > 1 && pair.get(1) instanceof byte[] ? (byte[])pair.get(1) : null;
                    if (value != null) builder.setValue(ByteString.copyFrom(value));

                    responseObserver.onNext(builder.build());
                    currentKey = key;
                    count++;
                }
                if(count < BATCH_SIZE) break;
                firstBatch = false;
            }
            responseObserver.onCompleted();
        } catch(Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            var result = client.eval(
                    "return box.space.KV:count()",
                    Arrays.asList(),
                    Object.class
            ).join().get();

            long total = ((Number) result.getFirst()).longValue();

            responseObserver.onNext(CountResponse.newBuilder().setCount(total).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}
