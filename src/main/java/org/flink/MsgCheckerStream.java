package org.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgCheckerStream extends KeyedProcessFunction<String, Message, Message> {
    private static final Logger LOG = LoggerFactory.getLogger(MsgCheckerStream.class);
    private transient ValueState<Long> lastSeenTimeState;

    // Initialize the state to track the last seen timestamp for each req_id
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>(
                "lastSeenTimeState", Long.class);
        lastSeenTimeState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(Message message, Context context,Collector<Message> out) throws Exception {
        String reqId = message.getReqId();
        long currentTime = System.currentTimeMillis();

        // Check if this req_id has been seen before
        Long lastSeenTime = lastSeenTimeState.value();

        if (lastSeenTime != null) {
            // If the req_id has already been seen, skip processing
            LOG.info("Skipping req_id: {} (already processed)", reqId);
            lastSeenTimeState.update(currentTime);
            return;
        }

        // First time seeing this req_id; update the state
        lastSeenTimeState.update(currentTime);

        // Register a timer that will expire in 24 hours (1 day) after the event is seen
        // long oneDayInMillis = 24 * 60 * 60 * 1000; // 24 hours in milliseconds
        // context.timerService().registerProcessingTimeTimer(currentTime + oneDayInMillis);

        // Register a timer that will expire in 5 minutes
        long fiveMinutesInMillis = 5 * 60 * 1000; // 5 minutes in milliseconds
        context.timerService().registerProcessingTimeTimer(currentTime + fiveMinutesInMillis);


        LOG.info("Processing req_id: {}", reqId);
        out.collect(message);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Message> out) throws Exception {
        // Timer has triggered after 24 hours. Clean up the state if the req_id was not seen again.
        Message msg = new Message();   //24 * 60 * 60 * 1000
        long currentTime = System.currentTimeMillis();
        if (lastSeenTimeState.value() != null && currentTime - lastSeenTimeState.value() >=  5 * 60 * 1000) {
            // Expire the state for the req_id (clear it from RocksDB)
            lastSeenTimeState.clear();
            LOG.info("Expired state for req_id: " + ctx.getCurrentKey());
            msg.setReqId(ctx.getCurrentKey());
            out.collect(msg);
        }
    }
}
