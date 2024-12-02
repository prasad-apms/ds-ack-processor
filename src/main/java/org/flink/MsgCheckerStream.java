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



/**
 * 
The onTimer method in Flink's KeyedProcessFunction is called when a timer that was registered earlier using context.timerService().registerProcessingTimeTimer(...) expires. Here's how it works in your code:

Timer Registration:
    $ In the processElement method, you register a timer for 5 minutes (300,000 milliseconds) from the current processing time:

    $ java Copy code long fiveMinutesInMillis = 5 * 60 * 1000; // 5 minutes in milliseconds
    $ context.timerService().registerProcessingTimeTimer(currentTime + fiveMinutesInMillis);
    $ This sets up a timer that will trigger onTimer after 5 minutes.

When onTimer Is Called:
    $ Trigger Time: The onTimer method is called exactly at the time you specified during registration (currentTime + fiveMinutesInMillis).
    $ Key-Specific: Each timer is bound to the key currently being processed. Flink ensures that when onTimer is invoked, the function is operating on the correct key context (ctx.getCurrentKey()).
    $ Flow of Execution:

Process Incoming Messages:

    $ When a new Message arrives, processElement checks if it’s the first time seeing the req_id using the state lastSeenTimeState.
    $ If it's a new req_id, a timer is registered to trigger 5 minutes later.

Timer Expiration:

   $  After 5 minutes, the timer expires and calls onTimer for the associated req_id.

State Cleanup in onTimer:
    $ The onTimer method checks if the current state for the req_id has not been updated (i.e., currentTime - lastSeenTimeState.value() >= 5 * 60 * 1000).
    $ If the condition is true, it clears the state for that req_id and outputs a new message indicating the expiration.

Important Notes:
    $ Timers Are Independent per Key: Each key has its own timer. The timers are maintained separately for every key processed in the keyed stream.

Timer Types: 
    $ The registerProcessingTimeTimer method schedules a timer based on processing time, which is the system's wall-clock time.

Scaling: 
    $ If your job is scaled across multiple task slots, timers for different keys are managed by their respective task slots.

Example Scenario:
    $ A message with req_id=123 arrives at 12:00:00 PM.

    $ lastSeenTimeState is updated with 12:00:00 PM.
    $ A timer is registered for 12:05:00 PM.
    $ If no other messages with req_id=123 arrive by 12:05:00 PM, the onTimer method is called for req_id=123.

During onTimer:

    $ If lastSeenTimeState is still the same or older than 5 minutes, it clears the state and outputs a message indicating expiration.
    $ If another message with req_id=123 arrives before 12:05:00 PM:

The timer is re-registered, extending the expiration by another 5 minutes.

 */