package org.example.kafkaspringcloudstream.handlers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.example.kafkaspringcloudstream.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHanler {
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input) -> {
            System.out.println("************************************");
            System.out.println("Received Event : " + input.toString());
            System.out.println("************************************");

        };
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return () -> new PageEvent
                (Math.random()>0.5?"P1":"P2",
                        Math.random()>0.5?"user1":"user2" ,
                        new Date() ,
                        Math.round(Math.random() * 10000));
    }
    @Bean
    public Function<KStream<String ,PageEvent> , KStream<String,Long>> KStreamFunction(){
        return (input) ->
             input.filter((k , v) -> v.duration() > 100)
                    .map((k,v) -> new KeyValue<>(v.name() , v.duration()))
                     .groupByKey(Grouped.with(Serdes.String() , Serdes.Long()))
                     .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                     .count(Materialized.as("count-store"))
                     .toStream()
                     .map((k,v)->new KeyValue<>(k.key(),v));
        };

}

