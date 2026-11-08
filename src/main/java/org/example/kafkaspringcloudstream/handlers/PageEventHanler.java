package org.example.kafkaspringcloudstream.handlers;

import org.example.kafkaspringcloudstream.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.function.Consumer;
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
}
