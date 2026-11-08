package org.example.kafkaspringcloudstream.controllers;

import org.example.kafkaspringcloudstream.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class PageEvenetController {
    @Autowired
    private StreamBridge streamBridge;
    @GetMapping("/publish")
    public PageEvent publish(String name , String topic){
        PageEvent event = new PageEvent(name , Math.random() > 0.5 ? "user1" : "user2" , new Date() , Math.round(Math.random() * 10000));
        streamBridge.send(topic , event);
        return event;
    }
}
