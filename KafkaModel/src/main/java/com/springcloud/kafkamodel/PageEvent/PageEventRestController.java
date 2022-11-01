package com.springcloud.kafkamodel.PageEvent;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;


import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


@RestController
@RequestMapping(path = "/event")
public class PageEventRestController {
    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private InteractiveQueryService interactiveQueryService; 
    @GetMapping(path = "/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name){
        PageEvent pageEvent = new PageEvent(name, "ie017", new Date(), new Random().nextInt(99));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
       
    }
    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics(){ /*Flux est un flux qui continue un objet Map qui doit etre converter en format json*/
        return Flux.interval(Duration.ofSeconds(1))./*Pour générer un flux d'enregistrement chaque seconde*/
                map(sequence->{
                    /*Pour chaque seconde je crée un objet de type HashMap*/
            Map<String, Long> stringLongMap = new HashMap<>();
            ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
            Instant now = Instant.now();
            Instant from = now.minusMillis(5000);
            KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now); 
            //WindowStoreIterator<Long> fetch = stats.fetch(name, from, now);
            while(fetchAll.hasNext()){
                KeyValue<Windowed<String>, Long> next = fetchAll.next();
            }
            return stringLongMap;
        }).share();
    }
}
