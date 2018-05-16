/*
 *  Copyright (c) 2014-2017 Kumuluz and/or its affiliates
 *  and other contributors as indicated by the @author tags and
 *  the contributor list.
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/MIT
 *
 *  The software is provided "AS IS", WITHOUT WARRANTY OF ANY KIND, express or
 *  implied, including but not limited to the warranties of merchantability,
 *  fitness for a particular purpose and noninfringement. in no event shall the
 *  authors or copyright holders be liable for any claim, damages or other
 *  liability, whether in an action of contract, tort or otherwise, arising from,
 *  out of or in connection with the software or the use or other dealings in the
 *  software. See the License for the specific language governing permissions and
 *  limitations under the License.
*/

package com.kumuluz.ee.samples.kafka.consumer;


import com.kumuluz.ee.streaming.common.annotations.StreamListener;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@ApplicationScoped
public class TestConsumer {


    private static final Logger log = Logger.getLogger(TestConsumer.class.getName());

    private List<String> messages = new ArrayList<>();

    private static final String DEFAULT_MONGO_URL = "mongodb://localhost:27017/local?serverSelectionTimeoutMS=2000";


    MongoCollection<Document> collection;
    MongoDatabase database;


    @PostConstruct
    private void init() {
        System.out.println("TEST1");

        MongoClient mongoClient = null;



        try {
            MongoClientURI mongoClientURI = new MongoClientURI(DEFAULT_MONGO_URL);
            mongoClient = new MongoClient(mongoClientURI);

            if (dbExists(mongoClient, mongoClientURI.getDatabase())) {
                database = mongoClient.getDatabase("mydb");


            } else {
                log.severe("Mongo database not found.");

            }
        } catch (Exception exception) {
            log.log(Level.SEVERE, "An exception occurred when trying to establish connection to Mongo.", exception);

        } finally {
            if (mongoClient != null) {
                //mongoClient.close();
            }

        }
    }

    private Boolean dbExists(MongoClient mongoClient, String databaseName) {

        if (mongoClient != null && databaseName != null) {

            for (String s : mongoClient.listDatabaseNames()) {
                if (s.equals(databaseName))
                    return true;
            }
        }

        return false;
    }

    @StreamListener(topics = {"test"})
    public void onMessage(ConsumerRecord<String, String> record) {

        Document doc = new Document("time", new Date(record.timestamp()))
                .append("offset", record.offset())
                .append("key", record.key())
                .append("value", record.value());

        collection = database.getCollection("test");
        collection.insertOne(doc);

        log.info(String.format("Consumed message: offset = %d, key = %s, value = %s%n", record.offset(), record.key()
                , record.value()));

        messages.add(record.value());
    }


    public List<String> getLastFiveMessages() {
        if (messages.size() < 5)
            return messages;
        return messages.subList(messages.size() - 5, messages.size());
    }


}
