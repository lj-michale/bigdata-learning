package com.luoj.plusar;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * @author lj.michale
 * @description
 * @date 2021-05-25
 */
public class PlusarTest {

    public static void main(String[] args)  {

        final ClientConfigurationData configurationData = new ClientConfigurationData();
        configurationData.setServiceUrl("pulsar://127.0.0.1:6650");

        //Your Pulsar Token
        final AuthenticationToken token =
                new AuthenticationToken(
                        "eyJxxxxxxxxxxx.eyxxxxxxxxxxxxx.xxxxxxxxxxx");
        configurationData.setAuthentication(token);

        try (final PulsarMetadataReader reader =
                     new PulsarMetadataReader("http://127.0.0.1:8443",
                             configurationData,
                             "",
                             new HashMap(),
                             -1,
                             -1)) {
            //获取namespaces
            final List<String> namespaces = reader.listNamespaces();
            System.out.println("namespaces: " + namespaces.toString());

            for (final String namespace : namespaces) {
                //获取Topics
                final List<String> topics = reader.getTopics(namespace);
                System.out.println("topic: " + topics.toString());

                for (String topic : topics) {
                    //获取字段SchemaInfo
                    final SchemaInfo schemaInfo = reader.getPulsarSchema(topic);
                    final String name = schemaInfo.getName();
                    System.out.println("SchemaName:" + name); //topicName
                    final SchemaType type = schemaInfo.getType();
                    System.out.println("SchemaType:" + type.toString());// "JSON"...
                    final Map<String, String> properties = schemaInfo.getProperties();
                    System.out.println(properties);
                    final String schemaDefinition = schemaInfo.getSchemaDefinition();
                    System.out.println(schemaDefinition); // Field info.
                }
            }

        } catch (IOException | PulsarAdminException e) {
            e.printStackTrace();
        }


    }


}