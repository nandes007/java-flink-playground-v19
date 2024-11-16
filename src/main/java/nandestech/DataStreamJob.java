/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nandestech;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("processed_data")
				.setGroupId("flink")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> inputStream = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		inputStream.print();

		DataStream<Tuple2<String, String>> parsedStream = inputStream
				.map(line -> {
					String[] parts = line.split(":");
					String key = parts[0];
					String value = parts.length > 1 ? parts[1] : "";
					return new Tuple2<>(key, value);
				})
				.returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));

//		KafkaSink<Tuple2<String, String>> sink = KafkaSink.<Tuple2<String, String>>builder()
//				.setBootstrapServers("localhost:9092")
//				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
//						.setTopic("processed_data")
//						.setValueSerializationSchema(new TupleToStringSerializationSchema())
//						.build())
//				.setKafkaProducerConfig(new Properties() {{
//					put(ProducerConfig.ACKS_CONFIG, "all");
//					put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//					put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//				}})
//				.build();
//
//		parsedStream.sinkTo(sink);

		parsedStream.addSink(JdbcSink.sink(
				"INSERT INTO public.test_data (key_column, value_column) VALUES (?, ?)",
				new JdbcStatementBuilder<Tuple2<String, String>>() {
					@Override
					public void accept(PreparedStatement ps, Tuple2<String, String> record) throws SQLException {
						ps.setString(1, record.f0); // Set the key
						ps.setString(2, record.f1); // Set the value
					}
				},
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl("jdbc:postgresql://172.27.216.159:5432/core_local") // Replace with your database URL
						.withDriverName("org.postgresql.Driver")
						.withUsername("nandes") // Replace with your PostgreSQL username
						.withPassword("postgre") // Replace with your PostgreSQL password
						.build()
		));

		env.execute("Kafka to PostgreSQL Data Pipeline");

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
//		env.execute("Flink Java API Skeleton");
	}

	public static class TupleToStringSerializationSchema implements SerializationSchema<Tuple2<String, String>> {
		@Override
		public byte[] serialize(Tuple2<String, String> element) {
			String serializedValue = element.f0 + ":" + element.f1;
			return serializedValue.getBytes();
		}
	}
}
