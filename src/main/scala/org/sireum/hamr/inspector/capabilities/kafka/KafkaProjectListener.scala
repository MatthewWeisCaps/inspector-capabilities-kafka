package org.sireum.hamr.inspector.capabilities.kafka

import java.util.Properties

import art.Art.{PortId, Time}
import art.DataContent
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringSerializer}
import org.sireum.hamr.inspector.capabilities.{InspectorHAMRLauncher, ProjectListener}
import org.sireum.hamr.inspector.common.Injection

class KafkaProjectListener extends ProjectListener /*with Serializer[SerializeInfo] with Deserializer[Injection]*/ {

  val producer = new KafkaHamrProducer("topic-1")

  override def start(time: Time): Unit = {

  }

  override def stop(time: Time): Unit = {

  }

  override def output(src: PortId, dst: PortId, data: DataContent, time: Time): Unit = {
//    producer.send(new ProducerRecord[String, String](topic, srcPartition, time.toLong, Integer.toString(src.toInt)))

    producer.send(src.toInt, dst.toInt, time.toLong, InspectorHAMRLauncher.serializer(data))
  }

//  override def serialize(topic: String, data: SerializeInfo): Array[Byte] = {
//    val serializedString =
//      java.lang.Integer.toString(data.src.toInt) + ","
//      java.lang.Integer.toString(data.dst.toInt) + ","
////      java.lang.Long.toString(data.time.toLong) + ","
//      InspectorHAMRLauncher.serializer(data.data)
//
//    return serializedString.getBytes()
//  }
//
//  override def deserialize(topic: String, data: Array[Byte]): Injection = {
//    throw new NotImplementedError("todo")
//  }

//  // todo make capabilities api accept properties, this is relevant for many impls w/o spring
//  private def createKafkaProducer(): KafkaProducer[String, String] = {
//    val properties = new Properties();
//    properties.put("bootstrap.servers", "localhost:9092")
//
//    properties.put("acks", "all") // if we want to acknowledge producer requests
//    properties.put("retries", 0)
//
//    properties.put("batch.size", 16384)
//    properties.put("linger.ms", 1)
//    properties.put("buffer.memory", 33554432)
//
//    properties.put("key.serializer", getClass.toString)
//    properties.put("value.serializer", getClass.toString)
//
//    return new KafkaProducer[String, String](properties)
//  }



}
