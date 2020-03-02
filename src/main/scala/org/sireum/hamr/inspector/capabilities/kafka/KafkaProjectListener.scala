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
    // toLongOpt.get works around toLong bug
    producer.send(src.toInt, dst.toInt, time.toLongOpt.get, InspectorHAMRLauncher.serializer(data))
  }

}
