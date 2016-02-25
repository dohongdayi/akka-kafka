package io.github.junheng.akka.kafka.protocol

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import akka.serialization.SerializerWithStringManifest
import io.github.junheng.akka.kafka.protocol.KGroupProtocol.Pulled
import io.github.junheng.akka.kafka.protocol.KTopicProtocol.{BatchPayload, Payload}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

class ProtocolSerializer extends SerializerWithStringManifest {
  override def identifier: Int = 0xFFC1

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    manifest match {
      case "io.github.junheng.akka.kafka.protocol.KGroupProtocol.Pulled" => bytesToPulled(bytes)
      case "io.github.junheng.akka.kafka.protocol.KTopicProtocol.BatchPayload" => bytesToBatchPayload(bytes)
      case "io.github.junheng.akka.kafka.protocol.KTopicProtocol.Payload" => bytesToPayload(bytes)
      case other => throw new RuntimeException(s"can not deserialize message: $other")
    }
  }

  override def manifest(o: AnyRef): String = o.getClass.getCanonicalName

  override def toBinary(origin: AnyRef): Array[Byte] = {
    origin match {
      case pulled: Pulled => pulledToBytes(pulled)
      case batchPayload: BatchPayload => batchPayloadToBytes(batchPayload)
      case payload: Payload => payloadToBytes(payload)
      case other => throw new RuntimeException(s"can not serialize: ${other.getClass.getCanonicalName}")
    }
  }

  def bytesToBatchPayload(bytes: Array[Byte]): BatchPayload = {
    val (bis, dis) = asIS(bytes)
    try {
      val payloads = ArrayBuffer[Payload]()
      0 until dis.readInt() foreach { i =>
        val bytesOfPayload = new Array[Byte](dis.readInt())
        dis.read(bytesOfPayload)
        payloads += bytesToPayload(bytesOfPayload)
      }
      BatchPayload(payloads.toList)
    } finally {
      dis.close()
      bis.close()
    }
  }

  def batchPayloadToBytes(batch: BatchPayload): Array[Byte] = {
    val (bos, dos) = asOS()
    try {
      dos.writeInt(batch.payloads.length)
      batch.payloads foreach { payload =>
        val bytesOfPayload = payloadToBytes(payload)
        dos.writeInt(bytesOfPayload.length)
        dos.write(bytesOfPayload)
      }
      bos.toByteArray
    } finally {
      dos.close()
      bos.close()
    }
  }

  def bytesToPayload(bytes: Array[Byte]): Payload = {
    val (bis, dis) = asIS(bytes)
    try {
      val bytesOfKey = new Array[Byte](dis.readInt())
      dis.read(bytesOfKey)
      val bytesOfContent = new Array[Byte](dis.readInt())
      dis.read(bytesOfContent)
      Payload(bytesOfKey, bytesOfContent)
    } finally {
      dis.close()
      bis.close()
    }
  }

  def payloadToBytes(payload: Payload): Array[Byte] = {
    val (bos, dos) = asOS()
    try {
      dos.writeInt(payload.key.length)
      dos.write(payload.key)
      dos.writeInt(payload.content.length)
      dos.write(payload.content)
      bos.toByteArray
    } finally {
      dos.close()
      bos.close()
    }
  }

  def bytesToPulled(bytes: Array[Byte]): Pulled = {
    val (bis, dis) = asIS(bytes)
    try {
      val bytesOfTopic = new Array[Byte](dis.readInt())
      dis.read(bytesOfTopic)
      val bytesOfGroup = new Array[Byte](dis.readInt())
      dis.read(bytesOfGroup)
      val payloads = ArrayBuffer[Array[Byte]]()
      0 until dis.readInt() foreach { i =>
        val bytesOfPayload = new Array[Byte](dis.readInt())
        dis.read(bytesOfPayload)
        payloads += bytesOfPayload
      }
      Pulled(new String(bytesOfTopic), new String(bytesOfGroup), payloads.toList)
    } finally {
      dis.close()
      bis.close()
    }
  }

  def pulledToBytes(pulled: Pulled): Array[Byte] = {
    val (bos, dos) = asOS()
    try {
      val bytesOfTopic = pulled.topic.getBytes
      val bytesOfGroup = pulled.group.getBytes()
      dos.writeInt(bytesOfTopic.length)
      dos.write(bytesOfTopic)
      dos.writeInt(bytesOfGroup.length)
      dos.write(bytesOfGroup)
      dos.writeInt(pulled.payloads.length)
      pulled.payloads foreach { payload =>
        dos.writeInt(payload.length)
        dos.write(payload)
      }
      bos.toByteArray
    } finally {
      dos.close()
      bos.close()
    }
  }

  def asIS(bytes: Array[Byte]): (ByteArrayInputStream, DataInputStream) = {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)
    (bis, dis)
  }

  def asOS() = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    (bos, dos)
  }
}
