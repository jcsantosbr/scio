/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders

import java.io.{ByteArrayOutputStream, IOException, InputStream, OutputStream}

import com.google.common.io.ByteStreams
import com.google.common.reflect.ClassPath
import com.google.protobuf.Message
import com.twitter.chill._
import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.coders.Coder.Context
import org.apache.beam.sdk.coders._
import org.apache.beam.sdk.util.VarInt
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers.JIterableWrapper

private object KryoRegistrarLoader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def load(k: Kryo): Unit = {
    logger.debug("Loading KryoRegistrars: " + registrars.mkString(", "))
    registrars.foreach(_(k))
  }

  private val registrars: Seq[IKryoRegistrar] = {
    logger.debug("Initializing KryoRegistrars")
    val classLoader = Thread.currentThread().getContextClassLoader
    ClassPath.from(classLoader).getAllClasses.asScala.toSeq
      .filter(_.getName.endsWith("KryoRegistrar"))
      .flatMap { clsInfo =>
        val optCls: Option[IKryoRegistrar] = try {
          val cls = clsInfo.load()
          if (classOf[AnnotatedKryoRegistrar] isAssignableFrom cls) {
            Some(cls.newInstance().asInstanceOf[IKryoRegistrar])
          } else {
            None
          }
        } catch {
          case _: Throwable => None
        }
        optCls
      }
  }

}

private[scio] class KryoAtomicCoder[T] extends AtomicCoder[T] {

  @transient
  private lazy val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo] {
    override def initialValue(): Kryo = {
      val k = KryoSerializer.registered.newKryo()

      k.forClass(new CoderSerializer(InstantCoder.of()))
      k.forClass(new CoderSerializer(TableRowJsonCoder.of()))

      // java.lang.Iterable.asScala returns JIterableWrapper which causes problem.
      // Treat it as standard Iterable instead.
      k.register(classOf[JIterableWrapper[_]], new JIterableWrapperSerializer)

      k.forSubclass[SpecificRecordBase](new SpecificAvroSerializer)
      k.forSubclass[GenericRecord](new GenericAvroSerializer)
      k.forSubclass[Message](new ProtobufSerializer)

      k.forClass(new KVSerializer)
      // TODO:
      // TimestampedValueCoder

      new AlgebirdRegistrar()(k)
      KryoRegistrarLoader.load(k)

      k
    }
  }

  override def encode(value: T, outStream: OutputStream, context: Context): Unit = {
    if (value == null) {
      throw new CoderException("cannot encode a null value")
    }
    if (context.isWholeStream) {
      val output = new Output(outStream)
      kryo.get().writeClassAndObject(output, value)
      output.flush()
    } else {
      val s = new ByteArrayOutputStream()
      val output = new Output(s)
      kryo.get().writeClassAndObject(output, value)
      output.flush()

      VarInt.encode(s.size(), outStream)
      outStream.write(s.toByteArray)
    }
  }

  override def decode(inStream: InputStream, context: Context): T = {
    val o = if (context.isWholeStream) {
      kryo.get().readClassAndObject(new Input(inStream))
    } else {
      val length = VarInt.decodeInt(inStream)
      if (length < 0) {
        throw new IOException("invalid length " + length)
      }

      val value = Array.ofDim[Byte](length)
      ByteStreams.readFully(inStream, value)
      kryo.get().readClassAndObject(new Input(value))
    }
//    logger.info("CODER DEBUG KryoAtomicCoder#decode " + o + " " + ScioUtil.debugLocation)
    o.asInstanceOf[T]
  }

  /*
  override def getEncodedElementByteSize(value: T, context: Context): Long = value match {
    case (key, values: Wrappers.JIterableWrapper[_]) =>
      val i = values.iterator
      val keySize = kryoEncodedElementByteSize(key, context: Context)
      val valSize = if (i.hasNext) {
        kryoEncodedElementByteSize(i.next(), context: Context)
      } else {
        0
      }
      values.underlying match {
        case xs: JCollection[_] =>
          logger.info("CODER DEBUG KryoAtomicCoder#getEncodedElementByteSize JCollection " +
            s"$values ${values.getClass} ${xs.getClass} " + ScioUtil.debugLocation)
          keySize + xs.size() * valSize
        case _ =>
          logger.info("CODER DEBUG KryoAtomicCoder#getEncodedElementByteSize ??? " +
            s"$values ${values.getClass} " + ScioUtil.debugLocation)
          // FIXME
          keySize + valSize
      }
    case _ =>
      super.getEncodedElementByteSize(value, context)
  }
  */
  override def getEncodedElementByteSize(value: T, context: Context): Long =
    throw new NotImplementedError("CODER DEBUG")

  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver,
                                        context: Context): Unit = value match {
    case (key, wrapped: Wrappers.JIterableWrapper[_]) =>
      observer.update(kryoEncodedElementByteSize(key, context))
      val underlying = wrapped.underlying
      val isO = underlying.isInstanceOf[ElementByteSizeObservableIterable[_, _]]
      val isC = underlying.isInstanceOf[JCollection[_]]
      logger.info(s"CODER DEBUG KryoAtomicCoder#registerByteSizeObserver " +
        s"${wrapped.getClass} ${underlying.getClass} $isO $isC")
      underlying match {
        case xs: ElementByteSizeObservableIterable[_, _] =>
          logger.info("CODER DEBUG KryoAtomicCoder#registerByteSizeObserver ElementByteSizeObservableIterable")
          xs.addObserver(new IteratorObserver(observer, underlying.isInstanceOf[JCollection[_]]))
        case _: JCollection[_] =>
          logger.info("CODER DEBUG KryoAtomicCoder#registerByteSizeObserver JCollection")
          observer.update(kryoEncodedElementByteSize(value, context))
        case _ =>
          logger.info(s"CODER DEBUG KryoAtomicCoder#registerByteSizeObserver ${underlying.getClass}")
          val iter = underlying.iterator()
          while (iter.hasNext) {
            observer.update(kryoEncodedElementByteSize(iter.next(), context))
          }
      }
    case _ =>
      observer.update(kryoEncodedElementByteSize(value, context))
  }

  private def kryoEncodedElementByteSize(obj: Any, context: Context): Long = {0
    val s = new CountingOutputStream(ByteStreams.nullOutputStream())
    val output = new Output(s)
    kryo.get().writeClassAndObject(output, obj)
    output.flush()
    if (context.isWholeStream) s.getCount else s.getCount + VarInt.getLength(s.getCount)
  }

  /** Ported from [[IterableLikeCoder]] .*/
  private class IteratorObserver(val outerObserver: ElementByteSizeObserver,
                                 val countable: Boolean) extends Observer {
    outerObserver.update(if (countable) 4L else 5L)
    override def update(o: Observable, arg: scala.Any): Unit = {
      require(arg.isInstanceOf[Long] || arg.isInstanceOf[JLong], "unexpected parameter object")
      if (countable) {
        outerObserver.update(o, arg)
      } else {
        outerObserver.update(o, 1L + arg.asInstanceOf[Long])
      }
    }
  }

}

private[scio] object KryoAtomicCoder {
  def apply[T]: Coder[T] = new KryoAtomicCoder[T]
}
