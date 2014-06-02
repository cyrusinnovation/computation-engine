package com.cyrusinnovation.computation.persistence.writer

import java.io.{OutputStreamWriter, OutputStream}
import org.yaml.snakeyaml.Yaml
import java.util.{HashMap => JHMap}
import java.util.{Map => JMap}
import collection.JavaConverters._
import org.joda.time.DateTime
import java.text.SimpleDateFormat

object YamlWriter {
  private val formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")

  def forOutputStream(outputStream: OutputStream): Writer = {
    new YamlWriter(outputStream, new Yaml())
  }
}

class YamlWriter(stream: OutputStream, snakeYaml: Yaml) extends Writer {

  sealed abstract class Node

  case class EntryNode(label: String, attrs: Map[String, String], children: List[Context]) extends Node

  case class ListNode(label: String, children: List[String]) extends Node

  case class ContextListNode(label: String, children: List[Context]) extends Node

  case class MapNode(label: String, children: Map[String, String]) extends Node

  type Context = Node

  protected override def createNode(label: String, attrs: Map[String, String], children: List[Context]): Context = {
    EntryNode(label, attrs, children)
  }

  protected override def createNodeList(label: String, children: List[String]): Context = {
    ListNode(label, children)
  }

  protected override def createContextNodeList(label: String, children: List[Context]): Context = {
    ContextListNode(label, children)
  }

  protected override def createMapNode(label: String, children: Map[String, String]): Context = {
    MapNode(label, children)
  }

  protected override def dateTime(d: DateTime): String = {
    YamlWriter.formatter.format(d.toDate)
  }

  protected override def persist(nodeContext: Context) {
    val streamWriter = new OutputStreamWriter(stream)
    try {
      val context = nodeContext.asInstanceOf[EntryNode]
      val flattenedLibrary = context.copy(children = List())
      val ver = context.children.head.asInstanceOf[EntryNode]
      val verWithOutKids = ver.copy(children = List())
      val blah = (List(flattenedLibrary, verWithOutKids) ++ ver.children).map(extract).asJava
      snakeYaml.dump(blah, streamWriter)
    } finally {
      streamWriter.close()
    }
  }

  private def extract(node: Node): JMap[Object, Object] = {
    val map = new JHMap[Object, Object]
    node match {
      case aNode: EntryNode    => {
        val children = new JHMap[Object, Object]
        map.put(aNode.label, children)
        aNode.attrs.foreach(x => children.put(x._1, x._2))
        aNode.children.map(extract).foreach(x => children.putAll(x))
        map
      }
      case aContextNodeList: ContextListNode => {
        map.put(aContextNodeList.label, aContextNodeList.children.map(extract).asJava)
        map
      }
      case aNodeList: ListNode => {
        map.put(aNodeList.label, aNodeList.children.asJava)
        map
      }
      case aMapNode: MapNode   => {
        aMapNode.label match {
          case "" => {
            //A blank label means the children do not have a parent map
            //and should be added directly
            aMapNode.children.foreach(x => map.put(x._1, x._2))
          }
          case _  => map.put(aMapNode.label, aMapNode.children.asJava)
        }
        map
      }
    }
  }
}