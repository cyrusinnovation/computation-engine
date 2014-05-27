package com.cyrusinnovation.computation.persistence.reader

import org.joda.time.DateTime
import com.cyrusinnovation.computation.specification._
import com.cyrusinnovation.computation.specification.Inputs
import com.cyrusinnovation.computation.specification.Imports
import com.cyrusinnovation.computation.specification.Mapping
import com.cyrusinnovation.computation.specification.MappingComputationSpecification
import com.cyrusinnovation.computation.specification.AbortIfNoResultsComputationSpecification
import com.cyrusinnovation.computation.specification.IterativeComputationSpecification
import com.cyrusinnovation.computation.specification.AbortIfHasResultsComputationSpecification
import com.cyrusinnovation.computation.specification.NamedComputationSpecification
import com.cyrusinnovation.computation.specification.AbortIfComputationSpecification
import com.cyrusinnovation.computation.specification.Version
import com.cyrusinnovation.computation.specification.Library
import com.cyrusinnovation.computation.specification.SequentialComputationSpecification
import com.cyrusinnovation.computation.specification.FoldingComputationSpecification
import com.cyrusinnovation.computation.specification.SimpleComputationSpecification
import java.util.NoSuchElementException

trait PersistentNode {
  def label : String
}

trait PersistentTextBearingNode extends PersistentNode {
  val text : String
}

trait Reader {
  private val defaultMap = collection.mutable.Map.empty[String, String]

  val rootNode : PersistentNode
  def unmarshal: Library = unmarshal(rootNode).asInstanceOf[Library]

  def unmarshal(node: PersistentNode) : SyntaxTreeNode = node.label match {
    case "library" => Library(attr(node, "name"), versionMap(node))
    case "version" => version(node)
    case "simpleComputation" => simpleComputationFactory(node)
    case "abortIfComputation" => abortIfComputationFactory(node)
    case "namedComputation" => namedComputation(node)
    case "abortIfNoResultsComputation" => abortIfNoResultsComputation(node)
    case "abortIfHasResultsComputation" => abortIfNoResultsComputation(node)
    case "mappingComputation" => mappingComputation(node)
    case "iterativeComputation" => iterativeComputation(node)
    case "foldingComputation" => foldingComputation(node)
    case "sequentialComputation" => sequentialComputation(node)
    case "innerComputations" => throw new RuntimeException("innerComputations node should not be unmarshaled directly")
    case "innerComputation" => throw new RuntimeException("innerComputation node should not be unmarshaled directly")
    case "ref" => reference(node)
    case "imports" => throw new RuntimeException("imports nodes should not be unmarshaled to AstNode")
    case "inputs"  => throw new RuntimeException("inputs nodes should not be unmarshaled to AstNode")
    case "inputTuple" => singleTuple(node)
    case "accumulatorTuple" => singleTuple(node)
    case "key" => throw new RuntimeException("key node should not be unmarshaled to AstNode")
    case "value" => throw new RuntimeException("value node should not be unmarshaled to AstNode")
    case "initialAccumulatorKey" => throw new RuntimeException("initialAccumulatorKey node should not be unmarshaled to AstNode")
    case "resultKey" => throw new RuntimeException("resultKey node should not be unmarshaled to AstNode")
    case "computationExpression" => throw new RuntimeException("computationExpression node should not be unmarshaled to AstNode")
    case "predicateExpression" => throw new RuntimeException("predicateExpression node should not be unmarshaled to AstNode")
    case "logger" => throw new RuntimeException("logger node should not be unmarshaled to AstNode")
    case "securityConfiguration" => throw new RuntimeException("securityConfiguration node should not be unmarshaled to AstNode")
  }

  def unmarshalChildren(node: PersistentNode, label: String): SyntaxTreeNode = label match {
    case "imports" => imports(children(node, label))
    case "inputs"  => inputs(children(node, label))
  }

  def versionMap(node: PersistentNode) : Map[String, Version] = {
    val versions = children(node, "version")
    versions.foldLeft(Map[String,Version]()) {
      (mapSoFar, versionNode) => {
        val version = unmarshal(versionNode).asInstanceOf[Version]
        mapSoFar + (version.versionNumber -> version)
      }
    }
  }

  def version(versionNode: PersistentNode) : Version = {
    val defaults = children(versionNode).find(_.label == "defaults").fold(Map.empty[String, String])(attrValues)
    defaults.foreach(x => defaultMap.put(x._1,x._2))
    try {
      val topLevelComputations = children(versionNode).filterNot(_.label == "defaults")
      Version(attr(versionNode, "versionNumber"),
        versionState(attr(versionNode, "state")),
        optionalAttrValue(versionNode, "commitDate").map(timeString => dateTime(timeString)),
        optionalAttrValue(versionNode, "lastEditDate").map(timeString => dateTime(timeString)),
        unmarshal(topLevelComputations.head).asInstanceOf[TopLevelComputationSpecification],
        topLevelComputations.tail.map(computationNode => unmarshal(computationNode).asInstanceOf[TopLevelComputationSpecification]):_*
      )
    } finally {
      defaultMap.clear()
    }
  }

  protected def versionState(stateString: String) : VersionState = {
    VersionState.fromString(stateString)
  }

  protected def simpleComputationFactory(node: PersistentNode) : SimpleComputationSpecification = {
    SimpleComputationSpecification(
      attr(node, "package"),
      attr(node, "name"),
      attr(node, "description"),
      attr(node, "changedInVersion"),
      attr(node, "shouldPropagateExceptions").toBoolean,
      attr(node, "computationExpression"),
      unmarshalChildren(node, "imports").asInstanceOf[Imports],
      unmarshalChildren(node, "inputs").asInstanceOf[Inputs],
      attr(node, "resultKey"),
      attr(node, "logger"),
      attr(node, "securityConfiguration")
    )
  }

  protected def abortIfComputationFactory(node: PersistentNode) : AbortIfComputationSpecification = {
    AbortIfComputationSpecification(
      attr(node, "package"),
      attr(node, "name"),
      attr(node, "description"),
      attr(node, "changedInVersion"),
      attr(node, "shouldPropagateExceptions").toBoolean,
      attr(node, "predicateExpression"),
      extractInnerComputationFrom(childOfType(node, "innerComputation")),
      unmarshalChildren(node, "imports").asInstanceOf[Imports],
      unmarshalChildren(node, "inputs").asInstanceOf[Inputs],
      attr(node, "logger"),
      attr(node, "securityConfiguration")
    )
  }

  protected def namedComputation(node: PersistentNode) : NamedComputationSpecification = {
    NamedComputationSpecification(
      attr(node, "package"),
      attr(node, "name"),
      attr(node, "description"),
      attr(node, "changedInVersion"),
      unmarshal(child(node)).asInstanceOf[NamableComputationSpecification]
    )
  }

  protected def abortIfNoResultsComputation(node: PersistentNode) : AbortIfNoResultsComputationSpecification = {
    AbortIfNoResultsComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"))
    )
  }

  protected def abortIfHasResultsComputation(node: PersistentNode) : AbortIfHasResultsComputationSpecification = {
    AbortIfHasResultsComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"))
    )
  }

  protected def mappingComputation(node: PersistentNode) : MappingComputationSpecification = {
    MappingComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation")),
      unmarshal(childOfType(node, "inputTuple")).asInstanceOf[Mapping],
      attr(node, "resultKey")
    )
  }

  protected def iterativeComputation(node: PersistentNode) : IterativeComputationSpecification = {
    IterativeComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation")),
      unmarshal(childOfType(node, "inputTuple")).asInstanceOf[Mapping],
      attr(node, "resultKey")
    )
  }

  protected def foldingComputation(node: PersistentNode) : FoldingComputationSpecification = {
    FoldingComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation")),
      attr(node, "initialAccumulatorKey"),
      unmarshal(childOfType(node, "inputTuple")).asInstanceOf[Mapping],
      unmarshal(childOfType(node, "accumulatorTuple")).asInstanceOf[Mapping]
    )
  }

  protected def sequentialComputation(node: PersistentNode) : SequentialComputationSpecification = {
    val innerComputations = children(node, "innerComputations").map(x => extractInnerComputationFrom(x))

    SequentialComputationSpecification (
      innerComputations.head,
      innerComputations.tail:_*
    )
  }

  protected def reference(node: PersistentNode) : Ref = {
    new Ref(unmarshalToString(node))
  }

  protected def imports(nodes: List[PersistentNode]): Imports = {
    Imports(nodes.map(unmarshalToString).toList: _*)
  }

  protected def inputs(nodes: List[PersistentNode]): Inputs = {
    val mappings = nodes.map(mapping).flatten
    Inputs(mappings.head, mappings.tail: _*)
  }

  protected def mapping(node: PersistentNode) = {
    attrValues(node).map(mapping => Mapping(mapping._1, mapping._2))
  }

  protected def singleTuple(node: PersistentNode): Mapping = {
    mapping(node).head
  }

  protected def extractInnerComputationFrom(innerComputationNode: PersistentNode) : InnerComputationSpecification = {
    assert(children(innerComputationNode).size == 1)
    val innerComputation = children(innerComputationNode).head
    unmarshal(innerComputation).asInstanceOf[InnerComputationSpecification]
  }

  private def attr(node: PersistentNode, key: String): String = {
    try {
      this.attrValue(node, key)
    } catch {
      case ex: NoSuchElementException => {
        this.defaultMap.get(key) match {
          case None => throw ex
          case Some(v: String) => v
        }
      }
    }
  }

  protected def attrValue(node: PersistentNode, key: String) : String
  protected def attrValues(node: PersistentNode): Map[String, String]
  protected def optionalAttrValue(node: PersistentNode, key: String): Option[String]
  protected def children(node: PersistentNode) : List[PersistentNode]
  protected def children(node: PersistentNode, label: String) : List[PersistentNode]
  protected def asTextBearingNode(node: PersistentNode) : PersistentTextBearingNode
  protected def dateTime(timeString: String): DateTime

  //TODO This is hackery. Make this more consistent.
  protected def unmarshalToString(persistentNode: PersistentNode) : String = {
    asTextBearingNode(persistentNode).text
  }

  protected def child(persistentNode: PersistentNode) : PersistentNode = {
    children(persistentNode).head
  }

  protected def childOfType(persistentNode: PersistentNode, label: String) : PersistentNode = {
    children(persistentNode, label).head
  }
}

