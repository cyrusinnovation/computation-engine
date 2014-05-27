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
  val rootNode : PersistentNode

  def unmarshal: Library = unmarshal(rootNode, Map.empty[String, String]).asInstanceOf[Library]

  def unmarshal(node: PersistentNode, nodeContext: Map[String, String]): SyntaxTreeNode = node.label match {
    case "library" => Library(attr(node, "name", nodeContext), versionMap(node, nodeContext))
    case "version" => version(node, nodeContext)
    case "simpleComputation" => simpleComputationFactory(node, nodeContext)
    case "abortIfComputation" => abortIfComputationFactory(node, nodeContext)
    case "namedComputation" => namedComputation(node, nodeContext)
    case "abortIfNoResultsComputation" => abortIfNoResultsComputation(node, nodeContext)
    case "abortIfHasResultsComputation" => abortIfNoResultsComputation(node, nodeContext)
    case "mappingComputation" => mappingComputation(node, nodeContext)
    case "iterativeComputation" => iterativeComputation(node, nodeContext)
    case "foldingComputation" => foldingComputation(node, nodeContext)
    case "sequentialComputation" => sequentialComputation(node, nodeContext)
    case "innerComputations" => throw new RuntimeException("innerComputations node should not be unmarshaled directly")
    case "innerComputation" => throw new RuntimeException("innerComputation node should not be unmarshaled directly")
    case "ref" => reference(node, nodeContext)
    case "imports" => throw new RuntimeException("imports nodes should not be unmarshaled to AstNode")
    case "inputs"  => throw new RuntimeException("inputs nodes should not be unmarshaled to AstNode")
    case "inputTuple" => singleTuple(node, nodeContext)
    case "accumulatorTuple" => singleTuple(node, nodeContext)
    case "key" => throw new RuntimeException("key node should not be unmarshaled to AstNode")
    case "value" => throw new RuntimeException("value node should not be unmarshaled to AstNode")
    case "initialAccumulatorKey" => throw new RuntimeException("initialAccumulatorKey node should not be unmarshaled to AstNode")
    case "resultKey" => throw new RuntimeException("resultKey node should not be unmarshaled to AstNode")
    case "computationExpression" => throw new RuntimeException("computationExpression node should not be unmarshaled to AstNode")
    case "predicateExpression" => throw new RuntimeException("predicateExpression node should not be unmarshaled to AstNode")
    case "logger" => throw new RuntimeException("logger node should not be unmarshaled to AstNode")
    case "securityConfiguration" => throw new RuntimeException("securityConfiguration node should not be unmarshaled to AstNode")
  }

  def unmarshalChildren(node: PersistentNode, label: String, nodeContext: Map[String, String]): SyntaxTreeNode = label match {
    case "imports" => imports(children(node, label), nodeContext)
    case "inputs"  => inputs(children(node, label), nodeContext)
  }

  def versionMap(node: PersistentNode, nodeContext: Map[String, String]) : Map[String, Version] = {
    val versions = children(node, "version")
    versions.foldLeft(Map[String,Version]()) {
      (mapSoFar, versionNode) => {
        val version = unmarshal(versionNode, nodeContext).asInstanceOf[Version]
        mapSoFar + (version.versionNumber -> version)
      }
    }
  }

  def version(versionNode: PersistentNode, nodeContext: Map[String, String]) : Version = {
    val defaults = children(versionNode).find(_.label == "defaults").fold(Map.empty[String, String])(attrValues)
    val topLevelComputations = children(versionNode).filterNot(_.label == "defaults")
    Version(attr(versionNode, "versionNumber", nodeContext),
      versionState(attr(versionNode, "state", nodeContext)),
      optionalAttrValue(versionNode, "commitDate").map(timeString => dateTime(timeString)),
      optionalAttrValue(versionNode, "lastEditDate").map(timeString => dateTime(timeString)),
      unmarshal(topLevelComputations.head, defaults).asInstanceOf[TopLevelComputationSpecification],
      topLevelComputations.tail.map(computationNode => unmarshal(computationNode, defaults).asInstanceOf[TopLevelComputationSpecification]): _*
    )
  }

  protected def versionState(stateString: String) : VersionState = {
    VersionState.fromString(stateString)
  }

  protected def simpleComputationFactory(node: PersistentNode, nodeContext: Map[String, String]) : SimpleComputationSpecification = {
    SimpleComputationSpecification(
      attr(node, "package", nodeContext),
      attr(node, "name", nodeContext),
      attr(node, "description", nodeContext),
      attr(node, "changedInVersion", nodeContext),
      attr(node, "shouldPropagateExceptions", nodeContext).toBoolean,
      attr(node, "computationExpression", nodeContext),
      unmarshalChildren(node, "imports", nodeContext).asInstanceOf[Imports],
      unmarshalChildren(node, "inputs", nodeContext).asInstanceOf[Inputs],
      attr(node, "resultKey", nodeContext),
      attr(node, "logger", nodeContext),
      attr(node, "securityConfiguration", nodeContext)
    )
  }

  protected def abortIfComputationFactory(node: PersistentNode, nodeContext: Map[String, String]) : AbortIfComputationSpecification = {
    AbortIfComputationSpecification(
      attr(node, "package", nodeContext),
      attr(node, "name", nodeContext),
      attr(node, "description", nodeContext),
      attr(node, "changedInVersion", nodeContext),
      attr(node, "shouldPropagateExceptions", nodeContext).toBoolean,
      attr(node, "predicateExpression", nodeContext),
      extractInnerComputationFrom(childOfType(node, "innerComputation"), nodeContext: Map[String, String]),
      unmarshalChildren(node, "imports", nodeContext).asInstanceOf[Imports],
      unmarshalChildren(node, "inputs", nodeContext).asInstanceOf[Inputs],
      attr(node, "logger", nodeContext),
      attr(node, "securityConfiguration", nodeContext)
    )
  }

  protected def namedComputation(node: PersistentNode, nodeContext: Map[String, String]): NamedComputationSpecification = {
    NamedComputationSpecification(
      attr(node, "package", nodeContext),
      attr(node, "name", nodeContext),
      attr(node, "description", nodeContext),
      attr(node, "changedInVersion", nodeContext),
      unmarshal(child(node), nodeContext).asInstanceOf[NamableComputationSpecification]
    )
  }

  protected def abortIfNoResultsComputation(node: PersistentNode, nodeContext: Map[String, String]) : AbortIfNoResultsComputationSpecification = {
    AbortIfNoResultsComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"), nodeContext)
    )
  }

  protected def abortIfHasResultsComputation(node: PersistentNode, nodeContext: Map[String, String]) : AbortIfHasResultsComputationSpecification = {
    AbortIfHasResultsComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"), nodeContext)
    )
  }

  protected def mappingComputation(node: PersistentNode, nodeContext: Map[String, String]): MappingComputationSpecification = {
    MappingComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"), nodeContext),
      unmarshal(childOfType(node, "inputTuple"),nodeContext).asInstanceOf[Mapping],
      attr(node, "resultKey", nodeContext)
    )
  }

  protected def iterativeComputation(node: PersistentNode,nodeContext: Map[String, String]) : IterativeComputationSpecification = {
    IterativeComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"), nodeContext),
      unmarshal(childOfType(node, "inputTuple"), nodeContext).asInstanceOf[Mapping],
      attr(node, "resultKey", nodeContext)
    )
  }

  protected def foldingComputation(node: PersistentNode,nodeContext: Map[String, String]) : FoldingComputationSpecification = {
    FoldingComputationSpecification(
      extractInnerComputationFrom(childOfType(node, "innerComputation"), nodeContext),
      attr(node, "initialAccumulatorKey", nodeContext),
      unmarshal(childOfType(node, "inputTuple"), nodeContext).asInstanceOf[Mapping],
      unmarshal(childOfType(node, "accumulatorTuple"), nodeContext).asInstanceOf[Mapping]
    )
  }

  protected def sequentialComputation(node: PersistentNode, nodeContext: Map[String, String]) : SequentialComputationSpecification = {
    val innerComputations = children(node, "innerComputations").map(x => extractInnerComputationFrom(x, nodeContext))

    SequentialComputationSpecification (
      innerComputations.head,
      innerComputations.tail:_*
    )
  }

  protected def reference(node: PersistentNode, nodeContext: Map[String, String]) : Ref = {
    new Ref(unmarshalToString(node))
  }

  protected def imports(nodes: List[PersistentNode], nodeContext: Map[String, String]): Imports = {
    Imports(nodes.map(unmarshalToString).toList: _*)
  }

  protected def inputs(nodes: List[PersistentNode], nodeContext: Map[String, String]): Inputs = {
    val mappings = nodes.map(mapping(_, nodeContext)).flatten
    Inputs(mappings.head, mappings.tail: _*)
  }

  protected def mapping(node: PersistentNode, nodeContext: Map[String, String]) = {
    attrValues(node).map(mapping => Mapping(mapping._1, mapping._2))
  }

  protected def singleTuple(node: PersistentNode, nodeContext: Map[String, String]): Mapping = {
    mapping(node, nodeContext).head
  }

  protected def extractInnerComputationFrom(innerComputationNode: PersistentNode, nodeContext: Map[String, String]) : InnerComputationSpecification = {
    assert(children(innerComputationNode).size == 1)
    val innerComputation = children(innerComputationNode).head
    unmarshal(innerComputation, nodeContext).asInstanceOf[InnerComputationSpecification]
  }

  private def attr(node: PersistentNode, key: String, defaults: Map[String, String]): String = {
    try {
      this.attrValue(node, key)
    } catch {
      case ex: NoSuchElementException => {
        defaults.get(key) match {
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

