package com.cyrusinnovation.computation.persistence.writer

import com.cyrusinnovation.computation.specification._
import org.joda.time.DateTime

trait Writer {
  type Context

  def write(library: Library) {
    persist(marshal(library))
  }

  protected def marshal(node: SyntaxTreeNode): Context = {
    node match {
      case node: Library                            => library(node)
      case node: Version                            => version(node)
      case node: SimpleComputationSpecification     => simpleComputationSpec(node)
      case node: AbortIfComputationSpecification    => abortIfComputationSpec(node)
      case node: NamedComputationSpecification      => namedComputationSpec(node)
      case node: SequentialComputationSpecification => sequentialComputationSpec(node)
      case node: MappingComputationSpecification    => mappingComputationSpec(node)
      case node: FoldingComputationSpecification    => foldingComputationSpec(node)
      case node: Ref                                => ref(node)
      case node: MappingWrapper                     => mapping(node)
      case node: Inputs                             => inputs(node)
      case node: Imports                            => imports(node)
    }
  }

  private def library(library: Library) = {
    val versions = library.versions.map(x => {
      marshal(x._2)
    }).toList
    createNode("library", Map("name" -> library.name), versions)
  }

  protected def version(version: Version) = {
    val lastEditDate = version.lastEditDate.map(dateTime).getOrElse(null)
    val kids = List(marshal(version.firstTopLevelComputation)) ++ version.moreTopLevelComputations.map(marshal(_))
    createNode("version", Map("versionNumber" -> version.versionNumber, "state" -> version.state.toString, "lastEditDate" -> lastEditDate), kids)
  }

  private def simpleComputationSpec(computation: SimpleComputationSpecification) = {
    val attrs = Map("package" -> computation.packageValue,
      "name" -> computation.name,
      "description" -> computation.description,
      "changedInVersion" -> computation.changedInVersion,
      "shouldPropagateExceptions" -> computation.shouldPropagateExceptions.toString,
      "computationExpression" -> computation.computationExpression,
      "resultKey" -> computation.resultKey,
      "logger" -> computation.logger,
      "securityConfiguration" -> computation.securityConfiguration)
    createNode("simpleComputation", attrs, List(marshal(computation.imports), marshal(computation.input)))
  }

  private def abortIfComputationSpec(computation: AbortIfComputationSpecification) = {
    val attrs = Map("package" -> computation.packageValue,
      "name" -> computation.name,
      "description" -> computation.description,
      "changedInVersion" -> computation.changedInVersion,
      "logger" -> computation.logger,
      "securityConfiguration" -> computation.securityConfiguration,
      "shouldPropagateExceptions" -> computation.shouldPropagateExceptions.toString,
      "predicateExpression" -> computation.predicateExpression)

    val inners = List(marshal(computation.innerSpecification))
    val innerComputation = createNode("innerComputation", Map(), inners)
    createNode("abortIfComputation", attrs, List(innerComputation, marshal(computation.imports), marshal(computation.input)))
  }

  private def namedComputationSpec(computation: NamedComputationSpecification) = {
    val attrs = Map("package" -> computation.packageValue,
      "name" -> computation.name,
      "description" -> computation.description,
      "changedInVersion" -> computation.changedInVersion)
    createNode("namedComputation", attrs, List(marshal(computation.specForNamableComputation)))
  }

  protected def sequentialComputationSpec(computation: SequentialComputationSpecification) = {
    val inners = computation.innerSpecs.map(marshal(_))
    val ictx = createContextNodeList("innerComputations", inners)
    createNode("sequentialComputation", Map(), List(ictx))
  }

  private def mappingComputationSpec(computation: MappingComputationSpecification) = {
    val attrs = Map("resultKey" -> computation.resultKey)
    val inputTupeCtx = marshal(MappingWrapper("inputTuple", computation.inputTuple))
    val innerCompCtx = createNode("innerComputation", Map(), List(marshal(computation.innerSpecification)))
    createNode("mappingComputation", attrs, List(inputTupeCtx, innerCompCtx))
  }

  private def foldingComputationSpec(computation: FoldingComputationSpecification) = {
    val attrs = Map("initialAccumulatorKey" -> computation.initialAccumulatorKey)
    val inputTupelCtx = marshal(MappingWrapper("inputTuple", computation.inputTuple))
    val accumulatorTupleCtx = marshal(MappingWrapper("accumulatorTuple", computation.accumulatorTuple))
    val innerCompCtx = createNode("innerComputation", Map(), List(marshal(computation.innerSpecification)))
    createNode("foldingComputation", attrs, List(inputTupelCtx, accumulatorTupleCtx, innerCompCtx))
  }

  protected def ref(ref: Ref) = {
    createMapNode("", Map("ref" -> ref.referencedSpecification))
  }

  protected def mapping(mapping: MappingWrapper) = {
    createMapNode(mapping.label, Map(mapping.mapping.key -> mapping.mapping.value))
  }

  protected def inputs(inputs: Inputs) = {
    val map = inputs.inputMappings.map(x => marshal(MappingWrapper("", x))).toList
    createContextNodeList("inputs", map)
  }

  protected def imports(imports: Imports) = {
    val s = imports.importSequence
    createNodeList("imports", s.toList)
  }

  protected def dateTime(d: DateTime): String

  protected def createNode(label: String, attrs: Map[String, String], children: List[Context]): Context

  protected def createNodeList(label: String, children: List[String]): Context

  protected def createMapNode(label: String, children: Map[String, String]): Context

  protected def createContextNodeList(label: String, children: List[Context]): Context

  protected def persist(context: Context)

  protected case class MappingWrapper(label: String, mapping: Mapping) extends SyntaxTreeNode {
    def children : List[SyntaxTreeNode] = List.empty
  }
}