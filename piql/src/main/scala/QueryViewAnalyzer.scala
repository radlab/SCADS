package edu.berkeley.cs.scads.piql
package opt

import org.apache.avro.Schema
import scala.collection.JavaConversions._

import edu.berkeley.cs.scads.storage.client.index._

import plans._

import net.lag.logging.Logger

class QueryViewAnalyzer(plan: LogicalPlan, queryName: Option[String] = None) {
  val logger = Logger()

  lazy val relations = plan.flatGather(_ match {
    case r: Relation => List(r)
    case other => Nil
  })

  lazy val predicates = plan.flatGather(_ match {
    case Selection(pred, c) => List(pred)
    case other => Nil
  })

  lazy val viewName = "view_" + queryName.getOrElse("anonymous") + "__" + viewAttrs.map(a => a.field.name).mkString("_")

  lazy val (viewSchema, fieldInView) = {
    val fieldInView = scala.collection.mutable.Map[Value,Field]()
    val schema = Schema.createRecord(viewName, null, null, false)
    schema.setFields(viewAttrs.map(a => {
      val f = a.field
      val s = new Schema.Field(a.relation.name + "_" + f.name, f.schema, f.doc, null)
      fieldInView(a) = s
      for (u <- unityMap.get(a).getOrElse(Nil)) {
        fieldInView(u) = s
      }
      s
    }))
    (schema, fieldInView.toMap)
  }

  lazy val getViewNamespace = {
    val base = viewAttrs(0).relation.provider
    val ns = new IndexNamespace(viewName, base.cluster, base.cluster.namespaces, viewSchema)
    ns.open()
    ScadsView(ns)
  }

  lazy val rewrittenQuery = {
    rewrite(plan)(getViewNamespace)
  }

  protected def relationsNeededToProject(values: Seq[Value]) = {
    var relations = Set[TupleProvider]()
    for (v <- values) {
      v match {
        case a: QualifiedAttributeValue => 
          if (!fieldInView.contains(a))
            relations += a.relation
        case other => other
      }
    }
    logger.info("need these extra relations to project: " + relations)
    relations
  }
 
  protected def rewrite(plan: LogicalPlan)(implicit view: ScadsView): LogicalPlan = plan match {
    case Project(values, child, s) =>
      var inner = rewrite(child)
      for (r <- relationsNeededToProject(values)) {
        inner = Join(inner, r.asInstanceOf[LogicalPlan])
        for (k <- r.keyAttributes) {
          inner = Selection(
            EqualityPredicate(k, QualifiedAttributeValue(view, fieldInView(k))),
            inner)
        }
      }
      Project(values.map(rewrite), inner, s)
    case StopAfter(limit, child) =>
      StopAfter(limit, rewrite(child))
    case Selection(EqualityPredicate(v1: ParameterValue, v2: QualifiedAttributeValue), child) =>
      Selection(EqualityPredicate(v1, rewrite(v2)), rewrite(child))
    case Selection(EqualityPredicate(v1: QualifiedAttributeValue, v2: ParameterValue), child) =>
      Selection(EqualityPredicate(rewrite(v1), v2), rewrite(child))
    case Selection(p, child) =>
      rewrite(child)
    case Join(Selection(p, child), right) =>
      Selection(rewrite(p), rewrite(child))
    case Join(left, Selection(p, child)) =>
      Selection(rewrite(p), rewrite(child))
    case Join(left, right) => view // not sure if this works in all cases
    case r: Relation => view
  }

  protected def rewrite(p: Predicate)(implicit view: ScadsView): Predicate = p match {
    case EqualityPredicate(v1: QualifiedAttributeValue, v2: ParameterValue) =>
      EqualityPredicate(rewrite(v1), v2)
    case EqualityPredicate(v1: ParameterValue, v2: QualifiedAttributeValue) =>
      EqualityPredicate(v1, rewrite(v2))
    case other => other
  }
  
  protected def rewrite(v: Value)(implicit view: ScadsView): Value = v match {
    case a: QualifiedAttributeValue => 
      if (fieldInView.contains(a))
        QualifiedAttributeValue(view, fieldInView(a))
      else
        a // will fetch its value from join with base relation
    case other => other
  }
  
  lazy val (viewAttrs, unityMap) = {
    val delta = calcDelta(plan, null)

    val keyAttrs = relations.flatMap(_.keyAttributes)
    logger.debug("key attrs: %s", keyAttrs)

    val viewAttrs = delta.equalityAttributes ++ delta.ordering
    logger.debug("equality attrs: %s ordering attrs: %s", delta.equalityAttributes, delta.ordering)

    val suffixAttrs = keyAttrs.filterNot(viewAttrs contains _)
    logger.debug("suffix attrs: %s", suffixAttrs)

    val unityMap =
      delta.unified
        .flatMap { case EqualityPredicate(v1, v2) => (v1, v2) :: (v2, v1) :: Nil }
        .groupBy(_._1)
        .map { case (v1, v2s) => (v1, v2s.map(_._2).toSet) }
        .toMap
    logger.debug("unity map: %s", unityMap)

    val attrs = (viewAttrs ++ suffixAttrs)
      .foldRight((List[Value](), List[Value]())) {
        case (a, (p, cov)) =>
          if (cov contains a)
            (p, cov)
          else
            (a +: p, cov ++ unityMap.get(a).getOrElse(Nil))
      }._1.map(_ match { case a: QualifiedAttributeValue => a })
    
    (attrs, unityMap)
  }

  lazy val deltaQueries = {
    relations.map(r => {
      val delta = calcDelta(plan, r)
      val fields = scala.collection.mutable.Map[QualifiedAttributeValue,Int]()
      var i = -1

      /* pass to replace delta relation with parameters */
      def deltify(plan: LogicalPlan): LogicalPlan = plan match {
        case Project(values, child, s) =>
          Project(values.map(parameterize), deltify(child), s)
        case StopAfter(limit, child) =>
          StopAfter(limit, deltify(child))
        case Selection(p, child) =>
          /* TODO use cardinality constraints instead of lying */
          DataStopAfter(FixedLimit(123), Selection(deltifyp(p), deltify(child)))
        case Join(left, right) if (left == r) => deltify(right)
        case Join(left, right) if (right == r) => deltify(left)
        case Join(left, right) => Join(deltify(left), deltify(right))
        case r: Relation => r
      }

      def deltifyp(predicate: Predicate): Predicate = predicate match {
        case EqualityPredicate(v1, v2) =>
          EqualityPredicate(parameterize(v1), parameterize(v2))
      }

      def parameterize(w: Value): Value = w match {
        case v: QualifiedAttributeValue => v
          if (v.relation == r) {
            if (fields.contains(v)) {
              ParameterValue(fields(v))
            } else {
              i += 1
              fields(v) = i
              ParameterValue(i)
            }
          } else {
            v
          }
      }
      val ret = (r, deltify(Project(viewAttrs, delta.plan, viewSchema)))
      /* TODO figure out how to hook this into base relations */
      logger.debug("remapped fields to parameters: " + fields)
      ret
    }).toMap
  }

  case class SubPlan(plan: LogicalPlan, equalityAttributes: Seq[QualifiedAttributeValue], unified: Seq[EqualityPredicate], ordering: Seq[Value], ordered: Boolean)

  protected def calcDelta(plan: LogicalPlan, relation: TupleProvider): SubPlan = plan match {
    case Selection(EqualityPredicate(v1: ParameterValue, v2: QualifiedAttributeValue), child) =>
      val deltaChild = calcDelta(child, relation)
      deltaChild.copy(equalityAttributes=(v2 +: deltaChild.equalityAttributes))
    case Selection(EqualityPredicate(v1: QualifiedAttributeValue, v2: ParameterValue), child) =>
      val deltaChild = calcDelta(child, relation)
      deltaChild.copy(equalityAttributes=v1 +: deltaChild.equalityAttributes)
    case Selection(p, child) =>
      val deltaChild = calcDelta(child, relation)
      p match {
        case ep: EqualityPredicate => deltaChild.copy(plan=Selection(p, deltaChild.plan), unified=ep +: deltaChild.unified )
        case op => deltaChild.copy(plan=Selection(p, deltaChild.plan))
      }
    case Sort(attrs, asc, child)
      if(attrs.collect {case q: QualifiedAttributeValue => q}.map(_.relation) contains relation) =>
        calcDelta(child, relation).copy(ordering = attrs, ordered=false)
    case Sort(attrs, asc, child) =>
      val deltaChild = calcDelta(child, relation)
      deltaChild.copy(plan=Sort(attrs, asc, deltaChild.plan), ordering=attrs, ordered=true)
    case StopAfter(count, child) =>
      val deltaChild = calcDelta(child, relation)
      if(deltaChild.ordered)
        deltaChild.copy(plan= StopAfter(count, deltaChild.plan))
      else deltaChild
    case DataStopAfter(count, child) =>
      calcDelta(child, relation)
    case Project(values, child, s) =>
      calcDelta(child, relation)
    case Join(left, right) =>
      val deltaLeft = calcDelta(left, relation)
      val deltaRight = calcDelta(right, relation)
      SubPlan(Join(deltaLeft.plan, deltaRight.plan),
        deltaLeft.equalityAttributes ++ deltaRight.equalityAttributes,
        deltaLeft.unified ++ deltaRight.unified,
        Nil,
        false)
    case Paginate(cnt, c) =>
      val deltaChild = calcDelta(c, relation)
      if(deltaChild.ordered)
        deltaChild.copy(plan=Paginate(cnt, deltaChild.plan))
      else
        deltaChild
    case r: Relation => SubPlan(r, Nil, Nil, Nil, false)
  }
}
