package com.googlecode.avro

import scala.collection.mutable.{HashMap,HashSet,ListBuffer}

/**
 * Textbook directed graph using adj lists
 * Could probably benefit from some nice operator overloading,
 * but that's not really that important
 */
class DirectedGraph[T] {
    private val nodes = new HashMap[T,Node[T]]

    def add(payload: T) {
        println("add(): " + payload)
        if (nodes.contains(payload))
            throw new IllegalArgumentException("Already contains node: " + payload)
        nodes.put(payload, Node(payload))
    }

    /**
     * Add (s -> t) to this graph
     */
    def addEdge(s: T, t: T) {
        println("addEdge(s -> t): " + s + "~>" + t)
        ensureContains(s)
        ensureContains(t)
        if (nodes.get(s).get.nbrs.contains(nodes.get(t).get))
            throw new IllegalArgumentException("Already contains edge: " + s + "~>" +  t)
        nodes.get(s).get.nbrs.add(nodes.get(t).get)
    }

    def containsEdge(s: T, t: T) = {
        if (!contains(s) || !contains(t))
            false
        else
            nodes.get(s).get.nbrs.contains(nodes.get(t).get)
    }

    def contains(payload: T) = {
        println("contains(): " + payload)
        nodes.contains(payload)
    }

    private def ensureContains(n: T) {
        if (!nodes.contains(n))
            throw new IllegalArgumentException("Does not already contains node: " + n)
    }

    /**
     * Only defined if hasCycle is false
     * Runs a standard DFS algorithm, recording finish times
     */
    def topologicalSort: List[T] = {
        if (hasCycle)
            throw new IllegalStateException("Cannot ask for topological sort with a cycle")
        val seenBefore = new HashSet[T]
        val sorted = new ListBuffer[T]
        nodes.foreach( kv => visit( kv._2, (n:Node[T]) => (println("Visiting: " + n)), (n:Node[T]) => { sorted += n.payload }, seenBefore ) ) 
        sorted.toList
    }

    /**
     * Standard DFS visitation method.
     */
    private def visit(node: Node[T], preVisit: (Node[T])=>Unit, postVisit: (Node[T])=>Unit, seenBefore: HashSet[T]) {
        if (!seenBefore.contains(node.payload)) {
            preVisit(node)
            seenBefore.add(node.payload)
            node.nbrs.foreach( nbr => visit(nbr, preVisit, postVisit, seenBefore) )
            postVisit(node)
        }
    }

    /**
     * Standard 3-color DFS cycle detection algorithm
     */
    def hasCycle: Boolean = {
        val colors = new HashMap[T, Color]
        nodes.foreach( kv => colors.put( kv._1, White ) )
        nodes.foreach( kv => {
            if (colors.get( kv._1 ).get == White)
                if (visitColor( kv._2, colors ))
                    return true
        })
        false
    }

    private def visitColor(node: Node[T], colors: HashMap[T,Color]): Boolean = {
        colors.put( node.payload, Grey )
        node.nbrs.foreach( nbr => {
            if (colors.get( nbr.payload ).get == Grey)
                return true
            else if (colors.get( nbr.payload ).get == White)
                if (visitColor(nbr, colors))
                    return true
        })
        colors.put( node.payload, Blue )
        false
    }

    override def toString = nodes.mkString("[", ",", "]")

    sealed trait Color 
    object White extends Color
    object Blue extends Color
    object Grey extends Color
}




/**
 * Node which encapsulates a T
 */
case class Node[T](val payload: T) {

    val nbrs = new HashSet[Node[T]]

}
