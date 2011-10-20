package edu.berkeley.cs.scads.storage
package transactions
package conflict

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import scala.collection.mutable.ListBuffer
import org.apache.avro.Schema

class ConflictResolver(val valueSchema: Schema, val ics: FieldICList) {
  type CommandSets = ArrayBuffer[CommandHashSet]

  // TODO: pending vs. committed.
  // TODO: base record for cstructs.

  def getLUB(cstructs: Seq[CStruct]): CStruct = {
    if (cstructs.length == 1) {
      cstructs.head
    } else {
      val head = cstructs.head
      val result = convertToSets(head.commands)

      // Iterate over the rest of the cstructs.
      cstructs.tail.foreach(x => {
        updateMerge(result, convertToSets(x.commands), intersect=false)
      })

      CStruct(head.value, convertToSeq(result))
    }
  }

  def getGLB(cstructs: Seq[CStruct]): CStruct = {
    if (cstructs.length == 1) {
      cstructs.head
    } else {
      val head = cstructs.head
      val result = convertToSets(head.commands)

      // Iterate over the rest of the cstructs.
      cstructs.tail.foreach(x => {
        updateMerge(result, convertToSets(x.commands))
      })

      CStruct(head.value, convertToSeq(result))
    }
  }

  // Returns true if c1 is a subset of c2.  If strict is true,
  // only returns true if c1 is a strict subset of c2.
  def isSubset(c1: CStruct, c2: CStruct, strict: Boolean = false): Boolean = {
    val cs1 = convertToSets(c1.commands)
    val cs2 = convertToSets(c2.commands)
    var valid = true

    cs1.zipAll(cs2, null, null).foreach(x => {
      if (valid) {
        x match {
          case (null, b) =>
          case (a, null) => valid = false
          case (a, b) => {
            val aSize = a.size
            val bSize = b.size
            val intSize = a.intersect(b).size
            if (aSize == intSize) {
              valid = strict match {
                case true => aSize < bSize
                case false => true
              }
            } else {
              valid = false
            }
          }
        }
      }
    })
    valid
  }

  def isStrictSubset(cstruct1: CStruct, cstruct2: CStruct) = isSubset(cstruct1, cstruct2, true)

  def provedSafe(cstructs: Seq[CStruct], quorum : Int): CStruct = null

  // Returns a tuple pair (safe, leftover), where safe is the safe cstruct, and
  // leftover is a Seq[CStructCommand] of commands proposed but not safe.
  // Assumes that the base of the cstructs are all the same.
  def provedSafe(cstructs: Seq[CStruct], fastQuorumSize: Int,
                 classicQuorumSize: Int, N: Int): (CStruct, Seq[CStructCommand]) = {
    // TODO: does cstructs require fastQuorumSize number of elements?

    // Collect all commands
    val leftover = new CommandHashSet
    cstructs.foreach(cs => {
      cs.commands.foreach(leftover.add(_))
    })

    // All the sizes of quorums to check within the cstructs seq.
    // TODO: Tim-> Is this necessary????
    val sizes = (classicQuorumSize - (N - fastQuorumSize)) to classicQuorumSize

    // All possible quorums which intersect with the cstructs.
    val allCombos = sizes.map(cstructs.combinations(_).toSeq).reduceLeft(_ ++ _)

    // GLB for each possible quorum.
    val allGLBs = allCombos.map(getGLB _)

    // LUB of all possible GLBs.
    val lub = getLUB(allGLBs)

    // Compute leftover commands, not in provedSafe.
    leftover.remove(lub.commands)

    // TODO: Check if LUB is valid w.r.t. constraints?
    (lub, leftover.toList)
  }

  // Modifies first CommandSet to be merged with the second CommandSet.
  // If intersect is true, computes the intersection.  Otherwise, computes the
  // union.
  private def updateMerge(result: CommandSets, c2: CommandSets,
                          intersect: Boolean = true) = {
    // Iterator for result.
    var i = 0
    // Iterator for c2.
    var j = 0

    while (i < result.length) {
      val currentSet = result(i)
      // Find the corresponding HashSet in c2.
      val newSet = c2.indexWhere(!_.intersect(currentSet).isEmpty, j) match {
        case -1 => {
          // None of the currentSet is in c2.
          if (intersect) {
            currentSet.clear
          }
          currentSet
        }
        case x => {
          j = x
          if (intersect) {
            currentSet.intersect(c2(x))
          } else {
            currentSet.union(c2(x))
          }
        }
      }
      // TODO: If there is an empty intersection, should it short circuit the
      //       rest of the comparisons?
      result.update(i, newSet)
      i += 1
    }
  }

  private def convertToSeq(c: CommandSets): Seq[CStructCommand] = {
    val result = new ArrayBuffer[CStructCommand]
    c.foreach(x => {
      result.appendAll(x.toList)
    })
    result
  }

  private def convertToSets(c: Seq[CStructCommand]): CommandSets = {
    val result = new CommandSets()
    var isLogical = false
    c.foreach(x => {
      x.command match {
        case up: LogicalUpdate => {
          if (!isLogical) {
            result.append(new CommandHashSet)
          }
          result.last.add(x)
          isLogical = true
        }
        case up: PhysicalUpdate => {
          result.append(new CommandHashSet)
          result.last.add(x)
          isLogical = false
        }
        case _ =>
      }
    })
    result
  }

/***********************************************************************
 **************** old code, will probably go away **********************
 ********************************************************************* */

  def isCompatible(cstructs: Seq[CStruct]): Boolean = {
    if (cstructs.length <= 1) {
      true
    } else {
      val commandsList = cstructs.map(x => x.commands)
      // TODO: only pending?
      val head = reduceCommandList(commandsList.head.filter(_.pending))
      val tail = commandsList.tail
      tail.foldLeft[Boolean](true)(foldCommandList(head))
    }
  }

  private def compareCommandList(c1: Seq[Seq[CStructCommand]],
                                 c2: Seq[Seq[CStructCommand]]) = {
    val zipped = c1.zipAll(c2, List(), List())
    var success = true

    zipped.foreach(t => {
      if (success) {
        t match {
          case (l1: Seq[CStructCommand], l2: Seq[CStructCommand]) =>
            success = compareCommands(l1, l2)
          case (_, _) => success = false
        }
      }
    })
    success
  }

  private def compareCommands(c1: Seq[CStructCommand],
                              c2: Seq[CStructCommand]) = {
    if (c1.size != c2.size) {
      false
    } else {
      // Match xids for now.
      val map1 = c1.map(x => (x.xid, 1)).toMap
      var success = true
      c2.foreach(x => {
        if (!map1.contains(x.xid)) {
          success = false
        }
      })
      success
    }
  }

  // Reduces a sequence of logical updates into a single sequence.
  private def reduceCommandList(c: Seq[CStructCommand]) = {
    val reduced = new ListBuffer[ListBuffer[CStructCommand]]()
    var isLogical = false
    c.foreach(x => {
      x.command match {
        case up: LogicalUpdate => {
          if (isLogical) {
            reduced.last.append(x)
          } else {
            val newIds = new ListBuffer[CStructCommand]()
            newIds.append(x)
            reduced.append(newIds)
          }
          isLogical = true
        }
        case up: PhysicalUpdate => {
          val newIds = new ListBuffer[CStructCommand]()
          newIds.append(x)
          reduced.append(newIds)
          isLogical = false
        }
        case _ =>
      }
    })
    reduced
  }

  private def foldCommandList(head: Seq[Seq[CStructCommand]])(valid: Boolean, commands: Seq[CStructCommand]) = {
    if (!valid) {
      !valid
    } else {
      val pendingCommands = reduceCommandList(commands.filter(_.pending))
      compareCommandList(head, pendingCommands)
    }
  }
}

// This HashSet is aware of CStructCommands and considers the pending flag and
// the commit flag.
class CommandHashSet {
  protected val map = new HashMap[ScadsXid, CStructCommand]

  def toList = map.values.toList
  def clear = map.clear
  def isEmpty = map.isEmpty
  def size = map.size

  def add(c: CStructCommand) = {
    map.put(c.xid, c)
  }

  def get(c: CStructCommand): Option[CStructCommand] = {
    map.get(c.xid)
  }

  def intersect(chs: CommandHashSet): CommandHashSet = {
    val copy = new CommandHashSet
    map.values.foreach(c => {
      chs.get(c).map(x => {
        if (x.pending) {
          copy.add(c)
        } else {
          copy.add(x)
        }
      })
    })
    copy
  }

  def union(chs: CommandHashSet): CommandHashSet = {
    val copy = new CommandHashSet
    map.values.foreach(c => {
      chs.get(c) match {
        case None => copy.add(c)
        case Some(x) => {
          if (x.pending && c.pending) {
            // TODO: what is the union of two pending commands, one accept and
            //       one reject?
            if (x.commit == c.commit) {
              copy.add(c)
            }
          } else if (x.pending) {
            copy.add(c)
          } else {
            copy.add(x)
          }
        }
      }
    })
    copy
  }

  def remove(c: Seq[CStructCommand]) = {
    c.foreach(c => {
      map.remove(c.xid)
    })
  }
}
