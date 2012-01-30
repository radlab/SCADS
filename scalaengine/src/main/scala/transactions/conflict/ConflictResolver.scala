package edu.berkeley.cs.scads.storage
package transactions
package conflict

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import scala.collection.mutable.ListBuffer
import org.apache.avro.Schema

class ConflictResolver(val valueSchema: Schema, val ics: FieldICList) {
  type CommandSets = ArrayBuffer[CommandHashSet]

  private val logicalRecordUpdater = new LogicalRecordUpdater(valueSchema)

  // Assumes that the base of the cstructs are all the same.
  def getLUB(cstructs: Seq[CStruct]): CStruct = {
    if (cstructs.length == 1) {
      cstructs.head
    } else {
      val head = cstructs.head
      var result = convertToSets(head.commands)

      // Iterate over the rest of the cstructs.
      cstructs.tail.foreach(x => {
        result = updateMerge(result, convertToSets(x.commands), intersect=false)
      })

      CStruct(head.value, convertToSeq(result))
    }
  }

  // Assumes that the base of the cstructs are all the same.
  def getGLB(cstructs: Seq[CStruct]): CStruct = {
    if (cstructs.length == 1) {
      cstructs.head
    } else {
      val head = cstructs.head
      var result = convertToSets(head.commands)

      // Iterate over the rest of the cstructs.
      cstructs.tail.foreach(x => {
        result = updateMerge(result, convertToSets(x.commands))
      })

      CStruct(head.value, convertToSeq(result))
    }
  }

  // Returns true if c1 is a subset of c2.  If strict is true,
  // only returns true if c1 is a strict subset of c2.
  // Assumes that the base of the cstructs are all the same.
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

  // Assumes that the base of the cstructs are all the same.
  def isStrictSubset(cstruct1: CStruct, cstruct2: CStruct) = isSubset(cstruct1, cstruct2, true)


  // Returns a tuple pair (safe, leftover), where safe is the safe cstruct, and
  // leftover is a Seq[CStructCommand] of commands proposed but not safe.
  // Assumes that the base of the cstructs are all the same.
  // If cstructs does not have enough elements, the first CStruct is returned.
  def provedSafe(cstructs: Seq[CStruct], kQuorumSize: Int,
                 mQuorumSize: Int, N: Int): (CStruct, Seq[SinglePropose]) = {
    // Collect all commands.
    val leftover = new CommandHashSet
    cstructs.foreach(cs => {
      cs.commands.foreach(leftover.add(_))
    })

    // Minimum size of quorums to check within the cstructs seq.
    val minSize = kQuorumSize - (N - mQuorumSize)

    val safe = if (minSize < cstructs.size) {
      // Not enough overlap.  Return first cstruct.
      cstructs.head
    } else {
      // Compute the learned value.
      getLearnedCStruct(cstructs, kQuorumSize, mQuorumSize, N)
    }

    // Compute leftover commands, not in provedSafe.
    leftover.remove(safe.commands)

    // TODO: Check if LUB is valid w.r.t. constraints?
    (safe, leftover.toList.map(c => SinglePropose(c.xid, c.command)))
  }

  def getLearnedCStruct(cstructs: Seq[CStruct], kQuorumSize: Int,
                      mQuorumSize: Int, N: Int): CStruct = {
    // Minimum size of quorums to check within the cstructs seq.
    val minSize = kQuorumSize - (N - mQuorumSize)

    // All subsets which intersect with the cstructs.
    val allCombos = cstructs.combinations(minSize).toSeq

    // GLB for each possible subset.
    val allGLBs = allCombos.map(getGLB _)

    // LUB of all possible GLBs.
    val lub = getLUB(allGLBs)

    lub
  }

  // Returns the new compressed cstruct, along with the list of xids of the
  // committed and aborted txs, reflected in the base of the cstruct.
  // (cstruct, committed xid list, aborted xid list)
  def compressCStruct(c: CStruct): (CStruct, Seq[ScadsXid], Seq[ScadsXid]) = {
    // TODO: Is reordering across pending commands allowed?
    //       For now, not reordering...

    // Extract contiguous sequence of non-pending commands.
    val pendingIndex = c.commands.indexWhere(_.pending) match {
      case -1 => c.commands.length
      case x => x
    }

    if (pendingIndex == 0) {
      // First is pending, so compression is not possible.
      (c, List(), List())
    } else {
      // pending is a seq of pending commands, NOT being compressed.
      val (nonpending, pending) = c.commands.splitAt(pendingIndex)
      val nonpendingCommit = nonpending.filter(_.commit)

      val newBase = ApplyUpdates.applyUpdatesToBase(
        logicalRecordUpdater, c.value, nonpendingCommit)

      val nonpendingCommitXids = nonpendingCommit.map(_.xid)
      val nonpendingAbortXids = nonpending.filter(!_.commit).map(_.xid)
      println("COMPRESS: " + c + " new: " + CStruct(newBase, pending))

      (CStruct(newBase, pending), nonpendingCommitXids, nonpendingAbortXids)
    }
  }

  // Returns a new CommandSet which is the c1 merged with c2.
  // If intersect is true, computes the intersection.  Otherwise, computes the
  // union.
  private def updateMerge(c1: CommandSets, c2: CommandSets,
                          intersect: Boolean = true): CommandSets = {
    // Iterator for result.
    var i = 0
    // Iterator for c2.
    var j = 0
    val results = new CommandSets

    while (i < c1.length) {
      val currentSet = c1(i)
      // Find the corresponding HashSet in c2.
      val newSet = c2.indexWhere(!_.intersect(currentSet, false).isEmpty, j) match {
        case -1 => {
          // None of the currentSet is in c2.
          if (intersect) {
            currentSet.clear
          }
          currentSet
        }
        case x => {
          val merged = if (intersect) {
            currentSet.intersect(c2(x))
          } else {
            (j until x).foreach(idx => {
              results.append(c2(idx))
            })
            currentSet.union(c2(x))
          }
          j = x + 1
          merged
        }
      }
      // TODO: If there is an empty intersection, should it short circuit the
      //       rest of the comparisons?
      results.append(newSet)
      i += 1
    }

    if (!intersect) {
      (j until c2.length).foreach(idx => {
        results.append(c2(idx))
      })
    }

    results
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

  def intersect(chs: CommandHashSet, compareFlags: Boolean = true): CommandHashSet = {
    val copy = new CommandHashSet
    map.values.foreach(c => {
      chs.get(c).map(x => {
        if (x.pending && c.pending) {
          // TODO: what is the union of two pending commands, one accept and
          //       one reject?
          if ((x.commit == c.commit) || !compareFlags) {
            copy.add(c)
          }
        } else if (x.pending) {
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
