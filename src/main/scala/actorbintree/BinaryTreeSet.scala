/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, elem: Int) =>
      root ! Insert(requester, id, elem)
    case Contains(requester: ActorRef, id: Int, elem: Int) =>
      root ! Contains(requester, id, elem)
    case Remove(requester: ActorRef, id: Int, elem: Int) =>
      root ! Remove(requester, id, elem)
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => // do nothing
    case CopyFinished =>
      root = newRoot
      // FIXME: recheck this, order of messages may not be preserved
      context.become(normal)
      pendingQueue.foreach( self ! _ )
    case o: Operation =>
      pendingQueue = pendingQueue.enqueue(o)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  private def sendToSubTree(operation: Operation, ifNoSubTreeResponse: OperationReply) = {
    val nextNode: Option[ActorRef] =
      if(elem < operation.elem) subtrees.get(Right)
      else subtrees.get(Left)
    nextNode match {
      case Some(actorRef) => actorRef ! operation
      case None => operation.requester ! ifNoSubTreeResponse
    }
  }

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, insertElem: Int) =>
      val newNode = context.actorOf(props(insertElem, initiallyRemoved = false))
      if(elem > insertElem) {
        //insert into left subtree
        if(subtrees contains Left) {
          subtrees(Left) ! Insert(requester, id, insertElem)
        } else {
          subtrees = subtrees + (Left -> newNode)
          requester ! OperationFinished(id)
        }
      } else if(elem < insertElem) {
        //insert into right subtree
        if(subtrees contains Right) {
          subtrees(Right) ! Insert(requester, id, insertElem)
        } else {
          subtrees = subtrees + (Right -> newNode)
          requester ! OperationFinished(id)
        }
      } else {
        removed = false
        requester ! OperationFinished(id)
      }

    case Contains(requester: ActorRef, id: Int, containElem: Int) =>
      if(elem == containElem) requester ! ContainsResult(id, result = !removed)
      else sendToSubTree(Contains(requester, id, containElem), ContainsResult(id, result = false))

    case Remove(requester: ActorRef, id: Int, removeElement: Int) =>
      if(elem == removeElement) {
        removed = true
        requester ! OperationFinished(id)
      } else sendToSubTree(Remove(requester, id, removeElement), OperationFinished(id))

    case CopyTo(treeNode: ActorRef) =>
      if(!removed) treeNode ! Insert(self, elem, elem)
      subtrees foreach { case (_, child) => child ! CopyTo(treeNode) }
      context.become(copying(subtrees.valuesIterator.toSet, insertConfirmed = removed))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id: Int) =>
      context.become(copying(expected, insertConfirmed = true))
      self ! CopyFinished

    case CopyFinished =>
      if(expected.isEmpty && insertConfirmed) {
        // if node has no subtrees
        context.parent ! CopyFinished
        self ! PoisonPill
      }
      else if(expected.size == 1 && (expected contains sender()) && insertConfirmed) {
        // if node has subtrees and will be sent only when all the subtrees replied
        context.parent ! CopyFinished
        self ! PoisonPill
      }
      else {
        //if one of the subtree completes its copying
        context.become(copying(expected - sender(), insertConfirmed = true))
      }
  }


}
