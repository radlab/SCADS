package com.googlecode.avro.runtime
trait HasCollectionBuilders {
  implicit def scala$collection$BitSetFactory = new BuilderFactory[Int, scala.collection.BitSet] {
    def newBuilder = scala.collection.BitSet.newBuilder
  }
  implicit def scala$collection$IndexedSeqFactory[E] = new BuilderFactory[E, scala.collection.IndexedSeq[E]] {
    def newBuilder = scala.collection.IndexedSeq.newBuilder[E]
  }
  implicit def scala$collection$IterableFactory[E] = new BuilderFactory[E, scala.collection.Iterable[E]] {
    def newBuilder = scala.collection.Iterable.newBuilder[E]
  }
  implicit def scala$collection$LinearSeqFactory[E] = new BuilderFactory[E, scala.collection.LinearSeq[E]] {
    def newBuilder = scala.collection.LinearSeq.newBuilder[E]
  }
  implicit def scala$collection$MapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.Map[A,B]] {
    def newBuilder = scala.collection.Map.newBuilder[A,B]
  }
  implicit def scala$collection$SeqFactory[E] = new BuilderFactory[E, scala.collection.Seq[E]] {
    def newBuilder = scala.collection.Seq.newBuilder[E]
  }
  implicit def scala$collection$SetFactory[E] = new BuilderFactory[E, scala.collection.Set[E]] {
    def newBuilder = scala.collection.Set.newBuilder[E]
  }
  implicit def scala$collection$SortedMapFactory[A,B](implicit ord: scala.math.Ordering[A]) = new BuilderFactory[(A,B), scala.collection.SortedMap[A,B]] {
    def newBuilder = scala.collection.SortedMap.newBuilder[A,B]
  }
  implicit def scala$collection$SortedSetFactory[E](implicit ord: scala.math.Ordering[E]) = new BuilderFactory[E, scala.collection.SortedSet[E]] {
    def newBuilder = scala.collection.SortedSet.newBuilder[E]
  }
  implicit def scala$collection$TraversableFactory[E] = new BuilderFactory[E, scala.collection.Traversable[E]] {
    def newBuilder = scala.collection.Traversable.newBuilder[E]
  }
}
trait HasImmutableBuilders {
  implicit def scala$collection$immutable$BitSetFactory = new BuilderFactory[Int, scala.collection.immutable.BitSet] {
    def newBuilder = scala.collection.immutable.BitSet.newBuilder
  }
  implicit def scala$collection$immutable$HashMapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.immutable.HashMap[A,B]] {
    def newBuilder = scala.collection.immutable.HashMap.newBuilder[A,B]
  }
  implicit def scala$collection$immutable$HashSetFactory[E] = new BuilderFactory[E, scala.collection.immutable.HashSet[E]] {
    def newBuilder = scala.collection.immutable.HashSet.newBuilder[E]
  }
  implicit def scala$collection$immutable$IndexedSeqFactory[E] = new BuilderFactory[E, scala.collection.immutable.IndexedSeq[E]] {
    def newBuilder = scala.collection.immutable.IndexedSeq.newBuilder[E]
  }
  implicit def scala$collection$immutable$IterableFactory[E] = new BuilderFactory[E, scala.collection.immutable.Iterable[E]] {
    def newBuilder = scala.collection.immutable.Iterable.newBuilder[E]
  }
  implicit def scala$collection$immutable$LinearSeqFactory[E] = new BuilderFactory[E, scala.collection.immutable.LinearSeq[E]] {
    def newBuilder = scala.collection.immutable.LinearSeq.newBuilder[E]
  }
  implicit def scala$collection$immutable$ListFactory[E] = new BuilderFactory[E, scala.collection.immutable.List[E]] {
    def newBuilder = scala.collection.immutable.List.newBuilder[E]
  }
  implicit def scala$collection$immutable$ListMapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.immutable.ListMap[A,B]] {
    def newBuilder = scala.collection.immutable.ListMap.newBuilder[A,B]
  }
  implicit def scala$collection$immutable$ListSetFactory[E] = new BuilderFactory[E, scala.collection.immutable.ListSet[E]] {
    def newBuilder = scala.collection.immutable.ListSet.newBuilder[E]
  }
  implicit def scala$collection$immutable$MapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.immutable.Map[A,B]] {
    def newBuilder = scala.collection.immutable.Map.newBuilder[A,B]
  }
  implicit def scala$collection$immutable$QueueFactory[E] = new BuilderFactory[E, scala.collection.immutable.Queue[E]] {
    def newBuilder = scala.collection.immutable.Queue.newBuilder[E]
  }
  implicit def scala$collection$immutable$SeqFactory[E] = new BuilderFactory[E, scala.collection.immutable.Seq[E]] {
    def newBuilder = scala.collection.immutable.Seq.newBuilder[E]
  }
  implicit def scala$collection$immutable$SetFactory[E] = new BuilderFactory[E, scala.collection.immutable.Set[E]] {
    def newBuilder = scala.collection.immutable.Set.newBuilder[E]
  }
  implicit def scala$collection$immutable$SortedMapFactory[A,B](implicit ord: scala.math.Ordering[A]) = new BuilderFactory[(A,B), scala.collection.immutable.SortedMap[A,B]] {
    def newBuilder = scala.collection.immutable.SortedMap.newBuilder[A,B]
  }
  implicit def scala$collection$immutable$SortedSetFactory[E](implicit ord: scala.math.Ordering[E]) = new BuilderFactory[E, scala.collection.immutable.SortedSet[E]] {
    def newBuilder = scala.collection.immutable.SortedSet.newBuilder[E]
  }
  implicit def scala$collection$immutable$StackFactory[E] = new BuilderFactory[E, scala.collection.immutable.Stack[E]] {
    def newBuilder = scala.collection.immutable.Stack.newBuilder[E]
  }
  implicit def scala$collection$immutable$StreamFactory[E] = new BuilderFactory[E, scala.collection.immutable.Stream[E]] {
    def newBuilder = scala.collection.immutable.Stream.newBuilder[E]
  }
  implicit def scala$collection$immutable$TraversableFactory[E] = new BuilderFactory[E, scala.collection.immutable.Traversable[E]] {
    def newBuilder = scala.collection.immutable.Traversable.newBuilder[E]
  }
  implicit def scala$collection$immutable$TreeMapFactory[A,B](implicit ord: scala.math.Ordering[A]) = new BuilderFactory[(A,B), scala.collection.immutable.TreeMap[A,B]] {
    def newBuilder = scala.collection.immutable.TreeMap.newBuilder[A,B]
  }
  implicit def scala$collection$immutable$VectorFactory[E] = new BuilderFactory[E, scala.collection.immutable.Vector[E]] {
    def newBuilder = scala.collection.immutable.Vector.newBuilder[E]
  }
}
trait HasMutableBuilders {
  implicit def scala$collection$mutable$ArrayBufferFactory[E] = new BuilderFactory[E, scala.collection.mutable.ArrayBuffer[E]] {
    def newBuilder = scala.collection.mutable.ArrayBuffer.newBuilder[E]
  }
  implicit def scala$collection$mutable$ArraySeqFactory[E] = new BuilderFactory[E, scala.collection.mutable.ArraySeq[E]] {
    def newBuilder = scala.collection.mutable.ArraySeq.newBuilder[E]
  }
  implicit def scala$collection$mutable$BitSetFactory = new BuilderFactory[Int, scala.collection.mutable.BitSet] {
    def newBuilder = scala.collection.mutable.BitSet.newBuilder
  }
  implicit def scala$collection$mutable$BufferFactory[E] = new BuilderFactory[E, scala.collection.mutable.Buffer[E]] {
    def newBuilder = scala.collection.mutable.Buffer.newBuilder[E]
  }
  implicit def scala$collection$mutable$DoubleLinkedListFactory[E] = new BuilderFactory[E, scala.collection.mutable.DoubleLinkedList[E]] {
    def newBuilder = scala.collection.mutable.DoubleLinkedList.newBuilder[E]
  }
  implicit def scala$collection$mutable$HashMapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.mutable.HashMap[A,B]] {
    def newBuilder = scala.collection.mutable.HashMap.newBuilder[A,B]
  }
  implicit def scala$collection$mutable$HashSetFactory[E] = new BuilderFactory[E, scala.collection.mutable.HashSet[E]] {
    def newBuilder = scala.collection.mutable.HashSet.newBuilder[E]
  }
  implicit def scala$collection$mutable$IndexedSeqFactory[E] = new BuilderFactory[E, scala.collection.mutable.IndexedSeq[E]] {
    def newBuilder = scala.collection.mutable.IndexedSeq.newBuilder[E]
  }
  implicit def scala$collection$mutable$IterableFactory[E] = new BuilderFactory[E, scala.collection.mutable.Iterable[E]] {
    def newBuilder = scala.collection.mutable.Iterable.newBuilder[E]
  }
  implicit def scala$collection$mutable$LinearSeqFactory[E] = new BuilderFactory[E, scala.collection.mutable.LinearSeq[E]] {
    def newBuilder = scala.collection.mutable.LinearSeq.newBuilder[E]
  }
  implicit def scala$collection$mutable$LinkedHashMapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.mutable.LinkedHashMap[A,B]] {
    def newBuilder = scala.collection.mutable.LinkedHashMap.newBuilder[A,B]
  }
  implicit def scala$collection$mutable$LinkedHashSetFactory[E] = new BuilderFactory[E, scala.collection.mutable.LinkedHashSet[E]] {
    def newBuilder = scala.collection.mutable.LinkedHashSet.newBuilder[E]
  }
  implicit def scala$collection$mutable$LinkedListFactory[E] = new BuilderFactory[E, scala.collection.mutable.LinkedList[E]] {
    def newBuilder = scala.collection.mutable.LinkedList.newBuilder[E]
  }
  implicit def scala$collection$mutable$ListBufferFactory[E] = new BuilderFactory[E, scala.collection.mutable.ListBuffer[E]] {
    def newBuilder = scala.collection.mutable.ListBuffer.newBuilder[E]
  }
  implicit def scala$collection$mutable$ListMapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.mutable.ListMap[A,B]] {
    def newBuilder = scala.collection.mutable.ListMap.newBuilder[A,B]
  }
  implicit def scala$collection$mutable$MapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.mutable.Map[A,B]] {
    def newBuilder = scala.collection.mutable.Map.newBuilder[A,B]
  }
  implicit def scala$collection$mutable$ResizableArrayFactory[E] = new BuilderFactory[E, scala.collection.mutable.ResizableArray[E]] {
    def newBuilder = scala.collection.mutable.ResizableArray.newBuilder[E]
  }
  implicit def scala$collection$mutable$SeqFactory[E] = new BuilderFactory[E, scala.collection.mutable.Seq[E]] {
    def newBuilder = scala.collection.mutable.Seq.newBuilder[E]
  }
  implicit def scala$collection$mutable$SetFactory[E] = new BuilderFactory[E, scala.collection.mutable.Set[E]] {
    def newBuilder = scala.collection.mutable.Set.newBuilder[E]
  }
  implicit def scala$collection$mutable$TraversableFactory[E] = new BuilderFactory[E, scala.collection.mutable.Traversable[E]] {
    def newBuilder = scala.collection.mutable.Traversable.newBuilder[E]
  }
  implicit def scala$collection$mutable$WeakHashMapFactory[A,B] = new BuilderFactory[(A,B), scala.collection.mutable.WeakHashMap[A,B]] {
    def newBuilder = scala.collection.mutable.WeakHashMap.newBuilder[A,B]
  }
}
