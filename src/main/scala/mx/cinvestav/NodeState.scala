package mx.cinvestav
case class ChordNodeInfo(chordId:Int, start:Int, end:Int){
  def chordNodeId:String = s"ch-$chordId"
}
case class NodeState(
                      chordInfos:List[ChordNodeInfo],
                      chordsData:List[ChordNodeInfo],
                      successors:List[ChordNodeInfo],
                      predecessors:List[ChordNodeInfo],
                      notFoundKeysCounter:Map[String,Boolean]=Map.empty[String,Boolean],
                      //
                      fingerTable:List[ChordNodeInfo],
//                      Map[Int,ChordNodeInfo],
                      totalOfNodes:Int,
                      totalOfKeyIds:Int,
                      data:Map[String,String],
                      status:Chord.status.Status = Chord.status.Up
                    )
