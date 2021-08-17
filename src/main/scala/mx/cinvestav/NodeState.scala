package mx.cinvestav

import java.math.BigInteger

case class ChordNodeInfo(chordId:Int, start:Int, end:Int){
  def chordNodeId:String = s"ch-$chordId"
}
case class NodeState(
                      chordHashId:BigInteger,
                      chordInfos:List[ChordNodeInfo],
                      chordsData:List[ChordNodeInfo],
                      successors:List[ChordNodeInfo],
                      predecessors:List[ChordNodeInfo],
                      //
                      fingerTable:List[ChordNodeInfo],
                      fingerTableV2:Map[BigInteger,BigInteger],
                      totalOfNodes:Int,
                      totalOfKeyIds:Int,
                      data:Map[String,String],
                      status:Chord.status.Status = Chord.status.Up
                    )
