package mx.cinvestav
case class ChordNodeInfo(chordId:Int, start:Int, end:Int, step:Int){
  def chordNodeId:String = s"ch-$chordId"
}
case class NodeState(
                      chordInfo:ChordNodeInfo,
                      fingerTable:Map[Int,ChordNodeInfo],
                      successor:ChordNodeInfo,
                      predecessor:ChordNodeInfo,
                      totalOfNodes:Int,
                      data:Map[String,String],
                      status:Chord.status.Status = Chord.status.Up
                    )
