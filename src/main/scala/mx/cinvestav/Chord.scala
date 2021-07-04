package mx.cinvestav

object Chord {
  object policies {
    final val SIMPLE   = "simple"
    final val SCALABLE = "scalable"
    final val DEFAULT  = "default"
  }

  object status{
    sealed trait Status
    case object Up extends Status
    case object Lock extends Status
  }
//  case object
  def buildChordInfoById(chordId:Int, step:Int = 10):ChordNodeInfo = {
    val start = chordId*step
    val end  = (start +  step)-1
    ChordNodeInfo(chordId = chordId,start =  start, end =  end, step = step)
  }
  def checkIfBelongs(dataId:Int, chordId:Int, step:Int): Boolean = {
    val info = buildChordInfoById(chordId = chordId,step=step)
    ((info.start <= dataId) && (info.end >= dataId))
  }
  def checkIfBelongsToChordNode(dataId:Int,chordNode:ChordNodeInfo): Boolean = {
    ((chordNode.start <= dataId) && (chordNode.end >= dataId))
  }
  def chordIdWithPrefix(chordId:Int):String =s"ch-$chordId"

}
