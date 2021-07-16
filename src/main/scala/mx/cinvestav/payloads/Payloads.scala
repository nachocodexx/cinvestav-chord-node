package mx.cinvestav.payloads

object Payloads {
  case class AddKey(
                            key:String,
                            value:String
                          )

  case class Lookup(key:String,replyTo:String,exchangeName:String)
  case class Lock(id:String,crashedNodeId:String,fromNodeId:String,crashedChordId:Int,timestamp:Long)
  case class KeyFound(key:String,value:String,timestamp:Long,chordNodeId:String)
  case class KeyNotFound(key:String,timestamp:Long,chordNodeId:String)

}
