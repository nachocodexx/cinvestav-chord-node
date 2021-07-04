package mx.cinvestav.payloads

object Payloads {
  case class AddKey(
                            key:String,
                            value:String
                          )

  case class Lookup(key:String,replyTo:String)

}
