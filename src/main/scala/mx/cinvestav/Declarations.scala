package mx.cinvestav

import cats.effect.{IO, Ref}
import mx.cinvestav.config.{DefaultConfig, RabbitMQClusterConfig}
import mx.cinvestav.utils.v2.{PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.types.Metadata

import java.math.BigInteger

object Declarations {
  trait NodeError extends Error
  case class NoMessageId() extends NodeError {
    override def getMessage: String = "No <MESSAGE_ID> property provided"
  }
  case class NoReplyTo() extends NodeError {
    override def getMessage: String = "No <REPLY_TO> property provided"
  }
  case class ChordNode(nodeId:String,hash:BigInteger,publisher:Option[PublisherV2])
  case class LookupResult(key:BigInteger,chordNode: ChordNode,belongsToMe:Boolean=false )
//  object ChordNode {
//    def findSuccesor(x:BigInteger) = {
//      val isGreaterThan
//
//    }
//  }
//
  case class FingerTableEntry(chordNode: ChordNode,value:BigInteger)
  case class DefaultConfigV5(
                              nodeId:String,
                              poolId:String,
                              chordM:Int,
                              chordSlots:Int,
                              chordNodes:List[String],
                              rabbitmq:RabbitMQClusterConfig
                            )
  case class NodeStateV5(
                          ip:String,
//                          nodeIdHash:BigInteger,
                          chordNode: ChordNode,
                          nodesHashes: Map[String,ChordNode]= Map.empty[String,ChordNode],
                          fingerTable:List[FingerTableEntry],
//                          Map[BigInteger,String],
                          data:Map[String,Metadata] = Map.empty[String,Metadata]
                        )
  case class NodeContextV5(
                          state:Ref[IO,NodeStateV5],
                          rabbitContext: RabbitMQContext,
                          logger:Logger[IO],
                          config:DefaultConfigV5
                        )




}
