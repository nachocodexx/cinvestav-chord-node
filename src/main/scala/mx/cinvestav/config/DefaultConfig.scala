package mx.cinvestav.config

case class DefaultConfig(
                          nodeId:String,
                          chordId:Int,
                          keysPerNode:Int,
                          poolId:String,
                          totalOfNodes:Int,
                          //
                          numberOfSuccessors:Int,
                          numberOfPredecessors:Int,
                          lookupPolicy:String,
                          rabbitmq: RabbitMQConfig,
                        )
