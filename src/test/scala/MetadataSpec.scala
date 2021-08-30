//import mx.cinvestav.Declarations.{ChunkMetadata, ChunkReplicas, Metadata}
import mx.cinvestav.commons.types.{ChunkHandle, ChunkMetadata, Location, Metadata}
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

class MetadataSpec extends munit .CatsEffectSuite {
  test("Basics"){
    val chunks = List(
      ChunkHandle(
        chunkId = "cds1212_0",
        sizeIn = 0L,
        sizeOut=0L,
        locations= List(Location(url="http://69.0.0.1/download",source = "/sn-0/ch0/ch0_0")),
        index = 0,
        compressionAlgorithm = "LZ4"
      )
    )
    val metadata =Metadata(
      fileId = "f0",
//      filename = "my_book",
      extension = "pdf",
      sizeIn = 0L,
      sizeOut = 0L,
      replicationFactor = 2,
      chunks = chunks,
      version = 1,
      userId = "u0",
      timestamp = 0L
    )
    println(metadata.asJson)
  }

}
