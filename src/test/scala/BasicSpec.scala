import cats.implicits._,cats.effect._
import fs2.Stream
import mx.cinvestav.Chord
class BasicSpec extends munit.CatsEffectSuite {
  test(""){
    val app = Stream
      .iterate(0)(_+1)
      .covary[IO]
      .take(5)
      .filter(_!=3)
      .dropWhile{nId =>
        if(nId==4) !Chord.checkIfBelongs(10,nId,step=6)
        else !Chord.checkIfBelongs(10,nId,step=3)
      }
      .map(_%5)
//      .debug()

    val chordNodeId = app.compile.toList.map(_.headOption)
    chordNodeId.flatMap(IO.println)
  }

  test("Basic: 01"){
    val info = Chord.buildChordInfoById(2,3)
    println(info)
  }

}
