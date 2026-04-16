import cats.effect._
import cats.effect.unsafe.IORuntime
import timshel.s3dedupproxy.{Application, Database, Metadata}
import munit.CatsEffectSuite
import skunk._

import com.google.common.hash.HashCode;

class PgIntegrationTests extends CatsEffectSuite {

  val database = ResourceSuiteLocalFixture(
    "application",
    Application
      .config()
      .flatMap(config =>
        Application
          .pool(config)
          .map(pool => Database(pool)(using IORuntime.global))
          .preAllocate {
            Application.migration(config.db)
          }
      )
  )

  override def munitFixtures = List(database)

  def use[T](f: Database => IO[T]): IO[T] = {
    IO(database()).flatMap(f)
  }

  // Helper to bypass the Dangling 1h protection
  def oldMetadatas(pool: Resource[IO, Session[IO]]): IO[Int] = {
    import skunk.implicits._
    import skunk.data.Completion

    val cmd = sql"""
      UPDATE file_metadata SET created = created - INTERVAL '2 hour'
    """.command

    pool.use {
      _.prepare(cmd)
        .flatMap { pc => pc.execute(skunk.Void) }
        .map {
          case Completion.Update(count) => count
          case _                        => throw new AssertionError("oldMetadatas execution should only return Update")
        }
    }
  }

  test("Query file_metadata run against db") {
    use { database =>
      val hashCode  = HashCode.fromInt(12);
      val hashCode2 = HashCode.fromInt(24);

      for {
        _ <- database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- database.putMetadata(hashCode, hashCode, 12L, "B", "CT")
        _ <- assertIO(database.getMetadata(hashCode), Some(Metadata(10L, "B", "CT")))
        _ <- database.putMetadata(hashCode2, hashCode2, 12L, "B", "CT")
        _ <- database.putMapping("A", "bucket", "B", hashCode2)
        _ <- assertIO(database.getDangling(1000), List.empty)
        _ <- assertIO(oldMetadatas(database.pool), 2)
        _ <- assertIO(database.getDangling(1000), List(hashCode))
        _ <- assertIO(database.delDanglingMetadatas(List(hashCode, hashCode2)), 1) // Check that hashCode2 is not deleted
        _ <- assertIO(database.delMappings("A", "bucket"), 1)
        _ <- assertIO(database.getDangling(1000), List(hashCode2))
        _ <- assertIO(database.delDanglingMetadatas(List(hashCode2)), 1)
        _ <- assertIO(database.getMetadata(hashCode), None)
      } yield ()
    }
  }

  test("Query file_mappings run against db") {
    use { database =>
      val hashCode = HashCode.fromInt(12);

      for {
        _ <- database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(database.getMappings("A", "bucket").map(_._1.map(_.hash)), List(hashCode))
        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(database.getMappingHash("A", "bucket", "B"), Some(hashCode))
        _ <- assertIO(database.getMapping("A", "bucket", "B").map(_.map(_.hash)), Some(hashCode))
        _ <- assertIO(database.countMappings(hashCode), 1L)
        _ <- assertIO(database.delMapping("A", "bucket", "B"), 1)
        _ <- assertIO(database.countMappings(hashCode), 0L)

        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- database.putMapping("A", "bucket", "C", hashCode)
        _ <- assertIO(database.delMappingKeys("A", "bucket", List("B", "C")), 2)

        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- database.putMapping("B", "bucket", "B", hashCode)
        _ <- assertIO(database.countMappings("A", "bucket"), 1L)
        _ <- assertIO(database.delMappings(List(("A", "bucket", "B"), ("B", "bucket", "B"))), 2)

        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- database.putMapping("A", "bucket", "C", hashCode)
        _ <- assertIO(database.delMappings("A", "bucket"), 2)

        _ <- database.putMapping("A", "bucket", "prefix1", hashCode)
        _ <- database.putMapping("A", "bucket", "prefix2", hashCode)
        _ <- database.putMapping("A", "bucket", "wrefix2", hashCode)
        _ <- assertIO(database.countMappings("A", "bucket"), 3L)
        _ <- assertIO(database.delMappings("A", "bucket", "prefix"), 2)
        _ <- assertIO(database.delMappings("A", "bucket", "wrefix"), 1)

        _ <- assertIO(database.delDanglingMetadatas(List(hashCode)), 1)
        _ <- assertIO(database.countMappings(hashCode), 0L)
      } yield ()
    }
  }
}
