import cats.effect._
import skunk._
import skunk.implicits._
import skunk.codec.all._
import natchez.Trace.Implicits.noop // (1)

object Db {

  val session: Resource[IO, Session[IO]] =
    Session.single( // (2)
      host = "localhost",
      port = 5432,
      user = "s3dedup",
      database = "s3dedup",
      password = Some("s3dedup")
    )

  val a: Query[Void, String] =
    sql"SELECT id FROM test".query(varchar)

  def selectId(): IO[List[String]] =
    session.use { s =>
      s.execute(a)
    }

}
