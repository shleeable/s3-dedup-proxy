import timshel.s3dedupproxy.GlobalConfig
import com.jortage.poolmgr.Poolmgr
import org.flywaydb.core.Flyway;

@main def main(): Unit =
  pureconfig.ConfigSource.default.load[GlobalConfig] match {
    case Left(e)       => System.err.println(e.prettyPrint())
    case Right(config) =>
      val ds = org.postgresql.ds.PGSimpleDataSource()
      ds.setServerNames(Array(config.db.host))
      ds.setPortNumbers(Array(config.db.port))
      ds.setUser(config.db.user)
      ds.setPassword(config.db.pass)
      ds.setDatabaseName(config.db.database)

      val flyway = Flyway.configure().dataSource(ds).load()
      flyway.migrate()

      com.jortage.poolmgr.Poolmgr.start(config)
  }



