import timshel.s3dedupproxy.GlobalConfig
import com.jortage.poolmgr.Poolmgr

@main def main(): Unit =
  pureconfig.ConfigSource.default.load[GlobalConfig] match {
    case Left(e)       => System.err.println(e.prettyPrint())
    case Right(config) => com.jortage.poolmgr.Poolmgr.start(config)
  }
