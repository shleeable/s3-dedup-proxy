package timshel.s3dedupproxy

import com.comcast.ip4s.{Host, Port}
import java.net.URI;
import java.nio.file.{Files, Path}
import org.http4s.Uri;
import org.quartz.CronExpression;
import pureconfig.ConfigReader;
import pureconfig.generic.semiauto.deriveReader;
import pureconfig.generic.*;
import scala.deriving.Mirror
import scala.util.Try;

given hostReader: ConfigReader[Host] = ConfigReader.fromStringOpt(Host.fromString)
given portReader: ConfigReader[Port] = implicitly[ConfigReader[Int]].emap { p =>
  Port.fromInt(p).toRight(pureconfig.error.CannotConvert(p.toString, "Port", "Impossible"))
}
given cronReader: ConfigReader[CronExpression] = ConfigReader.fromStringTry { str => Try(org.quartz.CronExpression(str)) }
given uriReader: ConfigReader[Uri] = implicitly[ConfigReader[String]].emap { s =>
  Uri.fromString(s).left.map { _ => pureconfig.error.CannotConvert(s, "Uri", "Impossible") }
}

extension (c: ConfigReader.type)
  inline def derived[A: Mirror.Of: ProductHint: CoproductHint]: ConfigReader[A] = deriveReader[A]

final case class API(
    host: Host = Host.fromString("0.0.0.0").get,
    port: Port = Port.fromInt(23279).get,
) derives ConfigReader

final case class Proxy(
    host: Host = Host.fromString("0.0.0.0").get,
    port: Port = Port.fromInt(23278).get,
    purge: CronExpression = org.quartz.CronExpression("0 0 0 * * ?"),
) derives ConfigReader {
  val uri = new URI("http", null, host.toString, port.value, null, null, null)
}

final case class DBConfig(
    host: Host,
    port: Port = Port.fromInt(5432).get,
    user: String,
    pass: String,
    database: String
) derives ConfigReader

final case class BackendConfig(
    protocol: String,
    endpoint: String,
    virtualHost: Boolean,
    accessKey: String,
    secretKey: String,
    bucket: String,
    publicHost: Uri
) derives ConfigReader

final case class GlobalConfig(
    api: API = API(),
    proxy: Proxy = Proxy(),
    backend: BackendConfig,
    db: DBConfig,
    users: Map[String, String],
    usersFile: Option[Path]
) derives ConfigReader
