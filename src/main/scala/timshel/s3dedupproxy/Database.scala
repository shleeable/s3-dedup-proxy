import cats.effect._
import cats.effect.unsafe.IORuntime
import skunk._
import skunk.data.Completion
import skunk.implicits._
import skunk.codec.all._

import com.google.common.hash.HashCode;

case class Database(
  session: Session[IO]
)(implicit runtime: IORuntime){

  val hashD: Decoder[HashCode] = bytea.map(HashCode.fromBytes(_))
  val hashE: Encoder[HashCode] = bytea.contramap(_.asBytes())

  val mappingHashQ: Query[String ~ String, HashCode] =
    sql"""
      SELECT hash FROM file_mappings
        WHERE user_name = $text
        AND file_key = $text"
    """.query(hashD)

  def getMappingHash(user_name: String, file_key: String): Option[HashCode] =
    session.prepare(mappingHashQ).flatMap { ps =>
      ps.option(user_name, file_key)
    }.unsafeRunSync()

  val putMappingC: Command[(String, String, HashCode, HashCode)] =
    sql"""
      INSERT INTO file_mappings (user_name, file_key, hash) VALUES ($text, $text, $hashE)
        ON CONFLICT (user_name, file_key) DO UPDATE SET hash = $hashE, updated = now();
    """.command

  def putMapping(user_name: String, file_key: String, hash: HashCode): Completion = {
    session.prepare(putMappingC).flatMap { pc =>
      pc.execute(user_name, file_key, hash, hash)
    }.unsafeRunSync()
  }

  val delMappingC: Command[(String, String)] =
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND file_key = $text
    """.command

  def delMapping(user_name: String, file_key: String): Completion = {
    session.prepare(delMappingC).flatMap { pc =>
      pc.execute(user_name, file_key)
    }.unsafeRunSync()
  }

  val countMappingQ: Query[HashCode, Int] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE hash = $hashE
    """.query(int4)

  def countMappings(hash: HashCode): Int =
    session.prepare(countMappingQ).flatMap { ps =>
      ps.unique(hash)
    }.unsafeRunSync()

  def isMapped(hash: HashCode): Boolean = countMappings(hash) > 0

  val putMetadataC: Command[(String, Long, Long)] =
    sql"""
      INSERT INTO file_metadata (file_key, size) VALUES ($text, $int8)
        ON CONFLICT (file_key) DO UPDATE SET size = $int8, updated = now();
    """.command

  def putMetadata(file_key: String, size: Long): Completion = {
    session.prepare(putMetadataC).flatMap { pc =>
      pc.execute(file_key, size, size)
    }.unsafeRunSync()
  }

  val delMetadataC: Command[String] =
    sql"""
      DELETE FROM file_metadata WHERE file_key = $text
    """.command

  def delMetadata(file_key: String): Completion = {
    session.prepare(delMetadataC).flatMap { pc =>
      pc.execute(file_key)
    }.unsafeRunSync()
  }

  val putPendingC: Command[HashCode] =
    sql"""
      INSERT INTO pending_backup (hash) VALUES ($hashE)
    """.command

  def putPending(hash: HashCode): Completion = {
    session.prepare(putPendingC).flatMap { pc =>
      pc.execute(hash)
    }.unsafeRunSync()
  }

  val delPendingC: Command[HashCode] =
    sql"""
      DELETE FROM pending_backup WHERE hash = $hashE
    """.command

  def delPending(hash: HashCode): Completion = {
    session.prepare(delPendingC).flatMap { pc =>
      pc.execute(hash)
    }.unsafeRunSync()
  }

  val putMultipartC: Command[(String, String, String, String)] =
    sql"""
      INSERT INTO multipart_uploads (user_name, file_key, tempfile) VALUES ($text, $text, $text)
        ON CONFLICT (user_name, file_key) DO UPDATE SET tempfile = $text, updated = now();
    """.command

  def putMultipart(user_name: String, file_key: String, temp_file: String): Completion = {
    session.prepare(putMultipartC).flatMap { pc =>
      pc.execute(user_name, file_key, temp_file, temp_file)
    }.unsafeRunSync()
  }

  val multipartFileQ: Query[String ~ String, String] =
    sql"""
      SELECT tempfile FROM multipart_uploads
        WHERE user_name = $text
        AND file_key = $text"
    """.query(text)

  def getMultipartFile(user_name: String, file_key: String): Option[String] =
    session.prepare(multipartFileQ).flatMap { ps =>
      ps.option(user_name, file_key)
    }.unsafeRunSync()

  val multipartKeyQ: Query[String, String] =
    sql"""
      SELECT file_key FROM multipart_uploads WHERE tempfile = $text
    """.query(text)

  def getMultipartKey(tempfile: String): Option[String] =
    session.prepare(multipartKeyQ).flatMap { ps =>
      ps.option(tempfile)
    }.unsafeRunSync()

  val delMultipartC: Command[String] =
    sql"""
      DELETE FROM multipart_uploads WHERE tempfile = $text
    """.command

  def delMultipart(tempfile: String): Completion = {
    session.prepare(delMultipartC).flatMap { pc =>
      pc.execute(tempfile)
    }.unsafeRunSync()
  }

}
