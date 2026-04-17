package timshel.s3dedupproxy

import cats.effect._
import cats.effect.unsafe.IORuntime
import com.google.common.hash.HashCode;
import java.time.OffsetDateTime
import java.util.UUID
import skunk._
import skunk.data.Completion
import skunk.implicits._
import skunk.codec.all._
import scala.reflect.TypeTest

case class Metadata(
    size: Long,
    eTag: String,
    contentType: String
)

case class Mapping(
    uuid: UUID,
    bucket: String,
    key: String,
    hash: HashCode,
    md5: HashCode,
    size: Long,
    eTag: String,
    contentType: String,
    created: OffsetDateTime,
    updated: OffsetDateTime
)

object Database {
  val log = com.typesafe.scalalogging.Logger(classOf[Database])

  val void = new Decoder[Void] {
    def types                                         = List(skunk.data.Type.void)
    def decode(offset: Int, ss: List[Option[String]]) = Right(Void)
  }

  val hashD: Decoder[HashCode] = bytea.map(HashCode.fromBytes(_))
  val hashE: Encoder[HashCode] = bytea.contramap(_.asBytes())
  val mappingD: Decoder[Mapping] = (uuid ~ text ~ text ~ hashD ~ hashD ~ int8 ~ text ~ text ~ timestamptz ~ timestamptz).map {
    case uu ~ b ~ k ~ h ~ m ~ s ~ e ~ ct ~ c ~ u => Mapping(uu, b, k, h, m, s, e, ct, c, u)
  }
  val metadataD: Decoder[Metadata] = (int8 ~ text ~ text).map { case s ~ e ~ ct => Metadata(s, e, ct) }
  val PAGE_SIZE                    = 100

  def lockKey(hash: HashCode): Long = java.nio.ByteBuffer.wrap(hash.asBytes().take(8)).getLong()
}

case class Database(
    pool: Resource[IO, Session[IO]]
)(implicit runtime: IORuntime) {
  import Database.*

  val lockQ: Query[Long, Void] = sql"SELECT pg_advisory_lock($int8)".query(void)

  val unlockQ: Query[Long, Boolean] = sql"SELECT  pg_advisory_unlock($int8)".query(bool)

  /** Acquires a session scoped advisory lock keyed on the hash.
    * Uses the first 8 bytes of the hash as the lock key for good distribution.
    */
  def withAdvisoryLock[A](hash: HashCode)(body: Session[IO] => IO[A]): IO[A] = {
    val lockKey = Database.lockKey(hash)
    pool.use { s =>
      s.prepare(lockQ)
        .flatMap(_.unique(lockKey))
        .flatMap { _ => body(s) }
        .guarantee {
          s.prepare(unlockQ).flatMap(_.unique(lockKey)).map(_ => ())
        }
    }
  }

  val mappingQ: Query[String *: String *: String *: EmptyTuple, Mapping] =
    sql"""
      SELECT
          file_mappings.uuid, file_mappings.bucket, file_mappings.file_key,
          file_metadata.hash, file_metadata.md5, file_metadata.size, file_metadata.etag, file_metadata.content_type,
          file_mappings.created, file_mappings.updated
        FROM file_mappings
          INNER JOIN file_metadata ON file_metadata.hash = file_mappings.hash
        WHERE user_name = $text
          AND bucket = $text
          AND file_key = $text
    """.query(mappingD)

  def getMapping(user_name: String, bucket: String, file_key: String): IO[Option[Mapping]] =
    pool.use {
      _.prepare(mappingQ)
        .flatMap { ps =>
          ps.option(user_name, bucket, file_key)
        }
    }

  val mappingHashQ: Query[String *: String *: String *: EmptyTuple, HashCode] =
    sql"""
      SELECT hash FROM file_mappings
        WHERE user_name = $text
          AND bucket = $text
          AND file_key = $text
    """.query(hashD)

  def getMappingHash(user_name: String, bucket: String, file_key: String): IO[Option[HashCode]] =
    pool.use {
      _.prepare(mappingHashQ)
        .flatMap { ps =>
          ps.option(user_name, bucket, file_key)
        }
    }

  val putMappingC: Command[(String, String, String, HashCode, HashCode)] =
    sql"""
      INSERT INTO file_mappings (user_name, bucket, file_key, hash) VALUES ($text, $text, $text, $hashE)
        ON CONFLICT (user_name, bucket, file_key) DO UPDATE SET hash = $hashE, updated = now();
    """.command

  def putMapping(user_name: String, bucket: String, file_key: String, hash: HashCode): IO[Completion] = {
    pool.use {
      _.prepare(putMappingC)
        .flatMap { pc =>
          pc.execute(user_name, bucket, file_key, hash, hash)
        }
    }
  }

  def delMappingsC(count: Int): Command[List[(String, String, String)]] = {
    val enc = (text *: text *: text).values.list(count)
    sql"""
      DELETE FROM file_mappings WHERE (user_name, bucket, file_key) = ANY(Array[$enc])
    """.command
  }

  def delMappings(mappings: List[(String, String, String)]): IO[Int] = {
    if (mappings.nonEmpty) {
      pool.use {
        _.prepare(delMappingsC(mappings.size))
          .flatMap { pc => pc.execute(mappings) }
          .map {
            case Completion.Delete(count) => count
            case _                        => throw new AssertionError("delMappings execution should only return Delete")
          }
      }
    } else IO.pure(0)
  }

  def delMappingKeysC(count: Int): Command[(String, String, List[String])] = {
    sql"""
      DELETE FROM file_mappings
        WHERE user_name = $text
          AND bucket = $text
          AND file_key IN (${text.list(count)})
    """.command
  }

  def delMappingKeys(user_name: String, bucket: String, keys: List[String]): IO[Int] = {
    if (keys.nonEmpty) {
      pool.use {
        _.prepare(delMappingKeysC(keys.size))
          .flatMap { pc => pc.execute(user_name, bucket, keys) }
          .map {
            case Completion.Delete(count) => count
            case _                        => throw new AssertionError("delMappingKeys execution should only return Delete")
          }
      }
    } else IO.pure(0)
  }

  def delMapping(user_name: String, bucket: String, file_key: String): IO[Int] =
    delMappingKeys(user_name, bucket, List(file_key))

  val delMappingsBucketC: Command[(String, String)] = {
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text
    """.command
  }

  def delMappings(user_name: String, bucket: String): IO[Int] = {
    pool.use {
      _.prepare(delMappingsBucketC)
        .flatMap { pc => pc.execute(user_name, bucket) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMappings execution should only return Delete")
        }
    }
  }

  val delMappingsPrefixC: Command[(String, String, String)] = {
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text AND starts_with(file_key, $text)
    """.command
  }

  def delMappings(user_name: String, bucket: String, prefix: String): IO[Int] = {
    pool.use {
      _.prepare(delMappingsPrefixC)
        .flatMap { pc => pc.execute(user_name, bucket, prefix) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMappings execution should only return Delete")
        }
    }
  }

  val countMappingsQ: Query[HashCode, Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE hash = $hashE
    """.query(int8)

  def countMappings(hash: HashCode): IO[Long] =
    pool.use {
      _.prepare(countMappingsQ)
        .flatMap { ps =>
          ps.unique(hash)
        }
    }

  val countMappingsBucketQ: Query[(String, String), Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE user_name = $text AND bucket = $text
    """.query(int8)

  def countMappings(user_name: String, bucket: String): IO[Long] =
    pool.use {
      _.prepare(countMappingsBucketQ)
        .flatMap { ps => ps.unique(user_name, bucket) }
    }

  val putMetadataC: Command[(HashCode, HashCode, Long, String, String, String, String)] =
    sql"""
      INSERT INTO file_metadata (hash, md5, size, etag, content_type) VALUES ($hashE, $hashE, $int8, $text, $text)
        ON CONFLICT (hash) DO UPDATE SET etag= $text, content_type = $text, updated = now(), checked = now();
    """.command

  def putMetadata(hash: HashCode, md5: HashCode, size: Long, eTag: String, contentType: String)(
      session: Session[IO]
  ): IO[Completion] = {
    session
      .prepare(putMetadataC)
      .flatMap { pc =>
        pc.execute(hash, md5, size, eTag, contentType, eTag, contentType)
      }
  }

  val checkMetadataQ: Query[HashCode, Metadata] =
    sql"""
      UPDATE file_metadata
        SET checked = now()
        WHERE hash = $hashE
        RETURNING size, etag, content_type
    """.query(metadataD)

  def checkMetadata(hashCode: HashCode)(session: Session[IO]): IO[Option[Metadata]] = {
    session.prepare(checkMetadataQ).flatMap { ps => ps.option(hashCode) }
  }

  def delDanglingMetadatasC(count: Int): Command[(List[HashCode])] = {
    sql"""
      DELETE FROM file_metadata
      USING file_metadata as fm
      LEFT JOIN file_mappings AS map ON fm.hash = map.hash
      WHERE file_metadata.hash = fm.hash
        AND fm.hash IN (${hashE.list(count)})
        AND map.uuid IS NULL
    """.command
  }

  def delDanglingMetadatas(hashes: List[HashCode])(session: Session[IO]): IO[Int] = {
    if (hashes.nonEmpty) {
      session
        .prepare(delDanglingMetadatasC(hashes.size))
        .flatMap { pc => pc.execute(hashes) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMetadatas execution should only return Delete")
        }
    } else IO.pure(0)
  }

  val getDanglingQ: Query[Int, (HashCode, Void)] =
    sql"""
      WITH dangling as (
        SELECT file_metadata.hash
          FROM file_metadata
            LEFT JOIN file_mappings ON file_mappings.hash = file_metadata.hash
          WHERE file_mappings.uuid IS NULL
            AND file_metadata.checked < NOW() - INTERVAL '1 hour'
          ORDER BY file_metadata.checked ASC
          LIMIT $int4
      )
      SELECT hash, pg_advisory_lock(hash_key(hash)) from dangling
    """.query(hashD ~ void)

  def getDangling(limit: Int)(session: Session[IO]): IO[List[HashCode]] =
    session
      .prepare(getDanglingQ)
      .flatMap { pc => pc.stream(limit, limit).map(_._1).compile.toList }

  val releaseLockQ: Query[Void, Void] = sql"SELECT pg_advisory_unlock_all()".query(void)

  def releaseLocks(s: Session[IO]): IO[Unit] = {
    s.prepare(releaseLockQ).flatMap(_.option(skunk.Void)).map(_ => ())
  }

  def locksReleaseBlock[A](body: Session[IO] => IO[A]): IO[A] = {
    pool.use { s =>
      body(s).guarantee {
        releaseLocks(s).map(_ => ())
      }
    }
  }

  def withMaker(maxResults: Int)(mappings: List[Mapping]): (List[Mapping], Option[String]) = {
    if (mappings.size == maxResults) {
      (mappings, mappings.lastOption.map(_.key))
    } else (mappings, None)
  }

  val getContainersQ: Query[String, String] =
    sql"""
      SELECT distinct file_mappings.bucket
        FROM file_mappings
        WHERE user_name = $text
        ORDER BY bucket ASC
    """.query(text)

  def getContainers(user_name: String): IO[List[String]] = {
    pool
      .use {
        _.prepare(getContainersQ)
          .flatMap { pc => pc.stream(user_name, PAGE_SIZE).compile.toList }
      }
  }

  val getMappingsQ: Query[(String, String, String, String, Int), Mapping] =
    sql"""
      SELECT
          file_mappings.uuid, file_mappings.bucket, file_mappings.file_key,
          file_metadata.hash, file_metadata.md5, file_metadata.size, file_metadata.etag, file_metadata.content_type,
          file_mappings.created, file_mappings.updated
        FROM file_mappings
          INNER JOIN file_metadata ON file_metadata.hash = file_mappings.hash
        WHERE user_name = $text
          AND file_mappings.bucket = $text
          AND starts_with(file_mappings.file_key, $text)
          AND file_mappings.file_key > $text
        ORDER BY file_mappings.file_key ASC
        LIMIT $int4
    """.query(mappingD)

  def getMappings(
      user_name: String,
      bucket: String,
      prefix: Option[String] = None,
      marker: Option[String] = None,
      maxResults: Option[Int] = None
  ): IO[(List[Mapping], Option[String])] = {
    val after = marker.getOrElse("")
    val pre   = prefix.getOrElse("")
    val limit = maxResults.getOrElse(PAGE_SIZE)

    pool
      .use {
        _.prepare(getMappingsQ)
          .flatMap { pc => pc.stream((user_name, bucket, pre, after, limit), limit).compile.toList }
      }
      .map(withMaker(limit))
  }

}
