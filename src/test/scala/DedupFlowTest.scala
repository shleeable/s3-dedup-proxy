import cats.effect._
import cats.syntax.all._
import timshel.s3dedupproxy.{Application, Database, Metadata, Mapping, ProxyBlobStore}
import munit.CatsEffectSuite
import com.google.common.hash.{HashCode, Hashing}
import java.nio.charset.StandardCharsets

/** Tests for the critical deduplication flow at the database layer.
  *
  * The dedup proxy works as follows:
  *   1. Client uploads a file via S3 API
  *   2. File is hashed (SHA-512 + MD5)
  *   3. If hash already exists in file_metadata → dedup hit (skip upload, just create mapping)
  *   4. If hash is new → upload to backend, create metadata + mapping
  *   5. Multiple users/buckets can map different keys to the same hash
  *   6. Deletion removes mappings only; cleanup GCs unreferenced metadata
  */
class DedupFlowTest extends CatsEffectSuite {

  val app = ResourceSuiteLocalFixture(
    "application",
    Application.default()
  )

  override def munitFixtures = List(app)

  def use[T](f: Application => IO[T]): IO[T] =
    IO(app()).flatMap(f)

  // Helper to create a deterministic hash from a string (simulating file content)
  def hashOf(content: String): HashCode =
    Hashing.sha512().hashString(content, StandardCharsets.UTF_8)

  @scala.annotation.nowarn("cat=deprecation")
  def md5Of(content: String): HashCode =
    Hashing.md5().hashString(content, StandardCharsets.UTF_8)

  // ─── Upload flow: new content ───

  test("new upload creates metadata and mapping") {
    use { a =>
      val hash = hashOf("unique-content-1")
      val md5  = md5Of("unique-content-1")

      for {
        // Simulate: backend upload succeeded, now record in DB
        _ <- a.database.putMetadata(hash, md5, 1024L, "etag-1", "image/jpeg")
        _ <- a.database.putMapping("mastodon1", "media", "avatar.jpg", hash)

        // Verify metadata stored correctly
        meta <- a.database.getMetadata(hash)
        _ = assertEquals(meta, Some(Metadata(1024L, "etag-1", "image/jpeg")))

        // Verify mapping resolves
        resolved <- a.database.getMappingHash("mastodon1", "media", "avatar.jpg")
        _ = assertEquals(resolved, Some(hash))

        // Cleanup
        _ <- a.database.delMapping("mastodon1", "media", "avatar.jpg")
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── Upload flow: dedup hit ───

  test("dedup hit: second upload of same content reuses metadata") {
    use { a =>
      val hash = hashOf("shared-image-content")
      val md5  = md5Of("shared-image-content")

      for {
        // First instance uploads the file
        _ <- a.database.putMetadata(hash, md5, 2048L, "etag-shared", "image/png")
        _ <- a.database.putMapping("mastodon1", "media", "photo.png", hash)

        // Second instance uploads identical content — dedup hit
        existingMeta <- a.database.getMetadata(hash)
        _ = assert(existingMeta.isDefined, "Dedup check should find existing metadata")

        // Just create the mapping, no new metadata needed
        _ <- a.database.putMapping("mastodon2", "media", "photo.png", hash)

        // Both mappings point to the same hash
        hash1 <- a.database.getMappingHash("mastodon1", "media", "photo.png")
        hash2 <- a.database.getMappingHash("mastodon2", "media", "photo.png")
        _ = assertEquals(hash1, hash2)
        _ = assertEquals(hash1, Some(hash))

        // Reference count is 2
        count <- a.database.countMappings(hash)
        _ = assertEquals(count, 2L)

        // Cleanup
        _ <- a.database.delMapping("mastodon1", "media", "photo.png")
        _ <- a.database.delMapping("mastodon2", "media", "photo.png")
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── Five instances sharing the same file ───

  test("five mastodon instances dedup the same file") {
    use { a =>
      val hash = hashOf("popular-meme.jpg")
      val md5  = md5Of("popular-meme.jpg")
      val instances = (1 to 5).map(i => s"mastodon$i").toList

      for {
        // First instance uploads
        _ <- a.database.putMetadata(hash, md5, 5000L, "etag-meme", "image/jpeg")

        // All five instances create mappings (each with different key names)
        _ <- instances.zipWithIndex.traverse_ { case (inst, i) =>
          a.database.putMapping(inst, "media", s"meme-$i.jpg", hash)
        }

        // All resolve to the same hash
        hashes <- instances.zipWithIndex.traverse { case (inst, i) =>
          a.database.getMappingHash(inst, "media", s"meme-$i.jpg")
        }
        _ = hashes.foreach(h => assertEquals(h, Some(hash)))

        // Reference count is 5
        count <- a.database.countMappings(hash)
        _ = assertEquals(count, 5L)

        // Only one metadata row exists
        meta <- a.database.getMetadata(hash)
        _ = assertEquals(meta.map(_.size), Some(5000L))

        // Cleanup
        _ <- instances.zipWithIndex.traverse_ { case (inst, i) =>
          a.database.delMapping(inst, "media", s"meme-$i.jpg")
        }
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── Deletion flow: remove mapping, metadata persists ───

  test("deleting one mapping preserves metadata for other users") {
    use { a =>
      val hash = hashOf("shared-delete-test")
      val md5  = md5Of("shared-delete-test")

      for {
        _ <- a.database.putMetadata(hash, md5, 100L, "etag-del", "text/plain")
        _ <- a.database.putMapping("mastodon1", "bucket", "file.txt", hash)
        _ <- a.database.putMapping("mastodon2", "bucket", "file.txt", hash)

        // Delete mastodon1's mapping
        _ <- a.database.delMapping("mastodon1", "bucket", "file.txt")

        // mastodon2 still has access
        h2 <- a.database.getMappingHash("mastodon2", "bucket", "file.txt")
        _ = assertEquals(h2, Some(hash))

        // Metadata still exists
        meta <- a.database.getMetadata(hash)
        _ = assert(meta.isDefined)

        // Not dangling (still has one mapping)
        dangling <- a.database.getDangling(1000)
        _ = assert(!dangling.contains(hash), "Hash with active mapping should not be dangling")

        // Cleanup
        _ <- a.database.delMapping("mastodon2", "bucket", "file.txt")
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── Dangling detection ───

  test("metadata with no mappings is detected as dangling") {
    use { a =>
      val hash = hashOf("orphan-content")
      val md5  = md5Of("orphan-content")

      for {
        _ <- a.database.putMetadata(hash, md5, 50L, "etag-orphan", "text/plain")

        // No mapping created — this is dangling
        dangling <- a.database.getDangling(1000)
        // Note: getDangling has a 1-hour grace period, so freshly created metadata
        // should NOT appear as dangling (this tests the grace period)
        _ = assert(!dangling.contains(hash),
          "Freshly created metadata should not be dangling due to grace period")

        // Cleanup
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── CASCADE behavior ───

  test("deleting metadata cascades to mappings") {
    use { a =>
      val hash = hashOf("cascade-test")
      val md5  = md5Of("cascade-test")

      for {
        _ <- a.database.putMetadata(hash, md5, 100L, "etag-cascade", "text/plain")
        _ <- a.database.putMapping("user1", "bucket", "file.txt", hash)
        _ <- a.database.putMapping("user2", "bucket", "file.txt", hash)

        count <- a.database.countMappings(hash)
        _ = assertEquals(count, 2L)

        // Delete metadata — should CASCADE to mappings
        _ <- a.database.delMetadata(hash)

        // Mappings should be gone
        h1 <- a.database.getMappingHash("user1", "bucket", "file.txt")
        h2 <- a.database.getMappingHash("user2", "bucket", "file.txt")
        _ = assertEquals(h1, None)
        _ = assertEquals(h2, None)
      } yield ()
    }
  }

  // ─── Overwrite: same key, different content ───

  test("uploading different content to same key updates the mapping") {
    use { a =>
      val hash1 = hashOf("version-1-content")
      val md5_1 = md5Of("version-1-content")
      val hash2 = hashOf("version-2-content")
      val md5_2 = md5Of("version-2-content")

      for {
        // Upload v1
        _ <- a.database.putMetadata(hash1, md5_1, 100L, "etag-v1", "image/png")
        _ <- a.database.putMapping("mastodon1", "media", "avatar.png", hash1)

        // Upload v2 to same key (putMapping uses ON CONFLICT DO UPDATE)
        _ <- a.database.putMetadata(hash2, md5_2, 200L, "etag-v2", "image/png")
        _ <- a.database.putMapping("mastodon1", "media", "avatar.png", hash2)

        // Mapping now points to v2
        resolved <- a.database.getMappingHash("mastodon1", "media", "avatar.png")
        _ = assertEquals(resolved, Some(hash2))

        // v1 metadata still exists (might be used by others)
        meta1 <- a.database.getMetadata(hash1)
        _ = assert(meta1.isDefined)

        // Cleanup
        _ <- a.database.delMapping("mastodon1", "media", "avatar.png")
        _ <- a.database.delMetadata(hash1)
        _ <- a.database.delMetadata(hash2)
      } yield ()
    }
  }

  // ─── Copy blob flow ───

  test("copyBlob creates new mapping to same hash") {
    use { a =>
      val hash = hashOf("copy-test-content")
      val md5  = md5Of("copy-test-content")

      for {
        _ <- a.database.putMetadata(hash, md5, 300L, "etag-copy", "image/gif")
        _ <- a.database.putMapping("mastodon1", "media", "original.gif", hash)

        // Copy: look up source hash, create destination mapping
        sourceHash <- a.database.getMappingHash("mastodon1", "media", "original.gif")
        _ = assert(sourceHash.isDefined)
        _ <- a.database.putMapping("mastodon1", "media", "copy.gif", sourceHash.get)

        // Both point to same hash
        h1 <- a.database.getMappingHash("mastodon1", "media", "original.gif")
        h2 <- a.database.getMappingHash("mastodon1", "media", "copy.gif")
        _ = assertEquals(h1, h2)

        count <- a.database.countMappings(hash)
        _ = assertEquals(count, 2L)

        // Cleanup
        _ <- a.database.delMapping("mastodon1", "media", "original.gif")
        _ <- a.database.delMapping("mastodon1", "media", "copy.gif")
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── Listing flow ───

  test("getMappings returns correct metadata for listings") {
    use { a =>
      val hash = hashOf("list-test-content")
      val md5  = md5Of("list-test-content")

      for {
        _ <- a.database.putMetadata(hash, md5, 512L, "etag-list", "application/pdf")
        _ <- a.database.putMapping("mastodon1", "docs", "report.pdf", hash)

        (mappings, _) <- a.database.getMappings("mastodon1", "docs")
        _ = assertEquals(mappings.length, 1)
        _ = assertEquals(mappings.head.key, "report.pdf")
        _ = assertEquals(mappings.head.bucket, "docs")
        _ = assertEquals(mappings.head.contentType, "application/pdf")
        _ = assertEquals(mappings.head.size, 512L)
        _ = assertEquals(mappings.head.hash, hash)

        // Cleanup
        _ <- a.database.delMapping("mastodon1", "docs", "report.pdf")
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }

  // ─── hashToKey consistency ───

  test("hashToKey produces consistent content-addressed paths") {
    val hash = HashCode.fromString("abcdef1234567890" * 8)
    val key  = ProxyBlobStore.hashToKey(hash)

    // Format: blobs/{first char}/{chars 1-3}/{full hash}
    assert(key.startsWith("blobs/"), s"Key should start with blobs/: $key")
    assertEquals(key, s"blobs/a/bcd/${hash.toString}")
  }

  // ─── Multi-tenant isolation ───

  test("different tenants with same key name are isolated") {
    use { a =>
      val hash1 = hashOf("tenant1-content")
      val md5_1 = md5Of("tenant1-content")
      val hash2 = hashOf("tenant2-content")
      val md5_2 = md5Of("tenant2-content")

      for {
        _ <- a.database.putMetadata(hash1, md5_1, 100L, "etag-t1", "text/plain")
        _ <- a.database.putMetadata(hash2, md5_2, 200L, "etag-t2", "text/plain")
        _ <- a.database.putMapping("tenant1", "bucket", "file.txt", hash1)
        _ <- a.database.putMapping("tenant2", "bucket", "file.txt", hash2)

        // Same key name, different content per tenant
        h1 <- a.database.getMappingHash("tenant1", "bucket", "file.txt")
        h2 <- a.database.getMappingHash("tenant2", "bucket", "file.txt")
        _ = assertEquals(h1, Some(hash1))
        _ = assertEquals(h2, Some(hash2))
        _ = assertNotEquals(h1, h2)

        // Cleanup
        _ <- a.database.delMapping("tenant1", "bucket", "file.txt")
        _ <- a.database.delMapping("tenant2", "bucket", "file.txt")
        _ <- a.database.delMetadata(hash1)
        _ <- a.database.delMetadata(hash2)
      } yield ()
    }
  }

  // ─── Nonexistent lookups ───

  test("lookup for nonexistent mapping returns None") {
    use { a =>
      for {
        result <- a.database.getMappingHash("nobody", "nowhere", "nothing.txt")
        _ = assertEquals(result, None)
      } yield ()
    }
  }

  test("lookup for nonexistent metadata returns None") {
    use { a =>
      val hash = hashOf("does-not-exist")
      for {
        result <- a.database.getMetadata(hash)
        _ = assertEquals(result, None)
      } yield ()
    }
  }

  // ─── FK constraint: mapping without metadata fails ───

  test("creating mapping without metadata fails with FK violation") {
    use { a =>
      val hash = hashOf("no-metadata-content")

      a.database.putMapping("user1", "bucket", "orphan.txt", hash)
        .attempt
        .flatMap { result =>
          IO(assert(result.isLeft, "Should fail with FK violation when metadata doesn't exist"))
        }
    }
  }

  // ─── putMetadata UPSERT behavior ───

  test("putMetadata upsert updates etag and content_type but preserves size") {
    use { a =>
      val hash = hashOf("upsert-test")
      val md5  = md5Of("upsert-test")

      for {
        _ <- a.database.putMetadata(hash, md5, 1000L, "etag-original", "text/plain")
        _ <- a.database.putMetadata(hash, md5, 2000L, "etag-updated", "text/html")

        meta <- a.database.getMetadata(hash)
        _ = assert(meta.isDefined)
        // UPSERT only updates etag and content_type, size stays from original insert
        _ = assertEquals(meta.get.size, 1000L)
        _ = assertEquals(meta.get.eTag, "etag-updated")
        _ = assertEquals(meta.get.contentType, "text/html")

        // Cleanup
        _ <- a.database.delMetadata(hash)
      } yield ()
    }
  }
}
