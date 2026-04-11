import cats.effect._
import cats.syntax.all._
import timshel.s3dedupproxy.{Application, Database, Metadata, Mapping, ProxyBlobStore}
import munit.CatsEffectSuite
import com.google.common.hash.{HashCode, Hashing}
import java.nio.charset.StandardCharsets
import scala.util.Random

/** Full E2E test simulating 5 Mastodon instances uploading, downloading,
  * copying, and deleting files through the dedup proxy's database layer.
  *
  * Tests every critical Database function and simulates realistic
  * multi-tenant workloads including deduplication, overwrite, and cleanup.
  */
class E2EProxyTest extends CatsEffectSuite {

  val app = ResourceSuiteLocalFixture("application", Application.default())
  override def munitFixtures = List(app)
  def use[T](f: Application => IO[T]): IO[T] = IO(app()).flatMap(f)

  def sha512(s: String): HashCode = Hashing.sha512().hashString(s, StandardCharsets.UTF_8)
  @scala.annotation.nowarn("cat=deprecation")
  def md5(s: String): HashCode    = Hashing.md5().hashString(s, StandardCharsets.UTF_8)

  val instances = (1 to 5).map(i => s"mastodon$i").toList

  // ═══════════════════════════════════════════════════════════════════
  // Database function tests — every critical function
  // ═══════════════════════════════════════════════════════════════════

  // --- getMappingHash ---

  test("getMappingHash returns None for nonexistent mapping") {
    use { a =>
      a.database.getMappingHash("nobody", "bucket", "file.txt").map(r => assertEquals(r, None))
    }
  }

  test("getMappingHash returns correct hash after putMapping") {
    use { a =>
      val h = sha512("gmh-test")
      for {
        _ <- a.database.putMetadata(h, md5("gmh-test"), 10L, "e", "text/plain")
        _ <- a.database.putMapping("u1", "b1", "f1", h)
        r <- a.database.getMappingHash("u1", "b1", "f1")
        _ = assertEquals(r, Some(h))
        _ <- a.database.delMapping("u1", "b1", "f1")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("getMappingHash is scoped by user/bucket/key") {
    use { a =>
      val h = sha512("scope-test")
      for {
        _ <- a.database.putMetadata(h, md5("scope-test"), 10L, "e", "t")
        _ <- a.database.putMapping("u1", "b1", "f1", h)
        // Different user
        r1 <- a.database.getMappingHash("u2", "b1", "f1")
        _ = assertEquals(r1, None)
        // Different bucket
        r2 <- a.database.getMappingHash("u1", "b2", "f1")
        _ = assertEquals(r2, None)
        // Different key
        r3 <- a.database.getMappingHash("u1", "b1", "f2")
        _ = assertEquals(r3, None)
        _ <- a.database.delMapping("u1", "b1", "f1")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // --- putMapping UPSERT ---

  test("putMapping upsert changes hash for same key") {
    use { a =>
      val h1 = sha512("v1"); val h2 = sha512("v2")
      for {
        _ <- a.database.putMetadata(h1, md5("v1"), 10L, "e1", "t")
        _ <- a.database.putMetadata(h2, md5("v2"), 20L, "e2", "t")
        _ <- a.database.putMapping("u", "b", "f", h1)
        r1 <- a.database.getMappingHash("u", "b", "f")
        _ = assertEquals(r1, Some(h1))
        _ <- a.database.putMapping("u", "b", "f", h2)
        r2 <- a.database.getMappingHash("u", "b", "f")
        _ = assertEquals(r2, Some(h2))
        _ <- a.database.delMapping("u", "b", "f")
        _ <- a.database.delMetadata(h1)
        _ <- a.database.delMetadata(h2)
      } yield ()
    }
  }

  // --- putMetadata ---

  test("putMetadata stores and retrieves correctly") {
    use { a =>
      val h = sha512("pm-test")
      for {
        _ <- a.database.putMetadata(h, md5("pm-test"), 999L, "etag-pm", "image/png")
        m <- a.database.getMetadata(h)
        _ = assertEquals(m, Some(Metadata(999L, "etag-pm", "image/png")))
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("putMetadata upsert updates etag and content_type only") {
    use { a =>
      val h = sha512("upsert-meta")
      for {
        _ <- a.database.putMetadata(h, md5("upsert-meta"), 100L, "old-etag", "text/plain")
        _ <- a.database.putMetadata(h, md5("upsert-meta"), 200L, "new-etag", "text/html")
        m <- a.database.getMetadata(h)
        _ = assertEquals(m.get.size, 100L, "size should not change on upsert")
        _ = assertEquals(m.get.eTag, "new-etag")
        _ = assertEquals(m.get.contentType, "text/html")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("getMetadata returns None for nonexistent hash") {
    use { a =>
      a.database.getMetadata(sha512("nonexistent")).map(r => assertEquals(r, None))
    }
  }

  // --- delMapping / delMappingKeys / delMappings ---

  test("delMapping returns 0 for nonexistent mapping") {
    use { a =>
      a.database.delMapping("nobody", "nowhere", "nothing").map(r => assertEquals(r, 0))
    }
  }

  test("delMappingKeys with empty list returns 0") {
    use { a =>
      a.database.delMappingKeys("u", "b", Nil).map(r => assertEquals(r, 0))
    }
  }

  test("delMappings with empty list returns 0") {
    use { a =>
      a.database.delMappings(Nil).map(r => assertEquals(r, 0))
    }
  }

  test("delMappings by bucket deletes all keys in bucket") {
    use { a =>
      val h = sha512("bucket-del")
      for {
        _ <- a.database.putMetadata(h, md5("bucket-del"), 10L, "e", "t")
        _ <- a.database.putMapping("u", "b", "f1", h)
        _ <- a.database.putMapping("u", "b", "f2", h)
        _ <- a.database.putMapping("u", "b", "f3", h)
        c <- a.database.delMappings("u", "b")
        _ = assertEquals(c, 3)
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("delMappings by prefix only deletes matching keys") {
    use { a =>
      val h = sha512("prefix-del")
      for {
        _ <- a.database.putMetadata(h, md5("prefix-del"), 10L, "e", "t")
        _ <- a.database.putMapping("u", "b", "media/photo1.jpg", h)
        _ <- a.database.putMapping("u", "b", "media/photo2.jpg", h)
        _ <- a.database.putMapping("u", "b", "cache/thumb1.jpg", h)
        c <- a.database.delMappings("u", "b", "media/")
        _ = assertEquals(c, 2)
        remaining <- a.database.countMappings("u", "b")
        _ = assertEquals(remaining, 1L)
        _ <- a.database.delMappings("u", "b")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // --- countMappings ---

  test("countMappings by hash tracks reference count") {
    use { a =>
      val h = sha512("refcount")
      for {
        _ <- a.database.putMetadata(h, md5("refcount"), 10L, "e", "t")
        c0 <- a.database.countMappings(h)
        _ = assertEquals(c0, 0L)
        _ <- a.database.putMapping("u1", "b", "f", h)
        c1 <- a.database.countMappings(h)
        _ = assertEquals(c1, 1L)
        _ <- a.database.putMapping("u2", "b", "f", h)
        c2 <- a.database.countMappings(h)
        _ = assertEquals(c2, 2L)
        _ <- a.database.delMapping("u1", "b", "f")
        c3 <- a.database.countMappings(h)
        _ = assertEquals(c3, 1L)
        _ <- a.database.delMapping("u2", "b", "f")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("countMappings by bucket is scoped to user") {
    use { a =>
      val h = sha512("bucket-count")
      for {
        _ <- a.database.putMetadata(h, md5("bucket-count"), 10L, "e", "t")
        _ <- a.database.putMapping("u1", "b", "f1", h)
        _ <- a.database.putMapping("u1", "b", "f2", h)
        _ <- a.database.putMapping("u2", "b", "f1", h)
        c1 <- a.database.countMappings("u1", "b")
        _ = assertEquals(c1, 2L)
        c2 <- a.database.countMappings("u2", "b")
        _ = assertEquals(c2, 1L)
        _ <- a.database.delMappings("u1", "b")
        _ <- a.database.delMappings("u2", "b")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // --- getDangling ---

  test("getDangling respects grace period") {
    use { a =>
      val h = sha512("grace-period")
      for {
        _ <- a.database.putMetadata(h, md5("grace-period"), 10L, "e", "t")
        d <- a.database.getDangling(1000)
        _ = assert(!d.contains(h), "Fresh metadata should not be dangling due to 1h grace period")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("getDangling excludes metadata with active mappings") {
    use { a =>
      val h = sha512("not-dangling")
      for {
        _ <- a.database.putMetadata(h, md5("not-dangling"), 10L, "e", "t")
        _ <- a.database.putMapping("u", "b", "f", h)
        d <- a.database.getDangling(1000)
        _ = assert(!d.contains(h))
        _ <- a.database.delMapping("u", "b", "f")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // --- getContainers ---

  test("getContainers returns distinct buckets for user") {
    use { a =>
      val h = sha512("containers")
      for {
        _ <- a.database.putMetadata(h, md5("containers"), 10L, "e", "t")
        _ <- a.database.putMapping("u", "bucket-a", "f1", h)
        _ <- a.database.putMapping("u", "bucket-b", "f1", h)
        _ <- a.database.putMapping("u", "bucket-a", "f2", h) // duplicate bucket
        cs <- a.database.getContainers("u")
        _ = assertEquals(cs.sorted, List("bucket-a", "bucket-b"))
        _ <- a.database.delMappings("u", "bucket-a")
        _ <- a.database.delMappings("u", "bucket-b")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // --- getMappings with pagination ---

  test("getMappings returns ordered results with pagination marker") {
    use { a =>
      val h = sha512("pagination")
      for {
        _ <- a.database.putMetadata(h, md5("pagination"), 10L, "e", "t")
        _ <- (1 to 5).toList.traverse_(i => a.database.putMapping("u", "b", f"file$i%03d.txt", h))
        (page1, marker1) <- a.database.getMappings("u", "b", maxResults = Some(3))
        _ = assertEquals(page1.length, 3)
        _ = assertEquals(page1.map(_.key), List("file001.txt", "file002.txt", "file003.txt"))
        _ = assert(marker1.isDefined, "Should have pagination marker")
        (page2, marker2) <- a.database.getMappings("u", "b", marker = marker1, maxResults = Some(3))
        _ = assertEquals(page2.length, 2)
        _ = assertEquals(page2.map(_.key), List("file004.txt", "file005.txt"))
        _ = assertEquals(marker2, None, "Last page should have no marker")
        _ <- a.database.delMappings("u", "b")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("getMappings with prefix filters correctly") {
    use { a =>
      val h = sha512("prefix-list")
      for {
        _ <- a.database.putMetadata(h, md5("prefix-list"), 10L, "e", "t")
        _ <- a.database.putMapping("u", "b", "media/photo.jpg", h)
        _ <- a.database.putMapping("u", "b", "media/video.mp4", h)
        _ <- a.database.putMapping("u", "b", "cache/thumb.jpg", h)
        (results, _) <- a.database.getMappings("u", "b", prefix = Some("media/"))
        _ = assertEquals(results.map(_.key).sorted, List("media/photo.jpg", "media/video.mp4"))
        _ <- a.database.delMappings("u", "b")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // --- CASCADE behavior ---

  test("CASCADE: deleting metadata removes all mappings across users") {
    use { a =>
      val h = sha512("cascade-multi")
      for {
        _ <- a.database.putMetadata(h, md5("cascade-multi"), 10L, "e", "t")
        _ <- instances.traverse_(inst => a.database.putMapping(inst, "b", "shared.jpg", h))
        c <- a.database.countMappings(h)
        _ = assertEquals(c, 5L)
        _ <- a.database.delMetadata(h)
        // All mappings should be gone
        results <- instances.traverse(inst => a.database.getMappingHash(inst, "b", "shared.jpg"))
        _ = results.foreach(r => assertEquals(r, None))
      } yield ()
    }
  }

  // --- FK constraint ---

  test("FK: putMapping fails when metadata does not exist") {
    use { a =>
      a.database.putMapping("u", "b", "orphan.txt", sha512("no-metadata"))
        .attempt
        .map(r => assert(r.isLeft, "Should fail with FK violation"))
    }
  }

  // --- hashToKey ---

  test("hashToKey format is blobs/{char0}/{char1-3}/{fullhash}") {
    val h = HashCode.fromString("abcdef" * (128 / 6) + "ab")
    val k = ProxyBlobStore.hashToKey(h)
    assert(k.startsWith("blobs/a/bcd/"), s"Unexpected prefix: $k")
    assert(k.endsWith(h.toString), s"Should end with full hash: $k")
  }

  test("hashToKey is deterministic") {
    val h = sha512("deterministic")
    assertEquals(ProxyBlobStore.hashToKey(h), ProxyBlobStore.hashToKey(h))
  }

  // ═══════════════════════════════════════════════════════════════════
  // E2E: 5 Mastodon instances — full lifecycle simulation
  // ═══════════════════════════════════════════════════════════════════

  /** Generate a set of "files" — some unique per instance, some shared across all.
    * Returns (contentId, fileName, contentType, size) tuples.
    */
  def generateFiles(seed: Int): List[(String, String, String, Long)] = {
    val rng = new Random(seed)
    // 10 shared files (same content across instances — the dedup case)
    val shared = (1 to 10).map { i =>
      (s"shared-content-$i", s"media/shared-$i.jpg", "image/jpeg", (rng.nextInt(10000) + 100).toLong)
    }
    // 5 unique files per instance
    val unique = instances.flatMap { inst =>
      (1 to 5).map { i =>
        (s"unique-$inst-$i", s"media/$inst-unique-$i.png", "image/png", (rng.nextInt(5000) + 50).toLong)
      }
    }
    (shared ++ unique).toList
  }

  test("E2E: 5 instances upload shared + unique files, dedup works correctly") {
    use { a =>
      val files = generateFiles(42)
      val sharedFiles = files.filter(_._1.startsWith("shared-"))
      val uniqueFiles = files.filter(_._1.startsWith("unique-"))

      // Pre-compute hashes for all unique content
      val contentHashes: Map[String, (HashCode, HashCode)] = files.map { case (contentId, _, _, _) =>
        contentId -> (sha512(contentId), md5(contentId))
      }.toMap

      for {
        // Phase 1: Create metadata for all unique content
        _ <- files.distinctBy(_._1).traverse_ { case (contentId, _, ct, size) =>
          val (h, m) = contentHashes(contentId)
          a.database.putMetadata(h, m, size, s"etag-$contentId", ct)
        }

        // Phase 2: Each instance uploads all shared files + their unique files
        _ <- instances.traverse_ { inst =>
          val instFiles = sharedFiles ++ uniqueFiles.filter(_._1.contains(inst))
          instFiles.traverse_ { case (contentId, fileName, _, _) =>
            val (h, _) = contentHashes(contentId)
            a.database.putMapping(inst, "media-bucket", fileName, h)
          }
        }

        // Verify: each shared file hash has 5 mappings (one per instance)
        sharedCounts <- sharedFiles.distinctBy(_._1).traverse { case (contentId, _, _, _) =>
          val (h, _) = contentHashes(contentId)
          a.database.countMappings(h)
        }
        _ = sharedCounts.foreach(c => assertEquals(c, 5L, "Each shared file should have 5 mappings"))

        // Verify: each unique file hash has 1 mapping
        uniqueCounts <- uniqueFiles.traverse { case (contentId, _, _, _) =>
          val (h, _) = contentHashes(contentId)
          a.database.countMappings(h)
        }
        _ = uniqueCounts.foreach(c => assertEquals(c, 1L, "Each unique file should have 1 mapping"))

        // Verify: total metadata rows = 10 shared + 25 unique = 35
        // (dedup means shared content only has 1 metadata row)
        allHashes = files.distinctBy(_._1).map(f => contentHashes(f._1)._1)
        metaResults <- allHashes.traverse(h => a.database.getMetadata(h))
        _ = assertEquals(metaResults.count(_.isDefined), 35)

        // Verify: each instance can resolve their files
        _ <- instances.traverse_ { inst =>
          val instShared = sharedFiles.map(_._2)
          instShared.traverse_ { fileName =>
            a.database.getMappingHash(inst, "media-bucket", fileName).map { r =>
              assert(r.isDefined, s"$inst should resolve $fileName")
            }
          }
        }

        // Verify: listing per instance returns correct count
        _ <- instances.traverse_ { inst =>
          a.database.countMappings(inst, "media-bucket").map { c =>
            assertEquals(c, 15L, s"$inst should have 10 shared + 5 unique = 15 files")
          }
        }

        // Phase 3: Instance 1 deletes all their files
        _ <- a.database.delMappings("mastodon1", "media-bucket")

        // Shared files still accessible by other instances
        _ <- (2 to 5).toList.traverse_ { i =>
          sharedFiles.traverse_ { case (contentId, fileName, _, _) =>
            a.database.getMappingHash(s"mastodon$i", "media-bucket", fileName).map { r =>
              assert(r.isDefined, s"mastodon$i should still resolve $fileName after mastodon1 deletion")
            }
          }
        }

        // Shared content ref count dropped from 5 to 4
        sharedCountsAfter <- sharedFiles.distinctBy(_._1).traverse { case (contentId, _, _, _) =>
          val (h, _) = contentHashes(contentId)
          a.database.countMappings(h)
        }
        _ = sharedCountsAfter.foreach(c => assertEquals(c, 4L))

        // mastodon1's unique files now have 0 mappings
        m1Unique = uniqueFiles.filter(_._1.contains("mastodon1"))
        m1Counts <- m1Unique.traverse { case (contentId, _, _, _) =>
          val (h, _) = contentHashes(contentId)
          a.database.countMappings(h)
        }
        _ = m1Counts.foreach(c => assertEquals(c, 0L))

        // Phase 4: mastodon2 overwrites a shared file with new content
        newHash = sha512("replaced-content")
        newMd5 = md5("replaced-content")
        _ <- a.database.putMetadata(newHash, newMd5, 777L, "etag-new", "image/webp")
        _ <- a.database.putMapping("mastodon2", "media-bucket", sharedFiles.head._2, newHash)

        // mastodon2 now sees new content, others still see original
        m2Hash <- a.database.getMappingHash("mastodon2", "media-bucket", sharedFiles.head._2)
        _ = assertEquals(m2Hash, Some(newHash))
        m3Hash <- a.database.getMappingHash("mastodon3", "media-bucket", sharedFiles.head._2)
        _ = assertEquals(m3Hash, Some(contentHashes(sharedFiles.head._1)._1))

        // Phase 5: Copy operation — mastodon3 copies a file
        srcFile = sharedFiles(1)._2
        srcHash <- a.database.getMappingHash("mastodon3", "media-bucket", srcFile)
        _ <- a.database.putMapping("mastodon3", "media-bucket", "media/copied-file.jpg", srcHash.get)
        copiedHash <- a.database.getMappingHash("mastodon3", "media-bucket", "media/copied-file.jpg")
        _ = assertEquals(copiedHash, srcHash)

        // Cleanup everything
        _ <- instances.traverse_(inst => a.database.delMappings(inst, "media-bucket"))
        _ <- a.database.delMetadata(newHash)
        _ <- allHashes.traverse_(h => a.database.delMetadata(h))
      } yield ()
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Concurrent upload simulation
  // ═══════════════════════════════════════════════════════════════════

  test("concurrent uploads of same content from different instances") {
    use { a =>
      val content = "viral-meme-content"
      val h = sha512(content)
      val m = md5(content)

      for {
        // All 5 instances upload the same content concurrently
        _ <- a.database.putMetadata(h, m, 4096L, "etag-viral", "image/gif")
        _ <- instances.parTraverse_ { inst =>
          a.database.putMapping(inst, "media", s"meme-${inst}.gif", h)
        }

        // All should succeed and resolve
        results <- instances.traverse { inst =>
          a.database.getMappingHash(inst, "media", s"meme-${inst}.gif")
        }
        _ = results.foreach(r => assertEquals(r, Some(h)))
        count <- a.database.countMappings(h)
        _ = assertEquals(count, 5L)

        // Cleanup
        _ <- instances.traverse_(inst => a.database.delMapping(inst, "media", s"meme-${inst}.gif"))
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("concurrent deletes from different instances") {
    use { a =>
      val h = sha512("concurrent-del")
      for {
        _ <- a.database.putMetadata(h, md5("concurrent-del"), 10L, "e", "t")
        _ <- instances.traverse_(inst => a.database.putMapping(inst, "b", "f.txt", h))

        // All instances delete concurrently
        counts <- instances.parTraverse { inst =>
          a.database.delMapping(inst, "b", "f.txt")
        }
        _ = assertEquals(counts.sum, 5, "Total deleted should be 5")
        remaining <- a.database.countMappings(h)
        _ = assertEquals(remaining, 0L)
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("concurrent putMapping upserts to same key converge") {
    use { a =>
      val h1 = sha512("race-v1"); val h2 = sha512("race-v2")
      for {
        _ <- a.database.putMetadata(h1, md5("race-v1"), 10L, "e1", "t")
        _ <- a.database.putMetadata(h2, md5("race-v2"), 20L, "e2", "t")
        _ <- a.database.putMapping("u", "b", "race.txt", h1)

        // Concurrent upserts — last writer wins, but no crash
        _ <- (1 to 10).toList.parTraverse_ { i =>
          val h = if (i % 2 == 0) h1 else h2
          a.database.putMapping("u", "b", "race.txt", h)
        }

        // Should resolve to one of the two hashes
        result <- a.database.getMappingHash("u", "b", "race.txt")
        _ = assert(result.contains(h1) || result.contains(h2))

        _ <- a.database.delMapping("u", "b", "race.txt")
        _ <- a.database.delMetadata(h1)
        _ <- a.database.delMetadata(h2)
      } yield ()
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Edge cases and data integrity
  // ═══════════════════════════════════════════════════════════════════

  test("delMetadatas with empty list returns 0") {
    use { a =>
      a.database.delMetadatas(Nil).map(r => assertEquals(r, 0))
    }
  }

  test("delMetadata returns 0 for nonexistent hash") {
    use { a =>
      a.database.delMetadata(sha512("ghost")).map(r => assertEquals(r, 0))
    }
  }

  test("getMappings returns joined metadata fields correctly") {
    use { a =>
      val h = sha512("joined-meta")
      for {
        _ <- a.database.putMetadata(h, md5("joined-meta"), 12345L, "etag-joined", "video/mp4")
        _ <- a.database.putMapping("u", "b", "video.mp4", h)
        (mappings, _) <- a.database.getMappings("u", "b")
        _ = assertEquals(mappings.length, 1)
        m = mappings.head
        _ = assertEquals(m.key, "video.mp4")
        _ = assertEquals(m.bucket, "b")
        _ = assertEquals(m.hash, h)
        _ = assertEquals(m.md5, md5("joined-meta"))
        _ = assertEquals(m.size, 12345L)
        _ = assertEquals(m.eTag, "etag-joined")
        _ = assertEquals(m.contentType, "video/mp4")
        _ = assert(m.created != null)
        _ = assert(m.updated != null)
        _ <- a.database.delMapping("u", "b", "video.mp4")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("special characters in file keys are handled correctly") {
    use { a =>
      val h = sha512("special-chars")
      val specialKeys = List(
        "file with spaces.jpg",
        "path/to/deep/file.png",
        "日本語ファイル.txt",
        "file%20encoded.jpg",
        "file+plus.jpg",
        "file'quote.jpg"
      )
      for {
        _ <- a.database.putMetadata(h, md5("special-chars"), 10L, "e", "t")
        _ <- specialKeys.traverse_(k => a.database.putMapping("u", "b", k, h))
        results <- specialKeys.traverse(k => a.database.getMappingHash("u", "b", k))
        _ = results.foreach(r => assertEquals(r, Some(h)))
        _ <- a.database.delMappings("u", "b")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("very long file key is stored and retrieved correctly") {
    use { a =>
      val h = sha512("long-key")
      val longKey = "a" * 1000 + ".jpg"
      for {
        _ <- a.database.putMetadata(h, md5("long-key"), 10L, "e", "t")
        _ <- a.database.putMapping("u", "b", longKey, h)
        r <- a.database.getMappingHash("u", "b", longKey)
        _ = assertEquals(r, Some(h))
        _ <- a.database.delMapping("u", "b", longKey)
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  test("empty string content type is stored correctly") {
    use { a =>
      val h = sha512("empty-ct")
      for {
        _ <- a.database.putMetadata(h, md5("empty-ct"), 10L, "e", "")
        m <- a.database.getMetadata(h)
        _ = assertEquals(m.get.contentType, "")
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Full lifecycle: upload → list → download-check → copy → delete → cleanup
  // ═══════════════════════════════════════════════════════════════════

  test("E2E: full lifecycle for a single file across 3 instances") {
    use { a =>
      val content = "profile-picture-bytes"
      val h = sha512(content)
      val m = md5(content)

      for {
        // 1. mastodon1 uploads (new content)
        existingMeta <- a.database.getMetadata(h)
        _ = assertEquals(existingMeta, None, "Should be new content")
        _ <- a.database.putMetadata(h, m, 2048L, "etag-profile", "image/jpeg")
        _ <- a.database.putMapping("mastodon1", "avatars", "user42.jpg", h)

        // 2. mastodon2 uploads same content (dedup hit)
        dedupCheck <- a.database.getMetadata(h)
        _ = assert(dedupCheck.isDefined, "Dedup should find existing metadata")
        _ <- a.database.putMapping("mastodon2", "avatars", "user99.jpg", h)

        // 3. mastodon3 uploads same content (dedup hit)
        _ <- a.database.putMapping("mastodon3", "avatars", "user7.jpg", h)

        // 4. List: each instance sees their file
        (m1List, _) <- a.database.getMappings("mastodon1", "avatars")
        _ = assertEquals(m1List.length, 1)
        _ = assertEquals(m1List.head.key, "user42.jpg")
        _ = assertEquals(m1List.head.contentType, "image/jpeg")

        // 5. Download check: all resolve to same backend key
        h1 <- a.database.getMappingHash("mastodon1", "avatars", "user42.jpg")
        h2 <- a.database.getMappingHash("mastodon2", "avatars", "user99.jpg")
        h3 <- a.database.getMappingHash("mastodon3", "avatars", "user7.jpg")
        _ = assertEquals(h1, h2)
        _ = assertEquals(h2, h3)
        _ = assertEquals(ProxyBlobStore.hashToKey(h1.get), ProxyBlobStore.hashToKey(h2.get))

        // 6. Copy: mastodon1 copies to a new name
        _ <- a.database.putMapping("mastodon1", "avatars", "user42-backup.jpg", h)
        copyHash <- a.database.getMappingHash("mastodon1", "avatars", "user42-backup.jpg")
        _ = assertEquals(copyHash, Some(h))

        // 7. Delete: mastodon1 removes original (backup + other instances still have it)
        _ <- a.database.delMapping("mastodon1", "avatars", "user42.jpg")
        gone <- a.database.getMappingHash("mastodon1", "avatars", "user42.jpg")
        _ = assertEquals(gone, None)
        backupStill <- a.database.getMappingHash("mastodon1", "avatars", "user42-backup.jpg")
        _ = assert(backupStill.isDefined)

        // 8. Ref count: 3 remaining (backup + mastodon2 + mastodon3)
        refCount <- a.database.countMappings(h)
        _ = assertEquals(refCount, 3L)

        // 9. Not dangling (still has mappings)
        dangling <- a.database.getDangling(1000)
        _ = assert(!dangling.contains(h))

        // 10. Delete all remaining mappings
        _ <- a.database.delMapping("mastodon1", "avatars", "user42-backup.jpg")
        _ <- a.database.delMapping("mastodon2", "avatars", "user99.jpg")
        _ <- a.database.delMapping("mastodon3", "avatars", "user7.jpg")

        // 11. Now metadata is orphaned (but grace period protects it)
        refCountFinal <- a.database.countMappings(h)
        _ = assertEquals(refCountFinal, 0L)
        danglingFinal <- a.database.getDangling(1000)
        _ = assert(!danglingFinal.contains(h), "Grace period should protect fresh orphan")

        // Cleanup
        _ <- a.database.delMetadata(h)
      } yield ()
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Stress: many files, many instances
  // ═══════════════════════════════════════════════════════════════════

  test("stress: 50 files across 5 instances with mixed operations") {
    use { a =>
      val fileCount = 50
      val contents = (1 to fileCount).map(i => s"stress-content-$i").toList
      val hashes = contents.map(c => (c, sha512(c), md5(c)))

      for {
        // Create all metadata
        _ <- hashes.traverse_ { case (c, h, m) =>
          a.database.putMetadata(h, m, c.length.toLong, s"etag-$c", "application/octet-stream")
        }

        // Each instance maps to a random subset of 20 files
        rng = new Random(123)
        _ <- instances.traverse_ { inst =>
          val subset = rng.shuffle(hashes).take(20)
          subset.traverse_ { case (c, h, _) =>
            a.database.putMapping(inst, "stress-bucket", s"$c.bin", h)
          }
        }

        // Verify each instance has exactly 20 files
        _ <- instances.traverse_ { inst =>
          a.database.countMappings(inst, "stress-bucket").map { c =>
            assertEquals(c, 20L, s"$inst should have 20 files")
          }
        }

        // Delete half of mastodon1's files
        (m1Files, _) <- a.database.getMappings("mastodon1", "stress-bucket")
        toDelete = m1Files.take(10).map(_.key)
        _ <- a.database.delMappingKeys("mastodon1", "stress-bucket", toDelete)
        m1Count <- a.database.countMappings("mastodon1", "stress-bucket")
        _ = assertEquals(m1Count, 10L)

        // Other instances unaffected
        _ <- (2 to 5).toList.traverse_ { i =>
          a.database.countMappings(s"mastodon$i", "stress-bucket").map { c =>
            assertEquals(c, 20L)
          }
        }

        // Cleanup
        _ <- instances.traverse_(inst => a.database.delMappings(inst, "stress-bucket"))
        _ <- hashes.traverse_ { case (_, h, _) => a.database.delMetadata(h) }
      } yield ()
    }
  }
}
