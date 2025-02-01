import munit.CatsEffectSuite

class PgIntegrationTests extends CatsEffectSuite {
  test("Query is run against db") {
    assertIO(Db.selectId(), List.empty)
  }
}