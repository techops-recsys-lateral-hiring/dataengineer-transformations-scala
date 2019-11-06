package thoughtworks.sanitytest

import org.scalatest.{FunSuite, Matchers}

class SanityTest extends FunSuite with Matchers {

  test("true should be true") {
    true should be(true)
  }

  test("true should not be false") {
    true should not be(false)
  }

}
