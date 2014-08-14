package ypleads.common

import org.scalatest.FlatSpec
import ypleads.common.Common.functions
import functions._

class SetSpec extends FlatSpec {

  "functions.cleanString" should "do nothing when string is already clean" in {
    val aCleanString = "luisOrSomethingElse I can't think about"
    assert(cleanString(aCleanString) == aCleanString)
  }

  it should "clean 'dirty' strings" in {
    val aDirtyString = "luis\"lala\"OrSomethingElse I can't think about"
    val aCleanString = "luislalaOrSomethingElse I can't think about"
    withClue(s"cleanString(${aDirtyString}) = ${cleanString(aDirtyString)}") {
      assert(cleanString(aDirtyString) == aCleanString)
    }
  }

  "'Dominos'" should "be close enough to 'Dominos P'" in {
    assert(isCloseEnough("Dominos", "Dominos P"))
  }

  it should "be close enough to 'Dominos Pizza'" in {
    assert(isCloseEnough("Dominos", "Dominos Pizza"))
  }

  it should "be close enough to 'Dmino'" in {
    assert(isCloseEnough("Dominos", "Dmino"))
  }

  "'Dominos Pizza'" should "be close enough to 'Dominos P'" in {
    assert(isCloseEnough("Dominos Pizza", "Dominos P"))
  }

  it should "be close enough to 'Dmino Pzz'" in {
    assert(isCloseEnough("Dominos Pizza", "Dmino Pzz"))
  }

}
