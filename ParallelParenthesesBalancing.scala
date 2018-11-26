package reductions

// Made by PedroCorreiaLuis and RuiGSValente

import scala.annotation._
import org.scalameter._
import common._

import scala.collection.immutable.Nil

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  def balance(chars: Array[Char]): Boolean = {
    def f(chars: Array[Char], i: Int): Int =
      if(i>=0 && !chars.isEmpty)
          if (chars.head == '(') f(chars.tail, i+1)
          else if (chars.head == ')')  f(chars.tail, i-1)
          else f(chars.tail, i)
      else i
    f(chars, 0)==0
  }
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
          def traverse(from: Int, until: Int): (Int, Int) = {
            val num: Array[Int] = chars.slice(from, until).map {
              case '(' => 1
              case ')' => -1
              case _ => 0
            }
            val acc = num.scanLeft(0)(_ + _).tail
            val m: Int = acc.min
            val l: Int = acc.last
            (m, l)
          }

          def reduce(from: Int, until: Int):(Int, Int) = {
            if (until - from <= threshold) {
              traverse(from, until)
            }
            else{
              val (l, r) = parallel(reduce(from, (from + until) / 2), reduce((from + until) / 2, until))
              (l._2 + r._1 , l._2 + r._2)
      }
    }
    if (chars.head == ')') false
    else reduce(0, chars.length) == (0, 0)
  }
}
