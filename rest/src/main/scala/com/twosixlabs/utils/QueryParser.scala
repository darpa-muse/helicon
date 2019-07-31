package com.twosixlabs.utils
import com.twosixlabs.muse_utils.App

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.parsing.combinator.JavaTokenParsers


class ParsingEngine extends JavaTokenParsers {

  def things : Parser[Any] = stringLiteral|ident|decimalNumber|floatingPointNumber|wholeNumber
  def stringL : Parser[Any] = """[-@#$^%&*_+{}\\[\\]|;:\"'`<,>.?/\\\\]+""".r
  // def specialChars : Parser[Any] = """.[+]+""".r //^^ {case "++" => {"++"}}
  // def anyVal : Parser[Any] = things | rep(opt(stringL)~things~opt(stringL))
  def anyVal : Parser[Any] = things | "\""~things~"\""|things~"/"~things
  def value: Parser[Any] = anyVal
  def key: Parser[Any] = anyVal

  def relOp: Parser[Any] = "lte" |"<=" | "eq" | "!=" | "ne" |
    ">" | "gt" | "<" | "lt" |
    "gte" | ">=" | "="

  var scans = Map[String, String]()

  def expr: Parser[Any] = key~relOp~value ^^ {
    // handle the row elements, topic, and language cases
    // this will return the colQ
    case "version"  ~ "=" ~value => {getSymbol("version=" + value)} //colF
    case "name"     ~ "=" ~value => {getSymbol("name=" + value)} //colF
    case "date"     ~ "=" ~value => {getSymbol("date=" + value)} //colF
    case "repo"     ~ "=" ~value => {getSymbol("repo=" + value)}  //colF
    case "uuid"     ~ "=" ~value => {getSymbol("uuid=" + value)}  //colF
    case "language" ~ "=" ~value => {getSymbol("language=" + value)} //colF
    case "topic"    ~ "=" ~value => {getSymbol("topic=" + value)} //colF
    case left       ~ relOp ~value => {getSymbol(left + relOp.toString + value)}  //colF=projectMetadata, colQ=left, v=value
  }
  private def getSymbol(term: String) = {
    scans = scans + (term -> scans.get(term).getOrElse("t" + scans.size));
    scans(term)
  }

  // there may be holes but testing is proving otherwise...
  def b_expression: Parser[Any] = b_term ~ rep("or" ~ b_term)          ^^ { case f1 ~ fs ⇒ fs.foldLeft(new StringBuilder(f1.toString)){ (sb, s) => sb append " | " append s.toString().replace("or~","") }.toString } //(f1 /: fs)(_ || _._2) }
  def b_term: Parser[Any] = (b_not_factor ~ rep("and" ~ b_not_factor)) ^^ { case f1 ~ fs ⇒ fs.foldLeft(new StringBuilder(f1.toString)){ (sb, s) => sb append " & " append s.toString().replace("and~","")}.toString} // (f1 /: fs)(_ && _._2) }
  def b_not_factor:  Parser[Any] = opt("not") ~ b_factor               ^^ (x ⇒ x match { case Some(v) ~ f ⇒ "invert" + f; case None ~ f ⇒ f })
  def b_factor:      Parser[Any] = expr | ("(" ~ b_expression ~ ")" ^^ { case "(" ~ exp ~ ")" ⇒ "(" + exp + ")"})

}
import com.bpodgursky.jbool_expressions.Expression
import com.bpodgursky.jbool_expressions.parsers.ExprParser
import com.bpodgursky.jbool_expressions.rules.RuleSet

object QueryParser extends ParsingEngine {
  def main(args: Array[String]) {

    val a = "language = \"c++\""
    val b = "language = c"
    val c = "language = ada"
    val d = "topic = \"input-output\""
    val e = "topic = networking"
    val f = "topic = \"network_count\""
    val g = "stargazers_count > 20"
    val h = "topic = android"
    val i = "language = java"

    println(parse(s"($i and $a and $h) or $c"))
    println(parse(s"$g"))
    println(parse(s"($a or $b) and $f"))
    println(parse(s"$a or $b"))
    println(parse(s"$a and $b"))
    println(parse(s"$a or $b or $c and $a"))
    println(parse(s"$a and $b or (($a and $b) or $a)"))
    println(parse(s"$a or $b and $c or ($a and $c) or $a"))
    println(parse(s"$a or $b and $c or ($a and ($c or $a))"))
    println(parse(s"$a or $b and $c or ($a and ($c or $b))"))
    println(parse(s"$a and $b and $a and $c or $b"))
    println(parse(s"$a or $b or $c"))
    println(parse(s"$a and $b and $c"))

  }
  import collection.JavaConverters._

  // returns a list of list of scans to perform
  // each item is a separate list of scans
  // - for row elements this is an intersectingIterator scan
  // - for non-row elements, this is regex iterator
  def parse(queryString : String):java.util.List[java.util.List[String]] ={
    var result:ParseResult[Any] = parseAll(b_expression, queryString)

    try{
      App.logger.info("\nQuery: " + queryString)
      result = parseAll(b_expression, queryString)
    } catch {
      case unknown:Throwable => println("Got this unknown exception: " + unknown)
    }
    var terms:Seq[Seq[String]] = null;
    result.successful match {
      case false => {
        val line = result.next.pos.line
        val col = result.next.pos.column
       // val msg:Int = result.
        val msg = result.toString.substring(result.toString.indexOf("failure:")) //.replaceAll("\n", "<br>")
        terms = Seq(Seq[String](s"Error: $msg parsing failed somewhere near line $line, col $col (please review help for tips)."))
      }
      case true => {
        val mappedExpr = ExprParser.parse(result.get.toString)
        val sopForm = RuleSet.toDNF(RuleSet.simplify(mappedExpr))
        val symbol2Expr = scans.map(_.swap)
        terms = sopForm.toString.split("\\|").toSeq.map(_.replaceAll("[()]","").trim.split("&").toSeq.map(_.trim).map(symbol2Expr(_)))
      }
    }
    terms.map(_.asJava).asJava
  }
}

/*
case class LogicalExpression(variableMap: Map[String, Boolean]) extends JavaTokenParsers {
  private lazy val b_expression: Parser[Boolean] = b_term ~ rep("or" ~ b_term)          ^^ { case f1 ~ fs ⇒ (f1 /: fs)(_ || _._2) }
  private lazy val b_term: Parser[Boolean] = (b_not_factor ~ rep("and" ~ b_not_factor)) ^^ { case f1 ~ fs ⇒ (f1 /: fs)(_ && _._2) }
  private lazy val b_not_factor:  Parser[Boolean] = opt("not") ~ b_factor                          ^^ (x ⇒ x match { case Some(v) ~ f ⇒ !f; case None ~ f ⇒ f })
  private lazy val b_factor:      Parser[Boolean] = b_literal | b_variable | ("(" ~ b_expression ~ ")" ^^ { case "(" ~ exp ~ ")" ⇒ exp })
  private lazy val b_literal: Parser[Boolean] = "true" ^^ (x ⇒ true) | "false" ^^ (x ⇒ false)


  // This will construct the list of variables for this parser
  private lazy val b_variable: Parser[Boolean] = variableMap.keysIterator.map(Parser(_)).reduceLeft(_ | _) ^^ (x ⇒ variableMap(x))

  def sparse(expression: String) = this.parseAll(b_expression, expression)
}

object LogicalExpression {

  def sparse(variables: Map[String, Boolean])(value: String) {
    println(LogicalExpression(variables).sparse(value))
  }
}

object Sofoklis {
  def main(args: Array[String]) {
    println("testing parser")

    val variables = Map("a" -> true, "b" -> false, "c" -> true)
    val variableParser = LogicalExpression.sparse(variables) _

    variableParser("a or b")
    variableParser("a and b")
    variableParser("a or b or c and a")
    variableParser("a and b or ((a and b) or a)")
    variableParser("a or b and c or (a and c)or a")
    variableParser("a and b and a and c or b")
    variableParser("a or b or d")
    variableParser("a and b and c")
    variableParser("")

  }
}
*/