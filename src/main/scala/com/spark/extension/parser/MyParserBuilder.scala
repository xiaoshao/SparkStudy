package com.spark.extension.parser

import com.spark.extension.rule.MyRule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.rules.Rule

object MyParserBuilder {


  type ExtensionBuilder = SparkSessionExtensions => Unit


  val extensionBuilder: ExtensionBuilder = { e => {
    e.injectParser((spark, parser) => new MyParser(parser))
    e.injectOptimizerRule(spark => new MyRule);
  }
  }
}
