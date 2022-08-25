package com.spark.extension.rule

import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class MyRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case Add(left, right, _) => {
        println("this is add opt")
        println("left " + left.getClass)
        println("right " + right.getClass)
        if (isStaticAdd(left)) {
          right
        } else if (isStaticAdd(right)) {
          Add(left, Literal(3L))
        } else {
          Add(left, right)
        }
      }
    }
  }

  private def isStaticAdd(exception: Expression): Boolean = {
    exception.isInstanceOf[Literal] && exception.asInstanceOf[Literal].toString() == "0"
  }
}
