// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package thoughtworks

import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen, Suite}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

trait DefaultFeatureSpecWithSpark extends  BeforeAndAfterAll with Matchers { self: Suite =>

  private var _spark: SparkSession = null

  protected implicit def spark: SparkSession = _spark

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  override protected def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession.builder
        .appName("Spark Test App")
        .config("spark.driver.host","127.0.0.1")
        .master("local")
        .getOrCreate()
      print("beforeAll")
    }

    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        print("afterAll")
      }
    }
  }
}
