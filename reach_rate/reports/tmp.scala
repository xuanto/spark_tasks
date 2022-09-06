
  /** 返回sku叉乘OG -> 广告主ID 以及 子品牌叉乘OG -> 广告主ID 的映射关系 **/
  def getKeyOg2UidMapping(joinedDF: DataFrame): (RDD[(String, Long)], RDD[(String, Long)]) = {
    val midDF = joinedDF.groupBy("advertiser_id", "sub_brand", "sku", "og")
                        .agg(sum("achieve_cost").alias("achieve_cost"))
                        .withColumn()
                        .select()

    // sku og -广告主ID. 我们只需要子品牌下所有的SKU
    val skuAdvertiser = subBrandSkuTdwData.map { row =>
      val subBrand = row._2
      val sku = row._3
      val advertiserId = row._1
      (s"$sku,$subBrand", advertiserId)
    }

    // 子品牌 og - 广告主ID
    val subBrandAdvertiser = subBrandSkuTdwData.map { row =>
      val subBrand = row._2
      val advertiserId = row._1
      (subBrand, advertiserId)
    }
    (skuAdvertiser, subBrandAdvertiser)
  }
