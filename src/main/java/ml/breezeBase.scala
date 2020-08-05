package ml

import breeze.linalg.{DenseMatrix, DenseVector, det, inv, sum, trace}

/**
 * Created byX on 2020-07-29 00:24
 * Desc:
 */
object breezeBase {
  def main(args: Array[String]): Unit = {
    val m1 = DenseMatrix((1.0, 2.0), (3.0, 4.0));
    val m2 = DenseMatrix.zeros[Int](3, 4);
    println(f"${m2}")
    println(f"${m1}")

    val m3 = DenseMatrix.ones[Float](6, 6);
    println(f"${m3}")

    val v1 = DenseVector.ones[Float](3)
    println(f"${v1}")
    println(f"${v1.t}")

    val v2 = DenseVector(1, 2, 3, 4, 5)
    println(f"${v2}")
    println(f"${v2.t}")

    val v3 = DenseVector.rand(5)

    println(f"${v3}")
    println(f"${v3(0)}")

    val m4 = DenseMatrix((1, 2, 3, 4, 5), (6, 7, 8, 9, 0))

    val v4 = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9)

    println(f"${m4(0, 2)}")
    println(f"${m4(::, 2)}")
    println(f"${v4(2)}")
    println(f"${v4(1 to 4)}")
    println(f"${v4(1 to -1)}")
    println(f"${v4(1 to 7 by 2)}")

    val m5 = DenseMatrix((5, 4, 3, 2, 1), (0, 9, 8, 7, 6))


    println(f"${m4 + m5}")
    println(f"${m4 * m5.t}")
    println(f"${sum(m4)}")
    println(f"${trace(m4 * m5.t)}")
    println(f"${det(m4)}")
    println(f"${inv(m4)}")
    //    println(f"${svd(m4) }")


    println(f"${m4.rows; m4.size; m4.cols}")


  }

}
