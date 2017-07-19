package com.dgn.core.utils

import com.dgn.testbase.DGNBaseTest

/**
  * Created by ophchu on 5/11/17.
  */
class HttpUtils$Test extends DGNBaseTest {

  test("getRequest"){
    val pumaReqUrl = "http://internal-puma-538019197.us-east-1.elb.amazonaws.com/puma/user/profile/1783059/ANDROID_APPLICATION"
    val res = HttpUtils.getRequest(pumaReqUrl)
    println(res)
  }
  test("postRequest"){
    val pumaReqUrl = "http://52.90.68.49/puma/user/profile/"
    val params = Map(
      "user_id" -> "1783059",
      "application_type" -> "ANDROID_APPLICATION"
    )
    val res = HttpUtils.postRequest(pumaReqUrl, params)
    println(res)
  }
}
