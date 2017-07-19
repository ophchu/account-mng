package com.dgn.helloworld

/**
  * Created by ophchu on 12/28/16.
  */
object HelloWorld extends App {

  def reverseHelloWorld = {
    def reverseString(name: String): String = {
      name.length == 1 match {
        case true => name
        case false => reverseString(name.substring(1)) + name.substring(0,1)
      }
    }
    reverseString("HelloWorld!")
  }
  println(reverseHelloWorld)

}
