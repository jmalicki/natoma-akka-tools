package com.natomatrading.util.akka

import scala.util.control.NoStackTrace

case class TE(message: String) extends RuntimeException(message) with NoStackTrace
