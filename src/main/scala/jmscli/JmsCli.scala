package jmscli

import java.io.{IOException, PrintWriter}
import javax.jms._

import org.apache.commons.cli.{HelpFormatter, Options, PosixParser}
import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.{HornetQJMSClient, JMSFactoryType}
import org.hornetq.core.remoting.impl.netty.{NettyConnectorFactory, TransportConstants}

import scala.collection.JavaConversions._
import scala.io.Source

object Config {
  var jmsHost: String = _
  var jmsPort: Int = _
  var jmsUser: String = _
  var jmsPassword: String = _
  var jmsHeaders: Map[String, String] = _
  var jmsDestination: String = _

  var topic: Boolean = _

  var connectionUrl: String = _

  var verbose: Boolean = _
}

object JmsCli {

  def exitError(message: String): Nothing = {
    Console.err.println("error: %s".format(message))
    sys.exit(2)
  }

  def verbose(message: String): Unit = if (Config.verbose) Console.out.println("* %s".format(message))

  // jms://192.168.1.11:5445/draftEvent
  def processCommandLine(fullArgs: Array[String]): String = {
    val cliOptions = new Options
    cliOptions.addOption("t", "topic", false, "Treat destination as a topic instead of a queue")
    cliOptions.addOption("H", "header", true, "JMS message attributes")
    cliOptions.addOption("T", "jmstype", true, "JMSType attribute value")
    cliOptions.addOption("f", "input-file", true, "File name for input data, otherwise read from stdin")
    cliOptions.addOption("v", "verbose", false, "Detailed output on what's going on")

    def exitUsageError(error: String): Nothing = {
      val formatter = new HelpFormatter
      val writer = new PrintWriter(Console.out)
      formatter.printHelp(writer, 120,
        "jmscli jms://[user:pass@]host:port/destination",
        null,
        cliOptions,
        4, 4,
        null,
        false
      )
      writer.flush()
      sys.exit(2)
    }

    val cliParser = new PosixParser
    val cliParsed = cliParser.parse(cliOptions, fullArgs)

    Config.verbose = cliParsed.hasOption('v')

    Config.topic = cliParsed.hasOption('t')

    Config.jmsHeaders = Iterator(
      Option(cliParsed.getOptionValue('T')).map(x => Array("JMSType=%s".format(x))),
      Option(cliParsed.getOptionValues('H')): Option[Array[String]]
    ).flatten.flatten.map { kvComboText =>
      val kv = kvComboText.split("=", 2)
      if (kv.length != 2) {
        exitError("Invalid header format, expected: key=value, got: %s".format(kvComboText))
      }
      kv(0) -> kv(1)
    }.toMap

    val args = cliParsed.getArgs
    if (args.length < 1) exitUsageError("Missing connection URL")

    // jms://user:password@host:port/queuename
    Config.connectionUrl = args(0)
    """^jms://(([^:]+):([^@]+)@)?([^:]+):(\d+)/(.+)$""".r.findFirstMatchIn(Config.connectionUrl) match {
      case Some(m) =>
        Config.jmsUser = m.group(2)
        Config.jmsPassword = m.group(3)
        Config.jmsHost = m.group(4)
        Config.jmsPort = m.group(5).toInt
        Config.jmsDestination = m.group(6)
      case None =>
        exitError("invalid connection URL")
    }

    val inputText = try Option(cliParsed.getOptionValue('f')).map(x => Source.fromFile(x)).getOrElse(Source.stdin).mkString catch {
      case e: IOException => exitError("reading input: %s".format(e.getMessage))
    }

    if (inputText.trim.length < 1) exitError("empty input, not sending anything")

    inputText
  }

  def main(args: Array[String]): Unit = {

    val stdinText = processCommandLine(args)

    val jmsConnectionFactory: ConnectionFactory = HornetQJMSClient.createConnectionFactoryWithoutHA(
      JMSFactoryType.CF,
      new TransportConfiguration(
        classOf[NettyConnectorFactory].getName,
        mapAsJavaMap(Map(
          TransportConstants.HOST_PROP_NAME -> Config.jmsHost,
          TransportConstants.PORT_PROP_NAME -> new Integer(Config.jmsPort)
        ))
      )
    )

    verbose("Connecting to %s:%d".format(Config.jmsHost, Config.jmsPort))
    val jmsConnection = try {
      if (Config.jmsUser != null) {
        jmsConnectionFactory.createConnection(Config.jmsUser, Config.jmsPassword)
      } else {
        jmsConnectionFactory.createConnection()
      }
    } catch { case e: javax.jms.JMSException =>
      exitError("can't connect with %s: %s".format(Config.connectionUrl, e.getCause.getMessage))
    }
    verbose("Connected")

    val jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val jmsDestination = if (Config.topic)
      HornetQJMSClient.createTopic(Config.jmsDestination)
    else
      HornetQJMSClient.createQueue(Config.jmsDestination)

    val destType: String = if (Config.topic) "topic" else "queue"
    verbose("Accessing %s %s".format(destType, Config.jmsDestination))
    val jmsProducer = try jmsSession.createProducer(jmsDestination) catch { case e: javax.jms.InvalidDestinationException =>
        exitError("accessing %s \"%s\": %s".format(destType, Config.jmsDestination, e.getMessage))
    }
    verbose("Ok")

    val jmsMessage = jmsSession.createTextMessage(stdinText)

    Config.jmsHeaders.foreach { case (k, v) =>
        jmsMessage.setStringProperty(k, v)
    }

    verbose("Sending message (len=%d): %s".format(stdinText.length, stdinText.trim))
    jmsProducer.send(jmsMessage)
    verbose("Sent")
  }

}
