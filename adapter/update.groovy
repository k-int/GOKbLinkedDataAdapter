#!/usr/bin/groovy

// @GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
@GrabResolver(name='kint', root='http://projects.k-int.com/nexus-webapp-1.4.0/content/repositories/releases')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.3'),
  @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.1.2'),
  @Grab(group='org.apache.httpcomponents', module='httpclient', version='4.0'),
  @Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.5.0'),
  @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.1.2'),
  @Grab(group='com.k-int', module='goai', version='1.0.2'),
])


import groovy.util.slurpersupport.GPathResult
import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.*
import groovyx.net.http.*
import org.apache.http.entity.mime.*
import org.apache.http.entity.mime.content.*
import java.nio.charset.Charset
import org.apache.http.*
import org.apache.http.protocol.*
import org.apache.log4j.*
import au.com.bytecode.opencsv.CSVWriter
import java.text.SimpleDateFormat
import com.k_int.goai.*;

def config_file = new File('GOKbLinkedDataAdapter-config.groovy')

def config = new ConfigSlurper().parse(config_file.toURL())
if ( ! config.maxtimestamp ) {
  println("Intialise timestamp");
  config.maxtimestamp = 0
}

OaiClient oaiclient = new OaiClient(host:'https://gokb.k-int.com/gokb/oai/orgs');
println("Starting...");

oaiclient.getChangesSince(null, 'gokb') { org ->
}

println("Done.");

config_file.withWriter { writer ->
  config.writeTo(writer)
}
