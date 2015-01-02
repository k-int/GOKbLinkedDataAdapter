#!/usr/bin/groovy

// @GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
@GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/')
@GrabResolver(name='kint', root='http://projects.k-int.com/nexus-webapp-1.4.0/content/repositories/releases')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.3'),
  @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.1.2'),
  @Grab(group='org.apache.httpcomponents', module='httpclient', version='4.0'),
  @Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.5.0'),
  @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.1.2'),
  @Grab(group='com.k-int', module='goai', version='1.0.2'),
  @Grab(group='org.slf4j', module='slf4j-api', version='1.7.6'),
  @Grab(group='org.slf4j', module='jcl-over-slf4j', version='1.7.6'),
  @Grab(group='net.sourceforge.nekohtml', module='nekohtml', version='1.9.14'),
  @Grab(group='xerces', module='xercesImpl', version='2.9.1'),
  @Grab(group='org.apache.jena', module='jena-tdb', version='1.0.2'),
  @Grab(group='org.apache.jena', module='jena-core', version='2.11.2'),
  @Grab(group='org.apache.jena', module='jena-arq', version='2.11.2'),
  @Grab(group='org.apache.jena', module='jena-iri', version='1.0.2'),
  @Grab(group='org.apache.jena', module='jena-spatial', version='1.0.1'),
  @Grab(group='org.apache.jena', module='jena-security', version='2.11.2'),
  @Grab(group='org.apache.jena', module='jena-text', version='1.0.1'),
  @Grab(group='virtuoso', module='virtjena', version='2'),
  @Grab(group='virtuoso', module='virtjdbc', version='4.1')
])

import groovyx.net.http.*
import static groovyx.net.http.ContentType.URLENC
import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.*
import groovyx.net.http.*
import org.apache.http.entity.mime.*
import org.apache.http.entity.mime.content.*
import org.apache.http.*
import org.apache.http.protocol.*
import java.nio.charset.Charset
import static groovy.json.JsonOutput.*
import virtuoso.jena.driver.*;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.* ;
import com.hp.hpl.jena.graph.*;
import java.text.SimpleDateFormat
import groovy.util.slurpersupport.GPathResult
import org.apache.log4j.*
import com.k_int.goai.*;

def config_file = new File('GOKbLinkedDataAdapter-config.groovy')

def config = new ConfigSlurper().parse(config_file.toURL())
if ( ! config.maxtimestamp ) {
  println("Intialise timestamp");
  config.maxtimestamp = 0
}

println("Starting...");

try {
  
  def graph = new VirtGraph('uri://gokb.org/', config.store_uri, "dba", "dba");
  
  Node foaf_org_type = Node.createURI('http://xmlns.com/foaf/0.1/Organization');
  Node foaf_agent_type = Node.createURI('http://xmlns.com/foaf/0.1/Agent');
  Node schema_org_organisation_type = Node.createURI('http://schema.org/Organisation');
  Node bibframe_organisation_type = Node.createURI('http://bibframe.org/vocab-list/#Organisation');
  Node owl_organisation_type = Node.createURI('http://www.w3.org/2002/07/owl#Organisation');
  Node owl_thing_type = Node.createURI('http://www.w3.org/2002/07/owl#Thing');
  Node owl_work_type = Node.createURI('http://www.w3.org/2002/07/owl#Work');
  
  Node type_pred = Node.createURI('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');

  Node skos_pref_label_pred = Node.createURI('http://www.w3.org/2004/02/skos/core#prefLabel');
  Node skos_alt_label_pred = Node.createURI('http://www.w3.org/2004/02/skos/core#altLabel');
  Node owl_same_as_pred = Node.createURI('http://www.w3.org/2002/07/owl#sameAs');

  Node foaf_homepage_pred = Node.createURI('http://xmlns.com/foaf/0.1/homepage');

  Node bibframe_provider_role_pred = Node.createURI('http://bibframe.org/vocab-list/#providerRole');

  Node dc_publisher_pred = Node.createURI('http://purl.org/dc/terms/publisher');
  
  OaiClient oaiclient_orgs = new OaiClient(host:config.oai_server+'/gokb/oai/orgs');
  // OaiClient oaiclient = new OaiClient(host:'https://gokb.k-int.com/gokb/oai/orgs');
  // OaiClient oaiclient = new OaiClient(host:'https://gokb.kuali.org/gokb/oai/orgs');

  oaiclient_orgs.getChangesSince(null, 'gokb') { record ->
    println("Org... ${record.header.identifier}");
    println("       ${record.metadata.gokb.org.@id}");
    println("       ${record.metadata.gokb.org.name.text()}");
  
    Node orgUri = Node.createURI('uri://'+record.header.identifier);
  
    graph.add(new Triple(orgUri, type_pred, foaf_org_type));
    graph.add(new Triple(orgUri, type_pred, foaf_agent_type));
    graph.add(new Triple(orgUri, type_pred, schema_org_organisation_type));
    graph.add(new Triple(orgUri, type_pred, bibframe_organisation_type));
    graph.add(new Triple(orgUri, type_pred, owl_organisation_type));
    graph.add(new Triple(orgUri, type_pred, owl_thing_type));
    graph.add(new Triple(orgUri, skos_pref_label_pred,  Node.createLiteral(record.metadata.gokb.org.name.text())));
    graph.add(new Triple(orgUri, foaf_homepage_pred,  Node.createLiteral(record.metadata.gokb.org.homepage?.text())));
  
    record.metadata.gokb.org.identifiers.identifier.each {
      println(it.text())
      graph.add(new Triple(orgUri, owl_same_as_pred,  Node.createLiteral(it.text())));
    }

    record.metadata.gokb.org.variantNames.variantName.each {
      println(it.text())
      graph.add(new Triple(orgUri, skos_alt_label_pred,  Node.createLiteral(it.text())));
    }

    record.metadata.gokb.org.roles.role.each {
      println(it.text())
      graph.add(new Triple(orgUri, bibframe_provider_role_pred,  Node.createLiteral(it.text())));
    }

  }

  OaiClient oaiclient_titles = new OaiClient(host:config.oai_server+'/gokb/oai/titles');
  oaiclient_orgs.getChangesSince(null, 'gokb') { record ->
    Node titleUri = Node.createURI('uri://'+record.header.identifier);
    graph.add(new Triple(titleUri, type_pred, owl_work_type));

    graph.add(new Triple(titleUri, skos_pref_label_pred, Node.createLiteral(record.metadata.gokb.title.name.text()) ));
    graph.add(new Triple(titleUri, dc_publisher_pred, Node.createLiteral(record.metadata.gokb.title.publisher?name.text()) ));


    record.metadata.gokb.org.identifiers.identifier.each {
      println(it.text())
      graph.add(new Triple(orgUri, owl_same_as_pred,  Node.createLiteral(it.text())));
    }

    record.metadata.gokb.org.variantNames.variantName.each {
      println(it.text())
      graph.add(new Triple(orgUri, skos_alt_label_pred,  Node.createLiteral(it.text())));
    }

  }

  OaiClient oaiclient_platforms = new OaiClient(host:config.oai_server+'/gokb/oai/platforms');
  oaiclient_platforms.getChangesSince(null, 'gokb') { record ->
  }
  
  
  OaiClient oaiclient_packages = new OaiClient(host:config.oai_server+'/gokb/oai/packages');
  oaiclient_packages.getChangesSince(null, 'gokb') { record ->
  }

  graph.close();
}
catch ( Exception e ) {
  e.printStackTrace();
}
finally {
}

println("Done.");

config_file.withWriter { writer ->
  config.writeTo(writer)
}
