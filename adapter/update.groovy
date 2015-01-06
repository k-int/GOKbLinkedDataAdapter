#!/usr/bin/groovy

// @GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
@GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/')
@GrabResolver(name='kint', root='http://nexus.k-int.com/content/repositories/releases');
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
  
  Node foaf_org_type = NodeFactory.createURI('http://xmlns.com/foaf/0.1/Organization');
  Node foaf_agent_type = NodeFactory.createURI('http://xmlns.com/foaf/0.1/Agent');
  Node schema_paymenttype_type = NodeFactory.createURI('http://schema.org/PaymentMethod')
  Node schema_org_organisation_type = NodeFactory.createURI('http://schema.org/Organisation');
  Node bibframe_organisation_type = NodeFactory.createURI('http://bibframe.org/vocab-list/#Organisation');
  Node owl_organisation_type = NodeFactory.createURI('http://www.w3.org/2002/07/owl#Organisation');
  Node owl_thing_type = NodeFactory.createURI('http://www.w3.org/2002/07/owl#Thing');
  Node owl_work_type = NodeFactory.createURI('http://www.w3.org/2002/07/owl#Work');
  Node dc_service_type = NodeFactory.createURI('http://purl.org/dc/dcmitype/Service');
  Node dc_collection_type = NodeFactory.createURI('http://purl.org/dc/dcmitype/Collection');
  Node dc_text_type = NodeFactory.createURI('http://purl.org/dc/dcmitype/Text')
  Node dc_type_type = NodeFactory.createURI('http://purl.org/dc/terms/type ')
  Node type_pred = NodeFactory.createURI('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  Node rdfs_resource_pred = NodeFactory.createURI('http://www.w3.org/2000/01/rdf-schema#Resource');
  Node foaf_document_type = NodeFactory.createURI('http://xmlns.com/foaf/spec/#term_Document');

  Node skos_pref_label_pred = NodeFactory.createURI('http://www.w3.org/2004/02/skos/core#prefLabel');
  Node skos_alt_label_pred = NodeFactory.createURI('http://www.w3.org/2004/02/skos/core#altLabel');
  Node owl_same_as_pred = NodeFactory.createURI('http://www.w3.org/2002/07/owl#sameAs');

  Node foaf_homepage_pred = NodeFactory.createURI('http://xmlns.com/foaf/0.1/homepage');

  Node bibframe_provider_role_pred = NodeFactory.createURI('http://bibframe.org/vocab-list/#providerRole');

  Node service_provides_pred = NodeFactory.createURI('http://dini-ag-kim.github.io/service-ontology/service.html#provides')
  Node dc_publisher_pred = NodeFactory.createURI('http://purl.org/dc/terms/publisher');
  Node dc_format_pred = NodeFactory.createURI('http://purl.org/dc/terms/format');
  Node dc_medium_pred = NodeFactory.createURI('http://purl.org/dc/terms/medium')
  Node bibo_status_pred = NodeFactory.createURI('http://purl.org/ontology/bibo/status');

  Node datacite_issn_pred = NodeFactory.createURI('http://purl.org/spar/datacite/issn')
  Node datacite_eissn_pred = NodeFactory.createURI('http://purl.org/spar/datacite/eissn')

  Node gokb_mission_pred = NodeFactory.createURI('http://gokb.org/organization/#mission');
  Node gokb_pureOpen_pred = NodeFactory.createURI('http://gokb.org/title/#pureOpenAccess')
  Node gokb_hasTitle_pred = NodeFactory.createURI('http://gokb.org/tipp/#hasTitle')
  Node gokb_hasPackage_pred = NodeFactory.createURI('http://gokb.org/tipp/#hasPackage')
  Node gokb_hasPlatform_pred = NodeFactory.createURI('http://gokb.org/tipp/#hasPlatform')
  Node gokb_accessStart_pred = NodeFactory.createURI('http://gokb.org/property/#accessStartDate')
  Node gokb_accessEnd_pred = NodeFactory.createURI('http://gokb.org/property/#accessEndDate')
  Node gokb_coverageStartDate_pred = NodeFactory.createURI('http://gokb.org/property/#coverageStartDate')
  Node gokb_coverageEndDate_pred = NodeFactory.createURI('http://gokb.org/property/#coverageEndDate')
  Node gokb_coverageStartIssue_pred = NodeFactory.createURI('http://gokb.org/property/#coverageStartIssue')
  Node gokb_coverageEndIssue_pred = NodeFactory.createURI('http://gokb.org/property/#coverageEndIssue')
  Node gokb_coverageStartVolume_pred = NodeFactory.createURI('http://gokb.org/property/#coverageStartVolume')
  Node gokb_coverageEndVolume_pred = NodeFactory.createURI('http://gokb.org/property/#coverageEndVolume')
  Node gokb_coverageEmbargo_pred = NodeFactory.createURI('http://gokb.org/property/#coverageEmbargo')
  Node gokb_belongsToPkg_pred = NodeFactory.createURI('http://gokb.org/property/#belongsToPkg')

  Node bibo_precededBy_pred = NodeFactory.createURI('http://bibframe.org/vocab-list/#precededBy') 
  Node bibo_succeeded_pred = NodeFactory.createURI('http://bibframe.org/vocab-list/#succeeded') 

  Node mods_identifier_pred = NodeFactory.createURI('http://www.loc.gov/standards/mods/modsrdf/v1/#identifier')
  Node mods_locationUrl_pred = NodeFactory.createURI('http://www.loc.gov/standards/mods/modsrdf/v1/#locationUrl')
  Node stac_authenticationMethod_pred = NodeFactory.createURI('http://securitytoolbox.appspot.com/stac#AuthenticationMethod');
  Node service_providedby_pred = NodeFactory.createURI('http://dini-ag-kim.github.io/service-ontology/service.html#providedby');

  println("Connect to ${config.oai_server}gokb/oai/orgs");
  OaiClient oaiclient_orgs = new OaiClient(host:config.oai_server+'gokb/oai/orgs');
  // OaiClient oaiclient = new OaiClient(host:'https://gokb.k-int.com/gokb/oai/orgs');
  // OaiClient oaiclient = new OaiClient(host:'https://gokb.kuali.org/gokb/oai/orgs');

  oaiclient_orgs.getChangesSince(null, 'gokb') { record ->
    println("Org... ${record.header.identifier}");
    println("       ${record.metadata.gokb.org.@id}");
    println("       ${record.metadata.gokb.org.name.text()}");
  
    Node orgUri = NodeFactory.createURI('http://www.gokb.org/data/orgs/'+record.metadata.gokb.org.@id);
  
    graph.add(new Triple(orgUri, type_pred, foaf_org_type));
    graph.add(new Triple(orgUri, type_pred, foaf_agent_type));
    graph.add(new Triple(orgUri, type_pred, schema_org_organisation_type));
    graph.add(new Triple(orgUri, type_pred, bibframe_organisation_type));
    graph.add(new Triple(orgUri, type_pred, owl_organisation_type));
    graph.add(new Triple(orgUri, type_pred, owl_thing_type));
    graph.add(new Triple(orgUri, skos_pref_label_pred,  NodeFactory.createLiteral(record.metadata.gokb.org.name.text())));
    graph.add(new Triple(orgUri, foaf_homepage_pred,  NodeFactory.createLiteral(record.metadata.gokb.org.homepage?.text())));
  
    record.metadata.gokb.org.identifiers.identifier.each {
      graph.add(new Triple(orgUri, owl_same_as_pred,  NodeFactory.createLiteral(it.text())));
    }

    record.metadata.gokb.org.variantNames.variantName.each {
      graph.add(new Triple(orgUri, skos_alt_label_pred,  NodeFactory.createLiteral(it.text())));
    }

    record.metadata.gokb.org.roles.role.each {
      graph.add(new Triple(orgUri, bibframe_provider_role_pred,  NodeFactory.createLiteral(it.text())));
    }

    graph.add(new Triple(orgUri,gokb_mission_pred, NodeFactory.createLiteral(record.metadata.gokb.org.mission.text())));
  }

  OaiClient oaiclient_titles = new OaiClient(host:config.oai_server+'gokb/oai/titles');
  oaiclient_orgs.getChangesSince(null, 'gokb') { record ->

    Node titleUri = NodeFactory.createURI('http://www.gokb.org/data/titles/'+record.metadata.gokb.title.@id);
    graph.add(new Triple(titleUri, type_pred, owl_work_type));
    graph.add(new Triple(titleUri, dc_format_pred, NodeFactory.createLiteral(record.metadata.gokb.title.medium.text())));

    graph.add(new Triple(titleUri, skos_pref_label_pred, NodeFactory.createLiteral(record.metadata.gokb.title.name.text()) ));
    if ( record.metadata.gokb.title.publisher?.@id ) {
      Node publisher = NodeFactory.createURI('http://www.gokb.org/data/orgs/'+record.metadata.gokb.title.publisher?.@id);
      graph.add(new Triple(titleUri, dc_publisher_pred, publisher));
    }

    record.metadata.gokb.title.identifiers.identifier.each {
      graph.add(new Triple(titleUri, owl_same_as_pred,  NodeFactory.createLiteral(it.text())));
    }

    record.metadata.gokb.title.variantNames.variantName.each {
      graph.add(new Triple(titleUri, skos_alt_label_pred,  NodeFactory.createLiteral(it.text())));
    }

    record.metadata.gokb.title.identifiers.identifier.each {
      if(it.@namespace.text() == 'issn')
          graph.add(new Triple(titleUri, datacite_issn_pred,  NodeFactory.createLiteral(it.text())));
      else if (it.@namespace.text() == 'eissn')
          graph.add(new Triple(titleUri, datacite_eissn_pred,  NodeFactory.createLiteral(it.text()))); 
    }
    
    // graph.add(new Triple(titleUri, bibo_status_pred, NodeFactory.createLiteral(record.metadata.gokb.title.OAStatus.text()));
    graph.add(new Triple(titleUri,gokb_pureOpen_pred,NodeFactory.createLiteral(record.metadata.gokb.title.pureOA.text())));

    record.metadata.gokb.title.history.historyEvent.each { he ->
      he.from.each { fromIt ->
        Node precTitle = NodeFactory.createURI('http://www.gokb.org/data/titles/'+fromIt.internalId.text());
        graph.add(new Triple(titleUri,bibo_precededBy_pred,precTitle));

      }
      he.to.each { fromItto ->
        Node precTitle = NodeFactory.createURI('http://www.gokb.org/data/titles/'+to.internalId.text());
        graph.add(new Triple(titleUri,bibo_succeeded_pred,precTitle));

      }
    }


  }

  OaiClient oaiclient_platforms = new OaiClient(host:config.oai_server+'gokb/oai/platforms');
  oaiclient_platforms.getChangesSince(null, 'gokb') { record ->
    Node platformUri = NodeFactory.createURI('http://www.gokb.org/data/platforms/'+record.metadata.gokb.platform.@id);
    graph.add(new Triple(platformUri, skos_pref_label_pred, NodeFactory.createLiteral(record.metadata.gokb.platform.name.text()) ));

    record.metadata.gokb.platform.variantNames.variantName.each {
      graph.add(new Triple(platformUri, skos_alt_label_pred,  NodeFactory.createLiteral(it.text())));
    }

    graph.add(new Triple(platformUri, type_pred, dc_service_type));

    graph.add(new Triple(platformUri, bibo_status_pred, NodeFactory.createLiteral(record.metadata.gokb.platform.status.text())));
    
    graph.add(new Triple(platformUri, mods_locationUrl_pred, NodeFactory.createLiteral(record.metadata.gokb.platform.primaryUrl.text())));

    graph.add(new Triple(platformUri, stac_authenticationMethod_pred, NodeFactory.createLiteral(record.metadata.gokb.platform.authentication.text())));

    record.metadata.gokb.platform.identifiers.identifier.each {
      graph.add(new Triple(platformUri, owl_same_as_pred,  NodeFactory.createLiteral(it.text())));
    }
    record.metadata.gokb.platform.variantNames.variantName.each {
      graph.add(new Triple(platformUri, skos_alt_label_pred,  NodeFactory.createLiteral(it.text())));
    }

  }
  
  
  OaiClient oaiclient_packages = new OaiClient(host:config.oai_server+'gokb/oai/packages');
  oaiclient_packages.getChangesSince(null, 'gokb') { record ->
    Node packageUri = NodeFactory.createURI('http://www.gokb.org/data/packages/'+record.metadata.gokb.package.@id);
    graph.add(new Triple(packageUri, skos_pref_label_pred, NodeFactory.createLiteral(record.metadata.gokb.package.name.text()) ));
    record.metadata.gokb.package.identifiers.identifier.each {
      graph.add(new Triple(packageUri, owl_same_as_pred,  NodeFactory.createLiteral(it.text())));
    }

    graph.add(new Triple(packageUri, type_pred, dc_collection_type))

    graph.add(new Triple(packageUri, dc_type_type, NodeFactory.createLiteral(record.metadata.gokb.package.scope.text())))
    graph.add(new Triple(packageUri, schema_paymenttype_type, NodeFactory.createLiteral(record.metadata.gokb.package.paymentType.text())))
    record.metadata.gokb.package.variantNames.variantName.each {
      graph.add(new Triple(packageUri, skos_alt_label_pred,  NodeFactory.createLiteral(it.text())));
    }

    def providedby = NodeFactory.createLiteral(record.metadata.gokb.package.nominalProvider.text())
    graph.add(new Triple(packageUri, service_providedby_pred,providedby))
    
    record.metadata.gokb.package.TIPPs.TIPP.each { tipp ->
      Node tippUri = NodeFactory.createURI('http://www.gokb.org/data/packages/'+tipp.@id);
      def prefLabel = tipp.title.name.text() + ' in package ' + record.metadata.gokb.package.name.text() + ' via ' + tipp.platform.name.text()
      graph.add(new Triple(tippUri, skos_pref_label_pred, NodeFactory.createLiteral(prefLabel)))
      graph.add(new Triple(tippUri, type_pred, dc_collection_type))
      graph.add(new Triple(tippUri, gokb_hasTitle_pred, NodeFactory.createLiteral('http://www.gokb.org/data/titles/'+tipp.title.@id)));
      graph.add(new Triple(tippUri, gokb_hasPackage_pred, NodeFactory.createLiteral('http://www.gokb.org/data/packages/'+record.metadata.gokb.package.@id)));
      graph.add(new Triple(tippUri, gokb_hasPlatform_pred, NodeFactory.createLiteral('http://www.gokb.org/data/platform/'+record.metadata.gokb.platform.@id)));
      graph.add(new Triple(tippUri,bibo_status_pred, NodeFactory.createLiteral(tipp.status.text())))

      graph.add(new Triple(tippUri, gokb_accessStart_pred, NodeFactory.createLiteral(tipp.access.@start.text())))
      graph.add(new Triple(tippUri, gokb_accessEnd_pred, NodeFactory.createLiteral(tipp.access.@end.text())))

      graph.add(new Triple(tippUri, mods_locationUrl_pred, NodeFactory.createLiteral(tipp.url.text())))

      graph.add(new Triple(tippUri, dc_medium_pred, NodeFactory.createLiteral(tipp.medium.text())))

      graph.add(new Triple(tippUri, gokb_coverageStartDate_pred, NodeFactory.createLiteral(tipp.coverage.@startDate.text())))
      graph.add(new Triple(tippUri, gokb_coverageEndDate_pred, NodeFactory.createLiteral(tipp.coverage.@endDate.text())))
      graph.add(new Triple(tippUri, gokb_coverageStartIssue_pred, NodeFactory.createLiteral(tipp.coverage.@startIssue.text())) )
      graph.add(new Triple(tippUri, gokb_coverageEndIssue_pred, NodeFactory.createLiteral(tipp.coverage.@endIssue.text())) )
      graph.add(new Triple(tippUri, gokb_coverageStartVolume_pred, NodeFactory.createLiteral(tipp.coverage.@startVolume.text())) )
      graph.add(new Triple(tippUri, gokb_coverageEndVolume_pred, NodeFactory.createLiteral(tipp.coverage.@endVolume.text())) )
      graph.add(new Triple(tippUri, gokb_coverageEmbargo_pred, NodeFactory.createLiteral(tipp.coverage.@embargo.text())) )

      graph.add(new Triple(tippUri, mods_identifier_pred, NodeFactory.createLiteral(tipp.@id.text() )))

      graph.add(new Triple(tippUri, gokb_belongsToPkg_pred, packageUri ))

    }
    
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
