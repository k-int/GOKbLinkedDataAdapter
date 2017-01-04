#!/usr/bin/groovy

// @GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
@GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/')
@GrabResolver(name='kint', root='http://nexus.k-int.com/content/repositories/releases')
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

  //changed the graph uri
  graph = new VirtGraph('uri://localhost:8890/', config.store_uri, "dba", "dba");

  //These are all URIs that are stored into variables?
  Node foaf_org_type = NodeFactory.createURI('http://xmlns.com/foaf/0.1/Organization');
  Node foaf_agent_type = NodeFactory.createURI('http://xmlns.com/foaf/0.1/Agent');
//Replace with isAccessibleForFree  Node schema_paymenttype_type = NodeFactory.createURI('http://schema.org/PaymentMethod')
  Node schema_org_organization_type = NodeFactory.createURI('http://schema.org/Organization');
  Node bibframe_organization_type = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/Organization');
  Node bibframe_work_type = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/Work');
  Node dbpedia_organisation_type = NodeFactory.createURI('http://dbpedia.org/ontology/Organisation');
  Node owl_thing_type = NodeFactory.createURI('http://www.w3.org/2002/07/owl#Thing');
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

  Node bibframe_electronicLocator_pred = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/electronicLocator')
  Node bibframe_precededBy_pred = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/precededBy')
  Node bibframe_role_pred = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/role');
  Node bibframe_status_pred = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/status');
  Node bibframe_succeededBy_pred = NodeFactory.createURI('http://id.loc.gov/ontologies/bibframe/succeededBy');

  Node service_provides_pred = NodeFactory.createURI('http://dini-ag-kim.github.io/service-ontology/service.html#provides')
  Node dc_format_pred = NodeFactory.createURI('http://purl.org/dc/terms/format');
  Node dc_identifier_pred = NodeFactory.createURI('http://purl.org/dc/terms/identifier')
  Node dc_publisher_pred = NodeFactory.createURI('http://purl.org/dc/terms/publisher');
  Node dc_type_pred = NodeFactory.createURI('http://purl.org/dc/terms/type');

  Node datacite_issn_pred = NodeFactory.createURI('http://purl.org/spar/datacite/issn')
  Node datacite_eissn_pred = NodeFactory.createURI('http://purl.org/spar/datacite/eissn')

  Node gokb_tipp_type = NodeFactory.createURI('http://gokb.org/type/#tipp');
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

  Node stac_authenticationMethod_pred = NodeFactory.createURI('http://securitytoolbox.appspot.com/stac#AuthenticationMethod');
  Node schema_provider_pred = NodeFactory.createURI('http://schema.org/provider');
  Node schema_isAccessibleForFree_pred = NodeFactory.createURI('http://schema.org/isAccessibleForFree');

//Orgs
  println("Connect to ${config.oai_server}gokb/oai/orgs");

  OaiClient oaiclient_orgs = new OaiClient(host:config.oai_server+'gokb/oai/orgs');

  oaiclient_orgs.getChangesSince(null, 'gokb') { record ->
    try{
      println("Org... ${record.header.identifier}");
      println("       ${record.metadata.gokb.org.@id}");
      println("       ${record.metadata.gokb.org.name.text()}");

      Node orgUri = NodeFactory.createURI("${config.base_resource_url}/data/orgs/" +record.metadata.gokb.org.@id);

      addToGraph(orgUri, type_pred, foaf_org_type, true);
      addToGraph(orgUri, type_pred, foaf_agent_type, true);
      addToGraph(orgUri, type_pred, schema_org_organization_type, true);
      addToGraph(orgUri, type_pred, bibframe_organization_type, true);
      addToGraph(orgUri, type_pred, dbpedia_organisation_type, true);
      addToGraph(orgUri, type_pred, owl_thing_type, true);
      addToGraph(orgUri, skos_pref_label_pred, record.metadata.gokb.org.name.text(),false);
      addUriToGraph(orgUri, foaf_homepage_pred, record.metadata.gokb.org.homepage?.text(),false);

      record.metadata.gokb.org.identifiers.identifier.each {
        if ( it.@datatype == 'uri' ) {
          addUriToGraph(orgUri, owl_same_as_pred,it.text(),false);
        } else if ( it.text().toLowerCase().startsWith('http') ) {
            addUriToGraph(orgUri, owl_same_as_pred,it.text(),false);
          } else {
          addToGraph(orgUri, owl_same_as_pred,it.text(),false);
          }
        }

      record.metadata.gokb.org.variantNames.variantName.each {
        addToGraph(orgUri, skos_alt_label_pred, it.text(),false);
      }

      record.metadata.gokb.org.roles.role.each {
        addToGraph(orgUri, bibframe_provider_role_pred, it.text(),false);
      }

      addToGraph(orgUri,gokb_mission_pred, record.metadata.gokb.org.mission.text(),false);
    }catch(Exception e){
      println "EXCEPTION WHILE PROCESSING ORGS"
      e.printStackTrace();
    }
  }

//Titles
  println("Connect to ${config.oai_server}gokb/oai/titles");

  OaiClient oaiclient_titles = new OaiClient(host:config.oai_server+'gokb/oai/titles');
  oaiclient_titles.getChangesSince(null, 'gokb') { record ->
    try{
      println("Process title with id:: ${record.metadata.gokb.title.@id}");

//Create URI for the selected title resource
      Node titleUri = NodeFactory.createURI("${config.base_resource_url}/data/titles/" +record.metadata.gokb.title.@id);
      addToGraph(titleUri, type_pred, bibframe_work_type, true);
      addToGraph(titleUri, dc_type_pred, record.metadata.gokb.title.medium.text(),false);

      addToGraph(titleUri, skos_pref_label_pred,record.metadata.gokb.title.name.text(), false);
      if ( record.metadata.gokb.title.publisher.@id?.text()?.trim() ) {
        Node publisher = NodeFactory.createURI("${config.base_resource_url}/data/orgs/" +record.metadata.gokb.title.publisher.@id);
        addToGraph(titleUri, dc_publisher_pred, publisher, true);
      }

//Make sure the value is a URL (begins with http). Don't want text values
      record.metadata.gokb.title.identifiers.identifier.each {
        if ( it.text().toLowerCase().startsWith('http') ) {
            addUriToGraph(titleUri, owl_same_as_pred,it.text(),false);
          }
        }

      record.metadata.gokb.title.variantNames.variantName.each {
       addToGraph(titleUri, skos_alt_label_pred, it.@value.text(),false);
      }

      record.metadata.gokb.title.identifiers.identifier.each {
        if(it.@namespace == 'issn'){
           addUriToGraph(titleUri, datacite_issn_pred, 'issn:'+it.@value.text(), false);
           } else if (it.@namespace == 'eissn'){
           addUriToGraph(titleUri, datacite_eissn_pred, 'eissn:'+it.@value.text(), false);
           } else if ( it.@datatype == 'uri' ) {
            addUriToGraph(titleUri, dc_identifier_pred, it.@namespace+':'+it.@value.text(), false);
          } else if (it.text().toLowerCase().startsWith('http')) {
              addUriToGraph(titleUri, dc_identifier_pred, it.@value.text(), false);
            } else {
          addToGraph(titleUri, dc_identifier_pred, it.@value.text(), false);
          }
        }

      // addToGraph(titleUri, bibframe_status_pred, NodeFactory.createLiteral(record.metadata.gokb.title.OAStatus.text()));
      addToGraph(titleUri,gokb_pureOpen_pred, record.metadata.gokb.title.pureOA.text(), false);

      record.metadata.gokb.title.history.historyEvent.each { he ->
        he.from.each { fromIt ->
          internalId_he_from = fromIt.internalId.text()
          if(internalId_he_from){
            Node precTitle = NodeFactory.createURI("${config.base_resource_url}/data/titles/"  + internalId_he_from);
            addToGraph(titleUri,bibframe_precededBy_pred,precTitle, true);
          }

        }
        he.to.each { fromTo ->
          internalId_he_to = fromTo.internalId.text()
          if(internalId_he_to){
            Node precTitle = NodeFactory.createURI("${config.base_resource_url}/data/titles/"  + internalId_he_to);
            addToGraph(titleUri,bibframe_succeededBy_pred,precTitle, true);
          }
        }
      }
    }catch(Exception e){
      println "EXCEPTION WHILE PROCESSING TITLES"
      e.printStackTrace();
    }
  }

//Platforms
  println("Connect to ${config.oai_server}gokb/oai/platforms");
  OaiClient oaiclient_platforms = new OaiClient(host:config.oai_server+'gokb/oai/platforms');
  oaiclient_platforms.getChangesSince(null, 'gokb') { record ->
    try{
      Node platformUri = NodeFactory.createURI("${config.base_resource_url}/data/platforms/" +record.metadata.gokb.platform.@id);
      addToGraph(platformUri, skos_pref_label_pred,record.metadata.gokb.platform.name.text(), false);

      record.metadata.gokb.platform.variantNames.variantName.each {
        addToGraph(platformUri, skos_alt_label_pred, it.text(),false );
      }

      addToGraph(platformUri, type_pred, dc_service_type, true);

      addToGraph(platformUri, bibo_status_pred, record.metadata.gokb.platform.status.text(), false);

      addUriToGraph(platformUri, bibframe_electronicLocator_pred, record.metadata.gokb.platform.primaryUrl.text(), false);

      addToGraph(platformUri, stac_authenticationMethod_pred, record.metadata.gokb.platform.authentication.text(), false);

      record.metadata.gokb.platform.identifiers.identifier.each {
        addUriToGraph(platformUri, owl_same_as_pred, it.text(), false);
      }
      record.metadata.gokb.platform.variantNames.variantName.each {
        addToGraph(platformUri, skos_alt_label_pred, it.text(), false);
      }
    }catch(Exception e){
      println "EXCEPTION WHILE PROCESSING PLATFORMS"
      e.printStackTrace();
    }
  }

//Packages
  println("Connect to ${config.oai_server}gokb/oai/packages");
 OaiClient oaiclient_packages = new OaiClient(host:config.oai_server+'gokb/oai/packages');
  oaiclient_packages.getChangesSince(null, 'gokb') { record ->
    try{
      Node packageUri = NodeFactory.createURI("${config.base_resource_url}/data/packages/" +record.metadata.gokb.package.@id);
      println "Create package with URI: ${packageUri}"
      addToGraph(packageUri, skos_pref_label_pred, record.metadata.gokb.package.name.text(), false);

      addToGraph(packageUri, type_pred, dc_collection_type, true);

      addToGraph(packageUri, dc_type_type, record.metadata.gokb.package.scope.text(), false);
      if (record.metadata.gokb.package.paymentType.text() == "Free"){
      addToGraph(packageUri, schema_isAccessibleForFree_pred, "true", false);
      } else {
      addToGraph(packageUri, schema_isAccessibleForFree_pred, "false", false);
      }
      record.metadata.gokb.package.variantNames.variantName.each {
        addToGraph(packageUri, skos_alt_label_pred, it.text(), false);
      }

      addToGraph(packageUri, schema_provider_pred,record.metadata.gokb.package.nominalProvider.text(), false);

      record.metadata.gokb.package.TIPPs.TIPP.each { tipp ->
        try{
          Node tippUri = NodeFactory.createURI("${config.base_resource_url}/data/tipps/" +tipp.@id);
          def prefLabel = tipp.title.name.text() + ' in package ' + record.metadata.gokb.package.name.text() + ' via ' + tipp.platform.name.text()
          addToGraph(tippUri, skos_pref_label_pred, prefLabel, false);
          addToGraph(tippUri, type_pred, gokb_tipp_type, true);
          if(tipp.title.@id.text().trim()){
            addToGraph(tippUri, gokb_hasTitle_pred, "${config.base_resource_url}/data/titles/" +tipp.title.@id, false)
          }
          if(record.metadata.gokb.package.@id.text().trim()){
            addToGraph(tippUri, gokb_hasPackage_pred, "${config.base_resource_url}/data/packages/" +record.metadata.gokb.package.@id, false)
          }
          if(record.metadata.gokb.platform.@id.text().trim()){
            addToGraph(tippUri, gokb_hasPlatform_pred, "${config.base_resource_url}/data/platform/" +record.metadata.gokb.platform.@id, false)
          }
          addToGraph(tippUri,bibo_status_pred, tipp.status.text(), false)

          addToGraph(tippUri, gokb_accessStart_pred, tipp.access.@start.text(), false)
          addToGraph(tippUri, gokb_accessEnd_pred, tipp.access.@end.text(), false)

          addUriToGraph(tippUri, bibframe_electronicLocator_pred, tipp.url.text(), false)

          addToGraph(tippUri, dc_format_pred, tipp.medium.text(), false)

          addToGraph(tippUri, gokb_coverageStartDate_pred, tipp.coverage.@startDate.text(), false)
          addToGraph(tippUri, gokb_coverageEndDate_pred, tipp.coverage.@endDate.text(), false)
          addToGraph(tippUri, gokb_coverageStartIssue_pred, tipp.coverage.@startIssue.text(),false)
          addToGraph(tippUri, gokb_coverageEndIssue_pred, tipp.coverage.@endIssue.text(), false)
          addToGraph(tippUri, gokb_coverageStartVolume_pred, tipp.coverage.@startVolume.text(), false)
          addToGraph(tippUri, gokb_coverageEndVolume_pred, tipp.coverage.@endVolume.text(), false)
          addToGraph(tippUri, gokb_coverageEmbargo_pred, tipp.coverage.@embargo.text(), false)

          if (tipp.@id.text().toLowerCase().startsWith('http')) {
          addUriToGraph(tippUri, dc_identifier_pred, tipp.@id.text(), false);
          } else {
          addToGraph(tippUri, dc_identifier_pred, tipp.@id.text(), false);
          }

          addToGraph(tippUri, gokb_belongsToPkg_pred, packageUri, true )
        }catch(Exception e){
          println "EXCEPTION WHILE PROCESSING TIPPS"
          e.printStackTrace();
        }
      }
    }catch(Exception e){
      println "EXCEPTION WHILE PROCESSING PACKAGES"
      e.printStackTrace();
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

def addToGraph(subj, pred, obj, isNode){
  if(isNode){
    graph.add(new Triple(subj, pred, obj));
  }else{
    if(obj?.trim()){
      graph.add(new Triple(subj, pred, NodeFactory.createLiteral(obj)));
    }
  }
}


def addUriToGraph(subj, pred, uri, isNode){
  if(isNode){
    graph.add(new Triple(subj, pred, uri));
  }else{
    if(uri?.trim()){
      graph.add(new Triple(subj, pred, NodeFactory.createURI(uri)));
    }
  }
}
