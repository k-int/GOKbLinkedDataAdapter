GOKbLinkedDataAdapter
=====================

Take GOKb data feeds and maintain a linked data store


Virtuoso reset procedure::

isql
RDF_GLOBAL_RESET ();




Set grapeConfig.xml in ~/.groovy...


TO get the right grape download settings

  <ivysettings>
    <settings defaultResolver="downloadGrapes"/>
    <resolvers>
      <chain name="downloadGrapes">
        <filesystem name="cachedGrapes">
          <ivy pattern="${user.home}/.groovy/grapes/[organisation]/[module]/ivy-[revision].xml"/>
          <artifact pattern="${user.home}/.groovy/grapes/[organisation]/[module]/[type]s/[artifact]-[revision].[ext]"/>
        </filesystem>

        <ibiblio name="codehaus" root="http://repository.codehaus.org/" m2compatible="true"/>
        <ibiblio name="snapshots.codehaus" root="http://snapshots.repository.codehaus.org/" m2compatible="true"/>
        <ibiblio name="apache" root="http://people.apache.org/repo/m2-ibiblio-rsync-repository/" m2compatible="true"/>
        <ibiblio name="apache-incubating" root="http://people.apache.org/repo/m2-incubating-repository/" m2compatible="true"/>
        <ibiblio name="maven" root="http://repo2.maven.org/maven2/" m2compatible="true"/>
      </chain>
    </resolvers>
  </ivysettings>



Copy 

virtjdbc-4.1.jar
and
virtjena-2.jar

to 



~/.groovy/grapes/virtuoso/virtjdbc/jars/
and
~/.groovy/grapes/virtuoso/virtjena/jars/
A

mkdir -p ~/.groovy/grapes/virtuoso/virtjdbc/jars/
mkdir -p ~/.groovy/grapes/virtuoso/virtjena/jars
cp virtjdbc-4.1.jar ~/.groovy/grapes/virtuoso/virtjdbc/jars/
cp virtjena-2.jar ~/.groovy/grapes/virtuoso/virtjena/jars/



To avoid slow startup downloading grapes


groovy -Dgroovy.grape.autoDownload=false  ./xxx.groovy


