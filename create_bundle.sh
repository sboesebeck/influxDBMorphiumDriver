#!/bin/bash

mvn package gpg:sign -Dgpg.passphrase="$GPGPWD" -Dmaven.test.skip=true $@ || exit 1
echo "creating bundle"
version=$(grep '<version>' pom.xml | head -n1 | tr -d ' a-z<>/')
echo "Version $version"
cd target
rm -f bundle_${version}.jar*
jar -cvf bundle_${version}.jar influxdbdriver-${version}.pom influxdbdriver-${version}.pom.asc influxdbdriver-${version}.jar.asc influxdbdriver-${version}.jar influxdbdriver-${version}-javadoc.jar influxdbdriver-${version}-javadoc.jar.asc influxdbdriver-${version}-sources.jar.asc influxdbdriver-${version}-sources.jar
