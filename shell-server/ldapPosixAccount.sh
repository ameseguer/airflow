#!/bin/sh

SERVER=openldap
BASE_DN="$1"
BIND_DN="$2"
PASSW="$3"
LDAP_UID="$4"
LDIFF="/tmp/ldiff"


# get students group
LDAP_GROUP=$(ldapsearch -LLL  -h "$SERVER" -b "cn=students,ou=groups,dc=akamaipartnertraining,dc=com" \
-D "$BIND_DN" -w"$PASSW" gidNumber |sed '/^$/d'|tail -n1 |awk -F':' '{print $2}') &&

# fetch all users with no posixAccount
ldapsearch -LLL  -h "$SERVER" -b "$BASE_DN" \
-D "$BIND_DN" -w"$PASSW"\
 '(&(!(objectClass=posixAccount))(objectClass=organizationalPerson))'  'dn' |\
 sed '/^$/d' |\

while read line ; do
    username=$(echo "$line"|awk -F ',' '{print $1}'|awk -F '=' '{print $2}')
    echo "adding posixaccount to $username uidNumber=$LDAP_UID"
    echo "$line" >> $LDIFF
    echo "changetype: modify" >>  $LDIFF 
    echo "replace: objectclass" >>  $LDIFF
    echo "objectclass: inetOrgPerson" >>  $LDIFF
    echo "objectclass: organizationalPerson" >>  $LDIFF
    echo "objectclass: top" >>  $LDIFF
    echo "objectclass: posixAccount" >>  $LDIFF
    echo "-" >>  $LDIFF
    echo "add: gidNumber" >>  $LDIFF
    echo "gidNumber: $LDAP_GROUP" >> $LDIFF
    echo "-" >>  $LDIFF
    echo "add: homeDirectory" >>  $LDIFF
    echo "homeDirectory: /home/$username" >> $LDIFF
    echo "-" >>  $LDIFF
    echo "add: uidNumber" >>  $LDIFF
    echo "uidNumber: $LDAP_UID" >> $LDIFF
    echo "-" >>  $LDIFF
    echo "add: loginShell" >>  $LDIFF
    echo "loginShell: /bin/bash" >> $LDIFF
    echo "" >> $LDIFF
    LDAP_UID=$(($LDAP_UID+1))
done &&

if [ -f "$LDIFF" ] ; then 
    ldapmodify   -h "$SERVER"  -D "$BIND_DN" -w"$PASSW" < $LDIFF >> /dev/null;
    rm "$LDIFF"
fi
