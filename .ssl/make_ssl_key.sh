#!/bin/sh
keytool -genkeypair -keyalg RSA -alias selfsigned -keystore keystore.jks -storepass pass_for_self_signed_cert -dname "CN=localhost, OU=Developers, O=Bull Bytes, L=Linz, C=AT"