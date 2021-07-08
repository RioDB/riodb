

     HTTPS, keystore, and authentication for RioDB


Below are steps to configure HTTPS API.
non-encrypted HTTP does not require these steps. 

-------------------------------------------------------------------------------------
STEP 1:  Generate the keystore with the following command:
(provide a password when prompted)

keytool -genkeypair -keyalg RSA -alias selfsigned -keystore keystore.jks -storepass pass_for_self_signed_cert -dname "CN=localhost, OU=Developers, O=Bull Bytes, L=Linz, C=AT"

-------------------------------------------------------------------------------------
STEP2:  update the configuration file (conf/riodb.conf) 

At the bottom of the conf file, uncomment and edit the HTTPS parameters
indicating the https_port number, 
the path to the keystore.jks file name
the keystore password. 

-------------------------------------------------------------------------------------
STEP3:  Setup authentication/authorization

The configuration file (conf/riodb.conf) also has a parameter for credentials_file
It is optional. To enable authentication/authorization, uncomment the credentials_file parameter
and point it to a path of the credentials file. 
By default, a credentials file is provided under .access/users.dat
The default configuration file has user  "admin"  with default password  "riodb"



