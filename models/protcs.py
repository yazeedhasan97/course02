import os.path
import subprocess

import logging

from models.utils import Model

try:
    import jks
    from OpenSSL import crypto

    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509 import Certificate
except:
    pass


class JKSConverter:
    def __init__(self, path, password, logger=None):
        self._logger = logger if logger else logging.getLogger(__name__)
        self.__path = path
        self.__password = password
        self.__keystore = self.__load_keystore()

    @property
    def path(self):
        return self.__path

    @path.setter
    def path(self, new_path):
        if not new_path:
            self._logger.error("Path cannot be empty.")
            raise ValueError("Path cannot be empty.")
        self.__path = new_path
        self.__keystore = self.__load_keystore()

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, new_password):
        if not new_password:
            self._logger.error("Password cannot be empty.")
            raise ValueError("Password cannot be empty.")
        self.__password = new_password
        self.__keystore = self.__load_keystore()

    def __load_keystore(self):
        try:
            with open(self.__path, 'rb') as f:
                keystore_data = f.read()
        except IOError as e:
            self._logger.error(f"Error reading keystore file: {e}")
            raise IOError("Error reading keystore file.")

        try:
            p12 = pkcs12.load_key_and_certificates(
                keystore_data, self.__password.encode(), backend=default_backend())
            self._logger.info("Keystore loaded successfully as PKCS12.")
            return p12
        except ValueError:
            self._logger.info("Failed to load as PKCS12, trying JKS/JCEKS.")

        try:
            keystore = jks.KeyStore.loads(keystore_data, self.__password)
            self._logger.info("Keystore loaded successfully as JKS/JCEKS.")
            return keystore
        except jks.util.BadKeystoreFormatException:
            self._logger.error("Keystore file format is not supported.")
            raise ValueError("Keystore file format is not supported.")

    def extract_key_and_cert(self, alias):
        if isinstance(self.__keystore, tuple):
            private_key, cert, additional_certs = self.__keystore
            if private_key and cert:
                return private_key, cert
            else:
                raise ValueError("No private key or certificate found.")
        elif alias in self.__keystore.private_keys:
            pk_entry = self.__keystore.private_keys[alias]
            private_key = serialization.load_der_private_key(
                pk_entry.pkey, password=None, backend=default_backend())
            cert = crypto.load_certificate(crypto.FILETYPE_ASN1, pk_entry.cert_chain[0][1])
            return private_key, cert
        else:
            raise ValueError(f"No private key found for alias {alias}")

    def convert_to_pem(self, private_key, cert):
        if not isinstance(private_key, crypto.PKey):
            private_key = self.load_private_key(private_key)
        if not isinstance(cert, crypto.X509):
            cert = self.load_certificate(cert)
        private_key_pem = crypto.dump_privatekey(crypto.FILETYPE_PEM, private_key).decode('utf-8')
        cert_pem = crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode('utf-8')
        return private_key_pem, cert_pem

    def load_private_key(self, private_key):
        if isinstance(private_key, rsa.RSAPrivateKey):
            key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption())
            pkey_openssl = crypto.load_privatekey(crypto.FILETYPE_ASN1, key_der)
            return pkey_openssl
        elif isinstance(private_key, bytes):
            if b'-----BEGIN' in private_key:
                return crypto.load_privatekey(crypto.FILETYPE_PEM, private_key)
            else:
                return crypto.load_privatekey(crypto.FILETYPE_ASN1, private_key)
        else:
            raise TypeError("Unsupported private key type.")

    def convert_jks_to_pem(self, alias):
        try:
            private_key, cert = self.extract_key_and_cert(alias)
            return self.convert_to_pem(private_key, cert)

        except Exception as e:
            self._logger.error(f"Error converting JKS to PEM: {e}")
            raise

    def load_certificate(self, cert):
        if isinstance(cert, Certificate):
            cert_der = cert.public_bytes(serialization.Encoding.DER)
            return crypto.load_certificate(crypto.FILETYPE_ASN1, cert_der)
        elif isinstance(cert, bytes):
            return crypto.load_certificate(crypto.FILETYPE_PEM,
                                           cert) if b'-----BEGIN' in cert else crypto.load_certificate(
                crypto.FILETYPE_ASN1, cert)
        else:
            raise TypeError("Unsupported certificate type.")


class CryptoHandler:
    def __init__(self, key: bytes, logger=None):
        self._logger = logger if logger else logging.getLogger(__name__)
        self._cipher = Fernet(key)

    def encrypt(self, data: str) -> str:
        """Encrypts the provided string data."""
        return self._cipher.encrypt(data.encode()).decode()

    def decrypt(self, data: str) -> str:
        """Decrypts the provided string data."""
        return self._cipher.decrypt(data.encode()).decode()


###########################################################################################

class QueryConfig(Model):
    def __init__(self, sslrootcert: str = None, storepassword: str = None,
                 sslmode: str = "require", logger=None):

        self._sslrootcert = sslrootcert
        self._storepassword = storepassword
        self._sslmode = sslmode
        self._finalsslrootcert = None
        self._logger = logger if logger else logging.getLogger(__name__)

        if self._sslrootcert.endswith('.jks'):
            self._converter = JKSConverter(path=self._sslrootcert, password=self._storepassword, logger=self._logger)
        else:
            self._converter = None

    @property
    def sslrootcert(self) -> str:
        return self._sslrootcert

    @sslrootcert.setter
    def sslrootcert(self, sslrootcert: str):
        if not isinstance(sslrootcert, str) or sslrootcert == "":
            raise ValueError("SSL certificate must be a non-empty string")
        self._sslrootcert = sslrootcert

    @property
    def storepassword(self) -> str:
        return self._storepassword

    @storepassword.setter
    def storepassword(self, storepassword: str):
        if not isinstance(storepassword, str) or not storepassword:
            raise ValueError("Store password must be a non-empty string")
        self._storepassword = storepassword

    @property
    def sslmode(self) -> str:
        return self._sslmode

    @sslmode.setter
    def sslmode(self, sslmode: str):
        if not isinstance(sslmode, str) or not sslmode:
            raise ValueError("SSL mode must be a non-empty string")
        self._sslmode = sslmode

    def convert_jks_cert(self, alias):
        """
        Converts the JKS SSL certificate to PEM format and returns a new instance.
        The alias is the name of the user within the JKS file to convert.
        """
        try:
            private_key, cert = self._converter.convert_jks_to_pem(alias)

            path = self._write_pem_file(cert)

            # self._finalsslrootcert = cert_pem
            self._finalsslrootcert = path
        except Exception as e:
            self._logger.error(f"Error converting SSL certificate: {e}")
            raise

    def _write_pem_file(self, value):
        if not os.path.exists(self._sslrootcert):
            raise IOError(f"Could not find the cert at {self._sslrootcert}")

        if self._sslrootcert.endswith('.jks'):
            path = self._sslrootcert.replace('.jks', '.cert')

            with open(path, 'tw') as file:
                file.write(value)
        else:
            path = self._sslrootcert
        return path

    def build_db_connect_args(self):
        args = {  # TODO: below only works for presto [tested on IC], other DBs need other parameters
            'protocol': 'https',
            # "requests_kwargs": {'verify': self._finalsslrootcert},
            # TODO: it is highly recommended to provide the certificate, but there is an issue with
            #   validating hostname with the error:
            #   certificate verify failed: Hostname mismatch, certificate is not valid for 'svr-daasname-01.mtn.ci'
            #   even through the certificate is defined to allow '*.mtn.ci'
            "requests_kwargs": {'verify': False},
        }
        return args.copy()


class KerberosConfig(Model):
    def __init__(self,
                 krb5_config: str, principal: str,
                 keytab_path: str, kerberos_service_name: str = 'hive',
                 logger=None
                 ):
        self._krb5_config = krb5_config
        self._principal = principal
        self._keytab_path = keytab_path
        self._kerberos_service_name = kerberos_service_name
        self._logger = logger if logger else logging.getLogger(__name__)
        self.acquire()

    @property
    def krb5_config(self) -> str:
        return self._krb5_config

    @krb5_config.setter
    def krb5_config(self, value: str):
        if not isinstance(value, str) or value == "":
            raise ValueError("Kerberos config must be a non-empty string")
        self._krb5_config = value

    @property
    def principal(self) -> str:
        return self._principal

    @principal.setter
    def principal(self, value: str):
        if not isinstance(value, str) or value == "":
            raise ValueError("Principal must be a non-empty string")
        self._principal = value

    @property
    def keytab_path(self) -> str:
        return self._keytab_path

    @keytab_path.setter
    def keytab_path(self, value: str):
        if not isinstance(value, str) or value == "":
            raise ValueError("Keytab path must be a non-empty string")
        self._keytab_path = value

    @property
    def kerberos_service_name(self) -> str:
        return self._kerberos_service_name

    @kerberos_service_name.setter
    def kerberos_service_name(self, value: str):
        if not isinstance(value, str) or value == "":
            raise ValueError("Kerberos service name must be a non-empty string")
        self._kerberos_service_name = value

    def acquire(self, ):
        command = ['kinit', '-kt', self._keytab_path, self._principal]
        # TODO: self._keytab_path is the password to the principal, principal is the username
        self._logger.info(f"Kerberos command: {' '.join(command)}")

        try:
            # Run the command and capture the output
            result = subprocess.run(
                command,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False
            )

            self._logger.info(f"Kerberos session acquire: Error Code - {result.returncode}")
            if result.returncode == 0:
                return True

            return False
        except subprocess.CalledProcessError as e:
            self._logger.error(f"Failed to acquire Kerberos session due terminal error: {e}")
            raise e
        except Exception as e:
            self._logger.error(f"Failed to acquire Kerberos session due unknown error: {e}")
            raise e

    def build_db_connect_args(self):
        return {
            'auth': 'KERBEROS',
            'kerberos_service_name': self._kerberos_service_name,
            #'configuration': {
            #    'hive.server2.authentication.kerberos.principal': self._principal,
            #    'hive.server2.authentication.kerberos.keytab': self._keytab_path
            #}
        }.copy()