/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.ssl;

import static com.datastax.dsbulk.commons.internal.io.IOUtils.assertAccessibleFile;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SslHandlerFactoryFactory {

  @Nullable
  public static SslHandlerFactory createSslHandlerFactory(LoaderConfig config)
      throws GeneralSecurityException, IOException {
    String sslProvider = config.getString("provider");
    switch (sslProvider.toLowerCase()) {
      case "none":
        return null;
      case "jdk":
        return createJdkSslHandlerFactory(config);
      case "openssl":
        return createNettySslHandlerFactory(config);
      default:
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for dsbulk.driver.ssl.provider, expecting None, JDK, or OpenSSL, got: '%s'",
                sslProvider));
    }
  }

  private static SslHandlerFactory createJdkSslHandlerFactory(LoaderConfig config)
      throws GeneralSecurityException, IOException {
    KeyManagerFactory kmf = createKeyManagerFactory(config);
    TrustManagerFactory tmf = createTrustManagerFactory(config);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        kmf != null ? kmf.getKeyManagers() : null,
        tmf != null ? tmf.getTrustManagers() : null,
        new SecureRandom());
    List<String> cipherSuites = config.getStringList("cipherSuites");
    SslEngineFactory sslEngineFactory =
        new ProgrammaticSslEngineFactory(
            sslContext, cipherSuites.isEmpty() ? null : cipherSuites.toArray(new String[0]), true);
    return new JdkSslHandlerFactory(sslEngineFactory);
  }

  private static SslHandlerFactory createNettySslHandlerFactory(LoaderConfig config)
      throws GeneralSecurityException, IOException {
    if (config.hasPath("openssl.keyCertChain") != config.hasPath("openssl.privateKey")) {
      throw new BulkConfigurationException(
          "Settings "
              + "dsbulk.driver.ssl.openssl.keyCertChain"
              + " and "
              + "dsbulk.driver.ssl.openssl.privateKey"
              + " must be provided together or not at all when using the OpenSSL provider");
    }
    TrustManagerFactory tmf = createTrustManagerFactory(config);
    SslContextBuilder builder =
        SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(tmf);
    if (config.hasPath("openssl.keyCertChain")) {
      Path sslOpenSslKeyCertChain = config.getPath("openssl.keyCertChain");
      Path sslOpenSslPrivateKey = config.getPath("openssl.privateKey");
      assertAccessibleFile(sslOpenSslKeyCertChain, "OpenSSL key certificate chain file");
      assertAccessibleFile(sslOpenSslPrivateKey, "OpenSSL private key file");
      builder.keyManager(
          new BufferedInputStream(new FileInputStream(sslOpenSslKeyCertChain.toFile())),
          new BufferedInputStream(new FileInputStream(sslOpenSslPrivateKey.toFile())));
    }
    List<String> cipherSuites = config.getStringList("cipherSuites");
    if (!cipherSuites.isEmpty()) {
      builder.ciphers(cipherSuites);
    }
    SslContext sslContext = builder.build();
    return new NettySslHandlerFactory(sslContext);
  }

  private static TrustManagerFactory createTrustManagerFactory(LoaderConfig config)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    if (config.hasPath("truststore.path") != config.hasPath("truststore.password")) {
      throw new BulkConfigurationException(
          "Settings "
              + "dsbulk.driver.ssl.truststore.path"
              + ", "
              + "dsbulk.driver.ssl.truststore.password"
              + " and "
              + "dsbulk.driver.ssl.truststore.algorithm"
              + " must be provided together or not at all");
    }
    TrustManagerFactory tmf = null;
    if (config.hasPath("truststore.path")) {
      Path sslTrustStorePath = config.getPath("truststore.path");
      assertAccessibleFile(sslTrustStorePath, "SSL truststore file");
      String sslTrustStorePassword = config.getString("truststore.password");
      String sslTrustStoreAlgorithm = config.getString("truststore.algorithm");
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new BufferedInputStream(new FileInputStream(sslTrustStorePath.toFile())),
          sslTrustStorePassword.toCharArray());
      tmf = TrustManagerFactory.getInstance(sslTrustStoreAlgorithm);
      tmf.init(ks);
    }
    return tmf;
  }

  private static KeyManagerFactory createKeyManagerFactory(LoaderConfig config)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
          UnrecoverableKeyException {
    if (config.hasPath("keystore.path") != config.hasPath("keystore.password")) {
      throw new BulkConfigurationException(
          "Settings "
              + "dsbulk.driver.ssl.keystore.path"
              + ", "
              + "dsbulk.driver.ssl.keystore.password"
              + " and "
              + "dsbulk.driver.ssl.truststore.algorithm"
              + " must be provided together or not at all when using the JDK SSL provider");
    }

    KeyManagerFactory kmf = null;
    if (config.hasPath("keystore.path")) {
      Path sslKeyStorePath = config.getPath("keystore.path");
      assertAccessibleFile(sslKeyStorePath, "SSL keystore file");
      String sslKeyStorePassword = config.getString("keystore.password");
      String sslTrustStoreAlgorithm = config.getString("truststore.algorithm");
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new BufferedInputStream(new FileInputStream(sslKeyStorePath.toFile())),
          sslKeyStorePassword.toCharArray());

      kmf = KeyManagerFactory.getInstance(sslTrustStoreAlgorithm);
      kmf.init(ks, sslKeyStorePassword.toCharArray());
    }
    return kmf;
  }
}
