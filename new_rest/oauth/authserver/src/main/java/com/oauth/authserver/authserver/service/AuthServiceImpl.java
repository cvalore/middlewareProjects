package com.oauth.authserver.authserver.service;

import com.oauth.authserver.authserver.model.Database;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Date;

import io.jsonwebtoken.Jwts;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import static java.nio.charset.StandardCharsets.US_ASCII;

@Service("authService")
public class AuthServiceImpl implements AuthService {

      private static final String TEST_KEY = "test_key";

      private Connection conn = null;
      private Statement stmt = null;
      private String query = null;

      @Override
      public boolean verifyClientInfo(String clientId, String redirectUrl) {

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT * FROM oauth_users";
                  ResultSet res = stmt.executeQuery(query);
                  System.out.println("oauth_users: ");
                  while(res.next()){
                        System.out.println("-> " + res.getString("username"));
                        System.out.println("-> " + res.getString("password"));
                        System.out.println("-> " + res.getString("enabled"));
                        System.out.println("-> " + "-------------");
                  }
                  res.close();
            } catch (SQLException e) {e.printStackTrace();}
            finally {
                  try {
                        if (stmt != null){
                              stmt.close();
                        }
                  } catch (SQLException e) {e.printStackTrace();}
                  try {
                        if (conn != null) {
                              conn.close();
                        }
                  } catch (SQLException e) {
                        e.printStackTrace();
                  }
            }

            //todo
            return true;
      }

      @Override
      public boolean verifyUserInfo(String username, String password) {

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT * FROM oauth_user_authorities";
                  ResultSet res = stmt.executeQuery(query);
                  System.out.println("oauth_user_authorities: ");
                  while(res.next()){
                        System.out.println("-> " + res.getString("username"));
                        System.out.println("-> " + res.getString("authority"));
                        System.out.println("-> " + "-------------");
                  }
                  res.close();
            } catch (SQLException e) {e.printStackTrace();}
            finally {
                  try {
                        if (stmt != null){
                              stmt.close();
                        }
                  } catch (SQLException e) {e.printStackTrace();}
                  try {
                        if (conn != null) {
                              conn.close();
                        }
                  } catch (SQLException e) {
                        e.printStackTrace();
                  }
            }

            //todo
            return true;
      }

      @Override
      public String issueAuthCode(String clientId, String redirectUrl) {
            /*byte[] credDecoded = Base64.getDecoder().decode(authToken);
            String credentials = new String(credDecoded, StandardCharsets.UTF_8);*/
            String concat = clientId.concat(":").concat(redirectUrl);
            byte[] bytes = concat.getBytes();
            byte[] encoded = Base64.getEncoder().encode(bytes);
            String authCode = new String(encoded, StandardCharsets.UTF_8);

            //TODO
            //add to the db with expire time ecc

            return authCode;
      }

      @Override
      public String issueToken(String authCode) {
            String token;
            final int EXP_MINUTES = 5;
            Date expirationDate = new Date(System.currentTimeMillis()+EXP_MINUTES*60*1000);
            SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
            byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(TEST_KEY);
            Key key = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());

            token = Jwts.builder()
                        .setIssuer("My-Authorization-Server")//todo should be a url to the auth
                        .setSubject(authCode)
                        .setExpiration(expirationDate)
                        .signWith(signatureAlgorithm, key)
                        .compact();

            return token;
      }

      @Override
      public String getTestKey() {
            return TEST_KEY;
      }
}
