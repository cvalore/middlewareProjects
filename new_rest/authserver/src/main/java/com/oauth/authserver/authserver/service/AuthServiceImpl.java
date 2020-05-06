package com.oauth.authserver.authserver.service;

import com.oauth.authserver.authserver.model.*;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import io.jsonwebtoken.Jwts;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

@Service("authService")
public class AuthServiceImpl implements AuthService {

      private Connection conn = null;
      private Statement stmt = null;
      private String query = null;

      private static final String EXPIRED_CODE_TABLE = "oauth_expired_codes";
      private static final String ISSUED_CODE_TABLE = "oauth_issued_codes";

      @Autowired
      private PasswordEncoder passwordEncoder;

      @Override
      public boolean verifyClientInfo(String clientId, String redirectUrl) {
            List<ClientDetails> clientDetailsList = new ArrayList<>();

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT client_id, redirect_url FROM oauth_client_details";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        clientDetailsList.add(
                                    new ClientDetails(
                                                res.getString("client_id"),
                                                res.getString("redirect_url")
                                    )
                        );
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

            for(ClientDetails c : clientDetailsList) {
                  if(c.getClientId().equals(clientId) && c.getRedirectUrl().equals(redirectUrl)) {
                        return true;
                  }
            }
            return false;
      }

      @Override
      public boolean verifyClientInfo(String clientId, String redirectUrl, String clientSecret, String grantType) {
            List<ClientDetails> clientDetailsList = new ArrayList<>();

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT client_id, redirect_url, client_secret, authorized_grant_types FROM oauth_client_details";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        clientDetailsList.add(
                                    new ClientDetails(
                                                res.getString("client_id"),
                                                res.getString("redirect_url"),
                                                res.getString("client_secret"),
                                                res.getString("authorized_grant_types")
                                    )
                        );
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

            for(ClientDetails c : clientDetailsList) {
                  if(c.getClientId().equals(clientId) && c.getRedirectUrl().equals(redirectUrl)
                        && c.getClientSecret().equals(clientSecret) && c.getGrantType().contains(grantType)) {
                        return true;
                  }
            }
            return false;
      }

      @Override
      public boolean verifyUserInfo(String username, String password) {
            List<UserDetails> userDetailsList = new ArrayList<>();

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT username, password FROM oauth_users";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        userDetailsList.add(
                                    new UserDetails(
                                                res.getString("username"),
                                                res.getString("password")
                                    )
                        );
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

            for(UserDetails u : userDetailsList) {
                  if(u.getUsername().equals(username) && passwordEncoder.matches(password, u.getPassword())) {
                        return true;
                  }
            }
            return false;
      }

      @Override
      public String issueAuthCode(String clientId, String redirectUrl, String username) {
            String concat = clientId.concat(":").concat(redirectUrl).concat(":").concat(username);
            byte[] bytes = concat.getBytes();
            byte[] encoded = Base64.getEncoder().encode(bytes);
            String authCode = new String(encoded, StandardCharsets.UTF_8);
            Date expires = new Date(System.currentTimeMillis() + Database.EXPIRES_AUTH_CODE*60*1000);

            if(alreadyExistsCode(authCode, ISSUED_CODE_TABLE)) {
                  updateExpiration(authCode);
                  return authCode;
            }

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "INSERT INTO oauth_issued_codes " +
                              "VALUES ('"+ authCode +"','"+ clientId +"','"
                              + redirectUrl +"','"+ new Timestamp(expires.getTime()) +"')";
                  stmt.executeUpdate(query);
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

            return authCode;
      }

      private void updateExpiration(String authCode) {
            Date expires = new Date(System.currentTimeMillis() + Database.EXPIRES_AUTH_CODE*60*1000);

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "UPDATE oauth_issued_codes SET expires_in = '" +
                              new Timestamp(expires.getTime()) +
                              "' WHERE code = '" + authCode + "'";
                  stmt.executeUpdate(query);
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
      }

      private boolean alreadyExistsCode(String authCode, String tableName) {
            List<String> codes = new ArrayList<>();
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT code FROM "+tableName+" WHERE code = '" + authCode + "'";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        codes.add(res.getString("code"));
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
            for(String s : codes) {
                  if(s.equals(authCode)) {
                        return true;
                  }
            }
            return false;
      }

      @Override
      public boolean isExpired(String authCode, String clientId, String redirectUrl) {
            Timestamp expiresIn = null;
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT expires_in FROM oauth_issued_codes WHERE code = '" + authCode + "'";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        expiresIn = res.getTimestamp("expires_in");
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

            if(expiresIn == null) {
                  return true;
            }

            long now = new Date(System.currentTimeMillis()).getTime();
            boolean expired = now > expiresIn.getTime();

            removeAllExpiredCodes();

            return expired;
      }

      private void removeAllExpiredCodes() {
            List<Code> expiredCodes = findExpiredCodes();
            for(Code c : expiredCodes) {
                  removeExpiredCode(c.getAuthCode(), c.getClientId(), c.getRedirectUrl());
            }
      }

      private List<Code> findCodes() {
            List<Code> codes = new ArrayList<>();
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT * FROM oauth_issued_codes";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        codes.add(
                                    new Code(
                                                res.getString("code"),
                                                res.getString("client_id"),
                                                res.getString("redirect_url"),
                                                res.getTimestamp("expires_in")
                                    )
                        );
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

            return codes;
      }

      private List<Code> findExpiredCodes() {
            List<Code> codes = findCodes();
            return codes
                        .stream()
                        .filter(code ->
                                    new Date(System.currentTimeMillis()).getTime() >
                                                code.getExpiresIn().getTime())
                        .collect(Collectors.toList());
      }

      private void removeExpiredCode(String authCode, String clientId, String redirectUrl) {

            boolean doInsert = !alreadyExistsCode(authCode, EXPIRED_CODE_TABLE);

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  if(doInsert) {
                        query = "INSERT INTO oauth_expired_codes VALUES('" + authCode + "', '" + clientId + "', '" + redirectUrl + "')";
                        stmt.executeUpdate(query);
                  }
                  query = "DELETE FROM oauth_issued_codes WHERE code='" + authCode +"'";
                  stmt.executeUpdate(query);
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
      }

      @Override
      public boolean matchesCodeClient(String authCode, String clientId, String redirectUrl) {
            List<Code> codes = findCodes();
            Code code = null;
            for(Code c : codes) {
                  if(c.getAuthCode().equals(authCode)) {
                        code = c;
                  }
            }
            if(code == null) {
                  return false;
            }
            return code.getClientId().equals(clientId) && code.getRedirectUrl().equals(redirectUrl);
      }

      @Override
      public String issueToken(String authCode) {
            String token;
            Date expirationDate = new Date(System.currentTimeMillis()+Database.EXPIRES_TOKEN*60*1000);
            SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
            byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(Database.TEST_KEY);
            Key key = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());

            token = Jwts.builder()
                        .setIssuer(Database.ME)
                        .setSubject(authCode)
                        .setExpiration(expirationDate)
                        .signWith(signatureAlgorithm, key)
                        .compact();

            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "INSERT INTO oauth_issued_tokens " +
                              "VALUES ('"+ token +"','"+ authCode +"')";
                  stmt.executeUpdate(query);
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

            byte[] credDecoded = Base64.getDecoder().decode(authCode);
            String credentials = new String(credDecoded, StandardCharsets.UTF_8);
            String [] values = credentials.split(":", 3);

            removeExpiredCode(authCode, values[0], values[1]);

            return token;
      }

      @Override
      public boolean checkTokenMatches(String accessToken, String clientId, String clientSecret) {
            Token token = null;
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT * FROM oauth_issued_tokens WHERE token = '" + accessToken + "'";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        token = new Token(
                                    res.getString("token"),
                                    res.getString("code")
                        );
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

            if(token == null) {
                  return false;
            }

            Claims claim = decodeToken(accessToken);
            if(claim == null) {
                  return false;
            }
            ClientDetails client = getClientForCode(claim.getSubject());
            return
                        client.getClientId() != null && client.getClientSecret() != null &&
                        client.getClientId().equals(clientId) &&
                        client.getClientSecret().equals(clientSecret);
      }

      private ClientDetails getClientForCode(String authCode) {
            ClientDetails client = null;
            //search into active codes
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT client_id FROM oauth_issued_codes WHERE code = '" + authCode + "'";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        client = new ClientDetails();
                        client.setClientId(res.getString("client_id"));
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

            //search into expired codes
            if(client == null) {
                  try {
                        conn = Database.getConnection();
                        if(conn == null) {
                              throw new RuntimeException("Cannot connect to db");
                        }
                        stmt = conn.createStatement();
                        query = "SELECT client_id FROM oauth_expired_codes WHERE code = '" + authCode + "'";
                        ResultSet res = stmt.executeQuery(query);
                        while(res.next()){
                              client = new ClientDetails();
                              client.setClientId(res.getString("client_id"));
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
            }

            if(client != null) {
                  try {
                        conn = Database.getConnection();
                        if(conn == null) {
                              throw new RuntimeException("Cannot connect to db");
                        }
                        stmt = conn.createStatement();
                        query = "SELECT client_secret FROM oauth_client_details WHERE client_id = '" + client.getClientId() + "'";
                        ResultSet res = stmt.executeQuery(query);
                        while(res.next()){
                              client.setClientSecret(res.getString("client_secret"));
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
            }
            return client;
      }

      @Override
      public boolean checkExpiredToken(String accessToken) {
            Claims claim = decodeToken(accessToken);
            if(claim == null) {
                  return true;
            }
            boolean expired = new Date(System.currentTimeMillis()).getTime() > claim.getExpiration().getTime();
            if(expired) {
                  removeAllExpiredTokens();
            }
            return expired;
      }

      private Claims decodeToken(String accessToken) {
            Claims claims;
            try {
                  claims = Jwts.parser()
                              .setSigningKey(DatatypeConverter.parseBase64Binary(Database.TEST_KEY))
                              .parseClaimsJws(accessToken).getBody();

            }
            catch (SignatureException e) {
                  e.printStackTrace();
                  return null;
            }

            return claims;
      }

      private void removeAllExpiredTokens() {
            List<Token> expiredTokens = findExpiredTokens();
            for(Token t : expiredTokens) {
                  removeExpiredToken(t.getToken(), t.getCode());
            }
      }

      private List<Token> findTokens() {
            List<Token> tokens = new ArrayList<>();
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "SELECT * FROM oauth_issued_tokens";
                  ResultSet res = stmt.executeQuery(query);
                  while(res.next()){
                        tokens.add(
                                    new Token(
                                                res.getString("token"),
                                                res.getString("code")
                                    )
                        );
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

            return tokens;
      }

      private List<Token> findExpiredTokens() {
            List<Token> tokens = findTokens();
            List<Token> expiredTokens = new ArrayList<>();
            for(Token t : tokens) {
                  Claims claim = decodeToken(t.getToken());
                  if(claim != null) {
                        if(new Date(System.currentTimeMillis()).getTime() >
                              claim.getExpiration().getTime()) {
                              expiredTokens.add(t);
                        }
                  }
            }
            return expiredTokens;
      }

      private void removeExpiredToken(String token, String code) {
            try {
                  conn = Database.getConnection();
                  if(conn == null) {
                        throw new RuntimeException("Cannot connect to db");
                  }
                  stmt = conn.createStatement();
                  query = "INSERT INTO oauth_expired_tokens VALUES('"+ token +"', '"+ code +"')";
                  stmt.executeUpdate(query);
                  query = "DELETE FROM oauth_issued_tokens WHERE token='" + token +"'";
                  stmt.executeUpdate(query);
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
      }


}
