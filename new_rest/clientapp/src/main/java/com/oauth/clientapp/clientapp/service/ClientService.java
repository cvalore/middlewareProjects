package com.oauth.clientapp.clientapp.service;

public interface ClientService {

      boolean addUser(String username, String password);

      boolean logUser(String username, String password);

      String getClientId();

      String getClientSecret();

      String getRedirectUrl();

      String getTokenPath();

      String getResourcePath();

      String checkForToken(String username);

      boolean addUsernamesToken(String username, String accessToken);

      void deleteToken(String username);
}
