package com.oauth.clientapp.clientapp.service;

import com.oauth.clientapp.clientapp.model.Database;
import org.springframework.stereotype.Service;

@Service("clientService")
public class ClientServiceImpl implements ClientService {
      @Override
      public boolean addUser(String username, String password) {
            return Database.addUsernamePassword(username, password);
      }

      @Override
      public boolean logUser(String username, String password) {
            return Database.logUsernamePassword(username, password);
      }

      @Override
      public String getClientId() {
            return Database.CLIENT_ID;
      }

      @Override
      public String getClientSecret() {
            return Database.CLIENT_SECRET;
      }

      @Override
      public String getRedirectUrl() {
            return Database.REDIRECT_URL;
      }

      @Override
      public String getTokenPath() {
            return Database.TOKEN_PATH;
      }

      @Override
      public String getResourcePath() {
            return Database.RESOURCE_URL;
      }

      @Override
      public String checkForToken(String username) {
            return Database.checkForToken(username);
      }

      @Override
      public boolean addUsernamesToken(String username, String accessToken) {
            return Database.addUsernameToken(username, accessToken);
      }

      @Override
      public void deleteToken(String username) {
            Database.deleteToken(username);
      }
}
