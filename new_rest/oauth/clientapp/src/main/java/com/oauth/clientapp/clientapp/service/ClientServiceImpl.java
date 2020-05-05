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
            return Database.getClientId();
      }

      @Override
      public String getClientSecret() {
            return Database.getClientSecret();
      }

      @Override
      public String getRedirectUrl() {
            return Database.getRedirectUrl();
      }

      @Override
      public String getTokenPath() {
            return Database.getTokenPath();
      }

      @Override
      public String getResourcePath() {
            return Database.getResourceUrl();
      }

      @Override
      public String checkForToken(String username) {
            return Database.checkForToken(username);
      }


      @Override
      public boolean addUsernamesToken(String username, String accessToken) {
            return Database.addUsernameToken(username, accessToken);
      }
}
