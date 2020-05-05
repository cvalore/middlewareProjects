package com.oauth.clientapp.clientapp.controller;

import com.oauth.clientapp.clientapp.model.ParameterStringBuilder;
import com.oauth.clientapp.clientapp.service.ClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

@Controller
public class ClientController {

      private ClientService clientService;

      @Autowired
      public ClientController(ClientService clientService) {
            this.clientService = clientService;
      }

      @GetMapping(value = "/signin")
      public String signin() {
            return "signinForm";
      }

      @PostMapping(value = "/signin")
      public String signinRequest(
                  @RequestParam("username") String username,
                  @RequestParam("password") String password,
                  Model model) {

            boolean ok = clientService.addUser(username, password);
            if (!ok) {
                  return "error";
            }

            model.addAttribute("username", username);
            /*model.addAttribute("client_id", clientService.getClientId());
            model.addAttribute("redirect_url", clientService.getRedirectUrl());*/
            return "loggedForm";
      }

      @GetMapping(value = "/login")
      public String login() {
            return "loginForm";
      }

      @PostMapping(value = "/login")
      public String loginRequest(
                  @RequestParam("username") String username,
                  @RequestParam("password") String password,
                  Model model) {

            boolean ok = clientService.logUser(username, password);
            if (!ok) {
                  return "error";
            }

            model.addAttribute("username", username);
            /*model.addAttribute("client_id", clientService.getClientId());
            model.addAttribute("redirect_url", clientService.getRedirectUrl());*/
            return "loggedForm";
      }

      @PostMapping(value = "/checkForToken")
      public void checkForToken(
                  @RequestParam("username") String username,
                  HttpServletResponse response
      ) {
            String token = clientService.checkForToken(username);
            if(token != null) {
                  System.out.println("Token PRESENT");
                  System.out.println("token : " + token);

                  //redirect to the resource server
                  String resRedirect = clientService.getResourcePath()
                              + "?access_token=" + token
                              ;

                  response.setHeader("Location", resRedirect);
                  response.setStatus(HttpStatus.MOVED_PERMANENTLY.value());
            }
            else {
                  System.out.println("Token NOT present");
                  //do the request for token to
                  //"http://localhost:8081/authserver/api/auth?response_type=code&client_id=" +
                  //            clientId + "&redirect_url=" + redirectUrl;
                  String authRedirect = "http://localhost:8081/authserver/api/auth" +
                              "?response_type=code&client_id=" +
                              clientService.getClientId() +
                              "&redirect_url=" +
                              clientService.getRedirectUrl();

                  response.setHeader("Location", authRedirect);
                  response.setStatus(HttpStatus.MOVED_PERMANENTLY.value());
            }
      }

      @GetMapping(value = "/callback")
      public String callback(
                  @RequestParam("username") String username,
                  @RequestParam("authorization_code") String authCode,
                  Model model
      ) {
            if(authCode == null) {
                  return "error";
            }

            String accessToken;

            try {
                  Map<String, String> parameters = new HashMap<>();
                  parameters.put("grant_type", "authorization_code");
                  parameters.put("authorization_code", authCode);
                  parameters.put("client_id", clientService.getClientId());
                  parameters.put("client_secret", clientService.getClientSecret());
                  parameters.put("redirect_url", clientService.getRedirectUrl());

                  URL url = new URL(clientService.getTokenPath());
                  HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                  conn.setRequestMethod("POST");
                  conn.setDoOutput(true);
                  DataOutputStream out = new DataOutputStream(conn.getOutputStream());
                  out.writeBytes(ParameterStringBuilder.getParamsString(parameters));
                  out.flush();
                  out.close();

                  int status = conn.getResponseCode();

                  if(status != HttpStatus.OK.value()) {
                        System.out.println("OK SONO QUA");
                        return "error";
                  }

                  //read response
                  BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                  String inputLine;
                  StringBuffer content = new StringBuffer();
                  while ((inputLine = in.readLine()) != null) {
                        content.append(inputLine);
                  }
                  in.close();

                  //must contains the access_token parameter
                  if(!content.toString().contains("access_token:")) {
                        return "error";
                  }

                  int index = content.toString().indexOf("access_token:") + "access_token:".length();
                  accessToken = content.toString().substring(index);

                  conn.disconnect();

            } catch (IOException e) {
                  e.printStackTrace();
                  return "error";
            }

            System.out.println("access token = " + accessToken);
            if(! clientService.addUsernamesToken(username, accessToken)) {
                  return "error";
            }

            model.addAttribute("username", username);
            return "loggedForm";
      }
}
