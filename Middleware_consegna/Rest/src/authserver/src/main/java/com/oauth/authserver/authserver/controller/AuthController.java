package com.oauth.authserver.authserver.controller;

import com.oauth.authserver.authserver.service.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

@Controller
public class AuthController {
      private AuthService authService;


      @Autowired
      public AuthController(AuthService authService) {
            this.authService = authService;
      }

      @GetMapping(value = "/auth")
      public String auth(
                  @RequestParam("response_type") String responseType,
                  @RequestParam("client_id") String clientId,
                  @RequestParam("redirect_url") String redirectUrl,
                  Model model
      ) {
            if(responseType == null || !responseType.equals("code") || clientId == null || redirectUrl == null) {
                  return "error";
            }
            if(! authService.verifyClientInfo(clientId, redirectUrl)) {
                  return "error";
            }
            model.addAttribute("clientId", clientId);
            model.addAttribute("redirectUrl", redirectUrl);
            return "grantForm";
      }

      @PostMapping(value = "/signin")
      public void auth(
                  @RequestParam("username") String username,
                  @RequestParam("password") String password,
                  @RequestParam("client_id") String clientId,
                  @RequestParam("redirect_url") String redirectUrl,
                  HttpServletResponse response
      ) {
            if(username == null || password == null || clientId == null || redirectUrl == null) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  return;
            }
            if(! authService.verifyClientInfo(clientId, redirectUrl)) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  return;
            }
            if(! authService.verifyUserInfo(username, password)) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  return;
            }

            //String authCode = "this_should_be_the_code";
            String authCode = authService.issueAuthCode(clientId, redirectUrl, username);
            System.out.println("Issued authorization code : " + authCode);

            response.setHeader("Location", redirectUrl +
                        "?username="+ username + "&authorization_code=" + authCode);
            response.setStatus(HttpStatus.MOVED_PERMANENTLY.value());
      }

      @ResponseBody
      @PostMapping(value = "/token")
      public String token(
                  @RequestParam("grant_type") String grantType,
                  @RequestParam("authorization_code") String authCode,
                  @RequestParam("client_id") String clientId,
                  @RequestParam("client_secret") String clientSecret,
                  @RequestParam("redirect_url") String redirectUrl,
                  HttpServletResponse response
      ) {

            if(grantType == null || !grantType.equals("authorization_code") || authCode == null || clientId == null || clientSecret == null || redirectUrl == null) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  return "error";
            }

            if(! authService.verifyClientInfo(clientId, redirectUrl, clientSecret, grantType)) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  return "error";
            }

            if(! authService.matchesCodeClient(authCode, clientId, redirectUrl)) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  return "error";
            }

            if(authService.isExpired(authCode, clientId, redirectUrl)) {
                  response.setStatus(HttpStatus.UNAUTHORIZED.value());
                  System.out.println("Auth code is expired");
                  return "error";
            }

            response.setStatus(HttpStatus.OK.value());
            return "access_token:"+authService.issueToken(authCode);
      }

      @ResponseBody
      @PostMapping(value = "/checkToken")
      public boolean checkToken(
                  @RequestParam("access_token") String accessToken,
                  @RequestParam("client_id") String clientId,
                  @RequestParam("client_secret") String clientSecret ) {

            return authService.checkTokenMatches(accessToken, clientId, clientSecret) &&
                        !authService.checkExpiredToken(accessToken);
      }

}
