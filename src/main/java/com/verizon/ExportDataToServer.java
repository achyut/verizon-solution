/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.verizon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

/**
 *
 * @author gokarna
 */
public class ExportDataToServer {
    
    public void sendDataToRESTService(String data){
        
        
        try {
            String url = "http://localhost:8080";

            HttpClient client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(url);
            List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
            urlParameters.add(new BasicNameValuePair("data", data));
            
            HttpEntity stringEnt = new StringEntity(data);
            post.setEntity(stringEnt);
            
            HttpResponse response = client.execute(post);
            System.out.println("Response Code : "+ response.getStatusLine().getStatusCode());
        } catch (IOException ex) {
           System.out.println("Unable to send data to streaming service..");
        }
        
        
        
    }
}
