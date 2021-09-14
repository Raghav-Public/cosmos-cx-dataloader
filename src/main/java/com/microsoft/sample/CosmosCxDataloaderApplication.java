package com.microsoft.sample;

import java.util.Random;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.datastax.driver.core.Session;

@SpringBootApplication
public class CosmosCxDataloaderApplication {

	public static void main(String[] args) {
		try {
			SpringApplication.run(CosmosCxDataloaderApplication.class, args);
			
			CosmosCx cosmosCx = new CosmosCx();
			Session session = cosmosCx.getSession();
			int userId = cosmosCx.getMaxValue(session, "userid");
			
			while(true) {
				User user = new User(userId, getSaltString(), getSaltString() + "@microsoft.com");
				userId ++;
				cosmosCx.insertData(session, user);
				System.out.println("Writing user:" + user.toString());
				Thread.sleep(1000 * 60);
			}
		}
		catch(Exception exp) {
			System.out.print(exp.getMessage());
		}
	}
	
	
	private static String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
}
