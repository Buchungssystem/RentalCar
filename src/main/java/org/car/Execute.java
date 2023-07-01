package org.car;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.utils.Operations;
import org.utils.UDPMessage;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.UUID;

public class Execute {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        RentalCar r = new RentalCar();
        LocalDate startDate = LocalDate.of(2023, 8, 1);
        LocalDate endDate = LocalDate.of(2023, 8, 14);
        r.getAvailableItems(startDate, endDate, UUID.randomUUID());
        while (true) {
            try (DatagramSocket dgSocket = new DatagramSocket(4445)) {
                byte[] buffer = new byte[65507];
                DatagramPacket dgPacket = new DatagramPacket(buffer, buffer.length);
                System.out.println("Listening on Port 4445..");
                dgSocket.receive(dgPacket);
                String data = new String(dgPacket.getData(), 0, dgPacket.getLength());
                UDPMessage dataObject = objectMapper.readValue(data, UDPMessage.class);

                switch (dataObject.getOperation()){
                    case PREPARE -> {

                    }
                    case COMMIT -> {

                    }
                    case ABORT -> {

                    }
                    case READY -> {

                    }
                    case AVAILIBILITY -> {
                        // retrieve data -> probably start and endDate
                        // call availableItems method
                        // return res
                    }
                }

            } catch (Exception e) {

            }
        }
    }
}