package org.car;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.utils.*;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.UUID;

public class Execute {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        RentalCar r = new RentalCar();
        /*LocalDate startDate = LocalDate.of(2023, 8, 1);
        LocalDate endDate = LocalDate.of(2023, 8, 14);
        r.getAvailableItems(startDate, endDate);*/

        while (true) {
            try (DatagramSocket dgSocket = new DatagramSocket(Participant.rentalCarPort)) {
                byte[] buffer = new byte[65507];
                //DatagramPacket for recieving data
                DatagramPacket dgPacketIn = new DatagramPacket(buffer, buffer.length);

                System.out.println("Listening on Port " + Participant.rentalCarPort);
                dgSocket.receive(dgPacketIn);
                String data = new String(dgPacketIn.getData(), 0, dgPacketIn.getLength());
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
                        byte[] messageData = dataObject.getData();
                        AvailabilityData availabilityData = objectMapper.readValue(messageData, AvailabilityData.class);

                        ArrayList<Object> availableItems = r.getAvailableItems(availabilityData.getStartDate(), availabilityData.getEndDate());
                        System.out.println(availableItems.get(0));
                        if(!(availableItems == null)){
                            byte[] parsedItems = objectMapper.writeValueAsBytes(availableItems);
                            UDPMessage responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), parsedItems, SendingInformation.RENTALCAR, Operations.AVAILIBILITY);
                            byte [] parsedMessage = objectMapper.writeValueAsBytes(responseMessage);
                            //Datagrampacket for sending the response
                            DatagramPacket dgPacketOut = new DatagramPacket(parsedMessage, parsedMessage.length, r.localhost, r.travelBrokerPort);
                            dgSocket.send(dgPacketOut);

                        }
                    }
                }

            } catch (Exception e) {

            }
        }
    }
}