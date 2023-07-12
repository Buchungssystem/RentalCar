package org.car;

import com.fasterxml.jackson.annotation.JsonIgnoreType;
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
        r.getAvailableItems(LocalDate.of(2023, 6, 1), LocalDate.of(2023, 6, 10));
        /*LocalDate startDate = LocalDate.of(2023, 8, 1);
        LocalDate endDate = LocalDate.of(2023, 8, 14);
        r.getAvailableItems(startDate, endDate);*/

        while (true) {
            try (DatagramSocket dgSocket = new DatagramSocket(Participant.rentalCarPort)) {
                byte[] buffer = new byte[65507];
                //DatagramPacket for recieving data
                DatagramPacket dgPacketIn = new DatagramPacket(buffer, buffer.length);
                //byte data of UDP message
                byte[] messageData;
                //response UDP Message
                UDPMessage responeseMessage;
                //byte UDP Message
                byte[] parsedMessage;


                System.out.println("Listening on Port " + Participant.rentalCarPort);
                dgSocket.receive(dgPacketIn);
                String data = new String(dgPacketIn.getData(), 0, dgPacketIn.getLength());
                UDPMessage dataObject = objectMapper.readValue(data, UDPMessage.class);

                switch (dataObject.getOperation()){
                    case PREPARE -> {
                        System.out.println("prepare recieved");
                        messageData = dataObject.getData();
                        data = new String(messageData, 0, messageData.length);
                        BookingData bookingData = objectMapper.readValue(data, BookingData.class);

                        Operations response = r.prepare(bookingData, dataObject.getTransaktionNumber());
                        responeseMessage = new UDPMessage(dataObject.getTransaktionNumber(), new byte[0], SendingInformation.RENTALCAR, response);

                        //send response back to TravelBroker
                        messageData = objectMapper.writeValueAsBytes(responeseMessage);
                        DatagramPacket dgOutPrepare = new DatagramPacket(messageData, messageData.length, Participant.localhost, Participant.travelBrokerPort);

                        dgSocket.send(dgOutPrepare);
                        System.out.println("prepare answered: " + response);
                    }
                    case COMMIT -> {
                        System.out.println("commit recieved");
                        if(r.commit(dataObject.getTransaktionNumber())) {
                            responeseMessage = new UDPMessage(dataObject.getTransaktionNumber(), new byte[0], SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responeseMessage);
                            DatagramPacket dgOutCommit = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.travelBrokerPort);

                            dgSocket.send(dgOutCommit);
                            System.out.println("commit answered - Ok");
                        }
                    }
                    case ABORT -> {
                        System.out.println("abort recieved");
                        if(r.abort(dataObject.getTransaktionNumber())){
                            responeseMessage = new UDPMessage(dataObject.getTransaktionNumber(), new byte[0], SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responeseMessage);
                            DatagramPacket dgOutAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.travelBrokerPort);

                            dgSocket.send(dgOutAbort);
                            System.out.println("abort answered");
                        }
                    }
                    case AVAILIBILITY -> {
                        messageData = dataObject.getData();
                        AvailabilityData availabilityData = objectMapper.readValue(messageData, AvailabilityData.class);

                        ArrayList<Object> availableItems = r.getAvailableItems(availabilityData.getStartDate(), availabilityData.getEndDate());
                        System.out.println(availableItems.get(0));

                        //Des macht nix Sinn diese - If einfach weg und leere nachricht schicken, besser als gar nix schicken??
                        if(!(availableItems.size() == 0)){
                            System.out.println("happpepeeeendndnendndn");
                            byte[] parsedItems = objectMapper.writeValueAsBytes(availableItems);
                            UDPMessage responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), parsedItems, SendingInformation.RENTALCAR, Operations.AVAILIBILITY);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);
                            //Datagrampacket for sending the response
                            DatagramPacket dgPacketOut = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.travelBrokerPort);
                            dgSocket.send(dgPacketOut);

                        }
                    }
                }

            } catch (Exception e) {

            }
        }
    }
}