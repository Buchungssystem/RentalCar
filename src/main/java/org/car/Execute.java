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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Execute {

    private final static Logger LOGGER = Logger.getLogger(Execute.class.getName());

    private static TransactionContext transactionContext;

    private static Map<UUID, TransactionContext> transactionContextMap = new HashMap<>();

    private static LogWriter<TransactionContext> logWriter = new LogWriter<>();

    public static void main(String[] args) {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());


        RentalCar r = new RentalCar();

        while (true) {
            try {

                //byte data of UDP message
                byte[] messageData;

                //byte UDP Message
                byte[] parsedMessage;
                byte[] buffer = new byte[65507];
                //DatagramPacket for receiving data
                DatagramPacket dgPacketIn = new DatagramPacket(buffer, buffer.length);
                //response UDPMessage
                UDPMessage responseMessage;

                LOGGER.log(Level.INFO, "RentalCar listening on Port: " + Participant.rentalCarPort);
                r.dgSocket.receive(dgPacketIn);

                //string data to parse it into Objects
                String data = new String(dgPacketIn.getData(), 0, dgPacketIn.getLength());
                //parsed UDP message
                UDPMessage dataObject = objectMapper.readValue(data, UDPMessage.class);

                //store TransactionID for context on further processing
                UUID transactionId = dataObject.getTransaktionNumber();

                //store originPort of the requesting travelBroker
                int originPort = dataObject.getOriginPort();

                switch (dataObject.getOperation()) {
                    case PREPARE -> {
                        //log prepare received but not answered
                        transactionContext = new TransactionContext(States.PREPARE, originPort, false);
                        logWriter.write(transactionId, transactionContext);
                        transactionContextMap.put(transactionId, transactionContext);

                        LOGGER.log(Level.INFO, "2PC: Prepare - " + transactionId);

                        //get data of message
                        messageData = dataObject.getData();
                        data = new String(messageData, 0, messageData.length);
                        BookingData bookingData = objectMapper.readValue(data, BookingData.class);

                        //run actual prepare and safe the response (Commit or Abort)
                        Operations response = r.prepare(bookingData, dataObject.getTransaktionNumber());
                        responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), SendingInformation.RENTALCAR, response);

                        //send response back to corresponding TravelBroker instance
                        messageData = objectMapper.writeValueAsBytes(responseMessage);
                        DatagramPacket dgOutPrepare = new DatagramPacket(messageData, messageData.length, Participant.localhost, originPort);

                        r.dgSocket.send(dgOutPrepare);

                        //log prepare answered
                        transactionContext = new TransactionContext(States.PREPARE, originPort, true);
                        logWriter.write(transactionId, transactionContext);
                        transactionContextMap.put(transactionId, transactionContext);
                    }
                    case COMMIT -> {
                        //log commit received but not answered
                        transactionContext = new TransactionContext(States.COMMIT, originPort, false);
                        logWriter.write(transactionId, transactionContext);
                        transactionContextMap.put(transactionId, transactionContext);

                        LOGGER.log(Level.INFO, "2PC: Commit - " + transactionId);

                        //run actual commit and check if transaction was completed successfully if not, no answer will be sent
                        if (r.commit(dataObject.getTransaktionNumber())) {
                            //create and parse response
                            responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            //send response to corresponding travelBroker instance
                            DatagramPacket dgOutCommit = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, originPort);
                            r.dgSocket.send(dgOutCommit);

                            //log commit answered
                            transactionContext = new TransactionContext(States.COMMIT, originPort, true);
                            logWriter.write(transactionId, transactionContext);
                            transactionContextMap.put(transactionId, transactionContext);
                        }
                    }
                    case ABORT -> {
                        //log abort but not answered
                        transactionContext = new TransactionContext(States.ABORT, originPort, false);
                        logWriter.write(transactionId, transactionContext);
                        transactionContextMap.put(transactionId, transactionContext);

                        LOGGER.log(Level.INFO, "2PC: Abort - " + transactionId);

                        transactionContext = transactionContextMap.get(transactionId);

                        //check if there is an transaction Context because we already send a abort if received from one participant.
                        //there could be a chance that this participant also returned abort to the prepare request which would lead to no entry for this transactionID
                        //in our transaction ContextMap
                        if (transactionContext != null) {
                            //run actual abort and check if aborted successfully
                            if (r.abort(dataObject.getTransaktionNumber())) {
                                //prepare answer
                                responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), SendingInformation.RENTALCAR, Operations.OK);
                                parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                                //send response to corresponding travelBroker instance
                                DatagramPacket dgOutAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, originPort);
                                r.dgSocket.send(dgOutAbort);

                                //log abort answered
                                transactionContext = new TransactionContext(States.ABORT, originPort, true);
                                logWriter.write(transactionId, transactionContext);
                                transactionContextMap.put(transactionId, transactionContext);
                            }
                        }
                    }

                case AVAILIBILITY -> {
                    LOGGER.log(Level.INFO, "Availability: request from travelBroker");

                    //parse availability data back to class
                    messageData = dataObject.getData();
                    AvailabilityData availabilityData = objectMapper.readValue(messageData, AvailabilityData.class);

                    //run actual availability check with requested params and store available rooms
                    ArrayList<Object> availableItems = r.getAvailableItems(availabilityData.getStartDate(), availabilityData.getEndDate());

                    //if rooms not null prepare data and send answer
                    if (!(availableItems == null)) {
                        byte[] parsedItems = objectMapper.writeValueAsBytes(availableItems);
                        responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), parsedItems, SendingInformation.RENTALCAR, Operations.AVAILIBILITY, Participant.rentalCarPort);
                        parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                        //send response to corresponding travelBroker instance
                        DatagramPacket dgOutAvailability = new DatagramPacket(parsedMessage, parsedMessage.length, r.localhost, originPort);
                        r.dgSocket.send(dgOutAvailability);

                    }
                }
            }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "The Socket or the objectMapper threw an error", e);
            }
        }
    }
}