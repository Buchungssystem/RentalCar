package org.car;

import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mysql.cj.log.Log;
import org.utils.*;

import javax.xml.crypto.Data;
import java.io.File;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.file.Path;
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

    private static SharedRessourcesTimerThread sharedRessourcesTimerThread;

    public static void main(String[] args) {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());


        RentalCar r = new RentalCar();

        //recovery from crash

        //check if logs are empty since we wouldn't need to recover if there are no logs
        if(logWriter.isLogsNotEmpty()){
            File dir = new File(logWriter.getDirectory());
            File[] files = dir.listFiles();


            //loop through all files of logs directory
            for(File currentContextFile : files){

                String name = currentContextFile.getName();
                name = name.substring(0, name.length() - 4);

                //write context back to contextMap
                UUID transactionId = UUID.fromString(name);
                transactionContext = logWriter.readLogFile(transactionId);
                transactionContextMap.put(transactionId, transactionContext);

                LOGGER.log(Level.INFO, "Transaction Context with the ID: " + transactionId + " was restored");
            }

            //loop through context map and react accordingly
            for(Map.Entry<UUID, TransactionContext> entry : transactionContextMap.entrySet()){
                //get transaction current context
                TransactionContext currContext = entry.getValue();

                UUID transactionId = entry.getKey();
                switch(entry.getValue().getCurrentState()){
                    case PREPARE -> {
                        LOGGER.log(Level.INFO, "restore pepare was triggered");
                        //the prepare was answered but the decision was never received

                        //if we answered the prepare with abort there has nothing to be done. Since the coordinator decides abort based of our response.
                        //we didn't set a booking since we answered with abort

                        //if we answered commit we need to request what decission was made from the coordinator
                        if(currContext.getDecission() == Operations.READY){
                            UDPMessage message = new UDPMessage(transactionId, SendingInformation.RENTALCAR, Operations.REQUESTDECISION);

                            try {
                                byte[] parsedMessage = objectMapper.writeValueAsBytes(message);
                                DatagramPacket dpRequestDecission = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, currContext.getCoordinatorPort());

                                r.dgSocket.send(dpRequestDecission);
                            }catch (Exception e){
                                LOGGER.log(Level.SEVERE, "There was an error with sending the Request of the Decision - " + transactionId, e);
                            }
                        }
                    }
                }

            }
        }

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

                        sharedRessourcesTimerThread = new SharedRessourcesTimerThread(false);

                        //start timer thread
                        //if there wasn't any Response received after 30 seconds the coordinator likely crashed, so we request if the other participant knows the response
                        TimerThread timerThread = new TimerThread(transactionId, 30, Participant.rentalCarPort, false, sharedRessourcesTimerThread);
                        timerThread.start();

                        //log prepare answered
                        transactionContext = new TransactionContext(States.PREPARE, originPort, true, response, sharedRessourcesTimerThread);
                        logWriter.write(transactionId, transactionContext);
                        transactionContextMap.put(transactionId, transactionContext);


                        //Fehlerfall 2
                        //System.exit(0);
                    }
                    case COMMIT -> {
                        //we received a response from coordinator, so we can cancel the timerThread
                        sharedRessourcesTimerThread = transactionContextMap.get(transactionId).getSharedRessourcesTimerThread();

                        sharedRessourcesTimerThread.setInterrupt(true);

                        LOGGER.log(Level.INFO, "2PC: Commit - " + transactionId);
                        if(transactionContextMap.get(transactionId) != null){
                            r.commit(dataObject.getTransaktionNumber());
                            //create and parse response
                            responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            DatagramPacket dgOutCommit;

                            //send response to corresponding travelBroker instance
                            if(dataObject.isRecovery()){
                                 dgOutCommit = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, transactionContextMap.get(transactionId).getCoordinatorPort());
                            }else {
                                 dgOutCommit = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, originPort);
                            }

                            r.dgSocket.send(dgOutCommit);

                            //log delete logfile
                            logWriter.delete(transactionId);
                            transactionContextMap.remove(transactionId);
                        }else {
                            //recovery

                            //create and parse response
                            responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            //send response to corresponding travelBroker instance
                            DatagramPacket dgOutCommit = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, originPort);
                            r.dgSocket.send(dgOutCommit);

                            //log delete logfile
                            logWriter.delete(transactionId);
                            transactionContextMap.remove(transactionId);
                        }
                    }
                    case ABORT -> {
                        //we received a response from coordinator, so we can cancel the timerThread
                        sharedRessourcesTimerThread = transactionContextMap.get(transactionId).getSharedRessourcesTimerThread();

                        sharedRessourcesTimerThread.setInterrupt(true);

                        LOGGER.log(Level.INFO, "2PC: Abort - " + transactionId);

                        transactionContext = transactionContextMap.get(transactionId);

                        //check if there is an transaction Context because we already send a abort if received from one participant.
                        //there could be a chance that this participant also returned abort to the prepare request which would lead to no entry for this transactionID
                        //in our transaction ContextMap
                        if (transactionContext != null) {
                            r.abort(transactionId);
                            //prepare answer
                            responseMessage = new UDPMessage(transactionId, SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            //send response to corresponding travelBroker instance
                            DatagramPacket dgOutAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, originPort);
                            r.dgSocket.send(dgOutAbort);

                            //delete logfile
                            logWriter.delete(transactionId);
                            transactionContextMap.remove(transactionId);
                        } else {
                            //prepare answer
                            responseMessage = new UDPMessage(transactionId, SendingInformation.RENTALCAR, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            //send response to corresponding travelBroker instance
                            DatagramPacket dgOutAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, originPort);
                            r.dgSocket.send(dgOutAbort);

                            //delete logfile
                            logWriter.delete(transactionId);
                            transactionContextMap.remove(transactionId);
                        }
                    }
                    case COORDINATORDOWN -> {
                        LOGGER.log(Level.INFO, "Coordinator down");
                        //Timer thread was finished and tells us to request the decision from other participant, since the coordinator likely crashed

                        responseMessage = new UDPMessage(transactionId, SendingInformation.RENTALCAR, Operations.REQUESTDECISION);
                        parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                        DatagramPacket dpRequestDecision = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.hotelPort);

                        r.dgSocket.send(dpRequestDecision);
                    }
                    case REQUESTDECISION -> {
                        LOGGER.log(Level.INFO, "Request decision was received");
                        // the coordinator crashed and the other participant requests if this one already knows the decision
                        Operations decision = r.requestDecision(transactionId);

                        if(decision == null){
                            //response was abort since there was no entry in the db
                            responseMessage = new UDPMessage(transactionId, SendingInformation.RENTALCAR, Operations.ABORT, true);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            DatagramPacket dpDecisionAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.hotelPort);

                            r.dgSocket.send(dpDecisionAbort);
                        }
                        if(decision == Operations.COMMIT){
                            //response was commit since there was a stable entry in the db
                            responseMessage = new UDPMessage(transactionId, SendingInformation.RENTALCAR, decision, true);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);

                            DatagramPacket dpDecisionAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.hotelPort);

                            r.dgSocket.send(dpDecisionAbort);
                        }

                        //otherwise we don't know the decission either and don't send an answer
                        // the protocol is blocket till the coordinator is back life again

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
                        //erster fehlerfall testing
                        //System.exit(0);
                    }
            }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "The Socket or the objectMapper threw an error", e);
            }
        }
    }
}