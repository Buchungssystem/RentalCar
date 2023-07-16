package org.car;
import org.utils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.utils.Operations;
import org.utils.Participant;
import org.utils.SendingInformation;
import org.utils.UDPMessage;

import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RentalCar extends Participant{

    public DatagramSocket dgSocket;

    private final static Logger LOGGER = Logger.getLogger(Execute.class.getName());

    public RentalCar(){
        try{
            dgSocket = new DatagramSocket(Participant.rentalCarPort);
        }catch(Exception e){
            LOGGER.log(Level.SEVERE, "Socket was not set", e);
        }
    }

    @Override
    public Operations prepare(BookingData bookingData, UUID transactionId) {
        DatabaseConnection dbConn = new DatabaseConnection();
        LocalDate startDate = bookingData.getStartDate();
        LocalDate endDate = bookingData.getEndDate();
        int requestedId = bookingData.getSelectedCar();

        try(Connection con = dbConn.getConn()) {

            //Check if there is already any Booking over this Item
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            ResultSet rs = stm.executeQuery();

            //loop through responded table and check if cars are available or not
            while (rs.next()) {
                if(rs.getInt("carID") == requestedId){
                    LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                    LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                    if (!isAvailable(startDate, endDate, startDateEntry, endDateEntry)) {
                        //if not available return abort
                        return Operations.ABORT;
                    }

                }
            }

            //Car is available
            stm = con.prepareStatement("INSERT INTO booking VALUES (\"" + transactionId + "\", \"" + startDate + "\", \"" + endDate + "\", 0, " + bookingData.getSelectedCar() + ")");
            stm.executeUpdate();

            return Operations.READY;
        }catch (Exception e){
            LOGGER.log(Level.SEVERE, "Something went wrong with the database connection", e);
            return Operations.ABORT;
        }

    }

    @Override
    public boolean commit(UUID transactionId){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("UPDATE booking SET stable = 1 WHERE bookingID = \"" + transactionId + "\"");
            stm.executeUpdate();
            //return true since the statement was successful
            return true;
        }catch(Exception e){
            LOGGER.log(Level.SEVERE, "Something went wrong with the database connection", e);
            //return false since the transaction wasn't successful set stable
            return false;
        }
    }

    @Override
    public boolean abort(UUID transactionId){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("DELETE FROM booking WHERE bookingID = \"" + transactionId + "\"");
            stm.executeUpdate();
            //return true since the statement was successful
            return true;
        }catch(Exception e){
            LOGGER.log(Level.SEVERE, "Something went wrong with the database connection", e);
            //return false since the transaction wasn't successful aborted
            return false;
        }
    }

    @Override
    public ArrayList<Object> getAvailableItems(LocalDate startDate, LocalDate endDate) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableCarIds = new ArrayList<>();
        ResultSet rs;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            //loop through result table and check if cars are available or not
            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if (!isAvailable(startDate, endDate, startDateEntry, endDateEntry)) {
                    //add room if not available to exclude in query later
                    availableCarIds.add(rs.getString("carID"));
                }
            }

            //build string to be in right format for the sql query
            String availableCarsIds = "";
            for (int i = 0; i < availableCarIds.size(); i++) {
                if( i < availableCarIds.size() - 1){
                    availableCarsIds += availableCarIds.get(i) + ", ";
                }else{
                    availableCarsIds += availableCarIds.get(i);
                }
            }

            stm = con.prepareStatement("SELECT * FROM car WHERE carID NOT IN (?)");
            //append prepared string at the position of "?"
            stm.setString(1, availableCarsIds);
            rs = stm.executeQuery();
            ArrayList<Object> availableCars = new ArrayList<>();
            //add all available rooms to returned table
            while(rs.next()){
                availableCars.add(new Car(rs.getInt("carID"), rs.getString("brand"), rs.getString("type")));
            }

            return availableCars;

        }catch(Exception e){
            LOGGER.log(Level.SEVERE, "Something went wrong with the database connection", e);
            //return null since there couldn't be any cars selected
            return null;
        }
    }

    public boolean isAvailable(LocalDate startDate, LocalDate endDate, LocalDate startDateEntry, LocalDate endDateEntry){
        if (startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate)) {
            return true;
        }
        return false;
    }

    public Operations requestDecision(UUID transactionId) {
        DatabaseConnection dbConn = new DatabaseConnection();

        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT stable FROM booking WHERE bookingID = \"" + transactionId + "\"");
            ResultSet rs = stm.executeQuery();
            if(!rs.next()){
                return null;
            }

            int stable = rs.getInt("stable");
            if(stable == 1){
                // return commit since transaction has been set to stable
                return Operations.COMMIT;
            }else{
                // return prepare since we also only received prepare and got no response from it
                return Operations.PREPARE;
            }

        }catch(Exception e){
            LOGGER.log(Level.SEVERE, "Something went wrong with the database connection", e);
            return Operations.PREPARE;
        }
    }
}

