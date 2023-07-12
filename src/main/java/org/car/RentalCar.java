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

public class RentalCar extends Participant{

    public DatagramSocket dgSocket;

    public RentalCar(){
        try{
            dgSocket = new DatagramSocket(Participant.rentalCarPort);
        }catch(Exception e){
            System.out.println("Socket was not set: " + e.getMessage());
        }
    }

    @Override
    public Operations prepare(BookingData bookingData, UUID transaktionId) {
        DatabaseConnection dbConn = new DatabaseConnection();
        LocalDate startDate = bookingData.getStartDate();
        LocalDate endDate = bookingData.getEndDate();
        int requestedId = bookingData.getSelectedCar();

        try(Connection con = dbConn.getConn()) {

            //Check if there is already any Booking over this Item
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            ResultSet rs = stm.executeQuery();

            while (rs.next()) {
                if(rs.getInt("carID") == requestedId){
                    LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                    LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                    if (!isAvailable(startDate, endDate, startDateEntry, endDateEntry)) {
                        return Operations.ABORT;
                    }

                }
            }

            //Car is available
            stm = con.prepareStatement("INSERT INTO booking VALUES (\"" + transaktionId + "\", \"" + startDate + "\", \"" + endDate + "\", 0, " + bookingData.getSelectedCar() + ")");
            stm.executeUpdate();
            System.out.println("successfully booked");

            return Operations.ABORT;
        }catch (Exception e){
            System.out.println("kapput, weil " + e.getMessage());
            return Operations.ABORT;
        }

    }

    @Override
    public boolean commit(UUID transaktionId){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("UPDATE booking SET stable = 1 WHERE bookingID = \"" + transaktionId + "\"");
            stm.executeUpdate();
            return true;
        }catch(Exception e){
            System.out.println("des kann jetzt nicht Wahrsteiner im commit");
            return false;
        }
    }

    @Override
    public boolean abort(UUID transaktionId){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("DELETE FROM booking WHERE bookingID = \"" + transaktionId + "\"");
            stm.executeUpdate();
            return true;
        }catch(Exception e){
            System.out.println("des kann jetzt nicht Wahrsteiner im abort");
            return false;
        }
    }

    @Override
    public ArrayList<Object> getAvailableItems(LocalDate startDate, LocalDate endDate) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableCarIds = new ArrayList<>();
        ResultSet rs;
        UDPMessage udpMessage;
        byte[] data;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if (!isAvailable(startDate, endDate, startDateEntry, endDateEntry)) {
                    availableCarIds.add(rs.getString("carID"));
                }
            }
            String availableCarsIds = "";
            for (int i = 0; i < availableCarIds.size(); i++) {
                if( i < availableCarIds.size() - 1){
                    availableCarsIds += availableCarIds.get(i) + ", ";
                }else{
                    availableCarsIds += availableCarIds.get(i);
                }
            }

            stm = con.prepareStatement("SELECT * FROM car WHERE carID NOT IN (?)");
            stm.setString(1, availableCarsIds);
            rs = stm.executeQuery();
            ArrayList<Object> availableCars = new ArrayList<>();
            while(rs.next()){
                availableCars.add(new Car(rs.getInt("carID"), rs.getString("brand"), rs.getString("type")));
            }
            System.out.println(availableCars);
            return availableCars;

        }catch(Exception e){
            System.out.println("An Error has occured: " + e.getMessage());
            return null;
        }
    }

    public boolean isAvailable(LocalDate startDate, LocalDate endDate, LocalDate startDateEntry, LocalDate endDateEntry){
        if (startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate)) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

    }
}

