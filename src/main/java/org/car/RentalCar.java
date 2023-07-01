package org.car;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.utils.Operations;
import org.utils.Participant;
import org.utils.SendingInformation;
import org.utils.UDPMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.UUID;

public class RentalCar extends Participant{

    @Override
    public Operations Vote() {
        return Operations.ABORT;
    }

    @Override
    public void book() {

    }

    @Override
    public byte[] getAvailableItems(LocalDate startDate, LocalDate endDate, UUID pTransaktionnumber) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableCarIds = new ArrayList<>();
        ResultSet rs = null;
        ObjectMapper objectMapper = new ObjectMapper();
        UDPMessage udpMessage;
        byte[] data;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if (startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate)) {
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

            stm = con.prepareStatement("SELECT * FROM car WHERE carID IN (?)");
            stm.setString(1, availableCarsIds);
            rs = stm.executeQuery();
            ArrayList<Car> availableCars = new ArrayList<>();
            while(rs.next()){
                availableCars.add(new Car(rs.getInt("carID"), rs.getString("brand"), rs.getString("type")));
            }

            data = objectMapper.writeValueAsBytes(availableCars);
            udpMessage = new UDPMessage(pTransaktionnumber, data, SendingInformation.RENTALCAR, Operations.AVAILIBILITY);

            return objectMapper.writeValueAsBytes(udpMessage);

        }catch(Exception e){
            String errorMessage = "Something went wrong:\n" + e.getMessage();
            byte[] res;
            try {
                data = objectMapper.writeValueAsBytes(errorMessage);
                udpMessage = new UDPMessage(pTransaktionnumber, data, SendingInformation.RENTALCAR, Operations.AVAILIBILITY);
                res = objectMapper.writeValueAsBytes(udpMessage);
            }catch (com.fasterxml.jackson.core.JsonProcessingException er){
                return errorMessage.getBytes();
            }

            return res;
        }
    }

    public static void main(String[] args) {

    }
}

