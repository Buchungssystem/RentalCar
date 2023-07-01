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

   // @Override
   // public Operations Vote() {
    //    return Operations.ABORT;
  //  }

    @Override
    // check if start-enddate and ID correlates with existing bookings (like available cars);
    // if not, then create new booking (stable=false) with start- and enddate and car_ID and return COMMIT;
    // else return ABORT;
    public Operations prepare(LocalDate startDate, LocalDate endDate, int ID) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ResultSet rs;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();
            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                int idEntry = rs.getInt("carID");
                if ((startDate.isAfter(startDateEntry) && startDate.isBefore(endDateEntry)) || (endDate.isAfter(startDateEntry) && endDate.isBefore(endDateEntry))) {
                    if (idEntry == ID){
                        return Operations.ABORT;
                    }
                }
            }
            PreparedStatement insert = con.prepareStatement("INSERT INTO booking (startDate, endDate, stable) VALUES (startDateVariable, endDateVariable, False)");
            insert.executeQuery();
            return Operations.COMMIT;
            }
        catch(Exception e){}
    }


    @Override
    public Operations book(int ID) {
        // setting booking attribute stable to true, to confirm booking
        // return SUCCESS, otherwise FAIL and ID
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement update = con.prepareStatement("UPDATE booking SET stable = True WHERE (bookingID = ID)");
            update.executeQuery();
            return Operations.SUCCESS;

        }catch(Exception e){
            return Operations.FAIL;
        }

    }



    @Override
    // delete booking with ID, when ABORT
    public void bookingCleanUp(int ID){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement delete = con.prepareStatement("DELETE FROM booking WHERE bookingID = ID");
            delete.executeQuery();

            }catch(Exception e){}

        }



    @Override
    public byte[] getAvailableItems(LocalDate startDate, LocalDate endDate, UUID pTransaktionnumber) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableCarIds = new ArrayList<>();
        ResultSet rs;
        ObjectMapper objectMapper = new ObjectMapper();
        UDPMessage udpMessage;
        byte[] data;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if ((startDate.isAfter(startDateEntry) && startDate.isBefore(endDateEntry)) || (endDate.isAfter(startDateEntry) && endDate.isBefore(endDateEntry))) {
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

